""" In general for data processing producitons we need to completely abandon the 'by hand'
    reschedule operation such that accidental reschedulings don't result in data being processed twice.
    
    For all above cases the following procedure should be used to achieve 100%:
    
    - Starting from the data in the Production DB for each transformation
      look for files in the following status:
         Assigned   
         MaxReset
      some of these will correspond to the final WMS status 'Failed'.
    
    For files in MaxReset and Assigned:
    - Discover corresponding job WMS ID
    - Check that there are no outstanding requests for the job 
      o wait until all are treated before proceeding
    - Check that none of the job input data has BK descendants for the current production
      o if the data has a replica flag it means all was uploaded successfully - should be investigated by hand
      o if there is no replica flag can proceed with file removal from LFC / storage (can be disabled by flag)
    - Mark the recovered input file status as 'Unused' in the ProductionDB
"""

__RCSID__   = "$Id: DataRecovery.py 18182 2009-11-11 14:45:10Z paterson $"
__VERSION__ = "$Revision: 1.9 $"

from DIRAC                                                     import S_OK, S_ERROR, gConfig, gLogger, rootPath
from DIRAC.Core.Base.AgentModule                               import AgentModule
from DIRAC.DataManagementSystem.Client.ReplicaManager          import ReplicaManager
from DIRAC.RequestManagementSystem.Client.RequestClient        import RequestClient
from DIRAC.Core.Utilities.List                                 import uniqueElements
from DIRAC.Core.Utilities.Time                                 import timeInterval,dateTime
from DIRAC.Core.Utilities.Shifter                              import setupShifterProxyInEnv
from DIRAC.Core.DISET.RPCClient                                import RPCClient

#from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient      import BookkeepingClient
from DIRAC.TransformationSystem.Client.TransformationClient      import TransformationClient  
from ILCDIRAC.Core.Utilities.ProductionData import constructProductionLFNs
from DIRAC.Core.Workflow.Workflow                   import *


import string,re,datetime

AGENT_NAME = 'ProductionManagement/DataRecoveryAgent'

class DataRecoveryAgent():
  def __init__(self):
    self.name = 'toto'
    self.log=gLogger
  #############################################################################
  def initialize(self):
    """Sets defaults
    """
    self.enableFlag = '' #defined below
    self.replicaManager = ReplicaManager()
    self.prodDB = TransformationClient()
    #self.bkClient = BookkeepingClient()
    self.requestClient = RequestClient()
    self.taskIDName = 'TaskID' 
    self.externalStatus = 'ExternalStatus'
    self.externalID = 'ExternalID'
    self.am_setOption('PollingTime',2*60*60) #no stalled jobs are considered so can be frequent
    self.am_setModuleParam("shifterProxy", "ProductionManager")
    #self.am_setModuleParam("shifterProxyLocation","%s/runit/%s/proxy" % (rootPath,AGENT_NAME))
    return S_OK()

  #############################################################################
  def execute(self):
    """ The main execution method.
    """  
    #Configuration settings
    self.enableFlag = True
#    self.enableFlag = self.am_getOption('EnableFlag',False)
    self.log.info('Enable flag is %s' %self.enableFlag)  
    self.removalOKFlag = True
    
    transformationTypes = ['MCReconstruction','MCSimulation','MCReconstruction_Overlay','Merge']
    transformationStatus = ['Active','Completing']
    fileSelectionStatus = ['Assigned','MaxReset']
    updateStatus = 'Unused'
    wmsStatusList = ['Failed']
        
    #only worry about files > 12hrs since last update    
    selectDelay = 1 #hours 

    transformationDict = {}
    for transStatus in transformationStatus:
      result = self.getEligibleTransformations(transStatus,transformationTypes)
      if not result['OK']:
        self.log.error(result)
        return S_ERROR('Could not obtain eligible transformations for status "%s"' %(transStatus))
      
      if not result['Value']:
        self.log.info('No "%s" transformations of types %s to process.' %(transStatus,string.join(transformationTypes,', ')))
        continue

      transformationDict.update(result['Value'])

    self.log.info('Selected %s transformations of types %s' %(len(transformationDict.keys()),string.join(transformationTypes,', ')))
    self.log.verbose('The following transformations were selected out of %s:\n%s' %(string.join(transformationTypes,', '),string.join(transformationDict.keys(),', ')))

    trans = []
    #initially this was useful for restricting the considered list
    #now we use the DataRecoveryAgent in setups where IDs are low
    ignoreLessThan = '724' 
    
    ########## Uncomment for debugging
#    self.enableFlag = False 
#    self.removalOKFlag = False
#    trans.append('6357')
#    trans.append('6361')
#    trans.append('6362')
#    trans.append('6338')
#    trans.append('6314')
    ########## Uncomment for debugging      
    
    if trans:
      self.log.info('Skipping all transformations except %s' %(string.join(trans,', ')))
          
    for transformation,typeName in transformationDict.items():
      if trans:
        if not transformation in trans:
          continue
      if ignoreLessThan:
        if int(ignoreLessThan)>int(transformation):
          self.log.verbose('Ignoring transformation %s ( is less than specified limit %s )' %(transformation,ignoreLessThan))
          continue

      self.log.info('='*len('Looking at transformation %s type %s:' %(transformation,typeName)))
      self.log.info('Looking at transformation %s:' %(transformation))

      result = self.selectTransformationFiles(transformation,fileSelectionStatus)
      if not result['OK']:
        self.log.error(result)
        self.log.error('Could not select files for transformation %s' %transformation)
        continue
  
      if not result['Value']:
        self.log.info('No files in status %s selected for transformation %s' %(string.join(fileSelectionStatus,', '),transformation))
        continue
    
      fileDict = result['Value']      
      result = self.obtainWMSJobIDs(transformation,fileDict,selectDelay,wmsStatusList)
      if not result['OK']:
        self.log.error(result)
        self.log.error('Could not obtain WMS jobIDs for files of transformation %s' %(transformation))
        continue
      if not result['Value']:
        self.log.info('No eligible WMS jobIDs found for %s files in list:\n%s ...' %(len(fileDict.keys()),fileDict.keys()[0]))
        continue
    
      jobFileDict = result['Value']
      fileCount = 0
      for job,lfnList in jobFileDict.items():
        fileCount+=len(lfnList)
      
      if not fileCount:
        self.log.info('No files were selected for transformation %s after examining WMS jobs.' %transformation)
        continue
      
      self.log.info('%s files are selected after examining related WMS jobs' %(fileCount))   
      result = self.checkOutstandingRequests(jobFileDict)
      if not result['OK']:
        self.log.error(result)
        continue

      if not result['Value']:
        self.log.info('No WMS jobs without pending requests to process.')
        continue
      
      jobFileNoRequestsDict = result['Value']
      fileCount = 0
      for job,lfnList in jobFileNoRequestsDict.items():
        fileCount+=len(lfnList)
      
      self.log.info('%s files are selected after removing any relating to jobs with pending requests' %(fileCount))
      result = self.checkDescendents(transformation,fileDict,jobFileNoRequestsDict)
      if not result['OK']:
        self.log.error(result)
        continue


      #jobsWithFilesOKToUpdate = result['Value']['jobfiledictok']       
##       problematicFiles = result['Value']['toremove']
      jobsWithFilesOKToUpdate = result['Value']['jobfiledictok']
      jobsWithFilesProcessed = result['Value']['filesprocessed']
##       jobsWithProblematicFiles = result['Value']['jobfiledictproblematic']
##       jobsWithDescendentsInBK = result['Value']['replicaflagproblematic']
      self.log.info('====> Transformation %s total jobs that can be updated now: %s' %(transformation,len(jobsWithFilesOKToUpdate.keys())))
##       self.log.info('====> Transformation %s total jobs with problematic descendent files but no replica flags: %s' %(transformation,len(jobsWithProblematicFiles.keys())))
##       self.log.info('====> Transformation %s total jobs with problematic descendent files having BK replica flags: %s' %(transformation,len(jobsWithDescendentsInBK.keys())))
            
      filesToUpdate = []
      for job,fileList in jobsWithFilesOKToUpdate.items():
        filesToUpdate+=fileList
      
      if filesToUpdate:
        result = self.updateFileStatus(transformation,filesToUpdate,updateStatus)
        if not result['OK']:
          self.log.error('Recoverable files were not updated with result:\n%s' %(result))
          continue          
      else:
        self.log.info('There are no files without problematic descendents to update for production %s in this cycle' %transformation)              
      
##       if problematicFiles:
##         if self.removalOKFlag:
##           result = self.removeOutputs(problematicFiles)
##           if not result['OK']:
##             self.log.error('Could not remove all problematic files with result\n%s' %(result))
##             continue
##         else:
##           for job,fileList in jobsWithProblematicFiles.items():
##             self.log.info('Job: %s, Input data: %s' %(job,string.join(fileList,'\n')))
##           self.log.info('!!!!!!!!Production %s has %s problematic descendent files without replica flags (found from %s jobs above).' %(transformation,len(problematicFiles),len(jobsWithProblematicFiles.keys())))
##           self.log.info('This must be investigated by hand or removalOKFlag should be set to True!!!!!!!!')
##           continue  
##       else:
##         self.log.info('No problematic files without replica flags were found to be removed for transformation %s' %(transformation))
      
##       problematicFilesToUpdate = []
##       for job,fileList in jobsWithProblematicFiles.items():
##         problematicFilesToUpdate+=fileList
      
##       if problematicFilesToUpdate:
##         result = self.updateFileStatus(transformation,problematicFilesToUpdate,updateStatus)
##         if not result['OK']:
##           self.log.error('Problematic files without replica flags were not updated with result:\n%s' %(result))
##           continue
##         self.log.info('%s problematic files without replica flags were recovered for transformation %s' %(len(problematicFilesToUpdate),transformation))
##       else:
##         self.log.info('There are no problematic files without replica flags to update for production %s in this cycle' %transformation)  

##       if jobsWithDescendentsInBK:
##         self.log.info('!!!!!!!! Note that transformation %s has descendents with BK replica flags for files that are not marked as processed !!!!!!!!' %(transformation))
##         for n,v in jobsWithDescendentsInBK.items():
##           self.log.info('Job %s, Files %s' %(n,v))

    if not self.enableFlag:
      self.log.info('%s is disabled by configuration option EnableFlag\ntherefore no "one-way" operations such as ProductionDB updates are performed.' %(AGENT_NAME))

    return S_OK()

  #############################################################################
  def getEligibleTransformations(self,status,typeList):
    """ Select transformations of given status and type.
    """
    res = self.prodDB.getTransformations(condDict = {'Status':status,'Type':typeList})
    self.log.debug(res)
    if not res['OK']:
      return res
    transformations = {}
    for prod in res['Value']:
      prodID = prod['TransformationID']
      transformations[str(prodID)]=prod['Type']
    return S_OK(transformations)
  
  #############################################################################
  def selectTransformationFiles(self,transformation,statusList):
    """ Select files, production jobIDs in specified file status for a given transformation.
    """
    #Until a query for files with timestamp can be obtained must rely on the
    #WMS job last update
    res = self.prodDB.getTransformationFiles(condDict={'TransformationID':transformation,'Status':statusList})
    self.log.debug(res)
    if not res['OK']:
      return res
    resDict = {}
    for fileDict in res['Value']:
      if not fileDict.has_key('LFN') or not fileDict.has_key(self.taskIDName) or not fileDict.has_key('LastUpdate'):
        self.log.info('LFN, %s and LastUpdate are mandatory, >=1 are missing for:\n%s' %(self.taskIDName,fileDict))
        continue
      lfn = fileDict['LFN']
      jobID = fileDict[self.taskIDName]
      lastUpdate = fileDict['LastUpdate']
      resDict[lfn] = jobID
    if resDict:
      self.log.info('Selected %s files overall for transformation %s' %(len(resDict.keys()),transformation))
    return S_OK(resDict)
  
  #############################################################################
  def obtainWMSJobIDs(self,transformation,fileDict,selectDelay,wmsStatusList):
    """ Group files by the corresponding WMS jobIDs, check the corresponding
        jobs have not been updated for the delay time.  Can't get into any 
        mess because we start from files only in MaxReset / Assigned and check
        corresponding jobs.  Mixtures of files for jobs in MaxReset and Assigned 
        statuses only possibly include some files in Unused status (not Processed 
        for example) that will not be touched.
    """
    prodJobIDs = uniqueElements(fileDict.values())
    self.log.info('The following %s production jobIDs apply to the selected files:\n%s' %(len(prodJobIDs),prodJobIDs))

    jobFileDict = {}
    condDict = {'TransformationID':transformation,self.taskIDName:prodJobIDs}
    delta = datetime.timedelta( hours = selectDelay )
    now = dateTime()
    olderThan = now-delta

    res = self.prodDB.getTransformationTasks(condDict=condDict,older=olderThan,timeStamp='LastUpdateTime',inputVector=True)
    self.log.debug(res)
    if not res['OK']:
      self.log.error('getTransformationTasks returned an error:\n%s')
      return res
    
    for jobDict in res['Value']:
      missingKey=False
      for key in [self.taskIDName,self.externalID,'LastUpdateTime',self.externalStatus,'InputVector']:
        if not jobDict.has_key(key):
          self.log.info('Missing key %s for job dictionary, the following is available:\n%s' %(key,jobDict))
          missingKey=True
          continue
      
      if missingKey:
        continue
        
      job = jobDict[self.taskIDName]
      wmsID = jobDict[self.externalID]
      lastUpdate = jobDict['LastUpdateTime']
      wmsStatus = jobDict[self.externalStatus]
      jobInputData = jobDict['InputVector']
      jobInputData = [lfn.replace('LFN:','') for lfn in jobInputData.split(';')]
      
      if not int(wmsID):
        self.log.info('Prod job %s status is %s (ID = %s) so will not recheck with WMS' %(job,wmsStatus,wmsID))
        continue
      
      self.log.info('Job %s, prod job %s last update %s, production management system status %s' %(wmsID,job,lastUpdate,wmsStatus))
      #Exclude jobs not having appropriate WMS status - have to trust that production management status is correct        
      if not wmsStatus in wmsStatusList:
        self.log.info('Job %s is in status %s, not %s so will be ignored' %(wmsID,wmsStatus,string.join(wmsStatusList,', ')))
        continue
        
      finalJobData = []
      #Must map unique files -> jobs in expected state
      for lfn,prodID in fileDict.items():
        if int(prodID)==int(job):
          finalJobData.append(lfn)
      
      self.log.info('Found %s files for job %s' %(len(finalJobData),job))    
      jobFileDict[wmsID]=finalJobData
 
    return S_OK(jobFileDict)
  
  #############################################################################
  def checkOutstandingRequests(self,jobFileDict):
    """ Before doing anything check that no outstanding requests are pending
        for the set of WMS jobIDs.
    """
    jobs = jobFileDict.keys()
    result = self.requestClient.getRequestForJobs(jobs)
    if not result['OK']:
      return result
    
    if not result['Value']:
      self.log.info('None of the jobs have pending requests')
      return S_OK(jobFileDict)
    
    for jobID,requestName in result['Value'].items():
      del jobFileDict[str(jobID)]  
      self.log.info('Removing jobID %s from consideration until requests are completed' %(jobID))
    
    return S_OK(jobFileDict)
  
##   #############################################################################
##   def checkDescendents(self,transformation,jobFileDict,bkDepth):
##     """ Check BK descendents for input files, prepare list of actions to be
##         taken for recovery.
##     """
##     toRemove=[]
##     problematicJobs = []
##     hasReplicaFlag = []
##     bkNotReachable = []
##     for job,fileList in jobFileDict.items():
##       if not fileList:
##         continue
##       self.log.info('Checking BK descendents for job %s...' %job)
##       #check any input data has descendant files...
##       result = self.bkClient.getAllDescendents(fileList,depth=bkDepth,production=int(transformation),checkreplica=False)
## #      result = self.bkClient.getDescendents(fileList,bkDepth)
##       if not result['OK']:
##         self.log.error('Could not obtain descendents for job %s with result:\n%s' %(job,result))
##         bkNotReachable.append(job)
##         continue
##       if result['Value']['Failed']:
##         self.log.error('Problem obtaining some descendents for job %s with result:\n%s' %(job,result['Value']))
##         bkNotReachable.append(job)
##         continue
##       jobFiles = result['Value']['Successful'].keys()
##       for fname in jobFiles:
##         descendents = result['Value']['Successful'][fname]
##         # IMPORTANT: Descendents of input files can be found with or without replica flags
##         if descendents:
##           metadata = self.bkClient.getFileMetadata(descendents)
##           if not metadata['OK']:
##             self.log.error('Could not get metadata from BK with result:\n%s' %(metadata))
##             continue
##           if result['Value']['Failed']:
##             self.log.error('Problem obtaining metadata from BK for some files with result:\n%s' %(metadata))
##             continue
          
##           #need to take a decision based on any one descendent having a replica flag
##           descendentsWithReplicas = False
##           for d in descendents:
##             if metadata['Value'][d]['GotReplica'].lower()=='yes':
##               descendentsWithReplicas=True
##               self.log.verbose('Descendent file for %s has replica flag:\n%s => %s' %(job,fname,d))
      
##           if descendentsWithReplicas:
##                With replica flag <====> Job could be OK and files processed, should investigate by hand          
##             hasReplicaFlag.append(job)
##           else:
##                Without replica flag <====> All data can be removed and a job recreated          
##             problematicJobs.append(job)
##             toRemove+=descendents
                  
##     if toRemove:
##       self.log.info('Found %s descendent files of transformation %s without BK replica flag to be removed:\n%s' %(len(toRemove),transformation,string.join(toRemove,'\n')))

##     if hasReplicaFlag:
##       self.log.info('Found %s jobs with descendent files that do have a BK replica flag' %(len(hasReplicaFlag)))
            
##     #Now resolve files that can be updated safely (e.g. even if the removalFlag is False these are updated as nothing is to be removed ;)
##     problematic = {}
##     for probJob in problematicJobs:
##       if jobFileDict.has_key(probJob):
##         pfiles = jobFileDict[probJob]
##         problematic[probJob]=pfiles
##         del jobFileDict[probJob]
    
##     #Finally resolve the jobs and files for which a descendent has a replica flag
##     replicaFlagProblematic = {}
##     for probJob in hasReplicaFlag:
##       if jobFileDict.has_key(probJob):
##         pfiles = jobFileDict[probJob]
##         replicaFlagProblematic[probJob]=pfiles
##         del jobFileDict[probJob]
    
##     #Remove files for which the BK could not be contacted from the jobFileDict
##     for removeMe in bkNotReachable:
##       if jobFileDict.has_key(removeMe):
##         del jobFileDict[removeMe]
    
##     result={'toremove':toRemove,'jobfiledictok':jobFileDict,'jobfiledictproblematic':problematic,'replicaflagproblematic':replicaFlagProblematic}
##     return S_OK(result)

  ############################################################################
  def checkDescendents(self,transformation,filedict,jobFileDict):
    """ look that all jobs produced, or not output
    """
    res = self.prodDB.getTransformationParameters(transformation,['Body'])
    if not res['OK']:
        self.log.error('Could not get Body from TransformationDB')
        return res
    body = res['Value']
    workflow = fromXMLString(body)
    workflow.resolveGlobalVars()

    list = []
    type = workflow.findParameter('JobType')
    if not type:
        self.log.error('Type for transformation %d was not defined'%transformation)
        return S_ERROR('Type for transformation %d was not defined'%transformation)
    for step in workflow.step_instances:
        param= step.findParameter('listoutput')
        if not param:
            continue
        list.extend(param.value)
    expectedlfns = []
    contactfailed = []
    fileprocessed = []
    for file,task in filedict.items():
        commons = {}
        commons['outputList'] = list
        commons['PRODUCTION_ID']= transformation
        commons['JOB_ID']=task
        commons['JobType']=type
        out = constructProductionLFNs(commons)
        expectedlfns = out['Value']['ProductionOutputData']
        res = self.replicaManager.getCatalogFileMetadata(expectedlfns)
        if not res['OK']:
          self.log.error('Getting metadata failed')
          contactfailed.append(file)
          continue
        success = res['Value']['Successful'].keys()
        failed = res['Value']['Failed'].keys()
        if len(success) and not len(failed):
          fileprocessed.append(file)

    for file in fileprocessed:
      if jobFileDict.has_key(file):
        del jobFileDict[file]
    result={'filesprocessed':fileprocessed,'jobfiledictok':jobFileDict}    
    return S_OK(result)

  ############################################################################
  def removeOutputs(self,problematicFiles):
    """ Remove outputs from any catalog / storages.
    """
    if not self.enableFlag:
      self.log.info('Enable flag False, would attempt to remove %s file(s)' %(len(problematicFiles)))
      return S_OK()
    
    self.log.info('Attempting to remove %s problematic files' %(len(problematicFiles)))
    result = self.replicaManager.removeFile(problematicFiles) #this does take a list ;)
    if not result['OK']:
      self.log.error(result)
      return result
  
    if result['Value']['Failed']:
      self.log.error(result)
      return S_ERROR(result['Value']['Failed'])
        
    self.log.info('The following problematic files were successfully removed:\n%s' %(string.join(problematicFiles,'\n')))
    return S_OK()

  #############################################################################
  def updateFileStatus(self,transformation,fileList,fileStatus):
    """ Update file list to specified status.
    """
    if not self.enableFlag:
      self.log.info('Enable flag is False, would update  %s files to "%s" status for %s' %(len(fileList),fileStatus,transformation))
      return S_OK()

    self.log.info('Updating %s files to "%s" status for %s' %(len(fileList),fileStatus,transformation))
    result = self.prodDB.setFileStatusForTransformation(int(transformation),fileStatus,fileList,force=True)
    self.log.debug(result)
    if not result['OK']:
      self.log.error(result)
      return result
    if result['Value']['Failed']:
      self.log.error(result['Value']['Failed'])
      return result
    
    msg = result['Value']['Successful']
    for lfn,message in msg.items():
      self.log.info('%s => %s' %(lfn,message))
    
    return S_OK()
