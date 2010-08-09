########################################################################
# $Id: UploadOutputData.py 25072 2010-05-11 12:18:24Z paterson $
########################################################################
""" Module to upload specified job output files according to the parameters
    defined in the production workflow.
"""

__RCSID__ = "$Id: UploadOutputData.py 25072 2010-05-11 12:18:24Z paterson $"

from DIRAC.DataManagementSystem.Client.FailoverTransfer    import FailoverTransfer
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from ILCDIRAC.Core.Utilities.ProductionData                import constructProductionLFNs
from ILCDIRAC.Core.Utilities.ResolveSE                     import getDestinationSEList
from ILCDIRAC.Core.Utilities.resolveOFnames                import getProdFilename
from ILCDIRAC.Workflow.Modules.ModuleBase                  import ModuleBase

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
import DIRAC

import string,os,random,time

class UploadOutputData(ModuleBase):

  #############################################################################
  def __init__(self):
    """Module initialization.
    """
    ModuleBase.__init__(self)
    self.version = __RCSID__
    self.log = gLogger.getSubLogger( "UploadOutputData" )
    self.commandTimeOut = 10*60
    self.jobID = ''
    self.enable=True
    self.failoverTest=False #flag to put file to failover SE by default
    self.failoverSEs = gConfig.getValue('/Resources/StorageElementGroups/Tier1-Failover',[])

    #List all parameters here
    self.inputData = []
    self.outputDataFileMask = ''
    self.outputMode='Any' #or 'Local' for reco case
    self.outputList = []
    self.request = None
    self.PRODUCTION_ID=""

  #############################################################################
  def resolveInputVariables(self):
    """ By convention the module parameters are resolved here.
    """
    self.log.verbose(self.workflow_commons)
    self.log.verbose(self.step_commons)

    if self.step_commons.has_key('UploadEnable'):
      self.enable=self.step_commons['UploadEnable']
      if not type(self.enable)==type(True):
        self.log.warn('Enable flag set to non-boolean value %s, setting to False' %self.enable)
        self.enable=False

    if self.step_commons.has_key('TestFailover'):
      self.enable=self.step_commons['TestFailover']
      if not type(self.failoverTest)==type(True):
        self.log.warn('Test failover flag set to non-boolean value %s, setting to False' %self.failoverTest)
        self.failoverTest=False

    if self.workflow_commons.has_key("PRODUCTION_ID"):
      self.PRODUCTION_ID = self.workflow_commons["PRODUCTION_ID"]

    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']
      self.log.verbose('Found WMS JobID = %s' %self.jobID)
    else:
      self.log.info('No WMS JobID found, disabling module via control flag')
      self.enable=False

    if self.workflow_commons.has_key('Request'):
      self.request = self.workflow_commons['Request']
    else:
      self.request = RequestContainer()
      self.request.setRequestName('job_%s_request.xml' % self.jobID)
      self.request.setJobID(self.jobID)
      self.request.setSourceComponent("Job_%s" % self.jobID)

    if self.workflow_commons.has_key('outputList'):
      self.outputList = self.workflow_commons['outputList']
      proddata = self.workflow_commons['ProductionOutputData'].split(";")
      self.log.verbose("prod data : %s"%proddata )
      olist = []
      for obj in self.outputList:
        for prodfile in proddata:
          if (obj['outputFile'].lower().count("_sim") and prodfile.lower().count("_sim_")) or (obj['outputFile'].lower().count("_rec") and prodfile.lower().count("_rec_")) or (obj['outputFile'].lower().count("_dst") and prodfile.lower().count("_dst_")):
            appdict = obj
            appdict['outputFile'] = os.path.basename(prodfile)
            olist.append(appdict)
            break
      self.outputList = olist
      self.log.verbose("OutputList : %s"%self.outputList)

    if self.workflow_commons.has_key('outputMode'):
      self.outputMode = self.workflow_commons['outputMode']

    if self.workflow_commons.has_key('outputDataFileMask'):
        self.outputDataFileMask = self.workflow_commons['outputDataFileMask']
        if not type(self.outputDataFileMask)==type([]):
          self.outputDataFileMask = [i.lower().strip() for i in self.outputDataFileMask.split(';')]

    result = constructProductionLFNs(self.workflow_commons)
    if not result['OK']:
      self.log.error('Could not create production LFNs',result['Message'])
      return result
    self.prodOutputLFNs=result['Value']['ProductionOutputData']

    if self.workflow_commons.has_key('InputData'):
      self.inputData = self.workflow_commons['InputData']

    if self.inputData:
      if type(self.inputData) != type([]):
        self.inputData = self.inputData.split(';')

    return S_OK('Parameters resolved')

  #############################################################################
  def execute(self):
    """ Main execution function.
    """
    self.log.info('Initializing %s' %self.version)
    result = self.resolveInputVariables()
    if not result['OK']:
      self.log.error(result['Message'])
      return result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' %(self.workflowStatus['OK'],self.stepStatus['OK']))
      return S_OK('No output data upload attempted')

    #Determine the final list of possible output files for the
    #workflow and all the parameters needed to upload them.
    result = self.getCandidateFiles(self.outputList,self.prodOutputLFNs,self.outputDataFileMask)
    if not result['OK']:
      self.setApplicationStatus(result['Message'])
      return result
    
    fileDict = result['Value']      
    result = self.getFileMetadata(fileDict)
    if not result['OK']:
      self.setApplicationStatus(result['Message'])
      return result

    if not result['Value']:
      self.log.info('No output data files were determined to be uploaded for this workflow')
      return S_OK()

    fileMetadata = result['Value']

    #Get final, resolved SE list for files
    final = {}
    for fileName,metadata in fileMetadata.items():
      result = getDestinationSEList(metadata['workflowSE'],DIRAC.siteName(),self.outputMode)
      if not result['OK']:
        self.log.error('Could not resolve output data SE',result['Message'])
        self.setApplicationStatus('Failed To Resolve OutputSE')
        return result
      
      resolvedSE = result['Value']
      final[fileName]=metadata
      final[fileName]['resolvedSE']=resolvedSE

    self.log.info('The following files will be uploaded: %s' %(string.join(final.keys(),', ')))
    for fileName,metadata in final.items():
      self.log.info('--------%s--------' %fileName)
      for n,v in metadata.items():
        self.log.info('%s = %s' %(n,v))

    #At this point can exit and see exactly what the module would have uploaded
    if not self.enable:
      self.log.info('Module is disabled by control flag, would have attempted to upload the following files %s' %string.join(final.keys(),', '))
      return S_OK('Module is disabled by control flag')

    #Disable the watchdog check in case the file uploading takes a long time
    self.log.info('Creating DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK in order to disable the Watchdog prior to upload')
    fopen = open('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK','w')
    fopen.write('%s' %time.asctime())
    fopen.close()
    
    #Instantiate the failover transfer client with the global request object
    failoverTransfer = FailoverTransfer(self.request)


    #One by one upload the files with failover if necessary
    failover = {}
    if not self.failoverTest:
      for fileName,metadata in final.items():
        self.log.info("Attempting to store file %s to the following SE(s):\n%s" % (fileName, string.join(metadata['resolvedSE'],', ')))
        result = failoverTransfer.transferAndRegisterFile(fileName,metadata['localpath'],metadata['lfn'],metadata['resolvedSE'],fileGUID=metadata['guid'],fileCatalog='FileCatalog')
        if not result['OK']:
          self.log.error('Could not transfer and register %s with metadata:\n %s' %(fileName,metadata))
          failover[fileName]=metadata
        else:
          lfn = metadata['lfn']
    else:
      failover = final

    cleanUp = False
    for fileName,metadata in failover.items():
      self.log.info('Setting default catalog for failover transfer to FileCatalog')
      random.shuffle(self.failoverSEs)
      targetSE = metadata['resolvedSE'][0]
      metadata['resolvedSE']=self.failoverSEs
      result = failoverTransfer.transferAndRegisterFileFailover(fileName,metadata['localpath'],metadata['lfn'],targetSE,metadata['resolvedSE'],fileGUID=metadata['guid'],fileCatalog='FileCatalog')
      if not result['OK']:
        self.log.error('Could not transfer and register %s with metadata:\n %s' %(fileName,metadata))
        cleanUp = True
        break #no point continuing if one completely fails

    #Now after all operations, retrieve potentially modified request object
    result = failoverTransfer.getRequestObject()
    if not result['OK']:
      self.log.error(result)
      return S_ERROR('Could not retrieve modified request')

    self.request = result['Value']

    #If some or all of the files failed to be saved to failover
    if cleanUp:
      lfns = []
      for fileName,metadata in final.items():
        lfns.append(metadata['lfn'])

      result = self.__cleanUp(lfns)
      self.workflow_commons['Request']=self.request
      return S_ERROR('Failed to upload output data')

#    #Can now register the successfully uploaded files in the BK
#    if not performBKRegistration:
#      self.log.info('There are no files to perform the BK registration for, all could be saved to failover')
#    else:
#      rm = ReplicaManager()
#      result = rm.addCatalogFile(performBKRegistration,catalogs=['BookkeepingDB'])
#      self.log.verbose(result)
#      if not result['OK']:
#        self.log.error(result)
#        return S_ERROR('Could Not Perform BK Registration')
#      if result['Value']['Failed']:
#        for lfn,error in result['Value']['Failed'].items():
#          self.log.info('BK registration for %s failed with message: "%s" setting failover request' %(lfn,error))
#          result = self.request.addSubRequest({'Attributes':{'Operation':'registerFile','ExecutionOrder':0, 'Catalogue':'BookkeepingDB'}},'register')
#          if not result['OK']:
#            self.log.error('Could not set registerFile request:\n%s' %result)
#            return S_ERROR('Could Not Set BK Registration Request')
#          fileDict = {'LFN':lfn,'Status':'Waiting'}
#          index = result['Value']
#          self.request.setSubRequestFiles(index,'register',[fileDict])

    self.workflow_commons['Request']=self.request
    return S_OK('Output data uploaded')

  #############################################################################
  def __cleanUp(self,lfnList):
    """ Clean up uploaded data for the LFNs in the list
    """
    # Clean up the current request
    for req_type in ['transfer','register']:
      for lfn in lfnList:
        result = self.request.getNumSubRequests(req_type)
        if result['OK']:
          nreq = result['Value']
          if nreq:
            # Go through subrequests in reverse order in order not to spoil the numbering
            ind_range = [0]
            if nreq > 1:
              ind_range = range(nreq-1,-1,-1)
            for i in ind_range:
              result = self.request.getSubRequestFiles(i,req_type)
              if result['OK']:
                fileList = result['Value']
                if fileList[0]['LFN'] == lfn:
                  result = self.request.removeSubRequest(i,req_type)

    # Set removal requests just in case
    for lfn in lfnList:
      result = self.request.addSubRequest({'Attributes':{'Operation':'removeFile','TargetSE':'','ExecutionOrder':1}},'removal')
      index = result['Value']
      fileDict = {'LFN':lfn,'PFN':'','Status':'Waiting'}
      self.request.setSubRequestFiles(index,'removal',[fileDict])

    return S_OK()

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#