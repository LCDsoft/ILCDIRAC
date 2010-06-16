########################################################################
# $Id: UserJobFinalization.py,v 1.17 2009/07/29 14:06:24 paterson Exp $
########################################################################
""" Module to upload specified job output files according to the parameters
    defined in the user workflow.
"""

__RCSID__ = "$Id: UserJobFinalization.py,v 1.17 2009/07/29 14:06:24 paterson Exp $"

from DIRAC.DataManagementSystem.Client.ReplicaManager      import ReplicaManager
from DIRAC.DataManagementSystem.Client.FailoverTransfer    import FailoverTransfer
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.Core.Security.Misc                              import getProxyInfo
from DIRAC.Core.Utilities                                  import List

from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from ILCDIRAC.Core.Utilities.ProductionData               import constructUserLFNs
from ILCDIRAC.Core.Utilities.ResolveSE                    import getDestinationSEList 

from DIRAC                                                 import S_OK, S_ERROR, gLogger, gConfig

import DIRAC
import string,os,random,time

class UserJobFinalization(ModuleBase):

  #############################################################################
  def __init__(self):
    """Module initialization.
    """
    ModuleBase.__init__(self)
    self.version = __RCSID__
    self.log = gLogger.getSubLogger( "UserJobFinalization" )
    self.jobID = ''
    self.enable=True
    self.failoverTest=False #flag to put file to failover SE by default
    self.defaultOutputSE = gConfig.getValue( '/Resources/StorageElementGroups/Tier1-USER',[])    
    self.failoverSEs = gConfig.getValue('/Resources/StorageElementGroups/Tier1-Failover',[])
    #List all parameters here
    self.userFileCatalog='FileCatalog'
    self.request = None
    self.lastStep = False
    #Always allow any files specified by users    
    self.outputDataFileMask = ''
    self.userOutputData = '' 
    self.userOutputSE = ''
    self.userOutputPath = ''
    self.jobReport = None
    
  #############################################################################
  def resolveInputVariables(self):
    """ By convention the module parameters are resolved here.
    """
    self.log.verbose(self.workflow_commons)
    self.log.verbose(self.step_commons)

    #Earlier modules may have populated the report objects
    if self.workflow_commons.has_key('JobReport'):
      self.jobReport = self.workflow_commons['JobReport']

    if self.step_commons.has_key('Enable'):
      self.enable=self.step_commons['Enable']
      if not type(self.enable)==type(True):
        self.log.warn('Enable flag set to non-boolean value %s, setting to False' %self.enable)
        self.enable=False

    if self.step_commons.has_key('TestFailover'):
      self.enable=self.step_commons['TestFailover']
      if not type(self.failoverTest)==type(True):
        self.log.warn('Test failover flag set to non-boolean value %s, setting to False' %self.failoverTest)
        self.failoverTest=False

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

    #Use LHCb utility for local running via dirac-jobexec
    if self.workflow_commons.has_key('UserOutputData'):
        self.userOutputData = self.workflow_commons['UserOutputData']
        if not type(self.userOutputData)==type([]):
          self.userOutputData = [i.strip() for i in self.userOutputData.split(';')]
    
    if self.workflow_commons.has_key('UserOutputSE'):
      specifiedSE = self.workflow_commons['UserOutputSE']
      if not type(specifiedSE)==type([]):
        self.userOutputSE = [i.strip() for i in specifiedSE.split(';')]
    else:
      self.log.verbose('No UserOutputSE specified, using default value: %s' %(string.join(self.defaultOutputSE,', ')))
      self.userOutputSE = self.defaultOutputSE

    if self.workflow_commons.has_key('UserOutputPath'):
      self.userOutputPath = self.workflow_commons['UserOutputPath']

    return S_OK('Parameters resolved')

  #############################################################################
  def execute(self):
    """ Main execution function.
    """
    #Have to work out if the module is part of the last step i.e. 
    #user jobs can have any number of steps and we only want 
    #to run the finalization once.
    currentStep = int(self.step_commons['STEP_NUMBER'])
    totalSteps = int(self.workflow_commons['TotalSteps'])
    if currentStep==totalSteps:
      self.lastStep=True
    else:
      self.log.verbose('Current step = %s, total steps of workflow = %s, UserJobFinalization will enable itself only at the last workflow step.' %(currentStep,totalSteps))            
        
    if not self.lastStep:
      return S_OK()    
    
    result = self.resolveInputVariables()
    if not result['OK']:
      self.log.error(result['Message'])
      return result

    self.log.info('Initializing %s' %self.version)
    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' %(self.workflowStatus['OK'],self.stepStatus['OK']))
      return S_OK('No output data upload attempted')
    
    if not self.userOutputData:
      self.log.info('No user output data is specified for this job, nothing to do')
      return S_OK('No output data to upload')
        
    #Determine the final list of possible output files for the
    #workflow and all the parameters needed to upload them.
    outputList = []
    for i in self.userOutputData:
      outputList.append({'outputDataType':string.upper(string.split(i,'.')[-1]),'outputDataSE':self.userOutputSE,'outputDataName':os.path.basename(i)})

    userOutputLFNs = []
    if self.userOutputData:
      self.log.info('Constructing user output LFN(s) for %s' %(string.join(self.userOutputData,', ')))
      if not self.jobID:
        self.jobID = 12345
      owner = ''
      if self.workflow_commons.has_key('Owner'):
        owner = self.workflow_commons['Owner']
      else:
        owner = self.getCurrentOwner()['Value']
      
      result = constructUserLFNs(int(self.jobID),owner,self.userOutputData,self.userOutputPath)
      if not result['OK']:
        self.log.error('Could not create production LFNs',result['Message'])
        return result
      userOutputLFNs=result['Value']

    self.log.verbose('Calling getCandidateFiles( %s, %s, %s)' %(outputList,userOutputLFNs,self.outputDataFileMask))
    result = self.getCandidateFiles(outputList,userOutputLFNs,self.outputDataFileMask)
    if not result['OK']:
      self.setApplicationStatus(result['Message'])
      return S_OK()
    
    fileDict = result['Value']      
    result = self.getFileMetadata(fileDict)
    if not result['OK']:
      self.setApplicationStatus(result['Message'])
      return S_OK()

    if not result['Value']:
      self.log.info('No output data files were determined to be uploaded for this workflow')
      self.setApplicationStatus('No Output Data Files To Upload')
      return S_OK()

    fileMetadata = result['Value']
    
    #First get the local (or assigned) SE to try first for upload and others in random fashion
    result = getDestinationSEList('Tier1-USER',DIRAC.siteName(),outputmode='local')
    if not result['OK']:
      self.log.error('Could not resolve output data SE',result['Message'])
      self.setApplicationStatus('Failed To Resolve OutputSE')
      return result      
    
    localSE=result['Value']
    self.log.verbose('Site Local SE for user outputs is: %s' %(localSE))
    orderedSEs = self.defaultOutputSE  
    for se in localSE:
      if se in orderedSEs:
        orderedSEs.remove(se)
    for se in self.userOutputSE:
      if se in orderedSEs:
        orderedSEs.remove(se)  

    orderedSEs = localSE + List.randomize(orderedSEs)    
    if self.userOutputSE:
      prependSEs = []
      for userSE in self.userOutputSE:
        if not userSE in orderedSEs:
          prependSEs.append(userSE)
      orderedSEs = prependSEs + orderedSEs
    
    self.log.info('Ordered list of output SEs is: %s' %(string.join(orderedSEs,', ')))    
    final = {}
    for fileName,metadata in fileMetadata.items():
      final[fileName]=metadata
      final[fileName]['resolvedSE']=orderedSEs

    #At this point can exit and see exactly what the module will upload
    if not self.enable:
      self.log.info('Module is disabled by control flag, would have attempted to upload the following files %s' %string.join(final.keys(),', '))
      for fileName,metadata in final.items():
        self.log.info('--------%s--------' %fileName)
        for n,v in metadata.items():
          self.log.info('%s = %s' %(n,v))

      return S_OK('Module is disabled by control flag')

    #Instantiate the failover transfer client with the global request object
    failoverTransfer = FailoverTransfer(self.request)

    #One by one upload the files with failover if necessary
    replication = {}
    failover = {}
    uploaded = []
    if not self.failoverTest:
      for fileName,metadata in final.items():
        self.log.info("Attempting to store file %s to the following SE(s):\n%s" % (fileName, string.join(metadata['resolvedSE'],', ')))
        result = failoverTransfer.transferAndRegisterFile(fileName,metadata['localpath'],metadata['lfn'],metadata['resolvedSE'],fileGUID=metadata['guid'],fileCatalog=self.userFileCatalog)
        if not result['OK']:
          self.log.error('Could not transfer and register %s with metadata:\n %s' %(fileName,metadata))
          failover[fileName]=metadata
        else:
          #Only attempt replication after successful upload
          lfn = metadata['lfn']
          uploaded.append(lfn)          
          seList = metadata['resolvedSE']
          replicateSE = ''
          if result['Value'].has_key('uploadedSE'):
            uploadedSE = result['Value']['uploadedSE']            
            for se in seList:
              if not se == uploadedSE:
                replicateSE = se
                break
          
          if replicateSE and lfn:
            self.log.info('Will attempt to replicate %s to %s' %(lfn,replicateSE))    
            replication[lfn]=replicateSE            
    else:
      failover = final

    cleanUp = False
    for fileName,metadata in failover.items():
      random.shuffle(self.failoverSEs)
      targetSE = metadata['resolvedSE'][0]
      metadata['resolvedSE']=self.failoverSEs
      result = failoverTransfer.transferAndRegisterFileFailover(fileName,metadata['localpath'],metadata['lfn'],targetSE,metadata['resolvedSE'],fileGUID=metadata['guid'],fileCatalog=self.userFileCatalog)
      if not result['OK']:
        self.log.error('Could not transfer and register %s with metadata:\n %s' %(fileName,metadata))
        cleanUp = True
        continue #for users can continue even if one completely fails
      else:
        lfn = metadata['lfn']
        uploaded.append(lfn)

    #For files correctly uploaded must report LFNs to job parameters
    if uploaded:
      report = string.join( uploaded, ', ' )
      self.jobReport.setJobParameter( 'UploadedOutputData', report )

    #Now after all operations, retrieve potentially modified request object
    result = failoverTransfer.getRequestObject()
    if not result['OK']:
      self.log.error(result)
      return S_ERROR('Could Not Retrieve Modified Request')

    self.request = result['Value']

    #If some or all of the files failed to be saved to failover
    if cleanUp:
      self.workflow_commons['Request']=self.request
      #Leave any uploaded files just in case it is useful for the user
      #do not try to replicate any files.
      return S_ERROR('Failed To Upload Output Data')
    
    #If there is now at least one replica for uploaded files can trigger replication
    rm = ReplicaManager()
    self.log.info('Sleeping for 10 seconds before attempting replication of recently uploaded files')
    time.sleep(10)
    for lfn,repSE in replication.items():
      result = rm.replicate(lfn,repSE)
      if not result['OK']:
        self.log.info('Replication failed with below error but file already exists in Grid storage with at least one replica:\n%s' %(result))

    self.workflow_commons['Request']=self.request
    
    #Now must ensure if any pending requests are generated that these are propagated to the job wrapper
    reportRequest = None
    if self.jobReport:
      result = self.jobReport.generateRequest()
      if not result['OK']:
        self.log.warn('Could not generate request for job report with result:\n%s' %(result))
      else:
        reportRequest = result['Value']
    if reportRequest:
      self.log.info('Populating request with job report information')
      self.request.update(reportRequest)
    
    if not self.request.isEmpty()['Value']:
      request_string = self.request.toXML()['Value']
      # Write out the request string
      fname = 'user_job_%s_request.xml' %(self.jobID)
      xmlfile = open(fname,'w')
      xmlfile.write(request_string)
      xmlfile.close()
      self.log.info('Creating failover request for deferred operations for job %s:' %self.jobID)
      result = self.request.getDigest()
      if result['OK']:
        digest = result['Value']
        self.log.info(digest)
    
    self.setApplicationStatus('Job Finished Successfully')
    return S_OK('Output data uploaded')

  #############################################################################
  def getCurrentOwner(self):
    """Simple function to return current DIRAC username.
    """
    result = getProxyInfo()
    if not result['OK']:
      return S_ERROR('Could not obtain proxy information')
    
    if not result['Value'].has_key('username'):
      return S_ERROR('Could not get username from proxy')
    
    username = result['Value']['username']
    return S_OK(username)

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#