"""
Module to upload specified job output files according to the parameters
defined in the user workflow.

:author: S. Poss
:since: Sep 01, 2010
"""

__RCSID__ = "$Id$"

from DIRAC.DataManagementSystem.Client.DataManager         import DataManager
from DIRAC.DataManagementSystem.Client.FailoverTransfer    import FailoverTransfer

from DIRAC.Core.Security.ProxyInfo                         import getProxyInfo

from DIRAC.Core.Utilities                                  import List

from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from ILCDIRAC.Core.Utilities.ProductionData               import constructUserLFNs
from ILCDIRAC.Core.Utilities.ResolveSE                    import getDestinationSEList

from DIRAC                                                 import S_OK, S_ERROR, gLogger, gConfig

import DIRAC
import os, random, time

class UserJobFinalization(ModuleBase):
  """ User Job finalization: takes care of uploading the output data to the specified storage elements
  If it does not work, it will upload to a Failover SE, then register the request to replicate and remove.
  """
  #############################################################################
  def __init__(self):
    """Module initialization.
    """
    super(UserJobFinalization, self).__init__()
    self.version = __RCSID__
    self.log = gLogger.getSubLogger( "UserJobFinalization" )
    self.enable = True
    self.failoverTest = False #flag to put file to failover SE by default
    self.defaultOutputSE = gConfig.getValue( '/Resources/StorageElementGroups/Tier1-USER', [])
    self.failoverSEs = gConfig.getValue('/Resources/StorageElementGroups/Tier1-Failover', [])
    #List all parameters here
    self.userFileCatalog = self.ops.getValue('/UserJobs/Catalogs', ['FileCatalog'] )
    #Always allow any files specified by users
    self.outputDataFileMask = ''
    self.userOutputData = ''
    self.userOutputSE = ''
    self.userOutputPath = ''
    self.jobReport = None

  #############################################################################
  def applicationSpecificInputs(self):
    """ By convention the module parameters are resolved here.
    """

    #Earlier modules may have populated the report objects
    self.jobReport = self.workflow_commons.get('JobReport', self.jobReport)

    self.enable = self.step_commons.get('Enable', self.enable)
    if not type(self.enable) == type(True):
      self.log.warn('Enable flag set to non-boolean value %s, setting to False' %self.enable)
      self.enable = False

    if not type(self.failoverTest) == type(True):
      self.log.warn('Test failover flag set to non-boolean value %s, setting to False' % self.failoverTest)
      self.failoverTest = False

    self.jobID = os.environ.get('JOBID', self.jobID)
    if self.jobID != 0:
      self.log.verbose('Found WMS JobID = %s' % self.jobID)
    else:
      self.log.info('No WMS JobID found, disabling module via control flag')
      self.enable = False

    #Use LHCb utility for local running via dirac-jobexec
    self.userOutputData = self.workflow_commons.get('UserOutputData', self.userOutputData)
    if self.userOutputData and not type(self.userOutputData) == type([]):
      self.userOutputData = [i.strip() for i in self.userOutputData.split(';')]

    specifiedSE = self.workflow_commons.get('UserOutputSE', '')
    if not type(specifiedSE) == type([]) and specifiedSE:
      self.userOutputSE = [i.strip() for i in specifiedSE.split(';')]
      self.log.debug("userOutputSE is: " + str(self.userOutputSE) )
    else:
      self.log.verbose('No UserOutputSE specified, using default value: %s' % (', '.join(self.defaultOutputSE)))
      self.userOutputSE = self.defaultOutputSE

    self.userOutputPath = self.workflow_commons.get('UserOutputPath', self.userOutputPath)

    return S_OK('Parameters resolved')

  #############################################################################
  def execute(self):
    """ Main execution function.
    """
    #Have to work out if the module is part of the last step i.e.
    #user jobs can have any number of steps and we only want
    #to run the finalization once. Not a problem if this is not the last step so return S_OK()
    resultLS = self.isLastStep()
    if not resultLS['OK']:
      return S_OK()

    self.logWorkingDirectory()

    resultIV = self.resolveInputVariables()
    if not resultIV['OK']:
      self.log.error("Failed to resolve input parameters:", resultIV['Message'])
      return resultIV

    self.log.info('Initializing %s' % self.version)
    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'],
                                                                   self.stepStatus['OK']))
      return S_OK('No output data upload attempted')

    if not self.userOutputData:
      self.log.info('No user output data is specified for this job, nothing to do')
      return S_OK('No output data to upload')

    #Determine the final list of possible output files for the
    #workflow and all the parameters needed to upload them.
    outputList = self.getOutputList()

    userOutputLFNs = []
    if self.userOutputData:
      resultOLfn = self.constructOutputLFNs()
      if not resultOLfn['OK']:
        self.log.error('Could not create user LFNs', resultOLfn['Message'])
        return resultOLfn
      userOutputLFNs = resultOLfn['Value']

    self.log.verbose('Calling getCandidateFiles( %s, %s, %s)' % (outputList, userOutputLFNs, self.outputDataFileMask))
    self.log.debug("IgnoreAppErrors? '%s' " % self.ignoreapperrors)
    resultCF = self.getCandidateFiles(outputList, userOutputLFNs, self.outputDataFileMask)
    if not resultCF['OK']:
      if not self.ignoreapperrors:
        self.log.error(resultCF['Message'])
        self.setApplicationStatus(resultCF['Message'])
        return S_OK()
    fileDict = resultCF['Value']

    resultFMD = self.getFileMetadata(fileDict)
    if not resultFMD['OK']:
      if not self.ignoreapperrors:
        self.log.error(resultFMD['Message'])
        self.setApplicationStatus(resultFMD['Message'])
        return S_OK()

    if not resultFMD['Value']:
      if not self.ignoreapperrors:
        self.log.info('No output data files were determined to be uploaded for this workflow')
        self.setApplicationStatus('No Output Data Files To Upload')
        return S_OK()

    fileMetadata = resultFMD['Value']

    #First get the local (or assigned) SE to try first for upload and others in random fashion
    resultSEL = getDestinationSEList('Tier1-USER', DIRAC.siteName(), outputmode='local')
    if not resultSEL['OK']:
      self.log.error('Could not resolve output data SE', resultSEL['Message'])
      self.setApplicationStatus('Failed To Resolve OutputSE')
      return resultSEL
    localSE = resultSEL['Value']

    orderedSEs = [ se for se in self.defaultOutputSE if se not in localSE and se not in self.userOutputSE]

    orderedSEs = localSE + List.randomize(orderedSEs)
    if self.userOutputSE:
      prependSEs = []
      for userSE in self.userOutputSE:
        if not userSE in orderedSEs:
          prependSEs.append(userSE)
      orderedSEs = prependSEs + orderedSEs

    self.log.info('Ordered list of output SEs is: %s' % (', '.join(orderedSEs)))
    final = {}
    for fileName, metadata in fileMetadata.iteritems():
      final[fileName] = metadata
      final[fileName]['resolvedSE'] = orderedSEs

    #At this point can exit and see exactly what the module will upload
    self.printOutputInfo(final)
    if not self.enable:
      return S_OK('Module is disabled by control flag')

    #Instantiate the failover transfer client with the global request object
    failoverTransfer = FailoverTransfer(self._getRequestContainer())

    #One by one upload the files with failover if necessary
    filesToReplicate = {}
    filesToFailover = {}
    filesUploaded = []
    if not self.failoverTest:
      self.transferAndRegisterFiles(final, failoverTransfer, filesToFailover, filesUploaded, filesToReplicate)
    else:
      filesToFailover = final

    ##if there are files to be failovered, we do it now
    resultTRFF = self.transferRegisterAndFailoverFiles(failoverTransfer, filesToFailover, filesUploaded)
    cleanUp = resultTRFF['Value']['cleanUp']

    #For files correctly uploaded must report LFNs to job parameters
    if filesUploaded:
      report = ', '.join( filesUploaded )
      self.jobReport.setJobParameter( 'UploadedOutputData', report )

    self.workflow_commons['Request'] = failoverTransfer.request

    #If some or all of the files failed to be saved to failover
    if cleanUp:
      #Leave any uploaded files just in case it is useful for the user
      #do not try to replicate any files.
      return S_ERROR('Failed To Upload Output Data')

    #If there is now at least one replica for uploaded files can trigger replication
    datMan = DataManager( catalogs = self.userFileCatalog )
    self.log.info('Sleeping for 10 seconds before attempting replication of recently uploaded files')
    time.sleep(10)
    for lfn, repSE in filesToReplicate.items():
      resultRAR = datMan.replicateAndRegister(lfn, repSE)
      if not resultRAR['OK']:
        self.log.info('Replication failed with below error but file already exists in Grid storage with \
        at least one replica:\n%s' % (resultRAR))

    self.generateFailoverFile()

    self.setApplicationStatus('Job Finished Successfully')
    return S_OK('Output data uploaded')

  #############################################################################
  @staticmethod
  def getCurrentOwner():
    """Simple function to return current DIRAC username.
    """
    result = getProxyInfo()
    if not result['OK']:
      return S_ERROR('Could not obtain proxy information')

    if not 'username' in result['Value']:
      return S_ERROR('Could not get username from proxy')

    username = result['Value']['username']
    return S_OK(username)
  #############################################################################
  @staticmethod
  def getCurrentVO():
    """Simple function to return current DIRAC username.
    """
    result = getProxyInfo()
    if not result['OK']:
      return S_ERROR('Could not obtain proxy information')

    if not 'group' in result['Value']:
      return S_ERROR('Could not get group from proxy')

    group = result['Value']['group']
    vo = group.split("_")[0]
    return S_OK(vo)


  def constructOutputLFNs(self):
    """Returns a list of the outputDataLFNs"""
    self.log.info('Constructing user output LFN(s) for %s' % (', '.join(self.userOutputData)))
    if not self.jobID:
      self.jobID = 12345
    owner = self.workflow_commons.get('Owner', '')
    if not owner:
      res = self.getCurrentOwner()
      if not res['OK']:
        self.log.error('Could not find proxy')
        return S_ERROR('Could not obtain owner from proxy')
      owner = res['Value']

    vo = self.workflow_commons.get('VO', '')
    if not vo:
      res = self.getCurrentVO()
      if not res['OK']:
        self.log.error('Failed finding the VO')
        return S_ERROR('Could not obtain VO from proxy')
      vo = res['Value']

    result = constructUserLFNs(int(self.jobID), vo, owner, self.userOutputData, self.userOutputPath)
    if not result['OK']:
      self.log.error('Could not create user LFNs', result['Message'])
      return result

    return result


  def isLastStep(self):
    """returns S_OK() if this is the last step"""
    currentStep = int(self.step_commons['STEP_NUMBER'])
    totalSteps = int(self.workflow_commons['TotalSteps'])
    if currentStep == totalSteps:
      self.log.verbose("This is the last step, let's finalize this userjob")
      return S_OK()
    else:
      self.log.verbose('Current step = %s, total steps of workflow = %s, UserJobFinalization will enable itself only \
      at the last workflow step.' % (currentStep, totalSteps))
      return S_ERROR("Not the last step")

  def getOutputList(self):
    """returns list of dictionary with output files, paths and SEs
    userOutputData is list of files specified by user

    Q: Why is this taking the last part of the filename as outputPath????
    A: outputPath is not actually used anywhere
    """
    outputList = []
    for filename in self.userOutputData:
      outputList.append({'outputPath' : filename.split('.')[-1].upper(),
                         'outputDataSE' : self.userOutputSE,
                         'outputFile' : os.path.basename(filename)})
    self.log.debug("OutputList: %s" % outputList)
    return outputList

  def printOutputInfo(self, final):
    """print some information about what would be uploaded"""
    if not self.enable:
      self.log.info('Module is disabled by control flag, would have attempted to upload the following files')
    else:
      self.log.info('Attempt to upload the following files')
    for fileName, metadata in final.items():
      self.log.info('--------%s--------' % fileName)
      for metaName, metaValue in metadata.iteritems():
        self.log.info('%s = %s' %(metaName, metaValue))

  def transferAndRegisterFiles(self, final, failoverTransfer, filesToFailover, filesUploaded, filesToReplicate):
    """transfer and register files to storage elements

    fills filesToFailover, filesUploaded and filesToReplicate dicts
    """

    for fileName, metadata in final.items():
      self.log.info("Attempting to store file %s to the following SE(s):\n%s" % (fileName,
                                                                                 ', '.join(metadata['resolvedSE'])))
      resultFT = failoverTransfer.transferAndRegisterFile(fileName,
                                                          metadata['localpath'],
                                                          metadata['lfn'],
                                                          metadata['resolvedSE'],
                                                          fileMetaDict = metadata['filedict'],
                                                          fileCatalog = self.userFileCatalog)
      if not resultFT['OK']:
        self.log.error('Could not transfer and register %s with metadata:\n %s' % (fileName, metadata))
        filesToFailover[fileName] = metadata
      else:
        #Only attempt replication after successful upload
        lfn = metadata['lfn']
        filesUploaded.append(lfn)
        seList = metadata['resolvedSE']
        replicateSE = ''
        uploadedSE = resultFT['Value'].get('uploadedSE', '')
        if uploadedSE:
          for se in seList:
            if not se == uploadedSE:
              replicateSE = se
              break

        if replicateSE and lfn:
          self.log.info('Will attempt to replicate %s to %s' % (lfn, replicateSE))
          filesToReplicate[lfn] = replicateSE


  def transferRegisterAndFailoverFiles(self, failoverTransfer, filesToFailover, filesUploaded):
    """transfer and failover request"""
    cleanUp = False
    for fileName, metadata in filesToFailover.items():
      random.shuffle(self.failoverSEs)
      targetSE = metadata['resolvedSE'][0]

      ##make sure we don't upload to one SE and then try to move it there again
      failoverSEs = list(self.failoverSEs)
      if targetSE in failoverSEs:
        failoverSEs.remove(targetSE)
        if not failoverSEs:
          self.log.error("No more failoverSEs to consider, skipping file: %s" % fileName)
          self.log.error("TargetSE: %s, All FailoverSEs: %s" %( targetSE, self.failoverSEs))
          cleanUp = True
          continue
      self.log.verbose("TargetSE: %s, All FailoverSEs: %s, Cleaned FailoverSEs: %s"
                       % ( targetSE, self.failoverSEs, failoverSEs))

      metadata['resolvedSE'] = failoverSEs
      resultFT = failoverTransfer.transferAndRegisterFileFailover(fileName,
                                                                  metadata['localpath'],
                                                                  metadata['lfn'],
                                                                  targetSE,
                                                                  failoverSEs,
                                                                  fileMetaDict = metadata['filedict'],
                                                                  fileCatalog = self.userFileCatalog)
      if not resultFT['OK']:
        self.log.error('Could not transfer and register %s with metadata:\n %s' % (fileName, metadata))
        cleanUp = True
        continue #for users can continue even if one completely fails
      else:
        lfn = metadata['lfn']
        filesUploaded.append(lfn)

    return S_OK(dict(cleanUp=cleanUp))
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
