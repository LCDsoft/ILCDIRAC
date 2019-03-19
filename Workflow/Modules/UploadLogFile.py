""" 
UploadLogFile module is used to upload the files present in the working
directory.

:author: S. Poss
:since: Sep 01, 2010
"""
import os
import shutil
import glob
import random
import tarfile

from DIRAC.DataManagementSystem.Client.FailoverTransfer    import FailoverTransfer
from DIRAC.RequestManagementSystem.Client.Operation        import Operation
from DIRAC.RequestManagementSystem.Client.File             import File
from DIRAC.Resources.Storage.StorageElement import StorageElementItem as StorageElement
from DIRAC import S_OK, S_ERROR, gLogger, gConfig

from ILCDIRAC.Workflow.Modules.ModuleBase                  import ModuleBase
from ILCDIRAC.Core.Utilities.ProductionData import getLogPath, getExperimentFromPath

__RCSID__ = '$Id$'
LOG = gLogger.getSubLogger(__name__)


class UploadLogFile(ModuleBase):
  """ Handle log file uploads in the production jobs
  """
  #############################################################################
  def __init__(self):
    """Module initialization.
    """
    super(UploadLogFile, self).__init__()
    self.version = __RCSID__
    self.productionID = None
    self.jobID = None
    self.workflow_commons = None
    self.logFilePath = ""
    self.logLFNPath = ""
    self.logdir = ''
    self.logSE = StorageElement( self.ops.getValue('/LogStorage/LogSE', 'LogSE') )
    self.root = gConfig.getValue('/LocalSite/Root', os.getcwd())
    self.logSizeLimit = self.ops.getValue('/LogFiles/SizeLimit', 20 * 1024 * 1024)
    self.logExtensions = []
    self.failoverSEs = gConfig.getValue('/Resources/StorageElementGroups/Tier1-Failover', [])    
    self.experiment = 'CLIC'
    self.enable = True
    self.failoverTest = False #flag to put log files to failover by default
    self.jobID = ''

######################################################################
  def applicationSpecificInputs(self):
    """resolves the module parameters"""

    self.enable = self.step_commons.get('Enable', self.enable)
    if not isinstance(self.enable, bool):
      LOG.warn('Enable flag set to non-boolean value %s, setting to False' % self.enable)
      self.enable = False

    if not isinstance(self.failoverTest, bool):
      LOG.warn('Test failover flag set to non-boolean value %s, setting to False' % self.failoverTest)
      self.failoverTest = False

    self.jobID = os.environ.get('JOBID', self.jobID)
    if self.jobID != 0:
      LOG.verbose('Found WMS JobID = %s' % self.jobID)
    else:
      LOG.info('No WMS JobID found, disabling module via control flag')
      self.enable = False

    self.logFilePath = self.workflow_commons.get('LogFilePath', self.logFilePath)
    self.logLFNPath = self.workflow_commons.get('LogTargetPath', self.logLFNPath)
    if not (self.logFilePath and self.logLFNPath):
      LOG.info('LogFilePath parameter not found, creating on the fly')
      result = getLogPath(self.workflow_commons)
      if not result['OK']:
        LOG.error('Could not create LogFilePath', result['Message'])
        return result
      self.logFilePath = result['Value']['LogFilePath'][0]
      self.logLFNPath = result['Value']['LogTargetPath'][0]

    if not isinstance(self.logFilePath, basestring):
      self.logFilePath = self.logFilePath[0]
    if not isinstance(self.logLFNPath, basestring):
      self.logLFNPath = self.logLFNPath[0]

    self.experiment = getExperimentFromPath(LOG, self.logFilePath, self.experiment)

    return S_OK('Parameters resolved')

######################################################################
  def execute(self):
    """ Main execution method
    """
    LOG.info('Initializing %s' % self.version)
    # Add global reporting tool
    res = self.resolveInputVariables()
    if not res['OK']:
      LOG.error("Failed to resolve input parameters:", res['Message'])
      
    self.logWorkingDirectory()
    LOG.info('Job root is found to be %s' % (self.root))
    LOG.info('PRODUCTION_ID = %s, JOB_ID = %s ' % (self.productionID, self.jobID))
    self.logdir = os.path.realpath('./job/log/%s/%s' % (self.productionID, self.jobID))
    LOG.info('Selected log files will be temporarily stored in %s' % self.logdir)

    res = self.finalize()
    return res

  #############################################################################
  def finalize(self):
    """ finalize method performs final operations after all the job
        steps were executed. Only production jobs are treated.
    """
    
    LOG.verbose('Starting UploadLogFile finalize')
    LOG.debug("LogFilePath: %s" % self.logFilePath)
    LOG.debug("LogLFNPath:  %s" % self.logLFNPath)

    ##########################################
    # First determine the files which should be saved
    LOG.info('Determining the files to be saved in the logs.')
    resRelevantFiles = self._determineRelevantFiles()
    if not resRelevantFiles['OK']:
      LOG.error('Completely failed to select relevant log files.', resRelevantFiles['Message'])
      return S_OK()#because if the logs are lost, it's not the end of the world.

    selectedFiles = resRelevantFiles['Value']
    if not selectedFiles:
      LOG.info("No log files selected")
      return S_OK()

    LOG.info('The following %s files were selected to be saved:\n%s' % (len(selectedFiles),
                                                                             "\n".join(selectedFiles)))

    #########################################
    # Create a temporary directory containing these files
    LOG.info('Populating a temporary directory for selected files.')
    resLogDir = self._populateLogDirectory(selectedFiles)
    if not resLogDir['OK']:
      LOG.error('Completely failed to populate temporary log file directory.', resLogDir['Message'])
      self.setApplicationStatus('Failed To Populate Log Dir')
      return S_OK()#because if the logs are lost, it's not the end of the world.
    LOG.info('%s populated with log files.' % self.logdir)

    if not self.enable:
      LOG.info('Module is disabled by control flag')
      return S_OK('Module is disabled by control flag')

    #########################################
    #Make sure all the files in the log directory have the correct permissions
    result = self._setLogFilePermissions(self.logdir)
    if not result['OK']:
      LOG.error('Could not set permissions of log files to 0755 with message:\n%s' % (result['Message']))

    #########################################
    #Tar all the files
    resTarLogs = self._tarTheLogFiles()
    if not resTarLogs['OK']:
      return S_OK()#because if the logs are lost, it's not the end of the world.
    tarFileName = resTarLogs['Value']['fileName']

    #########################################
    # Attempt to upload log tar ball to the LogSE
    LOG.info('Transferring log tarball to the %s' % self.logSE.name)
    resTransfer = S_ERROR("Skipping log upload, because failoverTest")
    tarFileLFN = '%s/%s' % (self.logFilePath, tarFileName)
    tarFileLocal = os.path.realpath(os.path.join(self.logdir, tarFileName))
    if not self.failoverTest:
      LOG.info('PutFile %s %s %s' % (tarFileLFN, tarFileLocal, self.logSE.name))
      resTransfer = self.logSE.putFile({ tarFileLFN : tarFileLocal })
      LOG.debug("putFile result: %s" % resTransfer)
      if not resTransfer['OK']:
        LOG.error('Completely failed to upload log files to %s, will attempt upload to failover SE' % self.logSE.name,
                       resTransfer['Message'])
      elif resTransfer['OK'] and len(resTransfer['Value']['Failed']) == 0:
        LOG.info('Successfully upload log tarball to %s' % self.logSE.name)
        self.setJobParameter('Log LFN', tarFileLFN)
        LOG.info('Logs for this job may be retrieved with dirac-ilc-get-prod-log -F %s' % tarFileLFN)
        return S_OK()
      else:
        LOG.error('Completely failed to upload log files to %s, will attempt upload to failover SE' % self.logSE.name,
                       resTransfer['Value'])

    #########################################
    # Recover the logs to a failover storage element

    
    #if res['Value'][0]: #i.e. non-zero status
    #  LOG.error('Failed to create tar file from directory','%s %s' % (self.logdir,res['Value']))
    #  self.setApplicationStatus('Failed To Create Log Tar Dir')
    #  return S_OK()#because if the logs are lost, it's not the end of the world.

    ############################################################
    #Instantiate the failover transfer client with the global request object
    resFailover = self._tryFailoverTransfer(tarFileName, self.logdir)
    if not resFailover['Value']:
      return resFailover ##log files lost, but who cares...
    
    self.workflow_commons['Request'] = resFailover['Value']['Request']
    uploadedSE = resFailover['Value']['uploadedSE']
    res = self._createLogUploadRequest(self.logSE.name, self.logLFNPath, uploadedSE)
    if not res['OK']:
      LOG.error('Failed to create failover request', res['Message'])
      self.setApplicationStatus('Failed To Upload Logs To Failover')
    else:
      LOG.info('Successfully created failover request')
      LOG.info("Request %s" % self._getRequestContainer())
    return S_OK()

  #############################################################################
  def _determineRelevantFiles(self):
    """ The files which are below a configurable size will be stored in the logs.
        This will typically pick up everything in the working directory minus the output data files.
    """
    logFileExtensions = ['*.txt', '*.log', '*.out', '*.output', '*.xml', '*.sh', '*.info', '*.err','*.root']
    self.logExtensions = self.ops.getValue('/LogFiles/%s/Extensions' % self.experiment, [])

    if self.logExtensions:
      LOG.info('Using list of log extensions from CS:\n%s' % (', '.join(self.logExtensions)))
      logFileExtensions = self.logExtensions
    else:
      LOG.info('Using default list of log extensions:\n%s' % (', '.join(logFileExtensions)))

    candidateFiles = []
    for ext in logFileExtensions:
      LOG.debug('Looking at log file wildcard: %s' % ext)
      globList = glob.glob(ext)
      for check in globList:
        if os.path.isfile(check):
          LOG.debug('Found locally existing log file: %s' % check)
          candidateFiles.append(check)

    selectedFiles = []
    try:
      for candidate in candidateFiles:
        fileSize = os.stat(candidate)[6]
        if fileSize < self.logSizeLimit:
          selectedFiles.append(candidate)
        else:
          LOG.error('Log file found to be greater than maximum of %s bytes' % self.logSizeLimit, candidate)
      return S_OK(selectedFiles)
    except OSError as x:
      LOG.exception('Exception while determining files to save.', '', str(x))
      return S_ERROR('Could not determine log files')

  #############################################################################
  def _populateLogDirectory(self, selectedFiles):
    """ A temporary directory is created for all the selected files.
        These files are then copied into this directory before being uploaded
    """
    # Create the temporary directory
    try:
      if not os.path.exists(self.logdir):
        os.makedirs(self.logdir)
    except OSError as x:
      LOG.exception('PopulateLogDir: Exception while trying to create directory.', self.logdir, str(x))
      return S_ERROR()
    # Set proper permissions
    LOG.info('PopulateLogDir: Changing log directory %s permissions to 0755' % self.logdir)
    try:
      os.chmod(self.logdir, 0o755)
    except OSError as x:
      LOG.error('PopulateLogDir: Could not set logdir permissions to 0755:', '%s (%s)' % (self.logdir, str(x)))
    # Populate the temporary directory
    try:
      for myfile in selectedFiles:
        destinationFile = '%s/%s' % (self.logdir, os.path.basename(myfile))
        shutil.copy(myfile, destinationFile)
    except OSError as x:
      LOG.exception('PopulateLogDir: Exception while trying to copy file.', myfile, str(x))
      LOG.info('PopulateLogDir: File %s will be skipped and can be considered lost.' % myfile)

    # Now verify the contents of our target log dir
    successfulFiles = os.listdir(self.logdir)
    LOG.info("PopulateLogDir: successfulFiles %s" % successfulFiles)
    if len(successfulFiles) == 0:
      LOG.info('PopulateLogDir: Failed to copy any files to the target directory.')
      return S_ERROR()
    else:
      LOG.info('PopulateLogDir: Prepared %s files in the temporary directory.' % len(successfulFiles))
      return S_OK()
    
  #############################################################################
  def _createLogUploadRequest(self, targetSE, logFileLFN, uploadedSE):
    """ Set a request to upload job log files from the output sandbox
        Changed to be similar to LHCb createLogUploadRequest
        using LHCb LogUpload Request and Removal Request
    """
    LOG.info('Setting log upload request for %s at %s' % (targetSE, logFileLFN))
    request = self._getRequestContainer()

    logUpload = Operation()
    logUpload.Type = "LogUpload"
    logUpload.TargetSE = targetSE

    upFile = File()
    upFile.LFN = logFileLFN
    logUpload.addFile( upFile )

    logRemoval = Operation()
    logRemoval.Type = 'RemoveFile'
    logRemoval.TargetSE = uploadedSE
    logRemoval.addFile( upFile )

    request.addOperation ( logUpload )
    request.addOperation ( logRemoval )

    self.workflow_commons['Request'] = request

    return S_OK()

  #############################################################################
  def _setLogFilePermissions(self, logDir):
    """ Sets the permissions of all the files in the log directory to ensure
        they are readable.
    """
    try:
      for toChange in os.listdir(logDir):
        if not os.path.islink('%s/%s' % (logDir, toChange)):
          LOG.debug('Changing permissions of %s/%s to 0755' % (logDir, toChange))
          os.chmod('%s/%s' % (logDir, toChange), 0o755)
    except OSError as x:
      LOG.error('Problem changing shared area permissions', str(x))
      return S_ERROR(x)

    return S_OK()

  ################################################################################
  def _tryFailoverTransfer(self, tarFileName, tarFileDir):
    """tries to upload the log tarBall to the failoverSE and creates moving request"""
    failoverTransfer = FailoverTransfer(self._getRequestContainer())
    ##determine the experiment
    self.failoverSEs = self.ops.getValue("Production/%s/FailOverSE" % self.experiment, self.failoverSEs)
    catalogs = self.ops.getValue('Production/%s/Catalogs' % self.experiment, ['FileCatalog', 'LcgFileCatalog'])

    random.shuffle(self.failoverSEs)
    LOG.info("Attempting to store file %s to the following SE(s):\n%s" % (tarFileName,
                                                                               ', '.join(self.failoverSEs )))
    result = failoverTransfer.transferAndRegisterFile(tarFileName, '%s/%s' % (tarFileDir, tarFileName), self.logLFNPath,
                                                      self.failoverSEs, fileMetaDict = { "GUID": None },
                                                      fileCatalog = catalogs )
    if not result['OK']:
      LOG.error('Failed to upload logs to all destinations')
      self.setApplicationStatus('Failed To Upload Logs')
      return S_OK() #because if the logs are lost, it's not the end of the world.

    #Now after all operations, return potentially modified request object
    return S_OK( {'Request': failoverTransfer.request, 'uploadedSE': result['Value']['uploadedSE']})

  def _tarTheLogFiles(self):
    """returns S_OK/S_ERROR and puts all the relevantFiles into a tarball"""
    self.logLFNPath = '%s.gz' % self.logLFNPath
    tarFileName = os.path.basename(self.logLFNPath)

    LOG.debug("Log Directory: %s" % self.logdir)
    LOG.debug("Log LFNPath:   %s" % self.logLFNPath)
    LOG.debug("TarFileName:   %s" % tarFileName)
    start = os.getcwd()
    os.chdir(self.logdir)
    logTarFiles = os.listdir(self.logdir)
    #comm = 'tar czvf %s %s' % (tarFileName,string.join(logTarFiles,' '))
    tfile = tarfile.open(tarFileName, "w:gz")
    for item in logTarFiles:
      LOG.info("Adding file %s to tarfile %s" % (item, tarFileName))
      tfile.add(item)
    tfile.close()
    resExists = S_OK() if os.path.exists(tarFileName) else S_ERROR("File was not created")
    os.chdir(start)
    LOG.debug("TarFileName: %s" % (tarFileName,))
    if resExists['OK']:
      return S_OK(dict(fileName=tarFileName))
    else:
      LOG.error('Failed to create tar file from directory', '%s %s' % (self.logdir, resExists['Message']))
      self.setApplicationStatus('Failed To Create Log Tar Dir')
      return S_ERROR()

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
