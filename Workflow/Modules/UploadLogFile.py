########################################################################
# $HeadURL$
########################################################################
""" 
UploadLogFile module is used to upload the files present in the working
directory.

@author: S. Poss
@since: Sep 01, 2010
"""

__RCSID__ = "$Id$"

from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.DataManagementSystem.Client.ReplicaManager      import ReplicaManager
from DIRAC.DataManagementSystem.Client.FailoverTransfer    import FailoverTransfer
from DIRAC.Core.Utilities.Subprocess                       import shellCall

from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from ILCDIRAC.Core.Utilities.ProductionData               import getLogPath

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
import DIRAC

import os, shutil, glob, random, tarfile

class UploadLogFile(ModuleBase):
  """ Handle log file uploads in the production jobs
  """
  #############################################################################
  def __init__(self):
    """Module initialization.
    """
    super(UploadLogFile, self).__init__()
    self.version = __RCSID__
    self.log = gLogger.getSubLogger('UploadLogFile')
    self.productionID = None
    self.jobID = None
    self.workflow_commons = None
    self.request = None
    self.logFilePath = ""
    self.logLFNPath = ""
    self.logdir = ''
    self.logSE = self.ops.getValue('/LogStorage/LogSE', 'LogSE')
    self.root = gConfig.getValue('/LocalSite/Root', os.getcwd())
    self.logSizeLimit = self.ops.getValue('/LogFiles/SizeLimit', 20 * 1024 * 1024)
    self.logExtensions = []
    self.failoverSEs = gConfig.getValue('/Resources/StorageElementGroups/Tier1-Failover', [])    
    self.diracLogo = self.ops.getValue('/SAM/LogoURL', 
                                      'https://lhcbweb.pic.es/DIRAC/images/logos/DIRAC-logo-transp.png')
    self.rm = ReplicaManager()

    self.experiment = 'CLIC'
    self.enable = True
    self.failoverTest = False #flag to put log files to failover by default
    self.jobID = ''

######################################################################
  def applicationSpecificInputs(self):

    if self.step_commons.has_key('Enable'):
      self.enable = self.step_commons['Enable']
      if not type(self.enable) == type(True):
        self.log.warn('Enable flag set to non-boolean value %s, setting to False' % self.enable)
        self.enable = False

    if self.step_commons.has_key('TestFailover'):
      self.enable = self.step_commons['TestFailover']
      if not type(self.failoverTest) == type(True):
        self.log.warn('Test failover flag set to non-boolean value %s, setting to False' % self.failoverTest)
        self.failoverTest = False

    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']
      self.log.verbose('Found WMS JobID = %s' % self.jobID)
    else:
      self.log.info('No WMS JobID found, disabling module via control flag')
      self.enable = False

    if self.workflow_commons.has_key('LogFilePath') and self.workflow_commons.has_key('LogTargetPath'):
      self.logFilePath = self.workflow_commons['LogFilePath']
      self.logLFNPath = self.workflow_commons['LogTargetPath']
    else:
      self.log.info('LogFilePath parameter not found, creating on the fly')
      result = getLogPath(self.workflow_commons)
      if not result['OK']:
        self.log.error('Could not create LogFilePath', result['Message'])
        return result
      self.logFilePath = result['Value']['LogFilePath'][0]
      self.logLFNPath = result['Value']['LogTargetPath'][0]

    if not type(self.logFilePath) == type(' '):
      self.logFilePath = self.logFilePath[0]
    if not type(self.logLFNPath) == type(' '):
      self.logLFNPath = self.logLFNPath[0]
      
    example_file = self.logFilePath
    if "/ilc/prod/clic" in example_file:
      self.experiment = "CLIC"
    elif "/ilc/prod/ilc/sid" in example_file:
      self.experiment = 'ILC_SID'
    elif "/ilc/prod/ilc/mc-dbd" in example_file:
      self.experiment = 'ILC_ILD' 
    else:
      self.log.warn("Failed to determine experiment, reverting to default: %s" % self.experiment)

    self.request = self.workflow_commons.get('Request', None)
    if not self.request:
      self.request = RequestContainer()
      self.request.RequestName = 'job_%d_request.xml' % int(self.jobID)
      self.request.JobID = self.jobID
      self.request.SourceComponent = "Job_%d" % int(self.jobID)

    return S_OK('Parameters resolved')

######################################################################
  def execute(self):
    """ Main execution method
    """
    self.log.info('Initializing %s' % self.version)
    # Add global reporting tool
    res = self.resolveInputVariables()
    if not res['OK']:
      self.log.error("Failed to resolve input parameters:", res['Message'])
      
    res = shellCall(0,'ls -al')
    if res['OK'] and res['Value'][0] == 0:
      self.log.info('The contents of the working directory...')
      self.log.info(str(res['Value'][1]))
    else:
      self.log.error('Failed to list the log directory', str(res['Value'][2]))

    self.log.info('Job root is found to be %s' % (self.root))
    self.log.info('PRODUCTION_ID = %s, JOB_ID = %s '  % (self.productionID, self.jobID))
    self.logdir = os.path.realpath('./job/log/%s/%s' % (self.productionID, self.jobID))
    self.log.info('Selected log files will be temporarily stored in %s' % self.logdir)

    res = self.finalize()
    self.workflow_commons['Request'] = self.request
    return res

  #############################################################################
  def finalize(self):
    """ finalize method performs final operations after all the job
        steps were executed. Only production jobs are treated.
    """
    
    self.log.verbose('Starting UploadLogFile finalize')
    ##########################################
    # First determine the files which should be saved
    self.log.info('Determining the files to be saved in the logs.')
    res = self.determineRelevantFiles()
    if not res['OK']:
      self.log.error('Completely failed to select relevant log files.', res['Message'])
      return S_OK()#because if the logs are lost, it's not the end of the world.
    selectedFiles = res['Value']
    self.log.info('The following %s files were selected to be saved:\n%s' % (len(selectedFiles), 
                                                                             "\n".join(selectedFiles)))

    #########################################
    # Create a temporary directory containing these files
    self.log.info('Populating a temporary directory for selected files.')
    res = self.populateLogDirectory(selectedFiles)
    if not res['OK']:
      self.log.error('Completely failed to populate temporary log file directory.', res['Message'])
      self.setApplicationStatus('Failed To Populate Log Dir')
      return S_OK()#because if the logs are lost, it's not the end of the world.
    self.log.info('%s populated with log files.' % self.logdir)

    #########################################
    # Create a tailored index page
    #self.log.info('Creating an index page for the logs')
    #result = self.__createLogIndex(selectedFiles)
    #if not result['OK']:
    #  self.log.error('Failed to create index page for logs', res['Message'])

    if not self.enable:
      self.log.info('Module is disabled by control flag')
      return S_OK('Module is disabled by control flag')

    #########################################
    #Make sure all the files in the log directory have the correct permissions
    result = self.__setLogFilePermissions(self.logdir)
    if not result['OK']:
      self.log.error('Could not set permissions of log files to 0755 with message:\n%s' % (result['Message']))


    #########################################
    # Attempt to uplaod logs to the LogSE
    self.log.info('Transferring log files to the %s' % self.logSE)
    res = S_ERROR()
    if not self.failoverTest:
      self.log.info('PutDirectory %s %s %s' % (self.logFilePath, os.path.realpath(self.logdir), self.logSE))
      res = self.rm.putStorageDirectory({ self.logFilePath : os.path.realpath(self.logdir) }, 
                                        self.logSE, singleDirectory = True)
      self.log.verbose(res)
      if res['OK']:
        self.log.info('Successfully upload log directory to %s' % self.logSE)
        # TODO: The logURL should be constructed using the LogSE and StorageElement()
        #storageElement = StorageElement(self.logSE)
        #pfn = storageElement.getPfnForLfn(self.logFilePath)['Value']
        #logURL = getPfnForProtocol(res['Value'],'http')['Value']
        logURL = '%s' % self.logFilePath
        self.setJobParameter('Log LFN', logURL)
        self.log.info('Logs for this job may be retrieved with dirac-ilc-get-prod-log -F %s' % logURL)
        return S_OK()

    #########################################
    # Recover the logs to a failover storage element
    self.log.error('Completely failed to upload log files to %s, will attempt upload to failover SE' % self.logSE, 
                   res['Message'])

    tarFileDir = os.path.dirname(self.logdir)
    self.logLFNPath = '%s.gz' % self.logLFNPath
    tarFileName = os.path.basename(self.logLFNPath)
    start = os.getcwd()
    os.chdir(self.logdir)
    logTarFiles = os.listdir(self.logdir)
    #comm = 'tar czvf %s %s' % (tarFileName,string.join(logTarFiles,' '))
    tfile = tarfile.open(tarFileName, "w:gz")
    for item in logTarFiles:
      tfile.add(item)
    tfile.close()
    #res = shellCall(0,comm)
    if not os.path.exists(tarFileName):
      res = S_ERROR("File was not created")
    os.chdir(start)
    if not res['OK']:
      self.log.error('Failed to create tar file from directory','%s %s' % (self.logdir, res['Message']))
      self.setApplicationStatus('Failed To Create Log Tar Dir')
      return S_OK()#because if the logs are lost, it's not the end of the world.
    
    #if res['Value'][0]: #i.e. non-zero status
    #  self.log.error('Failed to create tar file from directory','%s %s' % (self.logdir,res['Value']))
    #  self.setApplicationStatus('Failed To Create Log Tar Dir')
    #  return S_OK()#because if the logs are lost, it's not the end of the world.

    ############################################################
    #Instantiate the failover transfer client with the global request object
    failoverTransfer = FailoverTransfer(self.request)
    ##determine the experiment
    self.failoverSEs = self.ops.getValue("Production/%s/FailOverSE" % self.experiment, self.failoverSEs)

    random.shuffle(self.failoverSEs)
    self.log.info("Attempting to store file %s to the following SE(s):\n%s" % (tarFileName, 
                                                                               ', '.join(self.failoverSEs )))
    result = failoverTransfer.transferAndRegisterFile(tarFileName, '%s/%s' % (tarFileDir, tarFileName), self.logLFNPath, 
                                                      self.failoverSEs, fileMetaDict = { "GUID": None },
                                                      fileCatalog = ['FileCatalog', 'LcgFileCatalog'])
    if not result['OK']:
      self.log.error('Failed to upload logs to all destinations')
      self.setApplicationStatus('Failed To Upload Logs')
      return S_OK() #because if the logs are lost, it's not the end of the world.
    
    #Now after all operations, retrieve potentially modified request object
    self.request = failoverTransfer.request
    res = self.createLogUploadRequest(self.logSE, self.logLFNPath)
    if not res['OK']:
      self.log.error('Failed to create failover request', res['Message'])
      self.setApplicationStatus('Failed To Upload Logs To Failover')
    else:
      self.log.info('Successfully created failover request')
      
    self.workflow_commons['Request'] = self.request    
    return S_OK()

  #############################################################################
  def determineRelevantFiles(self):
    """ The files which are below a configurable size will be stored in the logs.
        This will typically pick up everything in the working directory minus the output data files.
    """
    logFileExtensions = ['*.txt', '*.log', '*.out', '*.output', '*.xml', '*.sh', '*.info', '*.err','*.root']
    self.logExtensions = self.ops.getValue('/LogFiles/%s/Extensions' % self.experiment, [])

    if self.logExtensions:
      self.log.info('Using list of log extensions from CS:\n%s' % (', '.join(self.logExtensions)))
      logFileExtensions = self.logExtensions
    else:
      self.log.info('Using default list of log extensions:\n%s' % (', '.join(logFileExtensions)))

    candidateFiles = []
    for ext in logFileExtensions:
      self.log.debug('Looking at log file wildcard: %s' % ext)
      globList = glob.glob(ext)
      for check in globList:
        if os.path.isfile(check):
          self.log.debug('Found locally existing log file: %s' % check)
          candidateFiles.append(check)

    selectedFiles = []
    try:
      for candidate in candidateFiles:
        fileSize = os.stat(candidate)[6]
        if fileSize < self.logSizeLimit:
          selectedFiles.append(candidate)
        else:
          self.log.error('Log file found to be greater than maximum of %s bytes' % self.logSizeLimit, candidate)
      return S_OK(selectedFiles)
    except Exception, x:
      self.log.exception('Exception while determining files to save.', '', str(x))
      return S_ERROR('Could not determine log files')

  #############################################################################
  def populateLogDirectory(self, selectedFiles):
    """ A temporary directory is created for all the selected files.
        These files are then copied into this directory before being uploaded
    """
    # Create the temporary directory
    try:
      if not os.path.exists(self.logdir):
        os.makedirs(self.logdir)
    except Exception, x:
      self.log.exception('Exception while trying to create directory.', self.logdir, str(x))
      return S_ERROR()
    # Set proper permissions
    self.log.info('Changing log directory permissions to 0755')
    try:
      os.chmod(self.logdir, 0755)
    except Exception, x:
      self.log.error('Could not set logdir permissions to 0755:', '%s (%s)' % ( self.logdir, str(x) ) )
    # Populate the temporary directory
    try:
      for myfile in selectedFiles:
        destinationFile = '%s/%s' % (self.logdir, os.path.basename(myfile))
        shutil.copy(myfile, destinationFile)
    except Exception, x:
      self.log.exception('Exception while trying to copy file.', myfile, str(x))
      self.log.info('File %s will be skipped and can be considered lost.' % myfile)

    # Now verify the contents of our target log dir
    successfulFiles = os.listdir(self.logdir)
    if len(successfulFiles) == 0:
      self.log.info('Failed to copy any files to the target directory.')
      return S_ERROR()
    else:
      self.log.info('Prepared %s files in the temporary directory.' % self.logdir)
      return S_OK()
    
  #############################################################################
  def createLogUploadRequest(self, targetSE, logFileLFN):
    """ Set a request to upload job log files from the output sandbox
    """
    self.log.info('Setting log upload request for %s at %s' %(targetSE, logFileLFN))
    #FIXME: addSubRequest is gone
    res = self.request.addSubRequest({'Attributes':{'Operation':'uploadLogFiles',
                                                    'TargetSE':targetSE,
                                                    'ExecutionOrder':0}},
                                     'logupload')
    if not res['OK']:
      return res
    index = res['Value']
    fileDict = {}
    fileDict['Status'] = 'Waiting'
    fileDict['LFN'] = logFileLFN
    #FIXME: setSubRequestFiles is gone too
    self.request.setSubRequestFiles(index, 'logupload', [fileDict])
    return S_OK()

  #############################################################################
  def __setLogFilePermissions(self, logDir):
    """ Sets the permissions of all the files in the log directory to ensure
        they are readable.
    """
    try:
      for toChange in os.listdir(logDir):
        if not os.path.islink('%s/%s' % (logDir, toChange)):
          self.log.debug('Changing permissions of %s/%s to 0755' % (logDir, toChange))
          os.chmod('%s/%s' % (logDir, toChange), 0755)
    except Exception, x:
      self.log.error('Problem changing shared area permissions', str(x))
      return S_ERROR(x)

    return S_OK()

  #############################################################################
  def __createLogIndex(self, selectedFiles):
    """ Create a log index page for browsing the log files.
    """
    productionID = self.productionID
    prodJobID = self.jobID
    wmsJobID = self.jobID
    logFilePath = self.logFilePath

    targetFile = '%s/index.html' % (self.logdir)
    fopen = open(targetFile, 'w')
    fopen.write( """
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<head>\n""")
    fopen.write("<title>Logs for Job %s of Production %s (DIRAC WMS ID %s)</title>\n" % (prodJobID, productionID, 
                                                                                         wmsJobID))
    fopen.write( """</head>
<body text="#000000" bgcolor="#33ffff" link="#000099" vlink="#990099"
 alink="#000099"> \n
""")
    fopen.write("""<IMG SRC="%s" ALT="DIRAC" WIDTH="300" HEIGHT="120" ALIGN="right" BORDER="0">
<br>
""" %self.diracLogo)
    fopen.write("<h3>Log files for  Job %s_%s </h3> \n<br>"  % (productionID, prodJobID))
    for fileName in selectedFiles:
      fopen.write('<a href="%s">%s</a><br> \n' % (fileName, fileName))

    fopen.write("<p>Job %s_%s corresponds to WMS JobID %s executed at %s.</p><br>" % (productionID, prodJobID, wmsJobID,
                                                                                      DIRAC.siteName()))
    fopen.write("<h3>Parameter summary for job %s_%s</h3> \n"  %(prodJobID, productionID))
    check = ['SystemConfig', 'SoftwarePackages', 'BannedSites', 'LogLevel',
             'JobType', 'MaxCPUTime', 'ProductionOutputData', 'LogFilePath', 'InputData', 'InputSandbox']
    params = {}
    for n, v in self.workflow_commons.items():
      for item in check:
        if n == item and v:
          params[n] = str(v)

    finalKeys = params.keys()
    finalKeys.sort()
    rows = ''
    for k in finalKeys:
      rows += """

<tr>
<td> %s </td>
<td> %s </td>
</tr>
      """ % (k, params[k])

    table = """<table border="1" bordercolor="#000000" width="50%" bgcolor="#BCCDFE">
<tr>
<td>Parameter Name</td>
<td>Parameter Value</td>
</tr>"""+rows+"""
</table>
"""
    fopen.write(table)
    fopen.write("""</body>
</html>""" )
    fopen.close()
    return S_OK()
  
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
