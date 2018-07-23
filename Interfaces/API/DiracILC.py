"""
API to use to submit jobs in the ILC VO

:since:  Apr 13, 2010
:author: Stephane Poss
"""

import glob
import os
import shutil
import string
import sys
import tarfile
import tempfile
import urllib

from pprint import pformat

import DIRAC
from DIRAC.Core.Utilities.ModuleFactory import ModuleFactory
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.Interfaces.API.Dirac                     import Dirac
from DIRAC.DataManagementSystem.Client.DataManager  import DataManager
from DIRAC.ConfigurationSystem.Client.Helpers.Operations            import Operations
from DIRAC import gConfig, S_ERROR, S_OK, gLogger

from ILCDIRAC.Core.Utilities.ProcessList            import ProcessList

__RCSID__ = "$Id$"

#pylint: disable=protected-access

COMPONENT_NAME = 'DiracILC'
LOG = gLogger.getSubLogger(__name__)

class DiracILC(Dirac):
  """DiracILC is VO specific API Dirac
  
  Adding specific ILC functionalities to the Dirac class, and implement the :func:`preSubmissionChecks` method
  """
  def __init__(self, withRepo = False, repoLocation = ''):
    """Internal initialization of the ILCDIRAC API.
    """
    #self.dirac = Dirac(WithRepo=WithRepo, RepoLocation=RepoLocation)
    super(DiracILC, self).__init__(withRepo, repoLocation )
    #Dirac.__init__(self, withRepo = withRepo, repoLocation = repoLocation)
    self.software_versions = {}
    self.checked = False
    self.processList = None
    self.ops = Operations()
    
  def getProcessList(self): 
    """ Get the :mod:`ProcessList <ILCDIRAC.Core.Utilities.ProcessList.ProcessList>`
    needed by :mod:`Whizard <ILCDIRAC.Interfaces.API.NewInterface.Applications.Whizard>`.

    :return: process list object
    """
    processlistpath = gConfig.getValue("/LocalSite/ProcessListPath", "")
    if not processlistpath:
      LOG.info('Will download the process list locally. To gain time, please put it somewhere and add to \
      your dirac.cfg the entry /LocalSite/ProcessListPath pointing to the file')
      pathtofile = self.ops.getValue("/ProcessList/Location", "")
      if not pathtofile:
        LOG.error("Could not get path to process list")
        processlist = ""
      else:
        datMan = DataManager()
        datMan.getFile(pathtofile)
        processlist = os.path.basename(pathtofile)   
    else:
      processlist = processlistpath
    self.processList = ProcessList(processlist)
    return self.processList
    
  def preSubmissionChecks(self, job, mode = None):
    """Overridden method from :mod:`DIRAC.Interfaces.API.Dirac`
    
    Checks from CS that required software packages are available.
    
    :param job: job instance
    :type job: ~ILCDIRAC.Interfaces.API.NewInterface.Job.Job
    :param mode: submission mode, not used here.

    :return: :func:`~DIRAC.Core.Utilities.ReturnValues.S_OK` , :func:`~DIRAC.Core.Utilities.ReturnValues.S_ERROR`
    """
    
    if not job.oktosubmit:
      LOG.error('You should use job.submit(dirac)')
      return S_ERROR("You should use job.submit(dirac)")
    res = self._do_check(job)
    if not res['OK']:
      return res
    if not self.checked:
      res = job._askUser()
      if not res['OK']:
        return res
      self.checked = True
    return S_OK()
    
  def checkparams(self, job):
    """Helper method
    
    Method used for stand alone checks of job integrity. Calls the formulation error checking of the job
    
    Actually checks that all input are available and checks that the required software packages are available in the CS

    :param job: Job Instance
    :type job: ~ILCDIRAC.Interfaces.API.NewInterface.Job.Job
    :return: :func:`~DIRAC.Core.Utilities.ReturnValues.S_OK` , :func:`~DIRAC.Core.Utilities.ReturnValues.S_ERROR`
    """
    formulationErrors = job.errorDict

    if formulationErrors:
      for method, errorList in formulationErrors.items():
        LOG.error('>>>> Error in %s() <<<<\n%s' % (method, string.join(errorList, '\n')))
      return S_ERROR( formulationErrors )
    return self.preSubmissionChecks(job, mode = '')

  def giveProcessList(self):
    """ Returns the list of Processes
    """
    return self.processList
  
  def retrieveRepositoryOutputDataLFNs(self, requestedStates = None):
    """Helper function
    
    Get the list of uploaded output data for a set of jobs in a repository
    
    :param requestedStates: List of states requested for filtering the list
    :type requestedStates: list of strings
    :return: list
    """
    if requestedStates is None:
      requestedStates = ['Done']
    llist = []
    if not self.jobRepo:
      LOG.warn("No repository is initialised")
      return S_OK()
    jobs = self.jobRepo.readRepository()['Value']
    for jobID in sorted( jobs.keys() ):
      jobDict = jobs[jobID]
      if 'State' in jobDict and ( jobDict['State'] in requestedStates ):
        if ( 'UserOutputData' in jobDict and ( not int( jobDict['UserOutputData'] ) ) ) or \
           ( 'UserOutputData' not in jobDict ):
          params = self.parameters(int(jobID))
          if params['OK']:
            if 'UploadedOutputData' in params['Value']:
              lfn = params['Value']['UploadedOutputData']
              llist.append(lfn)
    return llist
  
  def _do_check(self, job):
    """ Main method for checks

    :param job: :mod:`job object <ILCDIRAC.Interfaces.API.NewInterface.Job.Job>`
    :return: :func:`~DIRAC.Core.Utilities.ReturnValues.S_OK` , :func:`~DIRAC.Core.Utilities.ReturnValues.S_ERROR`
    """
    #Start by taking care of sandbox
    if hasattr(job, "inputsandbox") and isinstance( job.inputsandbox, list ) and len( job.inputsandbox ):
      found_list = False
      for items in job.inputsandbox:
        if isinstance( items, list ):#We fix the SB in the case is contains a list of lists
          found_list = True
          for inBoxFile in items:
            if isinstance( inBoxFile, list ):
              return S_ERROR("Too many lists of lists in the input sandbox, please fix!")
            job.inputsandbox.append(inBoxFile)
          job.inputsandbox.remove(items)
      resolvedFiles = job._resolveInputSandbox( job.inputsandbox )
      if found_list:
        LOG.warn("Input Sandbox contains list of lists. Please avoid that.")
      fileList = string.join( resolvedFiles, ";" )
      description = 'Input sandbox file list'
      job._addParameter( job.workflow, 'InputSandbox', 'JDL', fileList, description )

    res = self.checkInputSandboxLFNs(job)
    if not res['OK']:
      return res
    
    platform = job.workflow.findParameter("Platform").getValue()
    apps = job.workflow.findParameter("SoftwarePackages")
    if apps:
      apps = apps.getValue()
      for appver in apps.split(";"):
        app = appver.split(".")[0].lower()#first element
        vers = appver.split(".")[1:]#all the others
        vers = string.join(vers,".")
        res = self._checkapp(platform, app, vers)
        if not res['OK']:
          return res
    outputpathparam = job.workflow.findParameter("UserOutputPath")
    if outputpathparam:
      outputpath = outputpathparam.getValue()
      res = self._checkoutputpath(outputpath)
      if not res['OK']:
        return res
    useroutputdata = job.workflow.findParameter("UserOutputData")
    useroutputsandbox = job.addToOutputSandbox
    if useroutputdata:
      res = self._checkdataconsistency(useroutputdata.getValue(), useroutputsandbox)
      if not res['OK']: 
        return res

    return S_OK()
  
  def _checkapp(self, platform, appName, appVersion):
    """ Check availability of application in CS

    :param string platform: System platform
    :param string appName: Application name
    :param string appVersion: Application version
    :return: :func:`~DIRAC.Core.Utilities.ReturnValues.S_OK` or :func:`~DIRAC.Core.Utilities.ReturnValues.S_ERROR`
    """
    csPathTarBall = "/AvailableTarBalls/%s/%s/%s/TarBall" %(platform, appName, appVersion)
    csPathCVMFS   ="/AvailableTarBalls/%s/%s/%s/CVMFSPath"%(platform, appName, appVersion)

    LOG.debug("Checking for software version in " + csPathTarBall)
    app_version = self.ops.getValue(csPathTarBall,'')

    LOG.debug("Checking for software version in " + csPathCVMFS)
    app_version_cvmfs = self.ops.getValue(csPathCVMFS,'')

    if not app_version and not app_version_cvmfs:
      LOG.error("Could not find the specified software %s_%s for %s, check in CS" % (appName, appVersion, platform))
      return S_ERROR("Could not find the specified software %s_%s for %s, check in CS" % (appName, appVersion, platform))
    return S_OK()
  
  def _checkoutputpath(self, path):
    """ Validate the outputpath specified for the application

    :param string path: Path of output data
    :return: :func:`~DIRAC.Core.Utilities.ReturnValues.S_OK` , :func:`~DIRAC.Core.Utilities.ReturnValues.S_ERROR`
    """
    if path.find("//") > -1 or path.find("/./") > -1 or path.find("/../") > -1:
      LOG.error("OutputPath of setOutputData() contains invalid characters, please remove any //, /./, or /../")
      return S_ERROR("Invalid path")
    path = path.rstrip()
    if path[-1] == "/":
      LOG.error("Please strip trailing / from outputPath in setOutputData()")
      return S_ERROR("Invalid path")
    return S_OK()
  
  def _checkdataconsistency(self, useroutputdata, useroutputsandbox):
    """ Make sure the files are either in OutpuSandbox or OutputData but not both

    :param list useroutputdata: List of files set in the outputdata
    :param list useroutputsandbox: List of files set in the output sandbox
    :return: :func:`~DIRAC.Core.Utilities.ReturnValues.S_OK` , :func:`~DIRAC.Core.Utilities.ReturnValues.S_ERROR`
    """
    useroutputdata = useroutputdata.split(";")
    for data in useroutputdata:
      for item in useroutputsandbox:
        if data == item:
          LOG.error("Output data and sandbox should not contain the same things.")
          return S_ERROR("Output data and sandbox should not contain the same things.")
      if data.find("*") > -1:
        LOG.error("Remove wildcard characters from output data definition: must be exact files")
        return S_ERROR("Wildcard character in OutputData definition")
    return S_OK()

  def checkInputSandboxLFNs(self, job):
    """ Check that LFNs in the InputSandbox exist in the FileCatalog

    :param job: :mod:`job object <ILCDIRAC.Interfaces.API.NewInterface.Job.Job>`
    :return: :func:`~DIRAC.Core.Utilities.ReturnValues.S_OK` , :func:`~DIRAC.Core.Utilities.ReturnValues.S_ERROR`
    """
    lfns = []
    inputsb = job.workflow.findParameter("InputSandbox")
    if inputsb:
      isblist = inputsb.getValue()
      if isblist:
        isblist = isblist.split(';')
        for inBoxFile in isblist:
          if inBoxFile.lower().count('lfn:'):
            lfns.append(inBoxFile.replace('LFN:', '').replace('lfn:', ''))

    if not lfns:
      return S_OK()

    res = self.getReplicas(lfns)
    if not res["OK"]:
      return S_ERROR('Could not get replicas')
    failed = res['Value']['Failed']
    if failed:
      LOG.error('Failed to find replicas for the following files %s' % string.join(failed, ', '))
      return S_ERROR('Failed to find replicas')

    LOG.info('All LFN files have replicas available')
    singleReplicaSEs = set(self.ops.getValue("/UserJobs/InputSandbox/SingleReplicaSEs", []))
    preferredSEs = set(self.ops.getValue("/UserJobs/InputSandbox/PreferredSEs", []))
    minimumNumberOfReplicas = self.ops.getValue("/UserJobs/InputSandbox/MinimumNumberOfReplicas", 2)
    failSubmission = []
    for lfn, replicas in res['Value']['Successful'].iteritems():
      sites = set(replicas.keys())
      if singleReplicaSEs.intersection(sites):
        continue
      if len(replicas) < minimumNumberOfReplicas:
        LOG.error("ERROR: File %r has less than %s replicas,"
                       "please use dirac-dms-replicate-lfn to replicate to e.g.,:%s"
                       % (lfn, minimumNumberOfReplicas, ", ".join(preferredSEs - set(replicas.keys()))))
        failSubmission.append(lfn)
        for site in set((preferredSEs - set(replicas.keys()))):
          LOG.error("  dirac-dms-replicate-lfn %s %s" % (lfn, site))
        LOG.error("Or use job.setInputData for data files")
    if failSubmission:
      return S_ERROR("Not enough replicas for %s" % ",".join(failSubmission))

    return S_OK()


  def submitJob( self, job, mode='wms' ):
    """Submit jobs to DIRAC WMS.
       These can be either:

        - Instances of the Job Class
           - VO Application Jobs
           - Inline scripts
           - Scripts as executables
           - Scripts inside an application environment

        - JDL File
        - JDL String

       Example usage:

       >>> print dirac.submitJob(job)
       {'OK': True, 'Value': '12345'}

       :param job: Instance of Job class or JDL string
       :type job: `ILCDIRAC.Interfaces.API.NewInterface.Job.Job` or str
       :param mode: Submit job locally with mode = ``'wms'`` (default), ``'local'`` to run workflow or ``'agent'`` to
           run full Job Wrapper locally
       :type mode: str
       :returns: S_OK,S_ERROR
    """
    LOG.debug("Submitting job")
    return super(DiracILC, self).submitJob(job, mode)

  def runLocal(self, job):
    """Run job locally.

    Internal function. This method is called by DIRAC API function
        submitJob(job,mode='Local').  All output files are written to the local
        directory.

    :param job: a job object
    :type job: ~DIRAC.Interfaces.API.Job.Job
    """
    LOG.notice('Executing workflow locally')
    curDir = os.getcwd()
    LOG.info('Executing from %s' % curDir)

    jobDir = tempfile.mkdtemp(suffix='_JobDir', prefix='Local_', dir=curDir)
    os.chdir(jobDir)
    LOG.info('Executing job at temp directory %s' % jobDir)

    tmpdir = tempfile.mkdtemp(prefix='DIRAC_')
    LOG.verbose('Created temporary directory for submission %s' % (tmpdir))
    jobXMLFile = tmpdir + '/jobDescription.xml'
    LOG.verbose('Job XML file description is: %s' % jobXMLFile)
    with open(jobXMLFile, 'w+') as fd:
      fd.write(job._toXML())  # pylint: disable=protected-access

    shutil.copy(jobXMLFile, '%s/%s' % (os.getcwd(), os.path.basename(jobXMLFile)))

    res = self._Dirac__getJDLParameters(job)  # pylint: disable=no-member
    if not res['OK']:
      LOG.error("Could not extract job parameters from job")
      return res
    parameters = res['Value']

    LOG.verbose("Job parameters: %s" % pformat(parameters))
    inputDataRes = self._getLocalInputData(parameters)
    if not inputDataRes['OK']:
      return inputDataRes
    inputData = inputDataRes['Value']

    if inputData:
      LOG.verbose("Job has input data: %s" % inputData)
      localSEList = gConfig.getValue('/LocalSite/LocalSE', '')
      if not localSEList:
        return self._errorReport('LocalSite/LocalSE should be defined in your config file')
      localSEList = localSEList.replace(' ', '').split(',')
      LOG.debug("List of local SEs: %s" % localSEList)
      inputDataPolicy = self._Dirac__getVOPolicyModule('InputDataModule')  # pylint: disable=no-member
      if not inputDataPolicy:
        return self._errorReport('Could not retrieve DIRAC/VOPolicy/InputDataModule for VO')

      LOG.info('Job has input data requirement, will attempt to resolve data for %s' % DIRAC.siteName())
      LOG.verbose('\n'.join(inputData if isinstance(inputData, (list, tuple)) else [inputData]))
      replicaDict = self.getReplicasForJobs(inputData)
      if not replicaDict['OK']:
        return replicaDict
      guidDict = self.getMetadata(inputData)
      if not guidDict['OK']:
        return guidDict
      for lfn, reps in replicaDict['Value']['Successful'].iteritems():
        guidDict['Value']['Successful'][lfn].update(reps)
      resolvedData = guidDict
      diskSE = gConfig.getValue(self.section + '/DiskSE', ['-disk', '-DST', '-USER', '-FREEZER'])
      tapeSE = gConfig.getValue(self.section + '/TapeSE', ['-tape', '-RDST', '-RAW'])
      configDict = {'JobID': None,
                    'LocalSEList': localSEList,
                    'DiskSEList': diskSE,
                    'TapeSEList': tapeSE}
      LOG.verbose(configDict)
      argumentsDict = {'FileCatalog': resolvedData,
                       'Configuration': configDict,
                       'InputData': inputData,
                       'Job': parameters}
      LOG.verbose(argumentsDict)
      moduleFactory = ModuleFactory()
      moduleInstance = moduleFactory.getModule(inputDataPolicy, argumentsDict)
      if not moduleInstance['OK']:
        LOG.warn('Could not create InputDataModule')
        return moduleInstance

      module = moduleInstance['Value']
      result = module.execute()
      if not result['OK']:
        LOG.warn('Input data resolution failed')
        return result

    softwarePolicy = self._Dirac__getVOPolicyModule('SoftwareDistModule')  # pylint: disable=no-member
    if softwarePolicy:
      moduleFactory = ModuleFactory()
      moduleInstance = moduleFactory.getModule(softwarePolicy, {'Job': parameters})
      if not moduleInstance['OK']:
        LOG.warn('Could not create SoftwareDistModule')
        return moduleInstance

      module = moduleInstance['Value']
      result = module.execute()
      if not result['OK']:
        LOG.warn('Software installation failed with result:\n%s' % (result))
        return result
    else:
      LOG.verbose('Could not retrieve DIRAC/VOPolicy/SoftwareDistModule for VO')

    sandbox = parameters.get('InputSandbox')
    if sandbox:
      LOG.verbose("Input Sandbox is %s" % sandbox)
      if isinstance(sandbox, basestring):
        sandbox = [isFile.strip() for isFile in sandbox.split(',')]
      for isFile in sandbox:
        LOG.debug("Resolving Input Sandbox %s" % isFile)
        if isFile.lower().startswith("lfn:"):  # isFile is an LFN
          isFile = isFile[4:]
        # Attempt to copy into job working directory, unless it is already there
        if os.path.exists(os.path.join(os.getcwd(), os.path.basename(isFile))):
          LOG.debug("Input Sandbox %s found in the job directory, no need to copy it" % isFile)
        else:
          if os.path.isabs(isFile) and os.path.exists(isFile):
            LOG.debug("Input Sandbox %s is a file with absolute path, copying it" % isFile)
            shutil.copy(isFile, os.getcwd())
          elif os.path.isdir(isFile):
            LOG.debug("Input Sandbox %s is a directory, found in the user working directory, copying it" % isFile)
            shutil.copytree(isFile, os.path.basename(isFile), symlinks=True)
          elif os.path.exists(os.path.join(curDir, os.path.basename(isFile))):
            LOG.debug("Input Sandbox %s found in the submission directory, copying it" % isFile)
            shutil.copy(os.path.join(curDir, os.path.basename(isFile)), os.getcwd())
          elif os.path.exists(os.path.join(tmpdir, isFile)):  # if it is in the tmp dir
            LOG.debug("Input Sandbox %s is a file, found in the tmp directory, copying it" % isFile)
            shutil.copy(os.path.join(tmpdir, isFile), os.getcwd())
          else:
            LOG.verbose("perhaps the file %s is in an LFN, so we attempt to download it." % isFile)
            getFile = self.getFile(isFile)
            if not getFile['OK']:
              LOG.warn('Failed to download %s with error: %s' % (isFile, getFile['Message']))
              return S_ERROR('Can not copy InputSandbox file %s' % isFile)

        isFileInCWD = os.getcwd() + os.path.sep + isFile

        basefname = os.path.basename(isFileInCWD)
        if tarfile.is_tarfile(basefname):
          try:
            with tarfile.open(basefname, 'r') as tf:
              for member in tf.getmembers():
                tf.extract(member, os.getcwd())
          except (tarfile.ReadError, tarfile.CompressionError, tarfile.ExtractError) as x:
            return S_ERROR('Could not untar or extract %s with exception %s' % (basefname, repr(x)))

    LOG.info('Attempting to submit job to local site: %s' % DIRAC.siteName())

    if 'Executable' in parameters:
      executable = os.path.expandvars(parameters['Executable'])
    else:
      return self._errorReport('Missing job "Executable"')

    arguments = parameters.get('Arguments', '')

    # Replace argument placeholders for parametric jobs
    # if we have Parameters then we have a parametric job
    if 'Parameters' in parameters:
      for par, value in parameters.iteritems():
        if par.startswith('Parameters.'):
          # we just use the first entry in all lists to run one job
          parameters[par[len('Parameters.'):]] = value[0]
      arguments = arguments % parameters

    command = '%s %s' % (executable, arguments)
    # If not set differently in the CS use the root from the current DIRAC installation
    siteRoot = gConfig.getValue('/LocalSite/Root', DIRAC.rootPath)

    os.environ['DIRACROOT'] = siteRoot
    LOG.verbose('DIRACROOT = %s' % (siteRoot))
    os.environ['DIRACPYTHON'] = sys.executable
    LOG.verbose('DIRACPYTHON = %s' % (sys.executable))

    LOG.info('Executing: %s' % command)
    executionEnv = dict(os.environ)
    variableList = parameters.get('ExecutionEnvironment')
    if variableList:
      LOG.verbose('Adding variables to execution environment')
      if isinstance(variableList, basestring):
        variableList = [variableList]
      for var in variableList:
        nameEnv = var.split('=')[0]
        valEnv = urllib.unquote(var.split('=')[1])  # this is needed to make the value contain strange things
        executionEnv[nameEnv] = valEnv
        LOG.verbose('%s = %s' % (nameEnv, valEnv))

    cbFunction = self._Dirac__printOutput  # pylint: disable=no-member

    result = shellCall(0, command, env=executionEnv, callbackFunction=cbFunction)
    if not result['OK']:
      return result

    status = result['Value'][0]
    LOG.verbose('Status after execution is %s' % (status))

    # FIXME: if there is an callbackFunction, StdOutput and StdError will be empty soon
    outputFileName = parameters.get('StdOutput')
    errorFileName = parameters.get('StdError')

    if outputFileName:
      stdout = result['Value'][1]
      if os.path.exists(outputFileName):
        os.remove(outputFileName)
      LOG.info('Standard output written to %s' % (outputFileName))
      with open(outputFileName, 'w') as outputFile:
        print >> outputFile, stdout
    else:
      LOG.warn('Job JDL has no StdOutput file parameter defined')

    if errorFileName:
      stderr = result['Value'][2]
      if os.path.exists(errorFileName):
        os.remove(errorFileName)
      LOG.verbose('Standard error written to %s' % (errorFileName))
      with open(errorFileName, 'w') as errorFile:
        print >> errorFile, stderr
      sandbox = None
    else:
      LOG.warn('Job JDL has no StdError file parameter defined')
      sandbox = parameters.get('OutputSandbox')

    if sandbox:
      if isinstance(sandbox, basestring):
        sandbox = [osFile.strip() for osFile in sandbox.split(',')]
      for i in sandbox:
        globList = glob.glob(i)
        for osFile in globList:
          if os.path.isabs(osFile):
            # if a relative path, it is relative to the user working directory
            osFile = os.path.basename(osFile)
          # Attempt to copy back from job working directory
          if os.path.isdir(osFile):
            shutil.copytree(osFile, curDir, symlinks=True)
          elif os.path.exists(osFile):
            shutil.copy(osFile, curDir)
          else:
            return S_ERROR('Can not copy OutputSandbox file %s' % osFile)

    LOG.verbose('Cleaning up %s...' % tmpdir)
    self._Dirac__cleanTmp(tmpdir)  # pylint: disable=no-member
    os.chdir(curDir)

    if status:
      return S_ERROR('Execution completed with non-zero status %s' % (status))
    return S_OK('Execution completed successfully')
