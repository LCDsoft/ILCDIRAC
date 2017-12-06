"""
API to use to submit jobs in the ILC VO

:since:  Apr 13, 2010
:author: Stephane Poss
"""

import os
import string

from DIRAC.Interfaces.API.Dirac                     import Dirac
from DIRAC.DataManagementSystem.Client.DataManager  import DataManager
from DIRAC.ConfigurationSystem.Client.Helpers.Operations            import Operations
from DIRAC import gConfig, S_ERROR, S_OK, gLogger

from ILCDIRAC.Core.Utilities.ProcessList            import ProcessList

__RCSID__ = "$Id$"

#pylint: disable=protected-access

COMPONENT_NAME = 'DiracILC'

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
    self.log = gLogger
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
      gLogger.info('Will download the process list locally. To gain time, please put it somewhere and add to \
      your dirac.cfg the entry /LocalSite/ProcessListPath pointing to the file')
      pathtofile = self.ops.getValue("/ProcessList/Location", "")
      if not pathtofile:
        gLogger.error("Could not get path to process list")
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
      self.log.error('You should use job.submit(dirac)')
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
        self.log.error( '>>>> Error in %s() <<<<\n%s' % ( method, string.join( errorList, '\n' ) ) )
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
      gLogger.warn( "No repository is initialised" )
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
        self.log.warn("Input Sandbox contains list of lists. Please avoid that.")
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

    self.log.debug("Checking for software version in " + csPathTarBall)
    app_version = self.ops.getValue(csPathTarBall,'')

    self.log.debug("Checking for software version in " + csPathCVMFS)
    app_version_cvmfs = self.ops.getValue(csPathCVMFS,'')

    if not app_version and not app_version_cvmfs:
      self.log.error("Could not find the specified software %s_%s for %s, check in CS" % (appName, appVersion, platform))
      return S_ERROR("Could not find the specified software %s_%s for %s, check in CS" % (appName, appVersion, platform))
    return S_OK()
  
  def _checkoutputpath(self, path):
    """ Validate the outputpath specified for the application

    :param string path: Path of output data
    :return: :func:`~DIRAC.Core.Utilities.ReturnValues.S_OK` , :func:`~DIRAC.Core.Utilities.ReturnValues.S_ERROR`
    """
    if path.find("//") > -1 or path.find("/./") > -1 or path.find("/../") > -1:
      self.log.error("OutputPath of setOutputData() contains invalid characters, please remove any //, /./, or /../")
      return S_ERROR("Invalid path")
    path = path.rstrip()
    if path[-1] == "/":
      self.log.error("Please strip trailing / from outputPath in setOutputData()")
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
          self.log.error("Output data and sandbox should not contain the same things.")
          return S_ERROR("Output data and sandbox should not contain the same things.")
      if data.find("*") > -1:
        self.log.error("Remove wildcard characters from output data definition: must be exact files")
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
      self.log.error('Failed to find replicas for the following files %s' % string.join(failed, ', '))
      return S_ERROR('Failed to find replicas')

    self.log.info('All LFN files have replicas available')
    singleReplicaSEs = set(self.ops.getValue("/UserJobs/InputSandbox/SingleReplicaSEs", []))
    preferredSEs = set(self.ops.getValue("/UserJobs/InputSandbox/PreferredSEs", []))
    minimumNumberOfReplicas = self.ops.getValue("/UserJobs/InputSandbox/MinimumNumberOfReplicas", 2)
    failSubmission = []
    for lfn, replicas in res['Value']['Successful'].iteritems():
      sites = set(replicas.keys())
      if singleReplicaSEs.intersection(sites):
        continue
      if len(replicas) < minimumNumberOfReplicas:
        self.log.error("ERROR: File %r has less than %s replicas,"
                       "please use dirac-dms-replicate-lfn to replicate to e.g.,:%s"
                       % (lfn, minimumNumberOfReplicas, ", ".join(preferredSEs - set(replicas.keys()))))
        failSubmission.append(lfn)
        for site in set((preferredSEs - set(replicas.keys()))):
          self.log.error("  dirac-dms-replicate-lfn %s %s" % (lfn, site))
        self.log.error("Or use job.setInputData for data files")
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
    self.log.debug( "Submitting job" )
    return super(DiracILC, self).submitJob(job, mode)
