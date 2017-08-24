"""
User Job class. Used to define user jobs!

Example usage:

>>> from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob
>>> from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
>>> myDiracInstance = DiracILC( withRepo=False )
>>> myJob = UserJob()
>>> ...
>>> myJob.append( myMarlinApp )
>>> myJob.submit(myDiracInstance)

:author: Stephane Poss
:author: Remi Ete
:author: Ching Bon Lam
"""

from DIRAC import S_OK
from DIRAC.Core.Security.ProxyInfo                          import getProxyInfo

from ILCDIRAC.Interfaces.API.NewInterface.Job import Job
from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
from DIRAC.Core.Utilities.List import breakListIntoChunks

__RCSID__ = "$Id$"

class UserJob(Job):
  """ User job class. To be used by users, not for production.
  """
  def __init__(self, script = None):
    super(UserJob, self).__init__( script )
    self.type = 'User'
    self.diracinstance = None
    self.usergroup = ['ilc_user', 'calice_user']
    self.proxyinfo = getProxyInfo()

    ########## SPLITTING STUFF : ATTRIBUTES ##########
    self._data = []
    self.splittingOption = None
    self._switch = {}
    self.numberOfJobs = None
    self.totalNumberOfEvents = None
    self.eventsPerJob = None
    self.numberOfFilesPerJob = 1

  def submit(self, diracinstance = None, mode = "wms"):
    """ Submit call: when your job is defined, and all applications are set, you need to call this to
    add the job to DIRAC.

    :param diracinstance: DiracILC instance
    :type diracinstance: ~ILCDIRAC.Interfaces.API.DiracILC.DiracILC
    :param str mode: "wms" (default), "agent", or "local"

    .. note ::
      The *local* mode means that the job will be run on the submission machine. Use this mode for testing of submission scripts

    """
    if self.splittingOption :
      result = self._split()
      if 'OK' in result and not result['OK']:
        return result
        
    #Check the credentials. If no proxy or not user proxy, return an error
    if not self.proxyinfo['OK']:
      self.log.error("Not allowed to submit a job, you need a %s proxy." % self.usergroup)
      return self._reportError("Not allowed to submit a job, you need a %s proxy." % self.usergroup,
                               self.__class__.__name__)
    if 'group' in self.proxyinfo['Value']:
      group = self.proxyinfo['Value']['group']
      if group not in self.usergroup:
        self.log.error("Not allowed to submit a job, you need a %s proxy." % self.usergroup)
        return self._reportError("Not allowed to submit job, you need a %s proxy." % self.usergroup,
                                 self.__class__.__name__)
    else:
      self.log.error("Could not determine group, you do not have the right proxy.")       
      return self._reportError("Could not determine group, you do not have the right proxy.")

    res = self._addToWorkflow()
    if not res['OK']:
      return res
    self.oktosubmit = True
    if not diracinstance:
      self.diracinstance = DiracILC()
    else:
      self.diracinstance = diracinstance
    return self.diracinstance.submit(self, mode)
    
  #############################################################################
  def setInputData( self, lfns ):
    """Specify input data by Logical File Name (LFN).

    Input files specified via this function will be automatically staged if necessary.

    Example usage:

    >>> job = UserJob()
    >>> job.setInputData(['/ilc/prod/whizard/processlist.whiz'])

    :param lfns: Logical File Names
    :type lfns: Single LFN string or list of LFNs
    """
    if isinstance( lfns, list ) and lfns:
      for i, lfn in enumerate( lfns ):
        lfns[i] = lfn.replace( 'LFN:', '' )
      #inputData = map( lambda x: 'LFN:' + x, lfns )
      inputData = lfns #because we don't need the LFN: for inputData, and it breaks the 
      #resolution of the metadata in the InputFilesUtilities
      inputDataStr = ';'.join( inputData )
      description = 'List of input data specified by LFNs'
      self._addParameter( self.workflow, 'InputData', 'JDL', inputDataStr, description )
    elif isinstance( lfns, basestring ): #single LFN
      description = 'Input data specified by LFN'
      self._addParameter( self.workflow, 'InputData', 'JDL', lfns, description )
    else:
      kwargs = {'lfns':lfns}
      return self._reportError( 'Expected lfn string or list of lfns for input data', **kwargs )

    return S_OK()

  def setInputSandbox(self, flist):
    """ Add files to the input sandbox, can be on the local machine or on the grid

    >>> job = UserJob()
    >>> job.setInputSandbox( ['LFN:/ilc/user/u/username/libraries.tar.gz',
    >>>                       'mySteeringFile.xml'] )

    :param flist: Files for the inputsandbox
    :type flist: `python:list` or `str`
    """
    if isinstance( flist, basestring ):
      flist = [flist]
    if not isinstance( flist, list ):
      return self._reportError("File passed must be either single file or list of files.") 
    self.inputsandbox.extend(flist)
    return S_OK()

  #############################################################################
  def setOutputData(self, lfns, OutputPath = '', OutputSE = ''):
    """For specifying output data to be registered in Grid storage.  If a list
    of OutputSEs are specified the job wrapper will try each in turn until
    successful.

    Example usage:

    >>> job = UserJob()
    >>> job.setOutputData(['Ntuple.root'])

    :param lfns: Output data file or files
    :type lfns: Single `str` or `python:list` of strings ['','']
    :param str OutputPath: Optional parameter to specify the Path in the Storage, postpended to /ilc/user/u/username/
    :param OutputSE: Optional parameter to specify the Storage Element to store data or files, e.g. CERN-SRM
    :type OutputSE: `python:list` or `str`
    """
    kwargs = {'lfns' : lfns, 'OutputSE' : OutputSE, 'OutputPath' : OutputPath}
    if isinstance( lfns, list ) and lfns:
      outputDataStr = ';'.join(lfns)
      description = 'List of output data files'
      self._addParameter(self.workflow, 'UserOutputData', 'JDL', outputDataStr, description)
    elif isinstance( lfns, basestring ):
      description = 'Output data file'
      self._addParameter(self.workflow, 'UserOutputData', 'JDL', lfns, description)
    else:
      return self._reportError('Expected file name string or list of file names for output data', **kwargs)

    if OutputSE:
      description = 'User specified Output SE'
      if isinstance( OutputSE, basestring ):
        OutputSE = [OutputSE]
      elif not isinstance( OutputSE, list ):
        return self._reportError('Expected string or list for OutputSE', **kwargs)
      OutputSE = ';'.join(OutputSE)
      self._addParameter(self.workflow, 'UserOutputSE', 'JDL', OutputSE, description)

    if OutputPath:
      description = 'User specified Output Path'
      if not isinstance( OutputPath, basestring ):
        return self._reportError('Expected string for OutputPath', **kwargs)
      # Remove leading "/" that might cause problems with os.path.join
      while OutputPath[0] == '/': 
        OutputPath = OutputPath[1:]
      if OutputPath.count("ilc/user"):
        return self._reportError('Output path contains /ilc/user/ which is not what you want', **kwargs)
      self._addParameter(self.workflow, 'UserOutputPath', 'JDL', OutputPath, description)

    return S_OK()
  
  #############################################################################
  def setOutputSandbox( self, files ):
    """Specify output sandbox files.  If specified files are over 10MB, these
    may be uploaded to Grid storage with a notification returned in the
    output sandbox.

    .. Note ::
       Sandbox files are removed after 2 weeks.

    Example usage:

    >>> job = UserJob()
    >>> job.setOutputSandbox(['*.log','*.sh', 'myfile.txt'])

    Use the output sandbox only for small files. Larger files should be stored
    on the grid and downloaded later if necessary. See :func:`setOutputData`

    :param files: Output sandbox files
    :type files: Single `str` or `python:list` of strings ['','']

    """
    if isinstance( files, list ) and files:
      fileList = ";".join( files )
      description = 'Output sandbox file list'
      self._addParameter( self.workflow, 'OutputSandbox', 'JDL', fileList, description )
    elif isinstance( files, basestring ):
      description = 'Output sandbox file'
      self._addParameter( self.workflow, 'OutputSandbox', 'JDL', files, description )
    else:
      kwargs = {'files' : files}
      return self._reportError( 'Expected file string or list of files for output sandbox contents', **kwargs )

    return S_OK()
    
  def setILDConfig(self,version):
    """ Define the Configuration package to obtain
    """
    appName = 'ILDConfig'
    self._addSoftware(appName.lower(), version)
    
    self._addParameter( self.workflow, 'ILDConfigPackage', 'JDL', appName+version, 'ILDConfig package' )
    return S_OK()


  def setCLICConfig(self, version):
    """Define the CLIC Configuration package to obtain, copies steering files
    from CLIC Configuration folder to working directory

    :param str version: version string, e.g.: 'ILCSoft-2017-07-27'
    """
    appName = 'ClicConfig'
    self._addSoftware(appName.lower(), version)

    self._addParameter( self.workflow, 'ClicConfigPackage', 'JDL', appName+version, 'CLIC Config package' )
    return S_OK()

  
  ##############################  SPLITTING STUFF : METHODS ##############################
  # Some methods have been added :
  #
  # 1) _atomicSubmission
  # 2) _checkJobConsistency
  # 3) _split
  # 4) _splitByData
  # 5) _splitByEvents
  # 6) _toInt
  #
  # Given the type of splitting (byEvents, byData), these functions compute
  # the right parameters of the method 'Job.setParameterSequence()'

  ##############################  SPLITTING STUFF : NEW METHODS ##############################
  def setSplitEvents( self, eventsPerJob=None, numberOfJobs=None, totalNumberOfEvents=None ):
    """This function sets split parameters for doing splitting over events

    Example usage:

    >>> job = UserJob()
    >>> job.setSplitEvents( numberOfJobs=42, totalNumberOfEvents=126 )
    
    :param eventsPerJob: The events processed by a single job
    :type eventsPerJob: int
    
    :param numberOfJobs: The number of jobs
    :type numberOfJobs: int
    
    :param totalNumberOfEvents: The total number of events processed by all jobs
    :type totalNumberOfEvents: int

    """

    self.totalNumberOfEvents =  totalNumberOfEvents
    self.eventsPerJob =  eventsPerJob
    self.numberOfJobs =  numberOfJobs

    self.splittingOption = "byEvents"

  def setSplitInputData( self, lfns, numberOfFilesPerJob = 1):
    """This function sets split parameters for doing splitting over input data
    
    Example usage:

    >>> job = UserJob()
    >>> job.setSplitInputData(['/ilc/prod/whizard/processlist.whiz'])

    :param lfns: Logical File Names
    :type lfns: Single LFN string or list of LFNs

    :param numberOfFilesPerJob: The number of input data processed by a single job
    :type numberOfFilesPerJob: int

    """

    self._data += lfns if isinstance(lfns, list) else [lfns]

    self.numberOfFilesPerJob =  numberOfFilesPerJob

    self.splittingOption = "byData"

  def _split(self):
    """This function checks the consistency of the job and call the right split method. 

    :return: The success or the failure of the consistency checking
    :rtype: DIRAC.S_OK, DIRAC.S_ERROR

    """

    # In Splitting :
    # By pass user prompt before submission
    # self.dontPromptMe()

    self.eventsPerJob = self._toInt(self.eventsPerJob)
    self.numberOfJobs = self._toInt(self.numberOfJobs)

    if False is self.numberOfJobs or False is self.eventsPerJob:
      return self._reportError("Splitting : Invalid values for splitting")

    # Switch case python emulation
    self._switch = {"byEvents" : self._splitByEvents,
                    "byData" : self._splitByData, None : self._atomicSubmission}

    self.log.info("Job : Job submission...")

    if not self._checkJobConsistency():
      errorMessage = (
        "Job : Job submission failed"
        "Job : _checkJobConsistency() failed"
      )
      self.log.error(errorMessage)
      return self._reportError(errorMessage)

    sequence = self._switch[self.splittingOption ]()

    if not sequence:
      errorMessage = (
        "Job : job submission failed"
        "Job : _splitBySomething() failed"
      )
      self.log.error(errorMessage)
      return self._reportError(errorMessage)

    sequenceType, sequenceList, addToWorkflow = sequence[0], sequence[1], sequence[2] 
   
    if sequenceType != "Atomic":
      self.setParameterSequence(sequenceType, sequenceList, addToWorkflow)

    infoMessage = "Job : Job submission successfull"
    self.log.info(infoMessage)

    return S_OK(infoMessage)

  #############################################################################
  def _atomicSubmission(self):
    """This function does not do splitting so do not return valid parameters fot setParameterSequence().
    
    :return: parameter name and parameter values for setParameterSequence()
    :rtype: str, list

    """

    infoMessage = "Job splitting : No splitting to apply, then 'atomic submission' will be used"
    self.log.info(infoMessage)

    return "Atomic", [], False

  #############################################################################
  def _checkJobConsistency(self):
    """This function :

    - Checks if Job parameters are valid.

    :return: The success or the failure of the consistency checking
    :rtype: bool

    :Example:

    >>> self._checkJobConsistency()

    """

    self.log.info("Job consistency : _checkJobConsistency()...")

    if self.splittingOption not in self._switch:
      errorMessage = (
        "Job splitting : Bad split value\n"
        "Possible values are :\n"
        "- byData\n"
        "- byEvents\n"
        "- None\n"
        "Job consistency : _checkJobConsistency() failed"
      )
      self.log.error(errorMessage)
      return False

    # All applications should have the same number of events
    # We can get this number from the first application for example
    sameNumberOfEvents = next(iter(self.applicationlist)).numberOfEvents

    if not all(app.numberOfEvents == sameNumberOfEvents for app in self.applicationlist):
      self.log.warn("Job : Applications should all have the same number of events")

    if (self.totalNumberOfEvents == -1 or sameNumberOfEvents == -1) and not self._data:
      warnMessage = (
        "Job : You set the number of events to -1 without input data\n"
        "Was that intentional ?"
      )
      self.log.warn(warnMessage)

    infoMessage = "Job consistency : _checkJobConsistency() successfull"
    self.log.info(infoMessage)

    return True

  #############################################################################
  def _splitByData(self):
    """This function wants that a job is submitted per input data.

    :return: parameter name and parameter values for setParameterSequence()
    :rtype: str, list

    """

    # reset split attribute to avoid infinite loop
    self.splittingOption = None

    infoMessage = "Job splitting : Splitting 'byData' method..."
    self.log.info(infoMessage)

    # Ensure that data have been specified by setInputData() method
    if not self._data:
      errorMessage = (
        "Job splitting : Can not continue, missing input data\n"
        "splitting 'byData' method needs input data"
      )
      self.log.error(errorMessage)
      return False


    if self.numberOfFilesPerJob > len(self._data):
      errorMessage = (
        "Job splitting : The number of input data file per job 'numberOfFilesPerJob'\n"
        "must be equal or lower than the number of input data you specify"
      )
      self.log.error(errorMessage)
      return False
          
    self._data = breakListIntoChunks(self._data, self.numberOfFilesPerJob)

    self.log.info("Job splitting : Your submission consists of %d job(s)" % len(self._data))

    return ["InputData", self._data , False]

  #############################################################################
  def _splitByEvents(self):
    """This function wants that a job is submitted per subset of events.
    
    :return: parameter name and parameter values for setParameterSequence()
    :rtype: str, list

    """

    # reset split attribute to avoid infinite loop
    self.splittingOption = None

    infoMessage = "Job splitting : splitting 'byEvents' method..."
    self.log.info(infoMessage)

    if self.eventsPerJob and self.numberOfJobs:
      
      # 1st case : (numberOfJobs=3, eventsPerJob=10)
      # trivial case => each job (total of 3) run applications of 10 events each
      
      debugMessage = (
        "Job splitting : 1st case\n"
        "events per job and number of jobs have been given (easy)"
      )
      self.log.debug(debugMessage)

      mapEventJob = [self.eventsPerJob] * self.numberOfJobs

    elif self.eventsPerJob and self.totalNumberOfEvents:
      
      # 2nd case : (split="byEvents", eventsPerJob=10, totalNumberOfEvents=10)
      # Given the number of events per job and total of number of event we want,
      # we can compute the unknown which is the number of jobs.
      

      debugMessage = (
        "Job splitting : 2nd case\n"
        "Events per job and total number of events have been given"
        " so we have to compute the number of jobs required"
      )
      self.log.debug(debugMessage)

      if self.eventsPerJob > self.totalNumberOfEvents:
        errorMessage = (
          "Job splitting : The number of events per job has to be"
          " lower than or equal to the total number of events"
        )
        self.log.error(errorMessage)
        return False


      numberOfJobsIntDiv = self.totalNumberOfEvents / self.eventsPerJob
      numberOfJobsRest = self.totalNumberOfEvents % self.eventsPerJob

      mapEventJob = [self.eventsPerJob] * numberOfJobsIntDiv

      mapEventJob += [numberOfJobsRest] if numberOfJobsRest != 0 else []

    else:
      
      # 3rd case : (split='byEvents', njobs=10, totalNumberOfEvents=10)
      # Then compute the right number of events per job  

      debugMessage = (
        "Job splitting : 3rd case\n"
        "The number of jobs and the total number of events"
        " have been given"
      )
      self.log.debug(debugMessage)

      if (not self.totalNumberOfEvents) or (self.totalNumberOfEvents < self.numberOfJobs):
        errorMessage = (
          "Job splitting : The number of events has to be set\n"
          "It has to be greater than or equal to the number of jobs"
        )
        self.log.error(errorMessage)
        return False

      eventPerJobIntDiv = self.totalNumberOfEvents / self.numberOfJobs
      eventPerJobRest = self.totalNumberOfEvents % self.numberOfJobs

      mapEventJob = [eventPerJobIntDiv] * self.numberOfJobs

      if eventPerJobRest != 0:
        for suplement in range(eventPerJobRest):
          mapEventJob[suplement] += 1


    debugMessage = (
      "Job splitting : Here is the 'distribution' of events over the jobs\n"
      "A list element corresponds to a job and the element value"
      " is the related number of events :\n%(map)s" % {'map':str(mapEventJob)}
    )
    self.log.debug(debugMessage)

    infoMessage = (
      "Job splitting : Your submission consists"
      " of %(number)d job(s)" % {'number':len(mapEventJob)}
    )
    self.log.info(infoMessage)

    return ['NumberOfEvents', mapEventJob, 'NbOfEvts']

  #############################################################################
  def _toInt(self, number):
    """This function casts number parameter to an integer.
    It also accepts 'string integer' parameter.

    :param number: the number to cast (number of events, number of jobs)
    :type number: str or int

    :return: The success or the failure of the casting
    :rtype: bool, int or None

    :Example:

    >>> number = self._toInt("1000")

    """

    if None is number:
      return number

    try:
      number = int(number)
      if number <= 0:
        raise ValueError
    except ValueError:
      errorMessage = (
        "Job splitting : Please, enter valid numbers :\n"
        "'events per job' and 'number of jobs' must be positive integers"
      )
      self.log.error(errorMessage)
      return False

    return number
