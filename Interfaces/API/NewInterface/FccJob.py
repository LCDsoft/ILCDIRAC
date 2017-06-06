"""FccJob Module

  This module implements FccJob class which provides Fcc job definition.

  Here is an example of how to use FccJob to run FCC PHYSICS::

  from ILCDIRAC.Interfaces.API.NewInterface.FccJob import FccJob
  from ILCDIRAC.Interfaces.API.NewInterface.Applications.Fcc import FccAnalysis

  j = FccJob()

  FccPhysics = FccAnalysis(
    fccConfFile=
    '/cvmfs/fcc.cern.ch/sw/0.7/fcc-physics/0.1/x86_64-slc6-gcc49-opt/share/ee_ZH_Zmumu_Hbb.txt',
    outputFile="ee_ZH_Zmumu_Hbb.root",
  )

  j.append(FccPhysics)
  jobID = j.submit()

"""

# standard libraries
import sys
import os

# It prints DIRAC environment script path available on AFS/CVMFS
# If you have installed the DIRAC client
# environment script must be here : ~/dirac/bashrc

def _initDirac():
  """This function checks DIRAC environment."""

  diracEnvMessage = (
    "DIRAC environment :\n"
    "Please ensure that you set up correctly DIRAC environment e.g. :\n"
    "source /afs/cern.ch/eng/clic/software/DIRAC/bashrc"
  )
  # DIRAC environment
  try:
    os.environ["DIRAC"]
  except KeyError:
    # Print AFS path of environment script as 'help'
    # Here we use python quit() function and we do not use dirac exit
    # because DIRAC libraries are not yet imported
    print(diracEnvMessage)
    quit()

_initDirac()

# After DIRAC environment checking done, we can import DIRAC libraries

# DIRAC libraries
from DIRAC.Core.Base import Script
Script.parseCommandLine()

from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob
from DIRAC import gLogger, exit as dexit


class FccJob(UserJob):
  """FccJob class is a definition of a FCC job.
  It inherits from the inheritance chain :
    -  UserJob -> Job

  In addition to UserJob functionalities,
  it proposes to split job over various parameters.

  Contrary to the parametric functions, bulk submission is done
  on client side and not on server side.
  """

  def __init__(self, script=None, repositoryName=""):

    super(FccJob, self).__init__(script)

    # DiracILC instance creation
    # 'False' means no repository, else put 'name_of_your_repo'
    # 'name_of_your_repo' is a file generated by DIRAC which can be usefull
    # It contains infos like jobId etc...
    self.ILC = DiracILC(False) if not repositoryName else DiracILC(True, repositoryName)

    # By pass user prompt before submission
    # like that we submit directly
    # without waiting for user shell interaction
    self.ILC.checked = True

    self._data = set()
    self._areDataConsumedBySplitting = False
    self.mode = ""
    self._switch = {}
    self.njobs = None
    self.eventsPerJob = None

    self._userApplications = set()

    # Used for the checking of many fccsw installations
    self._userFccswApplications = set()

    # Names of FCC applications 
    self.fccAppNames = ['FccSw','FccAnalysis']

    self._outputSandbox = set()
    self._inputSandbox = set()
    self._fccStep = 0

  def append(self, application):
    """Redefinition of Dirac.Interfaces.API.Job.append()
    in order to save applications in a list.

    We do not append now, we make splitting stuff and 're-compute'
    each application before really appending applications in
    _addApplication() method.

    """

    if application in self._userApplications:
      gLogger.error("You try to append many times the same application, please fix it !")
      dexit(1)

    #super(FccJob, self).append(application)
    self._userApplications.add(application)

    # Used for the checking of many fccsw installations
    if application.__class__.__name__.startswith(self.fccAppNames[0]):
      self._userFccswApplications.add(application)


  def setInputData(self, lfns):
    """Redefinition of Dirac.Interfaces.API.Job.setInputData()
    in order to save input data into a set.

    We do not set input data now, we just save all them for the moment.

    In the splitting stuff, new jobs may be created. If the
    'by_data' method is used then submit a job per input data.

    Do not send each job with all input data, we
    have to set the right data for each job, data that
    have been saved here in 'data' set.

    The user can specify input data to tell DIRAC to download input data
    (stored in DESY-SRM for example) on the CE.

    Data are registered to the DIRAC catalog and have a tape backend (OutputSE)
    Please refer to the documentation to see how to add files to the catalog.

    """

    self._data = self._outputSandbox.union(lfns) if isinstance(lfns, list) else self._data.union([lfns])
    #super(FccJob, self).setInputData(lfns)

  def setInputSandbox(self, files):
    """Redefinition of Dirac.Interfaces.API.Job.setInputSandbox()
    If the user want to use this function, it may 'erase' the input
    sandbox computed in FCC application.

    So we save job inputs here and add them
    later in _addApplication() method to the FCC application inputs.
    """
    self._inputSandbox = self._inputSandbox.union(files) if isinstance(files, list) else self._inputSandbox.union([files])

  def setOutputSandbox(self, files):
    """Redefinition of Dirac.Interfaces.API.Job.setOutputSandbox()
    If the user want to use this function, it may 'erase' the output
    sandbox computed in FCC application.

    So we save job outputs here and add them
    later in _sendJob() method to the FCC application outputs.
    """
    self._outputSandbox = self._outputSandbox.union(files) if isinstance(files, list) else self._outputSandbox.union([files])

  def submit(self, split=None, njobs=None, eventsPerJob=None, mode="wms"):
    """Redefinition of ILCDIRAC.Interfaces.API.NewInterface.UserJob.submit()
    to add splitting job stuff.

    There are 3 types of submission :

    - local without agent machinery
    - local with agent machinery
    - grid

    The advantage of the Local submission mode is
    that jobs are immediately executed on the local resource.

    :param split: The splitting method to use
    :type split: str

    :param njobs: The number of jobs to use for the splitting
    :type njobs: str or int

    :param eventsPerJob: The number of events to use for the splitting
    :type eventsPerJob: str or int

    :param mode: The submission mode
    :type mode: str

    :return: the id(s)
    :rtype: list

    :Example:

    >>> job.submit(split="byEvents", njobs="4", mode ='wms')

    """

    self.mode = mode

    self.eventsPerJob = self._toInt(eventsPerJob)
    self.njobs = self._toInt(njobs)

    if False is self.njobs or False is self.eventsPerJob:
      dexit(1)

    # Switch case python emulation
    self._switch = {"byEvents" : self._splitByEvents,
            "byData" : self._splitByData, None : self._atomicSubmission}

    gLogger.info("DIRAC : DIRAC submission beginning...")

    if not self._checkFccJobConsistency(split):
      gLogger.info("DIRAC : DIRAC submission failed")
      dexit(1)

    jobIds = self._switch[split]()

    if not jobIds:
      gLogger.info("DIRAC : DIRAC submission failed")
      dexit(1)

    gLogger.info("DIRAC : DIRAC submission ending")

    return jobIds

  ###############################  FccJob FUNCTIONS ##############################################

  def _addApplication(self, application):
    """This function adds an application to the job,
    catches error not raised by DIRAC and sets input sandbox.

    :param application: The application you want to add
    :type application: ILCDIRAC.Interfaces.API.NewInterface.Applications.FCC

    :Example:

    >>> self._addApplication(FCC_PHYSICS)

    """

    # We set the application index here because job knows the number of applications
    # This index is used after by the application to generate application log name etc...
    appName = application.__class__.__name__
    appIndex = '%s_%d' % (appName, self._fccStep)

    # If it is an 'Fcc' application then set fccAppIndex attribute
    if appName in self.fccAppNames:
      application.fccAppIndex = appIndex

    self._fccStep += 1

    debugMessage = (
      "Application : "
      "Application '%(name)s' appending..." % {'name':appName}
    )
    gLogger.debug(debugMessage)

    # If user calls setInputSandbox, we get the files in
    # the set '_inputSandbox' and add them to
    # the temporary input sandbox of applications for a future checking.
    # The temporary input sandbox of each application is 'extended'
    # At the end, the final input sandbox take the set of
    # all these files.
    # Indeed, We merge input sandbox files given at the application level
    # with the input sandbox files given at the job level.

    # If it is an 'Fcc' application then set _tempInputSandbox attribute
    if appName in self.fccAppNames and self._inputSandbox:
      application._tempInputSandbox = application._tempInputSandbox.union(self._inputSandbox)

    try:
      appAddition = super(FccJob, self).append(application)
    except Exception:
      excType, excValue, excTraceback = sys.exc_info()
      #print excType, excValue, excTraceback

      if 'NoneType' in str(excValue):
        # The problem is that when the proxy is outdated, DIRAC got an exception
        # but this exception is not raised, a message is printed to the stdout
        # (saying that PEM files are outdated).
        errorMessage = (
          "Application add operation : "
          "Please, configure your proxy before submitting a job from DIRAC\n"
          "If you did not set up a proxy, try to refresh it "
          "by typing :\n"
          "dirac-proxy-init\n"
        )
      else:
        errorMessage = (
          "Application add operation : Error in the description of the module\n"
          "Please pay attention to this message :\n"
          "%(type)s : %(value)s\n" % {'type':excType, 'value':excValue}
        )
      gLogger.error(errorMessage)
      return False

    if 'OK' in appAddition and not appAddition['OK']:
      errorMessage = (
        "Application appending : "
        "Application '%(name)s' appending failed\n%(msg)s" % {'name':appName, 'msg':appAddition['Message']}
      )
      gLogger.error(errorMessage)
      return False

    debugMessage = "Application '%s' appending successfull" % appIndex
    gLogger.debug(debugMessage)

    return True

  def _atomicSubmission(self):
    """This function submits only one job, no splitting."""

    infoMessage = "Job splitting : No splitting to apply,then 'atomic submission' will be used"
    gLogger.info(infoMessage)

    if self.njobs:
      errorMessage = (
        "Atomic submission : You did not specify a splitting method\n"
        "So you do not have to set the number of jobs\n"
        "If you want to split your job over the number of events\n"
        "Please choose 'byEvents' for the 'split' parameter"
      )
      gLogger.error(errorMessage)
      return False

    for application in self._userApplications:
          
      # If application is reading events from files like input data files
      # do not forget to give them to FCCDataSvc()

      # If it is an 'Fcc' application then set _fccInputData attribute
      if application.__class__.__name__ in self.fccAppNames and self._data:
        application._fccInputData = self._data

      if not self._addApplication(application):
        return False

    # send one job
    return self._sendJob()

  def _checkFccJobConsistency(self, split):
    """This function :

    - Checks if FccJob parameters are valid.
    - Detects if the user try to work with many FCCSW installations.

    :param split: the splitting method to apply
    :type split: str

    :return: success or failure of the consistency checking
    :rtype: bool

    :Example:

    >>> self._checkFccJobConsistency(split)

    """

    gLogger.info("FccJob : FccJob _checkFccJobConsistency()...")

    if not self._userApplications:
      errorMessage = (
        "FccJob : Your job is empty !\n"
        "You have to append at least one application\n"
        "FccJob : FccJob _checkFccJobConsistency failed"
      )
      gLogger.error(errorMessage)
      return False

    if split not in self._switch:
      errorMessage = (
        "Job splitting : Bad split value\n"
        "Possible values are :\n"
        "- byData\n"
        "- byEvents\n"
        "- None\n"
        "FccJob : FccJob _checkFccJobConsistency failed"
      )
      gLogger.error(errorMessage)
      return False

    # All applications must have the same number of events
    # We can get this number from the first application for example
    totalNumberOfEvents = next(iter(self._userApplications)).numberOfEvents

    # Ensure that all applications have the same total number of events
    if not all(app.numberOfEvents == totalNumberOfEvents for app in self._userApplications):
      errorMessage = (
        "FccJob : Applications must all have the same number of events\n"
        "FccJob : FccJob _checkFccJobConsistency failed"
      )
      gLogger.error(errorMessage)
      return False

    if totalNumberOfEvents == -1 and not self._data:
      warnMessage = (
        "FccJob : You set the number of events to -1 without input data\n"
        "Was that intentional ?"
      )
      gLogger.warn(warnMessage)

    if self._userFccswApplications:
      fccswPath = next(iter(self._userFccswApplications)).fccswPath

      if not all(app.fccswPath == fccswPath for app in self._userFccswApplications):
        errorMessage = (
          "Submission : You can't have many FCCSW applications\n"
          "running with different installations of FCCSW\n"
          "FccJob : FccJob _checkFccJobConsistency failed"
        )
        gLogger.error(errorMessage)
        return False

    infoMessage = "FccJob : FccJob _checkFccJobConsistency() successfull"
    gLogger.info(infoMessage)

    return True

  def _printSubmission(self, submission):
    """This function interprets dictionnary result returned by DIRAC submit call and prints
    relevant informations like ID of the job etc...

    :param submission: the returned value of submit call
    :type submission: str

    :return: id of the job
    :rtype: str

    :Example:

    >>> self._printSubmission(submission)

    """

    jobId = None

    if 'OK' in submission and not submission['OK']:
      submissionMessage = (
        "Submission : Submission Failure\n"
        "Please check the description of your job\n"
        "Pay attention to the following message :\n%(msg)s" % {'msg':submission['Message']}
      )
      gLogger.error(submissionMessage)
      return False

    # no job ID is given in local submission
    if 'JobID' in submission:
      jobId = submission['JobID']
      submissionMessage = (
        "GRID Submission : The job with the ID '%(id)s' "
        "has been submitted to the grid\n"
        "You can get output by visiting the DIRAC portal of your VO\n"
        "in the 'Job Monitor' tab or by typing :\n"
        "dirac-wms-job-get-output %(id)s" % {'id':str(jobId)}
      )
    else:
      jobId = "NO_ID_IN_LOCAL_SUBMISSION"
      submissionMessage = (
        "Local Submission : The job has been submitted locally\n"
        "Output results should be retrieved in the Local_* folder"
      )

    gLogger.info(submissionMessage)

    return jobId

  def _sendJob(self):
    """This function submits a job which may have been 're-computed' by the splitting stuff
    and set the output sandbox.
    """

    # We set the ouput sandbox here, after getting output sandbox files
    # in setOutputSandbox() method

    for application in self._userApplications:
      # If it is an 'Fcc' application then set _outputSandbox attribute          
      if application.__class__.__name__ in self.fccAppNames:    
        self._outputSandbox = self._outputSandbox.union(application._outputSandbox)
        #self._inputSandbox = self._inputSandbox.union(application._inputSandbox)

    if self._outputSandbox:
      result = super(FccJob, self).setOutputSandbox(list(self._outputSandbox))

      if 'OK' in result and not result['OK']:
        errorMessage = "Output Sandbox : Error in setting the output sandbox"    
        gLogger.error(errorMessage)
        return False

    # Duplicates problem for setInputSandbox :
    # Calling many times setInputSandbox keep the last list and add new list
    # to the old one in the JDL (we use instead the list attribute 'inputSB' of Application)

    #if self._inputSandbox:
    #    super(FccJob, self).setInputSandbox(list(self._inputSandbox))

    #print super(FccJob, self)._toJDL()

    # Because we redefined setInputData() method
    # do not forget to set them for the job except
    # when _splitByData() method is used (hence '_areDataConsumedBySplitting' attribute)

    if self._data and not self._areDataConsumedBySplitting:
      result = super(FccJob, self).setInputData(list(self._data))
      
      if 'OK' in result and not result['OK']:
        errorMessage = "Input Data : Error in setting input data"    
        gLogger.error(errorMessage)
        return False

    submission = super(FccJob, self).submit(self.ILC, mode=self.mode)

    # Reset app counter
    self._fccStep = 0

    return self._printSubmission(submission)

  def _splitByData(self):
    """This function submits a job per input data."""

    infoMessage = "Job splitting : Splitting 'byData' method..."
    gLogger.info(infoMessage)

    # Ensure that data have been specified by setInputData() method
    if not self._data:
      errorMessage = (
        "Job splitting : Can not continue, missing input data\n"
        "splitting 'byData' method needs input data"
      )
      gLogger.error(errorMessage)
      return False

    self._areDataConsumedBySplitting = True

    jobIds = []

    # Here, we submit a job for each data
    for data in self._data:
      # Job level
      # Like that the data will be downloaded in the job PWD
      result = super(FccJob, self).setInputData(data)

      if 'OK' in result and not result['OK']:
        errorMessage = "Job splitting : Error in setting input data"    
        gLogger.error(errorMessage)
        return False

      # application level
      for application in self._userApplications:
        # For FCCSW :
        # This file is given as input file for FCCDataSvc()
        # in the gaudi configuration file 'gaudi_options.py'

        # If it is an 'Fcc' application then set _fccInputData attribute
        if application.__class__.__name__ in self.fccAppNames:
          application._fccInputData = [data]

        if not self._addApplication(application):
          return False

      # Send a job per input data
      jobId = self._sendJob()

      if not jobId:
        return False

      jobIds.append(jobId)

    return jobIds

  def _splitByEvents(self):
    """This function submits a job per subset of events."""

    infoMessage = "Job splitting : splitting 'byEvents' method..."
    gLogger.info(infoMessage)

    # 1st case : submit(njobs=3,eventsPerJob=10)
    # trivial case => each job (total of 3) run applications of 10 events each
    # Do not consider number of event of the application (overwrite it)
    # it is done after

    if self.eventsPerJob and self.njobs:
      debugMessage = (
        "Job splitting : 1st case\n"
        "events per job and number of jobs has been given (easy)"
      )
      gLogger.debug(debugMessage)

      mapEventJob = [self.eventsPerJob] * self.njobs

    # 2nd case : submit(split="byEvents",eventsPerJob=10)
    # In this case the number of events has to be set inside applications
    # otherwise outputs error.
    # Given the number of events per job and total of number of event we want,
    # we can compute the unknown which is the number of jobs

    elif self.eventsPerJob and totalNumberOfEvents:
      debugMessage = (
        "Job splitting : 2nd case\n"
        "Only events per job has been given but we know the total"
        " number of events, so we have to compute the number of jobs required"
      )
      gLogger.debug(debugMessage)

      if self.eventsPerJob > totalNumberOfEvents:
        errorMessage = (
          "Job splitting : The number of events per job has to be"
          " lower than or equal to the total number of events"
        )
        gLogger.error(errorMessage)
        return False


      numberOfJobsIntDiv = totalNumberOfEvents / self.eventsPerJob
      numberOfJobsRest = totalNumberOfEvents % self.eventsPerJob

      mapEventJob = [self.eventsPerJob] * numberOfJobsIntDiv

      mapEventJob += [numberOfJobsRest] if numberOfJobsRest != 0 else []

    # 3rd case : submit(split='byEvents', njobs=10)
    # So the total number of events has to be set inside application
    # If not then output error

    else:
      debugMessage = (
        "Job splitting : 3rd case\n"
        "The number of jobs has to be given and the total number"
        " of events has to be set"
      )
      gLogger.debug(debugMessage)

      if (not totalNumberOfEvents) or (totalNumberOfEvents < self.njobs):
        errorMessage = (
          "Job splitting : The number of events has to be set\n"
          "It has to be greater than or equal to the number of jobs"
        )
        gLogger.error(errorMessage)
        return False

      eventPerJobIntDiv = totalNumberOfEvents / self.njobs
      eventPerJobRest = totalNumberOfEvents % self.njobs

      mapEventJob = [eventPerJobIntDiv] * self.njobs

      if eventPerJobRest != 0:
        for suplement in range(eventPerJobRest):
          mapEventJob[suplement] += 1


    debugMessage = (
      "Job splitting : Here is the 'distribution' of events over the jobs'\n"
      "A list element corresponds to a job and the element value'\n"
      "is the related number of events'\n%(map)s" % {'map':str(mapEventJob)}
    )
    gLogger.debug(debugMessage)

    jobIds = []

    for eventsPerJob in mapEventJob:
      for application in self._userApplications:
        # If application is reading events from files like input data files
        # do not forget to give them to FCCDataSvc().

        # If it is an 'Fcc' application then set _fccInputData attribute
        if application.__class__.__name__ in self.fccAppNames and self._data:
          application._fccInputData = self._data
    
        application.numberOfEvents = eventsPerJob
        if not self._addApplication(application):
          return False

      # Send a job according the mapping : mapEventJob
      jobId = self._sendJob()

      if not jobId:
        return False

      jobIds.append(jobId)

    return jobIds

  def _toInt(self, number):
    """This function casts number parameter to an integer.
    It also accepts 'string integer' parameter.

    :param number: the number to cast (number of events, number of jobs)
    :type number: str or int

    :return: success or failure of the casting
    :rtype: bool

    :Example:

    >>> number = self._toInt("1000")
    """

    if not number:
      return number

    try:
      number = int(number)
      if number <= 0:
        raise ValueError
    except ValueError:
      errorMessage = (
        "Job splitting : Please, enter valid numbers :'\n"
        "'events per job' and 'number of jobs' must be positive integers"
      )
      gLogger.error(errorMessage)
      return False

    return number
