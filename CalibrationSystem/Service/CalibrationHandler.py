"""
The CalibrationHandler collates (calibration results) and distributes (input parameters) information from
the calibration worker nodes and allows the creation of calibration runs. It will (re-)submit jobs and
distribute reconstruction workloads among them
"""

from collections import defaultdict
import xml.etree.ElementTree as ET
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.Proxy import executeWithUserProxy


__RCSID__ = "$Id$"


class CalibrationPhase(object):
  """ Represents the different phases a calibration can be in.
  Since Python 2 does not have enums, this is hardcoded for the moment.
  Should this solution not be sufficient any more, one can make a better enum implementation by hand or install a backport of the python3 implementation from PyPi. """
  ECalDigi, HCalDigi, MuonDigi, ElectroMagEnergy, HadronicEnergy = range(5)

  @classmethod
  def phaseIDFromString(cls, phase_name):
    """ Returns the ID of the given CalibrationPhase, passed as a string.

    :param basestring phase_name: Name of the CalibrationPhase. Allowed are: ECalDigi, HCalDigi, MuonDigi, ElectroMagEnergy, HadronicEnergy
    :returns: ID of this phase
    :rtype: int
    """
    if phase_name == 'ECalDigi':
      return 0
    elif phase_name == 'HCalDigi':
      return 1
    elif phase_name == 'MuonDigi':
      return 2
    elif phase_name == 'ElectroMagEnergy':
      return 3
    elif phase_name == 'HadronicEnergy':
      return 4
    else:
      raise ValueError('There is no CalibrationPhase with the name %s' % phase_name)

  @classmethod
  def phaseNameFromID(cls, phaseID):
    """ Returns the name of the CalibrationPhase with the given ID, as a string

    :param int phaseID: ID of the enquired CalibrationPhase
    :returns: The name of the CalibrationPhase
    :rtype: basestring
    """
    if phaseID == 0:
      return 'ECalDigi'
    elif phaseID == 1:
      return 'HCalDigi'
    elif phaseID == 2:
      return 'MuonDigi'
    elif phaseID == 3:
      return 'ElectroMagEnergy'
    elif phaseID == 4:
      return 'HadronicEnergy'
    else:
      raise ValueError('There is no CalibrationPhase with the name %d' % phaseID)


class WorkerInfo(object):
  """ Wrapper class to store information needed by workers to compute their result. """

  def __init__(self, parameterSet, offset):
    """ Creates a new WorkerInfo object, passed to the worker nodes to enable them to compute their results.

    :param object parameterSet: The histogram to use for the calibration.
    :param int offset: The offset in the event file, used to determine starting point of this computation.
    """
    self.parameterSet = parameterSet
    self.offset = offset

  def getInfo(self):
    return (self.parameterSet, self.offset)

#pylint: disable=no-self-use


class CalibrationResult(object):
  """ Wrapper class to store information about calibration computation interim results. Stores results
  from all worker nodes from a single step. """

  def __init__(self):
    self.results = dict()

  def addResult(self, workerID, result):
    """ Adds a result from a given worker to the object

    :param int workerID: ID of the worker providing the result
    :param result: list of floats representing the returned histogram
    :type result: `python:list`
    :returns: None
    """
    self.results[workerID] = result

  def getNumberOfResults(self):
    """ Return number of interim results stored in this wrapper

    :returns: Number of histograms stored in this object
    :rtype: int
    """
    return len(self.results)


class CalibrationRun(object):
  """ Object that stores information about a single run of the calibration software.
  Includes files, current parameter set, software version, the workers running as well as
  the results of each step.
  """

  def __init__(self, steeringFile, ilcsoftPath, inputFiles, numberOfJobs):
    self.steeringFile = steeringFile
    self.ilcsoftPath = ilcsoftPath
    self.inputFiles = inputFiles
    self.stepResults = defaultdict(CalibrationResult)
    self.currentPhase = CalibrationPhase.ECalDigi
    self.currentStep = 0
    self.currentParameterSet = None
    self.numberOfJobs = numberOfJobs
    self.calibrationFinished = False
    #self.workerJobs = [] ##FIXME: Disabled because not used? Maybe in submit initial jobs
    #self.activeWorkers = dict() ## dict between calibration and worker node? ##FIXME:Disabled because not used?
    #FIXME: Probably need to store a mapping workerID -> part of calibration that worker is working on. This then needs to be accessed by the agent in the case of resubmission

  @executeWithUserProxy
  def submitInitialJobs(self, calibrationID):
    """ Submit the calibration jobs to the workers for the first time.
    Use a specially crafted application that runs repeated Marlin reconstruction steps

    :param int calibrationID: ID of this calibration. Needed for the jobName parameter
    :returns: None
    """
    from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob
    from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
    dirac = DiracILC(True, 'some_job_repository.rep')
    results = []
    self.init_files()
    for i in xrange(0, self.numberOfJobs):
      curWorkerID = 1  # FIXME: Decide ID of worker somehow
      curJob = UserJob()
      curJob.check = False  # Necessary to turn off user confirmation
      curJob.setName('CalibrationService_calid_%s_workerid_%s' % (calibrationID, curWorkerID))
      curJob.setJobGroup('CalibrationService_calib_job')
      curJob.setExecutable('/bin/bash',
                           arguments='CalibrationSystem/Client/run_calibration.sh %s 0' % self.ilcsoftPath,
                           logFile='pfa_test.log')
      curJob.setCPUTime(3600)
      inputSB = ['GearOutput.xml', 'PandoraSettingsDefault.xml', 'PandoraLikelihoodData9EBin.xml']
      lcio_files = _getLCIOInputFiles(self.inputFiles[i])
      inputSB += lcio_files
      curJob.setInputSandbox(inputSB)
      curJob.setOutputSandbox(['*.log'])
      res = curJob.submit(dirac)
      results.append(res)
      #FIXME: Maybe move creation of job object to its own method and share code with resubmitJob method?
    return results

  def init_files(self):
    """ Initializes the necessary files etc.

    :returns: nothing
    :rtype: None
    """
    import tempfile
    tmp_path = tempfile.mkdtemp()
    #FIXME: Fix these to copy the actual directory
    # Copy GearOutput.xml, PandoraSettingsDefault.xml, PandoraLikelihoodData9EBin.xml to tmp dir
    GAMMA_FILES = execute_and_return_output(['python', 'Xml_Generation/countMatches.py',
                                             GAMMA_PATH, SLCIO_FORMAT]).split(' ')
    MUON_FILES = execute_and_return_output(['python', 'Xml_Generation/countMatches.py',
                                            MUON_PATH, SLCIO_FORMAT]).split(' ')
    KAON_FILES = execute_and_return_output(['python', 'Xml_Generation/countMatches.py',
                                            KAON_PATH, SLCIO_FORMAT]).split(' ')

  def addResult(self, stepID, workerID, result):
    """ Add a reconstruction result to the list of other results

    :param int stepID: ID of the step
    :param int workerID: ID of the worker providing the result
    :param result: reconstruction histogram from the worker node
    :type result: `python:list`
    :returns: None
    """
    self.stepResults[stepID].addResult(workerID, result)
    #FIXME: Do we add old step results? Current status is no, ensured in CalibrationHandler
    #FIXME: Do we delete old interim results?

  def getNewParameters(self, stepIDOnWorker):
    """ Returns the current parameters

    :param int stepIDOnWorker: The ID of the step the worker just completed.
    :returns: If the computation is finished, returns S_OK containing a success message string. If there is a new parameter set, a S_OK dict containing the updated parameter set. Else a S_ERROR
    :rtype: dict
    """
    if self.calibrationFinished:
      return S_ERROR('Calibration finished! End job now.')
    if self.currentStep > stepIDOnWorker:
      return S_OK(self.currentParameterSet)
    else:
      return S_OK('No new parameter set available yet. Current step in service: %s, step on worker: %s' % (self.currentStep, stepIDOnWorker))

  def endCurrentStep(self):
    """ Calculates the new parameter set based on the results from the computations and prepares the object
    for the next step. (StepCounter increased, ...)

    :returns: None
    """
    self.__calculateNewParams(self.currentStep)
    self.currentStep += 1
    #if self.currentStep > 15: #FIXME: Implement real stopping criterion
    if self.currentStep > 1:  # FIXME: replace with line above after testing
      self.calibrationFinished = True
      #FIXME: Decide how a job finishing should be handled - set this flag to True and have user poll, do something actively here, etc...
    #self.activeWorkers = dict()
    #FIXME: implement following logic:
    # Check if error small enough. If yes, set step to 0 and phase to $nextphase
    # Else increase step by one
    # If $nextphase too high, calibrationFinished = True

  def __addLists(self, list1, list2):
    """ Adds two lists together by adding the first element, second element, and so on. Throws an exception
    if the lists have a different number of elements.

    :param list1: List that should be added element-wise to another
    :type list1: `python:list`
    :param list2: Other list that should be added element-wise
    :type list2: `python:list`
    :returns: The list [ list1[0]+list2[0], list1[1]+list2[1], ... ]
    :rtype: list
    """
    if len(list1) != len(list2):
      raise ValueError('The two lists do not have the same number of elements. \n List 1: %s \n List 2: %s'
                       % (list1, list2))
    result = []
    for first_elem, second_elem in zip(list1, list2):
      result.append(first_elem + second_elem)
    return result

  def __calculateNewParams(self, stepID):
    """ Calculates the new parameter set from the returned histograms. Only call if enough
    results have been reported back!

    :param int stepID: ID of the current step
    :returns: None
    """
    histograms = [self.stepResults[stepID].results[key] for key in self.stepResults[stepID].results.keys()]
    if not histograms:
      raise ValueError('No step results provided!')
    length = len(histograms)
    # Sum over elements of histograms
    result = histograms[0]
    for i in xrange(1, length):
      result = self.__addLists(result, histograms[i])
    # Divide by number of elements to calculate arithmetic mean
    number_of_elements = len(result)
    for i in xrange(0, number_of_elements):
      result[i] = result[i] / float(number_of_elements)
    return result

  @executeWithUserProxy
  def resubmitJob(self, workerID):
    """ Resubmits a job to the worker with the given ID, passing the current parameterSet.
    This is caused by a worker node crashing/a job failing.

    :param int workerID: ID of the worker where the job failed
    :returns: None
    """
    #TODO: Implement Resubmit job, receive information what the failed job was working on somehow.
    pass


class CalibrationHandler(RequestHandler):
  """ Handles the information exchange between worker nodes and the service """
  @classmethod
  def initializeHandler(cls, _):
    """ Initializes the handler, setting required variables. Called once in the beginning of the service """
    cls.activeCalibrations = {}
    cls.calibrationCounter = 0
    return S_OK()

  def initialize(self):
    """ Initializes a single response, setting required variables. Called once /per request/.
    """
    pass

  auth_createCalibration = ['all']
  types_createCalibration = [basestring, basestring, list, int, basestring, basestring]

  def export_createCalibration(self, steeringFile, ilcsoftPath, inputFiles, numberOfJobs,
                               proxyUserName, proxyUserGroup):
    """ Called by users to create a calibration run (series of calibration iterations)

    :param basestring steeringFile: Steering file used in the calibration
    :param basestring ilcsoftPath: Path to the ilcsoft directory to be used, e.g. /cvmfs/clicdp.cern.ch/iLCSoft/builds/2017-02-17/x86_64-slc6-gcc62-opt
    :param inputFiles: Input files for the calibration
    :type inputFiles: `python:list`
    :param int numberOfJobs: Number of jobs this service will run (actual number will be slightly lower)
    :param basestring proxyUserName: Name of the user of the proxy the user is currently having
    :param basestring proxyUserGroup: Name of the group of the proxy the user is currently having
    :returns: S_OK containing ID of the calibration, used to retrieve results etc
    :rtype: dict
    """
    CalibrationHandler.calibrationCounter += 1
    calibrationID = CalibrationHandler.calibrationCounter
    newRun = CalibrationRun(steeringFile, ilcsoftPath, inputFiles, numberOfJobs)
    CalibrationHandler.activeCalibrations[calibrationID] = newRun
    #newRun.submitInitialJobs( calibrationID )
    #return S_OK( calibrationID )
    #FIXME: Find out how to get username and group for this.
    #FIXME: Check if lock is necessary.(Race condition?)
    # , executionLock = False ) #pylint: disable=unexpected-keyword-arg
    res = newRun.submitInitialJobs(calibrationID, proxyUserName=proxyUserName, proxyUserGroup=proxyUserGroup)
    if _calibration_creation_failed(res):
      # FIXME: This should be treated, since the successfully submitted jobs will still run
      ret_val = S_ERROR('Submitting at least one of the jobs failed')
      ret_val['calibrations'] = res
      return ret_val
    return S_OK((calibrationID, res))

  auth_submitResult = ['all']
  types_submitResult = [int, int, int, int, list]

  def export_submitResult(self, calibrationID, phaseID, stepID, workerID, resultHistogram):
    """ Called from the worker node to report the result of the calibration to the service

    :param int calibrationID: ID of the current calibration run
    :param int phaseID: ID of the phase the calibration is currently in
    :param int stepID: ID of the step in this calibration
    :param int workerID: ID of the reporting worker
    :param resultHistogram: The histogram containing the result of the reconstruction run
    :type resultHistogram: `python:list`
    :returns: S_OK in case of success or if the submission was ignored (since it belongs to an older step), S_ERROR if the requested calibration can not be found.
    :rtype: dict
    """
    #TODO: Fix race condition(if it exists)
    calibration = CalibrationHandler.activeCalibrations.get( calibrationID, None )
    if not calibration:
      return S_ERROR( 'Calibration with ID %d not found.' % calibrationID )
    # Only add result if it belongs to current step. Else ignore (it's ok)
    if phaseID is calibration.currentPhase and stepID is calibration.currentStep:
      calibration.addResult( stepID, workerID, resultHistogram )
    return S_OK()

  auth_checkForStepIncrement = ['all']
  types_checkForStepIncrement = []

  def export_checkForStepIncrement(self):
    """ Should only be called by the agent. Periodically checks whether there are any running
    Calibrations that received enough results to start the next step.

    :returns: S_OK when the check has been ended.
    :rtype: dict
    """
    for calibrationID in CalibrationHandler.activeCalibrations:
      calibration = CalibrationHandler.activeCalibrations.get(calibrationID, None)
      if self.finalInterimResultReceived(calibration, calibration.currentStep):
        calibration.endCurrentStep()
    return S_OK()

  finishedJobsForNextStep = 0.8 # X% of all jobs must have finished in order for the next step to begin.

  def finalInterimResultReceived(self, calibration, stepID):
    """ Called periodically. Checks for the given calibration if we now have enough results to compute
    a new ParameterSet.

    :param CalibrationRun calibration: The calibration to check
    :param int stepID: The ID of the current step of that calibration
    :returns: True if enough results have been submitted, False otherwise
    :rtype: bool
    """
    #FIXME: Find out of this is susceptible to race condition
    import math
    numberOfResults = calibration.stepResults[stepID].getNumberOfResults()
    maxNumberOfJobs = calibration.numberOfJobs
    return numberOfResults >= math.ceil(CalibrationHandler.finishedJobsForNextStep * maxNumberOfJobs)

  auth_getNewParameters = [ 'all' ]
  types_getNewParameters = [int, int]

  def export_getNewParameters(self, calibrationID, stepIDOnWorker):
    """ Called by the worker node to retrieve the parameters for the next iteration of the calibration

    :param int calibrationID: ID of the calibration being run on the worker
    :returns: S_ERROR in case of error (e.g. inactive calibration asking for params), S_OK with the parameter set and the id of the current step
    :rtype: dict
    """
    cal = CalibrationHandler.activeCalibrations.get(calibrationID, None)
    if not cal:
      gLogger.error("CalibrationID is not in active calibrations:",
                    "Active Calibrations:%s , asked for %s" % (self.activeCalibrations,
                                                               calibrationID))
      result = S_ERROR(
          "calibrationID is not in active calibrations: %s\nThis should mean that the calibration has finished" % calibrationID)
      return result
    res = cal.getNewParameters(stepIDOnWorker)
    res['current_phase'] = cal.currentPhase
    res['current_step'] = cal.currentStep
    return res

  auth_resubmitJobs = ['all']
  types_resubmitJobs = [list]

  def export_resubmitJobs(self, failedJobs):
    """ Takes a list of workerIDs and resubmits a job with the current parameterset there

    :param failedJobs: List of pairs of the form (calibrationID, workerID)
    :type failedJobs: `python:list`
    :returns: S_OK if successful, else a S_ERROR with a pair ( errorstring, list_of_failed_id_pairs )
    :rtype: dict
    """
    failedPairs = []
    for calibrationID, workerID in failedJobs:
      if calibrationID not in CalibrationHandler.activeCalibrations:
        failedPairs.append((calibrationID, workerID))
        continue
      CalibrationHandler.activeCalibrations[calibrationID].resubmitJob(
          workerID, proxyUserName='', proxyUserGroup='')  # pylint: disable=unexpected-keyword-arg
    if failedPairs:
      result = S_ERROR('Could not resubmit all jobs. Failed calibration/worker pairs are: %s' % failedPairs)
      result['failed_pairs'] = failedPairs
      return result
    else:
      return S_OK()

  auth_getNumberOfJobsPerCalibration = ['all']
  types_getNumberOfJobsPerCalibration = []

  def export_getNumberOfJobsPerCalibration(self):
    """ Returns a dictionary that maps active calibration IDs to the number of initial jobs they submitted.
    Used by the agent to determine when to resubmit jobs.

    :returns: S_OK containing the dictionary with mapping calibrationID -> numberOfJobs
    :rtype: dict
    """
    result = {}
    for calibrationID in CalibrationHandler.activeCalibrations:
      result[calibrationID] = CalibrationHandler.activeCalibrations[calibrationID].numberOfJobs
    return S_OK(result)

#TODO: Add stopping criterion to calibration loop. This should be checked when new parameter sets are calculated
#In that case, the calibration should be removed from activeCalibrations and the result stored.
#Should we then kill all jobs of that calibration?

####################################################################
#                                                                  #
#         Testcode, not to be used by production code              #
#                                                                  #
####################################################################

  auth_resetService = ['all']  # FIXME: Restrict to test usage only
  types_resetService = []

  def export_resetService(self):
    """ Called only by test methods! Resets the service so it can be tested.

    :returns: S_OK on success. (Should always succeed)
    :rtype: dict
    """
    CalibrationHandler.activeCalibrations = {}
    CalibrationHandler.calibrationCounter = 0
    return S_OK()

  auth_getInternals = ['all']  # FIXME: Restrict to test usage only
  types_getInternals = []

  def export_getInternals(self):
    """ Called only by test methods! Returns the class variables of this service,
    exposing its internals and making it testable.
    The activeCalibration dictionary is serialized using the dumps method from the pickle module.
    This is done since for an unknown reason one cannot return objects of custom (i.e. non-default python)
    classes through a service (else a socket timeout occurs).

    :returns: S_OK containing a tuple with the active calibrations dict (serialized with the pickle module) and the calibrationCounter
    :rtype: dict
    """
    import copy
    import pickle
    return S_OK((pickle.dumps(CalibrationHandler.activeCalibrations),
                 copy.deepcopy(CalibrationHandler.calibrationCounter)))

  auth_setRunValues = ['all']
  types_setRunValues = [int, int, object, bool]

  def export_setRunValues(self, calibrationID, currentStep, parameterSet, calFinished):
    """ Sets the values of the calibration with ID calibrationID. It is put to step currentStep,
    gets the parameterSet as current parameter set and the stepFinished status.

    :param int calibrationID: ID of the calibration whose values are to be changed.
    :param int currentStpe: step the calibration is set to.
    :param int parameterSet: New parameterSet for the CalibrationRun
    :param bool calFinished: New calibrationFinished status for the CalibrationRun
    :returns: S_OK after it has finished
    :rtype: dict
    """
    calibration = CalibrationHandler.activeCalibrations.get(calibrationID, None)
    if not calibration:
      return S_ERROR('Calibration with ID %s not in active calibrations.' % calibrationID)
    calibration.currentStep = currentStep
    calibration.currentParameterSet = parameterSet
    calibration.calibrationFinished = calFinished
    return S_OK()

  auth_getopts = ['all']
  types_getopts = [basestring]

  def export_getopts(self, option):
    """ Returns the value of the option stored in the gConfig that this service accesses.

    :param basestring option: name of the option to be queried
    :returns: S_OK containing the value of the option
    :rtype: dict
    """
    from DIRAC import gConfig
    return S_OK(gConfig.getValue(option))

  auth_getproxy_info = ['all']
  types_getproxy_info = []

  def export_getproxy_info(self):
    """ Returns the info of the proxy this service is using.

    :returns: S_OK containing the proxy info
    :rtype: dict
    """
    from DIRAC.Core.Security.ProxyInfo import getProxyInfo
    return S_OK(getProxyInfo())


def _calibration_creation_failed(results):
  """ Returns whether or not the creation of all calibration jobs was successful.

  :param results: List of S_OK/S_ERROR dicts that were returned by the submission call
  :returns: True if everything was successful, False otherwise
  :rtype: bool
  """
  success = True
  for job_result in results:
    success = success and job_result['OK']
  return not success


def _getLCIOInputFiles(xml_file):
  """ Extracts the LCIO input file from the given marlin input xml

  :param string xml_file: Path to the Marlin input xml
  :returns: List with all LCIOInput files
  :rtype: list
  """
  result = []
  tree = ET.parse(xml_file)
  root = tree.getroot()
  elements = root.findall(".//parameter")
  for element in elements:
    if element.get('name', '') == 'LCIOInputFiles' and not element.text.count('afs'):
      result.append(element.text.strip())
  return result
