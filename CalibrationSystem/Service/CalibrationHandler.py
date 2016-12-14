"""
The CalibrationHandler collates (calibration results) and distributes (input parameters) information from
the calibration worker nodes and allows the creation of calibration runs. It will (re-)submit jobs and
distribute reconstruction workloads among them
"""

from collections import defaultdict
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.DISET.RequestHandler import RequestHandler


__RCSID__ = "$Id$"

#pylint: disable=no-self-use


class CalibrationResult(object):
  """ Wrapper class to store information about calibration computation interim results. Stores results
  from all worker nodes from a single step. """

  def __init__(self):
    self.results = dict()

  def addResult(self, workerID, result):
    """ Adds a result from a given worker to the object

    :param int workerID: ID of the worker providing the result
    :param list result: list of floats representing the returned histogram
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

  def __init__(self, steeringFile, softwareVersion, inputFiles, numberOfJobs):
    self.steeringFile = steeringFile
    self.softwareVersion = softwareVersion
    self.inputFiles = inputFiles
    self.stepResults = defaultdict(CalibrationResult)
    self.currentStep = 0
    self.currentParameterSet = None
    self.numberOfJobs = numberOfJobs
    self.calibrationFinished = False
    #self.workerJobs = [] ##FIXME: Disabled because not used? Maybe in submit initial jobs
    #self.activeWorkers = dict() ## dict between calibration and worker node? ##FIXME:Disabled because not used?

  def submitInitialJobs(self, calibrationID):
    """ Submit the calibration jobs to the workers for the first time.
    Use a specially crafted application that runs repeated Marlin reconstruction steps

    :param int calibrationID: ID of this calibration. Needed for the jobName parameter
    :returns: None
    """
    from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob
    from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
    dirac = DiracILC(True, 'some_job_repository.rep')
    for _ in xrange(0, self.numberOfJobs):
      curWorkerID = 1  # FIXME: Get ID of worker somehow
      curJob = UserJob()
      curJob.setName('CalibrationService_calid_%s_workerid_%s' % (calibrationID, curWorkerID))
      curJob.setJobGroup('CalibrationService_calib_job')

      #curJob.setCPUTime(86400)
      #curJob.setInputSandbox(["file1","file2"])
      #curJob.setOutputSandbox(["fileout1","fileout2", "*.out", "*.log"])
      #curJob.setOutputData(["somefile1","somefile2"],"some/path","CERN-SRM")
      #from ILCDIRAC.Interfaces.API.NewInterface.Applications import CalibrationApp
      #ca = CalibrationApp()
      #res = curJob.append(ca)
      #curJob.submit( dirac )
      #FIXME: Construct and submit special jobs
      #FIXME: Maybe move creation of job object to its own method and share code with resubmitJob method?

  def addResult(self, stepID, workerID, result):
    """ Add a reconstruction result to the list of other results

    :param int stepID: ID of the step
    :param int workerID: ID of the worker providing the result
    :param list result: reconstruction histogram from the worker node
    :returns: None
    """
    self.stepResults[stepID].addResult(workerID, result)
    #FIXME: Do we add old step results?
    #FIXME: Do we delete old interim results?

  def getNewParameters(self, stepIDOnWorker):
    """ Returns the current parameters

    :param int stepIDOnWorker: The ID of the step the worker just completed.
    :returns: If the computation is finished, returns S_OK containing a success message string. If there is a new parameter set, a S_OK dict containing the updated parameter set. Else a S_ERROR
    :rtype: dict
    """
    if self.calibrationFinished:
      return S_OK('Calibration finished! End job now.')
    if self.currentStep > stepIDOnWorker:
      return S_OK(self.currentParameterSet)
    else:
      return S_ERROR('No new parameter set available yet.')

  def endCurrentStep(self):
    """ Calculates the new parameter set based on the results from the computations and prepares the object
    for the next step. (StepCounter increased, ...)

    :returns: None
    """
    self.__calculateNewParams(self.currentStep)
    self.currentStep += 1
    if self.currentStep > 15:  # FIXME: Implement real stopping criterion
      self.calibrationFinished = True
    #self.activeWorkers = dict()

  def __calculateNewParams(self, stepID):
    """ Calculates the new parameter set from the returned histograms. Only call if enough
    results have been reported back!

    :param int stepID: ID of the current step
    :returns: None
    """
    histograms = [self.stepResults[stepID] for key in self.stepResults.keys(
        )]  # FIXME: Use dict method that returns set of key, value pairs or similar?
    print histograms  # Calculate mean of histograms

  def resubmitJob(self, workerID):
    """ Resubmits a job to the worker with the given ID, passing the current parameterSet.
    This is caused by a worker node crashing/a job failing.

    :param int workerID: ID of the worker where the job failed
    :returns: None
    """
    #TODO: Resubmit job
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
  types_createCalibration = [basestring, basestring, list, int]

  def export_createCalibration(self, steeringFile, softwareVersion, inputFiles, numberOfJobs):
    """ Called by users to create a calibration run (series of calibration iterations)

    :param basestring steeringFile: Steering file used in the calibration
    :param basestring softwareVersion: Version of the software
    :param list inputFiles: Input files for the calibration
    :param int numberOfJobs: Number of jobs this service will run (actual number will be slightly lower)
    :returns: S_OK containing ID of the calibration, used to retrieve results etc
    :rtype: dict
    """
    CalibrationHandler.calibrationCounter += 1
    calibrationID = CalibrationHandler.calibrationCounter
    newRun = CalibrationRun(steeringFile, softwareVersion, inputFiles, numberOfJobs)
    CalibrationHandler.activeCalibrations[calibrationID] = newRun
    newRun.submitInitialJobs(calibrationID)
    return S_OK(calibrationID)

  auth_submitResult = ['all']
  types_submitResult = [int, int, int, list]

  def export_submitResult(self, calibrationID, stepID, workerID, resultHistogram):
    """ Called from the worker node to report the result of the calibration to the service

    :param int calibrationID: ID of the current calibration run
    :param int stepID: ID of the step in this calibration
    :param int workerID: ID of the reporting worker
    :param list resultHistogram: The histogram containing the result of the reconstruction run
    :returns: S_OK in case of success or if the submission was ignored (since it belongs to an older step), S_ERROR if the requested calibration can not be found.
    :rtype: dict
    """
    #TODO: Anmerkung Marco: Evtl via agent checken alle X sekunden, Fix race condition
    calibration = CalibrationHandler.activeCalibrations.get( calibrationID, None )
    if not calibration:
      return S_ERROR( 'Calibration with ID %d not found.' % calibrationID )
    if stepID is calibration.currentStep: #Only add result if it belongs to current step
      calibration.addResult( stepID, workerID, resultHistogram )
      if self.finalInterimResultReceived( calibrationID, stepID ):
        calibration.endCurrentStep()
    return S_OK()

  finishedJobsForNextStep = 0.8 # X% of all jobs must have finished in order for the next step to begin.
  def finalInterimResultReceived( self, calibrationID, stepID ):
    """ Called after receiving a result. Checks if adding exactly this result means we now have enough
    results to compute a new ParameterSet. (this method will return False, False, ..., False, True,
    False, False, ..., False)

    :param int calibrationID: The ID of the calibration to check
    :param int stepID: The ID of the current step of that calibration
    :returns: True if it is just now possible to go on to the next step, False if it's not possible yet or has been the case already
    :rtype: bool
    """
    #FIXME: Find out of this is susceptible to race condition
    import math
    currentRun = CalibrationHandler.activeCalibrations[ calibrationID ]
    numberOfResults = currentRun.stepResults[ stepID ].getNumberOfResults()
    maxNumberOfJobs = currentRun.numberOfJobs
    return numberOfResults is math.ceil( CalibrationHandler.finishedJobsForNextStep * maxNumberOfJobs )

  auth_getNewParameters = [ 'all' ]
  types_getNewParameters = [ int, int, int ]
  def export_getNewParameters( self, calibrationID, workerID, stepIDOnWorker ):
    """ Called by the worker node to retrieve the parameters for the next iteration of the calibration

    :param int calibrationID: ID of the calibration being run on the worker
    :param int workerID: ID of the worker
    :returns: S_ERROR in case of error (e.g. inactive calibration asking for params), S_OK with the parameter set
    :rtype: dict
    """
    if calibrationID not in self.activeCalibrations:
      gLogger.error("CalibrationID is not in active calibrations:",
                    "Active Calibrations:%s , asked for %s from worker %s" % (self.activeCalibrations,
                                                                              calibrationID,
                                                                              workerID))
      return S_ERROR("calibrationID is not in active calibrations: %s" % calibrationID)
    return CalibrationHandler.activeCalibrations[calibrationID].getNewParameters(stepIDOnWorker)
    #FIXME: This doesn't actually use the workerID at all. but needs fixing in several files.

  auth_resubmitJobs = ['all']
  types_resubmitJobs = [list]

  def export_resubmitJobs(self, failedJobs):
    """ Takes a list of workerIDs and resubmits a job with the current parameterset there

    :param list failedJobs: List of pairs of the form (calibrationID, workerID)
    :returns: S_OK if successful, else a S_ERROR with a pair ( errorstring, list_of_failed_id_pairs )
    :rtype: dict
    """
    failedPairs = []
    for calibrationID, workerID in failedJobs:
      if calibrationID not in CalibrationHandler.activeCalibrations:
        failedPairs.append((calibrationID, workerID))
        continue
      CalibrationHandler.activeCalibrations[calibrationID].resubmitJob(workerID)
    if failedPairs:
      return S_ERROR(('Could not resubmit all jobs. Failed calibration/worker pairs are: %s' % failedPairs,
                      failedPairs))  # FIXME: maybe dont break DIRAC code conventions
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
