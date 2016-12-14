""" :mod: CalibrationAgent

    Supervises the state of the jobs started by the CalibrationService and requests resubmission if
    too many failed.
"""
from collections import defaultdict

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.DISET.RPCClient import RPCClient

__RCSID__ = "$Id$"


class CalibrationAgent(AgentModule):
  """
  .. class:: CalibrationAgent

  Periodically checks the state of the jobs from the CalibrationService.
  If too few jobs are running, it tells the CalibrationService to restart the failed ones.
  """
  MIN_JOB_SUCCESS_RATE = 0.9  # X% of jobs have to succeed for the next step to start
  MAX_NUMBER_OF_JOBS = 1000  # Number of jobs initially started

  def initialize(self):
    """ Initialization of the Agent
    """
    self.calibrationService = RPCClient('Calibration/Calibration')
    self.currentCalibrations = []  # Contains IDs (int) of the calibrations
    self.currentJobStatuses = {}  # Contains a mapping calibrationID -> dict, the dict contains a mapping
    # WorkerID (int) -> jobStatus (enum)
    #TODO: Dict calibrationID -> numberOfJobs ?
    return S_OK()

  def execute(self):
    """ Executed once per cycle of the agent. Minimum time between two cycles is set via the PollingTime
    parameter (in seconds) in the ConfigTemplate.cfg
    """
    #TODO: Implement: call fetchJobStatuses, compare this to old status. If an entire calibrationRun finished,
    #remove it from data structures. If too many jobs failed, ask Service for resubmission. Then replace old
    #job status dict with new one
    #To clear up: Can a job disappear from this list? Or what happens if node crashes.
    currentStatuses = self.fetchJobStatuses()
    targetJobNumbers = self.calibrationService.getNumberOfJobsPerCalibration()
    self.requestResubmission(self.__calculateJobsToBeResubmitted(currentStatuses, targetJobNumbers))
    self.calibrationService.checkStepIncrement()
    return S_OK()

  def fetchJobStatuses(self):
    """ Requests the statuses of all CalibrationService jobs and returns them, mapped from
    calibrationID -> workerID -> jobStatus.

    :returns: Dictionary of type calibrationID -> dict, with dict of type workerID (int) -> jobStatus (enum)
    :rtype: dict
    """
    result = defaultdict({})
    jobMonitoringService = RPCClient('WorkloadManagement/JobMonitoring')
    jobIDs = jobMonitoringService.getJobs({'JobGroup': 'CalibrationService_calib_job'})['Value']
    jobStatuses = jobMonitoringService.getJobsParameters(jobIDs, ['Name', 'Status'])['Value']
    for _, attrDict in jobStatuses.iteritems():
      jobName = attrDict['Name']
      curCalibration = self.__getCalibrationIDFromJobName(jobName)
      result[curCalibration].update({self.__getWorkerIDFromJobName(jobName): attrDict['Status']})
    return result

  def requestResubmission(self, failedJobs):
    """ Requests the Service to resubmit the failed jobs.

    :param list failedJobs: List of 2-tuples ( calibrationID, workerID )
    :returns: None
    """
    self.calibrationService.resubmitJobs(failedJobs)  # FIXME: Check for error

  def __getWorkerIDFromJobName(self, jobname):
    """ Extracts the worker ID from the raw job name.

    :param basestring jobname: name of the job in the DIRAC DB
    :returns: the worker ID contained in the name string
    :rtype: int
    """
    return int(jobname.split('_')[4])

  def __getCalibrationIDFromJobName(self, jobname):
    """ Extracts the calibration ID from the raw job name.

    :param basestring jobname: name of the job in the DIRAC DB
    :returns: the calibration ID contained in the name string
    :rtype: int
    """
    return int(jobname.split('_')[2])

  def __calculateJobsToBeResubmitted(self, jobStatusDict, targetNumberDict):
    """ Checks if any of the active calibrations have not enough jobs running and if that is the case
    adds the worker nodes that need resubmission to a list that is returned.

    :param dict jobStatusDict: Dictionary with a mapping from calibrationID -> dict, with dict having a mapping workerID -> jobStatus
    :param dict targetNumberDict: Dictionary with a mapping from calibrationID -> number of jobs originally alotted to the calibration
    :returns: List containing 2-tuples ( calibrationID, workerID )
    :rtype: list
    """
    pass
