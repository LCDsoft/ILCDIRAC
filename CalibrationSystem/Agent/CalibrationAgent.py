""" :mod: CalibrationAgent

    Supervises the state of the jobs started by the CalibrationService and requests resubmission if
    too many failed.
"""

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
    return S_OK()

  def fetchJobStatuses(self):
    """ Requests the statuses of all CalibrationService jobs and returns them

    :returns: Dictionary of type workerID (int) -> jobStatus (enum)
    :rtype: dict
    """
    result = {1381: 'OK', 11743: 'FAILED'}
    #result = someAPICall('CalibrationService')
    return result

  def requestResubmission(self, failedJobs):
    """ Requests the Service to resubmit the failed jobs.

    :param list failedJobs: List of 2-tuples ( calibrationID, workerID )
    :returns: None
    """
    calibrationService = RPCClient('CalibrationSystem')  # FIXME: check name
    calibrationService.resubmitJobs(failedJobs)  # FIXME: Check for error
