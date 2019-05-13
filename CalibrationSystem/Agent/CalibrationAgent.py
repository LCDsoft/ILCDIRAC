""" :mod: CalibrationAgent

    Supervises the state of the jobs started by the CalibrationService and requests resubmission if
    too many failed.
"""
from collections import defaultdict

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Base.Client import Client
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient

__RCSID__ = "$Id$"


class CalibrationAgent(AgentModule):
  """
  .. class:: CalibrationAgent

  Periodically checks the state of the jobs from the CalibrationService.
  If too few jobs are running, it tells the CalibrationService to restart the failed ones.
  """
  # TODO FIXME these two constants are not used anywhere for time being
  # if they will be used - don't hardcode them. read from CS with self.am_getOption()
  MIN_JOB_SUCCESS_RATE = 0.9  # X% of jobs have to succeed for the next step to start
  MAX_NUMBER_OF_JOBS = 1000  # Number of jobs initially started

  def initialize(self):
    """ Initialization of the Agent
    """
    self.calibrationService = Client()
    self.calibrationService.setServer('Calibration/Calibration')
    self.currentCalibrations = []  # Contains IDs (int) of the calibrations
    self.currentJobStatuses = {}  # Contains a mapping calibrationID -> dict, the dict contains a mapping
    # WorkerID (int) -> jobStatus (enum)
    return S_OK()

  def execute(self):
    """ Executed once per cycle of the agent. Minimum time between two cycles is set via the PollingTime
    parameter (in seconds) in the ConfigTemplate.cfg
    """
    #TODO: Implement: call fetchJobStatuses, compare this to old status. If an entire calibrationRun finished,
    #remove it from data structures. If too many jobs failed, ask Service for resubmission. Then replace old
    #job status dict with new one
    #To clear up: Can a job disappear from this list? Or what happens if node crashes.
    res = self.fetchJobStatuses()
    if not res['OK']:
      return res
    self.currentJobStatuses = res['Value']
    res = self.calibrationService.getNumberOfJobsPerCalibration()
    if not res['OK']:
      return res
    targetJobNumbers = res['Value']
    self.currentCalibrations = list(targetJobNumbers.keys())
    self.log.info('Execute execute. currentJobStatuses: %s, targetJobNumbers: %s' %
                  (self.currentJobStatuses, targetJobNumbers))
    # TODO temporarily switched off resubmission. For testing purpose
    #  self.requestResubmission( self.__calculateJobsToBeResubmitted( currentStatuses, targetJobNumbers ) )
    res = self.calibrationService.checkForStepIncrement()
    if not res['OK']:
      return res
    return S_OK()

  def fetchJobStatuses(self):
    """ Requests the statuses of all CalibrationService jobs and returns them, mapped from
    calibrationID -> workerID -> jobStatus.

    :returns: Dictionary of type calibrationID -> dict, with dict of type workerID (int) -> jobStatus (enum)
    :rtype: dict
    """
    result = defaultdict(dict)  # defaults to {}
    jobMonitoringService = JobMonitoringClient()
    res = jobMonitoringService.getJobs({'JobGroup': 'CalibrationService_calib_job'})
    if not res['OK']:
      self.log.error("Failed getting job IDs from job DB! Error:", res['Message'])
      return S_ERROR('Failed getting job IDs from job DB!')
    jobIDs = res['Value']
    res = jobMonitoringService.getJobsParameters(_convert_to_int_list(jobIDs), ['JobName', 'Status', 'JobId'])
    if not res['OK']:
      pass
    jobStatuses = res['Value']
    #TODO: Secure for failure
    # Possible statuses in DIRAC: Received	Job is received by the DIRAC WMS
    #Checking:	Job is being checked for sanity by the DIRAC WMS
    #Waiting:	Job is entered into the Task Queue and is waiting to picked up for execution
    #Running:	Job is running
    #Stalled:	Job has not shown any sign of life since 2 hours while in the Running state
    #Completed:	Job finished execution of the user application, but some pending operations remain
    #Done:	Job is fully finished
    #Failed:	Job is finished unsuccessfully
    #Killed:	Job received KILL signal from the user
    #Deleted:	Job is marked for deletion
    # see https://twiki.cern.ch/twiki/bin/view/LHCb/DiracWebPortalJobmonitor
    for attrDict in jobStatuses.values():
      jobName = attrDict['JobName']
      curCalibration = CalibrationAgent.__getCalibrationIDFromJobName(jobName)
      result[curCalibration].update({CalibrationAgent.__getWorkerIDFromJobName(jobName):
                                     (attrDict['Status'], attrDict['JobId'])})
    return S_OK(dict(result))

# TODO FIXME finish implementation
  #  def checkForCalibrationsToBeKilled():
  #    result = self.calibrationService.getCalibrationsToBeKilled()
  #    if not res['OK']:
  #      self.log.error('Failed to get list of calibrations to be killed from service. errMsg: %s' % res['Message'])
  #      return None
  #    calibIds = res['Value']
  #    if len(calibIds)==0:
  #      return None
  #    else:
  #      ?????????????????????
  #      for iCalibId in calibIds:
  #        if not iCalibId in



  RESUBMISSION_RETRIES = 5  # How often the agent tries to resubmit jobs before giving up
  def requestResubmission(self, failedJobs):
    """ Requests the Service to resubmit the failed jobs.

    :param list failedJobs: List of 2-tuples ( calibrationID, workerID )
    :returns: None
    """
    jobs_to_resubmit = failedJobs
    number_of_tries = 0
    result = S_ERROR()
    while not result['OK'] and number_of_tries < CalibrationAgent.RESUBMISSION_RETRIES:
      result = self.calibrationService.resubmitJobs(jobs_to_resubmit)
      number_of_tries += 1
      # FIXME: ResubmitJobs will probably be implemented in a way that would allow some resubmissions to fail and some to work.
      # Thus, this method would need a list of all resubmissions that have yet to be done, which is updated
      # in each iteration. once it is empty, the method returns. If it takes too long, RuntimeError is raised
      if result['OK']:
        return
      else:
        jobs_to_resubmit = result['failed_pairs']
    raise RuntimeError('Cannot resubmit the necessary failed jobs. Problem: %s' % result)

  JOB_STATUS_ENDED = ['Failed', 'Killed', 'Done', 'Completed']
  JOB_STATUS_RUNNING = ['Running', 'Waiting', 'Checking', 'Staging']
  RESUBMISSION_THRESHOLD = 0.13  # When this percentage of jobs failed for good, resubmit new ones #FIXME: Tune this parameter
  def __calculateJobsToBeResubmitted(self, jobStatusDict, targetNumberDict):
    """ Checks if any of the active calibrations have not enough jobs running and if that is the case
    adds the worker nodes that need resubmission to a list that is returned.

    :param dict jobStatusDict: Dictionary with a mapping from calibrationID -> dict, with dict having a mapping
                               workerID -> jobStatus
    :param dict targetNumberDict: Dictionary with a mapping from calibrationID -> number of jobs originally alotted
                                  to the calibration
    :returns: List containing 2-tuples ( calibrationID, workerID )
    :rtype: list
    """
    #FIXME: Maybe use other strategy to resubmit
    result = []
    for calibrationID, workerDict in jobStatusDict.iteritems():
      possibly_successful_jobs = []
      failed_jobs = []
      for workerID, jobStatus in workerDict.iteritems():
        if jobStatus in CalibrationAgent.JOB_STATUS_RUNNING:
          possibly_successful_jobs.append(workerID)  # FIXME: Currently unused
          # FIXME JOB_STATUS_ENDED contains also 'Done' and 'Completed' statuses. These don't mean that job were failed, right?
        elif jobStatus in CalibrationAgent.JOB_STATUS_ENDED:
          failed_jobs.append(workerID)
      failed_ratio = float(len(failed_jobs)) / float(targetNumberDict[calibrationID])
      if failed_ratio > CalibrationAgent.RESUBMISSION_THRESHOLD:
        # add workerids to result
        for workerID in failed_jobs:
          result.append((calibrationID, workerID))
    return result

  @staticmethod
  def __getWorkerIDFromJobName(jobname):
    """ Extracts the worker ID from the raw job name.

    :param basestring jobname: name of the job in the DIRAC DB
    :returns: the worker ID contained in the name string
    :rtype: int
    """
    return int(jobname.split('_')[4])

  @staticmethod
  def __getCalibrationIDFromJobName(jobname):
    """ Extracts the calibration ID from the raw job name.

    :param basestring jobname: name of the job in the DIRAC DB
    :returns: the calibration ID contained in the name string
    :rtype: int
    """
    return int(jobname.split('_')[2])


def _convert_to_int_list(non_int_list):
  """ Takes a list and converts each entry to an integer, returning this new list.

  :param list non_int_list: List that contains entries that may not be integers but can be cast
  :returns: List that only contains integers.
  :rtype: list
  """
  result = []
  for entry in non_int_list:
    result.append(int(entry))
  return result
