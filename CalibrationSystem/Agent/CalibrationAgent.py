"""
CalibrationAgent.

Supervises the state of the jobs started by the CalibrationService and requests resubmission if
too many failed.
"""
from collections import defaultdict

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Base.Client import Client
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.WorkloadManagementSystem.Client.WMSClient import WMSClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from ILCDIRAC.CalibrationSystem.Utilities.functions import convert_to_int_list

__RCSID__ = "$Id$"


class CalibrationAgent(AgentModule):
  """CalibrationAgent.

  Periodically checks the state of the jobs from the CalibrationService.
  If too few jobs are running, it tells the CalibrationService to restart the failed ones.
  """

  def initialize(self):
    """Initialize the Agent."""
    self.calibrationService = Client()
    self.calibrationService.setServer('Calibration/Calibration')
    self.currentCalibrations = []  # Contains IDs (int) of the calibrations
    self.currentJobStatusesPerWorker = {}  # Contains a mapping calibrationID -> dict, the dict contains a mapping
    self.currentJobStatusesPerJobId = {}
    self.calibrationOwnership = {}
    self.ops = Operations()
    # WorkerID (int) -> jobStatus (enum)
    return S_OK()

  def execute(self):
    """Execute one per cycle.

    Minimum time between two cycles is set via the PollingTime
    parameter (in seconds) in the ConfigTemplate.cfg
    """
    # TODO: Implement: call fetchJobStatuses, compare this to old status. If an entire calibrationRun finished,
    # remove it from data structures. If too many jobs failed, ask Service for resubmission. Then replace old
    # job status dict with new one
    # To clear up: Can a job disappear from this list? Or what happens if node crashes.
    res = self.fetchJobStatuses()
    if not res['OK']:
      return res
    self.currentJobStatusesPerWorker = res['Value']['jobStatusVsWorkerId']
    self.currentJobStatusesPerJobId = res['Value']['jobStatusVsJobId']
    self.calibrationOwnership = res['Value']['calibrationOwnership']
    self.checkForCalibrationsToBeKilled()
    res = self.calibrationService.getNumberOfJobsPerCalibration()
    if not res['OK']:
      return res
    targetJobNumbers = res['Value']
    # FIXME self.currentCalibrations is not used anywhere...
    self.currentCalibrations = list(targetJobNumbers.keys())

    res = self.calibrationService.getRunningCalibrations()
    if not res['OK']:
      return res
    runningCalibs = res['Value']  # list of unfinished calibrations
    currentJobStatusesPerWorker_runningCalibs = {}
    targetJobNumbers_runningCalibs = {}
    for iCalib in runningCalibs:
      try:
        currentJobStatusesPerWorker_runningCalibs[iCalib] = self.currentJobStatusesPerWorker[iCalib]
        targetJobNumbers_runningCalibs[iCalib] = targetJobNumbers[iCalib]
      except KeyError:
        errMsgConst = 'Error while retrieving information for calibration'
        errMsgVariable = '#%s' % iCalib
        self.log.error(errMsgConst, errMsgVariable)

    jobsToResubmitted = CalibrationAgent.__calculateJobsToBeResubmitted(
        currentJobStatusesPerWorker_runningCalibs, targetJobNumbers_runningCalibs)

    if jobsToResubmitted:
      res = self.requestResubmission(jobsToResubmitted)
      if not res['OK']:
        return res

    res = self.calibrationService.checkForStepIncrement()
    if not res['OK']:
      return res
    return S_OK()

  def fetchJobStatuses(self):
    """Request the statuses of jobs of all active calibrations.

    Output is mapped as calibrationID -> workerID -> jobStatus.

    :returns: Dictionary of type calibrationID -> dict, with dict of type workerID (int) -> jobStatus (enum)
    :rtype: dict
    """
    jobMonitoringService = JobMonitoringClient()
    jobIDs = []
    res = self.calibrationService.getActiveCalibrations()
    if not res['OK']:
      return res
    activeCalibrations = res['Value']
    self.log.debug("jobMonitoringService:", " %s" % jobMonitoringService)
    self.log.debug("self.calibrationService:", " %s" % self.calibrationService)
    self.log.debug("activeCalibrations:", " %s" % activeCalibrations)
    for iCalib in activeCalibrations:
      res = jobMonitoringService.getJobs({'JobGroup': 'PandoraCaloCalibration_calid_%s' % iCalib})
      if not res['OK']:
        self.log.error("Failed getting job IDs from job DB! Error:", res['Message'])
        return S_ERROR('Failed getting job IDs from job DB!')
      jobIDs += res['Value']
    self.log.debug('jobIDs:', ' %s' % jobIDs)
    res = jobMonitoringService.getJobsParameters(convert_to_int_list(
        jobIDs), ['JobName', 'Status', 'JobID', 'Owner', 'OwnerGroup', 'OwnerDN'])
    if not res['OK']:
      self.log.error("Cannot retrieve jobs parameters from job monitoring service", res['Message'])
      return res
    jobStatuses = res['Value']
    result1 = defaultdict(dict)  # defaults to {}
    result2 = defaultdict(dict)  # defaults to {}
    result3 = defaultdict(dict)  # defaults to {}
    for attrDict in jobStatuses.values():
      jobName = attrDict['JobName']
      curCalibration = CalibrationAgent.__getCalibrationIDFromJobName(jobName)
      result1[curCalibration].update({CalibrationAgent.__getWorkerIDFromJobName(jobName):
                                      attrDict['Status']})
      result2[curCalibration].update({attrDict['JobID']: attrDict['Status']})
      if curCalibration not in result3.keys():
        result3[curCalibration].update({'Owner': attrDict['Owner']})
        result3[curCalibration].update({'OwnerGroup': attrDict['OwnerGroup']})
        result3[curCalibration].update({'OwnerDN': attrDict['OwnerDN']})
    return S_OK({'jobStatusVsWorkerId': dict(result1), 'jobStatusVsJobId': dict(result2),
                 'calibrationOwnership': dict(result3)})

  def sendKillSignalToJobManager(self, jobIdsToKill, iCalibId):
    """Send kill signals to job manager service."""
    jobManagerService = WMSClient(useCertificates=True, delegatedDN=self.calibrationOwnership[iCalibId]['OwnerDN'],
                                  delegatedGroup=self.calibrationOwnership[iCalibId]['OwnerGroup'])
    res = jobManagerService.killJob(jobIdsToKill)
    return res

  def checkForCalibrationsToBeKilled(self):
    """Ask calibation service for jobs to be killed and kill them."""
    res = self.calibrationService.getCalibrationsToBeKilled()
    if not res['OK']:
      self.log.error('Failed to get list of calibrations to be killed from service. errMsg:', '%s' % res['Message'])
      return S_OK()
    calibIds = res['Value']
    if len(calibIds) == 0:
      return S_OK()
    else:
      for iCalibId in calibIds:
        if iCalibId not in self.currentJobStatusesPerJobId.keys():
          self.log.info('No jobs to kill for calibration', '#%s' % iCalibId)
        else:
          jobIdsToKill = self.currentJobStatusesPerJobId[iCalibId].keys()
          self.sendKillSignalToJobManager(jobIdsToKill, iCalibId)
          if not res['OK']:
            self.log.error('Failed to kill jobs. errMsg:', '%s' % res['Message'])
          else:
            self.log.info('Kill jobs which belong to calibrations:', '%s' % res['Value'])
    return S_OK()

  def requestResubmission(self, failedJobs):
    """Request the Service to resubmit the failed jobs.

    :param list failedJobs: List of 2-tuples ( calibrationID, workerID )
    :returns: None
    """
    jobs_to_resubmit = failedJobs
    number_of_tries = 0
    result = S_ERROR()
    self.resubmissionRetries = self.ops.getValue('Calibration/ResubmissionRetries', 5)  # TODO add this to CS
    while not result['OK'] and number_of_tries < self.resubmissionRetries:
      result = self.calibrationService.resubmitJobs(jobs_to_resubmit)
      number_of_tries += 1
      # FIXME: ResubmitJobs will probably be implemented in a way that would allow some resubmissions to fail and some
      # to work. Thus, this method would need a list of all resubmissions that have yet to be done, which is updated
      # in each iteration. once it is empty, the method returns. If it takes too long, RuntimeError is raised
      if result['OK']:
        return S_OK()
      #  else:
      #    jobs_to_resubmit = result['failed_pairs']
    raise RuntimeError('Cannot resubmit the necessary failed jobs. Problem: %s' % result)

  @staticmethod
  def __calculateJobsToBeResubmitted(jobStatusDict, targetNumberDict):
    """Return list of worker node ids in each calibration where job execution failed.

    :param dict jobStatusDict: Dictionary with a mapping from calibrationID -> dict, with dict having a mapping
                               workerID -> jobStatus
    :param dict targetNumberDict: Dictionary with a mapping from calibrationID -> number of jobs originally alotted
                                  to the calibration
    :returns: List containing 2-tuples ( calibrationID, workerID )
    :rtype: list
    """
    JOB_STATUS_FAILED = ['Failed', 'Killed', 'Stalled']
    failedJobs = []
    for calibrationID, workerDict in jobStatusDict.iteritems():
      for workerID, jobStatus in workerDict.iteritems():
        if jobStatus in JOB_STATUS_FAILED:
          failedJobs.append((calibrationID, workerID))
    return failedJobs

  @staticmethod
  def __getWorkerIDFromJobName(jobname):
    """Extract the worker ID from the raw job name.

    :param str jobname: name of the job in the DIRAC DB
    :returns: the worker ID contained in the name string
    :rtype: int
    """
    return int(jobname.split('_')[4])

  @staticmethod
  def __getCalibrationIDFromJobName(jobname):
    """Extract the calibration ID from the raw job name.

    :param str jobname: name of the job in the DIRAC DB
    :returns: the calibration ID contained in the name string
    :rtype: int
    """
    return int(jobname.split('_')[2])
