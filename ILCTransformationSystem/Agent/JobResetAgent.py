"""
Job Reset Agent takes care of the following cases

* Finds jobs stuck in staging status and reschedules them if associated files are already staged
* Finds production jobs in completed status:
  * Marks a Job Done if request status is Done
  * Marks a Job Done if no request is found
  * Marks a Job Done if ReplicateAndRegister operation has an error "No such file"
  * Resets Failed requests with ReplicateAndRegister operations if error is other than "No such file"
* Finds production jobs in failed status with minor status "Pending Requests"
  * Marks a Job Failed with minor status "Requests Done" if request status is Done
  * Marks a Job Failed with minor status "Requests Done" if no request is found
  * Resets Failed RemoveFile requests
* Finds user jobs in completed status:
  * Marks a Job Done if there is no request and minorStatus and appStatus are in final states
  * Marks a Job Done if request status is Done
  * Resets requests if the request status is other than Done
"""

from collections import defaultdict
from datetime import datetime, timedelta

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.PrettyPrint import printTable
from DIRAC.Core.DISET.RPCClient import RPCClient

from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.DataManagementSystem.Client.DataManager import DataManager

__RCSID__ = "$Id$"

AGENT_NAME = 'ILCTransformation/JobResetAgent'

FINAL_APP_STATES = ["Job Finished Successfully",
                    "Unknown"]

FINAL_MINOR_STATES = ["Pending Requests",
                      "Application Finished Successfully"]


class JobResetAgent(AgentModule):
  """ JobResetAgent """

  def __init__(self, *args, **kwargs):
    AgentModule.__init__(self, *args, **kwargs)
    self.name = 'JobResetAgent'
    self.enabled = True
    self.shifterProxy = 'DataManager'

    self.userJobTypes = ['User']
    self.prodJobTypes = ['MCGeneration', 'MCSimulation', 'MCReconstruction', 'MCReconstruction_Overlay', 'Split',
                         'MCSimulation_ILD', 'MCReconstruction_ILD', 'MCReconstruction_Overlay_ILD', 'Split_ILD']

    self.addressTo = ["andre.philippe.sailer@cern.ch", "hamza.zafar@cern.ch"]
    self.addressFrom = "ilcdirac-admin@cern.ch"
    self.emailSubject = "JobResetAgent"

    self.accounting = defaultdict(list)
    self.errors = []

    self.nClient = NotificationClient()
    self.reqClient = ReqClient()
    self.jobMonClient = JobMonitoringClient()
    self.dataManager = DataManager()
    self.jobDB = JobDB()

    self.jobStateUpdateClient = RPCClient('WorkloadManagement/JobStateUpdate',
                                          useCertificates=False,
                                          timeout=10)

    self.jobManagerClient = RPCClient('WorkloadManagement/JobManager',
                                      useCertificates=False,
                                      timeout=10)

  def beginExecution(self):
    """ Reload the configurations before every cycle """
    self.shifterProxy = self.am_setOption('shifterProxy', 'DataManager')

    self.enabled = self.am_getOption('EnableFlag', True)
    self.addressTo = self.am_getOption('MailTo', self.addressTo)
    self.addressFrom = self.am_getOption('MailFrom', self.addressFrom)

    self.accounting.clear()

    return S_OK()

  def sendNotification(self):
    """ sends email notification about job treatments """

    if not(self.errors or self.accounting):
      return S_OK()

    emailBody = ""
    rows = []
    for jobType, val in self.accounting.iteritems():
      emailBody += "\nTotal number of %s Jobs treated: %s\n\n" % (jobType, len(val))
      for job in val:
        rows.append([[str(job['JobID'])], [job['JobStatus']], [job['Treatment']]])

      if rows:
        columns = ["Job ID", "Job Status", "Treatment"]
        emailBody += printTable(columns, rows, printOut=False, numbering=False, columnSeparator=' | ')
        rows = []

    if self.errors:
      emailBody += "\n\nErrors:"
      emailBody += "\n".join(self.errors)

    self.log.notice(emailBody)
    for address in self.addressTo:
      res = self.nClient.sendMail(address, self.emailSubject, emailBody, self.addressFrom, localAttempt=False)
      if not res['OK']:
        self.log.error("Failure to send Email notification to ", address)
        continue

    self.errors = []
    self.accounting.clear()
    return S_OK()

  def logError(self, errStr, varMsg=''):
    self.log.error(errStr, varMsg)
    self.errors.append(errStr + varMsg)

  def getJobs(self, status, jobType=None, minorStatus=None):
    """ returns jobs with a given status, job type, minor status and
        lastUpdateTime older than 1 day """

    attrDict = dict(Status=status)
    if jobType:
      attrDict['JobType'] = jobType

    if minorStatus:
      attrDict['MinorStatus'] = minorStatus

    time = datetime.now() - timedelta(days=1)
    res = self.jobDB.selectJobs(attrDict, older=time)
    if not res['OK']:
      self.logError("Failure to get Jobs", res['Message'])
      return res

    jobIDs = map(int, res['Value'])
    return S_OK(jobIDs)

  def treatUserJobWithNoReq(self, jobID):
    """ treatment for user jobs in completed status which don't have a request in RMS """

    self.log.notice("No request found for job: %s" % jobID)
    res = self.jobMonClient.getJobsMinorStatus([jobID])
    if not res['OK']:
      self.logError("Failure to get Minor Status", "Job ID: %s, Message: %s" % (jobID, res['Message']))
      return res

    minorStatus = res['Value'][jobID]['MinorStatus']

    res = self.jobMonClient.getJobsApplicationStatus([jobID])
    if not res['OK']:
      self.logError("Failure to get Application Status", "Job ID: %s, Message: %s" % (jobID, res['Message']))
      return res

    appStatus = res['Value'][jobID]['ApplicationStatus']

    if minorStatus in FINAL_MINOR_STATES and appStatus in FINAL_APP_STATES:
      res = self.markJob(jobID, "Done")
      if res["OK"]:
        self.accounting["User"].append({"JobID": jobID, "JobStatus": "Completed", "Treatment": ("Job Marked Done "
                                        "because Minor Status: '%s' and Application Status: '%s' are in Final States" %
                                        (minorStatus, appStatus))})
      return res

    self.log.warn("Something not as expected for Job Status, please check: %s" % jobID)
    return S_OK()

  def treatUserJobWithReq(self, jobID, request):
    """ treatment for user jobs in completed status which have a request in RMS """

    if request.Status == "Done":
      self.log.notice("Request is Done: %s " % request)
      res = self.markJob(jobID, "Done")
      if res["OK"]:
        self.accounting["User"].append({"JobID": jobID, "JobStatus": "Completed", "Treatment": ("Job Marked Done "
                                        "because associated Request with ID: %s is Done" % request.RequestID)})
      return res

    self.log.notice("Request not Done: %s " % request)
    res = self.resetRequest(request.RequestID)
    if res["OK"]:
      self.accounting["User"].append({"JobID": jobID, "JobStatus": "Completed", "Treatment": ("Resetting request "
                                      "with ID: %s and Status: %s" % (request.RequestID, request.Status))})
    return res

  def treatFailedProdWithReq(self, jobID, request):
    """ treatment for production jobs in failed status which have a request in RMS """
    if request.Status == 'Done':
      self.log.notice('Request is Done: %s ' % request)
      res = self.markJob(jobID, "Failed")
      if res["OK"]:
        self.accounting["Production"].append({"JobID": jobID, "JobStatus": "Failed with 'Pending Requests'",
                                              "Treatment": ("Job Marked Failed because associated Request "
                                              "with ID: %s is Done" % request.RequestID)})
      return res

    for op in request:
      lfns = "\n\t".join(lfn.LFN for lfn in op)
      self.log.notice('Operation for failed job: %s, %s, %s, %s\n\t%s' %
                      (request.RequestID, op.Type, op.Status, op.Error, lfns))

      if op.Type == "RemoveFile" and op.Status == 'Failed':
        filesToRemove = [lfn.LFN for lfn in op]
        self.log.notice("Removing files %s" % filesToRemove)
        if self.enabled:
          res = self.dataManager.removeFile(filesToRemove, force=True)
          if not res["OK"]:
            self.logError("Failure to remove Files", ":%s Message: %s" % (filesToRemove, res["Message"]))

          res = self.resetRequest(request.RequestID)
          if res["OK"]:
            self.accounting["Production"].append({"JobID": jobID, "JobStatus": "Failed with 'Pending Requests'",
                                                  "Treatment": ("Resetting request with ID: %s and Status: %s"
                                                  % (request.RequestID, request.Status))})
          return res
      elif op.Status == "Failed":
        self.log.notice("Can't handle operation of type: %s" % op.Type)

    return S_OK()

  def treatFailedProdWithNoReq(self, jobID):
    """ treatment for production jobs in failed status which don't have a request in RMS """

    res = self.markJob(jobID, "Failed")
    if res["OK"]:
      self.accounting["Production"].append({"JobID": jobID, "JobStatus": "Failed with 'Pending Requests'",
                                            "Treatment": ("Job Marked Failed with minor status 'Requests Done' "
                                            "and application status 'CompletedJobChecker' because no associated "
                                            "request is found")})
    return res

  def treatCompletedProdWithReq(self, jobID, request):
    """ treatment for production jobs in completed status which have a request in RMS """

    if request.Status == "Done":
      self.log.notice("Request is Done: %s " % request)
      res = self.markJob(jobID, "Done")
      if res["OK"]:
        self.accounting["Production"].append({"JobID": jobID, "JobStatus": "Completed", "Treatment": ("Job Marked "
                                              "Done because associated Request with ID: %s is Done" %
                                              request.RequestID)})
      return res

    for op in request:
      self.log.info("Operation for completed job: %s, %s, %s, %s" %
                    (request.RequestID, op.Type, op.Status, op.Error))
      if op.Type == 'ReplicateAndRegister' and op.Status == 'Failed':
        # Check if it failed because the file no longer exists
        for lfn in op:
          if "No such file" in lfn.Error:
            res = self.markJob(jobID, "Done")
            if res["OK"]:
              self.accounting["Production"].append({"JobID": jobID, "JobStatus": "Completed", "Treatment": ("Job "
                                                    "Marked Done because no file is found for ReplicateAndRegister "
                                                    "operation")})
            return res
        res = self.resetRequest(request.RequestID)
        if res["OK"]:
          self.accounting["Production"].append({"JobID": jobID, "JobStatus": "Completed", "Treatment": ("Resetting "
                                                "request with ID: %s and Status: %s" % (request.RequestID,
                                                 request.Status))})
        return res

      elif op.Status == "Failed":
        self.log.notice("Cannot handle Operation Type: %s" % op.Type)

    return S_OK()

  def treatCompletedProdWithNoReq(self, jobID):
    """ marks the job done  """
    res = self.markJob(jobID, "Done")
    if res["OK"]:
      self.accounting["Completed"].append({"JobID": jobID, "JobStatus": "Completed", "Treatment": ("Job Marked "
                                           "Done because no associated request is found")})
    return res

  def checkJobs(self, jobIDs, treatJobWithNoReq, treatJobWithReq):
    """ executes treatment functions for jobs with and without requests """

    res = self.reqClient.readRequestsForJobs(jobIDs)
    if not res['OK']:
      self.logError('Failure to read requests for jobs', res['Message'])
      return res

    result = res['Value']
    for jobID in jobIDs:
      if ((jobID not in result['Successful'] and jobID not in result['Failed']) or (jobID in result['Failed'] \
          and 'Request not found' in result['Failed'][jobID])):
        self.log.notice("No request found for job: %s" % jobID)
        treatJobWithNoReq(jobID)

      elif jobID in result['Successful']:
        self.log.notice("Found the request for Job: %s " % jobID)
        request = result['Successful'][jobID]
        treatJobWithReq(jobID, request)

    return S_OK()

  def getStagedFiles(self, lfns):
    """ returns a list of staged files """
    if not lfns:
      self.log.notice("No LFNs passed to check staging status")
      return S_OK()

    voName = lfns[0].split('/')[1]
    se = StorageElement("CERN-SRM", vo=voName)
    res = se.getFileMetadata(lfns)
    if not res["OK"]:
      self.logError("Failure to getFileMetadata for LFNs", "%s" % lfns)
      return res

    stagedFiles = [lfn for lfn, val in res["Value"]["Successful"].iteritems() if val["Cached"] > 0]
    return S_OK(stagedFiles)

  @staticmethod
  def cleanLFN(lfn):
    """ remove prefix from lfn """
    if lfn.lower().startswith('lfn'):
      lfn = lfn[4:]
    return lfn

  def getInputDataForJobs(self, jobList):
    """ returns the input data for a given list of jobIDs """
    inputData = defaultdict(list)
    for jobID in jobList:
      res = self.jobMonClient.getInputData(jobID)
      if not res['OK']:
        self.logError("Failure to get input data for", "JobID: %s, Message: %s" % (jobID, res["Message"]))
        continue

      for lfn in res['Value']:
        lfn = self.cleanLFN(lfn)
        inputData[lfn].append(jobID)

    return S_OK(inputData)

  def rescheduleJobs(self, jobsToReschedule):
    """ resets a list of jobs """
    result = dict(Failed=[], Successful=[])
    for job in jobsToReschedule:
      res = self.jobManagerClient.resetJob(job)
      if res['OK']:
        result['Successful'].append(job)
      else:
        self.logError("Failed to reset job", "%s: %s" % (job, res['Message']))
        result['Failed'].append(job)

    self.log.info("Reset jobs: %s" % result)
    return S_OK(result)

  def checkStagingJobs(self, jobList):
    """ gets input data and stager status, then reschedules jobs whose
        associated files are already staged """

    res = self.getInputDataForJobs(jobList)
    inputData = res['Value']

    if not inputData:
      self.log.notice("No input data found for job list %s" % jobList)
      return S_OK()

    self.log.notice("Input Data found: %s" % inputData)
    res = self.getStagedFiles(inputData.keys())
    if not res['OK']:
      return res

    stagedFiles = res['Value']

    jobsToReschedule = set()
    for lfn in stagedFiles:
      jobsToReschedule.add(inputData[lfn])
      self.log.notice("Jobs to be rescheduled: %s" % jobsToReschedule)

      if self.enabled and jobsToReschedule:
        res = self.rescheduleJobs(jobsToReschedule)
        if res["OK"]:
          for jobID in res["Value"]["Successful"]:
            self.accounting["Staging"].append({"JobID": jobID, "JobStatus": "Staging", "Treatment": (
                                               "Job Rescheduled because associated files are already Staged")})

    return S_OK()

  def resetRequest(self, requestID):
    """ resets failed requests for a given requestID """
    if not self.enabled:
      return S_OK()

    res = self.reqClient.resetFailedRequest(requestID, allR=True)
    if not res["OK"]:
      self.logError("Failed to reset request", "Request ID: %s, Message: %s" % (requestID, res['Message']))
      return res

    if res["Value"] == "Not reset":
      self.logError("Failed to reset request", "Request ID: %s" % (requestID))
      return S_ERROR()

    self.log.notice("Request %s is successfully reset" % requestID)
    return S_OK()

  def markJob(self, jobID, status, minorStatus="Requests Done", application="CompletedJobChecker"):
    """ marks a job with given status, minorStatus and application """

    self.log.notice("Marking job %s as %s" % (jobID, status))

    if not self.enabled:
      return S_OK()

    res = self.jobStateUpdateClient.setJobStatus(jobID, status, minorStatus, application)
    if not res["OK"]:
      self.logError("Failed to mark ", "Job: %s as %s" % (jobID, status))
      return res

    self.log.notice("Job %s is successfully maked as %s" % (jobID, status))
    return S_OK()

  def execute(self):
    """ main execution loop of Agent """

    # process completed prod jobs
    res = self.getJobs(status="Completed", jobType=self.prodJobTypes)
    if res["OK"]:
      completedJobIDs = res["Value"]
      if completedJobIDs:
        self.checkJobs(jobIDs=completedJobIDs,
                       treatJobWithNoReq=self.treatCompletedProdWithNoReq,
                       treatJobWithReq=self.treatCompletedProdWithReq)
      else:
        self.log.notice("No production jobs found with Completed status")

    # process failed prod jobs
    res = self.getJobs(status="Failed", jobType=self.prodJobTypes, minorStatus="Pending Requests")
    if res["OK"]:
      failedJobIDs = res["Value"]
      if failedJobIDs:
        self.checkJobs(jobIDs=failedJobIDs,
                       treatJobWithNoReq=self.treatFailedProdWithNoReq,
                       treatJobWithReq=self.treatFailedProdWithReq)
      else:
        self.log.notice("No production jobs found with Failed status and pending requests")

    # process completed user jobs
    res = self.getJobs(status="Completed", jobType=self.userJobTypes)
    if res["OK"]:
      completedUserJobs = res["Value"]
      if completedUserJobs:
        self.checkJobs(jobIDs=completedUserJobs,
                       treatJobWithNoReq=self.treatUserJobWithNoReq,
                       treatJobWithReq=self.treatUserJobWithReq)
      else:
        self.log.notice("No user jobs found with Completed status")

    # process STAGING jobs
    res = self.getJobs(status="Staging")
    if res["OK"]:
      stagingJobs = res["Value"]
      if stagingJobs:
        self.checkStagingJobs(stagingJobs)
      else:
        self.log.notice("No staging jobs found")

    # send email notification
    self.sendNotification()

    return S_OK()
