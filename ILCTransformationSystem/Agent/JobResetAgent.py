"""
documentation
"""

from collections import defaultdict

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.PrettyPrint import printTable
from DIRAC.Core.DISET.RPCClient import RPCClient

from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient

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
    self.enabled = False
    self.shifterProxy = 'DataManager'

    self.userJobTypes = ['User']
    self.prodJobTypes = ['MCGeneration', 'MCSimulation', 'MCReconstruction', 'MCReconstruction_Overlay', 'Split',
                         'MCSimulation_ILD', 'MCReconstruction_ILD', 'MCReconstruction_Overlay_ILD', 'Split_ILD',]

    self.addressTo = ["andre.philippe.sailer@cern.ch", "hamza.zafar@cern.ch"]
    self.addressFrom = "ilcdirac-admin@cern.ch"
    self.emailSubject = "JobResetAgent"

    self.accounting = defaultdict(list)
    self.errors = []

    self.nClient = NotificationClient()
    self.reqClient = ReqClient()
    self.jobMonClient = JobMonitoringClient()

    self.jobStateUpdateClient = RPCClient('WorkloadManagement/JobStateUpdate',
                                          useCertificates=False,
                                          timeout=10)

    self.jobManagerClient = RPCClient('WorkloadManagement/JobManager',
                                      useCertificates=False,
                                      timeout=10)


  def beginExecution(self):
    """ Reload the configurations before every cycle """
    self.enabled = self.am_getOption('EnableFlag', False)
    self.shifterProxy = self.am_setOption('shifterProxy', 'DataManager')

    self.addressTo = self.am_getOption('MailTo', ["andre.philippe.sailer@cern.ch", "hamza.zafar@cern.ch"])
    self.addressFrom = self.am_getOption('MailFrom', "ilcdirac-admin@cern.ch")

    self.accounting.clear()

    return S_OK()

  def sendNotification(self):
    """ sends email notification about jobs reset"""
    pass

  def logError(self, errStr, varMsg=''):
    self.log.error(errStr, varMsg)
    self.errors.append(errStr + varMsg)

  def getJobs(self, status, jobType=None, minorStatus=None):

    attrDict = dict(Status=status)
    if jobType:
      attrDict['JobType'] = jobType

    if minorStatus:
      attrDict['MinorStatus'] = minorStatus

    res = self.jobMonClient.getJobs(attrDict)
    if not res['OK']:
      self.logError("Failure to get Jobs", res['Message'])
      return res

    jobIDs = map(int, res['Value'])
    return S_OK(jobIDs)


  def treatUserJobWithNoRequest(self, jobID):
    self.log.notice("No request found for job: %s" % jobID)
    res = self.jobMonClient.getJobsMinorStatus(jobID)
    if not res['OK']:
      self.logError("Failure to get Minor Status", "Job IDs: %s, Message: %s" % (checkMinorStatList,
                                                                                 res['Message']))
    return res

    minorStatus = res['Value'][jobID]['MinorStatus']

    res = self.jobMonClient.getJobsApplicationStatus(checkAppStatList)
    if not res['OK']:
      self.logError("Failure to get Application Status", "Job IDs: %s, Message: %s" % (checkAppStatList,
                                                                                       res['Message']))
    return res

    appStatus = res['Value'][jobID]['ApplicationStatus']

    if minorStatus in FINAL_MINOR_STATES and appStatus in FINAL_APP_STATES:
      self.markJob(jobID, "Done")
    else:
      self.log.warn("Something not as expected for Job Status, please check: %s" % jobID)

    return S_OK()


  def treatUserJobWithRequest(self, jobID, request):
    if request.Status == "Done":
      self.log.notice("Request is Done: %s " % request)
      return self.markJob(jobID, "Done")

    self.log.notice("Request not Done: %s " % request)
    return self.resetRequest(request.RequestID)

  def treatFailedProdWithReq(self, jobID, request):
    if request.Status == 'Done':
      self.log.notice('Request is Done: %s ' % request)
      return self.markJob(jobID, "Failed")

    for op in request:
      lfns = "\n\t".join(lfn.LFN for lfn in op)
      self.log.notice('Operation for failed job: %s, %s, %s, %s\n\t%s' %
                      (request.RequestID, op.Type, op.Status, op.Error, lfns))

      if op.Type == "RemoveFile" and op.Status == 'Failed':
        filesToRemove=[lfn.LFN for lfn in op]
        #TODO: remove files using datamanager
        self.log.notice("Removing files %s"%filesToRemove)
        self.resetRequest(request.RequestID)
      elif op.Status == "Failed":
        self.log.notice("Can't handle operation of type: %s" % op.Type)

    return S_OK()


  def treatCompletedProdWithReq(self, jobID, request):
    if request.Status == "Done":
      self.log.notice("Request is Done: %s " % request )
      return self.markJob(jobID, "Done")

    for op in request:
      self.log.info("Operation for completed job: %s, %s, %s, %s" %
                   (request.RequestID, op.Type, op.Status, op.Error))
      if op.Type == 'ReplicateAndRegister' and op.Status == 'Failed':
        # Check if it failed because the file no longer exists
        for lfn in op:
          if lfn.Error == "No such file":
            return self.markJob(jobID, "Done")
        return self.resetRequest(requestID)

      elif op.Status == "Failed":
        self.log.notice("Cannot handle Operation of Type: %s " % op.Type )

    return S_OK()

  def checkJobs(self, jobIDs, treatJobWithNoReq, treatJobWithReq):
    """ docs... """

    res = self.reqClient.readRequestsForJobs(jobIDs)
    if not res['OK']:
      self.logError('Failure to read requests for jobs', res['Message'])
      return res

    result = res['Value']
    for jobID in jobIDs:
      if ((jobID not in result['Successful'] and jobID not in result['Failed']) or
         (jobID in result['Failed'] and result['Failed'][jobID]=='Request not found')):
        self.log.notice("No request found for job: %s" % jobID)
        treatJobWithNoReq(jobID)

      elif jobID in result['Successful']:
        self.log.notice("Found the request for Job: %s " % jobID)
        request = result['Successful'][jobID]
        treatJobWithReq(jobID, request)

    return S_OK("All Done")

  def resetRequests(self, requestID):
    """reset requests given the requestIDs list"""
    if not self.enabled:
      return S_OK()

    res = self.reqClient.resetFailedRequest(requestID, allR=True)
    if not res["OK"] or res['Value']=="Not reset":
      self.logError("Failed to reset request", "Request ID: %s, Message: %s"%(reqID, res['Message']))
      return res

    self.log.notice("Request %s is successfully reset" % reqID)
    return S_OK()

  def markJob(self, jobID, status, minorStatus="Requests Done", application="CompletedJobChecker"):
    """ docs here..."""

    self.log.notice("Marking job %s as %s" % (jobID, status))

    if not self.enabled:
      return S_OK()

    res = self.jobStateUpdateClient.setJobStatus(jobID, status, minorStatus, application)
    if not res["OK"]:
      self.logError("Failed to set mark", "Job: %s as %s"%(jobID, status))
      return res

    self.log.notice("Job %s is successfully maked as %s" % (jobID, status))
    return S_OK()


  def execute(self):
    """ main execution loop of Agent """
    pass
