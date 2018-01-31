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

  def getJobs(self, status, jobType=None):

    attrDict = dict(Status=status)
    if jobType:
      attrDict['JobType']=jobType

    res = self.jobMonClient.getJobs(attrDict)
    if not res['OK']:
      self.logError("Failure to get Jobs", res['Message'])
      return res

    jobIDs = map(int, res['Value'])
    return S_OK(jobIDs)

  def checkRequest(self, jobIDs):
    """ docs... """

    checkMinorStatList = []
    checkAppStatList = []
    markJobsDoneList = []
    resetRequestsList = []

    res = self.reqClient.readRequestsForJobs(jobIDs)

    if not res['OK']:
      self.logError('Failure to read requests for jobs', res['Message'])
      return res

    result = res['Value']
    for jobID in jobIDs:
      if ((jobID not in result['Successful'] and jobID not in result['Failed']) or
         (jobID in result['Failed'] and result['Failed'][jobID]=='Request not found')):
        self.log.notice("No request found for job: %s" % jobID)
        checkMinorStatList.append(jobID)

      elif jobID in result['Successful']:
        self.log.notice("Found the request for Job: %s " % jobID)
        request = result['Successful'][jobID]

        if request.Status == "Done":
          self.log.notice("Request is Done: %s " % request)
          self.log.notice("Setting job %s done" % jobID)
          markJobsDoneList.append(jobID)
        else:
          self.log.notice("Request not Done: %s " % request)
          self.log.notice("Resetting request for Job")
          resetRequestsList.append(request.RequestID)

    if checkMinorStatList:
      res = self.jobMonClient.getJobsMinorStatus(checkMinorStatList)
      if not res['OK']:
        self.logError("Failure to get Minor Status", "Job IDs: %s, Message: %s" % (checkMinorStatList,
                                                                                   res['Message']))
        return res

      for jobID in checkMinorStatList:
        if jobID in res['Value']:
          minorStatus = res['Value'][jobID]['MinorStatus']
          self.log.notice("Job %s has %s minor status" % (jobID, minorStatus))
          if minorStatus in FINAL_MINOR_STATES:
            checkAppStatList.append(jobID)
          else:
            self.log.warn("Minor Status for jobID %s is not in Final States, please check!" % jobID)

    if checkAppStatList:
      res = self.jobMonClient.getJobsApplicationStatus(checkAppStatList)
      if not res['OK']:
        self.logError("Failure to get Application Status", "Job IDs: %s, Message: %s" % (checkAppStatList,
                                                                                         res['Message']))
        return res

      for jobID in checkAppStatList:
        if jobID in res['Value']:
          appStatus = res['Value'][jobID]['ApplicationStatus']
          self.log.notice("Job %s has %s application status" % (jobID, appStatus))
          if appStatus in FINAL_APP_STATES:
            self.log.notice("Setting Job %s to Done" % jobID)
            markJobsDoneList.append(jobID)


    if markJobsDoneList:
      self.markJobs(markJobsDoneList, "Done")

    if resetRequestsList:
      self.resetRequests(resetRequestsList)

    return S_OK("All Done")

  def resetRequests(self, requestIDs):
    """reset requests given the requestIDs list"""
    if not self.enabled:
      return S_OK()

    for reqID in requestIDs:
      res = self.reqClient.resetFailedRequest(reqID, allR=True)
      if not res["OK"] or res['Value']=="Not reset":
        self.logError("Failed to reset request", "Request ID: %s, Message: %s"%(reqID, res['Message']))
        continue

      self.log.notice("Request %s is successfully reset" % reqID)

  def markJobs(self, jobIDs, status):
    """ docs here..."""
    if not self.enabled:
      return S_OK()

    for jobID in jobIDs:
      self.log.notice("Marking job %s as %s" % (jobID, status))
      res = self.jobStateUpdateClient.setJobStatus(jobID, status, "Requests Done", "CompletedJobChecker")
      if not res["OK"]:
        self.logError("Failed to set mark", "Job: %s as %s"%(jobID, status))
        continue

      self.log.notice("Job %s is successfully maked as %s" % (jobID, status))

  def execute(self):
    """ main execution loop of Agent """
    pass
