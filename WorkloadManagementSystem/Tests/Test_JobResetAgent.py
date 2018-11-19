"""Tests for JobResetAgent."""
# pylint: disable=protected-access
from contextlib import contextmanager
from datetime import datetime, timedelta

import pytest
from mock import MagicMock, call

import ILCDIRAC.WorkloadManagementSystem.Agent.JobResetAgent as JRA
from ILCDIRAC.WorkloadManagementSystem.Agent.JobResetAgent import JobResetAgent

import DIRAC.Resources.Storage.StorageElement as SeModule
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation

from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger
import DIRAC

__RCSID__ = "$Id$"


@pytest.fixture
def userProxyFixture(mocker):
  """Mock UserProxy."""
  @contextmanager
  def _mockedCM(*args, **kwargs):
    try:
      yield S_OK()
    finally:
      pass
  mocker.patch('ILCDIRAC.WorkloadManagementSystem.Agent.JobResetAgent.UserProxy', new=_mockedCM)


@pytest.fixture
def failingUserProxyFixture(mocker):
  """Mock UserProxy."""
  @contextmanager
  def _mockedCM(*args, **kwargs):
    try:
      yield S_ERROR("Failed to set up proxy")
    finally:
      pass
  mocker.patch('ILCDIRAC.WorkloadManagementSystem.Agent.JobResetAgent.UserProxy', new=_mockedCM)


@pytest.fixture
def fakeJobID():
  """Return fake job ID."""
  return 1


@pytest.fixture
def alreadyErrorMessage():
  """Return error message."""
  return "File already registered with alternative metadata"


@pytest.fixture
def today():
  """Return a day."""
  return datetime(2018, 12, 25, 0, 0, 0, 0)


@pytest.fixture
def jobResetAgent(today):
  """Fixture for jobResetAgent."""
  agent = JRA
  agent.AgentModule = MagicMock()
  agent.JobMonitoringClient = MagicMock()
  agent.DataManager = MagicMock(spec=DIRAC.DataManagementSystem.Client.DataManager.DataManager)
  agent.ReqClient = MagicMock(spec=DIRAC.RequestManagementSystem.Client.ReqClient.ReqClient)
  agent.NotificationClient = MagicMock(spec=DIRAC.FrameworkSystem.Client.NotificationClient.NotificationClient)
  agent.datetime = MagicMock()
  agent.datetime.now.return_value = today

  theAgent = JobResetAgent()
  theAgent.log = gLogger
  theAgent.enabled = True
  theAgent._fcClient = MagicMock(spec=DIRAC.Resources.Catalog.FileCatalogFactory.FileCatalogFactory)
  theAgent._fcClient.removeFile = MagicMock(return_value=S_ERROR('failed'))
  theAgent.jobManagerClient = MagicMock()
  theAgent.jobStateUpdateClient = MagicMock()
  theAgent._jobDB = MagicMock()

  return theAgent


def createRequest(requestID, opType, opStatus, fileStatus, lfnError=" ",
                  lfn="/ilc/fake/lfn"):
  """Create a request."""
  req = Request({"RequestID": requestID})
  op = Operation({"Type": opType, "Status": opStatus})
  op.addFile(File({"LFN": lfn, "Status": fileStatus, "Error": lfnError}))
  req.addOperation(op)
  return req


@pytest.fixture
def doneRemoveRequest():
  """Return done Remove Request."""
  return createRequest(requestID=1, opType="RemoveFile",
                       opStatus="Done", fileStatus="Done")


@pytest.fixture
def doneReplicateRequest():
  """Return done Replicate Request."""
  return createRequest(requestID=2, opType="ReplicateAndRegister",
                       opStatus="Done", fileStatus="Done")


@pytest.fixture
def failedReplicateRequest():
  """Return Failed Replicate Request."""
  return createRequest(requestID=3, opType="ReplicateAndRegister",
                       opStatus="Failed", fileStatus="Failed")


@pytest.fixture
def failedRemoveRequest():
  """Return Failed RemoveRequest Request."""
  return createRequest(requestID=4, opType="RemoveFile",
                       opStatus="Failed", fileStatus="Failed")


def test_init(jobResetAgent):
  """Test the constructor."""
  assert isinstance(jobResetAgent, JobResetAgent)
  assert isinstance(jobResetAgent.jobMonClient, MagicMock)
  assert isinstance(jobResetAgent.dataManager, MagicMock)
  assert isinstance(jobResetAgent.reqClient, MagicMock)
  assert isinstance(jobResetAgent.nClient, MagicMock)
  assert jobResetAgent.enabled
  assert jobResetAgent.addressFrom == "ilcdirac-admin@cern.ch"
  assert jobResetAgent.userJobTypes == ['User']
  assert jobResetAgent.prodJobTypes == ['MCGeneration', 'MCSimulation', 'MCReconstruction',
                                        'MCReconstruction_Overlay', 'Split', 'MCSimulation_ILD',
                                        'MCReconstruction_ILD', 'MCReconstruction_Overlay_ILD',
                                        'Split_ILD']


def test_begin_execution(jobResetAgent):
  """Test for beginExecution function."""
  jobResetAgent.accounting["Junk"].append("Funk")
  jobResetAgent.am_setOption = MagicMock()
  jobResetAgent.am_getOption = MagicMock()
  getOptionCalls = [call('EnableFlag', False),
                    call('MailTo', jobResetAgent.addressTo),
                    call('MailFrom', jobResetAgent.addressFrom),
                    call('UserJobs', jobResetAgent.userJobTypes),
                    call('ProdJobs', jobResetAgent.prodJobTypes)]

  jobResetAgent.beginExecution()
  jobResetAgent.am_getOption.assert_has_calls(getOptionCalls)
  # accounting dictionary should be cleared
  assert jobResetAgent.accounting == {}


def test_get_jobs(jobResetAgent, today):
  """Test for getJobs function."""
  jobStatus = "Done"
  jobType = "User"
  minorStatus = "Requests Done"
  attrDict = {"JobType": jobType,
              "MinorStatus": minorStatus,
              "Status": jobStatus}

  jobResetAgent.jobDB.selectJobs.return_value = S_ERROR()
  res = jobResetAgent.getJobs(jobStatus, jobType, minorStatus)
  assert not res["OK"]

  jobResetAgent.jobDB.selectJobs.reset_mock()
  jobResetAgent.jobDB.selectJobs.return_value = S_OK(["1", "2", "3"])
  res = jobResetAgent.getJobs(jobStatus, jobType, minorStatus)
  assert res["Value"] == [1, 2, 3]
  jobResetAgent.jobDB.selectJobs.assert_called_once_with(attrDict, older=today - timedelta(days=1))


def test_treat_User_Job_With_No_Req(jobResetAgent, fakeJobID):
  """Test for treatUserJobWithNoReq function."""
  jobResetAgent.markJob = MagicMock()

  # case if getJobsMinorStatus function returns an error
  jobResetAgent.jobMonClient.getJobsMinorStatus.return_value = S_ERROR()
  res = jobResetAgent.treatUserJobWithNoReq(fakeJobID)
  assert not res["OK"]
  jobResetAgent.jobMonClient.getJobsMinorStatus.called_once_with([fakeJobID])

  # case if getJobsMinorStatus executes successfully but getJobsApplicationStatus returns an error
  jobResetAgent.jobMonClient.getJobsMinorStatus.return_value = S_OK({fakeJobID: {'MinorStatus':
                                                                                 JRA.FINAL_MINOR_STATES[0],
                                                                                 'JobID': fakeJobID}})
  jobResetAgent.jobMonClient.getJobsApplicationStatus.return_value = S_ERROR()
  res = jobResetAgent.treatUserJobWithNoReq(fakeJobID)
  assert not res["OK"]

  # mark job done if ApplicationStatus and MinorStatus are in Final States
  jobResetAgent.jobMonClient.getJobsApplicationStatus.return_value = S_OK({fakeJobID:
                                                                           {'ApplicationStatus':
                                                                            JRA.FINAL_APP_STATES[0],
                                                                            'JobID': fakeJobID}})
  res = jobResetAgent.treatUserJobWithNoReq(fakeJobID)
  assert res["OK"]
  jobResetAgent.markJob.assert_called_once_with(fakeJobID, "Done")

  # dont do anything if ApplicationStatus and MinorStatus are not in Final States
  jobResetAgent.markJob.reset_mock()
  jobResetAgent.jobMonClient.getJobsMinorStatus.return_value = S_OK({fakeJobID:
                                                                     {'MinorStatus': 'other status',
                                                                      'JobID': fakeJobID}})
  jobResetAgent.jobMonClient.getJobsApplicationStatus.return_value = S_OK({fakeJobID:
                                                                           {'ApplicationStatus': 'other status',
                                                                            'JobID': fakeJobID}})
  res = jobResetAgent.treatUserJobWithNoReq(fakeJobID)
  assert res["OK"]
  jobResetAgent.markJob.assert_not_called()


def test_treat_User_Job_With_Req(jobResetAgent, fakeJobID):
  """Test for treatUserJobWithReq function."""
  doneRequest = createRequest(requestID=1, opType="RemoveFile", opStatus="Done", fileStatus="Done")
  failedRequestID = 2
  failedRequest = createRequest(requestID=failedRequestID, opType="RemoveFile", opStatus="Failed",
                                fileStatus="Failed")
  jobResetAgent.resetRequest = MagicMock()
  jobResetAgent.markJob = MagicMock()

  # if request status is 'Done' then job should also be marked 'Done'
  jobResetAgent.treatUserJobWithReq(fakeJobID, doneRequest)
  jobResetAgent.markJob.assert_called_once_with(fakeJobID, "Done")
  jobResetAgent.resetRequest.assert_not_called()

  # if request status is not 'Done' then reset request
  jobResetAgent.markJob.reset_mock()
  jobResetAgent.treatUserJobWithReq(fakeJobID, failedRequest)
  jobResetAgent.markJob.assert_not_called()
  jobResetAgent.resetRequest.assert_called_once_with(failedRequestID)

  # if request is waiting
  jobResetAgent.markJob.reset_mock()
  jobResetAgent.resetRequest.reset_mock()
  alreadyRegisteredRequest = createRequest(requestID=4, opType="RegisterFile",
                                           opStatus="Waiting", fileStatus="Waiting")
  assert jobResetAgent.treatUserJobWithReq(fakeJobID, alreadyRegisteredRequest)['OK']
  jobResetAgent.markJob.assert_not_called()
  jobResetAgent.resetRequest.assert_not_called()


def test_treat_Failed_Prod_With_Req(jobResetAgent, doneRemoveRequest, doneReplicateRequest,
                                    failedRemoveRequest, failedReplicateRequest):
  """Test for treatFailedProdWithReq function."""
  jobResetAgent.markJob = MagicMock()
  jobResetAgent.resetRequest = MagicMock()
  jobResetAgent.dataManager.removeFile.reset_mock()

  # if request is done then job should be marked failed
  jobResetAgent.treatFailedProdWithReq(fakeJobID, doneRemoveRequest)
  jobResetAgent.markJob.assert_called_once_with(fakeJobID, "Failed")

  jobResetAgent.markJob.reset_mock()
  jobResetAgent.treatFailedProdWithReq(fakeJobID, doneReplicateRequest)
  jobResetAgent.markJob.assert_called_once_with(fakeJobID, "Failed")

  # failed requests with removeFile operation should be reset
  jobResetAgent.markJob.reset_mock()
  jobResetAgent.treatFailedProdWithReq(fakeJobID, failedRemoveRequest)
  fileLfn = failedRemoveRequest[0][0].LFN
  jobResetAgent.dataManager.removeFile.assert_called_once_with([fileLfn], force=True)
  jobResetAgent.resetRequest.assert_called_once_with(getattr(failedRemoveRequest, "RequestID"))
  jobResetAgent.markJob.asset_not_called()

  # failed requests with operations other than removeFile should not be reset
  jobResetAgent.resetRequest.reset_mock()
  jobResetAgent.dataManager.reset_mock()
  jobResetAgent.treatFailedProdWithReq(fakeJobID, failedReplicateRequest)
  jobResetAgent.dataManager.assert_not_called()
  jobResetAgent.resetRequest.assert_not_called()
  jobResetAgent.markJob.asset_not_called()


def test_treat_Failed_Prod_With_No_Req(jobResetAgent, fakeJobID):
  """Test for treatFailedProdWithNoReq function."""
  jobResetAgent.markJob = MagicMock()
  jobResetAgent.treatFailedProdWithNoReq(fakeJobID)
  jobResetAgent.markJob.assert_called_once_with(fakeJobID, "Failed")


def test_treat_Completed_Prod_With_Req(jobResetAgent, fakeJobID, doneRemoveRequest, doneReplicateRequest,
                                       failedRemoveRequest, failedReplicateRequest):
  """Test for treatCompletedProdWithReq function."""
  jobResetAgent.markJob = MagicMock()
  jobResetAgent.resetRequest = MagicMock()

  # if request is done then job should be marked Done
  jobResetAgent.treatCompletedProdWithReq(fakeJobID, doneRemoveRequest)
  jobResetAgent.markJob.assert_called_once_with(fakeJobID, "Done")

  jobResetAgent.markJob.reset_mock()
  jobResetAgent.treatCompletedProdWithReq(fakeJobID, doneReplicateRequest)
  jobResetAgent.markJob.assert_called_once_with(fakeJobID, "Done")

  # job with failed ReplicateAndRegister operation should be marked done if file does not exist
  jobResetAgent.markJob.reset_mock()
  jobResetAgent.markJob = MagicMock(return_value=S_OK())
  request = createRequest(requestID=1, opType="RemoveFile", opStatus="Done",
                          fileStatus="Done", lfnError="No such file")
  jobResetAgent.treatCompletedProdWithReq(fakeJobID, request)
  jobResetAgent.markJob.assert_called_once_with(fakeJobID, "Done")

  # failed requests with ReplicateAndRegister operation should be reset
  jobResetAgent.markJob.reset_mock()
  jobResetAgent.treatCompletedProdWithReq(fakeJobID, failedReplicateRequest)
  jobResetAgent.resetRequest.assert_called_once_with(getattr(failedReplicateRequest, "RequestID"))

  # failed Remove file request should not be reset
  jobResetAgent.resetRequest.reset_mock()
  jobResetAgent.treatCompletedProdWithReq(fakeJobID, failedRemoveRequest)
  jobResetAgent.markJob.assert_not_called()
  jobResetAgent.resetRequest.assert_not_called()

  # is waiting
  jobResetAgent.markJob.reset_mock()
  jobResetAgent.resetRequest.reset_mock()
  alreadyRegisteredRequest = createRequest(requestID=4, opType="RegisterFile",
                                           opStatus="Waiting", fileStatus="Waiting")
  assert jobResetAgent.treatCompletedProdWithReq(fakeJobID, alreadyRegisteredRequest)['OK']
  jobResetAgent.markJob.assert_not_called()
  jobResetAgent.resetRequest.assert_not_called()

  # request failing because no such file, mark it done
  jobResetAgent.accounting.clear()
  request = createRequest(requestID=1, opType="ReplicateAndRegister", opStatus="Failed",
                          fileStatus="Failed", lfnError="No such file", lfn="/ilc/fake/file")
  jobResetAgent.markJob = MagicMock(return_value=S_OK())
  jobResetAgent.treatCompletedProdWithReq(1234, request)
  jobResetAgent.markJob.assert_called_once_with(1234, "Done")
  assert len(jobResetAgent.accounting["Production"]) == 1
  assert jobResetAgent.accounting["Production"][0]["JobID"] == 1234


def test_treat_Completed_Prod_With_No_Req(jobResetAgent, fakeJobID):
  """Test for treatCompletedProdWithNoReq function."""
  jobResetAgent.markJob = MagicMock()
  jobResetAgent.treatCompletedProdWithNoReq(fakeJobID)
  jobResetAgent.markJob.assert_called_once_with(fakeJobID, "Done")


def test_check_jobs(jobResetAgent):
  """Test for checkJobs function."""
  jobIDs = [1, 2]
  dummy_treatJobWithNoReq = MagicMock()
  dummy_treatJobWithReq = MagicMock()

  # if the readRequestsForJobs func returns error than checkJobs should exit and return an error
  jobResetAgent.reqClient.readRequestsForJobs.return_value = S_ERROR()
  res = jobResetAgent.checkJobs(jobIDs, treatJobWithNoReq=dummy_treatJobWithNoReq,
                                treatJobWithReq=dummy_treatJobWithReq)
  assert not res["OK"]

  # test if correct treatment functions are called
  jobResetAgent.reqClient.readRequestsForJobs.return_value = S_OK({'Successful': {},
                                                                   'Failed': {jobIDs[0]: 'Request not found'}})
  jobResetAgent.checkJobs(jobIDs, treatJobWithNoReq=dummy_treatJobWithNoReq,
                          treatJobWithReq=dummy_treatJobWithReq)
  dummy_treatJobWithNoReq.assert_has_calls([call(jobIDs[0]), call(jobIDs[1])])
  dummy_treatJobWithReq.assert_not_called()

  dummy_treatJobWithNoReq.reset_mock()
  req1 = Request({"RequestID": 1})
  req2 = Request({"RequestID": 2})
  jobResetAgent.reqClient.readRequestsForJobs.return_value = S_OK({'Successful': {jobIDs[0]: req1,
                                                                                  jobIDs[1]: req2},
                                                                   'Failed': {}})
  jobResetAgent.checkJobs(jobIDs, treatJobWithNoReq=dummy_treatJobWithNoReq,
                          treatJobWithReq=dummy_treatJobWithReq)
  dummy_treatJobWithNoReq.assert_not_called()
  dummy_treatJobWithReq.assert_has_calls([call(jobIDs[0], req1), call(jobIDs[1], req2)])


def test_get_staged_files(jobResetAgent):
  """Test for getStagedFiles function."""
  stagedFile = "/ilc/fake/lfn1/staged"
  nonStagedFile = "/ilc/fake/lfn2/nonStaged"
  lfns = [stagedFile, nonStagedFile]

  res = jobResetAgent.getStagedFiles([])
  assert res["OK"]

  SeModule.StorageElementItem.getFileMetadata = MagicMock(return_value=S_ERROR())
  res = jobResetAgent.getStagedFiles(lfns)
  assert not res["OK"]

  SeModule.StorageElementItem.getFileMetadata.return_value = S_OK({'Successful': {stagedFile: {'Cached': 1},
                                                                                  nonStagedFile: {'Cached': 0}}})
  res = jobResetAgent.getStagedFiles(lfns)
  assert res["Value"] == [stagedFile]


def test_get_input_data_for_jobs(jobResetAgent):
  """Test for getInputDataForJobs function."""
  jobIDs = [1, 2]
  lfn1 = "lfn:/ilc/fake/lfn1"
  lfn2 = "/ilc/fake/lfn2"
  jobResetAgent.jobMonClient.getInputData.return_value = S_ERROR()

  res = jobResetAgent.getInputDataForJobs(jobIDs)
  assert res["Value"] == {}

  jobResetAgent.jobMonClient.getInputData.return_value = S_OK([lfn1, lfn2])
  res = jobResetAgent.getInputDataForJobs(jobIDs)
  assert res["Value"] == {lfn1[4:]: jobIDs, lfn2: jobIDs}


def test_reschedule_jobs(jobResetAgent):
  """Test for rescheduleJobs function."""
  jobShouldFailToReset = 1
  jobShouldSuccessfullyReset = 2
  jobsToReschedule = [jobShouldFailToReset, jobShouldSuccessfullyReset]

  jobResetAgent.jobManagerClient.resetJob.side_effect = [S_ERROR(), S_OK()]
  res = jobResetAgent.rescheduleJobs(jobsToReschedule)
  assert res['OK']
  assert res["Value"]["Successful"] == [jobShouldSuccessfullyReset]
  assert res["Value"]["Failed"] == [jobShouldFailToReset]


def test_check_staging_jobs(jobResetAgent):
  """Test for checkStagingJobs function."""
  jobShouldBeRescheduled = 1
  jobShouldNotBeResecheduled = 2
  stagedFile = "/ilc/file/staged"
  notStagedFile = "/ilc/file/notStaged"
  jobIDs = [jobShouldBeRescheduled, jobShouldNotBeResecheduled]

  jobResetAgent.getInputDataForJobs = MagicMock()
  jobResetAgent.getStagedFiles = MagicMock()
  jobResetAgent.rescheduleJobs = MagicMock()

  jobResetAgent.getInputDataForJobs.return_value = S_OK({})
  res = jobResetAgent.checkStagingJobs(jobIDs)
  assert res['OK']
  jobResetAgent.getInputDataForJobs.assert_called_once_with(jobIDs)
  jobResetAgent.getStagedFiles.assert_not_called()

  jobsToReschedule = set()
  jobsToReschedule.add(jobShouldBeRescheduled)
  jobResetAgent.getInputDataForJobs.reset_mock()
  jobResetAgent.getInputDataForJobs.return_value = S_OK({stagedFile: jobShouldBeRescheduled,
                                                         notStagedFile: jobShouldNotBeResecheduled})
  jobResetAgent.getStagedFiles.return_value = S_OK([stagedFile])
  jobResetAgent.checkStagingJobs(jobIDs)
  jobResetAgent.rescheduleJobs.assert_called_once_with(jobsToReschedule)


def test_reset_request(jobResetAgent):
  """Test for resetRequest function."""
  fakeReqID = 1
  jobResetAgent.logError = MagicMock()
  jobResetAgent.reqClient.resetFailedRequest.return_value = S_ERROR()
  res = jobResetAgent.resetRequest(fakeReqID)
  jobResetAgent.logError.assert_called()
  assert not res["OK"]

  jobResetAgent.logError.reset_mock()
  jobResetAgent.reqClient.resetFailedRequest.return_value = S_OK("Not reset")
  res = jobResetAgent.resetRequest(fakeReqID)
  assert not res["OK"]
  jobResetAgent.logError.assert_called()

  jobResetAgent.logError.reset_mock()
  jobResetAgent.reqClient.resetFailedRequest.return_value = S_OK()
  res = jobResetAgent.resetRequest(fakeReqID)
  assert res['OK']
  jobResetAgent.logError.assert_not_called()


def test_mark_job(jobResetAgent):
  """Test for markJob function."""
  fakeMinorStatus = "fakeMinorStatus"
  fakeApp = "fakeApp"
  fakeJobStatus = "Done"
  defaultMinorStatus = "Requests Done"
  defaultApplication = "CompletedJobChecker"

  # default minorStatus should be "Requests Done" and application should be "CompletedJobChecker"
  jobResetAgent.jobStateUpdateClient.setJobStatus = MagicMock(return_value=S_ERROR())
  res = jobResetAgent.markJob(fakeJobID, fakeJobStatus)
  assert not res["OK"]
  jobResetAgent.jobStateUpdateClient.setJobStatus.assert_called_once_with(fakeJobID,
                                                                          fakeJobStatus,
                                                                          defaultMinorStatus,
                                                                          defaultApplication)

  jobResetAgent.jobStateUpdateClient.setJobStatus.reset_mock()
  jobResetAgent.jobStateUpdateClient.setJobStatus.return_value = S_OK()
  res = jobResetAgent.markJob(fakeJobID, fakeJobStatus, minorStatus=fakeMinorStatus, application=fakeApp)
  assert res['OK']
  jobResetAgent.jobStateUpdateClient.setJobStatus.assert_called_once_with(fakeJobID, fakeJobStatus,
                                                                          fakeMinorStatus, fakeApp)


def test_execute(jobResetAgent):
  """Test for execute function."""
  jobIDs = [1, 2]
  jobResetAgent.getJobs = MagicMock()
  jobResetAgent.checkJobs = MagicMock()
  jobResetAgent.checkStagingJobs = MagicMock()

  jobResetAgent.getJobs.return_value = S_OK(jobIDs)
  jobResetAgent.execute()
  # check if checkJobs function is called with correct arguments
  completedProdJobCall = call(jobIDs=jobIDs, treatJobWithNoReq=jobResetAgent.treatCompletedProdWithNoReq,
                              treatJobWithReq=jobResetAgent.treatCompletedProdWithReq)
  failedProdJobCall = call(jobIDs=jobIDs, treatJobWithNoReq=jobResetAgent.treatFailedProdWithNoReq,
                           treatJobWithReq=jobResetAgent.treatFailedProdWithReq)
  completedUserJob = call(jobIDs=jobIDs, treatJobWithNoReq=jobResetAgent.treatUserJobWithNoReq,
                          treatJobWithReq=jobResetAgent.treatUserJobWithReq)
  calls = [completedProdJobCall, failedProdJobCall, completedUserJob]
  jobResetAgent.checkJobs.assert_has_calls(calls)
  jobResetAgent.checkStagingJobs.assert_called_once_with(jobIDs)


def test_send_notification(jobResetAgent):
  """Test for sendNotification function."""
  jobResetAgent.errors = []
  jobResetAgent.accounting = {}

  # send mail should not be called if there are no errors and accounting information
  jobResetAgent.sendNotification()
  jobResetAgent.nClient.sendMail.assert_not_called()

  # send mail should be called if there are errors but no accounting information
  jobResetAgent.errors = ["some error"]
  jobResetAgent.sendNotification()
  jobResetAgent.nClient.sendMail.assert_called()

  # send email should be called if there is accounting information but no errors
  jobResetAgent.nClient.sendMail.reset_mock()
  jobResetAgent.errors = []
  jobResetAgent.accounting = {"User": [{"JobID": 123, "JobStatus": "Failed", "Treatment": "reset request"}],
                              "Prod": [{"JobID": 124, "JobStatus": "Failed", "Treatment": "reset request"}]}
  jobResetAgent.sendNotification()
  jobResetAgent.nClient.sendMail.assert_called()

  # try sending email to all addresses even if we get error for sending email to some address
  jobResetAgent.nClient.sendMail.reset_mock()
  jobResetAgent.errors = ["some error"]
  jobResetAgent.addressTo = ["name1@cern.ch", "name2@cern.ch"]
  jobResetAgent.nClient.sendMail.return_value = S_ERROR()
  jobResetAgent.sendNotification()
  assert len(jobResetAgent.nClient.sendMail.mock_calls) == len(jobResetAgent.addressTo)

  # accounting dict and errors list should be cleared after notification is sent
  assert jobResetAgent.accounting == {}
  assert jobResetAgent.errors == []


def test_treat_User_Job_With_RegisterFile(userProxyFixture, jobResetAgent, fakeJobID, alreadyErrorMessage):
  """Test user jobs with RegisterFile request operations."""
  jobResetAgent.markJob = MagicMock()
  jobResetAgent.resetRequest = MagicMock()

  # if Already registered file request
  jobResetAgent.markJob.reset_mock()
  jobResetAgent.resetRequest.reset_mock()
  jobResetAgent._fcClient.removeFile.return_value = S_OK(dict(Failed={},
                                                              Successful={'/ilc/fake/lfn': True}))
  alreadyRegisteredRequest = createRequest(requestID=4, opType="RegisterFile",
                                           opStatus="Failed", fileStatus="Failed",
                                           lfnError=alreadyErrorMessage)
  jobResetAgent.treatUserJobWithReq(fakeJobID, alreadyRegisteredRequest)
  jobResetAgent.markJob.assert_not_called()
  jobResetAgent.resetRequest.assert_called_once_with(4)
  jobResetAgent._fcClient.removeFile.assert_called_once_with('/ilc/fake/lfn')

  # request is waiting
  jobResetAgent.markJob.reset_mock()
  jobResetAgent.resetRequest.reset_mock()
  jobResetAgent._fcClient.removeFile.reset_mock()
  alreadyRegisteredRequest = createRequest(requestID=4, opType="RegisterFile",
                                           opStatus="Waiting", fileStatus="Waiting",
                                           lfnError=None)
  jobResetAgent.treatUserJobWithReq(fakeJobID, alreadyRegisteredRequest)
  jobResetAgent.markJob.assert_not_called()
  jobResetAgent.resetRequest.assert_not_called()
  jobResetAgent._fcClient.removeFile.assert_not_called()

  # request is waiting
  jobResetAgent.markJob.reset_mock()
  jobResetAgent.resetRequest.reset_mock()
  jobResetAgent._fcClient.removeFile.reset_mock()
  alreadyRegisteredRequest = createRequest(requestID=4, opType="RegisterFile",
                                           opStatus="Failed", fileStatus="Failed",
                                           lfnError="File not found")
  jobResetAgent.treatUserJobWithReq(fakeJobID, alreadyRegisteredRequest)
  jobResetAgent.markJob.assert_not_called()
  jobResetAgent._fcClient.removeFile.assert_not_called()

  # removeFile failed
  jobResetAgent.markJob.reset_mock()
  jobResetAgent.resetRequest.reset_mock()
  jobResetAgent._fcClient.removeFile.reset_mock()
  jobResetAgent._fcClient.removeFile.return_value = S_OK(dict(Failed={'/ilc/fake/lfn': "not author"},
                                                              Successful={}))
  alreadyRegisteredRequest = createRequest(requestID=4, opType="RegisterFile",
                                           opStatus="Failed", fileStatus="Failed",
                                           lfnError=alreadyErrorMessage)
  res = jobResetAgent.treatUserJobWithReq(fakeJobID, alreadyRegisteredRequest)
  assert not res['OK']
  jobResetAgent.markJob.assert_not_called()
  jobResetAgent.resetRequest.assert_not_called()
  jobResetAgent._fcClient.removeFile.assert_called_once_with("/ilc/fake/lfn")

  # removeFile failed with S_ERROR
  jobResetAgent.markJob.reset_mock()
  jobResetAgent.resetRequest.reset_mock()
  jobResetAgent._fcClient.removeFile.reset_mock()
  jobResetAgent._fcClient.removeFile.return_value = S_ERROR("Failed")
  alreadyRegisteredRequest = createRequest(requestID=4, opType="RegisterFile",
                                           opStatus="Failed", fileStatus="Failed",
                                           lfnError=alreadyErrorMessage)
  res = jobResetAgent.treatUserJobWithReq(fakeJobID, alreadyRegisteredRequest)
  assert not res['OK']
  jobResetAgent.markJob.assert_not_called()
  jobResetAgent.resetRequest.assert_not_called()
  jobResetAgent._fcClient.removeFile.assert_called_once_with("/ilc/fake/lfn")


def test_treat_User_Job_With_RegisterFile_2(failingUserProxyFixture, jobResetAgent, fakeJobID, alreadyErrorMessage):
  """Test when UserProxy failed."""
  jobResetAgent.markJob = MagicMock()
  jobResetAgent.resetRequest = MagicMock()

  jobResetAgent.markJob.reset_mock()
  jobResetAgent.resetRequest.reset_mock()
  jobResetAgent._fcClient.removeFile.reset_mock()
  jobResetAgent._fcClient.removeFile.return_value = S_ERROR("Failed")
  alreadyRegisteredRequest = createRequest(requestID=4, opType="RegisterFile",
                                           opStatus="Failed", fileStatus="Failed",
                                           lfnError=alreadyErrorMessage)
  res = jobResetAgent.treatUserJobWithReq(fakeJobID, alreadyRegisteredRequest)
  assert not res['OK']
  jobResetAgent.markJob.assert_not_called()
  jobResetAgent.resetRequest.assert_not_called()
  jobResetAgent._fcClient.removeFile.assert_not_called()
