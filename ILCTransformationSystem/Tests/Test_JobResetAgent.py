""" Test JobResetAgent """

import unittest

from datetime import datetime, timedelta
from mock import MagicMock, call

import ILCDIRAC.ILCTransformationSystem.Agent.JobResetAgent as JRA
from ILCDIRAC.ILCTransformationSystem.Agent.JobResetAgent import JobResetAgent

import DIRAC.Resources.Storage.StorageElement as SeModule
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation

from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger
import DIRAC

__RCSID__ = "$Id$"


class TestJobResetAgent(unittest.TestCase):
  """ TestJobResetAgent class """

  def setUp(self):
    self.agent = JRA
    self.agent.AgentModule = MagicMock()
    self.agent.JobDB = MagicMock(spec=DIRAC.WorkloadManagementSystem.DB.JobDB.JobDB)
    self.agent.JobMonitoringClient = MagicMock()
    self.agent.DataManager = MagicMock(spec=DIRAC.DataManagementSystem.Client.DataManager.DataManager)
    self.agent.ReqClient = MagicMock(spec=DIRAC.RequestManagementSystem.Client.ReqClient.ReqClient)
    self.agent.NotificationClient = MagicMock(spec=DIRAC.FrameworkSystem.Client.NotificationClient.NotificationClient)

    self.today = datetime(2018, 12, 25, 0, 0, 0, 0)
    self.agent.datetime = MagicMock()
    self.agent.datetime.now.return_value=self.today

    self.jobResetAgent = JobResetAgent()
    self.jobResetAgent.log = gLogger
    self.jobResetAgent.enabled = True
    self.fakeJobID = 1

    self.jobResetAgent.markJob = MagicMock()
    self.jobResetAgent.resetRequest = MagicMock()

    self.doneRemoveRequest = self.createRequest(requestID=1, opType="RemoveFile",
                                                opStatus="Done", fileStatus="Done")
    self.doneReplicateRequest = self.createRequest(requestID=2, opType="ReplicateAndRegister",
                                                   opStatus="Done", fileStatus="Done")
    self.failedReplicateRequest = self.createRequest(requestID=3, opType="ReplicateAndRegister",
                                                     opStatus="Failed", fileStatus="Failed")
    self.failedRemoveRequest = self.createRequest(requestID=4, opType="RemoveFile",
                                                  opStatus="Failed", fileStatus="Failed")
  def tearDown(self):
    pass

  def test_init(self):
    self.assertIsInstance(self.jobResetAgent, JobResetAgent)
    self.assertIsInstance(self.jobResetAgent.jobMonClient, MagicMock)
    self.assertIsInstance(self.jobResetAgent.dataManager, MagicMock)
    self.assertIsInstance(self.jobResetAgent.reqClient, MagicMock)
    self.assertIsInstance(self.jobResetAgent.nClient, MagicMock)
    self.assertTrue(self.jobResetAgent.enabled)
    self.assertEquals(self.jobResetAgent.addressFrom, "ilcdirac-admin@cern.ch")
    self.assertEquals(self.jobResetAgent.userJobTypes, ['User'])
    self.assertEquals(self.jobResetAgent.prodJobTypes, ['MCGeneration', 'MCSimulation', 'MCReconstruction',
                                                        'MCReconstruction_Overlay', 'Split', 'MCSimulation_ILD',
                                                        'MCReconstruction_ILD', 'MCReconstruction_Overlay_ILD',
                                                        'Split_ILD'])

  def test_begin_execution(self):
    """ test for beginExecution function """

    self.jobResetAgent.accounting["Junk"].append("Funk")
    self.jobResetAgent.am_setOption = MagicMock()
    self.jobResetAgent.am_getOption = MagicMock()
    getOptionCalls = [call('EnableFlag', True),
                      call('MailTo', self.jobResetAgent.addressTo),
                      call('MailFrom', self.jobResetAgent.addressFrom)]

    self.jobResetAgent.beginExecution()
    self.jobResetAgent.am_setOption.assert_any_call('shifterProxy', 'DataManager')
    self.jobResetAgent.am_getOption.assert_has_calls(getOptionCalls)
    # accounting dictionary should be cleared
    self.assertEquals(self.jobResetAgent.accounting, {})

  def test_get_jobs(self):
    """ test for getJobs function """
    jobStatus = "Done"
    jobType = "User"
    minorStatus = "Requests Done"
    attrDict = {"JobType": jobType,
                "MinorStatus": minorStatus,
                "Status": jobStatus}

    self.jobResetAgent.jobDB.selectJobs.return_value = S_ERROR()
    res = self.jobResetAgent.getJobs(jobStatus, jobType, minorStatus)
    self.assertFalse(res["OK"])

    self.jobResetAgent.jobDB.selectJobs.reset_mock()
    self.jobResetAgent.jobDB.selectJobs.return_value = S_OK(["1", "2", "3"])
    res = self.jobResetAgent.getJobs(jobStatus, jobType, minorStatus)
    self.assertEquals(res["Value"], [1, 2, 3])
    self.jobResetAgent.jobDB.selectJobs.assert_called_once_with(attrDict, older=self.today - timedelta(days=1))

  def test_treat_User_Job_With_No_Req(self):
    """ test for treatUserJobWithNoReq function """
    self.jobResetAgent.markJob.reset_mock()

    # case if getJobsMinorStatus function returns an error
    self.jobResetAgent.jobMonClient.getJobsMinorStatus.return_value = S_ERROR()
    res = self.jobResetAgent.treatUserJobWithNoReq(self.fakeJobID)
    self.assertFalse(res["OK"])
    self.jobResetAgent.jobMonClient.getJobsMinorStatus.called_once_with([self.fakeJobID])

    # case if getJobsMinorStatus executes successfully but getJobsApplicationStatus returns an error
    self.jobResetAgent.jobMonClient.getJobsMinorStatus.return_value = S_OK({self.fakeJobID: {'MinorStatus':
                                                                                        JRA.FINAL_MINOR_STATES[0],
                                                                                        'JobID': self.fakeJobID}})
    self.jobResetAgent.jobMonClient.getJobsApplicationStatus.return_value = S_ERROR()
    res = self.jobResetAgent.treatUserJobWithNoReq(self.fakeJobID)
    self.assertFalse(res["OK"])

    # mark job done if ApplicationStatus and MinorStatus are in Final States
    self.jobResetAgent.jobMonClient.getJobsApplicationStatus.return_value = S_OK({self.fakeJobID: {'ApplicationStatus':
                                                                                              JRA.FINAL_APP_STATES[0],
                                                                                              'JobID': self.fakeJobID}})
    res = self.jobResetAgent.treatUserJobWithNoReq(self.fakeJobID)
    self.assertTrue(res["OK"])
    self.jobResetAgent.markJob.assert_called_once_with(self.fakeJobID, "Done")

    # dont do anything if ApplicationStatus and MinorStatus are not in Final States
    self.jobResetAgent.markJob.reset_mock()
    self.jobResetAgent.jobMonClient.getJobsMinorStatus.return_value = S_OK({self.fakeJobID: {'MinorStatus': 'other status',
                                                                                        'JobID': self.fakeJobID}})
    self.jobResetAgent.jobMonClient.getJobsApplicationStatus.return_value = S_OK({self.fakeJobID: {'ApplicationStatus':
                                                                                              'other status',
                                                                                              'JobID': self.fakeJobID}})
    res = self.jobResetAgent.treatUserJobWithNoReq(self.fakeJobID)
    self.assertTrue(res["OK"])
    self.jobResetAgent.markJob.assert_not_called()

  def test_treat_User_Job_With_Req(self):
    """ test for treatUserJobWithReq function """
    doneRequest = self.createRequest(requestID=1, opType="RemoveFile", opStatus="Done", fileStatus="Done")
    failedRequestID = 2
    failedRequest = self.createRequest(requestID=failedRequestID, opType="RemoveFile", opStatus="Failed",
                                       fileStatus="Failed")
    self.jobResetAgent.markJob.reset_mock()
    self.jobResetAgent.resetRequest.reset_mock()

    # if request status is 'Done' then job should also be marked 'Done'
    self.jobResetAgent.treatUserJobWithReq(self.fakeJobID, doneRequest)
    self.jobResetAgent.markJob.assert_called_once_with(self.fakeJobID, "Done")
    self.jobResetAgent.resetRequest.assert_not_called()

    # if request status is not 'Done' then reset request
    self.jobResetAgent.markJob.reset_mock()
    self.jobResetAgent.treatUserJobWithReq(self.fakeJobID, failedRequest)
    self.jobResetAgent.markJob.assert_not_called()
    self.jobResetAgent.resetRequest.assert_called_once_with(failedRequestID)

  @staticmethod
  def createRequest(requestID, opType, opStatus, fileStatus, lfnError=" "):
    req = Request({"RequestID": requestID})
    op = Operation({"Type": opType, "Status": opStatus})
    op.addFile(File({"LFN": "/ilc/fake/lfn", "Status": fileStatus, "Error": lfnError}))
    req.addOperation(op)
    return req

  def test_treat_Failed_Prod_With_Req(self):
    """ test for treatFailedProdWithReq function """
    self.jobResetAgent.markJob.reset_mock()
    self.jobResetAgent.resetRequest.reset_mock()
    self.jobResetAgent.dataManager.removeFile.reset_mock()

    # if request is done then job should be marked failed
    self.jobResetAgent.treatFailedProdWithReq(self.fakeJobID, self.doneRemoveRequest)
    self.jobResetAgent.markJob.assert_called_once_with(self.fakeJobID, "Failed")

    self.jobResetAgent.markJob.reset_mock()
    self.jobResetAgent.treatFailedProdWithReq(self.fakeJobID, self.doneReplicateRequest)
    self.jobResetAgent.markJob.assert_called_once_with(self.fakeJobID, "Failed")

    # failed requests with removeFile operation should be reset
    self.jobResetAgent.markJob.reset_mock()
    self.jobResetAgent.treatFailedProdWithReq(self.fakeJobID, self.failedRemoveRequest)
    fileLfn = self.failedRemoveRequest[0][0].LFN
    self.jobResetAgent.dataManager.removeFile.assert_called_once_with([fileLfn], force=True)
    self.jobResetAgent.resetRequest.assert_called_once_with(getattr(self.failedRemoveRequest, "RequestID"))
    self.jobResetAgent.markJob.asset_not_called()

    # failed requests with operations other than removeFile should not be reset
    self.jobResetAgent.resetRequest.reset_mock()
    self.jobResetAgent.dataManager.reset_mock()
    self.jobResetAgent.treatFailedProdWithReq(self.fakeJobID, self.failedReplicateRequest)
    self.jobResetAgent.dataManager.assert_not_called()
    self.jobResetAgent.resetRequest.assert_not_called()
    self.jobResetAgent.markJob.asset_not_called()

  def test_treat_Failed_Prod_With_No_Req(self):
    """ test for treatFailedProdWithNoReq function """
    self.jobResetAgent.markJob.reset_mock()
    self.jobResetAgent.treatFailedProdWithNoReq(self.fakeJobID)
    self.jobResetAgent.markJob.assert_called_once_with(self.fakeJobID, "Failed")

  def test_treat_Completed_Prod_With_Req(self):
    """ test for treatCompletedProdWithReq function """
    self.jobResetAgent.markJob.reset_mock()
    self.jobResetAgent.resetRequest.reset_mock()

    # if request is done then job should be marked Done
    self.jobResetAgent.treatCompletedProdWithReq(self.fakeJobID, self.doneRemoveRequest)
    self.jobResetAgent.markJob.assert_called_once_with(self.fakeJobID, "Done")

    self.jobResetAgent.markJob.reset_mock()
    self.jobResetAgent.treatCompletedProdWithReq(self.fakeJobID, self.doneReplicateRequest)
    self.jobResetAgent.markJob.assert_called_once_with(self.fakeJobID, "Done")

    # job with failed ReplicateAndRegister operation should be marked done if file does not exist
    self.jobResetAgent.markJob.reset_mock()
    request = self.createRequest(requestID=1, opType="RemoveFile", opStatus="Done",
                                 fileStatus="Done", lfnError="No such file")
    self.jobResetAgent.treatCompletedProdWithReq(self.fakeJobID, request)
    self.jobResetAgent.markJob.assert_called_once_with(self.fakeJobID, "Done")

    # failed requests with ReplicateAndRegister operation should be reset
    self.jobResetAgent.markJob.reset_mock()
    self.jobResetAgent.treatCompletedProdWithReq(self.fakeJobID, self.failedReplicateRequest)
    self.jobResetAgent.resetRequest.assert_called_once_with(getattr(self.failedReplicateRequest, "RequestID"))

    # failed Remove file request should not be reset
    self.jobResetAgent.resetRequest.reset_mock()
    self.jobResetAgent.treatCompletedProdWithReq(self.fakeJobID, self.failedRemoveRequest)
    self.jobResetAgent.markJob.assert_not_called()
    self.jobResetAgent.resetRequest.assert_not_called()

if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase(TestJobResetAgent)
  TESTRESULT = unittest.TextTestRunner(verbosity=3).run(SUITE)
