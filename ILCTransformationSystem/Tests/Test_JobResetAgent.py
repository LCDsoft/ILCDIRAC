""" Test JobResetAgent """

import unittest

from datetime import datetime, timedelta
from mock import MagicMock, call

import ILCDIRAC.ILCTransformationSystem.Agent.JobResetAgent as JRA
import DIRAC.Resources.Storage.StorageElement as SeModule
from ILCDIRAC.ILCTransformationSystem.Agent.JobResetAgent import JobResetAgent

from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger
import DIRAC

__RCSID__ = "$Id$"


class TestJobResetAgent(unittest.TestCase):
  """ TestJobResetAgent class """

  def setUp(self):
    self.agent = JRA
    self.agent.AgentModule = MagicMock()
    self.agent.JobDB = MagicMock(spec=DIRAC.WorkloadManagementSystem.DB.JobDB)
    self.agent.JobMonitoringClient = MagicMock()
    self.agent.DataManager = MagicMock(spec=DIRAC.DataManagementSystem.Client.DataManager)
    self.agent.ReqClient = MagicMock(spec=DIRAC.RequestManagementSystem.Client.ReqClient)
    self.agent.NotificationClient = MagicMock(spec=DIRAC.FrameworkSystem.Client.NotificationClient)

    self.today = datetime(2018, 12, 25, 0, 0, 0, 0)
    self.agent.datetime = MagicMock()
    self.agent.datetime.now.return_value=self.today

    self.jobResetAgent = JobResetAgent()
    self.jobResetAgent.log = gLogger
    self.jobResetAgent.enabled = True

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
    fakeJobID = 1
    self.jobResetAgent.markJob = MagicMock()

    # case if getJobsMinorStatus function returns an error
    self.jobResetAgent.jobMonClient.getJobsMinorStatus.return_value = S_ERROR()
    res = self.jobResetAgent.treatUserJobWithNoReq(fakeJobID)
    self.assertFalse(res["OK"])
    self.jobResetAgent.jobMonClient.getJobsMinorStatus.called_once_with([fakeJobID])

    # case if getJobsMinorStatus executes successfully but getJobsApplicationStatus returns an error
    self.jobResetAgent.jobMonClient.getJobsMinorStatus.return_value = S_OK({fakeJobID: {'MinorStatus':
                                                                                        JRA.FINAL_MINOR_STATES[0],
                                                                                        'JobID': fakeJobID}})
    self.jobResetAgent.jobMonClient.getJobsApplicationStatus.return_value = S_ERROR()
    res = self.jobResetAgent.treatUserJobWithNoReq(fakeJobID)
    self.assertFalse(res["OK"])

    # mark job done if ApplicationStatus and MinorStatus are in Final States
    self.jobResetAgent.jobMonClient.getJobsApplicationStatus.return_value = S_OK({fakeJobID: {'ApplicationStatus':
                                                                                              JRA.FINAL_APP_STATES[0],
                                                                                              'JobID': fakeJobID}})
    res = self.jobResetAgent.treatUserJobWithNoReq(fakeJobID)
    self.assertTrue(res["OK"])
    self.jobResetAgent.markJob.assert_called_once_with(fakeJobID, "Done")

    # dont do anything if ApplicationStatus and MinorStatus are not in Final States
    self.jobResetAgent.markJob.reset_mock()
    self.jobResetAgent.jobMonClient.getJobsMinorStatus.return_value = S_OK({fakeJobID: {'MinorStatus': 'other status',
                                                                                        'JobID': fakeJobID}})
    self.jobResetAgent.jobMonClient.getJobsApplicationStatus.return_value = S_OK({fakeJobID: {'ApplicationStatus':
                                                                                              'other status',
                                                                                              'JobID': fakeJobID}})
    res = self.jobResetAgent.treatUserJobWithNoReq(fakeJobID)
    self.assertTrue(res["OK"])
    self.jobResetAgent.markJob.assert_not_called()

if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase(TestJobResetAgent)
  TESTRESULT = unittest.TextTestRunner(verbosity=3).run(SUITE)
