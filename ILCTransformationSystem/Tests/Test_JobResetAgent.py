""" Test JobResetAgent """

import unittest

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
    self.agent.JobDB = MagicMock()
    self.agent.JobMonitoringClient = MagicMock()
    self.agent.DataManager = MagicMock(spec=DIRAC.DataManagementSystem.Client.DataManager)
    self.agent.ReqClient = MagicMock(spec=DIRAC.RequestManagementSystem.Client.ReqClient)
    self.agent.NotificationClient = MagicMock(spec=DIRAC.FrameworkSystem.Client.NotificationClient)

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
    """ test for beginExecution function"""

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

if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase(TestJobResetAgent)
  TESTRESULT = unittest.TextTestRunner(verbosity=3).run(SUITE)
