""" Test RestartReqExeAgent """

import unittest

import ILCDIRAC.RequestManagementSystem.Agent.RestartReqExeAgent as RREA
from ILCDIRAC.RequestManagementSystem.Agent.RestartReqExeAgent import RestartReqExeAgent

from mock import MagicMock, call

from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger
import DIRAC

__RCSID__ = "$Id$"


class TestRestartReqExeAgent(unittest.TestCase):
  """ TestRestartReqExeAgent class """

  def setUp(self):
    self.agent = RREA
    self.agent.AgentModule = MagicMock()
    self.agent.NotificationClient = MagicMock(spec=DIRAC.FrameworkSystem.Client.NotificationClient.NotificationClient)

    self.restartAgent = RestartReqExeAgent()
    self.restartAgent.log = gLogger
    self.restartAgent.sysAdminClient = MagicMock()
    self.restartAgent.enabled = True

  def tearDown(self):
    pass

  def test_init(self):
    self.assertIsInstance(self.restartAgent, RestartReqExeAgent)
    self.assertIsInstance(self.restartAgent.nClient, MagicMock)
    self.assertIsInstance(self.restartAgent.sysAdminClient, MagicMock)
    self.assertTrue(self.restartAgent.enabled)
    self.assertEquals(self.restartAgent.addressFrom, "ilcdirac-admin@cern.ch")

  def test_begin_execution(self):
    """ test for beginExecution function """
    self.restartAgent.accounting["Junk"]["Funk"] = 1
    self.restartAgent.am_getOption = MagicMock()
    getOptionCalls = [call('Setup', self.restartAgent.setup),
                      call('EnableFlag', True),
                      call('MailTo', self.restartAgent.addressTo),
                      call('MailFrom', self.restartAgent.addressFrom)]

    self.restartAgent.getAllRunningAgents = MagicMock(return_value=S_OK())
    self.restartAgent.beginExecution()
    self.restartAgent.am_getOption.assert_has_calls(getOptionCalls)
    self.restartAgent.getAllRunningAgents.assert_called()

    # accounting dictionary should be cleared
    self.assertEquals(self.restartAgent.accounting, {})

if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase(TestRestartReqExeAgent)
  TESTRESULT = unittest.TextTestRunner(verbosity=3).run(SUITE)
