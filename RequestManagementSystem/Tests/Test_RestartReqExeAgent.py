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

  def test_send_notification(self):
    """ test for sendNotification function """
    self.restartAgent.errors = []
    self.restartAgent.accounting = {}

    # send mail should not be called if there are no errors and accounting information
    self.restartAgent.sendNotification()
    self.restartAgent.nClient.sendMail.assert_not_called()

    # send mail should be called if there are errors but no accounting information
    self.restartAgent.errors = ["some error"]
    self.restartAgent.sendNotification()
    self.restartAgent.nClient.sendMail.assert_called()

    # send email should be called if there is accounting information but no errors
    self.restartAgent.nClient.sendMail.reset_mock()
    self.restartAgent.errors = []
    self.restartAgent.accounting = {"Agent1": {"LogAge": 123, "Treatment": "Agent Restarted"}}
    self.restartAgent.sendNotification()
    self.restartAgent.nClient.sendMail.assert_called()

    # try sending email to all addresses even if we get error for sending email to some address
    self.restartAgent.nClient.sendMail.reset_mock()
    self.restartAgent.errors = ["some error"]
    self.restartAgent.addressTo = ["name1@cern.ch", "name2@cern.ch"]
    self.restartAgent.nClient.sendMail.return_value = S_ERROR()
    self.restartAgent.sendNotification()
    self.assertEquals(len(self.restartAgent.nClient.sendMail.mock_calls),
                      len(self.restartAgent.addressTo))

    # accounting dict and errors list should be cleared after notification is sent
    self.assertEquals(self.restartAgent.accounting, {})
    self.assertEquals(self.restartAgent.errors, [])


if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase(TestRestartReqExeAgent)
  TESTRESULT = unittest.TextTestRunner(verbosity=3).run(SUITE)
