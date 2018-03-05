""" Test RestartReqExeAgent """

import unittest
from datetime import timedelta

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
    self.restartAgent.am_getOption.assert_has_calls(getOptionCalls, any_order=True)
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

  def test_get_all_running_agents(self):
    """ test for getAllRunningAgents function """
    self.restartAgent.sysAdminClient.getOverallStatus = MagicMock()
    self.restartAgent.sysAdminClient.getOverallStatus.return_value = S_ERROR()

    res = self.restartAgent.getAllRunningAgents()
    self.assertFalse(res["OK"])

    agents = {'Agents': {'DataManagement': {'FTS3Agent': {'MEM': '0.3', 'Setup': True, 'PID': '18128',
                                            'RunitStatus': 'Run', 'Module': 'CleanFTSDBAgent',
                                            'Installed': True, 'VSZ': '375576', 'Timeup': '29841',
                                            'CPU': '0.0', 'RSS': '55452'}},
                         'Framework': {'ErrorMessageMonitor': {'MEM': '0.3', 'Setup': True, 'PID': '2303',
                                                               'RunitStatus': 'Run', 'Module': 'ErrorMessageMonitor',
                                                               'Installed': True, 'VSZ': '380392', 'Timeup': '3380292',
                                                               'CPU': '0.0', 'RSS': '56172'}}}}
    agents['Agents']['DataManagement']['FTSAgent'] = {'Setup': False, 'PID': 0, 'RunitStatus': 'Unknown',
                                                      'Module': 'FTSAgent', 'Installed': False, 'Timeup': 0}

    self.restartAgent.sysAdminClient.getOverallStatus.return_value = S_OK(agents)
    res = self.restartAgent.getAllRunningAgents()

    # only insalled agents with RunitStatus RUN should be returned
    self.assertTrue('FTSAgent' not in res["Value"])
    self.assertTrue('FTS3Agent' in res["Value"])
    self.assertTrue('ErrorMessageMonitor' in res["Value"])
    for agent in res["Value"]:
      self.assertTrue('PollingTime' in res["Value"][agent])
      self.assertTrue('LogFileLocation' in res["Value"][agent])
      self.assertTrue('PID' in res["Value"][agent])

  def test_execute(self):
    """ test for execute function """
    agentOne = 'FTS3Agent'
    agentTwo = 'FTSAgent'
    agentOnePollingTime = 100
    agentTwoPollingTime = 200
    agentOneLogLoc = '/fake/loc1'
    agentTwoLogLoc = '/fake/loc2'
    agentOnePID = '12345'
    agentTwoPID = '54321'

    self.restartAgent.agents = {agentOne: {'PollingTime': agentOnePollingTime,
                                           'LogFileLocation': agentOneLogLoc,
                                           'PID': agentOnePID},
                                agentTwo: {'PollingTime': agentTwoPollingTime,
                                           'LogFileLocation': agentTwoLogLoc,
                                           'PID': agentTwoPID}}

    self.restartAgent._checkAgent = MagicMock(side_effect=[S_OK(), S_ERROR()])

    res = self.restartAgent.execute()
    self.assertFalse(res["OK"])
    calls = [call(agentOne, agentOnePollingTime, agentOneLogLoc, agentOnePID),
             call(agentTwo, agentTwoPollingTime, agentTwoLogLoc, agentTwoPID)]

    self.restartAgent._checkAgent.assert_has_calls(calls, any_order=True)

  def test_check_agent(self):
    """ test for _checkAgent function """
    self.restartAgent.getLastAccessTime = MagicMock()
    self.restartAgent.restartAgent = MagicMock(return_value=S_OK())

    agentName = 'agentX'
    pollingTime = RREA.HOUR
    currentLogLocation = '/fake/log/file'
    pid = '12345'

    self.restartAgent.getLastAccessTime.return_value = S_ERROR()
    res = self.restartAgent._checkAgent(agentName, pollingTime, currentLogLocation, pid)
    self.assertFalse(res["OK"])

    # agents with log file age less than max(pollingTime+Hour, 2 Hour) should not be restarted
    logAge = timedelta(hours=1)
    self.restartAgent.getLastAccessTime.return_value = S_OK(logAge)
    res = self.restartAgent._checkAgent(agentName, pollingTime, currentLogLocation, pid)
    self.assertTrue(res["OK"])
    self.restartAgent.restartAgent.assert_not_called()

    # agents with log file age of more than max(pollingTime+Hour, 2 Hour) should be restarted
    logAge = timedelta(hours=3)
    self.restartAgent.getLastAccessTime.return_value = S_OK(logAge)
    res = self.restartAgent._checkAgent(agentName, pollingTime, currentLogLocation, pid)
    self.assertTrue(res["OK"])
    self.restartAgent.restartAgent.assert_called_once_with(int(pid))


if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase(TestRestartReqExeAgent)
  TESTRESULT = unittest.TextTestRunner(verbosity=3).run(SUITE)
