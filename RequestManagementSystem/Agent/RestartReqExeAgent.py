"""
Restarts an agent in case it gets stuck

This agent is designed to supervise the Agents and retarts them in case if they get stuck.
The agent checks the age of the log file and if it is deemed too old will kill
the agent so that it is restarted automatically.
"""

# imports
from collections import defaultdict
from datetime import datetime

import os
import psutil

# from DIRAC
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.PrettyPrint import printTable
from DIRAC.FrameworkSystem.Client.SystemAdministratorClient import SystemAdministratorClient
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient

__RCSID__ = "$Id$"
AGENT_NAME = "RequestManagement/RestartReqExeAgent"

#Define units
HOUR = 3600
MINUTES = 60
SECONDS = 1


class RestartReqExeAgent(AgentModule):
  """ RestartReqExeAgent class """

  def __init__(self, *args, **kwargs):
    AgentModule.__init__(self, *args, **kwargs)
    self.name = 'RestartReqExeAgent'
    self.setup = "Production"
    self.enabled = False
    self.diracLocation = "/opt/dirac/pro"

    self.sysAdminClient = SystemAdministratorClient("localhost")
    self.nClient = NotificationClient()
    self.agents = list()
    self.errors = list()
    self.accounting = defaultdict(dict)

    self.addressTo = ["andre.philippe.sailer@cern.ch", "hamza.zafar@cern.ch"]
    self.addressFrom = "ilcdirac-admin@cern.ch"
    self.emailSubject = "RestartReqExeAgent"

  def logError(self, errStr, varMsg=''):
    """ appends errors in a list, which is sent in email notification """
    self.log.error(errStr, varMsg)
    self.errors.append(errStr + varMsg)

  def beginExecution(self):
    """ Reload the configurations before every cycle """
    self.setup = self.am_getOption("Setup", self.setup)
    self.enabled = self.am_getOption("EnableFlag", self.enabled)
    self.diracLocation = os.environ.get("DIRAC", self.diracLocation)
    self.addressTo = self.am_getOption('MailTo', self.addressTo)
    self.addressFrom = self.am_getOption('MailFrom', self.addressFrom)

    res = self.getAllRunningAgents()
    if not res["OK"]:
      return S_ERROR("Failure to get running agents")
    self.agents = res["Value"]

    self.accounting.clear()
    return S_OK()

  def sendNotification(self):
    """ sends email notification about stuck agents """
    if not(self.errors or self.accounting):
      return S_OK()

    emailBody = ""
    rows = []
    for agentName, val in self.accounting.iteritems():
      rows.append([[agentName], [str(val["LogAge"])], [val["Treatment"]]])

    if rows:
      columns = ["Agent", "Log File Age (Minutes)", "Treatment"]
      emailBody += printTable(columns, rows, printOut=False, numbering=False, columnSeparator=' | ')

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

  def getAllRunningAgents(self):
    """ returns a dict of running agents. Key is agent's name, value contains Polling Time, PID
        and log file location """

    res = self.sysAdminClient.getOverallStatus()
    if not res["OK"]:
      self.logError("Failure to get agents from system administrator client", res["Message"])
      return res

    val = res['Value']['Agents']
    runningAgents = defaultdict(dict)
    for system, agents in val.iteritems():
      for agentName, agentInfo in agents.iteritems():
        if agentInfo['Setup'] and agentInfo['Installed'] and agentInfo['RunitStatus'] == 'Run':
          confPath = cfgPath('/Systems/' + system + '/' + self.setup + '/Agents/' + agentName + '/PollingTime')
          runningAgents[agentName]["PollingTime"] = gConfig.getValue(confPath, HOUR)
          runningAgents[agentName]["LogFileLocation"] = os.path.join(self.diracLocation, 'runit', system, agentName,
                                                                     'log', 'current')
          runningAgents[agentName]["PID"] = agentInfo["PID"]

    return S_OK(runningAgents)

  def on_terminate(self, agentName, process):
    """ callback executes when a process terminates gracefully """
    self.log.info("%s's process with ID: %s has been terminated successfully" % (agentName, process.pid))

  def execute(self):
    """ execution in one cycle """
    ok = True
    for agentName, val in self.agents.iteritems():
      res = self.checkAgent(agentName, val["PollingTime"], val["LogFileLocation"], val["PID"])
      if not res['OK']:
        self.logError("Failure when checking agent", "%s, %s" % (agentName, res['Message']))
        ok = False

    self.sendNotification()

    if not ok:
      return S_ERROR("Error during this cycle, check log")

    return S_OK()

  @staticmethod
  def getLastAccessTime(logFileLocation):
    """ return the age of log file """

    lastAccessTime = 0
    try:
      lastAccessTime = os.path.getmtime(logFileLocation)
      lastAccessTime = datetime.fromtimestamp(lastAccessTime)
    except OSError as e:
      return S_ERROR(str(e))

    now = datetime.now()
    age = now - lastAccessTime
    return S_OK(age)

  def restartAgent(self, pid):
    """ kills an agent which is then restarted automatically """
    try:
      agentProc = psutil.Process(int(pid))
      processesToTerminate = agentProc.children(recursive=True)
      processesToTerminate.append(agentProc)

      for proc in processesToTerminate:
        proc.terminate()

      gone, alive = psutil.wait_procs(processesToTerminate, timeout=5, callback=self.on_terminate)
      for proc in alive:
        self.log.info("Forcefully killing process %s" % proc.pid)
        proc.kill()

      return S_OK()

    except psutil.Error as err:
      self.logError("Exception occurred in terminating processes", "%s" % err)
      return S_ERROR()

  def checkAgent(self, agentName, pollingTime, currentLogLocation, pid):
    """ checks the age of agent's log file, if it is too old then restarts the agent """

    self.log.info("Checking Agent: %s" % agentName)
    self.log.info("Polling Time: %s" % pollingTime)
    self.log.info("Current Log File location: %s" % currentLogLocation)

    res = self.getLastAccessTime(currentLogLocation)
    if not res["OK"]:
      self.logError("Failed to access current log file for", "%s Message: %s" % (agentName, res["Message"]))
      return res

    age = res["Value"]
    self.log.info("Current log file for %s is %d minutes old" % (agentName, (age.seconds / MINUTES)))

    maxLogAge = max(pollingTime + HOUR, 2 * HOUR)
    if age.seconds > maxLogAge:
      self.log.info("Current log file is too old for Agent %s" % agentName)
      self.accounting[agentName]["LogAge"] = age.seconds / MINUTES

      if not self.enabled:
        self.log.info("Restarting agents is disabled, please restart %s manually" % agentName)
        self.accounting[agentName]["Treatment"] = "Please restart it manually"
        return S_OK()

      res = self.restartAgent(int(pid))
      if not res["OK"]:
        return res

      self.accounting[agentName]["Treatment"] = "Successfully Restarted"
      self.log.info("Agent %s has been successfully restarted" % agentName)

    return S_OK()
