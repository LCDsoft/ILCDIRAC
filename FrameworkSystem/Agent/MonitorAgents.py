"""
Restarts an agent or executor in case it gets stuck

This agent is designed to supervise the Agents and Executors, and restarts them in case they get stuck.
The agent checks the age of the log file and if it is deemed too old will kill
the agent so that it is restarted automatically. Executors will only be restarted if there are jobs in checking status



+----------------------------+--------------------------------------------+---------------------------------------+
|  **Option**                |**Description**                             |  **Example**                          |
+----------------------------+--------------------------------------------+---------------------------------------+
| Setup                      | Which setup to monitor                     | Production                            |
|                            |                                            |                                       |
+----------------------------+--------------------------------------------+---------------------------------------+
| EnableFlag                 | If agents or executors should be           | False                                 |
|                            | automatically restarted or not             |                                       |
|                            |                                            |                                       |
+----------------------------+--------------------------------------------+---------------------------------------+
| RestartExecutors           | If executors should be restarted           | False                                 |
|                            | automatically                              |                                       |
+----------------------------+--------------------------------------------+---------------------------------------+
| RestartServices            | If services should be restarted            | False                                 |
|                            | automatically                              |                                       |
+----------------------------+--------------------------------------------+---------------------------------------+
| MailTo                     | Email addresses receiving notifications    |                                       |
|                            |                                            |                                       |
+----------------------------+--------------------------------------------+---------------------------------------+
| MailFrom                   | Sender email address                       |                                       |
|                            |                                            |                                       |
+----------------------------+--------------------------------------------+---------------------------------------+

"""

# imports
from collections import defaultdict
from datetime import datetime
from functools import partial

import os
import psutil
import socket

# from DIRAC
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.PrettyPrint import printTable
from DIRAC.FrameworkSystem.Client.SystemAdministratorClient import SystemAdministratorClient
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient

__RCSID__ = "$Id$"
AGENT_NAME = "Framework/MonitorAgents"

#Define units
HOUR = 3600
MINUTES = 60
SECONDS = 1

# Define constant
NO_CHECKING_JOBS = 'NO_CHECKING_JOBS'
CHECKING_JOBS = 'CHECKING_JOBS'
NO_RESTART = 'NO_RESTART'


class MonitorAgents(AgentModule):
  """ MonitorAgents class """

  def __init__(self, *args, **kwargs):
    AgentModule.__init__(self, *args, **kwargs)
    self.name = 'MonitorAgents'
    self.setup = "Production"
    self.enabled = False
    self.restartExecutors = False
    self.restartServices = False
    self.diracLocation = "/opt/dirac/pro"

    self.sysAdminClient = SystemAdministratorClient(socket.gethostname())
    self.jobMonClient = JobMonitoringClient()
    self.nClient = NotificationClient()
    self.agents = dict()
    self.executors = dict()
    self.services = dict()
    self.errors = list()
    self.accounting = defaultdict(dict)

    self.addressTo = ["andre.philippe.sailer@cern.ch", "hamza.zafar@cern.ch"]
    self.addressFrom = "ilcdirac-admin@cern.ch"
    self.emailSubject = "MonitorAgents"

  def logError(self, errStr, varMsg=''):
    """ appends errors in a list, which is sent in email notification """
    self.log.error(errStr, varMsg)
    self.errors.append(errStr + varMsg)

  def beginExecution(self):
    """ Reload the configurations before every cycle """
    self.setup = self.am_getOption("Setup", self.setup)
    self.enabled = self.am_getOption("EnableFlag", self.enabled)
    self.restartExecutors = self.am_getOption("RestartExecutors", self.restartExecutors)
    self.restartServices = self.am_getOption("RestartServices", self.restartServices)
    self.diracLocation = os.environ.get("DIRAC", self.diracLocation)
    self.addressTo = self.am_getOption('MailTo', self.addressTo)
    self.addressFrom = self.am_getOption('MailFrom', self.addressFrom)

    res = self.getRunningInstances(instanceType='Agents')
    if not res["OK"]:
      return S_ERROR("Failure to get running agents")
    self.agents = res["Value"]

    res = self.getRunningInstances(instanceType='Executors')
    if not res["OK"]:
      return S_ERROR("Failure to get running executors")
    self.executors = res["Value"]

    res = self.getRunningInstances(instanceType='Services')
    if not res["OK"]:
      return S_ERROR("Failure to get running services")
    self.services = res["Value"]

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

  def getRunningInstances(self, instanceType='Agents'):
    """ returns a dict of running agents or executors. Key is agent's name, value contains Polling Time, PID
        and log file location

    :param str instanceType: 'Agents' or 'Executors'
    :returns: Dictionary of running instances
    """

    res = self.sysAdminClient.getOverallStatus()
    if not res["OK"]:
      self.logError("Failure to get agents from system administrator client", res["Message"])
      return res

    val = res['Value'][instanceType]
    runningAgents = defaultdict(dict)
    for system, agents in val.iteritems():
      for agentName, agentInfo in agents.iteritems():
        if agentInfo['Setup'] and agentInfo['Installed'] and agentInfo['RunitStatus'] == 'Run':
          confPath = cfgPath('/Systems/' + system + '/' + self.setup + '/%s/' % instanceType + agentName)
          for option, default in (('PollingTime', HOUR), ('Port', None)):
            optPath = os.path.join(confPath, option)
            runningAgents[agentName][option] = gConfig.getValue(optPath, default)
          runningAgents[agentName]["LogFileLocation"] = \
              os.path.join(self.diracLocation, 'runit', system, agentName, 'log', 'current')
          runningAgents[agentName]["PID"] = agentInfo["PID"]
          runningAgents[agentName]['System'] = system

    return S_OK(runningAgents)

  def on_terminate(self, agentName, process):
    """ callback executes when a process terminates gracefully """
    self.log.info("%s's process with ID: %s has been terminated successfully" % (agentName, process.pid))

  def execute(self):
    """ execution in one cycle """
    ok = True

    for instances, enabled, isExecutor in [(self.agents, self.enabled, False), (self.executors, self.restartExecutors, True)]:
      for agentName, val in instances.iteritems():
        res = self.checkAgent(agentName, val["PollingTime"], val["LogFileLocation"], val["PID"], enabled, isExecutor)
        if not res['OK']:
          self.logError("Failure when checking agent", "%s, %s" % (agentName, res['Message']))
          ok = False

    for service, options in self.services.iteritems():
      res = self.checkService(service, options)
      if not res['OK']:
        self.logError("Failure when checking service", "%s, %s" % (service, res['Message']))
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

  def restartAgent(self, pid, agentName, enabled=False, isExecutor=False):
    """ kills an agent which is then restarted automatically """

    if isExecutor:
      res = self.checkForCheckingJobs(agentName)
      if not res['OK']:
        return res
      if res['OK'] and res['Value'] == NO_CHECKING_JOBS:
        self.accounting.pop(agentName, None)
        return S_OK(NO_RESTART)

    if not (self.enabled and enabled):
      self.log.info("Restarting agents is disabled, please restart %s manually" % agentName)
      self.accounting[agentName]["Treatment"] = "Please restart it manually"
      return S_OK(NO_RESTART)

    try:
      agentProc = psutil.Process(int(pid))
      processesToTerminate = agentProc.children(recursive=True)
      processesToTerminate.append(agentProc)

      for proc in processesToTerminate:
        proc.terminate()

      _gone, alive = psutil.wait_procs(processesToTerminate, timeout=5, callback=partial(self.on_terminate, agentName))
      for proc in alive:
        self.log.info("Forcefully killing process %s" % proc.pid)
        proc.kill()

      return S_OK()

    except psutil.Error as err:
      self.logError("Exception occurred in terminating processes", "%s" % err)
      return S_ERROR()

  def checkService(self, serviceName, options):
    """Pings the service"""
    system = options['System']
    port = options['Port']
    host = socket.gethostname()
    url = 'dips://%s:%s/%s/%s' % (host, port, system, serviceName)
    self.log.info("Pinging service", url)
    pingRes = RPCClient(url).ping()
    if not pingRes['OK']:
      self.logError('Failure pinging service', pingRes['Message'])
      res = self.restartAgent(int(options['PID']), serviceName, enabled=self.restartServices)
      if not res["OK"]:
        return res
      elif res['OK'] and res['Value'] != NO_RESTART:
        self.accounting[agentName]["Treatment"] = "Successfully Restarted"
        self.log.info("Agent %s has been successfully restarted" % agentName)
    self.log.info("Service responded OK")
    return S_OK()

  def checkAgent(self, agentName, pollingTime, currentLogLocation, pid, restartEnabled=False, isExecutor=False):
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

      res = self.restartAgent(int(pid), agentName, restartEnabled, isExecutor)
      if not res["OK"]:
        return res
      elif res['OK'] and res['Value'] != NO_RESTART:
        self.accounting[agentName]["Treatment"] = "Successfully Restarted"
        self.log.info("Agent %s has been successfully restarted" % agentName)

    return S_OK()

  def checkForCheckingJobs(self, executorName):
    """ checks if there are checking jobs with the **executorName** as current MinorStatus """

    attrDict = {'Status': 'Checking', 'MinorStatus': executorName}

    # returns list of jobs IDs
    resJobs = self.jobMonClient.getJobs(attrDict)
    if not resJobs['OK']:
      self.logError("Could not get jobs for this executor", "%s: %s" % (executorName, resJobs['Message']))
      return resJobs
    if resJobs['Value']:
      self.log.info("Found %d jobs in 'Checking' status for %s" % (len(resJobs['Value']), executorName))
      return S_OK(CHECKING_JOBS)
    self.log.info("Found no jobs in 'Checking' status for %s" % executorName)
    return S_OK(NO_CHECKING_JOBS)
