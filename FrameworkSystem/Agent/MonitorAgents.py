"""Monitor agent, executor or service behaviour and intervene if necessary.

This agent is designed to supervise the Agents, Executors and Services, and restarts them in case they get stuck.
The agent checks the age of the log file and if it is deemed too old will kill
the agent so that it is restarted automatically. Executors will only be restarted if there are jobs in checking status

Check for running and stopped components and ensure they have the proper status as defined in the CS
Registry/Hosts/_HOST_/[Running|Stopped] sections. For services also the URL will be added or removed from the
configuration.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN MonitorAgents
  :end-before: ##END
  :dedent: 2
  :caption: MonitorAgents options

"""

# imports
from collections import defaultdict
from datetime import datetime
from functools import partial

import os
import socket
import psutil

# from DIRAC
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Base.Client import Client
from DIRAC.Core.Utilities.PrettyPrint import printTable
from DIRAC.FrameworkSystem.Client.SystemAdministratorClient import SystemAdministratorClient
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient

__RCSID__ = "$Id$"
AGENT_NAME = "Framework/MonitorAgents"

# Define units
HOUR = 3600
MINUTES = 60
SECONDS = 1

# Define constant
NO_CHECKING_JOBS = 'NO_CHECKING_JOBS'
CHECKING_JOBS = 'CHECKING_JOBS'
NO_RESTART = 'NO_RESTART'


class MonitorAgents(AgentModule):
  """MonitorAgents class."""

  def __init__(self, *args, **kwargs):
    """Initialize the agent, clients, default values."""
    AgentModule.__init__(self, *args, **kwargs)
    self.name = 'MonitorAgents'
    self.setup = "Production"
    self.enabled = False
    self.restartAgents = False
    self.restartExecutors = False
    self.restartServices = False
    self.controlComponents = False
    self.commitURLs = False
    self.diracLocation = "/opt/dirac/pro"

    self.sysAdminClient = SystemAdministratorClient(socket.gethostname())
    self.jobMonClient = JobMonitoringClient()
    self.nClient = NotificationClient()
    self.csAPI = None
    self.agents = dict()
    self.executors = dict()
    self.services = dict()
    self.errors = list()
    self.accounting = defaultdict(dict)

    self.addressTo = ["ilcdirac-admin@cern.ch"]
    self.addressFrom = "ilcdirac-admin@cern.ch"
    self.emailSubject = "MonitorAgents on %s" % socket.gethostname()

  def logError(self, errStr, varMsg=''):
    """Append errors to a list, which is sent in email notification."""
    self.log.error(errStr, varMsg)
    self.errors.append(errStr + " " + varMsg)

  def beginExecution(self):
    """Reload the configurations before every cycle."""
    self.setup = self.am_getOption("Setup", self.setup)
    self.enabled = self.am_getOption("EnableFlag", self.enabled)
    self.restartAgents = self.am_getOption("RestartAgents", self.restartAgents)
    self.restartExecutors = self.am_getOption("RestartExecutors", self.restartExecutors)
    self.restartServices = self.am_getOption("RestartServices", self.restartServices)
    self.diracLocation = os.environ.get("DIRAC", self.diracLocation)
    self.addressTo = self.am_getOption('MailTo', self.addressTo)
    self.addressFrom = self.am_getOption('MailFrom', self.addressFrom)
    self.controlComponents = self.am_getOption('ControlComponents', self.controlComponents)
    self.commitURLs = self.am_getOption('CommitURLs', self.commitURLs)

    self.csAPI = CSAPI()

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
    """Send email notification about changes done in the last cycle."""
    if not(self.errors or self.accounting):
      return S_OK()

    emailBody = ""
    rows = []
    for instanceName, val in self.accounting.iteritems():
      rows.append([[instanceName],
                   [val.get('Treatment', 'No Treatment')],
                   [str(val.get('LogAge', 'Not Relevant'))]])

    if rows:
      columns = ["Instance", "Treatment", "Log File Age (Minutes)"]
      emailBody += printTable(columns, rows, printOut=False, numbering=False, columnSeparator=' | ')

    if self.errors:
      emailBody += "\n\nErrors:"
      emailBody += "\n".join(self.errors)

    self.log.notice("Sending Email:\n" + emailBody)
    for address in self.addressTo:
      res = self.nClient.sendMail(address, self.emailSubject, emailBody, self.addressFrom, localAttempt=False)
      if not res['OK']:
        self.log.error("Failure to send Email notification to ", address)
        continue

    self.errors = []
    self.accounting.clear()

    return S_OK()

  def getRunningInstances(self, instanceType='Agents', runitStatus='Run'):
    """Return a dict of running agents, executors or services.

    Key is agent's name, value contains dict with PollingTime, PID, Port, Module, RunitStatus, LogFileLocation

    :param str instanceType: 'Agents', 'Executors', 'Services'
    :param str runitStatus: Return only those instances with given RunitStatus or 'All'
    :returns: Dictionary of running instances
    """
    res = self.sysAdminClient.getOverallStatus()
    if not res["OK"]:
      self.logError("Failure to get %s from system administrator client" % instanceType, res["Message"])
      return res

    val = res['Value'][instanceType]
    runningAgents = defaultdict(dict)
    for system, agents in val.iteritems():
      for agentName, agentInfo in agents.iteritems():
        if agentInfo['Setup'] and agentInfo['Installed']:
          if runitStatus != 'All' and agentInfo['RunitStatus'] != runitStatus:
            continue
          confPath = cfgPath('/Systems/' + system + '/' + self.setup + '/%s/' % instanceType + agentName)
          for option, default in (('PollingTime', HOUR), ('Port', None)):
            optPath = os.path.join(confPath, option)
            runningAgents[agentName][option] = gConfig.getValue(optPath, default)
          runningAgents[agentName]["LogFileLocation"] = \
              os.path.join(self.diracLocation, 'runit', system, agentName, 'log', 'current')
          runningAgents[agentName]["PID"] = agentInfo["PID"]
          runningAgents[agentName]['Module'] = agentInfo['Module']
          runningAgents[agentName]['RunitStatus'] = agentInfo['RunitStatus']
          runningAgents[agentName]['System'] = system

    return S_OK(runningAgents)

  def on_terminate(self, agentName, process):
    """Execute callback when a process terminates gracefully."""
    self.log.info("%s's process with ID: %s has been terminated successfully" % (agentName, process.pid))

  def execute(self):
    """Execute checks for agents, executors, services."""
    for instanceType in ('executor', 'agent', 'service'):
      for name, options in getattr(self, instanceType + 's').iteritems():
        # call checkAgent, checkExecutor, checkService
        res = getattr(self, 'check' + instanceType.capitalize())(name, options)
        if not res['OK']:
          self.logError("Failure when checking %s" % instanceType, "%s, %s" % (name, res['Message']))

    res = self.componentControl()
    if not res['OK']:
      if "Stopped does not exist" not in res['Message'] and \
         "Running does not exist" not in res['Message']:
        self.logError("Failure to control components", res['Message'])

    if not self.errors:
      res = self.checkURLs()
      if not res['OK']:
        self.logError("Failure to check URLs", res['Message'])
    else:
      self.logError('Something was wrong before, not checking URLs this time')

    self.sendNotification()

    if self.errors:
      return S_ERROR("Error during this cycle, check log")

    return S_OK()

  @staticmethod
  def getLastAccessTime(logFileLocation):
    """Return the age of log file."""
    lastAccessTime = 0
    try:
      lastAccessTime = os.path.getmtime(logFileLocation)
      lastAccessTime = datetime.fromtimestamp(lastAccessTime)
    except OSError as e:
      return S_ERROR('Failed to access logfile %s: %r' % (logFileLocation, e))

    now = datetime.now()
    age = now - lastAccessTime
    return S_OK(age)

  def restartInstance(self, pid, instanceName, enabled):
    """Kill a process which is then restarted automatically."""
    if not (self.enabled and enabled):
      self.log.info("Restarting is disabled, please restart %s manually" % instanceName)
      self.accounting[instanceName]["Treatment"] = "Please restart it manually"
      return S_OK(NO_RESTART)

    try:
      agentProc = psutil.Process(int(pid))
      processesToTerminate = agentProc.children(recursive=True)
      processesToTerminate.append(agentProc)

      for proc in processesToTerminate:
        proc.terminate()

      _gone, alive = psutil.wait_procs(processesToTerminate, timeout=5,
                                       callback=partial(self.on_terminate, instanceName))
      for proc in alive:
        self.log.info("Forcefully killing process %s" % proc.pid)
        proc.kill()

      return S_OK()

    except psutil.Error as err:
      self.logError("Exception occurred in terminating processes", "%s" % err)
      return S_ERROR()

  def checkService(self, serviceName, options):
    """Ping the service, restart if the ping does not respond."""
    url = self._getURL(serviceName, options)
    self.log.info("Pinging service", url)
    pingRes = Client().ping(url=url)
    if not pingRes['OK']:
      self.log.info('Failure pinging service: %s: %s' % (url, pingRes['Message']))
      res = self.restartInstance(int(options['PID']), serviceName, self.restartServices)
      if not res["OK"]:
        return res
      elif res['OK'] and res['Value'] != NO_RESTART:
        self.accounting[serviceName]["Treatment"] = "Successfully Restarted"
        self.log.info("Agent %s has been successfully restarted" % serviceName)
    self.log.info("Service responded OK")
    return S_OK()

  def checkAgent(self, agentName, options):
    """Check the age of agent's log file, if it is too old then restart the agent."""
    pollingTime, currentLogLocation, pid = options['PollingTime'], options['LogFileLocation'], options['PID']
    self.log.info("Checking Agent: %s" % agentName)
    self.log.info("Polling Time: %s" % pollingTime)
    self.log.info("Current Log File location: %s" % currentLogLocation)

    res = self.getLastAccessTime(currentLogLocation)
    if not res["OK"]:
      return res

    age = res["Value"]
    self.log.info("Current log file for %s is %d minutes old" % (agentName, (age.seconds / MINUTES)))

    maxLogAge = max(pollingTime + HOUR, 2 * HOUR)
    if age.seconds < maxLogAge:
      return S_OK()

    self.log.info("Current log file is too old for Agent %s" % agentName)
    self.accounting[agentName]["LogAge"] = age.seconds / MINUTES

    res = self.restartInstance(int(pid), agentName, self.restartAgents)
    if not res["OK"]:
      return res
    elif res['OK'] and res['Value'] != NO_RESTART:
      self.accounting[agentName]["Treatment"] = "Successfully Restarted"
      self.log.info("Agent %s has been successfully restarted" % agentName)

    return S_OK()

  def checkExecutor(self, executor, options):
    """Check the age of executor log file, if too old check for jobs in checking status, then restart the executors."""
    currentLogLocation = options['LogFileLocation']
    pid = options['PID']
    self.log.info("Checking executor: %s" % executor)
    self.log.info("Current Log File location: %s" % currentLogLocation)

    res = self.getLastAccessTime(currentLogLocation)
    if not res["OK"]:
      return res

    age = res["Value"]
    self.log.info("Current log file for %s is %d minutes old" % (executor, (age.seconds / MINUTES)))

    if age.seconds < 2 * HOUR:
      return S_OK()

    self.log.info("Current log file is too old for Executor %s" % executor)
    self.accounting[executor]["LogAge"] = age.seconds / MINUTES

    res = self.checkForCheckingJobs(executor)
    if not res['OK']:
      return res
    if res['OK'] and res['Value'] == NO_CHECKING_JOBS:
      self.accounting.pop(executor, None)
      return S_OK(NO_RESTART)

    res = self.restartInstance(int(pid), executor, self.restartExecutors)
    if not res["OK"]:
      return res
    elif res['OK'] and res['Value'] != NO_RESTART:
      self.accounting[executor]["Treatment"] = "Successfully Restarted"
      self.log.info("Executor %s has been successfully restarted" % executor)

    return S_OK()

  def checkForCheckingJobs(self, executorName):
    """Check if there are checking jobs with the **executorName** as current MinorStatus."""
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

  def componentControl(self):
    """Monitor and control component status as defined in the CS.

    Check for running and stopped components and ensure they have the proper status as defined in the CS
    Registry/Hosts/_HOST_/[Running|Stopped] sections

    :returns: :func:`~DIRAC:DIRAC.Core.Utilities.ReturnValues.S_OK`,
       :func:`~DIRAC:DIRAC.Core.Utilities.ReturnValues.S_ERROR`
    """
    # get the current status of the components

    resCurrent = self._getCurrentComponentStatus()
    if not resCurrent['OK']:
      return resCurrent
    currentStatus = resCurrent['Value']

    resDefault = self._getDefaultComponentStatus()
    if not resDefault['OK']:
      return resDefault
    defaultStatus = resDefault['Value']

    # ensure instances are in the right state
    shouldBe = {}
    shouldBe['Run'] = defaultStatus['Run'].intersection(currentStatus['Down'])
    shouldBe['Down'] = defaultStatus['Down'].intersection(currentStatus['Run'])
    shouldBe['Unknown'] = defaultStatus['All'].symmetric_difference(currentStatus['All'])

    self._ensureComponentRunning(shouldBe['Run'])
    self._ensureComponentDown(shouldBe['Down'])

    for instance in shouldBe['Unknown']:
      self.logError("Unknown instance", "%r, either uninstall or add to config" % instance)

    return S_OK()

  def _getCurrentComponentStatus(self):
    """Get current status for components."""
    resOverall = self.sysAdminClient.getOverallStatus()
    if not resOverall['OK']:
      return resOverall
    currentStatus = {'Down': set(), 'Run': set(), 'All': set()}
    informationDict = resOverall['Value']
    for systemsDict in informationDict.values():
      for system, instancesDict in systemsDict.items():
        for instanceName, instanceInfoDict in instancesDict.items():
          identifier = '%s__%s' % (system, instanceName)
          runitStatus = instanceInfoDict.get('RunitStatus')
          if runitStatus in ('Run', 'Down'):
            currentStatus[runitStatus].add(identifier)

    currentStatus['All'] = currentStatus['Run'] | currentStatus['Down']
    return S_OK(currentStatus)

  def _getDefaultComponentStatus(self):
    """Get the configured status of the components."""
    host = socket.gethostname()
    defaultStatus = {'Down': set(), 'Run': set(), 'All': set()}
    resRunning = gConfig.getOptionsDict(os.path.join('/Registry/Hosts/', host, 'Running'))
    resStopped = gConfig.getOptionsDict(os.path.join('/Registry/Hosts/', host, 'Stopped'))
    if not resRunning['OK']:
      return resRunning
    if not resStopped['OK']:
      return resStopped
    defaultStatus['Run'] = set(resRunning['Value'].keys())
    defaultStatus['Down'] = set(resStopped['Value'].keys())
    defaultStatus['All'] = defaultStatus['Run'] | defaultStatus['Down']

    if defaultStatus['Run'].intersection(defaultStatus['Down']):
      self.logError("Overlap in configuration", str(defaultStatus['Run'].intersection(defaultStatus['Down'])))
      return S_ERROR("Bad host configuration")

    return S_OK(defaultStatus)

  def _ensureComponentRunning(self, shouldBeRunning):
    """Ensure the correct components are running."""
    for instance in shouldBeRunning:
      self.log.info("Starting instance %s" % instance)
      system, name = instance.split('__')
      if self.controlComponents:
        res = self.sysAdminClient.startComponent(system, name)
        if not res['OK']:
          self.logError("Failed to start component:", "%s: %s" % (instance, res['Message']))
        else:
          self.accounting[instance]["Treatment"] = "Instance was down, started instance"
      else:
        self.accounting[instance]["Treatment"] = "Instance is down, should be started"

  def _ensureComponentDown(self, shouldBeDown):
    """Ensure the correct components are not running."""
    for instance in shouldBeDown:
      self.log.info("Stopping instance %s" % instance)
      system, name = instance.split('__')
      if self.controlComponents:
        res = self.sysAdminClient.stopComponent(system, name)
        if not res['OK']:
          self.logError("Failed to stop component:", "%s: %s" % (instance, res['Message']))
        else:
          self.accounting[instance]["Treatment"] = "Instance was running, stopped instance"
      else:
        self.accounting[instance]["Treatment"] = "Instance is running, should be stopped"

  def checkURLs(self):
    """Ensure that the running services have their URL in the Config."""
    self.log.info("Checking URLs")
    # get services again, in case they were started/stop in controlComponents
    gConfig.forceRefresh(fromMaster=True)
    res = self.getRunningInstances(instanceType='Services', runitStatus='All')
    if not res["OK"]:
      return S_ERROR("Failure to get running services")
    self.services = res["Value"]
    for service, options in self.services.iteritems():
      self.log.debug("Checking URL for %s with options %s" % (service, options))
      # ignore SystemAdministrator, does not have URLs
      if 'SystemAdministrator' in service:
        continue
      self._checkServiceURL(service, options)

    if self.csAPI.csModified and self.commitURLs:
      self.log.info("Commiting changes to the CS")
      result = self.csAPI.commit()
      if not result['OK']:
        self.logError('Commit to CS failed', result['Message'])
        return S_ERROR("Failed to commit to CS")
    return S_OK()

  def _checkServiceURL(self, serviceName, options):
    """Ensure service URL is properly configured in the CS."""
    url = self._getURL(serviceName, options)
    system = options['System']
    module = options['Module']
    self.log.info("Checking URLs for %s/%s" % (system, module))
    urlsConfigPath = os.path.join('/Systems', system, self.setup, 'URLs', module)
    urls = gConfig.getValue(urlsConfigPath, [])
    self.log.debug("Found configured URLs for %s: %s" % (module, urls))
    self.log.debug("This URL is %s" % url)
    runitStatus = options['RunitStatus']
    wouldHave = 'Would have ' if not self.commitURLs else ''
    if runitStatus == 'Run' and url not in urls:
      urls.append(url)
      message = "%sAdded URL %s to URLs for %s/%s" % (wouldHave, url, system, module)
      self.log.info(message)
      self.accounting[serviceName + "/URL"]["Treatment"] = message
      self.csAPI.modifyValue(urlsConfigPath, ",".join(urls))
    if runitStatus == 'Down' and url in urls:
      urls.remove(url)
      message = "%sRemoved URL %s from URLs for %s/%s" % (wouldHave, url, system, module)
      self.log.info(message)
      self.accounting[serviceName + "/URL"]["Treatment"] = message
      self.csAPI.modifyValue(urlsConfigPath, ",".join(urls))

  @staticmethod
  def _getURL(serviceName, options):
    """Return URL for the service."""
    system = options['System']
    port = options['Port']
    host = socket.gethostname()
    url = 'dips://%s:%s/%s/%s' % (host, port, system, serviceName)
    return url
