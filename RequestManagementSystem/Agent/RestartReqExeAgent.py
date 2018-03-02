"""Restart the an agent in case it gets stuck

This agent was intially designed to supervise the RequestExecutingAgent and
retart it in case it gets stuck.  At the moment the RequestExecutingAgent is the
only agent to really get stuck on a normal basis.  Can be extend to restart any
given agent of so desired.

The agent checks the age of the log file and if it is deemed too old will kill
the agent so that it is restarted automatically.

+----------------------------------------+-----------------------------------------+---------------------------------------+
|  **Option**                            |    **Description**                      |  **Example**                          |
+----------------------------------------+-----------------------------------------+---------------------------------------+
|  MaxLogAge                             | maximum Age of the log file in minues   | MaxLogAge = 60                        |
|                                        |                                         |                                       |
+----------------------------------------+-----------------------------------------+---------------------------------------+
|  AgentNames                            | name of the agent to monitor            | AgentNames=RequestExecutingAgent      |
|                                        |                                         |                                       |
|                                        |                                         |                                       |
+----------------------------------------+-----------------------------------------+---------------------------------------+


"""

# imports
import datetime
import os
import psutil

from collections import defaultdict

# from DIRAC
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.FrameworkSystem.Client.SystemAdministratorClient import SystemAdministratorClient
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient

__RCSID__ = "$Id$"
AGENT_NAME = "RequestManagement/RestartReqExeAgent"

#Define units
HOUR = 3600
MINUTES = 60
SECONDS = 1

########################################################################
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


  def beginExecution(self):
    """ Reload the configurations before every cycle """
    self.setup = self.am_getOption("Setup", self.setup)
    self.enabled = self.am_getOption("EnableFlag", self.enabled)
    self.diracLocation = os.environ.get("DIRAC", self.diracLocation)

    res = self.getAllRunningAgents()
    if not res["OK"]:
      return S_ERROR("Failure to get running agents")
    self.agents = res["Value"]

    return S_OK()


  def getAllRunningAgents(self):
    res = self.sysAdminClient.getOverallStatus()
    if not res["OK"]:
      self.log.error("Failure to get agents from system administrator client", res["Message"])
      return res

    val = res['Value']['Agents']
    runningAgents = {}
    for system, agents in  val.iteritems():
      for agentName, agentInfo in agents.iteritems():
        if agentInfo['Setup'] and agentInfo['Installed'] and agentInfo['RunitStatus'] == 'Run':
          confPath = cfgPath('/Systems/'+system+'/'+self.setup+'/Agents/'+agentName+'/PollingTime')
          runningAgents[agentName]["PollingTime"] = gConfig.getValue(confPath, HOUR)
          runningAgents[agentName]["LogFileLocation"] = os.path.join(self.diracLocation, 'runit', system, agentName,
                                                                     'log', 'current')
          runningAgents[agentName]["PID"] = agentInfo["PID"]

    return S_OK(runningAgents)

  def on_terminate(self, agentName, process):
    self.log.info("%s's process with ID: %s has been terminated successfully" % (agentName, process.pid))

  def execute( self ):
    """ execution in one cycle """
    ok = True
    for agentName, val in self.agents.iteritems():
      res = self._checkAgent(agentName, val["PollingTime"], val["LogFileLocation"], val["PID"])
      if not res['OK']:
        self.log.error("Failure when checking agent", "%s, %s" % (agentName, res['Message']))
        ok = False

    if not ok:
      return S_ERROR("Error during this cycle, check log")

    return S_OK()

  def _checkAgent(self, agentName, pollingTime, currentLogLocation, pid):
    """ docs... """

    self.log.info("Checking Agent: %s" % agentName)
    self.log.info("Polling Time: %s" % pollingTime)
    self.log.info("Current Log File location: %s" % currentLogLocation)

    ## get the age of the current log file
    lastAccessTime = 0
    try:
      lastAccessTime = os.path.getmtime(currentLogLocation)
      lastAccessTime = datetime.datetime.fromtimestamp(lastAccessTime)
    except OSError as e:
      self.log.error("Failed to access current log file", str(e))
      return S_ERROR("Failed to access current log file")

    now = datetime.datetime.now()
    age = now - lastAccessTime

    self.log.info("Current log file for %s is %d minutes old" % (agentName, (age.seconds/MINUTES)))

    maxLogAge = max(pollingTime+HOUR, 2*HOUR)
    if age.seconds > maxLogAge:
      self.log.info("Current log file is too old for Agent %s" % agentName)

      if not self.enabled:
        self.log.info("Restarting agents is disabled, please restart %s manually" % agentName)
        return S_OK()

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

      except psutil.Error as err:
        self.log.error("Exception occurred in terminating processes for", "%s: %s" % (agentName, err))

    return S_OK()
