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
__RCSID__ = "$Id$"

# # imports
import datetime
import os

# # from DIRAC
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.FrameworkSystem.Client.SystemAdministratorClient import SystemAdministratorClient
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath

AGENT_NAME = "RequestManagement/RestartReqExeAgent"

#Define units
HOUR = 3600
MINUTES = 60
SECONDS = 1

########################################################################
class RestartReqExeAgent( AgentModule ): #pylint: disable=R0904
  """
  .. class:: RestartReqExeAgent

  """
  def initialize(self):

    self.setup = self.am_getOption("Setup", "Production")
    self.enabled = self.am_getOption("Enabled")
    self.diracLocation = os.environ.get("DIRAC", "/opt/dirac/pro")

    self.sysAdminClient = SystemAdministratorClient("localhost")

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
          runningAgents["PollingTime"] = gConfig.getValue(confPath, HOUR)
          runningAgents["LogFileLocation"] = os.path.join(self.diracLocation, 'runit', system, agentName,
                                                          'log', 'current')
          runningAgents["System"] = system

    return S_OK(runningAgents)

  def execute( self ):
    """ execution in one cycle """
    ok = True
    for agentName, val in self.agents.iteritems():
      res = self._checkAgent(agentName, val["PollingTime"], val["LogFileLocation"], val["System"])
      if not res['OK']:
        self.log.error("Failure when checking agent", "%s, %s" % (agentName, res['Message']))
        ok = False

    if not ok:
      return S_ERROR("Error during this cycle, check log")

    return S_OK()

  def _checkAgent(self, agentName, pollingTime, currentLogLocation, system):
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
      self.log.info("Current log file is too old! Restarting Agent %s" % agentName)
      res = self.sysAdminClient.restartComponent(system, agentName)
      if not res["OK"]:
        self.log.error("Failure to restart Agent", "%s %s" % (agentName, res["Message"]))
        return res

      self.log.info("Agent %s has been successfully restarted" % agentName)

    return S_OK()
