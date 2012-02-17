'''
Created on Feb 17, 2012

@author: Stephane Poss
'''
__RCSID__ = "$ Id: $"

from DIRAC                                                                import S_OK, S_ERROR, gConfig, gLogger, gMonitor
from DIRAC.Core.Base.AgentModule                                          import AgentModule

AGENT_NAME = 'ProcessProduction/SoftwareManagementAgent'

class SoftwareManagementAgent(AgentModule):
  """ Agent to run software management things
  """
  def initialize(self):
    self.pollingTime = self.am_getOption('PollingTime',86400)
    gMonitor.registerActivity("Iteration","Agent Loops",AGENT_NAME,"Loops/min",gMonitor.OP_SUM)

    return S_OK()

  ##############################################################################
  def execute(self):
    return S_OK()