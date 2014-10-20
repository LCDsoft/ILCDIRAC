'''
Production Summary agent: writes out every day the production status 

Created on Apr 8, 2011

@author: Stephane Poss
'''
#pylint: skip-file
__RCSID__ = "$Id$"

from DIRAC                                                                import S_OK, gMonitor
from DIRAC.Core.Base.AgentModule                                          import AgentModule
from DIRAC.TransformationSystem.Client.TransformationClient               import TransformationClient
from DIRAC.Resources.Catalog.FileCatalogClient                            import FileCatalogClient

AGENT_NAME = 'Transformation/ProductionSummaryAgent'

class ProductionSummaryAgent( AgentModule ):
  """ Agent to produce the summary table that should be available in the twiki
  """
  def initialize(self):
    self.pollingTime = self.am_getOption('PollingTime', 86400)
    gMonitor.registerActivity("Iteration", "Agent Loops", AGENT_NAME, "Loops/min", gMonitor.OP_SUM)
    #self.transClient = TransformationClient('TransformationDB')
    self.transClient = TransformationClient()
    self.fcc = FileCatalogClient()
    return S_OK()

  ##############################################################################
  def execute(self):
    """Do something"""
    return S_OK()
