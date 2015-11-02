'''
Created on Jul 25, 2011

:author: Stephane Poss
'''

__RCSID__ = "$Id$"

from DIRAC.Core.Base.AgentModule                               import AgentModule
from DIRAC                                                     import S_OK, gLogger
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient

from ILCDIRAC.OverlaySystem.Client.OverlaySystemClient         import OverlaySystemClient

AGENT_NAME = 'Overlay/ResetCounters'

class ResetCounters ( AgentModule ):
  """ Reset the number of jobs at all sites: some sites are not updated properly, so 
  once in a while it's needed to restore the correct number of jobs.
  It does not need to be exact, but enough to clear some of the jobs.
  """
  def initialize(self):
    """ Initialize the agent.
    """
    self.am_setOption( "PollingTime", 60 )
    self.ovc = OverlaySystemClient()
    self.jobmon = JobMonitoringClient()
    return S_OK()
  
  def execute(self):
    """ This is called by the Agent Reactor
    """
    res = self.ovc.getSites()
    if not res['OK']:
      return res
    sitedict = {}
    sites = res['Value']
    gLogger.info("Will update info for sites %s" % sites)
    for site in sites:
      attribdict = {"Site" : site, "ApplicationStatus": 'Getting overlay files'}
      res = self.jobmon.getCurrentJobCounters(attribdict)
      if not res['OK']:
        continue
      if res['Value'].has_key('Running'):
        sitedict[site] = res['Value']['Running']
      else:
        sitedict[site] = 0
    gLogger.info("Setting new values %s" % sitedict)    
    res = self.ovc.setJobsAtSites(sitedict)
    if not res['OK']:
      gLogger.error(res['Message'])
      return res
    
    return S_OK()
