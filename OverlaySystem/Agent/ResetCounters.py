'''
Created on Jul 25, 2011

@author: Stephane Poss
'''

__RCSID__ = "$Id$"

from DIRAC.Core.Base.AgentModule                               import AgentModule
from DIRAC                                                     import S_OK, S_ERROR, gLogger
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient

from ILCDIRAC.OverlaySystem.DB.OverlayDB                       import OverlayDB
from ILCDIRAC.OverlaySystem.Client.OverlaySystemClient         import OverlaySystemClient

AGENT_NAME = 'Overlay/ResetCounters'

class ResetCounters ( AgentModule ):
  def initialize(self):
    self.am_setOption( "PollingTime", 60 )
    self.ovc = OverlaySystemClient()
    self.jobmon = JobMonitoringClient()
    return S_OK()
  
  def execute(self):
    res = self.ovc.getSites()
    if not res['OK']:
      return res
    sitedict = {}
    sites = res['Value']
    gLogger.info("Will update info for sites %s"%sites)
    for site in sites:
      attribdict = {"Site":site,"ApplicationStatus":'Getting overlay files'}
      res = self.jobmon.getCurrentJobCounters(attribdict)
      if not res['OK']:
        continue
      if res['Value'].has_key('Running'):
        sitedict[site]=res['Value']['Running']
      else:
        sitedict[site]= 0
    gLogger.info("Setting new values %s"%sitedict)    
    res = self.ovc.setJobsAtSites(sitedict)
    if not res['OK']:
      gLogger.error(res['Message'])
      return res
    
    return S_OK()