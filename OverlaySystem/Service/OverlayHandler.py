""" Services for Overlay System
"""

from types import StringTypes, DictType

from DIRAC                                              import S_OK
from DIRAC.Core.DISET.RequestHandler                    import RequestHandler

from ILCDIRAC.OverlaySystem.DB.OverlayDB                import OverlayDB

__RCSID__ = "$Id$"

#pylint: disable=unused-argument,no-self-use, global-statement

# This is a global instance of the OverlayDB class
OVERLAY_DB = False

def initializeOverlayHandler( serviceInfo ):
  """ Global initialize for the Overlay service handler
  """
  global OVERLAY_DB
  OVERLAY_DB = OverlayDB()
  return S_OK()

class OverlayHandler(RequestHandler):
  """ Service for Overlay
  """
  types_canRun = [StringTypes]
  def export_canRun(self, site):
    """ Check if current job can access the data
    """
    return OVERLAY_DB.canRun(site)

  types_jobDone = [StringTypes]
  def export_jobDone(self, site):
    """ report that a given job is done downloading the 
    files at a given site
    """
    return OVERLAY_DB.jobDone(site)
  
  types_getJobsAtSite =  [StringTypes]
  def export_getJobsAtSite(self, site):
    """ Get the jobs running at a given site
    """
    return OVERLAY_DB.getJobsAtSite(site)
  
  types_getSites = []
  def export_getSites(self):
    """ Get all sites registered
    """
    return OVERLAY_DB.getSites()
  
  types_setJobsAtSites = [ DictType ]
  def export_setJobsAtSites(self, sitedict):
    """ Set the number of jobs running at each site: 
    called from the ResetCounter agent
    """
    return OVERLAY_DB.setJobsAtSites(sitedict)
  