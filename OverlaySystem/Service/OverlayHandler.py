###########################################################################
# $HeadURL: $
###########################################################################

""" Services for Overlay System
"""
__RCSID__ = " $Id: $ "

from DIRAC                                              import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler                    import RequestHandler

from ILCDIRAC.OverlaySystem.DB.OverlayDB                import OverlayDB
from types import *


# This is a global instance of the OverlayDB class
overlayDB = False

def initializeOverlayHandler( serviceInfo ):

  global overlayDB
  overlayDB = OverlayDB()
  return S_OK()

class OverlayHandler(RequestHandler):

  types_canRun = [StringTypes]
  def export_canRun(self,site):
    return overlayDB.canRun(site)

  types_jobDone = [StringTypes]
  def export_jobDone(self,site):
    return overlayDB.jobDone(site)
  
  types_getJobsAtSite =  [StringTypes]
  def export_getJobsAtSite(self,site):
    return overlayDB.getJobsAtSite(site)
  
  types_getSites = []
  def export_getSites(self):
    return overlayDB.getSites()
  
  types_setJobsAtSites = [ DictType ]
  def export_setJobsAtSites(self,sitedict):
    return overlayDB.setJobsAtSites(sitedict)
  