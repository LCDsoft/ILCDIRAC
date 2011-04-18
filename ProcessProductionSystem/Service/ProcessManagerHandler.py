###########################################################################
# $HeadURL: $
###########################################################################

""" Services for ProcessProduction System
"""
__RCSID__ = " $Id: $ "

from DIRAC                                              import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler                    import RequestHandler
from types import *

from ILCDIRAC.ProcessProductionSystem.DB.ProcessDB import ProcessDB

# This is a global instance of the ProcessDB class
processDB = False

def initializeProcessManagerHandler( serviceInfo ):

  global processDB
  processDB = ProcessDB()
  return S_OK()

class ProcessManagerHandler(RequestHandler):

  types_addSoftware = [ StringTypes, StringTypes, StringTypes,StringTypes]
  def export_addSoftware(self,AppName,AppVersion,Comment,Path):
    """ Add new software in the DB
    """
    return processDB.addSoftware(AppName,AppVersion,Comment,Path)
  