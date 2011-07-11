###########################################################################
# $HeadURL: $
###########################################################################

""" Services for Overlay System
"""
__RCSID__ = " $Id: $ "

from DIRAC                                              import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler                    import RequestHandler

from ILCDIRAC.OverlayService.DB.OverlayDB import OverlayDB
# This is a global instance of the ProcessDB class
overlayDB = False

def initializeOverlayHandler( serviceInfo ):

  global overlayDB
  overlayDB = OverlayDB()
  return S_OK()

class OverlayHandler(RequestHandler):
  pass
