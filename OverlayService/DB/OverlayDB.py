###########################################################################
# $HeadURL: $
###########################################################################

""" DB for Overlay System
"""
__RCSID__ = " $Id: $ "

from DIRAC                                                             import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB                                                import DB

class OverlayDB ( DB ):
  def __init__( self, maxQueueSize = 10 ):
    """ 
    """
    self.dbname = 'OverlayDB'
    DB.__init__( self, self.dbname, 'OverlaySystem/OverlayDB', maxQueueSize  )
    
    