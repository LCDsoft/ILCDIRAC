"""
This class is needed to get the Limited plugin support.
"""

__RCSID__ = "$Id$"
from DIRAC.TransformationSystem.Client.Transformation import Transformation as DT

class Transformation(DT):
  """ILCDirac version of Transformation"""
  def __init__(self, transID = 0, transClient = None):
    super( Transformation, self ).__init__(transID = 0, transClient = None)
    self.supportedPlugins += ['Limited', 'Sliced', 'SlicedLimited']
    
  def _checkLimitedPlugin( self ):
    """checks the Limitited Plugin, just passes the checkStandardPluging"""
    return self._checkStandardPlugin()
  def _checkSlicedPlugin( self ):
    """checks the Sliced Plugin, just passes the checkStandardPluging"""
    return self._checkStandardPlugin()
  def _checkSlicedLimitedPlugin( self ):
    """checks the SlicedLimited Plugin, just passes the checkStandardPluging"""
    return self._checkStandardPlugin()
