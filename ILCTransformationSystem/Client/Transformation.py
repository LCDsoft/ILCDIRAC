"""
This class is needed to get the Limited plugin support.
"""

from DIRAC.TransformationSystem.Client.Transformation import Transformation as DT

class Transformation(DT):
  def __init__(self, transID = 0, transClient = None):
    super( Transformation, self ).__init__(transID = 0, transClient = None)
    self.supportedPlugins += ['Limited', 'Sliced', 'SlicedLimited']
    
  def _checkLimitedPlugin( self ):
    return self._checkStandardPlugin()
  def _checkSlicedPlugin( self ):
    return self._checkStandardPlugin()
  def _checkSlicedLimitedPlugin( self ):
    return self._checkStandardPlugin()  