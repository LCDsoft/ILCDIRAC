"""
This class is needed to get the Limited plugin support.
"""

from DIRAC.TransformationSystem.Client.Transformation import Transformation as DT

class Transformation(DT):
  def __init__(self, transID = 0, transClient = None):
    super( Transformation, self ).__init__(transID = 0, transClient = None)
    
  def _checkLimitedPlugin( self ):
    return self._checkStandardPlugin()