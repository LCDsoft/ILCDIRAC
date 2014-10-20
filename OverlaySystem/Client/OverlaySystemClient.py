'''
Created on Jul 25, 2011

@author: Stephane Poss
'''

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client                               import Client

class OverlaySystemClient (Client):
  """ Client of the OverlaySystemHandler. Used from the ResetCounter Agent
  """
  def __init__(self, **kwargs ):
    Client.__init__(self, **kwargs )
    self.setServer("Overlay/Overlay")

