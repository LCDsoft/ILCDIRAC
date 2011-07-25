'''
Created on Jul 25, 2011

@author: Stephane Poss
'''
from DIRAC.Core.Base.Client                               import Client
from DIRAC                                                import S_OK, S_ERROR,gLogger,gConfig

class OverlaySystemClient (Client):
  def __init__(self):
    self.setServer("Overlay/Overlay")
    