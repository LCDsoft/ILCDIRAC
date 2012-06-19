'''
Created on Jul 25, 2011

@author: Stephane Poss
'''
from DIRAC.Core.Base.Client                               import Client

class OverlaySystemClient (Client):
  def __init__(self):
    self.setServer("Overlay/Overlay")
    