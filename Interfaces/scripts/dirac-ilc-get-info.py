#!/bin/env python

from DIRAC.Core.Base import Script
from DIRAC import S_OK, S_ERROR, exit as dexit

class Params(object):
  def __init__(self):
    self.file = ""
    self.prodid = 0
    
  def setFile(self, opt):
    self.file = opt
    return S_OK()

  def setProdID(self, opt):
    try:
      self.prodid = int(opt)
    except ValueError:
      return S_ERROR('Prod ID must be integer')
    return S_OK()
  
  def registerSwitch(self):
    Script.registerSwitch('p:', "ProductionID=", "Production ID", self.setProdID)
    Script.registerSwitch('f:', "File=", "File name", self.setFile)    
    Script.setUsageMessage("%s -p 12345" % Script.scriptName)
  
if __name__ == "__main__":
  clip = Params()
  clip.registerSwitch()
  Script.parseCommandLine(script = True)
  
  dexit(0)