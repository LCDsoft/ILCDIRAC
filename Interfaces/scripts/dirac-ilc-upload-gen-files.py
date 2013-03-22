'''
Created on Mar 21, 2013

@author: stephane
'''

from DIRAC.Core.Base import Script
from DIRAC import S_OK, S_ERROR

class Params(object):
  def __init__(self):
    self.dir = ''
    
  def setDir(self, opt):
    self.dir = opt
    return S_OK()
  
  def registerSwitched(self):
    Script.registerSwitch('P:', 'Path=', 'Path where the files are', self.setDir)  
    Script.setUsageMessage('%s -P ' % Script.scriptName)
    
if __name__ == '__main__':
  clip = Params()
  
  Script.parseCommandLine()
  if not clip.dir:
    gLogger.error('You need the path')
    Script.showHelp()
    dexit(1)
    
  from DIRAC import gLogger, exit as dexit
  
  dexit(0)