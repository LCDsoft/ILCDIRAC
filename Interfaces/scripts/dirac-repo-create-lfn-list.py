#!/bin/env python
'''
Created on Apr 22, 2010

@author: Stephane Poss
'''
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC import exit as dexit
from DIRAC import S_OK
class Params(object):
  """dummy"""
  def __init__(self):
    self.repo = ''
  def setRepo(self, optionVal):
    self.repo = optionVal
    return S_OK()
  def registerSwitches(self):
    Script.registerSwitch('r:', 'repository=', 'Path to repository file', self.setRepo)
    Script.setUsageMessage('\n'.join( [ __doc__.split( '\n' )[1],
                                        '\nUsage:',
                                        '  %s [option|cfgfile] ...\n' % Script.scriptName ] ) )
    
def createLFNList():
  """create the LFnList"""
  cliparams = Params()
  cliparams.registerSwitches()
  Script.parseCommandLine( ignoreErrors = False )
  from DIRAC import gLogger
  
  repoLocation =  cliparams.repo
  if not repoLocation:
    Script.showHelp()
    dexit(2)
  from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
  dirac = DiracILC(True, repoLocation)
  
  dirac.monitorRepository(False)
  lfns = []
  lfns = dirac.retrieveRepositoryOutputDataLFNs()
  gLogger.notice("lfnlist=[")
  for lfn in lfns :
    gLogger.notice('"LFN:%s",' % lfn)
  gLogger.notice("]")
  dexit(0)

if __name__=="__main":
  createLFNList()
