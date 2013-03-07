'''
Created on Apr 22, 2010

@author: Stephane Poss
'''

import DIRAC
from DIRAC.Core.Base import Script
from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
import sys

class Params(object):
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
    
if __name__=="__main":
  cliparams = Params()
  cliparams.registerSwitches()
  Script.parseCommandLine( ignoreErrors = False )
  
  repoLocation =  cliparams.repo
  if not repoLocation:
    Script.showHelp()
    DIRAC.exit(2)
  dirac = DiracILC(True, repoLocation)
  
  dirac.monitorRepository(False)
  lfns = []
  lfns = dirac.retrieveRepositoryOutputDataLFNs()
  print "lfnlist=["
  for lfn in lfns :
    print '"LFN:%s",' % lfn
  print "]"
  DIRAC.exit(0)
