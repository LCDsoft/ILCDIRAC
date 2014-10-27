'''
Created on Mar 24, 2010

@author: sposs
'''
from DIRAC.Core.Base import Script
from DIRAC import S_OK, exit as dexit

__RCSID__ = "$Id$"

class Params(object):
  """ CLI params class
  """
  def __init__( self ):
    self.repo = ''
  def setRepo(self, val):
    self.repo = val
    return S_OK()

  def registerSwitches(self):
    Script.registerSwitch( "r:", "repo=", "repo file", self.setRepo )
    # Define a help message
    Script.setUsageMessage( '\n'.join( [ __doc__,
                                         'Usage:',
                                         '  %s [option|cfgfile] ' % Script.scriptName] ) )


def getOutputData():
  cliParams = Params()
  cliParams.registerSwitches()
  Script.parseCommandLine( ignoreErrors = False )
  if not cliParams.repo:
    Script.showHelp()
    dexit(2)
  from DIRAC.Interfaces.API.Dirac import Dirac
  
  dirac = Dirac(True, cliParams.repo)

  exitCode = 0
  dirac.monitorRepository(False)
  dirac.retrieveRepositoryData()

  dexit(exitCode)

if __name__=="__main__":
  getOutputData()
