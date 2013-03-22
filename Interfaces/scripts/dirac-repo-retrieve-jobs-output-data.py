'''
Created on Mar 24, 2010

@author: sposs
'''
from DIRAC.Core.Base import Script
from DIRAC import S_OK, exit as dexit

class Params(object):
  """ CLI params class
  """
  def __init__( self ):
    self.repo = ''
  def setRepo(self, val):
    self.repo = val
    return S_OK()

cliParams = Params()
Script.registerSwitch( "r:", "repo=", "repo file", cliParams.setRepo )
# Define a help message
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'Usage:',
                                     '  %s [option|cfgfile] ' % Script.scriptName] ) )
  
Script.parseCommandLine( ignoreErrors = False )

if __name__=="__main__":
  repoLocation = cliParams.repo
  if not repoLocation:
    Script.showHelp()
    dexit(2)
  from DIRAC.Interfaces.API.Dirac import Dirac
  
  dirac = Dirac(True, repoLocation)

  exitCode = 0
  dirac.monitorRepository(False)
  dirac.retrieveRepositoryData()

  dexit(exitCode)
