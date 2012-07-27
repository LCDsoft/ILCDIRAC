'''
Created on Mar 24, 2010

@author: sposs
'''
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.Dirac import Dirac
import sys

Script.parseCommandLine( ignoreErrors = False )
args = sys.argv

def usage():
  print 'Usage: %s repo' % (Script.scriptName)
  DIRAC.exit(2)
if len(args) < 2:
  usage()

repoLocation = args[1]
dirac = Dirac(True, repoLocation)

exitCode = 0
dirac.monitorRepository(False)
dirac.retrieveRepositoryData()

DIRAC.exit(exitCode)
