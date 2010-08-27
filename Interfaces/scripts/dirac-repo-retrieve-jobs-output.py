'''
Created on Mar 24, 2010

@author: sposs
'''
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.Dirac import Dirac
import os,sys

Script.parseCommandLine( ignoreErrors = False )

outputdata = False
repoLocation = None

def usage():
  print 'Usage: %s repo.cfg' % (Script.scriptName)
  DIRAC.exit(2)
  

repoLocation = sys.argv[1]

dirac=Dirac(True, repoLocation)

exitCode = 0
dirac.monitorRepository(False)
dirac.retrieveRepositorySandboxes()
if outputdata:
  dirac.retrieveRepositoryData()

DIRAC.exit(exitCode)
