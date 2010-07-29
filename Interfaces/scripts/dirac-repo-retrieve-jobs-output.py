'''
Created on Mar 24, 2010

@author: sposs
'''
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.Dirac import Dirac
import os,sys

Script.registerSwitch( "", "outputdata=","   retrieve also output data")

Script.parseCommandLine( ignoreErrors = False )

outputdata = False
repoLocation = None

def usage():
  print 'Usage: %s repo.cfg --outputdata=True' % (Script.scriptName)
  DIRAC.exit(2)
  
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "outputdata":
    outputdata=True

args = Script.getPositionalArgs()
if len(args)<2:
  usage()

repoLocation = args[1]

dirac=Dirac(True, repoLocation)

exitCode = 0
dirac.monitorRepository(False)
dirac.retrieveRepositorySandboxes()
if outputdata:
  dirac.retrieveRepositoryData()

DIRAC.exit(exitCode)
