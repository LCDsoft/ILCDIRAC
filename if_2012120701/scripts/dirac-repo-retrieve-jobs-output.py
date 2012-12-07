'''
Created on Mar 24, 2010

@author: sposs
'''
from DIRAC.Core.Base import Script
import sys

outputdata = False
repoLocation = None

Script.registerSwitch( 'a:', 'outputdata', 'retrieve also the outputdata')
Script.registerSwitch( 'r:', 'repository=', 'repository file to use')
Script.setUsageMessage( sys.argv[0] + '-r <repo location> (-a)')

Script.parseCommandLine( ignoreErrors = False )
switches = Script.getUnprocessedSwitches()
for switch in switches:
  opt = switch[0]
  arg = switch[1]
  if opt in ('r', 'repository'):
    repoLocation = arg
  else:
    Script.showHelp()
  if opt in ('a', 'outputdata'):
    outputdata = True  

if not repoLocation:
  Script.showHelp()
  sys.exit(1)
    
import DIRAC
from DIRAC.Interfaces.API.Dirac import Dirac

dirac = Dirac(True, repoLocation)

exitCode = 0
res = dirac.monitorRepository(False)
if not res['OK']:
  print "Failed because %s" % res['Message']
  DIRAC.exit(1)
  
res = dirac.retrieveRepositorySandboxes()
if not res['OK']:
  print "Failed because %s" % res['Message']
  DIRAC.exit(1)
if outputdata:
  res = dirac.retrieveRepositoryData()
  if not res['OK']:
    print "Failed because %s" % res['Message']
    DIRAC.exit(1)
DIRAC.exit(exitCode)
