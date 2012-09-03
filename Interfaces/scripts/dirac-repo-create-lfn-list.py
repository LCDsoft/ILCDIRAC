'''
Created on Apr 22, 2010

@author: Stephane Poss
'''

import DIRAC
from DIRAC.Core.Base import Script
from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
import sys

Script.parseCommandLine( ignoreErrors = False )
args = sys.argv

def usage():
  print 'Usage: %s repo' % (Script.scriptName)
  DIRAC.exit(2)
if len(args) < 2:
  usage()

repoLocation = args[1]
dirac = DiracILC(True, repoLocation)

exitCode = 0
dirac.monitorRepository(False)
lfns = []
lfns = dirac.retrieveRepositoryOutputDataLFNs()
print "lfnlist=["
for lfn in lfns :
  print '"LFN:%s",' % lfn
print "]"
DIRAC.exit(exitCode)
