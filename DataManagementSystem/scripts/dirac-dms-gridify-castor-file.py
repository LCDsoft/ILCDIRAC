#! /usr/bin/env python
from DIRAC.Core.Base                                      import Script
se = 'IN2P3-SRM'
Script.registerSwitch( "S:", "SE=","The destination storage element. Possibilities are CERN-SRM, IN2P3-SRM. [%s]" % se)
Script.parseCommandLine(ignoreErrors = True)
castorFiles = Script.getPositionalArgs()
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "s" or switch[0].lower() == "SE":
    se = switch[1]
import DIRAC
from DIRAC                                                import gLogger
from DIRAC.Core.Security.Misc                             import getProxyInfo
from ILCDIRAC.Interfaces.API.DiracILC                     import DiracILC
from DIRAC.DataManagementSystem.Client.ReplicaManager     import ReplicaManager
import re,os,hashlib

ilcdirac = DiracILC()
replicaManager = ReplicaManager()

res = getProxyInfo(False,False)
if not res['OK']:
  gLogger.error("Failed to get client proxy information.",res['Message'])
  DIRAC.exit(2)
proxyInfo = res['Value']
username = proxyInfo['username']
if not castorFiles:
  gLogger.info("No files suppied")
  gLogger.info("Usage: dirac-dms-gridify-castor-file castorpfn1 castorpfn2")
  gLogger.info("Try dirac-dms-gridify-castor-file  --help for options")
  DIRAC.exit(0)
exp = re.compile(r'/castor/cern.ch/user/[a-z]/[a-z]*/(\S+)$')
for physicalFile in castorFiles:
  if not physicalFile.startswith("/castor/cern.ch/user"):
    gLogger.info("%s is not a Castor user file (e.g. /castor/cern.ch/user/%s/%s). Ignored." % (physicalFile,username[0],username))
    continue
  if not re.findall(exp,physicalFile):
    gLogger.info("Failed to determine relative path for file %s. Ignored." % physicalFile)
    continue
  relativePath =  re.findall(exp,physicalFile)[0]
  gLogger.verbose("Found relative path of %s to be %s" % (physicalFile,relativePath))
  res = replicaManager.getStorageFile(physicalFile,'CERN-SRM',singleFile=True)
  localFile = os.path.basename(relativePath)
  if not res['OK']:
    gLogger.info("Failed to get local copy of %s" % physicalFile, res['Message'])
    if os.path.exists(localFile): os.remove(localFile)
    continue
  gLogger.verbose("Obtained local copy of %s at %s" % (physicalFile,localFile))
  lfn = '/ilc/user/%s/%s/Migrated/%s' % (username[0],username,relativePath)
  
  ##Calculate md5 sum
  md5 = hashlib.md5()
  f = file(localFile,"r")
  while not endOfFile:
    data = f.read(128)
    md5.update(data)
  
  res = ilcdirac.addFile(lfn,localfile,se,fileGuid=md5.digest(),printOutput=printOutput)
  if os.path.exists(localFile): os.remove(localFile)
  if not res['OK']:
    gLogger.error("Failed to upload %s to grid." % physicalFile,res['Message'])
    continue
  gLogger.info("Successfully uploaded %s to Grid. The corresponding LFN is %s" % (physicalFile,lfn))
DIRAC.exit(0)
