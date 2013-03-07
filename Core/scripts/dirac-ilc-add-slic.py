"""
Upload SLIC version and publish in CS
"""
from DIRAC.Core.Base import Script
import os, sys, shutil

from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.FrameworkSystem.Client.NotificationClient  import NotificationClient
from DIRAC.Interfaces.API.DiracAdmin                  import DiracAdmin
from DIRAC.Core.Security.Misc                         import getProxyInfo
from DIRAC                                            import gConfig, S_OK, S_ERROR
import DIRAC
from pydoc import cli
try:
  import hashlib as md5
except:
  import md5

class Params(object):
  def __init__( self ):
    self.version = ''
    self.tarballloc = ''
    self.comment = ''
    
  def setVersion( self, optionValue ):
    self.version = optionValue
    return S_OK()
  
  def setTarBallLoc( self, optionValue ):
    self.tarballloc = optionValue
    return S_OK()
  
  def setComment( self, optionValue ):
    self.comment = optionValue
    return S_OK()
  
  def registerSwitches(self):
    Script.registerSwitch("V:", "version=", "version", self.setVersion)
    Script.registerSwitch("T:", "tarball=", "path to local tar ball", self.setTarBallLoc)
    Script.registerSwitch("C:", "comment=", "Comment", self.setComment)

    Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                        '\nUsage:',
                                        '  %s [option|cfgfile] ...\n' % Script.scriptName ] ) )
    
def upload(path, appTar):
  rm = ReplicaManager()
  if not os.path.exists(appTar):
    print "Tar ball %s does not exists, cannot continue." % appTar
    return S_ERROR()
  if path.find("http://www.cern.ch/lcd-data") > -1:
    final_path = "/afs/cern.ch/eng/clic/data/software/"
    try:
      shutil.copy(appTar, "%s%s" % (final_path, appTar))
    except Exception, x:
      print "Could not copy because %s" % x
      return S_ERROR("Could not copy because %s" % x)
  elif path.find("http://") > -1:
    print "path %s was not forseen, location not known, upload to location yourself, and publish in CS manually" % path
    return S_ERROR()
  else:
    lfnpath = "%s%s" % (path, appTar)
    res = rm.putAndRegister(lfnpath, appTar, "CERN-SRM")
    if not res['OK']:
      return res
    res = rm.replicateAndRegister(lfnpath, "RAL-SRM")
    if not res['OK']:
      print "Replication to RAL-SRM failed"
    res = rm.replicateAndRegister(lfnpath, "IMPERIAL-SRM")
    if not res['OK']:
      print "Replication to IMPERIAL-SRM failed"
    res = rm.replicateAndRegister(lfnpath, "FNAL-SRM")
    if not res['OK']:
      print "Replication to FNAL-SRM failed"
    res = rm.replicateAndRegister(lfnpath, "IN2P3-SRM")
    if not res['OK']:
      print "Replication to IN2P3-SRM failed"
    return S_OK('Application uploaded')
  return S_OK()

if __name__=="__main__":
  cliParams = Params()
  cliParams.registerSwitches()
  Script.parseCommandLine( ignoreErrors = False )
  version = cliParams.version
  tarballloc = cliParams.tarballloc
  comment = cliParams.comment

  if not tarballloc and not version:
    Script.showHelp()
    sys.exit(2)

  if not os.path.exists(tarballloc):
    print "Cannot find the tar ball %s" % tarballloc
    sys.exit(2)
  diracAdmin = DiracAdmin()

  modifiedCS = False
  mailadress = 'ilc-dirac@cern.ch'


  softwareSection = "/Operations/Defaults/AvailableTarBalls/x86_64-slc5-gcc43/slic"

  tarballname = os.path.basename(tarballloc)
  appVersion = tarballname.slit("_")[0].split("-")[1]

  md5sum = md5.md5(file(tarballloc).read()).hexdigest()

  subject = 'slic %s added to DIRAC CS' % (appVersion)
  msg = 'New application slic %s declared into Configuration service\n %s' % (appVersion, comment)

  result = diracAdmin.csSetOption("%s/%s/TarBall" % (softwareSection, appVersion), tarballname)
  if result['OK']:
    modifiedCS = True
    tarballurl = gConfig.getOption("%s/TarBallURL" % (softwareSection),"")
    if len(tarballurl['Value']) > 0:
      res = upload(tarballurl['Value'], tarballname)
      if not res['OK']:
        print "Upload to %s failed" % tarballurl
        DIRAC.exit(255)
    result = diracAdmin.csSetOptionComment("%s/%s/TarBall" % (softwareSection, appVersion), comment)
    if not result['OK']:
      print "Error setting comment in CS"
  result = diracAdmin.csSetOption("%s/%s/Md5Sum" % (softwareSection, appVersion), md5sum)

  if modifiedCS:
    result = diracAdmin.csCommitChanges(False)
    print result
    if not result[ 'OK' ]:
      print 'ERROR: Commit failed with message = %s' % (result[ 'Message' ])
      DIRAC.exit(255)
    else:
      print 'Successfully committed changes to CS'
      notifyClient = NotificationClient()
      print 'Sending mail for software installation %s' % (mailadress)
      res = notifyClient.sendMail(mailadress, subject, msg, 'stephane.poss@cern.ch', localAttempt = False)
      if not res[ 'OK' ]:
        print 'The mail could not be sent'
  else:
    print 'No modifications to CS required'

  DIRAC.exit(0)  