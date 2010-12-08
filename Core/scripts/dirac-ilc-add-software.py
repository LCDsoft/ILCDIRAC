'''
Add specified software in CS. Allows not using the web interface, therefore reduces the error rate.

Created on May 5, 2010

@author: sposs
'''
from DIRAC.FrameworkSystem.Client.NotificationClient     import NotificationClient
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC import gConfig,S_OK,S_ERROR
import DIRAC
from DIRAC.Core.Base import Script
import string,os,shutil

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()
diracAdmin = DiracAdmin()
rm = ReplicaManager()
modifiedCS = False
mailadress = 'ilc-dirac@cern.ch'

def usage():
  print 'Usage: %s <PLATFORM> <NAME> <VERSION> [<COMMENT>]' %(Script.scriptName)
  DIRAC.exit(2)

def upload(path,appTar):
  if not os.path.exists(appTar):
    print "Tar ball %s does not exists, cannot continue."%appTar
    return S_ERROR()
  if path.find("http://www.cern.ch/lcd-data")>-1:
    final_path = "/afs/cern.ch/eng/clic/data/software/"
    try:
      shutil.copy(appTar,"%s%s"%(final_path,appTar))
    except Exception,x:
      print "Could not copy because %s"%x
      return S_ERROR("Could not copy because %s"%x)
  elif path.find("http://")>-1:
    print "path %s was not forseen, location not known, upload to location yourself, and publish in CS manually"%path
    return S_ERROR()
  else:
    res = rm.putAndRegister("%s%s"%(path,appTar),appTar,"CERN-SRM")
    if not res['OK']:
      return res
    res = rm.replicateAndRegister("%s%s"%(path,appTar),"IN2P3-SRM")
    if not res['Ok']:
      return res
    return S_OK('Application uploaded')
  return S_OK()

if len(args) < 3:
  usage()

softwareSection = "/Operations/AvailableTarBalls"
platform = "%s"%args[0]
appName = "%s"%args[1]
appVersion = "%s"%args[2]
comment = ""
if len(args)>3:
  comment = "%s"%args[3]
appTar = "%s%s.tgz"%(appName,appVersion)
subject = '%s %s added to DIRAC CS' %(args[1],args[2])
msg = 'New application %s %s declared into Configuration service\n %s' %(args[1],args[2],comment)

av_platforms = gConfig.getSections(softwareSection, [])
if av_platforms['OK']:
  if not platform in av_platforms['Value']:
    print "Platform %s unknown, available are %s."%(platform,string.join(av_platforms['Value'],", "))
    print "If yours is missing add it in CS"
    DIRAC.exit(255)
else:
  print "Could not find all platforms available in CS"
  DIRAC.exit(255)

av_apps = gConfig.getSections("%s/%s"%(softwareSection,platform),[])
if not av_apps['OK']:
  print "Could not find all applications available in CS"
  DIRAC.exit(255)

if appName.lower() in av_apps['Value']:
  versions = gConfig.getSections("%s/%s/%s"%(softwareSection,platform,appName.lower()),[])
  if not versions['OK']:
    print "Could not find all versions available in CS"
    DIRAC.exit(255)
  if appVersion in versions['Value']:
    print 'Application %s %s for %s already in CS, nothing to do'%(appName.lower(),appVersion,platform)
    DIRAC.exit(0)
  else:
    result = diracAdmin.csSetOption("%s/%s/%s/%s/TarBall"%(softwareSection,platform,appName.lower(),appVersion),appTar)
    if result['OK']:
      modifiedCS = True
      tarballurl = gConfig.getOption("%s/%s/%s/TarBallURL"%(softwareSection,platform,appName.lower()),"")
      if len(tarballurl['Value'])>0:
        res = upload(tarballurl['Value'],appTar)
        if not res['OK']:
          print "Upload to %s failed"%tarballurl
          DIRAC.exit(255)
    result = diracAdmin.csSetOptionComment("%s/%s/%s/%s/TarBall"%(softwareSection,platform,appName.lower(),appVersion),comment)
    if not result['OK']:
      print "Error setting comment in CS"

else:
  result = diracAdmin.csSetOption("%s/%s/%s/%s/TarBall"%(softwareSection,platform,appName.lower(),appVersion),appTar)
  if result['OK']:  
    modifiedCS = True
    tarballurl = gConfig.getOption("%s/%s/%s/TarBallURL"%(softwareSection,platform,appName.lower()),"")
    if len(tarballurl['Value'])>0:
      res = upload(tarballurl['Value'],appTar)
      if not res['OK']:
        print "Upload to %s failed"%tarballurl
        DIRAC.exit(255)
  result = diracAdmin.csSetOptionComment("%s/%s/%s/%s/TarBall"%(softwareSection,platform,appName.lower(),appVersion),comment)
  if not result['OK']:
    print "Error setting comment in CS"
#Commit the changes if nothing has failed and the CS has been modified
if modifiedCS:
  result = diracAdmin.csCommitChanges(False)
  print result
  if not result[ 'OK' ]:
    print 'ERROR: Commit failed with message = %s' %(result[ 'Message' ])
    DIRAC.exit(255)
  else:
    print 'Successfully committed changes to CS'
    notifyClient = NotificationClient()
    print 'Sending mail for software installation %s' %(mailadress)
    res = notifyClient.sendMail(mailadress,subject,msg,'stephane.poss@cern.ch',localAttempt=False)
    if not res[ 'OK' ]:
        print 'The mail could not be sent'
else:
  print 'No modifications to CS required'

DIRAC.exit(0)