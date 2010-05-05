'''
Created on May 5, 2010

@author: sposs
'''
from DIRAC.FrameworkSystem.Client.NotificationClient     import NotificationClient
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
from DIRAC import gConfig
from DIRAC.Core.Base import Script
import string

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()
diracAdmin = DiracAdmin()
modifiedCS = False
mailadress = 'stephane.poss@cern.ch'

def usage():
  print 'Usage: %s <PLATFORM> <NAME> <VERSION>' %(Script.scriptName)
  DIRAC.exit(2)

def changeCS(path,val):
  val.sort()
  result = diracAdmin.csModifyValue(path,string.join(val,', '))
  print result
  if not result['OK']:
    print "Cannot modify value of %s" %path
    print result['Message']
    DIRAC.exit(255)

if len(args) < 3:
  usage()

softwareSection = "/Operations/AvailableTarBalls"
platform = "%s"%args[0]
appName = args[1]
appVersion = args[2]
subject = '%s %s add to DIRAC CS' %(args[1],args[2])
msg = 'New application %s %s declared into CS' %(args[1],args[2])

av_platforms = gConfig.getSections(softwareSection, [])
if av_platforms['OK']:
  if not platform in av_platforms['Value']:
    print "Platform %s unknown, available are %s."%(platform,string.join(av_platforms['Value'],", "))
    DIRAC.exit(255)
else:
  print "Could not find all platforms available in CS"
  DIRAC.exit(255)

av_apps = gConfig.getSections("%s/%s"%(softwareSection,platform),[])
if not av_apps['OK']:
  print "Could not find all applications available in CS"
  DIRAC.exit(255)

if appName in av_apps['Value']:
  versions = gConfig.getSections("%s/%s/%s"%(softwareSection,platform,appName),[])
  if not versions['OK']:
    print "Could not find all versions available in CS"
    DIRAC.exit(255)
  if appVersion in versions['Value']:
    print 'Application %s %s for %s already in CS, nothing to do'%(appName,appVersion,platform)
    DIRAC.exit(0)
else:
  result = diracAdmin.csSetOption("%s/%s/%s/%s/TarBall"%(softwareSection,platform,appName,appVersion),"%s%s.tgz"%(appName,appVersion))
  modifiedCS = True

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