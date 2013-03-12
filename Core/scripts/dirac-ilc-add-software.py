#!/bin/env python
'''
Add specified software in CS. Allows not using the web interface, therefore reduces the error rate.

Created on May 5, 2010
'''
import sys
from DIRAC.Core.Base import Script
from DIRAC.FrameworkSystem.Client.NotificationClient       import NotificationClient

from DIRAC import gConfig, S_OK, S_ERROR, exit as dexit

import string, os, shutil
try:
  import hashlib as md5
except:
  import md5


class Params(object):
  def __init__( self ):
    self.version = ''
    self.platform = 'x86_64-slc5-gcc43-opt'
    self.comment = ''
    self.name = ''
    
  def setVersion(self, optionValue):
    self.version = optionValue
    return S_OK()
  
  def setPlatform(self, optionValue):
    self.platform = optionValue
    return S_OK()
  
  def setName(self, optionValue):
    self.name = optionValue
    return S_OK()
  
  def setComment(self, optionValue):
    self.comment = optionValue
    return S_OK()
  def registerSwitches(self):
    Script.registerSwitch("P:", "platform=", "Platform", self.setPlatform)
    Script.registerSwitch("N:", "name=", "Application name", self.setName)
    Script.registerSwitch("V:", "version=", "Version", self.setVersion)
    Script.registerSwitch("C:", "comment=", "Comment",self.setComment)
    Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                        '\nUsage:',
                                        '  %s [option|cfgfile] ...\n' % Script.scriptName ] ) )

def upload(path, appTar):
  """ Upload to storage
  """
  from DIRAC.DataManagementSystem.Client.ReplicaManager      import ReplicaManager
  from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
  from DIRAC.RequestManagementSystem.Client.RequestClient    import RequestClient
  rm = ReplicaManager()

  if not os.path.exists(appTar):
    print "Tar ball %s does not exists, cannot continue." % appTar
    return S_ERROR()
  if path.find("http://www.cern.ch/lcd-data") > -1:
    final_path = "/afs/cern.ch/eng/clic/data/software/"
    try:
      shutil.copy(appTar,"%s%s" % (final_path, appTar))
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
    request = RequestContainer()
    request.setCreationTime()
    requestClient = RequestClient()
    request.setRequestName('default_request.xml')
    request.setSourceComponent('ReplicateILCSoft')
    res = request.addSubRequest({'Attributes':{'Operation' : 'replicateAndRegister',
                                               'TargetSE' : 'IN2P3-SRM'},
                                 'Files':[{'LFN':lfnpath}]},
                                 'transfer')
    #res = rm.replicateAndRegister("%s%s"%(path,appTar),"IN2P3-SRM")
    if not res['OK']:
      return res
    requestName = os.path.basename(appTar).replace('.tgz', '')
    request.setRequestAttributes({'RequestName' : requestName})
    requestxml = request.toXML()['Value']
    res = requestClient.setRequest(requestName, requestxml)
    if not res['OK']:
      print 'Could not set replication request %s' % res['Message']
    return S_OK('Application uploaded')
  return S_OK()


if __name__=="__main__":
  cliParams = Params()
  cliParams.registerSwitches()
  Script.parseCommandLine( ignoreErrors = True )
  platform = cliParams.platform
  appName = cliParams.name
  appVersion = cliParams.version
  comment = cliParams.comment
  if (not platform) or (not appName) or not appVersion or not comment:
    Script.showHelp()
    dexit(2)
  from DIRAC.Interfaces.API.DiracAdmin                       import DiracAdmin
  
  diracAdmin = DiracAdmin()

  modifiedCS = False
  mailadress = 'ilc-dirac@cern.ch'
  
  softwareSection = "/Operations/Defaults/AvailableTarBalls"

  appTar = "%s%s.tgz" % (appName, appVersion)
  subject = '%s %s added to DIRAC CS' % (appName, appVersion)
  msg = 'New application %s %s declared into Configuration service\n %s' % (appName, appVersion, comment)

  md5sum = md5.md5(file(appTar).read()).hexdigest()

  av_platforms = gConfig.getSections(softwareSection, [])
  if av_platforms['OK']:
    if not platform in av_platforms['Value']:
      print "Platform %s unknown, available are %s." % (platform, string.join(av_platforms['Value'], ", "))
      print "If yours is missing add it in CS"
      dexit(255)
  else:
    print "Could not find all platforms available in CS"
    dexit(255)

  av_apps = gConfig.getSections("%s/%s" % (softwareSection, platform), [])
  if not av_apps['OK']:
    print "Could not find all applications available in CS"
    DIRAC.exit(255)

  if appName.lower() in av_apps['Value']:
    versions = gConfig.getSections("%s/%s/%s" % (softwareSection, platform, appName.lower()), [])
    if not versions['OK']:
      print "Could not find all versions available in CS"
      dexit(255)
    if appVersion in versions['Value']:
      print 'Application %s %s for %s already in CS, nothing to do' % (appName.lower(), appVersion, platform)
      dexit(0)
    else:
      result = diracAdmin.csSetOption("%s/%s/%s/%s/TarBall" % (softwareSection, platform, appName.lower(),
                                                             appVersion), appTar)
      if result['OK']:
        modifiedCS = True
        tarballurl = gConfig.getOption("%s/%s/%s/TarBallURL" % (softwareSection, platform, appName.lower()), "")
        if len(tarballurl['Value']) > 0:
          res = upload(tarballurl['Value'], appTar)
          if not res['OK']:
            print "Upload to %s failed" % tarballurl
            dexit(255)
      resutl = diracAdmin.csSetOption("%s/%s/%s/%s/Md5Sum" % (softwareSection, platform, appName.lower(),
                                                               appVersion), md5sum)
      if result['OK']:
        modifiedCS = True
      result = diracAdmin.csSetOptionComment("%s/%s/%s/%s/TarBall"%(softwareSection, platform,
                                                                    appName.lower(), appVersion), comment)
      if not result['OK']:
        print "Error setting comment in CS"

  else:
    result = diracAdmin.csSetOption("%s/%s/%s/%s/TarBall"%(softwareSection, platform, appName.lower(), appVersion), 
                                    appTar)
    if result['OK']:  
      modifiedCS = True
      tarballurl = gConfig.getOption("%s/%s/%s/TarBallURL" % (softwareSection, platform, appName.lower()), "")
      if len(tarballurl['Value']) > 0:
        res = upload(tarballurl['Value'], appTar)
        if not res['OK']:
          print "Upload to %s failed" % tarballurl
          dexit(255)
    resutl = diracAdmin.csSetOption("%s/%s/%s/%s/Md5Sum" % (softwareSection, platform, appName.lower(), appVersion),   
                                    md5sum)
    if result['OK']:
      modifiedCS = True
    result = diracAdmin.csSetOptionComment("%s/%s/%s/%s/TarBall" % (softwareSection, platform, appName.lower(), appVersion),
                                           comment)
    if not result['OK']:
      print "Error setting comment in CS"
      
  #Commit the changes if nothing has failed and the CS has been modified
  if modifiedCS:
    result = diracAdmin.csCommitChanges(False)
    print result
    if not result[ 'OK' ]:
      print 'ERROR: Commit failed with message = %s' % (result[ 'Message' ])
      dexit(255)
    else:
      print 'Successfully committed changes to CS'
      notifyClient = NotificationClient()
      print 'Sending mail for software installation %s' % (mailadress)
      res = notifyClient.sendMail(mailadress, subject, msg, 'stephane.poss@cern.ch', localAttempt = False)
      if not res[ 'OK' ]:
        print 'The mail could not be sent'
  else:
    print 'No modifications to CS required'

  dexit(0)