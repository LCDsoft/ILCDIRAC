#!/bin/env python
'''
Add specified software in CS. Allows not using the web interface, therefore reduces the error rate.

Created on May 5, 2010
'''
__RCSID__ = None

from DIRAC.Core.Base import Script

from DIRAC import gConfig, S_OK, exit as dexit

import os
try:
  import hashlib as md5
except ImportError:
  import md5


class Params(object):
  """Collection of Parameters set via CLI switches"""
  def __init__( self ):
    self.version = ''
    self.platform = 'x86_64-slc5-gcc43-opt'
    self.comment = ''
    self.name = ''
    self.tarball = ''
    
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
  
  def setTarBall(self, option):
    self.tarball = option
    return S_OK()
  
  def registerSwitches(self):
    Script.registerSwitch("P:", "Platform=", "Platform ex. %s" % self.platform, self.setPlatform)
    Script.registerSwitch("N:", "Name=", "Application name", self.setName)
    Script.registerSwitch("V:", "Version=", "Version", self.setVersion)
    Script.registerSwitch("T:", "TarBall=", "Tar ball location", self.setTarBall)
    Script.registerSwitch("C:", "Comment=", "Comment", self.setComment)
    Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                         '\nUsage:',
                                         '  %s [option|cfgfile] ...\n' % Script.scriptName ] ) )


def addSoftware():
  """Main Function"""
  cliParams = Params()
  cliParams.registerSwitches()
  Script.parseCommandLine( ignoreErrors = True )
  platform = cliParams.platform
  appName = cliParams.name
  appVersion = cliParams.version
  comment = cliParams.comment
  tarball_loc = cliParams.tarball
  if (not platform) or (not appName) or not comment:
    Script.showHelp()
    dexit(2)
    
  from DIRAC.Interfaces.API.DiracAdmin                       import DiracAdmin
  from DIRAC.FrameworkSystem.Client.NotificationClient       import NotificationClient
  from DIRAC import gLogger
  from ILCDIRAC.Core.Utilities.FileUtils import upload
  from DIRAC.Core.Security.ProxyInfo import getProxyInfo
  from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getUserOption
  diracAdmin = DiracAdmin()

  modifiedCS = False
  mailadress = 'ilc-dirac@cern.ch'
  
  softwareSection = "/Operations/Defaults/AvailableTarBalls"
  
  if tarball_loc:
    appTar = tarball_loc
    if appName == 'slic':
      appVersion = os.path.basename(tarball_loc).slit("_")[0].split("-")[1]
  else:
    if appVersion:
      appTar = "%s%s.tgz" % (appName, appVersion)
    else:
      gLogger.notice("Version not defined")

  if not os.path.exists(appTar):
    gLogger.error("Cannot find the file %s, exiting" % appTar)
    dexit(1)    
    
  appTar_name = os.path.basename(appTar)  
  subject = '%s %s added to DIRAC CS' % (appName, appVersion)
  msg = 'New application %s %s declared into Configuration service\n %s' % (appName, appVersion, comment)

  md5sum = md5.md5(file(appTar).read()).hexdigest()

  av_platforms = gConfig.getSections(softwareSection, [])
  if av_platforms['OK']:
    if not platform in av_platforms['Value']:
      gLogger.error("Platform %s unknown, available are %s." % (platform, ", ".join(av_platforms['Value'])))
      gLogger.error("If yours is missing add it in CS")
      dexit(255)
  else:
    gLogger.error("Could not find all platforms available in CS")
    dexit(255)

  av_apps = gConfig.getSections("%s/%s" % (softwareSection, platform), [])
  if not av_apps['OK']:
    gLogger.error("Could not find all applications available in CS")
    dexit(255)

  if appName.lower() in av_apps['Value']:
    versions = gConfig.getSections("%s/%s/%s" % (softwareSection, platform, appName.lower()), [])
    if not versions['OK']:
      gLogger.error("Could not find all versions available in CS")
      dexit(255)
    if appVersion in versions['Value']:
      gLogger.notice('Application %s %s for %s already in CS, nothing to do' % (appName.lower(), appVersion, platform))
      dexit(0)
    else:
      result = diracAdmin.csSetOption("%s/%s/%s/%s/TarBall" % (softwareSection, platform, appName.lower(),
                                                               appVersion), appTar_name)
      if result['OK']:
        modifiedCS = True
        tarballurl = gConfig.getOption("%s/%s/%s/TarBallURL" % (softwareSection, platform, appName.lower()), "")
        if len(tarballurl['Value']) > 0:
          res = upload(tarballurl['Value'], appTar)
          if not res['OK']:
            gLogger.error("Upload to %s failed" % tarballurl['Value'], res['Value'])
            dexit(255)
      result = diracAdmin.csSetOption("%s/%s/%s/%s/Md5Sum" % (softwareSection, platform, appName.lower(),
                                                              appVersion), md5sum)
      if result['OK']:
        modifiedCS = True
      result = diracAdmin.csSetOptionComment("%s/%s/%s/%s/TarBall"%(softwareSection, platform,
                                                                    appName.lower(), appVersion), comment)
      if not result['OK']:
        gLogger.error("Error setting comment in CS")

  else:
    result = diracAdmin.csSetOption("%s/%s/%s/%s/TarBall"%(softwareSection, platform, appName.lower(), appVersion), 
                                    appTar_name)
    if result['OK']:  
      modifiedCS = True
      tarballurl = gConfig.getOption("%s/%s/%s/TarBallURL" % (softwareSection, platform, appName.lower()), "")
      if len(tarballurl['Value']) > 0:
        res = upload(tarballurl['Value'], appTar)
        if not res['OK']:
          gLogger.error("Upload to %s failed" % tarballurl['Value'], res['Value'])
          dexit(255)
    result = diracAdmin.csSetOption("%s/%s/%s/%s/Md5Sum" % (softwareSection, platform, appName.lower(), appVersion),   
                                    md5sum)
    if result['OK']:
      modifiedCS = True
    result = diracAdmin.csSetOptionComment("%s/%s/%s/%s/TarBall" % (softwareSection, platform, appName.lower(), appVersion),
                                           comment)
    if not result['OK']:
      gLogger.error("Error setting comment in CS")
      
  #Commit the changes if nothing has failed and the CS has been modified
  if modifiedCS:
    result = diracAdmin.csCommitChanges(False)
    if not result[ 'OK' ]:
      gLogger.error('Commit failed with message = %s' % (result[ 'Message' ]))
      dexit(255)
    else:
      gLogger.info('Successfully committed changes to CS')
      notifyClient = NotificationClient()
      gLogger.info('Sending mail for software installation %s' % (mailadress))
      res = getProxyInfo()
      if not res['OK']:
        sender = 'stephane.poss@cern.ch'
      else:
        #sender = res['Value']['']
        if 'username' in res['Value']:
          sender = getUserOption(res['Value']['username'],'Email')
        else:
          sender = 'nobody@cern.ch'
      res = notifyClient.sendMail(mailadress, subject, msg, sender, localAttempt = False)
      if not res[ 'OK' ]:
        gLogger.error('The mail could not be sent')
  else:
    gLogger.info('No modifications to CS required')

  gLogger.notice("All done!")
  dexit(0)


if __name__=="__main__":
  addSoftware()
