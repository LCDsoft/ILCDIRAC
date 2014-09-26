#!/bin/env python
'''
Add specified software in CS. Allows not using the web interface, therefore reduces the error rate.

Created on May 5, 2010
'''

__RCSID__ = "$$"

from DIRAC.Core.Base import Script
from DIRAC import gLogger, gConfig, S_OK, exit as dexit
import os
try:
  import hashlib as md5
except ImportError:
  import md5


class Params(object):
  """Parameter object"""
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

class SoftwareAdder(object):
  """Container for all the objects and functions to add software to ILCDirac"""
  def __init__(self, platform, appName, tarball_loc, appVersion, comment):
    from DIRAC.Interfaces.API.DiracAdmin                       import DiracAdmin
    self.diracAdmin = DiracAdmin()
    self.modifiedCS = False
    self.appVersion = appVersion
    self.appName = appName
    self.platform = platform
    self.softSec = "/Operations/Defaults/AvailableTarBalls"

    self.appTar = self.checkForTarBall(tarball_loc)

    self.parameter = dict( softSec = self.softSec,
                           platform = self.platform,
                           appName = self.appName,
                           appname = self.appName.lower(),
                           appTar = self.appTar,
                           appTar_name = os.path.basename(self.appTar),
                           appVersion = self.appVersion,
                         )
    self.comment = comment
    self.mailadress = 'ilc-dirac@cern.ch'

  def checkConsistency(self):
    """checks if platform is defined, application exists, etc."""
    gLogger.notice("Checking consistency")
    av_platforms = gConfig.getSections(self.softSec, [])
    if av_platforms['OK']:
      if not self.platform in av_platforms['Value']:
        gLogger.error("Platform %s unknown, available are %s." % (self.platform, ", ".join(av_platforms['Value'])))
        gLogger.error("If yours is missing add it in CS")
        dexit(255)
    else:
      gLogger.error("Could not find all platforms available in CS")
      dexit(255)

    av_apps = gConfig.getSections("%(softSec)s/%(platform)s" % self.parameter, [])
    if not av_apps['OK']:
      gLogger.error("Could not find all applications available in CS")
      dexit(255)

  def commitToCS(self):
    """write changes to the CS to the server"""
    gLogger.notice("Commiting changes to the CS")
    if self.modifiedCS:
      result = self.diracAdmin.csCommitChanges(False)
      if not result[ 'OK' ]:
        gLogger.error('Commit failed with message = %s' % (result[ 'Message' ]))
        dexit(255)
      gLogger.info('Successfully committed changes to CS')
    else:
      gLogger.info('No modifications to CS required')

  def checkForTarBall(self,tarball_loc):
    """checks if the tarball exists"""
    gLogger.info("Check if tarball exists at %s" % tarball_loc)
    appTar = ''
    if tarball_loc:
      appTar = tarball_loc
      if self.appName == 'slic':
        self.appVersion = os.path.basename(tarball_loc).slit("_")[0].split("-")[1]
    else:
      if self.appVersion:
        appTar = "%s%s.tgz" % (self.appName, self.appVersion)
      else:
        gLogger.notice("Version not defined")

    if not os.path.exists(appTar):
      gLogger.error("Cannot find the file %s, exiting" % appTar)
      dexit(1)

    return appTar

  def notifyAboutNewSoftware(self):
    """Send an email to the mailing list if a new software version was defined"""

    #Only send email when something was actually added
    if not self.modifiedCS:
      return

    subject = '%s %s added to DIRAC CS' % (self.appName, self.appVersion)
    msg = 'New application %s %s declared into Configuration service\n %s' % (self.appName,
                                                                              self.appVersion,
                                                                              self.comment)
    from DIRAC.Core.Security.ProxyInfo import getProxyInfo
    from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getUserOption
    from DIRAC.FrameworkSystem.Client.NotificationClient       import NotificationClient

    notifyClient = NotificationClient()
    gLogger.notice('Sending mail for software installation to %s' % (self.mailadress))
    res = getProxyInfo()
    if not res['OK']:
      sender = 'ilcdirac-admin@cern.ch'
    else:
      if 'username' in res['Value']:
        sender = getUserOption(res['Value']['username'],'Email')
      else:
        sender = 'nobody@cern.ch'
    gLogger.info('*'*80)# surround email with stars
    res = notifyClient.sendMail(self.mailadress, subject, msg, sender, localAttempt = False)
    gLogger.info('*'*80)
    if not res[ 'OK' ]:
      gLogger.error('The mail could not be sent: %s' % res['Message'])



  def uploadTarBall(self):
    """get the tarballURL from the CS and upload the tarball there. Exits when error is encountered"""
    gLogger.notice("Uploading TarBall to the Grid")
    from ILCDIRAC.Core.Utilities.FileUtils import upload
    tarballurl = gConfig.getOption("%(softSec)s/%(platform)s/%(appname)s/TarBallURL" % self.parameter, "")
    if not tarballurl['OK'] or not tarballurl['Value']:
      gLogger.error('TarBallURL for application %(appname)s not defined' % self.parameter)
      dexit(255)
    res = upload(tarballurl['Value'], self.appTar)
    if not res['OK']:
      gLogger.error("Upload to %s failed" % tarballurl['Value'], res['Message'])
      dexit(255)


  def addVersionToCS(self):
    """adds the version of the application to the CS"""
    gLogger.notice("Adding version %(appVersion)s to the CS" % self.parameter)
    existingVersions = gConfig.getSections("%(softSec)s/%(platform)s/%(appname)s" % self.parameter, [])
    if not existingVersions['OK']:
      gLogger.error("Could not find all versions available in CS: %s" % existingVersions['Message'])
      dexit(255)
    if self.appVersion in existingVersions['Value']:
      gLogger.always('Application %s %s for %s already in CS, nothing to do' % (self.appName.lower(),
                                                                                self.appVersion,
                                                                                self.platform))
      dexit(0)

    result = self.diracAdmin.csSetOption("%(softSec)s/%(platform)s/%(appname)s/%(appVersion)s/TarBall" % self.parameter,
                                         self.parameter['appTar_name'])
    if result['OK']:
      self.modifiedCS = True
    else:
      gLogger.error ("Could not add version to CS")
      dexit(255)

  def addMD5SumToCS(self):
    """adds the MD5Sum of the Tarball fo the CS"""
    gLogger.notice("Adding MD5Sum to CS")
    md5sum = md5.md5(file(self.appTar).read()).hexdigest()
    result = self.diracAdmin.csSetOption("%(softSec)s/%(platform)s/%(appname)s/%(appVersion)s/Md5Sum" % self.parameter,
                                         md5sum)
    if result['OK']:
      self.modifiedCS = True
    else:
      gLogger.error("Could not add md5sum to CS")
      dexit(255)

  def addCommentToCS(self):
    """adds the comment for the TarBall to the CS"""
    gLogger.notice("Adding comment to CS: %s" % self.comment)
    result = self.diracAdmin.csSetOptionComment("%(softSec)s/%(platform)s/%(appname)s/%(appVersion)s/TarBall"% self.parameter,
                                                self.comment)
    if not result['OK']:
      gLogger.error("Error setting comment in CS")

  def addSoftware(self):
    """run all the steps to add software to grid and CS"""

    self.checkConsistency()
    self.addVersionToCS()
    self.addMD5SumToCS()
    self.addCommentToCS()
    self.uploadTarBall()
    self.commitToCS()
    self.notifyAboutNewSoftware()



def addSoftware():
  """uploads, registers, and sends email about new software package"""
  cliParams = Params()
  cliParams.registerSwitches()
  Script.parseCommandLine( ignoreErrors = True )
  platform = cliParams.platform
  appName = cliParams.name
  appVersion = cliParams.version
  comment = cliParams.comment
  tarball_loc = cliParams.tarball
  if not platform or not appName or not comment:
    Script.showHelp()
    dexit(2)

  softAdder = SoftwareAdder(platform, appName, tarball_loc, appVersion, comment)
  softAdder.addSoftware()

  gLogger.notice("All done!")
  dexit(0)

if __name__=="__main__":
  addSoftware()
