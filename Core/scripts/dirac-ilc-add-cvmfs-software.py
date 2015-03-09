#!/bin/env python
'''
Add software from CVMFS to the CS

Give list of applications, init_script path, MokkaDBSlice, ILDConfigPath (if set)

Created on Feb 18, 2015
'''

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC import gLogger, gConfig, S_OK, S_ERROR, exit as dexit

import os

class Params(object):
  """Collection of Parameters set via CLI switches"""
  def __init__( self ):
    self.version = ''
    self.platform = 'x86_64-slc5-gcc43-opt'
    self.comment = ''
    self.applicationList = ''
    self.dbSliceLocation = ''
    self.initScriptLocation = ''
    self.basePath = ''
    self.ildConfigPath = ''

  def setVersion(self, optionValue):
    self.version = optionValue
    return S_OK()

  def setPlatform(self, optionValue):
    self.platform = optionValue
    return S_OK()

  def setName(self, optionValue):
    apps = optionValue.split(',')
    self.applicationList = [ _.strip() for _ in apps ]
    return S_OK()

  def setComment(self, optionValue):
    self.comment = optionValue
    return S_OK()

  def setDBSlice(self, optionValue):
    self.dbSliceLocation = optionValue
    return S_OK()

  def setInitScript(self, optionValue):
    self.initScriptLocation = optionValue
    return S_OK()

  def setBasePath(self, optionValue):
    self.basePath = optionValue
    return S_OK()

  def setILDConfig(self, optionValue):
    self.ildConfigPath = optionValue
    return S_OK()


  def checkConsistency(self):
    """Check if all necessary parameter were defined"""
    if not self.version:
      return S_ERROR("Version must be given")

    if not self.initScriptLocation:
      return S_ERROR("Initscript location is not defined")

    if not self.basePath:
      return S_ERROR("BasePath is not defined")

    if not self.applicationList:
      return S_ERROR("No applications have beend defined")

    appListLower = [ _.lower() for _ in self.applicationList ]

    if 'mokka' in appListLower and not self.dbSliceLocation:
      return S_ERROR("Mokka in application list, but not dbSlice location given")

    if 'ildconfig' in appListLower and not self.ildConfigPath:
      return S_ERROR("ILDConfig in application list, but no location given")

    for val in ( self.initScriptLocation, self.basePath, self.dbSliceLocation ):
      if val and not os.path.exists(val):
        gLogger.error("Cannot find this path:", val)
        return S_ERROR("CVMFS not mounted, or path is misstyped")

    return S_OK()



  def registerSwitches(self):
    Script.registerSwitch("P:", "Platform=", "Platform ex. %s" % self.platform, self.setPlatform)
    Script.registerSwitch("A:", "Applications=", "Comma separated list of applications", self.setName)
    Script.registerSwitch("V:", "Version=", "Version name", self.setVersion)
    Script.registerSwitch("C:", "Comment=", "Comment", self.setComment)
    Script.registerSwitch("S:", "Script=", "Full path to initScript", self.setInitScript)
    Script.registerSwitch("B:", "Base=", "Path to Installation Base", self.setBasePath)

    Script.registerSwitch("O:", "ILDConfig=", "Path To ILDConfig (if it is in ApplicationPath)", self.setILDConfig)

    Script.registerSwitch("Q:", "DBSlice=", "Path to Mokka DB Slice", self.setDBSlice)


    Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                         '\nUsage:',
                                         '  %s [option|cfgfile] ...\n' % Script.scriptName ] ) )

class CVMFSAdder(object):
  """Container for all the objects and functions to add software to ILCDirac"""
  def __init__(self, cliParams ):
    from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
    self.diracAdmin = DiracAdmin()
    self.modifiedCS = False
    self.softSec = "/Operations/Defaults/AvailableTarBalls"
    self.mailadress = 'ilc-dirac@cern.ch'
    self.cliParams = cliParams
    self.parameter = dict( softSec = self.softSec,
                           platform = cliParams.platform,
                           version = cliParams.version,
                           basepath = cliParams.basePath,
                           initsctipt = cliParams.initScriptLocation
                         )
    self.applications = cliParams.applicationList


  def checkConsistency(self):
    """checks if platform is defined, application exists, etc."""
    gLogger.notice("Checking consistency")
    av_platforms = gConfig.getSections(self.softSec, [])
    if av_platforms['OK']:
      if not self.parameter['platform'] in av_platforms['Value']:
        gLogger.error("Platform %s unknown, available are %s." % (self.parameter['platform'], ", ".join(av_platforms['Value'])))
        gLogger.error("If yours is missing, add it in CS")
        return S_ERROR()
    else:
      gLogger.error("Could not find all platforms available in CS")
      return S_ERROR()

    for application in self.applications:
      av_apps = gConfig.getSections("%(softSec)s/%(platform)s/" % self.parameter + str(application), [])
      if not av_apps['OK']:
        gLogger.error("Could not find this application in the CS: '%s'" % application)
        gLogger.error("Add its section to the CS, if it is missing")
        return S_ERROR()

    gLogger.notice("All OK, continuing...")
    return S_OK()


  def commitToCS(self):
    """write changes to the CS to the server"""
    if self.modifiedCS:
      gLogger.notice("Commiting changes to the CS")
      result = self.diracAdmin.csCommitChanges(False)
      if not result[ 'OK' ]:
        gLogger.error('Commit failed with message = %s' % (result[ 'Message' ]))
        return S_ERROR("Failed to commit to CS")
      gLogger.info('Successfully committed changes to CS')
    else:
      gLogger.info('No modifications to CS required')
    return S_OK()

  def addAllToCS(self):
    """add all the applications to the CS, take care of special cases (mokka, ildconfig,...)"""

    for application in self.applications:
      csParameter = dict( CVMFSEnvScript = self.cliParams.initScriptLocation,
                          CVMFSPath      = self.parameter['basepath']
                        )

      if application == 'mokka':
        csParameter['CVMFSDBSlice'] = self.cliParams.dbSliceLocation

      elif application == 'ildconfig':
        del csParameter['CVMFSEnvScript']
        csParameter['CVMFSPath'] = self.cliParams.ildConfigPath

      resInsert = self.insertApplicationToCS(application, csParameter)
      if not resInsert['OK']:
        return resInsert

    return S_OK()


  def insertApplicationToCS(self, name, csParameter):
    """add given application found via CVMFS to the CS"""

    pars = dict(self.parameter)
    pars['name'] = name

    gLogger.notice("%(name)s: Adding version %(version)s to the CS" % pars)

    existingVersions = gConfig.getSections("%(softSec)s/%(platform)s/%(name)s" % pars, [])
    if not existingVersions['OK']:
      gLogger.error("Could not find all versions available in CS: %s" % existingVersions['Message'])
      dexit(255)
    if pars['version'] in existingVersions['Value']:
      gLogger.always('Application %s %s for %s already in CS, nothing to do' % (name.lower(),
                                                                                pars['version'],
                                                                                pars['platform']))
      return S_OK()

    csPath = self.softSec + ("/%(platform)s/%(name)s/%(version)s/" % pars)
    for par, val in csParameter.iteritems():
      gLogger.notice("Add: %s = %s" %(csPath+par, val))
      result = self.diracAdmin.csSetOption(csPath+par, val)
      if result['OK']:
        self.modifiedCS = True
      else:
        gLogger.error("Failure to add to CS", result['Message'])
        return S_ERROR("")

    return S_OK()

  def addSoftware(self):
    """run all the steps to add software to grid and CS"""

    resAdd = self.addAllToCS()
    if not resAdd['OK']:
      return resAdd

    resCommit = self.commitToCS()
    if not resCommit['OK']:
      return resCommit

    return S_OK()

def addSoftware():
  """uploads, registers, and sends email about new software package"""
  cliParams = Params()
  cliParams.registerSwitches()
  Script.parseCommandLine( ignoreErrors = True )

  consistent = cliParams.checkConsistency()
  if not consistent['OK']:
    gLogger.error("Error checking consistency:", consistent['Message'])
    Script.showHelp()
    dexit(2)

  softAdder = CVMFSAdder(cliParams)
  resCheck = softAdder.checkConsistency()

  if not resCheck['OK']:
    Script.showHelp()
    dexit(2)

  softAdder.addSoftware()

  gLogger.notice("All done!")
  dexit(0)

if __name__=="__main__":
  addSoftware()
