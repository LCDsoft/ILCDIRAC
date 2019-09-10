#!/bin/env python
'''
Add software from CVMFS to the CS

Give list of applications, init_script path, MokkaDBSlice, [Clic|ILD]ConfigPath (if set)


Options:

  -P, --Platform <value>            Platform ex. x86_64-slc5-gcc43-opt
  -A, --Applications <value>        Comma separated list of applications
  -V, --Version <value>             Version name
  -C, --Comment <value>             Comment
  -S, --Script <value>              Full path to initScript
  -B, --Base <value>                Path to Installation Base
  -O, --Config <value>              Path To [Clic|ILD]Config (if it is in ApplicationPath)
  -Q, --DBSlice <value>             Path to Mokka DB Slice
  --MarlinPandoraVersion <value>    Version of MarlinPandora, needed for pandora_calibration_scripts
  --PandoraAnalysisVersion <value>  Version of PandoraAnalysis, needed for pandora_calibration_scripts
  -N, --dry-run                     DryRun: do not commit to CS


:since: Feb 18, 2015

'''
import os

from DIRAC.Core.Base import Script
from DIRAC import gLogger, gConfig, S_OK, S_ERROR, exit as dexit

__RCSID__ = "$Id$"

class Params(object):
  """Collection of Parameters set via CLI switches"""
  def __init__( self ):
    self.version = ''
    self.platform = 'x86_64-slc5-gcc43-opt'
    self.comment = ''
    self.applicationSet = set()
    self.dbSliceLocation = ''
    self.initScriptLocation = ''
    self.basePath = ''
    self.configPath = ''
    self.dryRun = False
    self.pandoraAnalysisVersion = 'HEAD'
    self.marlinPandoraVersion = 'HEAD'

  def setVersion(self, optionValue):
    self.version = optionValue
    return S_OK()

  def setPlatform(self, optionValue):
    self.platform = optionValue
    return S_OK()

  def setName(self, optionValue):
    apps = optionValue.split(',')
    self.applicationSet = { _.strip() for _ in apps }
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

  def setConfig(self, optionValue):
    self.configPath = optionValue
    return S_OK()

  def setDryrun(self, _):
    self.dryRun = True
    return S_OK()

  def _setPAV(self, val):
    self.pandoraAnalysisVersion = val
    return S_OK()

  def _setMPV(self, val):
    self.marlinPandoraVersion = val
    return S_OK()

  def checkConsistency(self):
    """Check if all necessary parameter were defined"""

    if not self.applicationSet:
      return S_ERROR("No applications have beend defined")

    if not self.version:
      return S_ERROR("Version must be given")

    appListLower = { _.lower() for _ in self.applicationSet }

    ## if we have only config applications we do not need the initScript or the
    ## basepath, just the config path
    if all( not app.endswith('config') for app in appListLower ):

      if not self.initScriptLocation:
        return S_ERROR("Initscript location is not defined")

      if not self.basePath:
        return S_ERROR("BasePath is not defined")

    if 'mokka' in appListLower and not self.dbSliceLocation:
      return S_ERROR("Mokka in application list, but not dbSlice location given")

    ## if we have a config package we need the configPath
    if any( app.endswith('config') for app in appListLower ) and not self.configPath:
      return S_ERROR("Config in application list, but no location given")

    for val in ( self.initScriptLocation, self.basePath, self.dbSliceLocation, self.configPath ):
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

    Script.registerSwitch("O:", "Config=", "Path To [Clic|ILD]Config (if it is in ApplicationPath)", self.setConfig)

    Script.registerSwitch("Q:", "DBSlice=", "Path to Mokka DB Slice", self.setDBSlice)
    Script.registerSwitch('', 'MarlinPandoraVersion=', 'Version of MarlinPandora, needed for '
                          'pandora_calibration_scripts', self._setMPV)
    Script.registerSwitch('', 'PandoraAnalysisVersion=', 'Version of PandoraAnalysis, needed for '
                          'pandora_calibration_scripts', self._setPAV)

    Script.registerSwitch("N", "dry-run", "DryRun: do not commit to CS", self.dryRun)


    Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                         '\nUsage:',
                                         '  %s [option|cfgfile] ...\n' % Script.scriptName ] ) )

class CVMFSAdder(object):
  """Container for all the objects and functions to add software to ILCDirac"""
  def __init__(self, cliParams ):
    from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
    self.modifiedCS = False
    self.softSec = "/Operations/Defaults/AvailableTarBalls"
    self.mailadress = 'ilc-dirac@cern.ch'
    self.cliParams = cliParams
    self.parameter = dict( softSec = self.softSec,
                           platform = cliParams.platform,
                           version = cliParams.version,
                           basepath = cliParams.basePath,
                           initsctipt=cliParams.initScriptLocation,
                           marlinPandoraVersion=cliParams.marlinPandoraVersion,
                           pandoraAnalysisVersion=cliParams.pandoraAnalysisVersion,
                         )
    self.applications = cliParams.applicationSet
    self.detmodels = {}
    self.csAPI = CSAPI()

  def findDDSimDetectorModels( self ):
    """ find all detector models in lcgeo and
    fill the self.detmodels dictionary with Detmodel as key and path as value

    :returns: None
    """
    for root,dirs,_files in os.walk( os.path.join( self.parameter["basepath"], "lcgeo" ) ):
      for direct in dirs:
        if root.endswith("compact"):
          ## the main xml file must have the same name as the folder
          xmlPath = os.path.join( root, direct, direct+".xml")
          if os.path.exists( xmlPath ):
            self.detmodels[direct] = xmlPath

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
    if self.modifiedCS and not self.cliParams.dryRun:
      gLogger.notice("Commiting changes to the CS")
      result = self.csAPI.commit()
      if not result[ 'OK' ]:
        gLogger.error('Commit failed with message = %s' % (result[ 'Message' ]))
        return S_ERROR("Failed to commit to CS")
      gLogger.info('Successfully committed changes to CS')
    else:
      gLogger.info('No modifications to CS required')
    return S_OK()

  def addAllToCS(self):
    """add all the applications to the CS, take care of special cases (mokka, ildconfig, ddsim,...)"""
    from ILCDIRAC.ILCTransformationSystem.Utilities.ReleaseHelper import insertCSSection

    for application in self.applications:
      csParameter = dict( CVMFSEnvScript = self.cliParams.initScriptLocation,
                          CVMFSPath      = self.parameter['basepath']
                        )

      if application == 'mokka':
        csParameter['CVMFSDBSlice'] = self.cliParams.dbSliceLocation

      if application == 'pandora_calibration_scripts':
        csParameter['MarlinPandora'] = os.path.join(self.parameter['basepath'], 'MarlinPandora',
                                                    self.parameter['marlinPandoraVersion'], 'scripts')
        csParameter['PandoraAnalysis'] = os.path.join(self.parameter['basepath'], 'PandoraAnalysis',
                                                      self.parameter['pandoraAnalysisVersion'], 'bin')
        if not os.path.exists(csParameter['PandoraAnalysis']):
          return S_ERROR('PandoraAnalysis folder %r not found' % csParameter['PandoraAnalysis'])
        if not os.path.exists(csParameter['MarlinPandora']):
          return S_ERROR('MarlinPandora folder %r not found' % csParameter['MarlinPandora'])
        del csParameter['CVMFSPath']

      if application == 'ddsim':
        self.findDDSimDetectorModels()

        csPathModels = "Operations/Defaults/DDSimDetectorModels"
        csModels = { self.parameter["version"] : self.detmodels }
        insertCSSection( self.csAPI, csPathModels, csModels )
        self.modifiedCS = True

      elif application.endswith('config'):
        del csParameter['CVMFSEnvScript']
        csParameter['CVMFSPath'] = self.cliParams.configPath
        if self.cliParams.dbSliceLocation:
          csParameter['CVMFSDBSlice'] = self.cliParams.dbSliceLocation

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
      result = self.csAPI.setOption(csPath+par, val)
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
