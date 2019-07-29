"""
OverlayInput : Helper call to define Overlay processor/driver inputs
"""
# pylint: disable=expression-not-assigned

import types
from collections import defaultdict

from ILCDIRAC.Interfaces.API.NewInterface.LCUtilityApplication import LCUtilityApplication
from ILCDIRAC.Workflow.Modules.OverlayInput import allowedBkg
from ILCDIRAC.Core.Utilities.OverlayFiles import energyWithLowerCaseUnit
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.Core.Workflow.Parameter import Parameter
from DIRAC import S_OK, S_ERROR, gLogger, gConfig

LOG = gLogger.getSubLogger(__name__)
__RCSID__ = "$Id$"


class OverlayInput(LCUtilityApplication):
  """ Helper call to define Overlay processor/driver inputs.

  Example:

  >>> over = OverlayInput()
  >>> over.setBXOverlay(300)
  >>> over.setGGToHadInt(3.2)
  >>> over.setNumberOfSignalEventsPerJob(10)
  >>> over.setBackgroundType("gghad")
  >>> over.setMachine("clic_opt")
  >>> over.setDetectorModel("CLICdet_o3_v14")
  >>> over.setEnergy("3000")

  See list of available options:
  >>> over = OverlayInput()
  >>> over.printAvailableOptions()
  """
  def __init__(self, paramdict = None):
    self._ops = Operations()
    self.bxToOverlay = None
    self.numberOfGGToHadronInteractions = 0
    self.numberOfSignalEventsPerJob = 0
    self.backgroundEventType = ''
    self.prodID = 0
    self.machine = 'clic_cdr'
    self.detectorModel = ''
    self.useEnergyForFileLookup = True
    super(OverlayInput, self).__init__( paramdict )
    self.version = '1'
    self._modulename = "OverlayInput"
    self.appname = self._modulename
    self._moduledescription = 'Helper call to define Overlay processor/driver inputs'
    self.accountInProduction = False
    self._paramsToExclude.append('_ops')
    self.pathToOverlayFiles = ''
    self.processorName = ''

  def setMachine(self, machine):
    """Define the machine to use."""
    self._checkArgs({'machine': types.StringTypes})
    self.machine = machine

  def setProdID(self, pid):
    """Define the prodID to use as input, experts only."""
    self._checkArgs({'pid': types.IntType})
    self.prodID = pid
    return S_OK()

  def setUseEnergyForFileLookup(self, useEnergyForFileLookup):
    """Set the flag to use the energy meta data in the search of the background files.

    Disable the energy when you want to use files created for a different energy than the signal events

    :param bool useEnergyForFileLookup: Use the Energy in the metadata search or not
    """
    self._checkArgs({'useEnergyForFileLookup': types.BooleanType})
    self.useEnergyForFileLookup = useEnergyForFileLookup
    return S_OK()

  def setOverlayBXPerSigEvt(self, bxoverlay):
    """ Define number bunch crossings to overlay for each signal event.

    This is used to determine the number of required overlay events.
    It does not modify any of the actual application parameters using the overly input.
    Alias for :func:`setBXOverlay`

    :param int bxoverlay: Bunch crossings to overlay.
    """
    self._checkArgs({'bxoverlay': types.IntType})
    self.bxToOverlay = bxoverlay
    return S_OK()

  def setBXOverlay(self, bxoverlay):
    """ Define number bunch crossings to overlay for each signal event.

    This is used to determine the number of required overlay events.
    It does not modify any of the actual application parameters using the overly input.

    :param int bxoverlay: Bunch crossings to overlay.
    """
    return self.setOverlayBXPerSigEvt(bxoverlay)

  def setOverlayEvtsPerBX(self, ggtohadint):
    """ Define the number of overlay events per bunch crossing.

    This is used to determine the number of required overlay events.
    It does not modify any of the actual application parameters using the overly input.

    :param float ggtohadint: optional number of overlay events interactions per bunch crossing

    """
    self._checkArgs({'ggtohadint': types.FloatType})
    self.numberOfGGToHadronInteractions = ggtohadint
    return S_OK()

  def setGGToHadInt(self, ggtohadint):
    """Define the number of overlay events per bunch crossing.

    This is used to determine the number of required overlay events.
    It does not modify any of the actual application parameters using the overly input.

    Alias for :func:`setOverlayEvtsPerBX`

    :param float ggtohadint: optional number of overlay events interactions per bunch crossing
    """
    return self.setOverlayEvtsPerBX(ggtohadint)

  def setNbSigEvtsPerJob(self, nbsigevtsperjob):
    """Set the number of signal events per job.

    This is used to determine the number of required overlay events.
    It does not modify any of the actual application parameters using the overly input.

    :param int nbsigevtsperjob: Number of signal events per job

    """
    self._checkArgs({'nbsigevtsperjob': types.IntType})

    self.numberOfSignalEventsPerJob = nbsigevtsperjob
    return S_OK()


  def setDetectorModel(self, detectormodel):
    """ Set the detector type for the background files.

    Files are defined in the ConfigurationSystem: Operations/Overlay/<Accelerator>/<energy>/<Detector>

    :param str detectormodel: Detector type

    """
    self._checkArgs({'detectormodel': types.StringTypes})

    self.detectorModel = detectormodel
    return S_OK()

  def setPathToFiles(self, path):
    """Set the path to where the overlay files are located.

    Setting this option will ignore all other settings!

    :param str path: LFN path to the folder containing the overlay files
    """
    self._checkArgs({'path': types.StringTypes})
    self.pathToOverlayFiles = path
    return S_OK()

  def setBkgEvtType(self, backgroundEventType):
    """    Define the background type.

    .. deprecated:: 23r0
       Use :func:`setBackgroundType` instead

    :param str backgroundEventType: Background type.

    """
    self._checkArgs({'backgroundEventType': types.StringTypes})

    self.backgroundEventType = backgroundEventType
    return S_OK()

  def setBackgroundType(self, backgroundType):
    """Define the background type

    :param str backgroundType: Background type.

    """
    return self.setBkgEvtType(backgroundType)

  def setProcessorName(self, processorName):
    """Set the processorName to set the input files for.

    Necessary if multiple invocations of the overlay processor happen in marlin for example.
    Different processors must use different background types

    :param str processorName: Name of the Processor these input files are for
    """
    self._checkArgs({'processorName': types.StringTypes})
    self.processorName = processorName
    return S_OK()

  def setNumberOfSignalEventsPerJob(self, numberSignalEvents):
    """Alternative to :func:`setNbSigEvtsPerJob`

    Number used to determine the number of background files needed.

    :param int numberSignalEvents: Number of signal events per job
    """
    return self.setNbSigEvtsPerJob(numberSignalEvents)

  def _applicationModule(self):
    m1 = self._createModuleDefinition()
    m1.addParameter(Parameter("BXOverlay",       0,  "float", "", "", False, False,
                              "Bunch crossings to overlay"))
    m1.addParameter(Parameter("ggtohadint",      0,  "float", "", "", False, False,
                              "Optional number of gamma gamma -> hadrons interactions per bunch crossing, default is 3.2"))
    m1.addParameter(Parameter("NbSigEvtsPerJob", 0,    "int", "", "", False, False,
                              "Number of signal events per job"))
    m1.addParameter(Parameter("prodid",          0,    "int", "", "", False, False,
                              "ProdID to use"))
    m1.addParameter(Parameter("BkgEvtType",     "", "string", "", "", False, False,
                              "Background type."))
    m1.addParameter(Parameter("detectormodel",       "", "string", "", "", False, False,
                              "Detector type."))
    m1.addParameter(Parameter("machine",       "", "string", "", "", False, False,
                              "machine: clic_cdr or ilc_dbd"))
    m1.addParameter(Parameter("useEnergyForFileLookup", True, "bool", "", "", False, False,
                              "useEnergy to look for background files: True or False"))
    m1.addParameter(Parameter("pathToOverlayFiles", "", "string", "", "", False, False,
                              "use overlay files from this path"))
    m1.addParameter(Parameter("processorName",     "", "string", "", "", False, False,
                              "Processor Name"))

    m1.addParameter(Parameter("debug",          False,   "bool", "", "", False, False, "debug mode"))
    return m1


  def _applicationModuleValues(self, moduleinstance):
    moduleinstance.setValue("BXOverlay",         self.bxToOverlay)
    moduleinstance.setValue('ggtohadint',        self.numberOfGGToHadronInteractions)
    moduleinstance.setValue('NbSigEvtsPerJob',   self.numberOfSignalEventsPerJob)
    moduleinstance.setValue('prodid',            self.prodID)
    moduleinstance.setValue('BkgEvtType',        self.backgroundEventType)
    moduleinstance.setValue('detectormodel',     self.detectorModel)
    moduleinstance.setValue('debug',             self.debug)
    moduleinstance.setValue('machine',           self.machine  )
    moduleinstance.setValue('useEnergyForFileLookup', self.useEnergyForFileLookup  )
    moduleinstance.setValue('pathToOverlayFiles', self.pathToOverlayFiles )
    moduleinstance.setValue('processorName', self.processorName )

  def _userjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    if not res1["OK"] :
      return S_ERROR('userjobmodules failed')
    return S_OK()

  def _prodjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    if not res1["OK"] :
      return S_ERROR('prodjobmodules failed')
    return S_OK()

  def _addParametersToStep(self, stepdefinition):
    res = self._addBaseParameters(stepdefinition)
    if not res["OK"]:
      return S_ERROR("Failed to set base parameters")
    return S_OK()

  def _checkConsistency(self, job=None):
    """ Checks that all needed parameters are set
    """
    if self.pathToOverlayFiles:
      res = FileCatalogClient().findFilesByMetadata({}, self.pathToOverlayFiles)
      if not res['OK']:
        return res
      LOG.notice("Found %i files in path %s" % (len(res['Value']), self.pathToOverlayFiles))
      if not res['Value']:
        return S_ERROR("OverlayInput: PathToFiles is specified, but there are no files in that path")

    if not self.bxToOverlay :
      return S_ERROR("Number of overlay bunch crossings not defined")

    if not self.numberOfGGToHadronInteractions :
      return S_ERROR("Number of background events per bunch crossing is not defined")

    if not self.backgroundEventType :
      return S_ERROR("Background event type is not defined: Chose one gghad, aa_lowpt, ...")

    if self._jobtype == 'User' :
      if not self.numberOfSignalEventsPerJob :
        return S_ERROR("Number of signal event per job is not defined")
    else:
      self.prodparameters['detectorModel'] = self.detectorModel
      self.prodparameters['BXOverlay'] = self.bxToOverlay
      self.prodparameters['GGtoHadInt'] = self.numberOfGGToHadronInteractions

    return S_OK()

  @staticmethod
  def printAvailableOptions(machine=None, energy=None, detModel=None):
    """Print a list of the available options for each machine, energy and detector model.

    The parameters can be used to filter the output

    :param str machine: only list options for this machine
    :param str detModel: only list options for this detector model
    :param energy: only list options for this energy
    :type energy: int or float
    """
    overTree = gConfig.getConfigurationTree('/Operations/Defaults/Overlay')
    if not overTree['OK']:
      LOG.error('Could not find the configuration section "/Operations/Defaults/Overlay"')
      return

    if energy:
      energy = energyWithLowerCaseUnit(energy)

    # Dictionary of machines, energy, detModel, backgroundType
    availableOptions = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
    for value in overTree['Value']:
      values = value.strip('/').split('/')
      if len(values) <= 6:
        continue
      theMachine = values[3]
      theEnergy = values[4]
      theDetModel = values[5]
      backgroundType = values[6]
      if machine and machine != theMachine:
        continue
      if energy and energy != theEnergy:
        continue
      if detModel and detModel != theDetModel:
        continue
      availableOptions[theMachine][theEnergy][theDetModel].add(backgroundType)

    if machine or energy or detModel:
      if availableOptions:
        LOG.notice('Printing options compatible with')
      else:
        LOG.notice('No overlay options compatible with your selection')
      LOG.notice(' * Machine = %s' % machine) if machine else False
      LOG.notice(' * Energy = %s' % energy) if energy else False
      LOG.notice(' * DetModel = %s' % detModel) if detModel else False
    else:
      LOG.notice('All available overlay combinations')
    for theMachine, energies in availableOptions.items():
      LOG.notice('Machine: %s' % theMachine)
      for theEnergy, detModels in energies.items():
        LOG.notice('    %s' % theEnergy)
        for theDetModel, backgrounds in detModels.items():
          LOG.notice('        %s: %s' % (theDetModel, ', '.join(backgrounds)))

  def _checkFinalConsistency(self):
    """Check consistency before submission.

    The overlay files for the specifed energy must exist. Print all available overlay options on error
    """
    res = self.__checkFinalConsistency()
    if res['OK']:
      return res

    self.printAvailableOptions(machine=self.machine)
    self.printAvailableOptions(energy=self.energy)
    self.printAvailableOptions(detModel=self.detectorModel)
    return res

  def __checkFinalConsistency(self):
    """Check consistency of overlay options."""
    if self.pathToOverlayFiles:
      return S_OK() # can ignore other parameter

    if not self.energy:
      return S_ERROR("Energy MUST be specified for the overlay")

    res = self._ops.getSections('/Overlay')
    if not res['OK']:
      return S_ERROR("Could not resolve the CS path to the overlay specifications")
    sections = res['Value']
    if self.machine not in sections:
      return S_ERROR('Machine %r does not have overlay data' % self.machine)

    energytouse = energyWithLowerCaseUnit( self.energy )
    res = self._ops.getSections("/Overlay/%s" % self.machine)
    if energytouse not in res['Value']:
      return S_ERROR("No overlay files corresponding to %s" % energytouse)

    res = self._ops.getSections("/Overlay/%s/%s" % (self.machine, energytouse))
    if not res['OK']:
      return S_ERROR("Could not find the detector models")

    if self.detectorModel not in res['Value']:
      return S_ERROR('Detector model %r has no overlay data with energy %r and %r' %
                     (self.detectorModel, self.energy, self.machine))

    res = allowedBkg(self.backgroundEventType, energytouse, detectormodel=self.detectorModel, machine=self.machine)
    if not res['OK']:
      return res
    if res['Value'] < 0:
      return S_ERROR("No proper production ID found")
    return S_OK()
