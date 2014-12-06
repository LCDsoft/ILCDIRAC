"""
OverlayInput : Helper call to define Overlay processor/driver inputs
"""
__RCSID__ = "$Id$"

from ILCDIRAC.Interfaces.API.NewInterface.LCUtilityApplication import LCUtilityApplication
from ILCDIRAC.Workflow.Modules.OverlayInput import allowedBkg
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.Core.Workflow.Parameter import Parameter
from DIRAC import S_OK, S_ERROR
from decimal import Decimal
from math import modf
import types

class OverlayInput(LCUtilityApplication):
  """ Helper call to define Overlay processor/driver inputs.

  Example:

  >>> over = OverlayInput()
  >>> over.setBXOverlay(300)
  >>> over.setGGToHadInt(3.2)
  >>> over.setNbSigEvtsPerJob(10)
  >>> over.setBkgEvtType("gghad")

  """
  def __init__(self, paramdict = None):
    self._ops = Operations()
    self.BXOverlay = None
    self.GGToHadInt = 0
    self.NbSigEvtsPerJob = 0
    self.BkgEvtType = ''
    self.ProdID = 0
    self.Machine = 'clic_cdr'
    self.DetectorModel = ''
    self.useEnergyForFileLookup = True
    super(OverlayInput, self).__init__( paramdict )
    self.Version = '1'
    self._modulename = "OverlayInput"
    self.appname = self._modulename
    self._moduledescription = 'Helper call to define Overlay processor/driver inputs'
    self.accountInProduction = False
    self._paramsToExclude.append('_ops')
    self.pathToOverlayFiles = ''

  def setMachine(self, machine):
    """ Define the machine to use, clic_cdr or ilc_dbd
    """
    self._checkArgs( { 'machine' : types.StringTypes } )
    self.Machine = machine

  def setProdID(self, pid):
    """ Define the prodID to use as input, experts only
    """
    self._checkArgs( {'pid': types.IntType})
    self.ProdID = pid
    return S_OK()

  def setUseEnergyForFileLookup(self, useEnergyForFileLookup):
    """
    Sets the flag to use the energy meta data in the search of the background files.
    Disable the energy when you want to use files created for a different energy than the signal events

    @param useEnergyForFileLookup: Use the Energy in the metadata search or not
    @type useEnergyForFileLookup: bool
    """
    self._checkArgs( {'useEnergyForFileLookup': types.BooleanType } )
    self.useEnergyForFileLookup = useEnergyForFileLookup
    return S_OK()

  def setOverlayBXPerSigEvt( self, bxoverlay):
    """ Define number bunch crossings to overlay for each signal event.
    This is used to determine the number of required overlay events.
    It does not modify any of the actual application parameters using the overly input.

    @param bxoverlay: Bunch crossings to overlay.
    @type bxoverlay: float
    """
    self._checkArgs( { 'bxoverlay' : types.IntType } )
    self.BXOverlay = bxoverlay
    return S_OK()

  def setBXOverlay(self, bxoverlay):
    """ Define number bunch crossings to overlay for each signal event.
    This is used to determine the number of required overlay events.
    It does not modify any of the actual application parameters using the overly input.

    @param bxoverlay: Bunch crossings to overlay.
    @type bxoverlay: float
    """
    return self.setOverlayBXPerSigEvt( bxoverlay )

  def setOverlayEvtsPerBX( self, ggtohadint ):
    """ Define the number of overlay events per bunch crossing.
    This is used to determine the number of required overlay events.
    It does not modify any of the actual application parameters using the overly input.

    @param ggtohadint: optional number of overlay events interactions per bunch crossing
    @type ggtohadint: float

    """
    self._checkArgs( { 'ggtohadint' : types.FloatType } )
    self.GGToHadInt = ggtohadint
    return S_OK()

  def setGGToHadInt(self, ggtohadint):
    """ Define the number of overlay events per bunch crossing.
    This is used to determine the number of required overlay events.
    It does not modify any of the actual application parameters using the overly input.

    @param ggtohadint: optional number of overlay events interactions per bunch crossing
    @type ggtohadint: float

    """
    return self.setOverlayEvtsPerBX( ggtohadint )

  def setNbSigEvtsPerJob(self, nbsigevtsperjob):
    """ Set the number of signal events per job.
    This is used to determine the number of required overlay events.
    It does not modify any of the actual application parameters using the overly input.

    @param nbsigevtsperjob: Number of signal events per job
    @type nbsigevtsperjob: int

    """
    self._checkArgs( { 'nbsigevtsperjob' : types.IntType } )

    self.NbSigEvtsPerJob = nbsigevtsperjob
    return S_OK()


  def setDetectorModel(self, detectormodel):
    """ Set the detector type. Must be 'CLIC_ILD_CDR' or 'CLIC_SID_CDR' or 'sidloi3'

    @param detectormodel: Detector type. Must be 'CLIC_ILD_CDR' or 'CLIC_SID_CDR' or 'sidloi3'
    @type detectormodel: string

    """
    self._checkArgs( { 'detectormodel' : types.StringTypes } )

    self.DetectorModel = detectormodel
    return S_OK()

  def setPathToFiles(self, path):
    """ Sets the path to where the overlay files are located.
    Setting this option will ignore all other settings!

    @param path: LFN path to the folder containing the overlay files
    @type path: string

    """
    self._checkArgs( { 'path' : types.StringTypes } )
    self.pathToOverlayFiles = path
    return S_OK()

  def setBkgEvtType(self, BkgEvtType):
    """ Define the background type.

    @param BkgEvtType: Background type.
    @type BkgEvtType: string

    """
    self._checkArgs( { 'BkgEvtType' : types.StringTypes } )

    self.BkgEvtType = BkgEvtType
    return S_OK()



  def setBackgroundType(self, backgroundType):
    """Alternative to L{setBkgEvtType}

    @param backgroundType: Background type.
    @type backgroundType: string

    """
    return self.setBkgEvtType(backgroundType)

  def setNumberOfSignalEventsPerJob(self, numberSignalEvents):
    """Alternative to L{setNbSigEvtsPerJob}

    @param numberSignalEvents: Number of signal events per job
    @type numberSignalEvents: int

    """
    return self.setNbSigEvtsPerJob(numberSignalEvents)

#  def setProdIDToUse(self,prodid):
#    """ Optional parameter: Define the production ID to use as input
#
#    @param prodid: Production ID
#    @type prodid: int
#    """
#    self._checkArgs({"prodid" : types.IntType})
#    self.prodid = prodid
#    return S_OK()

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

    m1.addParameter(Parameter("debug",          False,   "bool", "", "", False, False, "debug mode"))
    return m1


  def _applicationModuleValues(self, moduleinstance):
    moduleinstance.setValue("BXOverlay",         self.BXOverlay)
    moduleinstance.setValue('ggtohadint',        self.GGToHadInt)
    moduleinstance.setValue('NbSigEvtsPerJob',   self.NbSigEvtsPerJob)
    moduleinstance.setValue('prodid',            self.ProdID)
    moduleinstance.setValue('BkgEvtType',        self.BkgEvtType)
    moduleinstance.setValue('detectormodel',     self.DetectorModel)
    moduleinstance.setValue('debug',             self.Debug)
    moduleinstance.setValue('machine',           self.Machine  )
    moduleinstance.setValue('useEnergyForFileLookup', self.useEnergyForFileLookup  )
    moduleinstance.setValue('pathToOverlayFiles', self.pathToOverlayFiles )

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

  def _checkConsistency(self):
    """ Checks that all needed parameters are set
    """
    if self.pathToOverlayFiles:
      res = FileCatalogClient().findFilesByMetadata({}, self.pathToOverlayFiles)
      self._log.notice("Found %i files in path %s" %( len(res['Value']), self.pathToOverlayFiles))
      if len(res['Value']) == 0 :
        return S_ERROR("OverlayInput: PathToFiles is specified, but there are no files in that path")

    if not self.BXOverlay :
      return S_ERROR("Number of overlay bunch crossings not defined")

    if not self.GGToHadInt :
      return S_ERROR("Number of background events per bunch crossing is not defined")

    if not self.BkgEvtType :
      return S_ERROR("Background event type is not defined: Chose one gghad, aa_lowpt, ...")

    if self._jobtype == 'User' :
      if not self.NbSigEvtsPerJob :
        return S_ERROR("Number of signal event per job is not defined")
    else:
      self.prodparameters['detectorModel'] = self.DetectorModel
      self.prodparameters['BXOverlay']  = self.BXOverlay
      self.prodparameters['GGtoHadInt'] = self.GGToHadInt

    return S_OK()

  def _checkFinalConsistency(self):
    """ Final check of consistency: the overlay files for the specifed energy must exist
    """
    if self.pathToOverlayFiles:
      return S_OK() # can ignore other parameter

    if not self.Energy:
      return S_ERROR("Energy MUST be specified for the overlay")

    res = self._ops.getSections('/Overlay')
    if not res['OK']:
      return S_ERROR("Could not resolve the CS path to the overlay specifications")
    sections = res['Value']
    if not self.Machine in sections:
      return S_ERROR("Machine %s does not have overlay data, use any of %s" % (self.Machine, sections))

    fracappen = modf(float(self.Energy)/1000.)
    if fracappen[1] > 0:
      energytouse = "%stev" % (Decimal(str(self.Energy))/Decimal("1000."))
    else:
      energytouse =  "%sgev" % (Decimal(str(self.Energy)))
    if energytouse.count(".0"):
      energytouse = energytouse.replace(".0", "")
    res = self._ops.getSections("/Overlay/%s" % self.Machine)
    if not energytouse in res['Value']:
      return S_ERROR("No overlay files corresponding to %s" % energytouse)

    res = self._ops.getSections("/Overlay/%s/%s" % (self.Machine, energytouse))
    if not res['OK']:
      return S_ERROR("Could not find the detector models")

    if not self.DetectorModel in res['Value']:
      return S_ERROR("Detector model specified has no overlay data with that energy and machine")

    res = allowedBkg(self.BkgEvtType, energytouse, detectormodel = self.DetectorModel, machine = self.Machine)
    if not res['OK']:
      return res
    if res['Value'] < 0:
      return S_ERROR("No proper production ID found")
    return S_OK()
