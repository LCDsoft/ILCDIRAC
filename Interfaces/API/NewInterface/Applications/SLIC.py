"""
SLIC : Simulation after Whizard or StdHepCut
"""
__RCSID__ = "$Id$"

from ILCDIRAC.Interfaces.API.NewInterface.LCApplication import LCApplication
from ILCDIRAC.Core.Utilities.InstalledFiles import Exists
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Workflow.Parameter import Parameter
import types, os

class SLIC(LCApplication):
  """ Call SLIC simulator (after Whizard, Pythia or StdHepCut)

  Usage:

  >>> wh = Whizard()
  >>> slic = SLIC()
  >>> slic.getInputFromApp(wh)
  >>> slic.setSteeringFile("mymacrofile.mac")
  >>> slic.setStartFrom(10)

  Use setExtraCLIArguments in case you want to use CLI parameters

  """
  def __init__(self, paramdict = None):

    self.StartFrom = 0
    self.RandomSeed = 0
    self.DetectorModel = ''
    super(SLIC, self).__init__( paramdict )
    ##Those 5 need to come after default constructor
    self._modulename = 'SLICAnalysis'
    self._moduledescription = 'Module to run SLIC'
    self.appname = 'slic'
    self.datatype = 'SIM'
    self.detectortype = 'SID'
    self._paramsToExclude.extend( [ "outputDstPath", "outputRecPath", "OutputDstFile", "OutputRecFile" ] )


  def setRandomSeed(self, RandomSeed):
    """ Optional: Define random seed to use. Default is Job ID.

    @param RandomSeed: Seed to use during simulation.
    @type RandomSeed: int
    """
    self._checkArgs( { 'RandomSeed' : types.IntType } )

    self.RandomSeed = RandomSeed

  def setDetectorModel(self, detectorModel):
    """ Define detector to use for Slic simulation

    @param detectorModel: Detector Model to use for Slic simulation. Default is ??????
    @type detectorModel: string
    """
    self._checkArgs( { 'detectorModel' : types.StringTypes } )
    if detectorModel.lower().count("lfn:"):
      self.inputSB.append(detectorModel)
    elif detectorModel.lower().count(".zip"):
      if os.path.exists(detectorModel):
        self.inputSB.append(detectorModel)
      else:
        self._log.notice("Specified detector model does not exist locally, I hope you know what you're doing")


    self.DetectorModel = os.path.basename(detectorModel).replace(".zip","")


  def setStartFrom(self, startfrom):
    """ Optional: Define from how slic start to read in the input file

    @param startfrom: from how slic start to read the input file
    @type startfrom: int
    """
    self._checkArgs( { 'startfrom' : types.IntType } )
    self.StartFrom = startfrom


  def _userjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setUserJobFinalization(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('userjobmodules failed')
    return S_OK()

  def _prodjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setOutputComputeDataList(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('prodjobmodules failed')
    return S_OK()

  def _checkConsistency(self):

    if not self.Version:
      return S_ERROR('No version found')
    if self.SteeringFile:
      if not os.path.exists(self.SteeringFile) and not self.SteeringFile.lower().count("lfn:"):
        res = Exists(self.SteeringFile)
        if not res['OK']:
          return res

    #res = self._checkRequiredApp()
    #if not res['OK']:
    #  return res

    if not self._jobtype == 'User':
      self._listofoutput.append({"outputFile":"@{OutputFile}", "outputPath":"@{OutputPath}",
                                 "outputDataSE":'@{OutputSE}'})
      self.prodparameters['slic_steeringfile'] = self.SteeringFile
      self.prodparameters['detectorType'] = self.detectortype
      if self.DetectorModel:
        self.prodparameters['slic_detectormodel'] = self.DetectorModel

    if not self.StartFrom :
      self._log.info('No startFrom defined for Slic : start from the begining')

    return S_OK()

  def _applicationModule(self):

    md1 = self._createModuleDefinition()
    md1.addParameter(Parameter("RandomSeed",           0,    "int", "", "", False, False,
                               "Random seed for the generator"))
    md1.addParameter(Parameter("detectorModel",       "", "string", "", "", False, False,
                               "Detecor model for simulation"))
    md1.addParameter(Parameter("startFrom",            0,    "int", "", "", False, False,
                               "From how Slic start to read the input file"))
    md1.addParameter(Parameter("debug",            False,   "bool", "", "", False, False, "debug mode"))
    return md1

  def _applicationModuleValues(self, moduleinstance):

    moduleinstance.setValue("RandomSeed",      self.RandomSeed)
    moduleinstance.setValue("detectorModel",   self.DetectorModel)
    moduleinstance.setValue("startFrom",       self.StartFrom)
    moduleinstance.setValue("debug",           self.Debug)

  def _checkWorkflowConsistency(self):
    return self._checkRequiredApp()

  def _resolveLinkedStepParameters(self, stepinstance):
    if type(self._linkedidx) == types.IntType:
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
    return S_OK()
