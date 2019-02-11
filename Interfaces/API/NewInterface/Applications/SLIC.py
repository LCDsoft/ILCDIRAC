"""
SLIC : Simulation after Whizard or StdHepCut
"""

import os
import types

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Workflow.Parameter import Parameter

from ILCDIRAC.Interfaces.API.NewInterface.LCApplication import LCApplication
from ILCDIRAC.Core.Utilities.InstalledFiles import Exists

LOG = gLogger.getSubLogger(__name__)
__RCSID__ = "$Id$"

class SLIC(LCApplication):
  """ Call SLIC simulator (after Whizard, Pythia or StdHepCut)

  Usage:

  >>> wh = Whizard()
  >>> slic = SLIC()
  >>> slic.getInputFromApp(wh)
  >>> slic.setSteeringFile("mymacrofile.mac")
  >>> slic.setStartFrom(10)

  Use :func:`setExtraCLIArguments` in case you want to use command line parameters

  """
  def __init__(self, paramdict = None):

    self.startFrom = 0
    self.randomSeed = 0
    self.detectorModel = ''
    super(SLIC, self).__init__( paramdict )
    ##Those 5 need to come after default constructor
    self._modulename = 'SLICAnalysis'
    self._moduledescription = 'Module to run SLIC'
    self.appname = 'slic'
    self.datatype = 'SIM'
    self.detectortype = 'SID'
    self._paramsToExclude.extend( [ "outputDstPath", "outputRecPath", "OutputDstFile", "OutputRecFile" ] )


  def setRandomSeed(self, randomSeed):
    """ Optional: Define random seed to use. Default is 0.

    :param int randomSeed: Seed to use during simulation.
    """
    self._checkArgs( { 'randomSeed' : types.IntType } )

    self.randomSeed = randomSeed

  def setDetectorModel(self, detectorModel):
    """ Define detector model to use for Slic simulation

    :param str detectorModel: Detector Model to use for Slic simulation.
    """
    self._checkArgs( { 'detectorModel' : types.StringTypes } )
    if detectorModel.lower().count("lfn:"):
      self.inputSB.append(detectorModel)
    elif detectorModel.lower().count(".zip"):
      if os.path.exists(detectorModel):
        self.inputSB.append(detectorModel)
      else:
        LOG.notice("Specified detector model does not exist locally, I hope you know what you're doing")


    self.detectorModel = os.path.basename(detectorModel).replace(".zip","")


  def setStartFrom(self, startfrom):
    """ Optional: Define from where slic starts to read in the input file

    :param int startfrom: from where slic starts to read the input file
    """
    self._checkArgs( { 'startfrom' : types.IntType } )
    self.startFrom = startfrom


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

  def _checkConsistency(self, job=None):

    if not self.version:
      return S_ERROR('No version found')
    if self.steeringFile:
      if not os.path.exists(self.steeringFile) and not self.steeringFile.lower().count("lfn:"):
        res = Exists(self.steeringFile)
        if not res['OK']:
          return res

    #res = self._checkRequiredApp()
    #if not res['OK']:
    #  return res

    if self._jobtype != 'User':
      self._listofoutput.append({"outputFile":"@{OutputFile}", "outputPath":"@{OutputPath}",
                                 "outputDataSE":'@{OutputSE}'})
      self.prodparameters['slic_steeringfile'] = self.steeringFile
      self.prodparameters['detectorType'] = self.detectortype
      if self.detectorModel:
        self.prodparameters['slic_detectormodel'] = self.detectorModel

    if not self.startFrom:
      LOG.info('No startFrom defined for Slic : start from the beginning')

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

    moduleinstance.setValue("RandomSeed",      self.randomSeed)
    moduleinstance.setValue("detectorModel",   self.detectorModel)
    moduleinstance.setValue("startFrom",       self.startFrom)
    moduleinstance.setValue("debug",           self.debug)

  def _checkWorkflowConsistency(self):
    return self._checkRequiredApp()

  def _resolveLinkedStepParameters(self, stepinstance):
    if isinstance( self._linkedidx, (int, long) ):
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
    return S_OK()
