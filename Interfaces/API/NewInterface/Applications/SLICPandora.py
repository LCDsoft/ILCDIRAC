"""
SLICPandora : Run Pandora in the SID context
"""

import os
import types

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Workflow.Parameter import Parameter

from ILCDIRAC.Interfaces.API.NewInterface.LCApplication import LCApplication
from ILCDIRAC.Core.Utilities.InstalledFiles import Exists

__RCSID__ = "$Id$"

class SLICPandora(LCApplication):
  """ Call SLICPandora

  Usage:

  >>> lcsim = LCSIM()
  ...
  >>> slicpandora = SLICPandora()
  >>> slicpandora.getInputFromApp(lcsim)
  >>> slicpandora.setPandoraSettings("~/GreatPathToHeaven/MyPandoraSettings.xml")
  >>> slicpandora.setStartFrom(10)

  Use :func:`setExtraCLIArguments` if you want to add arguments to the PandoraFrontend call

  """
  def __init__(self, paramdict = None):

    self.startFrom = 0
    self.pandoraSettings = ''
    self.detectorModel = ''
    super(SLICPandora, self).__init__( paramdict)
    ##Those 5 need to come after default constructor
    self._modulename = 'SLICPandoraAnalysis'
    self._moduledescription = 'Module to run SLICPANDORA'
    self.appname = 'slicpandora'
    self.datatype = 'REC'
    self.detectortype = 'SID'
    self._paramsToExclude.extend( [ "outputDstPath", "outputRecPath", "OutputDstFile", "OutputRecFile" ] )

  def setDetectorModel(self, detectorModel):
    """ Define detector to use for SlicPandora simulation

    :param string detectorModel: Detector Model to use for SlicPandora simulation.
    """
    self._checkArgs( { 'detectorModel' : types.StringTypes } )

    self.detectorModel = detectorModel
    if os.path.exists(detectorModel) or detectorModel.lower().count("lfn:"):
      self.inputSB.append(detectorModel)

  def setStartFrom(self, startfrom):
    """ Optional: Define from where slicpandora start to read in the input file

    :param int startfrom: from how slicpandora start to read the input file
    """
    self._checkArgs( { 'startfrom' : types.IntType } )
    self.startFrom = startfrom

  def setPandoraSettings(self, pandoraSettings):
    """ Optional: Define the path where pandora settings are

    :param string pandoraSettings: path where pandora settings are
    """
    self._checkArgs( { 'pandoraSettings' : types.StringTypes } )
    self.pandoraSettings = pandoraSettings
    if os.path.exists(pandoraSettings) or pandoraSettings.lower().count("lfn:"):
      self.inputSB.append(pandoraSettings)

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

    if not self.pandoraSettings:
      return S_ERROR("PandoraSettings not set, you need it")

    #res = self._checkRequiredApp()
    #if not res['OK']:
    #  return res

    if not self.startFrom :
      self._log.info('No startFrom defined for SlicPandora : start from the begining')

    if self._jobtype != 'User':
      self.prodparameters['slicpandora_steeringfile'] = self.steeringFile
      self.prodparameters['slicpandora_detectorModel'] = self.detectorModel


    return S_OK()

  def _applicationModule(self):

    md1 = self._createModuleDefinition()
    md1.addParameter(Parameter("pandorasettings",   "", "string", "", "", False, False,
                               "Pandora Settings"))
    md1.addParameter(Parameter("detectorxml",       "", "string", "", "", False, False,
                               "Detector model for simulation"))
    md1.addParameter(Parameter("startFrom",          0,    "int", "", "", False, False,
                               "From how SlicPandora start to read the input file"))
    md1.addParameter(Parameter("debug",          False,   "bool", "", "", False, False,
                               "debug mode"))
    return md1

  def _applicationModuleValues(self, moduleinstance):

    moduleinstance.setValue("pandorasettings",    self.pandoraSettings)
    moduleinstance.setValue("detectorxml",        self.detectorModel)
    moduleinstance.setValue("startFrom",          self.startFrom)
    moduleinstance.setValue("debug",              self.debug)

  def _checkWorkflowConsistency(self):
    return self._checkRequiredApp()

  def _resolveLinkedStepParameters(self, stepinstance):
    if isinstance( self._linkedidx, (int, long) ):
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
    return S_OK()
