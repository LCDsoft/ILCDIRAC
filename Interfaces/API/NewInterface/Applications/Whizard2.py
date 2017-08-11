"""
Whizard2: Interface to the Whizard 2 generator

.. versionadded:: v26r0p14

Usage:

>>> whiz = Whizard2()
>>> whiz.setVersion("2.3.1")
>>> whiz.setNumberOfEvents(30)
>>> whiz.setRandomSeed(15)
>>> whiz.setSinFile("__path_to__/process.sin")

"""

import types
import os

from ILCDIRAC.Interfaces.API.NewInterface.LCApplication import LCApplication
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Workflow.Parameter import Parameter
from DIRAC.ConfigurationSystem.Client.Helpers.Operations  import Operations

__RCSID__ = "$Id$"

class Whizard2( LCApplication ):
  """ Whizard2 Application Class """

  def __init__(self, paramdict = None):
    self.randomSeed = -1
    self.eventType = ''
    self.whizard2SinFile = ''
    super(Whizard2, self).__init__( paramdict )
    ##Those 5 need to come after default constructor
    self._modulename = 'Whizard2Analysis'
    self._moduledescription = 'Module to run Whizard2'
    self.appname = 'whizard2'
    self.datatype = 'GEN'
    self._paramsToExclude.extend( [ "outputDstPath", "outputRecPath", "OutputDstFile", "OutputRecFile" ] )
    self._ops = Operations()

  def setRandomSeed(self, randomSeed):
    """ Optional: Define random seed to use. Default is the jobID.

    :param int randomSeed: Seed to use during generation.
    """
    self._checkArgs( { 'randomSeed' : types.IntType } )
    self.randomSeed = randomSeed

  def setEvtType(self, evttype):
    """ Define process. If the process given is not found, when calling :func:`UserJob.append() <ILCDIRAC.Interfaces.API.NewInterface.UserJob.UserJob.append>` a full list is printed.

    :param string evttype: Process to generate
    """
    self._checkArgs( { 'evttype' : types.StringTypes } )
    if self.addedtojob:
      return self._reportError("Cannot modify this attribute once application has been added to Job")
    self.eventType = evttype

  def setSinFile(self, whizard2SinFilePath):
    """ Set the Whizard2 options to be used

    Usage:
      - Give path to Whizard2 streeing file.
      - The decay process in the file should be stored in the variable decay_dec e.g.:
          process decay_dec = "A", "A" => "b", "B"
      - IMPORTANT set seed via iLCDirac API -> whizard2.setRandomSeed(1)
      - IMPORTANT set n_events via iLCDirac API  -> whizard2.setNumberOfEvents(100)
      - IMPORTANT set OutputFile via iLCDirac API -> whizard2.setOutputFile( outputFilename )

    :param string whizard2SinFilePath: Path to the whizard2 sin file.
    """
    self._checkArgs( { 'whizard2SinFilePath' : types.StringType } )

    # Chech if file exist
    if not os.path.isfile(whizard2SinFilePath):
      return self._reportError('Whizard2 Sin file does not exist!')

    # Read file
    self.whizard2SinFile = open(whizard2SinFilePath).read()
    # Check that the file follows the above mentioned rules
    if "processdecay_proc" not in self.whizard2SinFile.replace(" ", ""):
      return self._reportError('The sin file does not contain a decay string "process decay_proc"')

    if "n_events" in self.whizard2SinFile:
      return self._reportError('Do not set n_events in the sin file, set it via the iLCDirac API')

    if "seed" in self.whizard2SinFile:
      return self._reportError('Do not set seed in the sin file, set it via the iLCDirac API')

    if "simulate(decay_proc)" in self.whizard2SinFile.replace(" ", ""):
      return self._reportError('Do not call "simulate (decay_proc)" in the sin file, this is done by iLCDirac')

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
    """ FIXME
    Check consistency of the Whizard2 application, this is called from the `Job` instance

    :param job: The instance of the job
    :type job: ~ILCDIRAC.Interfaces.API.NewInterface.Job.Job
    :returns: S_OK/S_ERROR
    """
    if not self.version:
      return S_ERROR('No version found!')

    if not self.whizard2SinFile:
      return S_ERROR('No sin file set!')

    if not self.numberOfEvents :
      return S_ERROR('Number of events not set!')

    if self._jobtype != 'User':
      self._listofoutput.append({"outputFile":"@{OutputFile}", "outputPath":"@{OutputPath}",
                                 "outputDataSE":'@{OutputSE}'})

      if self.eventType != '':
        self.prodparameters['Process'] = self.eventType
      else:
        return S_ERROR('evttype not set, please set event type!')

      self.prodparameters['nbevts'] = self.numberOfEvents

      parsedString = self.whizard2SinFile.replace(" ", "").split()
      sqrtMatches = [ x for x in parsedString if x.startswith('sqrts=') and x.endswith('GeV') ]
      if not sqrtMatches:
        return S_ERROR('No energy set in sin file, please set "sqrts=...GeV"')
      elif len(sqrtMatches) != 1:
        return S_ERROR('Multiple instances of "sqrts=..GeV" detected, only one can be processed')
      self.prodparameters['Energy'] = sqrtMatches[0].replace("sqrts=", "").replace("GeV", "")

      modelMatches = [ x for x in parsedString if x.startswith('model=') ]
      if not modelMatches:
        return S_ERROR('No model set in sin file, please set "model=..."')
      elif len(modelMatches) != 1:
        return S_ERROR('Multiple instances of "model=..." detected, only one can be processed')
      self.prodparameters['Model'] = modelMatches[0].replace("model=", "")

    return S_OK()

  def _applicationModule(self):
    md1 = self._createModuleDefinition()
    md1.addParameter(Parameter("randomSeed",           0,    "int", "", "", False, False,
                               "Random seed for the generator"))
    md1.addParameter(Parameter("debug",            False,   "bool", "", "", False, False, "debug mode"))
    md1.addParameter(Parameter("whizard2SinFile",     '', "string", "", "", False, False, "Whizard2 steering options"))
    return md1

  def _applicationModuleValues(self, moduleinstance):
    moduleinstance.setValue("randomSeed",      self.randomSeed)
    moduleinstance.setValue("debug",           self.debug)
    moduleinstance.setValue("whizard2SinFile", self.whizard2SinFile)

  def _checkWorkflowConsistency(self):
    return self._checkRequiredApp()
