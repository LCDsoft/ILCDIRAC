"""
Mokka: Simulation after Whizard or StdHepCut
"""

import os
import types

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Workflow.Parameter import Parameter

from ILCDIRAC.Interfaces.API.NewInterface.LCApplication import LCApplication

__RCSID__ = "$Id$"

class Mokka(LCApplication):
  """ Call Mokka simulator (after Whizard, Pythia or StdHepCut)

  To ensure reproductibility, the randomSeed is used as mcRunNumber. By default it's the jobID.

  Usage:

  >>> wh = Whizard()
  ...
  >>> mo = Mokka()
  >>> mo.getInputFromApp(wh)
  >>> mo.setSteeringFile("mysteer.steer")
  >>> mo.setMacFile('MyMacFile.mac')
  >>> mo.setStartFrom(10)

  Use :func:`setExtraCLIArguments` if you want to pass command line arguments to Mokka

  """
  def __init__(self, paramdict = None):

    self.startFrom = 0
    self.macFile = ''
    self.randomSeed = 0
    self.mcRunNumber = 0
    self.dbSlice = ''
    self.detectorModel = ''
    self.processID = ''
    super(Mokka, self).__init__( paramdict )
    ##Those 5 need to come after default constructor
    self._modulename = 'MokkaAnalysis'
    self._moduledescription = 'Module to run MOKKA'
    self.appname = 'mokka'
    self.datatype = 'SIM'
    self.detectortype = 'ILD'
    self._paramsToExclude.extend( [ "outputDstPath", "outputRecPath", "OutputDstFile", "OutputRecFile" ] )

  def setRandomSeed(self, randomSeed):
    """ Optional: Define random seed to use. Default is JobID.

    Also used as *mcRunNumber*.

    :param int randomSeed: Seed to use during integration and generation. Default is Job ID.
    """
    self._checkArgs( { 'randomSeed' : types.IntType } )

    self.randomSeed = randomSeed

  def setmcRunNumber(self, runnumber):
    """ Optional: Define mcRunNumber to use. Default is 0. In Production jobs, is equal to RandomSeed

    :param int runnumber: mcRunNumber parameter of Mokka
    """
    self._checkArgs( { 'runnumber' : types.IntType } )

    self.mcRunNumber = runnumber

  def setDetectorModel(self, detectorModel):
    """ Define detector to use for Mokka simulation

    :param string detectorModel: Detector Model to use for Mokka simulation.
    """
    self._checkArgs( { 'detectorModel' : types.StringTypes } )

    self.detectorModel = detectorModel

  def setMacFile(self, macfile):
    """ Optional: Define Mac File. Useful if using particle gun.

    :param string macfile: Macro file for Mokka
    """
    self._checkArgs( { 'macfile' : types.StringTypes } )
    self.macFile = macfile
    if os.path.exists(macfile) or macfile.lower().count("lfn:"):
      self.inputSB.append(macfile)
    elif self.macFile:
      self._log.notice("Mac file not found locally and is not an lfn, I hope you know what you are doing...")
      self._log.notice("MacFile:", self.macFile)
    else:
      pass

  def setStartFrom(self, startfrom):
    """ Optional: Define from where mokka starts to read in the generator file

    :param int startfrom: from which event mokka starts to read the input file
    """
    self._checkArgs( { 'startfrom' : types.IntType } )
    self.startFrom = startfrom


  def setProcessID(self, processID):
    """ Optional: Define the processID. This is added to the event header.

    :param string processID: process ID string

    """
    self._checkArgs( { 'processID' : types.StringTypes } )
    self.processID = processID


  def setDbSlice(self, dbSlice):
    """ Optional: Define the data base that will use mokka

    :param string dbSlice: database used by Mokka

    """
    self._checkArgs( { 'dbSlice' : types.StringTypes } )
    self.dbSlice = dbSlice
    if os.path.exists(dbSlice) or dbSlice.lower().count("lfn:"):
      self.inputSB.append(dbSlice)
    elif dbSlice:
      self._log.notice("Slice not found locally and is not an lfn, I hope you know what you are doing...")
      self._log.notice("DB slice:", self.dbSlice)
    else:
      pass


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

    if not self.steeringFile :
      return S_ERROR('No Steering File')

    #FIXME: delete dead code
    #if not os.path.exists(self.steeringFile) and not self.steeringFile.lower().count("lfn:"):
      ##res = Exists(self.SteeringFile)
      #res = S_OK()
      #if not res['OK']:
        #return res

    #res = self._checkRequiredApp()
    #if not res['OK']:
    #  return res

    if self._jobtype != 'User':
      self._listofoutput.append({"outputFile":"@{OutputFile}", "outputPath":"@{OutputPath}", "outputDataSE":'@{OutputSE}'})
      self.prodparameters['mokka_steeringfile'] = self.steeringFile
      if self.detectorModel:
        self.prodparameters['mokka_detectormodel'] = self.detectorModel
      self.prodparameters['detectorType'] = self.detectortype

    return S_OK()

  def _applicationModule(self):

    md1 = self._createModuleDefinition()
    md1.addParameter(Parameter("RandomSeed",           0,    "int", "", "", False, False,
                               "Random seed for the generator"))
    md1.addParameter(Parameter("mcRunNumber",          0,    "int", "", "", False, False,
                               "mcRunNumber parameter for Mokka"))
    md1.addParameter(Parameter("detectorModel",       "", "string", "", "", False, False,
                               "Detector model for simulation"))
    md1.addParameter(Parameter("macFile",             "", "string", "", "", False, False, "Mac file"))
    md1.addParameter(Parameter("startFrom",            0,    "int", "", "", False, False,
                               "From where Mokka start to read the input file"))
    md1.addParameter(Parameter("dbSlice",             "", "string", "", "", False, False, "Data base used"))
    md1.addParameter(Parameter("ProcessID",           "", "string", "", "", False, False, "Process ID"))
    md1.addParameter(Parameter("debug",            False,   "bool", "", "", False, False, "debug mode"))
    return md1

  def _applicationModuleValues(self, moduleinstance):

    moduleinstance.setValue("RandomSeed",      self.randomSeed)
    moduleinstance.setValue("detectorModel",   self.detectorModel)
    moduleinstance.setValue("mcRunNumber",     self.mcRunNumber)
    moduleinstance.setValue("macFile",         self.macFile)
    moduleinstance.setValue("startFrom",       self.startFrom)
    moduleinstance.setValue("dbSlice",         self.dbSlice)
    moduleinstance.setValue("ProcessID",       self.processID)
    moduleinstance.setValue("debug",           self.debug)

  def _checkWorkflowConsistency(self):
    return self._checkRequiredApp()

  def _resolveLinkedStepParameters(self, stepinstance):
    if isinstance( self._linkedidx, (int, long) ):
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
    return S_OK()
