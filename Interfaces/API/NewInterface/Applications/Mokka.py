"""
Mokka: Simulation after Whizard or StdHepCut
"""
__RCSID__ = "$Id$"

from ILCDIRAC.Interfaces.API.NewInterface.LCApplication import LCApplication
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Workflow.Parameter import Parameter
import types, os

class Mokka(LCApplication):
  """ Call Mokka simulator (after Whizard, Pythia or StdHepCut)

  To ensure reproductibility, the RandomSeed is used as mcRunNumber. By default it's the jobID.

  Usage:

  >>> wh = Whizard()
  ...
  >>> mo = Mokka()
  >>> mo.getInputFromApp(wh)
  >>> mo.setSteeringFile("mysteer.steer")
  >>> mo.setMacFile('MyMacFile.mac')
  >>> mo.setStartFrom(10)

  Use setExtraCLIArguments if you want to pass CLI arguments to Mokka

  """
  def __init__(self, paramdict = None):

    self.StartFrom = 0
    self.MacFile = ''
    self.RandomSeed = 0
    self.mcRunNumber = 0
    self.DbSlice = ''
    self.DetectorModel = ''
    self.ProcessID = ''
    super(Mokka, self).__init__( paramdict )
    ##Those 5 need to come after default constructor
    self._modulename = 'MokkaAnalysis'
    self._moduledescription = 'Module to run MOKKA'
    self.appname = 'mokka'
    self.datatype = 'SIM'
    self.detectortype = 'ILD'
    self._paramsToExclude.extend( [ "outputDstPath", "outputRecPath", "OutputDstFile", "OutputRecFile" ] )

  def setRandomSeed(self, RandomSeed):
    """ Optional: Define random seed to use. Default is JobID.

    Also used as mcRunNumber.

    @param RandomSeed: Seed to use during integration and generation. Default is Job ID.
    @type RandomSeed: int
    """
    self._checkArgs( { 'RandomSeed' : types.IntType } )

    self.RandomSeed = RandomSeed

  def setmcRunNumber(self, runnumber):
    """ Optional: Define mcRunNumber to use. Default is 0. In Production jobs, is equal to RandomSeed

    @param runnumber: mcRunNumber parameter of Mokka
    @type runnumber: int
    """
    self._checkArgs( { 'runnumber' : types.IntType } )

    self.mcRunNumber = runnumber

  def setDetectorModel(self, detectorModel):
    """ Define detector to use for Mokka simulation

    @param detectorModel: Detector Model to use for Mokka simulation. Default is ??????
    @type detectorModel: string
    """
    self._checkArgs( { 'detectorModel' : types.StringTypes } )

    self.DetectorModel = detectorModel

  def setMacFile(self, macfile):
    """ Optional: Define Mac File. Useful if using particle gun.

    @param macfile: Mac file for Mokka
    @type macfile: string
    """
    self._checkArgs( { 'macfile' : types.StringTypes } )
    self.MacFile = macfile
    if os.path.exists(macfile) or macfile.lower().count("lfn:"):
      self.inputSB.append(macfile)
    elif self.MacFile:
      self._log.notice("Mac file not found locally and is not an lfn, I hope you know what you are doing...")
      self._log.notice("MacFile:", self.MacFile)
    else:
      pass

  def setStartFrom(self, startfrom):
    """ Optional: Define from where mokka starts to read in the generator file

    @param startfrom: from how mokka start to read the input file
    @type startfrom: int
    """
    self._checkArgs( { 'startfrom' : types.IntType } )
    self.StartFrom = startfrom


  def setProcessID(self, processID):
    """ Optional: Define the processID. This is added to the event header.

    @param processID: ID's process
    @type processID: string
    """
    self._checkArgs( { 'processID' : types.StringTypes } )
    self.ProcessID = processID


  def setDbSlice(self, dbSlice):
    """ Optional: Define the data base that will use mokka

    @param dbSlice: data base used by mokka
    @type dbSlice: string
    """
    self._checkArgs( { 'dbSlice' : types.StringTypes } )
    self.DbSlice = dbSlice
    if os.path.exists(dbSlice) or dbSlice.lower().count("lfn:"):
      self.inputSB.append(dbSlice)
    elif dbSlice:
      self._log.notice("Slice not found locally and is not an lfn, I hope you know what you are doing...")
      self._log.notice("DB slice:", self.DbSlice)
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

  def _checkConsistency(self):

    if not self.Version:
      return S_ERROR('No version found')

    if not self.SteeringFile :
      return S_ERROR('No Steering File')

    if not os.path.exists(self.SteeringFile) and not self.SteeringFile.lower().count("lfn:"):
      #res = Exists(self.SteeringFile)
      res = S_OK()
      if not res['OK']:
        return res

    #res = self._checkRequiredApp()
    #if not res['OK']:
    #  return res

    if not self._jobtype == 'User':
      self._listofoutput.append({"outputFile":"@{OutputFile}", "outputPath":"@{OutputPath}", "outputDataSE":'@{OutputSE}'})
      self.prodparameters['mokka_steeringfile'] = self.SteeringFile
      if self.DetectorModel:
        self.prodparameters['mokka_detectormodel'] = self.DetectorModel
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

    moduleinstance.setValue("RandomSeed",      self.RandomSeed)
    moduleinstance.setValue("detectorModel",   self.DetectorModel)
    moduleinstance.setValue("mcRunNumber",     self.mcRunNumber)
    moduleinstance.setValue("macFile",         self.MacFile)
    moduleinstance.setValue("startFrom",       self.StartFrom)
    moduleinstance.setValue("dbSlice",         self.DbSlice)
    moduleinstance.setValue("ProcessID",       self.ProcessID)
    moduleinstance.setValue("debug",           self.Debug)

  def _checkWorkflowConsistency(self):
    return self._checkRequiredApp()

  def _resolveLinkedStepParameters(self, stepinstance):
    if type(self._linkedidx) == types.IntType:
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
    return S_OK()
