"""
StdhepCut: apply generator level cuts after pythia or whizard
"""
__RCSID__ = "$Id$"

from ILCDIRAC.Interfaces.API.NewInterface.LCApplication import LCApplication
from DIRAC.Core.Workflow.Parameter import Parameter
from DIRAC import S_OK, S_ERROR
import types

class StdhepCut(LCApplication):
  """ Call stdhep cut after whizard or pythia

  Usage:

  >>> py = Pythia()
  ...
  >>> cut = StdhepCut()
  >>> cut.getInputFromApp(py)
  >>> cut.setSteeringFile("mycut.cfg")
  >>> cut.setMaxNbEvts(10)
  >>> cut.setNbEvtsPerFile(10)

  """
  def __init__(self, paramdict = None):
    self.maxNumberOfEvents = 0
    self.numberOfEventsPerFile = 0
    self.selectionEfficiency = 0
    self.inlineCuts = ""
    super(StdhepCut, self).__init__( paramdict )

    self.appname = 'stdhepcut'
    self._modulename = 'StdHepCut'
    self._moduledescription = 'Module to cut on Generator (Whizard of PYTHIA)'
    self.datatype = 'gen'

  def setMaxNbEvts(self, nbevts):
    """ Max number of events passing cuts to write (number of events in the final file)

    @param nbevts: Maximum number of events passing cuts to write
    @type nbevts: int
    """
    self._checkArgs( { 'nbevts' : types.IntType } )
    self.maxNumberOfEvents = nbevts

  def setNbEvtsPerFile(self, nbevts):
    """ Number of events per file (not used)

    @param nbevts: Number of events to keep in each file.
    @type nbevts: int
    """
    self._checkArgs( { 'nbevts' : types.IntType } )
    self.numberOfEventsPerFile = nbevts

  def setSelectionEfficiency(self, efficiency):
    """ Selection efficiency of your cuts, needed to determine the number of files that will be created

    @param efficiency: Cut efficiency
    @type efficiency: float
    """
    self._checkArgs( { 'efficiency' : types.FloatType } )
    self.selectionEfficiency = efficiency

  def setInlineCuts(self, cutsstring):
    """ Define cuts directly, not by specifying a file
    @param cutsstring: Cut string. Can be multiline
    @type cutsstring: string
    """
    self._checkArgs( { 'cutsstring' : types.StringTypes } )

    self.inlineCuts = ";".join([cut.strip() for cut in cutsstring.strip().split("\n")])

  def _applicationModule(self):
    m1 = self._createModuleDefinition()
    m1.addParameter(Parameter("MaxNbEvts", 0, "int", "", "", False, False, "Number of events to read"))
    m1.addParameter(Parameter("debug", False, "bool", "", "", False, False, "debug mode"))
    m1.addParameter(Parameter("inlineCuts", "", "string", "", "", False, False, "Inline cuts"))

    return m1

  def _applicationModuleValues(self, moduleinstance):
    moduleinstance.setValue("MaxNbEvts", self.maxNumberOfEvents)
    moduleinstance.setValue("debug",     self.debug)
    moduleinstance.setValue("inlineCuts", self.inlineCuts )

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
    if not self.steeringFile and not self.inlineCuts:
      return S_ERROR("Cuts not specified")
    if self.steeringFile and self.inlineCuts:
      self._log.notice("You specifed a cuts file and InlineCuts. InlineCuts has precedence.")
    #elif not self.SteeringFile.lower().count("lfn:") and not os.path.exists(self.SteeringFile):
    # res = Exists(self.SteeringFile)
    # if not res['OK']:
    #   return res

    if not self.maxNumberOfEvents:
      return S_ERROR("You did not specify how many events you need to keep per file (MaxNbEvts)")

    if not self.selectionEfficiency:
      return S_ERROR('You need to know the selection efficiency of your cuts')

    if not self._jobtype == 'User':
      self._listofoutput.append({"outputFile":"@{OutputFile}", "outputPath":"@{OutputPath}",
                                 "outputDataSE":'@{OutputSE}'})
      self.prodparameters['nbevts_kept'] = self.maxNumberOfEvents
      self.prodparameters['cut_file'] = self.steeringFile

    #res = self._checkRequiredApp() ##Check that job order is correct
    #if not res['OK']:
    #  return res

    return S_OK()

  def _checkFinalConsistency(self):
    """ Final check of consistency: check that there are enough events generated
    """
    if not self.numberOfEvents:
      return S_ERROR('Please specify the number of events that will be generated in that step')

    kept = self.numberOfEvents * self.selectionEfficiency
    if kept < 2*self.maxNumberOfEvents:
      return S_ERROR("You don't generate enough events")

    return S_OK()


  def _checkWorkflowConsistency(self):
    return self._checkRequiredApp()

  def _resolveLinkedStepParameters(self, stepinstance):
    if type(self._linkedidx) == types.IntType:
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
    return S_OK()
