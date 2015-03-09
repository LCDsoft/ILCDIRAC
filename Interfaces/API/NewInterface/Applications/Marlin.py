"""
Marlin: Reconstructor after Mokka
"""
__RCSID__ = "$Id$"

from ILCDIRAC.Interfaces.API.NewInterface.LCApplication import LCApplication
from ILCDIRAC.Core.Utilities.CheckXMLValidity         import checkXMLValidity
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Workflow.Parameter import Parameter
import types, os

class Marlin(LCApplication):
  """ Call Marlin reconstructor (after Mokka simulator)

  Usage:

  >>> mo = Mokka()
  >>> marlin = Marlin()
  >>> marlin.getInputFromApp(mo)
  >>> marlin.setSteeringfile('SteeringFile.xml')
  >>> marlin.setOutputRecFile('MyOutputRecFile.rec')
  >>> marlin.setOutputDstFile('MyOutputDstFile.dst')

  Use setExtraCLIArguments if you want to get CLI parameters
  Needed for easy parameter scan, and passing non-standard strings (like cuts)

  """
  def __init__(self, paramdict = None):

    self.outputDstPath = ''
    self.OutputDstFile = ''
    self.outputRecPath = ''
    self.OutputRecFile = ''
    self.GearFile = ''
    self.ProcessorsToUse = []
    self.ProcessorsToExclude = []
    super(Marlin, self).__init__( paramdict )
    ##Those 5 need to come after default constructor
    self._modulename = 'MarlinAnalysis'
    self._moduledescription = 'Module to run MARLIN'
    self.appname = 'marlin'
    self.datatype = 'REC'
    self.detectortype = 'ILD'

  def setGearFile(self, GearFile):
    """ Define input gear file for Marlin

    @param GearFile: input gear file for Marlin reconstrcutor
    @type GearFile: string
    """
    self._checkArgs( { 'GearFile' : types.StringTypes } )

    self.GearFile = GearFile
    if os.path.exists(GearFile) or GearFile.lower().count("lfn:"):
      self.inputSB.append(GearFile)

  def setOutputRecFile(self, outputRecFile, path = None):
    """ Optional: Define output rec file for Marlin

    @param outputRecFile: output rec file for Marlin
    @type outputRecFile: string
    @param path: Path where to store the file. Used only in prouction context. Use setOutputData if
    you want to keep the file on the grid.
    @type path: string
    """
    self._checkArgs( { 'outputRecFile' : types.StringTypes } )
    self.OutputRecFile = outputRecFile
    self.prodparameters[self.OutputRecFile] = {}
    self.prodparameters[self.OutputRecFile]['datatype'] = 'REC'
    if path:
      self.outputRecPath = path

  def setOutputDstFile(self, outputDstFile, path = None):
    """ Optional: Define output dst file for Marlin

    @param outputDstFile: output dst file for Marlin
    @type outputDstFile: string
    @param path: Path where to store the file. Used only in prouction context. Use setOutputData if
    you want to keep the file on the grid.
    @type path: string
    """
    self._checkArgs( { 'outputDstFile' : types.StringTypes } )
    self.OutputDstFile = outputDstFile
    self.prodparameters[self.OutputDstFile] = {}
    self.prodparameters[self.OutputDstFile]['datatype'] = 'DST'
    if path:
      self.outputDstPath = path

  def setProcessorsToUse(self, processorlist):
    """ Define processor list to use

    Overwrite the default list (full reco). Useful for users willing to do dedicated analysis (TPC, Vertex digi, etc.)

    >>> ma.setProcessorsToUse(['libMarlinTPC.so','libMarlinReco.so','libOverlay.so','libMarlinTrkProcessors.so'])

    @param processorlist: list of processors to use
    @type processorlist: list
    """
    self._checkArgs( { 'processorlist' : types.ListType } )
    self.ProcessorsToUse = processorlist

  def setProcessorsToExclude(self, processorlist):
    """ Define processor list to exclude

    Overwrite the default list (full reco). Useful for users willing to do dedicated analysis (TPC, Vertex digi, etc.)

    >>> ma.setProcessorsToExclude(['libLCFIVertex.so'])

    @param processorlist: list of processors to exclude
    @type processorlist: list
    """
    self._checkArgs( { 'processorlist' : types.ListType } )
    self.ProcessorsToExclude = processorlist

  def _userjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setUserJobFinalization(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('userjobmodules failed')
    return S_OK()

  def _prodjobmodules(self, stepdefinition):

    ## Here one needs to take care of listoutput
    if self.OutputPath:
      self._listofoutput.append({'OutputFile' : '@{OutputFile}', "outputPath" : "@{OutputPath}",
                                 "outputDataSE" : '@{OutputSE}'})

    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setOutputComputeDataList(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('prodjobmodules failed')
    return S_OK()

  def _checkConsistency(self):

    if not self.Version:
      return S_ERROR('Version not set!')

    if self.SteeringFile:
      if not os.path.exists(self.SteeringFile) and not self.SteeringFile.lower().count("lfn:"):
        #res = Exists(self.SteeringFile)
        res = S_OK()
        if not res['OK']:
          return res
      if os.path.exists(self.SteeringFile):
        res = checkXMLValidity(self.SteeringFile)
        if not res['OK']:
          return S_ERROR("Supplied steering file cannot be read with xml parser: %s" % (res['Message']) )

    if not self.GearFile :
      self._log.info('GEAR file not given, will use GearOutput.xml (default from Mokka, CLIC_ILD_CDR model)')
    if self.GearFile:
      if not os.path.exists(self.GearFile) and not self.GearFile.lower().count("lfn:"):
        #res = Exists(self.GearFile)
        res = S_OK()
        if not res['OK']:
          return res

    #res = self._checkRequiredApp()
    #if not res['OK']:
    #  return res

    if not self.GearFile:
      self.GearFile = 'GearOutput.xml'

    if not self._jobtype == 'User' :
      if not self.OutputFile:
        self._listofoutput.append({"outputFile":"@{outputREC}", "outputPath":"@{outputPathREC}",
                                   "outputDataSE":'@{OutputSE}'})
        self._listofoutput.append({"outputFile":"@{outputDST}", "outputPath":"@{outputPathDST}",
                                   "outputDataSE":'@{OutputSE}'})
      self.prodparameters['detectorType'] = self.detectortype
      self.prodparameters['marlin_gearfile'] = self.GearFile
      self.prodparameters['marlin_steeringfile'] = self.SteeringFile


    return S_OK()

  def _applicationModule(self):

    md1 = self._createModuleDefinition()
    md1.addParameter(Parameter("inputGEAR",              '', "string", "", "", False, False,
                               "Input GEAR file"))
    md1.addParameter(Parameter("ProcessorListToUse",     [],   "list", "", "", False, False,
                               "List of processors to use"))
    md1.addParameter(Parameter("ProcessorListToExclude", [],   "list", "", "", False, False,
                               "List of processors to exclude"))
    md1.addParameter(Parameter("debug",               False,   "bool", "", "", False, False,
                               "debug mode"))
    return md1

  def _applicationModuleValues(self, moduleinstance):

    moduleinstance.setValue("inputGEAR",              self.GearFile)
    moduleinstance.setValue('ProcessorListToUse',     self.ProcessorsToUse)
    moduleinstance.setValue('ProcessorListToExclude', self.ProcessorsToExclude)
    moduleinstance.setValue("debug",                  self.Debug)

  def _checkWorkflowConsistency(self):
    return self._checkRequiredApp()

  def _resolveLinkedStepParameters(self, stepinstance):
    if type(self._linkedidx) == types.IntType:
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
    return S_OK()
