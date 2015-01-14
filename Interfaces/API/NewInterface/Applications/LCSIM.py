"""
LCSIM: Reconstruction after SLIC Simulation
"""
__RCSID__ = "$Id$"

from ILCDIRAC.Interfaces.API.NewInterface.LCApplication import LCApplication
from ILCDIRAC.Core.Utilities.CheckXMLValidity         import CheckXMLValidity
from ILCDIRAC.Core.Utilities.InstalledFiles import Exists
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Workflow.Parameter import Parameter
import types, os

class LCSIM(LCApplication):
  """ Call LCSIM Reconstructor (after SLIC Simulation)

  Usage:

  >>> slic = SLIC()
  >>> lcsim = LCSIM()
  >>> lcsim.getInputFromApp(slic)
  >>> lcsim.setSteeringFile("MySteeringFile.xml")
  >>> lcsim.setStartFrom(10)

  Use setExtraCLIArguments to add CLI arguments to the lcsim call

  """
  def __init__(self, paramdict = None):

    self.ExtraParams = ''
    self.AliasProperties = ''
    self.TrackingStrategy = ''
    self.DetectorModel = ''
    super(LCSIM, self).__init__( paramdict)
    ##Those 5 need to come after default constructor
    self._modulename = 'LCSIMAnalysis'
    self._moduledescription = 'Module to run LCSIM'
    self.appname = 'lcsim'
    self.datatype = 'REC'
    self.detectortype = 'SID'

  def setOutputRecFile(self, outputRecFile, path = None):
    """ Optional: Define output rec file for LCSIM

    @param outputRecFile: output rec file for LCSIM
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
    """ Optional: Define output dst file for LCSIM

    @param outputDstFile: output dst file for LCSIM
    @type outputDstFile: string
    @param path: Path where to store the file. Used only in prouction context. Use setOutputData if you want
    to keep the file on the grid.
    @type path: string
    """
    self._checkArgs( { 'outputDstFile' : types.StringTypes } )
    self.OutputDstFile = outputDstFile
    self.prodparameters[self.OutputDstFile] = {}
    self.prodparameters[self.OutputDstFile]['datatype'] = 'DST'
    if path:
      self.outputDstPath = path

  def setAliasProperties(self, alias):
    """ Optional: Define the path to the alias.properties file name that will be used

    @param alias: Path to the alias.properties file name that will be used
    @type alias: string
    """
    self._checkArgs( { 'alias' : types.StringTypes } )

    self.AliasProperties = alias
    if os.path.exists(alias) or alias.lower().count("lfn:"):
      self.inputSB.append(alias)

  def setDetectorModel(self, model):
    """ Detector Model to use

    @param model: name, zip file, or lfn that points to the detector model
    @type model: string
    """
    self._checkArgs( { 'model' : types.StringTypes } )
    self.DetectorModel = model
    if os.path.exists(model) or model.lower().count("lfn:"):
      self.inputSB.append(model)

  def setTrackingStrategy(self, trackingstrategy):
    """ Optional: Define the tracking strategy to use.

    @param trackingstrategy: path to the trackingstrategy file to use. If not called, will use whatever is
    in the steering file
    @type trackingstrategy: string
    """
    self._checkArgs( { 'trackingstrategy' : types.StringTypes } )
    self.TrackingStrategy = trackingstrategy
    if os.path.exists(self.TrackingStrategy) or self.TrackingStrategy.lower().count('lfn:'):
      self.inputSB.append(self.TrackingStrategy)

  def setExtraParams(self, extraparams):
    """ Optional: Define command line parameters to pass to java

    @param extraparams: Command line parameters to pass to java
    @type extraparams: string
    """
    self._checkArgs( { 'extraparams' : types.StringTypes } )

    self.ExtraParams = extraparams

  def willRunSLICPandora(self):
    """ You need this if you plan on running L{SLICPandora}
    """
    self.willBeCut = True

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

    if not self.Energy :
      self._log.info('Energy set to 0 !')

    if not self.NbEvts :
      self._log.info('Number of events set to 0 !')

    if not self.Version:
      return S_ERROR('No version found')

    if self.SteeringFile:
      if not os.path.exists(self.SteeringFile) and not self.SteeringFile.lower().count("lfn:"):
        res = Exists(self.SteeringFile)
        if not res['OK']:
          return res
      if os.path.exists(self.SteeringFile):
        res = CheckXMLValidity(self.SteeringFile)
        if not res['OK']:
          return S_ERROR("Supplied steering file cannot be read by XML parser: %s" % ( res['Message'] ) )
    if self.TrackingStrategy:
      if not os.path.exists(self.TrackingStrategy) and not self.TrackingStrategy.lower().count("lfn:"):
        res = Exists(self.TrackingStrategy)
        if not res['OK']:
          return res

    if self.DetectorModel:
      if not self.DetectorModel.lower().count(".zip"):
        return S_ERROR("setDetectorModel: You HAVE to pass an existing .zip file, either as local file or as LFN. \
        Or use the alias.properties.")

    #res = self._checkRequiredApp()
    #if not res['OK']:
    #  return res

    if not self._jobtype == 'User':
      #slicp = False
      if self._inputapp and not self.OutputFile and not self.willBeCut:
        for app in self._inputapp:
          if app.appname in ['slicpandora', 'marlin']:
            self._listofoutput.append({"outputFile":"@{outputREC}", "outputPath":"@{outputPathREC}",
                                       "outputDataSE":'@{OutputSE}'})
            self._listofoutput.append({"outputFile":"@{outputDST}", "outputPath":"@{outputPathDST}",
                                       "outputDataSE":'@{OutputSE}'})
            #slicp = True
            break
      self.prodparameters['detectorType'] = self.detectortype
      self.prodparameters['lcsim_steeringfile'] = self.SteeringFile
      self.prodparameters['lcsim_trackingstrategy'] = self.TrackingStrategy

      #if not slicp:
      #  self._listofoutput.append({"outputFile":"@{OutputFile}","outputPath":"@{OutputPath}","outputDataSE":'@{OutputSE}'})


    return S_OK()

  def _applicationModule(self):

    md1 = self._createModuleDefinition()
    md1.addParameter(Parameter("extraparams",           "", "string", "", "", False, False,
                               "Command line parameters to pass to java"))
    md1.addParameter(Parameter("aliasproperties",       "", "string", "", "", False, False,
                               "Path to the alias.properties file name that will be used"))
    md1.addParameter(Parameter("debug",              False,   "bool", "", "", False, False,
                               "debug mode"))
    md1.addParameter(Parameter("detectorModel",         "", "string", "", "", False, False,
                               "detector model zip file"))
    md1.addParameter(Parameter("trackingstrategy",      "", "string", "", "", False, False,
                               "trackingstrategy"))
    return md1

  def _applicationModuleValues(self, moduleinstance):

    moduleinstance.setValue("extraparams",        self.ExtraParams)
    moduleinstance.setValue("aliasproperties",    self.AliasProperties)
    moduleinstance.setValue("debug",              self.Debug)
    moduleinstance.setValue("detectorModel",      self.DetectorModel)
    moduleinstance.setValue("trackingstrategy",   self.TrackingStrategy)

  def _checkWorkflowConsistency(self):
    return self._checkRequiredApp()

  def _resolveLinkedStepParameters(self, stepinstance):
    if type(self._linkedidx) == types.IntType:
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
    return S_OK()
