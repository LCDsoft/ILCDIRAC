"""
LCSIM: Reconstruction after SLIC Simulation
"""
__RCSID__ = "$Id$"

from ILCDIRAC.Interfaces.API.NewInterface.LCApplication import LCApplication
from ILCDIRAC.Core.Utilities.CheckXMLValidity         import checkXMLValidity
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

  Use :func:`setExtraCLIArguments` to add command line arguments to the lcsim call

  """
  def __init__(self, paramdict = None):

    self.extraParams = ''
    self.aliasProperties = ''
    self.trackingStrategy = ''
    self.detectorModel = ''
    super(LCSIM, self).__init__( paramdict)
    ##Those 5 need to come after default constructor
    self._modulename = 'LCSIMAnalysis'
    self._moduledescription = 'Module to run LCSIM'
    self.appname = 'lcsim'
    self.datatype = 'REC'
    self.detectortype = 'SID'

  def setOutputRecFile(self, outputRecFile, path = None):
    """Optional: Define output rec file for LCSIM Used only in production
    context. Use :func:`UserJob.setOutputData
    <ILCDIRAC.Interfaces.API.NewInterface.UserJob.UserJob.setOutputData>` if you
    want to keep the file on the grid.

    :param string outputRecFile: output rec file for LCSIM
    :param string path: Path where to store the file.

    """
    self._checkArgs( { 'outputRecFile' : types.StringTypes } )
    self.outputRecFile = outputRecFile
    self.prodparameters[self.outputRecFile] = {}
    self.prodparameters[self.outputRecFile]['datatype'] = 'REC'
    if path:
      self.outputRecPath = path

  def setOutputDstFile(self, outputDstFile, path = None):
    """Optional: Define output dst file for LCSIM.Used only in production
    context. Use :func:`UserJob.setOutputData
    <ILCDIRAC.Interfaces.API.NewInterface.UserJob.UserJob.setOutputData>` if you
    want to keep the file on the grid.

    :param string outputDstFile: output dst file for LCSIM
    :param string path: Path where to store the file.

    """
    self._checkArgs( { 'outputDstFile' : types.StringTypes } )
    self.outputDstFile = outputDstFile
    self.prodparameters[self.outputDstFile] = {}
    self.prodparameters[self.outputDstFile]['datatype'] = 'DST'
    if path:
      self.outputDstPath = path

  def setAliasProperties(self, alias):
    """ Optional: Define the path to the alias.properties file name that will be used

    :param string alias: Path to the alias.properties file name that will be used
    """
    self._checkArgs( { 'alias' : types.StringTypes } )

    self.aliasProperties = alias
    if os.path.exists(alias) or alias.lower().count("lfn:"):
      self.inputSB.append(alias)

  def setDetectorModel(self, model):
    """ Detector Model to use

    :param string model: name, zip file, or lfn that points to the detector model
    """
    self._checkArgs( { 'model' : types.StringTypes } )
    self.detectorModel = model
    if os.path.exists(model) or model.lower().count("lfn:"):
      self.inputSB.append(model)

  def setTrackingStrategy(self, trackingstrategy):
    """ Optional: Define the tracking strategy to use.

    :param string trackingstrategy: path to the trackingstrategy file to use. If not called, will use whatever is
                                    in the steering file

    """
    self._checkArgs( { 'trackingstrategy' : types.StringTypes } )
    self.trackingStrategy = trackingstrategy
    if os.path.exists(self.trackingStrategy) or self.trackingStrategy.lower().count('lfn:'):
      self.inputSB.append(self.trackingStrategy)

  def setExtraParams(self, extraparams):
    """ Optional: Define command line parameters to pass to java

    :param string extraparams: Command line parameters to pass to java
    """
    self._checkArgs( { 'extraparams' : types.StringTypes } )

    self.extraParams = extraparams

  def willRunSLICPandora(self):
    """ You need this if you plan on running :mod:`~ILCDIRAC.Interfaces.API.NewInterface.Applications.SLICPandora`
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

    if not self.energy :
      self._log.info('Energy set to 0 !')

    if not self.numberOfEvents :
      self._log.info('Number of events set to 0 !')

    if not self.version:
      return S_ERROR('No version found')

    if self.steeringFile:
      if not os.path.exists(self.steeringFile) and not self.steeringFile.lower().count("lfn:"):
        res = Exists(self.steeringFile)
        if not res['OK']:
          return res
      if os.path.exists(self.steeringFile):
        res = checkXMLValidity(self.steeringFile)
        if not res['OK']:
          return S_ERROR("Supplied steering file cannot be read by XML parser: %s" % ( res['Message'] ) )
    if self.trackingStrategy:
      if not os.path.exists(self.trackingStrategy) and not self.trackingStrategy.lower().count("lfn:"):
        res = Exists(self.trackingStrategy)
        if not res['OK']:
          return res

    if self.detectorModel:
      if not self.detectorModel.lower().count(".zip"):
        return S_ERROR("setDetectorModel: You HAVE to pass an existing .zip file, either as local file or as LFN. \
        Or use the alias.properties.")

    #res = self._checkRequiredApp()
    #if not res['OK']:
    #  return res

    if not self._jobtype == 'User':
      #slicp = False
      if self._inputapp and not self.outputFile and not self.willBeCut:
        for app in self._inputapp:
          if app.appname in ['slicpandora', 'marlin']:
            self._listofoutput.append({"outputFile":"@{outputREC}", "outputPath":"@{outputPathREC}",
                                       "outputDataSE":'@{OutputSE}'})
            self._listofoutput.append({"outputFile":"@{outputDST}", "outputPath":"@{outputPathDST}",
                                       "outputDataSE":'@{OutputSE}'})
            #slicp = True
            break
      self.prodparameters['detectorType'] = self.detectortype
      self.prodparameters['lcsim_steeringfile'] = self.steeringFile
      self.prodparameters['lcsim_trackingstrategy'] = self.trackingStrategy

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

    moduleinstance.setValue("extraparams",        self.extraParams)
    moduleinstance.setValue("aliasproperties",    self.aliasProperties)
    moduleinstance.setValue("debug",              self.debug)
    moduleinstance.setValue("detectorModel",      self.detectorModel)
    moduleinstance.setValue("trackingstrategy",   self.trackingStrategy)

  def _checkWorkflowConsistency(self):
    return self._checkRequiredApp()

  def _resolveLinkedStepParameters(self, stepinstance):
    if type(self._linkedidx) == types.IntType:
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
    return S_OK()
