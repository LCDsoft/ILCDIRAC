"""
DDSim: Simulation based on DD4hep and LCGeo

.. versionadded:: v25r0p0

Usage:

>>> ddsim = DDSim()
>>> ddsim.setVersion("ILCSoft-01-18-00")
>>> ddsim.setDetectorModel("CLIC_o2_v03")
>>> ddsim.setInputFile("LFN:/ilc/prod/clic/500gev/Z_uds/gen/0/00.stdhep")
>>> ddsim.setNumberOfEvents(30)

Use :func:`setExtraCLIArguments` in case you want to use command line parameters


.. Todo::

   * Handle user provided plugins for detector models or other things

"""
import types
import os

from ILCDIRAC.Interfaces.API.NewInterface.LCApplication import LCApplication
from ILCDIRAC.Core.Utilities.InstalledFiles import Exists
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Workflow.Parameter import Parameter
from DIRAC.ConfigurationSystem.Client.Helpers.Operations  import Operations

__RCSID__ = "$Id$"

class DDSim( LCApplication ):
  """ DDSim Application Class """

  def __init__(self, paramdict = None):
    self.startFrom = 0
    self.randomSeed = 0
    self.detectorModel = ''
    super(DDSim, self).__init__( paramdict )
    ##Those 5 need to come after default constructor
    self._modulename = 'DDSimAnalysis'
    self._moduledescription = 'Module to run DDSim'
    self.appname = 'ddsim'
    self.datatype = 'SIM'
    self.detectortype = ''
    self._paramsToExclude.extend( [ "outputDstPath", "outputRecPath", "OutputDstFile", "OutputRecFile" ] )


  def setRandomSeed(self, randomSeed):
    """ Optional: Define random seed to use. Default is the jobID.

    :param int randomSeed: Seed to use during simulation.
    """
    self._checkArgs( { 'randomSeed' : types.IntType } )
    self.randomSeed = randomSeed

  def setDetectorModel(self, detectorModel):
    """Define detector model to use for ddsim simulation

    The detector model can be a collection of XML files Either one has to use a
    detector model provided by LCGeo or DD4hep, which can be found on CVMFS or
    the complete XML needs to be passed as a tarball in the input sandbox or on the grid

    The tarball name must be detectorModel plus extension
    
    :param string detectorModel: Detector Model to use for DDSim simulation. Can
      be on CVMFS, tarball LFN or inputSandbox tarball
    
    """
    self._checkArgs( { 'detectorModel' : types.StringTypes } )
    extensions = (".zip", ".tar.gz", ".tgz")

    ## file on the grid
    if detectorModel.lower().startswith("lfn:"):
      self.inputSB.append(detectorModel)
      self.detectorModel = os.path.basename(detectorModel) 
      for ext in extensions:
        if detectorModel.endswith(ext):
          self.detectorModel = os.path.basename(detectorModel).replace( ext, '' )
      return S_OK()

    ## local file
    elif detectorModel.endswith( extensions ):
      for ext in extensions:
        if detectorModel.endswith(ext):
          self.detectorModel = os.path.basename(detectorModel).replace( ext, '' )
          break

      if os.path.exists(detectorModel):
        self.inputSB.append(detectorModel)
      else:
        self._log.notice("Specified detector model file does not exist locally, I hope you know what you're doing")
      return S_OK()

    ## DetectorModel is part of the software
    else:
      knownDetectors = self.getKnownDetectorModels()
      if not knownDetectors['OK']:
        self._log.error("Failed to get knownDetectorModels", knownDetectors["Message"] )
        return knownDetectors
      elif detectorModel in knownDetectors['Value']:
        self.detectorModel = detectorModel
      else:
        self._log.error("Unknown detector model: ", detectorModel )
        return S_ERROR( "Unknown detector model in ddsim: %s" % detectorModel )
    return S_OK()

  def setStartFrom(self, startfrom):
    """ Optional: Define from where ddsim starts to read in the input file
    
    :param int startfrom: from where ddsim starts to read the input file
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

  def _checkConsistency(self):
    """ FIXME """
    if not self.version:
      return S_ERROR('No version found')
    if self.steeringFile:
      if not os.path.exists(self.steeringFile) and not self.steeringFile.lower().count("lfn:"):
        res = Exists(self.steeringFile)
        if not res['OK']:
          return res

    if not self.detectorModel:
      return S_ERROR("No detectorModel set")

    #res = self._checkRequiredApp()
    #if not res['OK']:
    #  return res

    if self._jobtype != 'User':
      self._listofoutput.append({"outputFile":"@{OutputFile}", "outputPath":"@{OutputPath}",
                                 "outputDataSE":'@{OutputSE}'})

      self.prodparameters['detectorType'] = self.detectortype
      if self.detectorModel:
        self.prodparameters['slic_detectormodel'] = self.detectorModel

    if not self.startFrom :
      self._log.info('No startFrom defined for DDSim : start from the beginning')

    return S_OK()

  def _applicationModule(self):

    md1 = self._createModuleDefinition()
    md1.addParameter(Parameter("randomSeed",           0,    "int", "", "", False, False,
                               "Random seed for the generator"))
    md1.addParameter(Parameter("detectorModel",       "", "string", "", "", False, False,
                               "Detecor model for simulation"))
    md1.addParameter(Parameter("startFrom",            0,    "int", "", "", False, False,
                               "From where DDSim starts to read the input file"))
    md1.addParameter(Parameter("debug",            False,   "bool", "", "", False, False, "debug mode"))
    return md1

  def _applicationModuleValues(self, moduleinstance):

    moduleinstance.setValue("randomSeed",      self.randomSeed)
    moduleinstance.setValue("detectorModel",   self.detectorModel)
    moduleinstance.setValue("startFrom",       self.startFrom)
    moduleinstance.setValue("debug",           self.debug)

  def _checkWorkflowConsistency(self):
    return self._checkRequiredApp()

  def _resolveLinkedStepParameters(self, stepinstance):
    if type(self._linkedidx) == types.IntType:
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
    return S_OK()


  def getKnownDetectorModels( self, version=None ):
    """return a list of known detectorModels

    Depends on the version of the software though...
    TODO: Pick this up from the ConfigSystem by version

    :param string version: Optional: Software version for which to print the detector models. If not given the version of the application instance is used.
    :returns: list of detector models known for this software version
    
    """
    if version is None and not self.version:
      return S_ERROR( "No software version defined" )
    platform="x86_64-slc5-gcc43-opt"
    detectorModels = Operations().getOptionsDict("/AvailableTarBalls/%s/%s/%s/DetectorModels" % (platform,
                                                                                                 "ddsim",
                                                                                                 self.version))

    return detectorModels
