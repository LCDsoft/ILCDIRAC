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
from ILCDIRAC.Interfaces.Utilities.DDInterfaceMixin import DDInterfaceMixin
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Workflow.Parameter import Parameter
from DIRAC.ConfigurationSystem.Client.Helpers.Operations  import Operations

__RCSID__ = "$Id$"

class DDSim( DDInterfaceMixin, LCApplication ):
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
    self._ops = Operations()

  def setRandomSeed(self, randomSeed):
    """ Optional: Define random seed to use. Default is the jobID.

    :param int randomSeed: Seed to use during simulation.
    """
    self._checkArgs( { 'randomSeed' : types.IntType } )
    self.randomSeed = randomSeed

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

  def _checkConsistency(self, job=None):
    """ FIXME
    Check consistency of the DDSim application, this is called from the `Job` instance

    :param job: The instance of the job
    :type job: `Job`
    :returns: S_OK/S_ERROR
    """

    ildconfig = job.workflow.parameters.find("ILDConfigPackage")
    configversion = ildconfig.value if ildconfig else None
    ## Platform must always be defined
    platform = job.workflow.parameters.find("Platform").value

    if not self.version:
      return S_ERROR('No version found')
    if self.steeringFile:
      if not os.path.exists(self.steeringFile) and not self.steeringFile.lower().startswith("lfn:"):
        res = Exists(self.steeringFile, platform=platform, configversion=configversion)
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
    if isinstance( self._linkedidx, (int, long) ):
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
    return S_OK()
