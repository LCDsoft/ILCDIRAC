"""
DDSim: Simulation based on DD4hep and LCGeo

.. versionadded:: v25r0p0

Usage:

>>> ddsim = DDSim()
>>> ddsim.setVersion("ILCSoft-01-18-00")
>>> ddsim.setDetectorModel("CLIC_o2_v03")
>>> ddsim.setInputFile("LFN:/ilc/prod/clic/500gev/Z_uds/gen/0/00.stdhep")
>>> ddsim.setNumberOfEvents(30)

Use :func:`DDSim.setExtraCLIArguments` in case you want to use command line parameters


.. versionadded:: v26r0p7

To use your own plugins, detector drivers, or custom lcgeo and dd4hep libraries add a
tarball to the inputSandbox (via LFN) which contains the 'lib' folder with the
shared object and the components file::

   ./lib/liblcgeo.so
   ./lib/liblcgeo.components
   ./lib/...

"""
import types
import os

from ILCDIRAC.Interfaces.API.NewInterface.LCApplication import LCApplication
from ILCDIRAC.Core.Utilities.InstalledFiles import Exists
from ILCDIRAC.Interfaces.Utilities.DDInterfaceMixin import DDInterfaceMixin
from ILCDIRAC.Core.Utilities.Utilities import canConvert
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Workflow.Parameter import Parameter
from DIRAC.ConfigurationSystem.Client.Helpers.Operations  import Operations

LOG = gLogger.getSubLogger(__name__)
__RCSID__ = "$Id$"


class DDSim( DDInterfaceMixin, LCApplication ):
  """ DDSim Application Class """

  def __init__(self, paramdict = None):
    self.startFrom = 0
    self.randomSeed = -1
    self.detectorModel = ''
    self.extraParticles = []
    super(DDSim, self).__init__( paramdict )
    ##Those 5 need to come after default constructor
    self._modulename = 'DDSimAnalysis'
    self._moduledescription = 'Module to run DDSim'
    self.appname = 'ddsim'
    self.datatype = 'SIM'
    self._paramsToExclude.extend( [ "outputDstPath", "outputRecPath", "OutputDstFile", "OutputRecFile" ] )
    self._ops = Operations()

  @property
  def detectortype(self):
    """  detectorType needed for transformations """
    return self.detectorModel
  @detectortype.setter
  def detectortype(self, value ):
    """ ignore setting of detector type for ddsim """
    pass

  def setRandomSeed(self, randomSeed):
    """ Optional: Define random seed to use. Default is the jobID.

    :param int randomSeed: Seed to use during simulation.
    """
    self._checkArgs( { 'randomSeed' : types.IntType } )
    self.randomSeed = randomSeed

  def setExtraParticles(self, extraParticles):
    """Optional: Add or modify the particles.tbl of DDG4.

    In case you want to modify default properties of particles or add particles that are not in the
    default DDSim particle.tbl.

    In case ``SIM.physics.pdgfile`` is specified in your steering file, this custom file will be modified,
    if the option is not set the file ``$DD4hep/DDG4/examples/particle.tbl`` from the chosen DD4hep version
    will be used.

    Example usage:

    >>> ddsim.setExtraParticles([[2000004,"susy-c_R", 2, 500, 1, 50],[2000005,"susy-b_R", 2, 1500, 1, 100]])

    or

    >>> ddsim.setExtraParticles("2000004 susy-c_R 2 500 1 50 2000005 susy-b_R 2 1500 1 100")

    :param extraParticles: If list entries should be e.g. [ID, "name", chg, mass, total width, lifetime]
                           If string entries should be whitespace separated e.g. "ID name chg mass total-width lifetime"
    :type extraParticles: list or str
    """
    if isinstance(extraParticles, str):
      extraParticles = extraParticles.split()
      if len(extraParticles) % 6 != 0:
        return self._reportError("The extraParticles string cannot be split up in multiples of 6")
      extraParticles = [extraParticles[x:x + 6] for x in xrange(0, len(extraParticles), 6)]
      for particle in extraParticles:
        condInt = canConvert(particle[0], int) and canConvert(particle[2], int)
        condFloat = canConvert(particle[3], float) and canConvert(particle[4], float) and canConvert(particle[5], float)
        if not (condInt and condFloat):
          return self._reportError("Cannot convert input string to proper format [int,str,int,float,float,float]")
        particle[0] = int(particle[0])
        particle[2] = int(particle[2])
        particle[3] = float(particle[3])
        particle[4] = float(particle[4])
        particle[5] = float(particle[5])

    self._checkArgs({'extraParticles': types.ListType})
    self.extraParticles = extraParticles

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
    :type job: ~ILCDIRAC.Interfaces.API.NewInterface.Job.Job
    :returns: S_OK/S_ERROR
    """

    parameterName = [ pN for pN in job.workflow.parameters.getParametersNames() if 'ConfigPackage' in pN ]
    if parameterName:
      LOG.notice("Found config parameter", parameterName)
      config = job.workflow.parameters.find( parameterName[0] )
      configversion = config.value
    else:
      configversion = None
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

    for particle in self.extraParticles:
      if not isinstance(particle, (list, tuple)):
        return S_ERROR("Wrong format, extra particles not a list of lists")
      if len(particle) != 6:
        return S_ERROR("Particle property not of correct format "
                       "[ID, \"name\", chg, mass, total width, lifetime]")
      if not isinstance(particle[0], int):
        return S_ERROR("Particle property ID not int")
      if not isinstance(particle[1], str):
        return S_ERROR("Particle property name not string")
      if not isinstance(particle[2], int):
        return S_ERROR("Particle property chg not int")
      if not isinstance(particle[3], float):
        return S_ERROR("Particle property mass not float")
      if not isinstance(particle[4], float):
        return S_ERROR("Particle property total width not float")
      if not isinstance(particle[5], float):
        return S_ERROR("Particle property lifetime not float")

    #res = self._checkRequiredApp()
    #if not res['OK']:
    #  return res

    if self._jobtype != 'User':
      self._listofoutput.append({"outputFile":"@{OutputFile}", "outputPath":"@{OutputPath}",
                                 "outputDataSE":'@{OutputSE}'})

      self.prodparameters['detectorType'] = self.detectortype
      #FIXME: Delete old code, detectorModel is checked for False already
      #if self.detectorModel:
        #self.prodparameters['slic_detectormodel'] = self.detectorModel
      self.prodparameters['slic_detectormodel'] = self.detectorModel

    if not self.startFrom:
      LOG.info('No startFrom defined for DDSim : start from the beginning')

    return S_OK()

  def _applicationModule(self):

    md1 = self._createModuleDefinition()
    md1.addParameter(Parameter("randomSeed",           0,    "int", "", "", False, False,
                               "Random seed for the generator"))
    md1.addParameter(Parameter("detectorModel",       "", "string", "", "", False, False,
                               "Detecor model for simulation"))
    md1.addParameter(Parameter("extraParticles", [], "list", "", "", False, False,
                               "Particles to be modified or added to particles.tbl"))
    md1.addParameter(Parameter("startFrom",            0,    "int", "", "", False, False,
                               "From where DDSim starts to read the input file"))
    md1.addParameter(Parameter("debug",            False,   "bool", "", "", False, False, "debug mode"))
    return md1

  def _applicationModuleValues(self, moduleinstance):

    moduleinstance.setValue("randomSeed",      self.randomSeed)
    moduleinstance.setValue("detectorModel",   self.detectorModel)
    moduleinstance.setValue("extraParticles", self.extraParticles)
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
