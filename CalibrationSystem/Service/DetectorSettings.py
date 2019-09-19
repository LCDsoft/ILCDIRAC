"""CalibrationSettings class which defines all required inputs from user."""

from DIRAC import gLogger
from ILCDIRAC.CalibrationSystem.Utilities.objectFactory import ObjectFactory

__RCSID__ = "$Id$"
LOG = gLogger.getSubLogger(__name__)


class CalibrationSettings(object):
  """Class which defines all required inputs from user."""

  def __init__(self):
    """Initialize settingsDict with default settings."""
    self.settingsDict = {}
    self.settingsDict['digitisationAccuracy'] = 0.05
    self.settingsDict['pandoraPFAAccuracy'] = 0.025
    # FIXME these 2 parameters only for debugging purposes
    self.settingsDict['startStage'] = 1
    self.settingsDict['startPhase'] = 0

    self.settingsDict['numberOfJobs'] = 100
    self.settingsDict['marlinVersion'] = 'ILCSoft-2019-04-01_gcc62'
    # fraction of all jobs must have finished in order for the next step to begin.
    self.settingsDict['fractionOfFinishedJobsNeededToStartNextStep'] = 0.9
    self.settingsDict['disableSoftwareCompensation'] = True
    self.settingsDict['DDPandoraPFANewProcessorName'] = 'MyDDMarlinPandora'
    self.settingsDict['DDCaloDigiName'] = 'MyDDCaloDigi'

    self.settingsDict['nEcalThinLayers'] = 40
    self.settingsDict['nEcalThickLayers'] = 0
    # correction factor which is applied to responce of thick layers of ECAL;
    # if all layer have the same thickness, this factor have to be 1.0
    self.settingsDict['ecalResponseCorrectionForThickLayers'] = 1.0

    # following settings has to be setup for daughter classes
    self.settingsDict['detectorModel'] = None
    self.settingsDict['ecalBarrelCosThetaRange'] = None
    self.settingsDict['ecalEndcapCosThetaRange'] = None
    self.settingsDict['hcalBarrelCosThetaRange'] = None
    self.settingsDict['hcalEndcapCosThetaRange'] = None
    self.settingsDict['nHcalLayers'] = None
    self.settingsDict['steeringFile'] = None

    # TODO temporary field in the settings. for testing only
    self.settingsDict['startCalibrationFinished'] = False
    self.settingsDict['stopStage'] = 3
    self.settingsDict['stopPhase'] = 4

    # these settings have no default values... they have to be set up by user
    self.settingsDict['outputPath'] = None
    self.settingsDict['outputSE'] = None

  def printSettings(self):
    """Print settings dict in nice format."""
    print('%-59s %s' % ('Settings', 'Value'))
    print('-' * 120)
    for key, value in self.settingsDict.iteritems():
      print('%-59s %s' % (key, value))


class CLDSettings(CalibrationSettings):
  """Child class of CalibrationSettings for CLD detector model."""

  def __init__(self):
    """Initialize."""
    super(CLDSettings, self).__init__()
    self.settingsDict['detectorModel'] = 'FCCee_o1_v04'
    self.settingsDict['ecalBarrelCosThetaRange'] = [0.0, 0.643]
    self.settingsDict['ecalEndcapCosThetaRange'] = [0.766, 0.94]
    self.settingsDict['hcalBarrelCosThetaRange'] = [0.15, 0.485]
    self.settingsDict['hcalEndcapCosThetaRange'] = [0.72, 0.94]
    self.settingsDict['nHcalLayers'] = 44
    # TODO this default will not work... one need to specify full LFN path
    self.settingsDict['steeringFile'] = 'fccReconstruction.xml'
    self.settingsDict['DDPandoraPFANewProcessorName'] = 'MyDDMarlinPandora_10ns'
    self.settingsDict['DDCaloDigiName'] = 'MyDDCaloDigi_10ns'


class CLICSettings(CalibrationSettings):
  """Child class of CalibrationSettings for CLIC detector model."""

  def __init__(self):
    """Initialize."""
    super(CLICSettings, self).__init__()
    self.settingsDict['detectorModel'] = 'CLIC_o3_v14'
    self.settingsDict['ecalBarrelCosThetaRange'] = [0.0, 0.65]
    self.settingsDict['ecalEndcapCosThetaRange'] = [0.8, 0.92]
    self.settingsDict['hcalBarrelCosThetaRange'] = [0.2, 0.6]
    self.settingsDict['hcalEndcapCosThetaRange'] = [0.8, 0.9]
    self.settingsDict['nHcalLayers'] = 60
    # TODO this default will not work... one need to specify full LFN path
    self.settingsDict['steeringFile'] = 'clicReconstruction.xml'


calibrationSettingsFactory = ObjectFactory()
calibrationSettingsFactory.registerBuilder('CLIC', CLICSettings)
calibrationSettingsFactory.registerBuilder('CLD', CLDSettings)


def createCalibrationSettings(detectorModel):
  """Get CalibrationSettings class which corresponds to input detector label and return an instance of the class."""
  calibrationSettingsClass = calibrationSettingsFactory.getClass(detectorModel)
  return calibrationSettingsClass()
