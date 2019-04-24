"""
???
"""

from DIRAC import S_OK, S_ERROR, gLogger
from ILCDIRAC.CalibrationSystem.Client.CalibrationClient import CalibrationPhase
from ILCDIRAC.CalibrationSystem.Utilities.objectFactory import ObjectFactory

__RCSID__ = "$Id$"
LOG = gLogger.getSubLogger(__name__)


class CalibrationSettings(object):
  def __init__(self):
    self.settingsDict = {}
    self.settingsDict['digitisationAccuracy'] = 0.05
    self.settingsDict['pandoraPFAAccuracy'] = 0.025
    # FIXME these 3 parameters only for debugging purposes
    self.settingsDict['startStage'] = 1
    self.settingsDict['startPhase'] = CalibrationPhase.ECalDigi
    self.settingsDict['startStep'] = 0

    self.settingsDict['numberOfJobs'] = 100
    self.settingsDict['platform'] = 'x86_64-slc5-gcc43-opt'  # FIXME does it the default platform in CS?
    self.settingsDict['marlinVersion'] = 'ILCSoft-2019-02-20_gcc62'  # FIXME this has to be equal to self.marlinVersion
    self.settingsDict['outputPath'] = '/ilc/user/o/oviazlo/clic_caloCalib/output/'
    self.settingsDict['steeringFile'] = ''

  def printSettings(self):
    print('%-59s %s' % ('Settings', 'Value'))
    print('-' * 120)
    for key, value in self.settingsDict.iteritems():
      print('%-59s %s' % (key, value))


class CLDSettings(CalibrationSettings):
  def __init__(self):
    super(CLDSettings, self).__init__()
    self.settingsDict['detectorModel'] = 'FCCee_o1_v04'
    self.settingsDict['ecalBarrelCosThetaRange'] = [-0.1, 0.643]
    self.settingsDict['ecalEndcapCosThetaRange'] = [0.766, 0.94]
    self.settingsDict['hcalBarrelCosThetaRange'] = [0.15, 0.485]
    self.settingsDict['hcalEndcapCosThetaRange'] = [0.72, 0.94]
    self.settingsDict['nHcalLayers'] = 44


class CLICSettings(CalibrationSettings):
  def __init__(self):
    super(CLICSettings, self).__init__()
    self.settingsDict['detectorModel'] = 'CLIC_o3_v14'
    self.settingsDict['ecalBarrelCosThetaRange'] = [-0.1, 0.65]
    self.settingsDict['ecalEndcapCosThetaRange'] = [0.8, 0.92]
    self.settingsDict['hcalBarrelCosThetaRange'] = [0.2, 0.6]
    self.settingsDict['hcalEndcapCosThetaRange'] = [0.8, 0.9]
    self.settingsDict['nHcalLayers'] = 60


class CalibrationSettingsFactory(ObjectFactory):
  def get(self, service_id):
    return self.create(service_id)


calibSettings = CalibrationSettingsFactory()
calibSettings.register_builder('CLIC', CLICSettings)
calibSettings.register_builder('CLD', CLDSettings)


def createCalibrationSettings(detectorModel):
  settings = calibSettings.get(detectorModel)
  return settings()
