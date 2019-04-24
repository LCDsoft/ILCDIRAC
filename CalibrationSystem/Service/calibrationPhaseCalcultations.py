"""
???
"""

from DIRAC import S_OK, S_ERROR, gLogger
from ILCDIRAC.CalibrationSystem.Client.CalibrationClient import CalibrationPhase
from ILCDIRAC.CalibrationSystem.Service.CalibrationRun import CalibrationRun
from ILCDIRAC.CalibrationSystem.Utilities.objectFactory import ObjectFactory

__RCSID__ = "$Id$"
LOG = gLogger.getSubLogger(__name__)


def readSetting(detSet, flag):
  if flag in detSet.settingsDict.keys():
    return detSet.settingsDict[flag]
  else:
    return None


def checkIfSettingExist(detSet, flag):
  if flag in detSet.settingsDict.keys():
    return True
  else:
    return False


class CalibrationSettingsFunctionFactory(ObjectFactory):
  def __call__(self, service_id, detSet, flag):
    return self.create(service_id)(detSet, flag)


calibSettingsFunctions = CalibrationSettingsFunctionFactory()
calibSettingsFunctions.register_builder('read', readSetting)
calibSettingsFunctions.register_builder('check', checkIfSettingExist)
