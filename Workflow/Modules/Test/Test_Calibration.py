"""
Unit tests for the Calibration.py file
"""

import unittest
import pytest
import os
from mock import mock_open, patch, MagicMock as Mock
from ILCDIRAC.Workflow.Modules.Calibration import Calibration
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertInImproved, \
    assertEqualsImproved, assertDiracFailsWith, assertDiracSucceeds, \
    assertDiracSucceedsWith, assertDiracSucceedsWith_equals, assertMockCalls, \
    assertDiracFails
from ILCDIRAC.Tests.Utilities.FileUtils import FileUtil
from DIRAC import S_OK, S_ERROR
from ILCDIRAC.CalibrationSystem.Utilities.functions import searchFilesWithPattern

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Workflow.Modules.Calibration'


@pytest.fixture
def calib():
  calib = Calibration()
  calib.applicationName = "Testing"
  calib.applicationVersion = "vTest"
  calib.debug = True
  calib.platform = 'dummy_platform'
  calib.applicationLog = 'dummy_applicationLog'

  import ILCDIRAC.CalibrationSystem.Utilities as utilities
  # FIXME this path will be different in production version probably... update it
  calib.SteeringFile = os.path.join(utilities.__path__[0], 'testing/in1.xml')

  yield calib  # provide the fixture value

  dirName = os.path.dirname(os.path.realpath(__file__))
  filesToRemove = searchFilesWithPattern(dirName, 'marlinSteeringFile_*.xml')
  filesToRemove += searchFilesWithPattern(dirName, '%s_%s_Run_*.sh' % (calib.applicationName, calib.applicationVersion))

  potentialFilesToRemove = ['temp.sh', 'localEnv.log']
  for iFile in potentialFilesToRemove:
    fullFileName = os.path.join(dirName, iFile)
    if os.path.exists(fullFileName):
      filesToRemove += [fullFileName]

  for iFile in filesToRemove:
    try:
      os.remove(iFile)
    except EnvironmentError, e:
      print("Failed to delete file: %s" % iFile, str(e))

  del calib
  #  assert False



def test_runScript_properInputArguments(calib, mocker):
  mocker.patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK()))
  mocker.patch('os.remove', new=Mock(return_value=True))
  inputxml = 'file1.xml'
  env_script_path = '/dummy/env/path'
  marlin_dll = '/dummy/marlin/dll'
  errorMessageToTest = "steeringfile is missing: %s" % inputxml
  res = calib.runScript(inputxml, env_script_path, marlin_dll)
  assert not res['OK']
  assert res['Message'].lower() == errorMessageToTest.lower()

  mocker.patch('%s.os.path.exists' % MODULE_NAME, return_value=True)
  res = calib.runScript(inputxml, env_script_path, marlin_dll)
  assert res['OK']


paramDictList = [
    {'currentStage': 1, 'currentPhase': 0, 'parameters': {
        "processor[@name='MyDDMarlinPandora']/parameter[@name='ECalToHadGeVCalibrationEndCap']": 'ECALTOHAD_YYYY'}},
    {'currentStage': 1, 'currentPhase': 1, 'parameters': {}},
    {'currentStage': 1, 'currentPhase': 2, 'parameters': {}},
    {'currentStage': 1, 'currentPhase': 3, 'parameters': {}},
    {'currentStage': 1, 'currentPhase': 4, 'parameters': {}},
    {'currentStage': 2, 'currentPhase': 5, 'parameters': {}},
    {'currentStage': 3, 'currentPhase': 0, 'parameters': {}},
    {'currentStage': 3, 'currentPhase': 1, 'parameters': {}},
    {'currentStage': 3, 'currentPhase': 2, 'parameters': {}},
    {'currentStage': 3, 'currentPhase': 3, 'parameters': {}},
    {'currentStage': 3, 'currentPhase': 4, 'parameters': {}},
    {'currentStage': 3, 'currentPhase': 4, 'parameters': {}, 'OK': True}]

def test_runIt_simple(calib, mocker):
  mocker.patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK()))
  mocker.patch('%s.os.remove' % MODULE_NAME, return_value=True)
  mocker.patch('%s.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value={'OK': True, 'Value': 'dummy_EnvScript'}))
  mocker.patch('%s.updateSteeringFile' % MODULE_NAME, new=Mock(return_value={'OK': True}))
  mocker.patch('%s.os.path.exists' % MODULE_NAME, return_value=True)

  calibClientMock = Mock(name='mock1')
  calibClientMock.requestNewParameters = Mock(side_effect=paramDictList, name='mock3')
  calibClientMock.requestNewPhotonLikelihood.return_value = Mock(return_value='dummy_NewPhotonLikelihood')
  calibClientMock.reportResult.return_value = Mock(return_value=True)
  mocker.patch('%s.CalibrationClient' % MODULE_NAME, new=Mock(return_value=calibClientMock, name='mock2'))

  calib.prepareMARLIN_DLL = Mock(return_value={'OK': True, 'Value': 'dummy_MARLIN_DLL'})

  res = calib.runIt()
  assert res['OK']
  print('currentStep: %s' % calib.currentStep)
  assert calib.currentStep == 11


def test_runIt_updatingSteeringFile(calib, mocker):
  mocker.patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK()))
  mocker.patch('%s.os.remove' % MODULE_NAME, return_value=True)
  mocker.patch('%s.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value={'OK': True, 'Value': 'dummy_EnvScript'}))
  #  mocker.patch('%s.updateSteeringFile' % MODULE_NAME, new=Mock(return_value={'OK': True}))
  mocker.patch('%s.os.path.exists' % MODULE_NAME, return_value=True)

  calibClientMock = Mock(name='mock1')
  calibClientMock.requestNewParameters = Mock(side_effect=paramDictList, name='mock3')
  calibClientMock.requestNewPhotonLikelihood.return_value = Mock(return_value='dummy_NewPhotonLikelihood')
  calibClientMock.reportResult.return_value = Mock(return_value=True)
  mocker.patch('%s.CalibrationClient' % MODULE_NAME, new=Mock(return_value=calibClientMock, name='mock2'))

  calib.prepareMARLIN_DLL = Mock(return_value={'OK': True, 'Value': 'dummy_MARLIN_DLL'})

  res = calib.runIt()
  assert res['OK']
  print('currentStep: %s' % calib.currentStep)
  assert calib.currentStep == 11
