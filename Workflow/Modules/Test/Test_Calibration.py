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

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Workflow.Modules.Calibration'


@pytest.fixture
def calib():
  calib = Calibration(1, 1)
  calib.applicationName = "Testing"
  calib.applicationVersion = "vTest"
  calib.debug = True
  calib.platform = 'dummy_platform'
  calib.applicationLog = 'dummy_applicationLog'

  import ILCDIRAC.CalibrationSystem.Utilities as utilities
  # FIXME this path will be different in production version probably... update it
  calib.SteeringFile = os.path.join(utilities.__path__[0], 'testing/in1.xml')

  yield calib  # provide the fixture value
  scriptName = '%s_%s_Run_%s.sh' % (calib.applicationName, calib.applicationVersion, calib.currentStep)
  scriptName = os.path.join(os.path.dirname(os.path.realpath(__file__)), scriptName)
  del calib
  try:
    os.remove(scriptName)
  except EnvironmentError, e:
    print("Failed to delete file: %s" % scriptName, str(e))


def test_runScript_properInputArguments(calib, mocker):
  mocker.patch('DIRAC.Core.Utilities.Subprocess.shellCall', new=Mock(return_value=S_OK()))
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


def test_runIt(calib, mocker):
  mocker.patch('DIRAC.Core.Utilities.Subprocess.shellCall', new=Mock(return_value=S_OK()))
  mocker.patch('%s.os.remove' % MODULE_NAME, return_value=True)
  mocker.patch('%s.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value={'OK': True, 'Value': 'dummy_EnvScript'}))
  mocker.patch('%s.updateSteeringFile' % MODULE_NAME, new=Mock(return_value=True))
  mocker.patch('%s.os.path.exists' % MODULE_NAME, return_value=True)
  mocker.patch.object(calib.cali, 'requestNewParameters', return_value={'OK': True, 'Value': 'dummy'})
  mocker.patch.object(calib.cali, 'requestNewPhotonLikelihood', return_value='dummy_NewPhotonLikelihood')
  mocker.patch.object(calib.cali, 'reportResult', return_value=True)
  calib.prepareMARLIN_DLL = Mock(return_value={'OK': True, 'Value': 'dummy_MARLIN_DLL'})

  calib.runIt()
  print('currentStep: %s' % calib.currentStep)
  assert calib.currentStep == 0
  #  assert False
