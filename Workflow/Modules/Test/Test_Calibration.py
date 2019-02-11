"""
Unit tests for the Calibration.py file
"""

import unittest
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


class CalibrationFixture(object):
  """ Contains the commonly used setUp and tearDown methods of the Tests"""

  def setUp(self):
    """set up the objects"""
    self.calib = Calibration()
    self.calib.applicationName = "Testing"
    self.calib.applicationVersion = "vTest"
    self.calib.STEP_NUMBER = 666
    self.calib.debug = True

  def tearDown(self):
    """Clean up test objects"""
    del self.calib

#pylint: disable=too-many-public-methods


class CalibrationTestCase(CalibrationFixture, unittest.TestCase):
  """ Base class for the ProductionJob test cases
  """
  @patch('DIRAC.Core.Utilities.Subprocess.shellCall', new=Mock(return_value=S_OK()))
  @patch('os.remove', new=Mock(return_value=True))
  def test_runScript_properInputArguments(self):
    inputxml = ['file1.xml', 'file2.xml']
    env_script_path = '/dummy/env/path'
    marlin_dll = '/dummy/marlin/dll'
    errorMessageToTest = "One or more marlin steering files are missing"
    assertDiracFailsWith(self.calib.runScript(inputxml, env_script_path, marlin_dll), errorMessageToTest, self)

    inputxml.append('file3.xml')
    errorMessageToTest = "SteeringFile is missing"
    assertDiracFailsWith(self.calib.runScript(inputxml, env_script_path, marlin_dll), errorMessageToTest, self)

    with patch('%s.os.path.exists' % MODULE_NAME, return_value=True):
      assertDiracSucceeds(self.calib.runScript(inputxml, env_script_path, marlin_dll), self)
