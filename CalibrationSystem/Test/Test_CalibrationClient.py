"""
Unit tests for the CalibrationClient
"""

import pytest
import os
from DIRAC import S_OK, S_ERROR, gLogger
from mock import call, patch, MagicMock as Mock
from ILCDIRAC.CalibrationSystem.Client.CalibrationClient import createCalibration
from ILCDIRAC.CalibrationSystem.Service.DetectorSettings import createCalibrationSettings
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertInImproved, \
    assertEqualsImproved, assertDiracFailsWith, assertDiracSucceeds, \
    assertDiracSucceedsWith, assertDiracSucceedsWith_equals, assertMockCalls, \
    assertDiracFails

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.CalibrationSystem.Client.CalibrationClient'


def test_pass():
  pass
