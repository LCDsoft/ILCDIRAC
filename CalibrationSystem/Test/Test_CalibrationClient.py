"""Unit tests for the CalibrationClient."""

import pytest
from ILCDIRAC.CalibrationSystem.Client.CalibrationClient import CalibrationClient

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.CalibrationSystem.Client.CalibrationClient'


@pytest.yield_fixture
def calibClient():
  """Create calibration handler."""
  calibClient = CalibrationClient(1, 1)
  return calibClient


def test_pass():
  """Skeleton for test funciton."""
  pass
