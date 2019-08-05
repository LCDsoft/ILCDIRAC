"""
Unit tests for the CalibrationClient
"""

from DIRAC import S_OK, S_ERROR
from mock import call, patch
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertInImproved, \
    assertEqualsImproved, assertDiracFailsWith, assertDiracSucceeds, \
    assertDiracSucceedsWith, assertDiracSucceedsWith_equals, assertMockCalls

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.CalibrationSystem.Client.CalibrationClient'


def test_pass():
  pass
