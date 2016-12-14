"""
Integration tests for the CalibrationService
"""

from collections import defaultdict
import unittest
from DIRAC.Core.Base.Script import parseCommandLine
from DIRAC.Core.DISET.RPCClient import RPCClient
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertDiracSucceedsWith_equals, assertEqualsImproved, \
    assertDiracSucceeds
from ILCDIRAC.CalibrationSystem.Service.CalibrationHandler import CalibrationHandler, \
    CalibrationResult


__RCSID__ = "$Id$"

parseCommandLine()


class TestCalibrationBase(unittest.TestCase):
  """ Provides the basic common test fixtures. Creates a RPCClient to the CalibrationService. """

  def setUp(self):
    self.calibrationService = RPCClient('Calibration/Calibration')

  def tearDown(self):
    assertDiracSucceeds(self.calibrationService.resetService(), self)


class TestCalibrationService(TestCalibrationBase):
  """ Tests the public CalibrationService methods. """

  def test_createCalibration(self):
    assertDiracSucceedsWith_equals(
        self.calibrationService.createCalibration('mySteerTest.file', 'softV.1',
                                                  ['test.input1', 'other_input.txt'], 10),
        1, self)
    internals = self.calibrationService.getInternals()
    assertDiracSucceeds(internals, self)
    (calibrations, _) = internals['Value']
    assert 1 in calibrations
    createdRun = calibrations[1]
    assertEqualsImproved(
        (createdRun.steeringFile, createdRun.softwareVersion, createdRun.inputFiles,
         createdRun.numberOfJobs, createdRun.calibrationFinished, createdRun.currentStep,
         createdRun.stepResults, createdRun.currentParameterSet),
        ('mySteerTest.file', 'softV.1', ['test.input1', 'other_input.txt'], 10, False, 0,
            defaultdict(CalibrationResult), None), self)

  def test_submitresult(self):
    pass


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestHelloHandler)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestCalibrationService))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
