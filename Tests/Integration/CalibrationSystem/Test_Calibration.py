"""
Integration tests for the CalibrationService
"""

from collections import defaultdict
import unittest
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.Script import parseCommandLine
from DIRAC.Core.DISET.RPCClient import RPCClient
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertDiracSucceedsWith_equals, assertEqualsImproved, \
    assertDiracSucceeds, assertDiracFailsWith, assertDiracSucceedsWith
from ILCDIRAC.CalibrationSystem.Service.CalibrationHandler import CalibrationResult


__RCSID__ = "$Id$"


class TestCalibrationBase(unittest.TestCase):
  """ Provides the basic common test fixtures. Creates a RPCClient to the CalibrationService. """

  @classmethod
  def setUpClass(cls):
    parseCommandLine()

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
    # Create 10 basic calibrations
    create_n_calibrations(self.calibrationService, 10, self, 10)
    results = []
    results.append(self.calibrationService.submitResult(1, 0, 7842, [9.2, 3.2, 1.0]))
    results.append(self.calibrationService.submitResult(11, 0, 2, [9.2, 3.2, 1.0]))
    results.append(self.calibrationService.submitResult(1, 0, 9831, [1.2, 0.21, 9.8]))
    results.append(self.calibrationService.submitResult(1, 0, 2, [0.2, 0.7, 10.20]))
    results.append(self.calibrationService.submitResult(4, 0, 43, []))
    results.append(self.calibrationService.submitResult(7, 1, 2, ['ignored']))
    results.append(self.calibrationService.submitResult(5, -1, 931, ['ignored']))
    results.append(self.calibrationService.submitResult(8, 0, 8472, [1.2]))
    results.append(self.calibrationService.submitResult(183754, 0, 7842, [9.2, 3.2, 1.0]))
    results.append(self.calibrationService.submitResult(8, 0, 9013, [4.3, 2.2, 5.4]))
    results.append(self.calibrationService.submitResult(10, 0, 3, [9013.2, 10.2, 3]))
    # Check expected vs actual values
    expected_results = [S_OK(), S_ERROR('Calibration with ID 11 not found.'), S_OK(),
                        S_OK(), S_OK(), S_OK(), S_OK(), S_OK(),
                        S_ERROR('Calibration with ID 183754 not found.'), S_OK(), S_OK()]
    assert len(results) is len(expected_results)
    for (actual, expectation) in zip(results, expected_results):
      assert actual['OK'] is expectation['OK']
      if not actual['OK']:
        assertEqualsImproved(actual['Message'], expectation['Message'], self)
    internals = self.calibrationService.getInternals()
    assertDiracSucceeds(internals, self)
    (calibrations, counter) = internals['Value']
    assert counter is 11
    assert 0 not in calibrations
    empty_calibs = [2, 3, 5, 6, 7, 9]
    # Assert unconsidered calibrations didn't change
    for calID in empty_calibs:
      unchanged_calibration = calibrations[calID]
      assertEqualsImproved(
          (unchanged_calibration.currentStep, unchanged_calibration.stepResults,
           unchanged_calibration.calibrationFinished, unchanged_calibration.currentParameterSet),
          (0, defaultdict(CalibrationResult), False, None), self)
    # Assert adding results worked, but the step hasn't been increased
    cur_cal = calibrations[1]
    expected_dict = defaultdict(CalibrationResult)
    expected_dict[0].results[7842] = [9.2, 3.2, 1.0]
    expected_dict[0].results[9831] = [1.2, 0.21, 9.8]
    expected_dict[0].results[2] = [0.2, 0.7, 10.20]
    assertEqualsImproved((cur_cal.currentStep, cur_cal.stepResults, cur_cal.calibrationFinished,
                          cur_cal.currentParameterSet),
                         (0, expected_dict, False, None), self)
    cur_cal = calibrations[4]
    expected_dict = defaultdict(CalibrationResult)
    expected_dict[0].results[43] = []
    assertEqualsImproved((cur_cal.currentStep, cur_cal.stepResults, cur_cal.calibrationFinished,
                          cur_cal.currentParameterSet),
                         (0, expected_dict, False, None), self)
    cur_cal = calibrations[8]
    expected_dict = defaultdict(CalibrationResult)
    expected_dict[0].results[8472] = [1.2]
    expected_dict[0].results[9013] = [4.3, 2.2, 5.4]
    assertEqualsImproved((cur_cal.currentStep, cur_cal.stepResults, cur_cal.calibrationFinished,
                          cur_cal.currentParameterSet),
                         (0, expected_dict, False, None), self)
    cur_cal = calibrations[10]
    expected_dict = defaultdict(CalibrationResult)
    expected_dict[0].results[3] = [9013.2, 10.2, 3]
    assertEqualsImproved((cur_cal.currentStep, cur_cal.stepResults, cur_cal.calibrationFinished,
                          cur_cal.currentParameterSet),
                         (0, expected_dict, False, None), self)

  def test_increment_step(self):
    """ Creates several calibrations, adds results to some, and runs the checkStepIncrement method.
    Then checks if the expected calibrations increased their step counters.
    """
    import random
    create_n_calibrations(self.calibrationService, 10, self, 10)
    # 8 Results necessary to increase the step
    assertDiracSucceeds(self.calibrationService.submitResult(1, 0, 3, [1.2, .2, .3]), self)
    assertDiracSucceeds(self.calibrationService.submitResult(1, 0, 5, [15, .1, .2]), self)
    random.seed(self)
    for i in xrange(0, 8):
      assertDiracSucceeds(self.calibrationService.submitResult(
          3, 0, 20 + i, [random.random(), random.random(), random.random()]), self)
    for i in xrange(0, 7):
      assertDiracSucceeds(self.calibrationService.submitResult(
          4, 0, 30 + i, [random.random(), random.random(), random.random()]), self)
    for i in xrange(0, 5):
      assertDiracSucceeds(self.calibrationService.submitResult(
          5, 0, 100 + i, [random.random(), random.random(), random.random()]), self)
    for i in xrange(0, 10):
      assertDiracSucceeds(self.calibrationService.submitResult(
          7, 0, 20 + i, [random.random(), random.random(), random.random()]), self)
    for i in xrange(0, 2):
      assertDiracSucceeds(self.calibrationService.submitResult(
          8, 0, 20 + i, [random.random(), random.random(), random.random()]), self)
    for i in xrange(0, 3):
      assertDiracSucceeds(self.calibrationService.submitResult(
          9, 0, 20 + i, [random.random(), random.random(), random.random()]), self)
    for i in xrange(0, 9):
      assertDiracSucceeds(self.calibrationService.submitResult(
          10, 0, 20 + i, [random.random(), random.random(), random.random()]), self)
    self.calibrationService.checkStepIncrement()
    expected_steps = [0, 0, 1, 1, 0, 0, 1, 0, 0, 1]
    internals = self.calibrationService.getInternals()
    assertDiracSucceeds(internals, self)
    (calibrations, counter) = internals['Value']
    assertEqualsImproved(counter, 11, self)
    for i in xrange(0, 10):
      assertEqualsImproved(calibrations[i + 1].currentStep, expected_steps[i], self)

  def test_getnumberofjobs(self):
    job_amounts = [10, 2, 148, 3, 190, 10000, 50, 0, 45987, 1378]
    for i in zip(xrange(0, 10), job_amounts):
      assertDiracSucceedsWith(self.calibrationService.createCalibration(
          'mySteerTest.file', 'softV.1', ['test.input1', 'other_input.txt'], job_amounts[i]), i + 1, self)
    assertDiracSucceedsWith_equals(self.calibrationService.getNumberOfJobsPerCalibration(),
                                   {1: 10, 2: 2, 3: 148, 4: 3, 5: 190, 6: 10000, 7: 50,
                                    8: 0, 9: 45987, 10: 1378}, self)

  def test_getnumberofjobs_empty(self):
    assertDiracSucceedsWith_equals(self.calibrationService.getNumberOfJobsPerCalibration(), {}, self)

  def test_getnewparams(self):
    create_n_calibrations(self.calibrationService, 5, self, 10)
    assertDiracFailsWith(self.calibrationService.getNewParameters(6, 1, 2),
                         'calibrationID is not in active calibrations: 11', self)
    self.calibrationService.setRunValues(1, 12, [2.1, 2.4, 1000.2], False)
    self.calibrationService.setRunValues(2, 1, [198, 2.9, 0.2], False)
    self.calibrationService.setRunValues(3, 0, [1.2], False)
    self.calibrationService.setRunValues(4, 523, [2.1, 2.4, 1000.2], True)
    self.calibrationService.setRunValues(5, 43, [2.1, 2.4, 1000.2], False)
    assertDiracFailsWith(self.calibrationService.getNewParameters(1, 1, 1),
                         'no new parameter set available', self)
    assertDiracSucceedsWith_equals(self.calibrationService.getNewParameters(2, 0, 0),
                                   [198, 2.9, 0.2], self)
    assertDiracFailsWith(self.calibrationService.getNewParameters(3, 0, 0),
                         'no new parameter set available', self)
    assertDiracSucceedsWith(self.calibrationService.getNewParameters(4, 1, 2), 'Calibration finished', self)
    assertDiracSucceedsWith_equals(self.calibrationService.getNewParameters(5, 1, 42),
                                   [2.1, 2.4, 1000.2], self)

#pylint: disable=invalid-name


def create_n_calibrations(calibrationService, n, assertobject, amountOfJobs=10):
  """ Creates n default calibrations to be further used by tests.

  :param RPCClient calibrationService: calibrationService used by the test
  :param int n: Amount of jobs created
  :param TestCase assertobject: the test case, used to gain the assert methods
  :param int amountOfJobs: number of jobs created in each calibration run. (constant for all runs)
  :returns: None
  """
  for i in xrange(0, n):
    res = calibrationService.createCalibration('mySteerTest.file', 'softV.1',
                                               ['test.input1', 'other_input.txt'], amountOfJobs)
    assertDiracSucceedsWith_equals(res, i + 1, assertobject)

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestHelloHandler)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestCalibrationService))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
