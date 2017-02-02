"""
Unit tests for the CalibrationService
"""

import unittest
from mock import patch, MagicMock as Mock
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from ILCDIRAC.CalibrationSystem.Service.CalibrationHandler import CalibrationHandler, \
    CalibrationRun
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertInImproved, \
    assertEqualsImproved, assertDiracFailsWith, assertDiracSucceeds, \
    assertDiracSucceedsWith, assertDiracSucceedsWith_equals, assertMockCalls, \
    assertDiracFails

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.CalibrationSystem.Service.CalibrationHandler'

#pylint: disable=protected-access


class CalibrationHandlerTest(unittest.TestCase):
  """ Tests the implementation of the methods of the CalibrationService classes """

  @classmethod
  def setUpClass(cls):
    CalibrationHandler.initializeHandler(None)

  def setUp(self):
    """ Create a CalibrationHandler instance so we can check some basic functionality. """
    self.transport_mock = Mock()
    RequestHandler._rh__initializeClass(Mock(), Mock(), Mock(), Mock())
    self.calh = CalibrationHandler({}, self.transport_mock)
    self.calh.initialize()

  def tearDown(self):
    CalibrationHandler.activeCalibrations = {}
    CalibrationHandler.calibrationCounter = 0

  def test_submitresult(self):
    with patch.object(CalibrationRun, 'submitInitialJobs', new=Mock()):
      self.calh.export_createCalibration('', '', [], 0)
    assertDiracSucceeds(self.calh.export_submitResult(1, 0, 8234, [12.2, 1.2, .3]), self)
    assertEqualsImproved(CalibrationHandler.activeCalibrations[1].stepResults[0].results[8234],
                         [12.2, 1.2, 0.3], self)

  def test_submitresult_old_stepid(self):
    with patch.object(CalibrationRun, 'submitInitialJobs', new=Mock()):
      for _ in xrange(0, 50):  # creates Calibrations with IDs 1-50
        self.calh.export_createCalibration('', '', [], 0)
    CalibrationHandler.activeCalibrations[27].currentStep = 13
    assertDiracSucceeds(self.calh.export_submitResult(27, 12, 9841, [5, 6, 2, 1, 7]), self)
    for i in xrange(0, 30):
      assertEqualsImproved(CalibrationHandler.activeCalibrations[27].stepResults[i].getNumberOfResults(),
                           0, self)

  def test_submitresult_wrong_calibrationID(self):
    with patch.object(CalibrationRun, 'submitInitialJobs', new=Mock()):
      for _ in xrange(0, 50):  # creates Calibrations with IDs 1-50
        self.calh.export_createCalibration('', '', [], 0)
    res = self.calh.export_submitResult(54, 1, 9841, [5, 6, 2, 1, 7])
    assertDiracFailsWith(res,
                         'Calibration with id 54 not found', self)
    for i in xrange(0, 50):
      assertEqualsImproved(CalibrationHandler.activeCalibrations[27].stepResults[i].getNumberOfResults(),
                           0, self)

  def test_createcalibration(self):
    CalibrationHandler.calibrationCounter = 834 - 1  # newly created Calibration gets ID 834
    result = self.calh.export_createCalibration('steeringfile', 'version', ['inputfile1', 'inputfile2'], 12)
    assertDiracSucceedsWith_equals(result, 834, self)
    testRun = CalibrationHandler.activeCalibrations[834]
    assertEqualsImproved(
        (testRun.steeringFile, testRun.softwareVersion, testRun.inputFiles, testRun.numberOfJobs),
        ('steeringfile', 'version', ['inputfile1', 'inputfile2'], 12), self)
    assertEqualsImproved(CalibrationHandler.calibrationCounter, 834, self)  # next calibration gets ID 835

  def test_resubmitjobs(self):
    calIDsWorkIDs = [(138, 1249), (123, 1357), (498626, 4368)]
    CalibrationHandler.activeCalibrations[138] = CalibrationRun('', '', [], 0)
    CalibrationHandler.activeCalibrations[123] = CalibrationRun('', '', [], 0)
    CalibrationHandler.activeCalibrations[498626] = CalibrationRun('', '', [], 0)
    with patch.object(CalibrationRun, 'resubmitJob', new=Mock()) as resubmit_mock:
      assertDiracSucceeds(self.calh.export_resubmitJobs(calIDsWorkIDs), self)
      assertMockCalls(resubmit_mock, [1249, 1357, 4368], self)

  def test_resubmitjobs_fails(self):
    calIDsWorkIDs = [(138, 1249), (198735, 1357), (498626, 4368)]
    CalibrationHandler.activeCalibrations[138] = CalibrationRun('', '', [], 0)
    CalibrationHandler.activeCalibrations[123] = CalibrationRun('', '', [], 0)
    CalibrationHandler.activeCalibrations[498626] = CalibrationRun('', '', [], 0)
    with patch.object(CalibrationRun, 'resubmitJob', new=Mock()):
      res = self.calh.export_resubmitJobs(calIDsWorkIDs)
      assertDiracFails(res, self)
      assertInImproved('Could not resubmit all jobs', res['Message'], self)
      assertInImproved('[(198735, 1357)]', res['Message'], self)
      assertEqualsImproved([(198735, 1357)], res['failed_pairs'], self)

  def test_getnumberofjobs(self):
    calrun_mock_1 = Mock()
    calrun_mock_1.numberOfJobs = 815
    calrun_mock_2 = Mock()
    calrun_mock_2.numberOfJobs = 421
    calrun_mock_3 = Mock()
    calrun_mock_3.numberOfJobs = 100
    calrun_mock_4 = Mock()
    calrun_mock_4.numberOfJobs = 0
    calrun_mock_5 = Mock()
    calrun_mock_5.numberOfJobs = 1040
    CalibrationHandler.activeCalibrations = {
        782145: calrun_mock_1, 72453: calrun_mock_2, 189455: calrun_mock_3,
        954692: calrun_mock_4, 29485: calrun_mock_5}
    result = self.calh.export_getNumberOfJobsPerCalibration()
    assertDiracSucceeds(result, self)
    assertEqualsImproved(result['Value'], {782145: 815, 72453: 421, 189455: 100,
                                           954692: 0, 29485: 1040}, self)

  def test_getnewparams_calculationfinished(self):
    testRun = CalibrationRun('', '', [], 13)
    testRun.calibrationFinished = True
    CalibrationHandler.activeCalibrations[2489] = testRun
    assertDiracSucceedsWith(self.calh.export_getNewParameters(2489, 193),
                            'Calibration finished! End job now', self)

  def test_getnewparams_nonewparamsyet(self):
    testRun = CalibrationRun('', '', [], 13)
    testRun.currentStep = 149
    CalibrationHandler.activeCalibrations[2489] = testRun
    assertDiracFailsWith(self.calh.export_getNewParameters(2489, 149),
                         'No new parameter set available yet', self)

  def test_getnewparams_newparams(self):
    testRun = CalibrationRun('', '', [], 13)
    testRun.currentStep = 36
    testRun.currentParameterSet = 982435
    CalibrationHandler.activeCalibrations[2489] = testRun
    assertDiracSucceedsWith_equals(self.calh.export_getNewParameters(2489, 35),
                                   982435, self)

  def test_getnewparams_inactive_calibration(self):
    with patch.object(CalibrationRun, 'submitInitialJobs', new=Mock()):
      for _ in xrange(0, 50):  # creates Calibrations with IDs 1-50
        self.calh.export_createCalibration('', '', [], 0)
    assertDiracFailsWith(self.calh.export_getNewParameters(135, 913),
                         'CalibrationID is not in active calibrations: 135', self)

  def test_calculate_params(self):
    from ILCDIRAC.CalibrationSystem.Service.CalibrationHandler import CalibrationResult
    a = [1, 2.3, 5]
    b = [0, 0.2, -0.5]
    c = [-10, -5.4, 2]
    obj = CalibrationRun('file', 'v123', 'input', 123)
    res = CalibrationResult()
    res.addResult(2384, a)
    res.addResult(742, b)
    res.addResult(9354, c)
    obj.stepResults[42] = res
    actual = obj._CalibrationRun__calculateNewParams(42)  # pylint: disable=no-member
    expected = [-3.0, -0.9666666666666668, 2.1666666666666665]
    assert len(actual) == len(expected)
    for expected_value, actual_value in zip(expected, actual):
      self.assertTrue(abs(expected_value - actual_value) <= max(1e-09 * max(abs(expected_value),
                                                                            abs(actual_value)), 0.0),
                      'Expected values to be (roughly) the same, but they were not:\n Actual = %s,\n Expected = %s' % (actual_value, expected_value))

  def atest_resubmitJob(self):
    pass  # FIXME: Finish atest

  def atest_submitInitialJobs(self):
    pass  # FIXME: Finish atest

#TODO: Integration test cases: Put service into well-defined state, then call export_ methods from client code. Run on VOILCDIRAC.
#Methods to test: submitResult, resubmitJobs, getNumberofJobsPerCalibration
