"""
Unit tests for the CalibrationService
"""

import unittest
import pytest
from mock import call, patch, MagicMock as Mock
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from ILCDIRAC.CalibrationSystem.Service.CalibrationHandler import CalibrationHandler, \
    CalibrationRun
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertInImproved, \
    assertEqualsImproved, assertDiracFailsWith, assertDiracSucceeds, \
    assertDiracSucceedsWith, assertDiracSucceedsWith_equals, assertMockCalls, \
    assertDiracFails

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.CalibrationSystem.Service.CalibrationHandler'

#pylint: disable=protected-access,too-many-public-methods,,no-member


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

# FIXME: Change tests to reflect new way of starting calibration creation

  def test_submitresult(self):
    with patch.object(CalibrationRun, 'submitJobs', new=Mock()):
      self.calh.export_createCalibration('', '', [], 0, '', '')
    assertDiracSucceeds(self.calh.export_submitResult(1, 0, 8234, [12.2, 1.2, .3]), self)
    assertEqualsImproved(CalibrationHandler.activeCalibrations[1].stepResults[0].results[8234],
                         [12.2, 1.2, 0.3], self)

  def test_submitresult_old_stepid(self):
    with patch.object(CalibrationRun, 'submitJobs', new=Mock()):
      for _ in xrange(0, 50):  # creates Calibrations with IDs 1-50
        self.calh.export_createCalibration('', '', [], 0, '', '')
    CalibrationHandler.activeCalibrations[27].currentStep = 13
    assertDiracSucceeds(self.calh.export_submitResult(27, 12, 9841, [5, 6, 2, 1, 7]), self)
    for i in xrange(0, 30):
      assertEqualsImproved(CalibrationHandler.activeCalibrations[27].stepResults[i].getNumberOfResults(),
                           0, self)

  def test_submitresult_wrong_calibrationID(self):
    with patch.object(CalibrationRun, 'submitJobs', new=Mock()):
      for _ in xrange(0, 50):  # creates Calibrations with IDs 1-50
        self.calh.export_createCalibration('', '', [], 0, '', '')
    res = self.calh.export_submitResult(54, 1, 9841, [5, 6, 2, 1, 7])
    assertDiracFailsWith(res, 'Calibration with id 54 not found', self)
    for i in xrange(0, 50):
      assertEqualsImproved(CalibrationHandler.activeCalibrations[27].stepResults[i].getNumberOfResults(),
                           0, self)

  def test_createcalibration(self):
    CalibrationHandler.calibrationCounter = 834 - 1  # newly created Calibration gets ID 834
    job_mock = Mock()
    with patch.object(CalibrationRun, 'submitJobs', new=job_mock):
      result = self.calh.export_createCalibration('steeringfile', 'version', ['inputfile1', 'inputfile2'],
                                                  12, '', '')
    assertDiracSucceedsWith_equals(result, (834, job_mock()), self)
    testRun = CalibrationHandler.activeCalibrations[834]
    assertEqualsImproved(
        (testRun.steeringFile, testRun.softwareVersion, testRun.inputFiles, testRun.numberOfJobs),
        ('steeringfile', 'version', ['inputfile1', 'inputfile2'], 12), self)
    assertEqualsImproved(CalibrationHandler.calibrationCounter, 834, self)  # next calibration gets ID 835

  def test_resubmitjobs(self):
    calIDsWorkIDs = [(138, 1249), (123, 1357), (498626, 4368)]
    CalibrationHandler.activeCalibrations[138] = CalibrationRun(1, '', '', [], 0)
    CalibrationHandler.activeCalibrations[123] = CalibrationRun(2, '', '', [], 0)
    CalibrationHandler.activeCalibrations[498626] = CalibrationRun(3, '', '', [], 0)
    with patch.object(CalibrationRun, 'resubmitJob', new=Mock()) as resubmit_mock:
      assertDiracSucceeds(self.calh.export_resubmitJobs(calIDsWorkIDs), self)
      assertEqualsImproved(resubmit_mock.mock_calls, [call(1249, proxyUserGroup='', proxyUserName=''),
                                                      call(1357, proxyUserGroup='', proxyUserName=''),
                                                      call(4368, proxyUserGroup='', proxyUserName='')],
                           self)

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
    testRun = CalibrationRun(1, '', '', [], 13)
    testRun.calibrationFinished = True
    CalibrationHandler.activeCalibrations[2489] = testRun
    assertDiracSucceedsWith(self.calh.export_getNewParameters(2489, 193),
                            'Calibration finished! End job now', self)

  def test_getnewparams_nonewparamsyet(self):
    testRun = CalibrationRun(1, '', '', [], 13)
    testRun.currentStep = 149
    CalibrationHandler.activeCalibrations[2489] = testRun
    assertDiracFailsWith(self.calh.export_getNewParameters(2489, 149),
                         'No new parameter set available yet', self)

  def test_getnewparams_newparams(self):
    testRun = CalibrationRun(1, '', '', [], 13)
    testRun.currentStep = 36
    testRun.currentParameterSet = 982435
    CalibrationHandler.activeCalibrations[2489] = testRun
    assertDiracSucceedsWith_equals(self.calh.export_getNewParameters(2489, 35),
                                   982435, self)

  def test_getnewparams_inactive_calibration(self):
    with patch.object(CalibrationRun, 'submitJobs', new=Mock()):
      for _ in xrange(0, 50):  # creates Calibrations with IDs 1-50
        self.calh.export_createCalibration('', '', [], 0, '', '')
    assertDiracFailsWith(self.calh.export_getNewParameters(135, 913),
                         'CalibrationID is not in active calibrations: 135', self)

  def test_calculate_params(self):
    from ILCDIRAC.CalibrationSystem.Service.CalibrationHandler import CalibrationResult
    result1 = [1, 2.3, 5]
    result2 = [0, 0.2, -0.5]
    result3 = [-10, -5.4, 2]
    obj = CalibrationRun(1, 'file', 'v123', 'input', 123)
    res = CalibrationResult()
    res.addResult(2384, result1)
    res.addResult(742, result2)
    res.addResult(9354, result3)
    obj.stepResults[42] = res
    actual = obj._CalibrationRun__calculateNewParams(42)  # pylint: disable=no-member
    expected = [-3.0, -0.9666666666666668, 2.1666666666666665]
    assert len(actual) == len(expected)
    for expected_value, actual_value in zip(expected, actual):
      self.assertTrue(abs(expected_value - actual_value) <= max(1e-09 * max(abs(expected_value),
                                                                            abs(actual_value)), 0.0),
                      'Expected values to be (roughly) the same, but they were not:\n Actual = %s,\n Expected = %s' % (actual_value, expected_value))

  def test_endcurrentstep(self):
    from ILCDIRAC.CalibrationSystem.Service.CalibrationHandler import CalibrationResult
    with patch.object(CalibrationRun, 'submitJobs', new=Mock()):
      self.calh.export_createCalibration('', '', [], 0, '', '')
    self.calh.activeCalibrations[1].currentStep = 15
    result1 = [1, 2.3, 5]
    result2 = [0, 0.2, -0.5]
    result3 = [-10, -5.4, 2]
    res = CalibrationResult()
    res.addResult(2384, result1)
    res.addResult(742, result2)
    res.addResult(9354, result3)
    self.calh.activeCalibrations[1].stepResults[15] = res
    self.calh.activeCalibrations[1].endCurrentStep()
    self.assertTrue(self.calh.activeCalibrations[1].calibrationFinished, 'Expecting calibration to be finished')

  def test_endcurrentstep_not_finished(self):
    from ILCDIRAC.CalibrationSystem.Service.CalibrationHandler import CalibrationResult
    with patch.object( CalibrationRun, 'submitJobs', new=Mock()):
      self.calh.export_createCalibration('', '', [], 0, '', '' )
    self.calh.activeCalibrations[ 1 ].currentStep = 14
    result1 = [ 1, 2.3, 5 ]
    result2 = [ 0, 0.2, -0.5 ]
    result3 = [ -10, -5.4, 2 ]
    res = CalibrationResult()
    res.addResult(2384, result1)
    res.addResult(742, result2)
    res.addResult(9354, result3)
    self.calh.activeCalibrations[1].stepResults[14] = res
    self.calh.activeCalibrations[1].endCurrentStep()
    self.assertFalse(self.calh.activeCalibrations[1].calibrationFinished,
                     'Expecting calibration to be finished')

  def test_addlists_work(self):
    # Simple case
    test_list_1 = [1, 148]
    test_list_2 = [-3, 0.2]
    testobj = CalibrationRun(1, '', '', [], 0)
    res = testobj._CalibrationRun__addLists(test_list_1, test_list_2)
    assertEqualsImproved([-2, 148.2], res, self)

  def test_addlists_work_2(self):
    # More complex case
    test_list_1 = [9013, -137.25, 90134, 4278, -123, 'abc', ['a', False]]
    test_list_2 = [0, 93, -213, 134, 98245, 'aifjg', ['some_entry', {}]]
    testobj = CalibrationRun(1, '', '', [], 0)
    res = testobj._CalibrationRun__addLists(test_list_1, test_list_2)
    assertEqualsImproved([9013, -44.25, 89921, 4412, 98122, 'abcaifjg',
                          ['a', False, 'some_entry', {}]], res, self)

  def test_addlists_empty(self):
    test_list_1 = []
    test_list_2 = []
    testobj = CalibrationRun(1, '', '', [], 0)
    res = testobj._CalibrationRun__addLists(test_list_1, test_list_2)
    assertEqualsImproved([], res, self)

  def test_addlists_incompatible(self):
    test_list_1 = [1, 83, 0.2, -123]
    test_list_2 = [1389, False, '']
    testobj = CalibrationRun(1, '', '', [], 0)
    with pytest.raises(ValueError) as ve:
      testobj._CalibrationRun__addLists(test_list_1, test_list_2)
    assertInImproved('the two lists do not have the same number of elements', ve.__str__().lower(), self)

  def test_calcnewparams_no_values(self):
    testrun = CalibrationRun(1, '', '', [], 0)
    with pytest.raises(ValueError) as ve:
      testrun._CalibrationRun__calculateNewParams(1)
    assertInImproved('no step results provided', ve.__str__().lower(), self)

  def atest_resubmitJob(self):
    pass  # FIXME: Finish atest once corresponding method is written

  def atest_submitJobs(self):
    pass  # FIXME: Finish atest once corresponding method is written
