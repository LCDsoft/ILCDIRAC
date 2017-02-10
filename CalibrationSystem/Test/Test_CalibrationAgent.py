"""
Unit tests for the CalibrationAgent
"""

from collections import defaultdict
import itertools
import unittest
import pytest
from mock import patch, MagicMock as Mock
from DIRAC import S_OK, S_ERROR
from ILCDIRAC.CalibrationSystem.Agent.CalibrationAgent import CalibrationAgent
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertInImproved, \
    assertEqualsImproved

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.CalibrationSystem.Agent.CalibrationAgent'

#pylint: disable=protected-access,no-member


class CalibrationAgentTest(unittest.TestCase):
  """ Tests the implementation of the methods of the CalibrationAgent """

  def setUp(self):
    """set up the objects"""
    import DIRAC
    with patch.object(DIRAC.ConfigurationSystem.Client.PathFinder, 'getAgentSection', new=Mock(return_value='')):
      self.calag = CalibrationAgent('Calibration/testCalAgent', 'testLoadname')
    self.rpc_mock = Mock()
    with patch('%s.RPCClient' % MODULE_NAME, new=self.rpc_mock):
      res = self.calag.initialize()
      assert res['OK']

  def test_getworkerid(self):
    assertEqualsImproved(self.calag._CalibrationAgent__getWorkerIDFromJobName(
        'CalibrationService_calid_149814_workerid_813102'), 813102, self)

  def test_getcalibrationid(self):
    assertEqualsImproved(self.calag._CalibrationAgent__getCalibrationIDFromJobName(
        'CalibrationService_calid_149814_workerid_813102'), 149814, self)

  def test_getworkerid_splitfails(self):
    with self.assertRaises(IndexError):
      self.calag._CalibrationAgent__getWorkerIDFromJobName('CalibrationServicecalid149814workerid813102')

  def test_getcalibrationid_splitfails(self):
    with self.assertRaises(IndexError):
      self.calag._CalibrationAgent__getCalibrationIDFromJobName('CalibrationServicecalid149814workerid813102')

  def test_getworkerid_noidgiven(self):
    with self.assertRaises(ValueError):
      self.calag._CalibrationAgent__getWorkerIDFromJobName('CalibrationService_calid_149814_workerid_')

  def test_getcalibrationid_noidgiven(self):
    with self.assertRaises(ValueError):
      self.calag._CalibrationAgent__getCalibrationIDFromJobName('CalibrationService_calid_')

  def test_getworkerid_conversion_fails(self):
    with self.assertRaises(ValueError):
      assertEqualsImproved(self.calag._CalibrationAgent__getWorkerIDFromJobName(
          'CalibrationService_calid_sixteen_workerid_twenty'), 813102, self)

  def test_getcalibrationid_conversion_fails(self):
    with self.assertRaises(ValueError):
      assertEqualsImproved(self.calag._CalibrationAgent__getCalibrationIDFromJobName(
          'CalibrationService_calid_sixteen_workerid_twenty'), 149814, self)

  def test_calcjobresubmittal(self):  # pylint: disable=too-many-branches
    workermappings = [{}, {}, {}, {}]
    for i in xrange(0, 4):
      if i == 0:  # 100 jobs in total, 87 are ok - expectation: No jobs to be resubmitted
        for j in xrange(25, 301, 6):  # adds 46 jobs
          workermappings[i][j] = 'Running'
        assert len(workermappings[i]) == 46
        for j in xrange(28, 274, 6):  # adds 41 jobs
          workermappings[i][j] = 'Finished'
        assert len(workermappings[i]) == 87
        for j in xrange(1, 25, 2):  # adds 12 jobs
          workermappings[i][j] = 'Killed'
        workermappings[i][2] = 'Failed'
        assert len(workermappings[i]) == 100
      elif i == 1:  # 20 jobs in total, 10 failed - expectation: Resubmit those 10
        for j in xrange(13, 73, 6):  # adds 10 jobs
          workermappings[i][j] = 'Finished'
        assert len(workermappings[i]) == 10
        for j in xrange(100, 128, 4):  # adds 7 jobs
          workermappings[i][j] = 'Killed'
        assert len(workermappings[i]) == 17
        for j in xrange(1, 7, 2):  # adds 3 jobs
          workermappings[i][j] = 'Failed'
        assert len(workermappings[i]) == 20
      elif i == 2:  # 1200 jobs, all running - expectation: No resubmission
        for j in xrange(1, 1201):  # adds 1200 jobs
          workermappings[i][j] = 'Running'
        assert len(workermappings[i]) == 1200
      else:  # 35 jobs, all killed - expectation: Resubmit all
        for j in xrange(1, 70, 2):  # adds 35 jobs
          workermappings[i][j] = 'Killed'
        assert len(workermappings[i]) == 35
    jobStatusDict = {89214: workermappings[0], 9824: workermappings[1],
                     9135: workermappings[2], 98245: workermappings[3]}
    targetNumberDict = {89214: 100, 9824: 20, 9135: 1200, 98245: 35}
    result = self.calag._CalibrationAgent__calculateJobsToBeResubmitted(jobStatusDict, targetNumberDict)
    countResubmissions = defaultdict(int)  # defaults to 0
    for calibrationID, _ in result:
      countResubmissions[calibrationID] += 1
    assertEqualsImproved((countResubmissions[89124], countResubmissions[9824],
                          countResubmissions[9135], countResubmissions[98245]),
                         (0, 10, 0, 35), self)
    expected_resubmissions = [[], itertools.chain(xrange(100, 128, 4), xrange(1, 7, 2)),
                              [], xrange(1, 70, 2)]
    calibIDs = [89124, 9824, 9135, 98245]
    assert len(expected_resubmissions) == len(calibIDs)
    for i, expectation in zip(calibIDs, expected_resubmissions):
      for expected_workerid in expectation:
        assertInImproved((i, expected_workerid), result, self)

  def test_requestResubmission(self):
    # assert nothing is thrown
    self.calag.requestResubmission([(13875, 137), (1735, 1938), (90452, 4981)])
    self.rpc_mock().resubmitJobs.assert_called_once_with([(13875, 137), (1735, 1938), (90452, 4981)])

  def test_requestResubmission_permanent_fail(self):
    # Calibration 1 works, Calibration 5 always fails
    def mock_resubmit(failedJobs):
      """ Mocks the resubmission method of the service """
      if ((1, 419857) in failedJobs and len(failedJobs) == 4) or \
         (len(failedJobs) == 1 and (5, 713) in failedJobs):
        result = S_ERROR('Could not resubmit all jobs. Failed calibration/worker pairs are: [(5,713)]')
        result['failed_pairs'] = [(5, 713)]
        return result
      else:
        raise IOError('test failed. list should never be empty.')
    self.rpc_mock().resubmitJobs.side_effect = mock_resubmit
    with pytest.raises(RuntimeError) as re:
      self.calag.requestResubmission([(1, 2847), (5, 713), (1, 419857), (1, 1498)])
      assertInImproved('cannot resubmit the necessary failed jobs', re.message.lower(), self)
      assertInImproved('5,713', re.message.lower(), self)

  def test_requestResubmission_fail_then_success(self):
    def mock_resubmit(failedJobs):
      """ Mocks the resubmission method of the service """
      print 'failedJobs: %s' % failedJobs
      if (3, 13135) in failedJobs and len(failedJobs) == 5:
        result = S_ERROR('Could not resubmit all jobs. Failed calibration/worker pairs are: [(6,39105),(2,1843)]')
        result['failed_pairs'] = [(6, 39105), (2, 1843)]
        return result
      elif len(failedJobs) == 2 and (2, 1843) in failedJobs and (6, 39105) in failedJobs:
        result = S_ERROR('Could not resubmit all jobs. Failed calibration/worker pairs are: [(6,39105)')
        result['failed_pairs'] = [(6, 39105)]
        return result
      elif len(failedJobs) == 1 and (6, 39105) in failedJobs:
        return S_OK()
      else:
        raise IOError('test failed. list should never be empty.')
    self.rpc_mock().resubmitJobs.side_effect = mock_resubmit
    # assert nothing is thrown
    self.calag.requestResubmission([(2, 1843), (3, 19485), (3, 13135), (3, 1835), (6, 39105)])

  def test_fetchJobStatuses(self):
    jobmon_mock = Mock()
    jobmon_mock().getJobs.return_value = S_OK([417251, 12741, 4178])
    jobmon_mock().getJobParameters.return_value = S_OK({
        'some_cal_1': {'Name': 'A_CalID_123_WorkID_123', 'Status': 'Running'},
        'some_cal_2': {'Name': 'A_CalID_4289_WorkID_742', 'Status': 'Failed'},
        'some_cal_3': {'Name': 'A_CalID_123_WorkID_124', 'Status': 'Running'},
        'some_cal_4': {'Name': 'A_CalID_1_WorkID_2', 'Status': 'Finished'},
        'some_other_cal': {'Name': 'A_CalID_1_WorkID_918437', 'Status': 'Killed'}})
    with patch('%s.RPCClient' % MODULE_NAME, new=jobmon_mock):
      status_dict = CalibrationAgent.fetchJobStatuses()
      assertEqualsImproved(status_dict, S_OK({123: {123: 'Running', 124: 'Running'},
                                              4289: {742: 'Failed'},
                                              1: {2: 'Finished', 918437: 'Killed'},
                                              'OK': True}, self))
    jobmon_mock().getJobs.assert_called_once_with({'JobGroup': 'CalibrationService_calib_job'})
    jobmon_mock().getJobParameters.assert_called_once_with([417251, 12741, 4178], ['Name', 'Status'])

