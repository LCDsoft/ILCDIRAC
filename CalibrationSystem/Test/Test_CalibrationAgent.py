"""Unit tests for the CalibrationAgent."""

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

# pylint: disable=protected-access,no-member


@pytest.yield_fixture
def calibAgent(mocker):
  """Create calibration handler."""
  mocker.patch('DIRAC.Core.Base.Client.Client', new=Mock(return_value=S_OK()))
  mocker.patch('DIRAC.Core.Base.AgentModule.AgentModule.__init__', new=Mock(return_value=None))
  calibAgent = CalibrationAgent("dummyAgentName", "dummyLoadName")
  return calibAgent


def test_execute_calibrationWithNoDiracJobsIsPresentInRunningCalibrationList(mocker, calibAgent):
  """Test behaviour of the execute() function when there is a running calibration with no dirac jobs assigned to it.

  Calibration #63 is present in the list of calibrations but doesn't have any grid jobs associated to is.
  When considering calibration #63 the agent should send an error to the logger and continue with execution.
  """
  dict1 = {64: {1: 'Running', 2: 'Failed'},
           65: {5: 'Running', 6: 'Finished'},
           66: {14: 'Killed'}}
  dict2 = {64: {417251: 'Running', 12741: 'Failed'},
           65: {4178: 'Running', 444: 'Finished'},
           66: {555: 'Killed'}}
  dict3 = {64: {'Owner': 'ow1', 'OwnerGroup': 'owGr1', 'OwnerDN': 'dummy'},
           65: {'Owner': 'ow2', 'OwnerGroup': 'owGr2', 'OwnerDN': 'dummy'},
           66: {'Owner': 'ow3', 'OwnerGroup': 'owGr3', 'OwnerDN': 'dummy'}}
  mockReturn_fetchJobStatuses = S_OK({'jobStatusVsWorkerId': dict1,
                                      'jobStatusVsJobId': dict2, 'calibrationOwnership': dict3})

  mockReturn_getNumberOfJobsPerCalibration = S_OK({63: 666, 64: 2, 65: 2, 66: 1})
  mockReturn_getRunningCalibrations = S_OK([63, 64, 65, 66])

  mockReturn_calibrationService = Mock()
  mockReturn_calibrationService.getNumberOfJobsPerCalibration = (
      Mock(return_value=mockReturn_getNumberOfJobsPerCalibration))
  mockReturn_calibrationService.getRunningCalibrations = Mock(return_value=mockReturn_getRunningCalibrations)
  mockReturn_calibrationService.checkForStepIncrement = Mock(return_value=S_OK())
  calibAgent.calibrationService = mockReturn_calibrationService

  calibAgent.log = Mock()

  mocker.patch.object(calibAgent, "fetchJobStatuses", new=Mock(return_value=mockReturn_fetchJobStatuses))
  mocker.patch.object(calibAgent, "checkForCalibrationsToBeKilled", new=Mock(return_value=S_OK()))
  mock_requestResubmission = Mock(return_value=S_OK())
  mocker.patch.object(calibAgent, "requestResubmission", side_effect=mock_requestResubmission)
  res = calibAgent.execute()

  mock_requestResubmission.assert_called_once_with([(64, 2), (66, 14)])
  assert res['OK']


class CalibrationAgentTest(unittest.TestCase):
  """Test the implementation of the methods of the CalibrationAgent."""

  def setUp(self):
    """Set up the objects."""
    import DIRAC
    with patch.object(DIRAC.ConfigurationSystem.Client.PathFinder, 'getAgentSection', new=Mock(return_value='')):
      self.calag = CalibrationAgent('Calibration/testCalAgent', 'testLoadname')
    self.rpc_mock = Mock()
    with patch('%s.Client' % MODULE_NAME, new=self.rpc_mock):
      res = self.calag.initialize()
      assert res['OK']

  def test_getworkerid(self):
    """Get worker id."""
    assertEqualsImproved(self.calag._CalibrationAgent__getWorkerIDFromJobName(
        'PandoraCaloCalibration_calid_149814_workerid_813102'), 813102, self)

  def test_getcalibrationid(self):
    """Get calibration id."""
    assertEqualsImproved(self.calag._CalibrationAgent__getCalibrationIDFromJobName(
        'PandoraCaloCalibration_calid_149814_workerid_813102'), 149814, self)

  def test_getworkerid_splitfails(self):
    """Get worker id."""
    with self.assertRaises(IndexError):
      self.calag._CalibrationAgent__getWorkerIDFromJobName('PandoraCaloCalibrationcalid149814workerid813102')

  def test_getcalibrationid_splitfails(self):
    """Get calibration id."""
    with self.assertRaises(IndexError):
      self.calag._CalibrationAgent__getCalibrationIDFromJobName('PandoraCaloCalibrationcalid149814workerid813102')

  def test_getworkerid_noidgiven(self):
    """Get worker id."""
    with self.assertRaises(ValueError):
      self.calag._CalibrationAgent__getWorkerIDFromJobName('PandoraCaloCalibration_calid_149814_workerid_')

  def test_getcalibrationid_noidgiven(self):
    """Get calibration id."""
    with self.assertRaises(ValueError):
      self.calag._CalibrationAgent__getCalibrationIDFromJobName('PandoraCaloCalibration_calid_')

  def test_getworkerid_conversion_fails(self):
    """Get worker id."""
    with self.assertRaises(ValueError):
      assertEqualsImproved(self.calag._CalibrationAgent__getWorkerIDFromJobName(
          'PandoraCaloCalibration_calid_sixteen_workerid_twenty'), 813102, self)

  def test_getcalibrationid_conversion_fails(self):
    """Get calibration id."""
    with self.assertRaises(ValueError):
      assertEqualsImproved(self.calag._CalibrationAgent__getCalibrationIDFromJobName(
          'PandoraCaloCalibration_calid_sixteen_workerid_twenty'), 149814, self)

  def test_calcjobresubmittal(self):  # pylint: disable=too-many-branches
    """Test calculateJobsToBeResubmitted function."""
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
    """Assert nothing is thrown."""
    self.calag.requestResubmission([(13875, 137), (1735, 1938), (90452, 4981)])
    self.rpc_mock().resubmitJobs.assert_called_once_with([(13875, 137), (1735, 1938), (90452, 4981)])

  def test_requestResubmission_permanent_fail(self):
    """Calibration 1 works, Calibration 5 always fails."""
    def mock_resubmit(failedJobs):
      """Mock the resubmission method of the service."""
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

  #  def test_requestResubmission_fail_then_success( self ):
  #    def mock_resubmit( failedJobs ):
  #      """ Mocks the resubmission method of the service """
  #      print 'failedJobs: %s' % failedJobs
  #      if ( 3, 13135 ) in failedJobs and len( failedJobs ) == 5:
  #        result = S_ERROR( 'Could not resubmit all jobs. Failed calibration/worker pairs are: [(6,39105),(2,1843)]' )
  #        result[ 'failed_pairs' ] = [ ( 6, 39105 ), ( 2, 1843 ) ]
  #        return result
  #      elif len( failedJobs ) == 2 and ( 2, 1843 ) in failedJobs and ( 6, 39105 ) in failedJobs:
  #        result = S_ERROR( 'Could not resubmit all jobs. Failed calibration/worker pairs are: [(6,39105)' )
  #        result[ 'failed_pairs' ] = [ ( 6, 39105 ) ]
  #        return result
  #      elif len( failedJobs ) == 1 and ( 6, 39105 ) in failedJobs:
  #        return S_OK()
  #      else:
  #        raise IOError( 'test failed. list should never be empty.' )
  #    self.rpc_mock().resubmitJobs.side_effect = mock_resubmit
  #    # assert nothing is thrown
  #    self.calag.requestResubmission( [ ( 2, 1843 ), ( 3, 19485 ), ( 3, 13135 ), ( 3, 1835 ), ( 6, 39105 ) ] )

  def test_fetchJobStatuses(self):
    """Test getJobsParameters function."""
    jobmon_mock = Mock(name='jobmon_mock')

    def mock_getJobs(inDict):
      if '64' in inDict['JobGroup']:
        return S_OK([417251, 12741])
      elif '65' in inDict['JobGroup']:
        return S_OK([4178, 444])
      elif '66' in inDict['JobGroup']:
        return S_OK([555])
      else:
        return S_ERROR()

    jobmon_mock().getJobs.side_effect = mock_getJobs
    jobmon_mock().getJobsParameters.return_value = S_OK({
        'some_cal_1': {'JobName': 'PandoraCaloCalibration_calid_64_workerid_1', 'Status': 'Running', 'JobID': 417251,
                       'Owner': 'ow1', 'OwnerGroup': 'owGr1', 'OwnerDN': 'dummy'},
        'some_cal_2': {'JobName': 'PandoraCaloCalibration_calid_64_workerid_2', 'Status': 'Failed', 'JobID': 12741,
                       'Owner': 'ow1', 'OwnerGroup': 'owGr1', 'OwnerDN': 'dummy'},
        'some_cal_3': {'JobName': 'PandoraCaloCalibration_calid_65_workerid_5', 'Status': 'Running', 'JobID': 4178,
                       'Owner': 'ow2', 'OwnerGroup': 'owGr2', 'OwnerDN': 'dummy'},
        'some_cal_4': {'JobName': 'PandoraCaloCalibration_calid_65_workerid_6', 'Status': 'Finished', 'JobID': 444,
                       'Owner': 'ow2', 'OwnerGroup': 'owGr2', 'OwnerDN': 'dummy'},
        'some_other_cal': {'JobName': 'PandoraCaloCalibration_calid_66_workerid_14', 'Status': 'Killed', 'JobID': 555,
                           'Owner': 'ow3', 'OwnerGroup': 'owGr3', 'OwnerDN': 'dummy'}})
    calibservice_mock = Mock(name='calibServiceMock')
    calibservice_mock().getActiveCalibrations.return_value = S_OK([64, 65, 66])
    with patch('%s.JobMonitoringClient' % MODULE_NAME, new=jobmon_mock):
      with patch('%s.Client' % MODULE_NAME, new=calibservice_mock):
        self.calag.initialize()
        status_dict = self.calag.fetchJobStatuses()
        dict1 = {64: {1: 'Running', 2: 'Failed'},
                 65: {5: 'Running', 6: 'Finished'},
                 66: {14: 'Killed'}}
        dict2 = {64: {417251: 'Running', 12741: 'Failed'},
                 65: {4178: 'Running', 444: 'Finished'},
                 66: {555: 'Killed'}}
        dict3 = {64: {'Owner': 'ow1', 'OwnerGroup': 'owGr1', 'OwnerDN': 'dummy'},
                 65: {'Owner': 'ow2', 'OwnerGroup': 'owGr2', 'OwnerDN': 'dummy'},
                 66: {'Owner': 'ow3', 'OwnerGroup': 'owGr3', 'OwnerDN': 'dummy'}}
        assertEqualsImproved(status_dict, S_OK({'jobStatusVsWorkerId': dict1,
                                                'jobStatusVsJobId': dict2, 'calibrationOwnership': dict3}), self)
    jobmon_mock().getJobs.assert_called_with({'JobGroup': 'PandoraCaloCalibration_calid_66'})
    jobmon_mock().getJobsParameters.assert_called_once_with([417251, 12741, 4178, 444, 555], [
        'JobName', 'Status', 'JobID', 'Owner', 'OwnerGroup', 'OwnerDN'])
