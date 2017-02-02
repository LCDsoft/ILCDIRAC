"""
Unit tests for the CalibrationAgent
"""

import unittest
from mock import patch, MagicMock as Mock
from ILCDIRAC.CalibrationSystem.Agent.CalibrationAgent import CalibrationAgent
from ILCDIRAC.Tests.Utilities.GeneralUtils import \
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

  def test_calcjobresubmittal(self):
    jobStatusDict = {}  # FIXME: Add ids
    targetNumberDict = {89214: 100, 9824: 20, 9135: 1200, 98245: 35}
    result = self.calAgent._CalibrationAgent__calculateJobsToBeResubmitted(jobStatusDict, targetNumberDict)
    countResubmissions = defaultdict(0)
    for calibrationID, workerID in result:
      countResubmissions[calibrationID] += 1
      #FIXME: Assert workerID in jobStatusDict
    #FIXME: Assert enough jobs are being resubmitted

  def test_requestResubmission(self):
    self.calag.requestResubmission([(13875, 137), (1735, 1938), (90452, 4981)])
    self.rpc_mock().resubmitJobs.assert_called_once_with([(13875, 137), (1735, 1938), (90452, 4981)])

  def test_fetchJobStatuses(self):
    pass  # FIXME: Implement mocked calls
