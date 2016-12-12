"""
Unit tests for the CalibrationAgent
"""

import unittest
from mock import mock_open, patch, MagicMock as Mock
from collections import defaultdict
from ILCDIRAC.CalibrationSystem.Agent.CalibrationAgent import CalibrationAgent
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertInImproved, \
    assertEqualsImproved, assertDiracFailsWith, assertDiracSucceeds, \
    assertDiracSucceedsWith, assertDiracSucceedsWith_equals, assertMockCalls
from DIRAC import S_OK, S_ERROR

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.CalibrationSystem.Agent.CalibrationAgent'

#pylint: disable=protected-access


class CalibrationAgentTest(object):
  """ Tests the implementation of the methods of the CalibrationAgent """

  def setUp(self):
    """set up the objects"""
    self.calag = CalibrationAgent('testCalAgent', 'testLoadname')
    with patch('%s.RPCClient' % MODULE_NAME, new=Mock(side_effect=IOError('works'))):
      res = self.calag.initialize()
      assert res['OK']

  def test_getworkerid(self):
    assertEqualsImproved(self.calag.__getWorkerIDFromJobName(
        'CalibrationService_calid_149814_workerid_813102'), 813102, self)

  def test_getcalibrationid(self):
    assertEqualsImproved(self.calag.__getWorkerIDFromJobName(
        'CalibrationService_calid_149814_workerid_813102'), 149814, self)
    #FIXME: Add more tests for failure of method, corner cases etc

  def test_calcjobresubmittal(self):
    jobStatusDict = {}  # FIXME: Add ids
    targetNumberDict = {89214: 100, 9824: 20, 9135: 1200, 98245: 35}
    result = self.calAgent.__calculateJobsToBeResubmitted(jobStatusDict, targetNumberDict)
    countResubmissions = defaultdict(0)
    for calibrationID, workerID in result:
      countResubmissions[calibrationID] += 1
      #FIXME: Assert workerID in jobStatusDict
    #FIXME: Assert enough jobs are being resubmitted

  def test_requestResubmission(self):
    pass  # FIXME: Implement mock call to calibration service

  def test_fetchJobStatuses(self):
    pass  # FIXME: Implement mocked calls
