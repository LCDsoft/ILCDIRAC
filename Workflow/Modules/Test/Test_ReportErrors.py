#!/usr/bin/env python
"""Tests the ReportErrors WorkflowModule."""

import unittest
from collections import defaultdict
from mock import MagicMock as Mock, call, patch

from ILCDIRAC.Workflow.Modules.ReportErrors import ReportErrors
from ILCDIRAC.Tests.Utilities.GeneralUtils import MatchStringWith

__RCSID__ = "$Id$"
MODULE_NAME = 'ILCDIRAC.Workflow.Modules.ReportErrors'


class TestReportErrors(unittest.TestCase):
  """Test ReportErrors."""

  def setUp(self):
    """Set up the tests."""
    self.log = Mock()
    self.log.info = Mock(name="LogInfo")
    self.log.error = Mock(name="LogError")

    self.patches = [patch('%s.LOG' % MODULE_NAME, new=self.log)]

    for patcher in self.patches:
      patcher.start()

    self.repErr = ReportErrors()
    self.repErr.workflow_commons = {}

  def tearDown(self):
    """Clean up test resources."""
    for patcher in self.patches:
      patcher.stop()

  def test_execute(self):
    """Test the execute function."""
    res = self.repErr.execute()
    self.assertTrue(res['OK'])
    self.log.info.assert_called_with(MatchStringWith('No errors encountered'))

    errorKey = "%s_%s" % ('appname', 'appver')
    message = 'something really bad'
    stdError = 'Segmentation Violation'
    self.repErr.workflow_commons.setdefault('ErrorDict', defaultdict(list))[errorKey].extend([message, stdError])

    res = self.repErr.execute()

    self.assertTrue(res['OK'])
    calls = [call(errorKey, 'something really bad'),
             call(errorKey, 'Segmentation Violation')]
    self.log.error.assert_has_calls(calls, any_order=True)
