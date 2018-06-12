#!/usr/bin/env python
"""Tests the ReportErrors WorkflowModule."""

import unittest
from collections import defaultdict
from mock import MagicMock as Mock, call

from ILCDIRAC.Workflow.Modules.ReportErrors import ReportErrors
from ILCDIRAC.Tests.Utilities.GeneralUtils import MatchStringWith

__RCSID__ = "$Id$"


class TestReportErrors(unittest.TestCase):
  """Test ReportErrors."""

  def setUp(self):
    """Set up the tests."""
    self.repErr = ReportErrors()
    self.repErr.log = Mock()
    self.repErr.log.info = Mock(name="LogInfo")
    self.repErr.log.error = Mock(name="LogError")
    self.repErr.workflow_commons = {}

  def test_execute(self):
    """Test the execute function."""
    res = self.repErr.execute()
    self.assertTrue(res['OK'])
    self.repErr.log.info.assert_called_with(MatchStringWith('No errors encountered'))

    errorKey = "%s_%s" % ('appname', 'appver')
    message = 'something really bad'
    stdError = 'Segmentation Violation'
    self.repErr.workflow_commons.setdefault('ErrorDict', defaultdict(list))[errorKey].extend([message, stdError])

    res = self.repErr.execute()

    self.assertTrue(res['OK'])
    calls = [call(errorKey, 'something really bad'),
             call(errorKey, 'Segmentation Violation')]
    self.repErr.log.error.assert_has_calls(calls, any_order=True)
