"""
Unit tests for the UploadLogFile module
"""

import unittest
from mock import patch, MagicMock as Mock

from ILCDIRAC.Workflow.Modules.UploadLogFile import UploadLogFile 
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertInImproved, \
  assertEqualsImproved, assertDiracFailsWith, assertDiracSucceeds, \
  assertDiracSucceedsWith, assertDiracSucceedsWith_equals
from DIRAC import S_OK, S_ERROR

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Workflow.Modules.UploadLogFile'

class UploadLogFileTestCase( unittest.TestCase ):
  """ Contains tests for the UploadLogFile class"""
  def setUp( self ):
    """set up the objects"""
    self.ulf = UploadLogFile()

  def test_execute( self ):
    self.ulf.jobID = 8194
    self.ulf.workflow_commons = { 'Request' : 'something' }
    with patch.object(self.ulf, 'resolveInputVariables', new=Mock(return_value=S_ERROR('my_testerr'))), \
         patch.object(self.ulf, '_determineRelevantFiles', new=Mock(return_value=S_ERROR('log_err'))):
      assertDiracSucceeds( self.ulf.execute(), self )

# TODO continue here












