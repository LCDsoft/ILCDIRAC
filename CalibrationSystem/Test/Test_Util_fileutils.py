"""Unit tests for the CalibrationAgent."""

import filecmp
import os
import unittest

from ILCDIRAC.CalibrationSystem.Utilities.fileutils import binaryFileToString, stringToBinaryFile


__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.CalibrationSystem.Agent.CalibrationAgent'


class TestsFileUtils(unittest.TestCase):
  """Test the utilities for the CalibrationSystem."""

  def setUp(self):
    """Set up the objects."""
    self.targetFile = "targetFile.root"

  def tearDown(self):
    """Tear down the objects."""
    # os.unlink( self.targetFile )
    pass

  def test_binaryToString(self):
    """Test stringToBinaryFile function."""
    filename = os.path.join(os.environ['DIRAC'], "ILCDIRAC", "Testfiles", "input.root")
    content = binaryFileToString(filename)
    stringToBinaryFile(content, self.targetFile)
    self.assertTrue(filecmp.cmp(filename, self.targetFile), "Files are not the same any more")
