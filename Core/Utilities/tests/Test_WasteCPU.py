"""Test WasteCPU """

import unittest
from mock import MagicMock as Mock, patch

from ILCDIRAC.Core.Utilities.WasteCPU import wasteCPUCycles

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Core.Utilities.WasteCPU'

class WasteCPUTest( unittest.TestCase ):
  """Test the WasteCPU"""

  def setUp ( self ):
    pass

  def tearDown ( self ):
    pass

  def test_success( self ):
    """wasteCPUCycles suceeeds to waste............................................................."""
    self.assertTrue( wasteCPUCycles(1)['OK'] )

  def test_fail1( self ):
    """wasteCPUCycles fails 1......................................................................."""
    with patch("%s.log" % MODULE_NAME, new=Mock(side_effect=ValueError("MockedValue"))):
      self.assertFalse( wasteCPUCycles(1)['OK'] )
      self.assertIn( "MockedValue",  wasteCPUCycles(1)['Message'] )

  def test_fail2( self ):
    """wasteCPUCycles fails 2......................................................................."""
    with patch("%s.log" % MODULE_NAME, new=Mock(side_effect=RuntimeError("MockedError"))):
      self.assertFalse( wasteCPUCycles(1)['OK'] )
      self.assertIn( "OtherException", wasteCPUCycles(1)['Message'] )
      self.assertIn( "RuntimeError('MockedError',)", wasteCPUCycles(1)['Message'] )

if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( WasteCPUTest )
  TESTRESULT = unittest.TextTestRunner( verbosity = 2 ).run( SUITE )

