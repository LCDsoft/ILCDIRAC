"""Test WasteCPU """
__RCSID__ = "$Id$"

import unittest

from ILCDIRAC.Core.Utilities.WasteCPU import wasteCPUCycles

class WasteCPUTest( unittest.TestCase ):
  """Test the WasteCPU"""

  def setUp ( self ):
    pass

  def tearDown ( self ):
    pass

  def test_success( self ):
    """test for wasteCPUCycles......................................................................"""
    self.assertTrue( wasteCPUCycles(1)['OK'] )

if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( WasteCPUTest )
  TESTRESULT = unittest.TextTestRunner( verbosity = 2 ).run( SUITE )

