"""Test the LFNPathUtilities"""

import unittest

from ILCDIRAC.Core.Utilities.LFNPathUtilities import joinPathForMetaData, cleanUpLFNPath

class LFNPathUtilitiesTests( unittest.TestCase ):
  """Test the LFNPathUtilities"""

  def setUp ( self ):
    self.logPath = "/ilc/prod/ilc/mc-dbd/ild/"
    self.jobID = 12
    pass

  def tearDown ( self ):
    pass


class TestJoinPathForMetaData( LFNPathUtilitiesTests ):
  """Test joinPathForMetaData"""
  def test_success( self ):
    """test for joinPathForMetaData"""
    self.assertEqual ( joinPathForMetaData ( "/ilc" , "grid" , "softwareVersion", "/" ) , "/ilc/grid/softwareVersion/" )
    self.assertEqual ( joinPathForMetaData ( "/ilc//grid","/" , "softwareVersion", "/" ) , "/ilc/grid/softwareVersion/" )
    self.assertEqual ( joinPathForMetaData ( "/ilc//grid","/" , "softwareVersion/", "/" ) , "/ilc/grid/softwareVersion/" )
    return True

class TestCleanLFNPath( LFNPathUtilitiesTests ):
  """Test cleanUpLFNPath"""
  def test_succes( self ):
    """Test cleanUpLFNPath"""
    return self.assertEqual ( cleanUpLFNPath ('%s/%s' % (self.logPath, str(int(self.jobID)/1000).zfill(3))),
                              "/ilc/prod/ilc/mc-dbd/ild/000")

if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( LFNPathUtilitiesTests )
  SUITE.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestCleanLFNPath ) )
  SUITE.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestJoinPathForMetaData ) )
  TESTRESULT = unittest.TextTestRunner( verbosity = 2 ).run( SUITE )

