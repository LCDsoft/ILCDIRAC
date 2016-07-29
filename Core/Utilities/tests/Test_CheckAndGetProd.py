"""Test the Core CheckAndGetProdProxy"""

import unittest
from mock import MagicMock as Mock, patch
from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Core.Utilities.CheckAndGetProdProxy import checkAndGetProdProxy

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Core.Utilities.CheckAndGetProdProxy'

class CheckProxyTest( unittest.TestCase ):
  """Test the CheckProxy"""

  def setUp ( self ):
    pass

  def tearDown ( self ):
    pass

  @patch("%s.call" % MODULE_NAME, new=Mock(return_value=0))
  @patch("%s.getProxyInfo" % MODULE_NAME, new=Mock(return_value=S_OK({"group":"ilc_prod"})))
  def test_success( self ):
    """test for CheckandGetProdProxy: success......................................................."""
    res = checkAndGetProdProxy()
    self.assertTrue( res['OK'] )


  @patch("%s.call" % MODULE_NAME, new=Mock(return_value=0))
  @patch("%s.getProxyInfo" % MODULE_NAME,
         new=Mock(side_effect=[S_OK({"group":"ilc_user"}),S_OK({"group":"ilc_prod"})]))
  def test_success_2( self ):
    """test for CheckandGetProdProxy: sucess 2......................................................"""
    res = checkAndGetProdProxy()
    self.assertTrue( res['OK'] )

    
  @patch("%s.call" % MODULE_NAME, new=Mock(return_value=1))
  @patch("%s.getProxyInfo" % MODULE_NAME, new=Mock(return_value=S_ERROR("No proxy info")))
  def test_failure( self ):
    """test for CheckandGetProdProxy: failure......................................................."""
    res = checkAndGetProdProxy()
    self.assertFalse( res['OK'] )


  @patch("%s.call" % MODULE_NAME, new=Mock(return_value=0))
  @patch("%s.getProxyInfo" % MODULE_NAME, new=Mock(return_value=S_ERROR("message")))
  def test_failure_2( self ):
    """test for CheckandGetProdProxy: semi failure 1................................................"""
    res = checkAndGetProdProxy()
    self.assertFalse( res['OK'] )

  @patch("%s.call" % MODULE_NAME, new=Mock(return_value=0))
  @patch("%s.getProxyInfo" % MODULE_NAME,
         new=Mock(side_effect=[S_OK({}),S_OK({"group":"ilc_prod"})]))
  def test_failure_3( self ):
    """test for CheckandGetProdProxy: semi failure 2................................................"""
    res = checkAndGetProdProxy()
    self.assertTrue( res['OK'] )


  @patch("%s.call" % MODULE_NAME, new=Mock(return_value=0))
  @patch("%s.getProxyInfo" % MODULE_NAME,
         new=Mock(side_effect=[S_ERROR("no proxy info"),S_OK({"group":"ilc_prod"})]))
  def test_failure_4( self ):
    """test for CheckandGetProdProxy: semi failure 3................................................"""
    res = checkAndGetProdProxy()
    self.assertTrue( res['OK'] )

  @patch("%s.call" % MODULE_NAME, new=Mock(return_value=0))
  @patch("%s.getProxyInfo" % MODULE_NAME,
         new=Mock(side_effect=[S_ERROR("no proxy info"),S_OK({"notgroup":"ilc_user"})]))
  def test_failure_5( self ):
    """test for CheckandGetProdProxy: semi failure 4................................................"""
    res = checkAndGetProdProxy()
    self.assertTrue( not res['OK'] )

  @patch("%s.call" % MODULE_NAME, new=Mock(return_value=0))
  @patch("%s.getProxyInfo" % MODULE_NAME,
         new=Mock(side_effect=[S_ERROR("no proxy info"),S_OK({"group":"ilc_user"})]))
  def test_failure_6( self ):
    """test for CheckandGetProdProxy: semi failure 5................................................"""
    res = checkAndGetProdProxy()
    self.assertTrue( not res['OK'] )

    
  @patch("%s.call" % MODULE_NAME, new=Mock(return_value=0))
  @patch("%s.getProxyInfo" % MODULE_NAME,
         new=Mock(side_effect=[S_ERROR("no proxy info"),S_ERROR("Still no proxy")]))
  def test_failure_7( self ):
    """test for CheckandGetProdProxy: semi failure 6................................................"""
    res = checkAndGetProdProxy()
    self.assertTrue( not res['OK'] )

if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( CheckProxyTest )
  TESTRESULT = unittest.TextTestRunner( verbosity = 2 ).run( SUITE )

