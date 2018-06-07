"""Test the Core CheckAndGetProdProxy"""

import unittest
from mock import MagicMock as Mock, patch
from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Core.Utilities.CheckAndGetProdProxy import checkAndGetProdProxy, checkOrGetGroupProxy

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Core.Utilities.CheckAndGetProdProxy'

class CheckProxyTest( unittest.TestCase ):
  """Test the CheckProxy"""

  def setUp ( self ):
    pass

  def tearDown ( self ):
    pass

  def test_success( self ):
    """test for CheckandGetProdProxy: success......................................................."""
    with patch("%s.call" % MODULE_NAME, new=Mock(return_value=0)), \
         patch("%s.getProxyInfo" % MODULE_NAME, new=Mock(return_value=S_OK({"group":"ilc_prod"}))):
      res = checkAndGetProdProxy()
      self.assertTrue( res['OK'] )

  def test_success_2( self ):
    """test for CheckandGetProdProxy: sucess 2......................................................"""
    with patch("%s.call" % MODULE_NAME, new=Mock(return_value=0)), \
         patch("%s.getProxyInfo" % MODULE_NAME, new=Mock(side_effect=[S_OK({"group":"ilc_user"}),S_OK({"group":"ilc_prod"})])):
      res = checkAndGetProdProxy()
      self.assertTrue( res['OK'] )

  def test_failure( self ):
    """test for CheckandGetProdProxy: failure......................................................."""
    with patch("%s.call" % MODULE_NAME, new=Mock(return_value=1)), \
         patch("%s.getProxyInfo" % MODULE_NAME, new=Mock(return_value=S_ERROR('No proxy info'))):
      res = checkAndGetProdProxy()
      self.assertFalse( res['OK'] )

  def test_failure_2( self ):
    """test for CheckandGetProdProxy: semi failure 1................................................"""
    with patch("%s.call" % MODULE_NAME, new=Mock(return_value=0)), \
         patch("%s.getProxyInfo" % MODULE_NAME, new=Mock(return_value=S_ERROR('message'))):
      res = checkAndGetProdProxy()
      self.assertFalse( res['OK'] )

  def test_failure_3( self ):
    """test for CheckandGetProdProxy: semi failure 2................................................"""
    with patch("%s.call" % MODULE_NAME, new=Mock(return_value=0)), \
         patch("%s.getProxyInfo" % MODULE_NAME, new=Mock(side_effect=[S_OK({}), S_OK({"group":"ilc_prod"})])):
      res = checkAndGetProdProxy()
      self.assertTrue( res['OK'] )

  def test_failure_4( self ):
    """test for CheckandGetProdProxy: semi failure 3................................................"""
    with patch("%s.call" % MODULE_NAME, new=Mock(return_value=0)), \
         patch("%s.getProxyInfo" % MODULE_NAME, new=Mock(side_effect=[S_ERROR("no proxy info"),S_OK({"group":"ilc_prod"})])):
      res = checkAndGetProdProxy()
      self.assertTrue( res['OK'] )

  def test_failure_5( self ):
    """test for CheckandGetProdProxy: semi failure 4................................................"""
    with patch("%s.call" % MODULE_NAME, new=Mock(return_value=0)), \
         patch("%s.getProxyInfo" % MODULE_NAME, new=Mock(side_effect=[S_ERROR("no proxy info"),S_OK({"notgroup":"ilc_user"})])):
      res = checkAndGetProdProxy()
      self.assertTrue( not res['OK'] )

  def test_failure_6( self ):
    """test for CheckandGetProdProxy: semi failure 5................................................"""
    with patch("%s.call" % MODULE_NAME, new=Mock(return_value=0)), \
         patch("%s.getProxyInfo" % MODULE_NAME, new=Mock(side_effect=[S_ERROR("no proxy info"),S_OK({"group":"ilc_user"})])):
      res = checkAndGetProdProxy()
      self.assertTrue( not res['OK'] )

  def test_failure_7( self ):
    """test for CheckandGetProdProxy: semi failure 6................................................"""
    with patch("%s.call" % MODULE_NAME, new=Mock(return_value=0)), \
         patch("%s.getProxyInfo" % MODULE_NAME, new=Mock(side_effect=[S_ERROR("no proxy info"),S_ERROR("Still no proxy")])):
      res = checkAndGetProdProxy()
      self.assertTrue( not res['OK'] )

  def test_checkOrGet(self):
    """test for checkOrGetGroupProxy: .............................................................."""
    with patch('%s.call' % MODULE_NAME, new=Mock(return_value=0)), \
         patch('%s.getProxyInfo' % MODULE_NAME, new=Mock(return_value=S_OK({'group': 'fcc_prod'}))):
      res = checkOrGetGroupProxy(['ilc_prod', 'fcc_prod'])
      self.assertTrue(res['OK'])
      self.assertEqual(res['Value'], 'fcc_prod')

    with patch('%s.call' % MODULE_NAME, new=Mock(return_value=0)), \
         patch('%s.getProxyInfo' % MODULE_NAME, new=Mock(return_value=S_OK({'group': 'ilc_user'}))):
      res = checkOrGetGroupProxy(['ilc_prod', 'fcc_prod'])
      self.assertFalse(res['OK'])
      self.assertIn('More than one', res['Message'])

    with patch('%s.call' % MODULE_NAME, new=Mock(return_value=0)), \
         patch('%s.getProxyInfo' % MODULE_NAME, new=Mock(return_value=S_OK({'group': 'ilc_user'}))):
      res = checkOrGetGroupProxy('ilc_user')
      self.assertTrue(res['OK'])
      self.assertEqual(res['Value'], 'ilc_user')


if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( CheckProxyTest )
  TESTRESULT = unittest.TextTestRunner( verbosity = 2 ).run( SUITE )
