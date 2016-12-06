"""
Tests for PrepareLibs

"""
import unittest
import sys
from mock import patch, MagicMock as Mock

from ILCDIRAC.Core.Utilities.PrepareLibs import removeLibc, main
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertMockCalls

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Core.Utilities.PrepareLibs'

class TestPrepareLibs( unittest.TestCase ):
  """ Tests the removeLibc method
  """
  def setUp( self ):
    sys_mock = Mock()
    sys_mock.argv = [ 'something', 'myothertestpath' ]
    mocked_modules = { 'sys' : sys_mock }
    self.module_patcher = patch.dict( sys.modules, mocked_modules )
    self.module_patcher.start()

  def tearDown( self ):
    self.module_patcher.stop()

  def test_remove_libc( self ):
    with patch( '%s.os.getcwd' % MODULE_NAME, new=Mock(side_effect=[ 'current_dir', 'mytestpath', 'mytestpath', 'mytestpath' ]) ) as getcwd_mock, \
         patch( '%s.os.chdir' % MODULE_NAME, new=Mock(return_value=True) ) as chdir_mock, \
         patch( '%s.os.remove' % MODULE_NAME, new=Mock(return_value=True) ) as remove_mock, \
         patch( '%s.os.listdir' % MODULE_NAME, new=Mock(return_value=[ 'directory_content1.txt', 'libc.so', 'libstdc++.so' ]) ) as listdir_mock:
      result = removeLibc( 'mytestpath' )
      self.assertTrue( result )
      assertMockCalls( chdir_mock, [ 'mytestpath', 'current_dir' ], self )
      listdir_mock.assert_called_once_with( 'mytestpath' )
      assertMockCalls( remove_mock, [ 'mytestpath/libc.so', 'mytestpath/libstdc++.so' ], self )
      assertMockCalls( getcwd_mock, [ () ] * 4, self )

  def test_remove_libc_chdir_fails( self ):
    with patch( '%s.os.getcwd' % MODULE_NAME, new=Mock(side_effect=[ 'current_dir', 'mytestpath', 'mytestpath', 'mytestpath' ]) ) as getcwd_mock, \
         patch( '%s.os.chdir' % MODULE_NAME, new=Mock(side_effect=OSError( 'chdir_test_os_err' )) ) as chdir_mock, \
         patch( '%s.os.remove' % MODULE_NAME, new=Mock(return_value=True) ) as remove_mock, \
         patch( '%s.os.listdir' % MODULE_NAME, new=Mock(return_value=[ 'directory_content1.txt', 'libc.so', 'libstdc++.so' ]) ) as listdir_mock:
      result = removeLibc( 'mytestpath' )
      self.assertTrue( result )
      chdir_mock.assert_called_once_with( 'mytestpath' )
      self.assertFalse( listdir_mock.called )
      self.assertFalse( remove_mock.called )
      getcwd_mock.assert_called_once_with()

  def test_remove_libc_remove_fails( self ):
    with patch( '%s.os.getcwd' % MODULE_NAME, new=Mock(side_effect=[ 'current_dir', 'mytestpath', 'mytestpath', 'mytestpath' ]) ) as getcwd_mock, \
         patch( '%s.os.chdir' % MODULE_NAME, new=Mock(return_value=True) ) as chdir_mock, \
         patch( '%s.os.remove' % MODULE_NAME, new=Mock(side_effect=OSError( 'test_cannot_remove_os_err' )) ) as remove_mock, \
         patch( '%s.os.listdir' % MODULE_NAME, new=Mock(return_value=[ 'directory_content1.txt', 'libc.so', 'libstdc++.so' ]) ) as listdir_mock:
      result = removeLibc( 'mytestpath' )
      self.assertFalse( result )
      assertMockCalls( chdir_mock, [ 'current_dir', 'mytestpath' ], self )
      listdir_mock.assert_called_once_with( 'mytestpath' )
      remove_mock.assert_called_once_with( 'mytestpath/libc.so' )
      assertMockCalls( getcwd_mock, [ () ] * 3, self )

  def test_remove_libc_nothing_to_remove( self ):
    with patch( '%s.os.getcwd' % MODULE_NAME, new=Mock(side_effect=[ 'current_dir', 'mytestpath', 'mytestpath', 'mytestpath' ]) ) as getcwd_mock, \
         patch( '%s.os.chdir' % MODULE_NAME, new=Mock(return_value=True) ) as chdir_mock, \
         patch( '%s.os.remove' % MODULE_NAME, new=Mock(return_value=True) ) as remove_mock, \
         patch( '%s.os.listdir' % MODULE_NAME, new=Mock(return_value=[ 'directory_content1.txt', 'not_a_library.txt', 'dont_delete_me_libsomething.so', 'innocentlibc_.so' ]) ) as listdir_mock:
      result = removeLibc( 'mytestpath' )
      self.assertTrue( result )
      assertMockCalls( chdir_mock, [ 'mytestpath', 'current_dir' ], self )
      listdir_mock.assert_called_once_with( 'mytestpath' )
      self.assertFalse( remove_mock.called )
      assertMockCalls( getcwd_mock, [ () ] * 2, self )

  def test_main( self ):
    with patch( '%s.os.getcwd' % MODULE_NAME, new=Mock(side_effect=[ 'current_dir', 'myothertestpath', 'myothertestpath', 'myothertestpath' ]) ), \
         patch( '%s.os.chdir' % MODULE_NAME, new=Mock(return_value=True) ), \
         patch( '%s.os.remove' % MODULE_NAME, new=Mock(return_value=True) ), \
         patch( '%s.os.listdir' % MODULE_NAME, new=Mock(return_value=[ 'directory_content1.txt', 'libc.so', 'libstdc++.so' ]) ):
      assertEqualsImproved( main(), 0, self )

  def test_main_no_args( self ):
    self.module_patcher.stop()
    sys_mock = Mock()
    sys_mock.argv = [ 'something' ]
    mocked_modules = { 'sys' : sys_mock }
    self.module_patcher = patch.dict( sys.modules, mocked_modules )
    self.module_patcher.start()
    with patch( '%s.os.getcwd' % MODULE_NAME, new=Mock(side_effect=[ 'current_dir', 'myothertestpath', 'myothertestpath', 'myothertestpath' ]) ), \
         patch( '%s.os.chdir' % MODULE_NAME, new=Mock(return_value=True) ), \
         patch( '%s.os.remove' % MODULE_NAME, new=Mock(return_value=True) ), \
         patch( '%s.os.listdir' % MODULE_NAME, new=Mock(return_value=[ 'directory_content1.txt', 'libc.so', 'libstdc++.so' ]) ):
      assertEqualsImproved( main(), 1, self )

  def test_main_remove_fails( self ):
    with patch( '%s.os.getcwd' % MODULE_NAME, new=Mock(side_effect=[ 'current_dir', 'myothertestpath', 'myothertestpath', 'myothertestpath' ]) ), \
         patch( '%s.os.chdir' % MODULE_NAME, new=Mock(return_value=True) ), \
         patch( '%s.os.remove' % MODULE_NAME, new=Mock(side_effect=OSError( 'test_cannot_remove_os_err' )) ), \
         patch( '%s.os.listdir' % MODULE_NAME, new=Mock(return_value=[ 'directory_content1.txt', 'libc.so', 'libstdc++.so' ]) ):
      assertEqualsImproved( main(), 1, self )
