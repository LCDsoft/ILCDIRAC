#!/usr/bin/env python
"""Test the FindSteeringFileDir class"""

import unittest
from mock import patch, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Core.Utilities.FindSteeringFileDir import getSteeringFileDir, getSteeringFileDirName
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertDiracFailsWith, assertDiracSucceedsWith_equals

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Core.Utilities.FindSteeringFileDir'

class TestFindSteeringFileDir( unittest.TestCase ):
  """ Test the different methods of the class
  """

  def setUp( self ):
    pass

  def test_getsteerfiledirname( self ):
    getval_mock = Mock()
    getval_mock.getValue.return_value = 'my_test_getval'
    ops_mock = Mock()
    ops_mock.return_value = getval_mock
    cvmfs_mock = Mock(return_value=S_ERROR('some_err'))
    soft_mock = Mock(return_value=S_OK('my_test_dir_retval'))
    check_mock = Mock(return_value=S_OK('ignoreme'))
    with patch("%s.Operations" % MODULE_NAME, new=ops_mock), \
         patch('%s.checkCVMFS' % MODULE_NAME, new=cvmfs_mock), \
         patch('%s.getSoftwareFolder' % MODULE_NAME, new=soft_mock), \
         patch('%s.check' % MODULE_NAME, new=check_mock):
      assertDiracSucceedsWith_equals( getSteeringFileDirName( 'mytest_plat', 'myappTestme', 'vTest1.0' ),
                                      'my_test_dir_retval', self )
      getval_mock.getValue.assert_called_once_with(
        '/AvailableTarBalls/mytest_plat/myappTestme/vTest1.0/Dependencies/steeringfiles/version', '' )

  def test_getsteerfiledirname_fails( self ):
    getval_mock = Mock()
    getval_mock.getValue.return_value = ''
    ops_mock = Mock()
    ops_mock.return_value = getval_mock
    with patch("%s.Operations" % MODULE_NAME, new=ops_mock):
      assertDiracFailsWith( getSteeringFileDirName( 'mytest_plat', 'myappTestme', 'vTest1.0' ),
                            'could not find attached steeringfile version', self )

  def test_getsteerfiledir_cvmfs_success( self ):
    cvmfs_mock = Mock(return_value=S_OK([ 'mytestListEntry#1', 'other_entry_dontusethis', '', '18319' ]))
    with patch('%s.checkCVMFS' % MODULE_NAME, new=cvmfs_mock):
      assertDiracSucceedsWith_equals( getSteeringFileDir( 'myTestPlatform_1', 'v123Test' ),
                                      'mytestListEntry#1', self )
      cvmfs_mock.assert_called_once_with( 'myTestPlatform_1', [ 'steeringfiles', 'v123Test' ] )

  def test_getsteerfiledir_getsoftware_fails( self ):
    cvmfs_mock = Mock(return_value=S_ERROR('some_err'))
    soft_mock = Mock(return_value=S_ERROR( 'software_fails_test' ))
    with patch('%s.checkCVMFS' % MODULE_NAME, new=cvmfs_mock), \
         patch('%s.getSoftwareFolder' % MODULE_NAME, new=soft_mock):
      assertDiracFailsWith( getSteeringFileDir( 'myTestPlatform_1', 'v123Test' ),
                            'software_fails_test', self )
      cvmfs_mock.assert_called_once_with( 'myTestPlatform_1', [ 'steeringfiles', 'v123Test' ] )
      soft_mock.assert_called_once_with( 'myTestPlatform_1', 'steeringfiles', 'v123Test' )

  def test_getsteerfiledir_check_fails( self ):
    cvmfs_mock = Mock(return_value=S_ERROR('some_err'))
    soft_mock = Mock(return_value=S_OK('softDir_test'))
    check_mock = Mock(return_value=S_ERROR('check_fails_testme'))
    with patch('%s.checkCVMFS' % MODULE_NAME, new=cvmfs_mock), \
         patch('%s.getSoftwareFolder' % MODULE_NAME, new=soft_mock), \
         patch('%s.check' % MODULE_NAME, new=check_mock):
      assertDiracFailsWith( getSteeringFileDir( 'myTestPlatform_1', 'v123Test' ),
                            'check_fails_testme', self )
      cvmfs_mock.assert_called_once_with( 'myTestPlatform_1', [ 'steeringfiles', 'v123Test' ] )
      soft_mock.assert_called_once_with( 'myTestPlatform_1', 'steeringfiles', 'v123Test' )
      check_mock.assert_called_once_with( 'steeringfiles.v123Test', '.', [ 'softDir_test' ] )
