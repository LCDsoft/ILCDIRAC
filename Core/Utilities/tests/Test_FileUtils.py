#!/usr/bin/env python
"""Test the FileUtils class"""

import sys
import unittest
from distutils import errors
from mock import patch, MagicMock as Mock

from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertDiracSucceeds, assertDiracFails, assertMockCalls, assertDiracSucceedsWith
from DIRAC import S_OK, S_ERROR

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Core.Utilities.FileUtils'

#pylint: disable=too-many-public-methods
class TestFileUtils( unittest.TestCase ):
  """ Test the different methods of the class
  """

  def setUp( self ):
    self.dm_mock = Mock()
    self.ops_mock = Mock()
    mocked_modules = { 'DIRAC.DataManagementSystem.Client.DataManager' : self.dm_mock,
                       'DIRAC.ConfigurationSystem.Client.Helpers.Operations' : self.ops_mock }
    self.module_patcher = patch.dict( sys.modules, mocked_modules )
    self.module_patcher.start()

  def tearDown( self ):
    self.module_patcher.stop()

  def test_upload_tarball_missing( self ):
    from ILCDIRAC.Core.Utilities.FileUtils import upload
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=False)) as exists_mock:
      assertDiracFails( upload( 'mypath', 'appTarTest' ), self )
      exists_mock.assert_called_once_with( 'appTarTest' )

  def test_upload_copy_fails( self ):
    from ILCDIRAC.Core.Utilities.FileUtils import upload
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)) as exists_mock, \
         patch('%s.shutil.copy' % MODULE_NAME, new=Mock(side_effect=EnvironmentError('copy_testfail'))) as copy_mock:
      assertDiracFailsWith( upload( 'http://www.cern.ch/lcd-data/mypath', 'appTarTest' ),
                            'could not copy because copy_testfail', self )
      exists_mock.assert_called_once_with( 'appTarTest' )
      copy_mock.assert_called_once_with( 'appTarTest', '/afs/cern.ch/eng/clic/data/software/appTarTest' )

  def test_upload_copy_works( self ):
    from ILCDIRAC.Core.Utilities.FileUtils import upload
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)) as exists_mock, \
         patch('%s.shutil.copy' % MODULE_NAME, new=Mock()) as copy_mock:
      assertDiracSucceeds( upload( 'http://www.cern.ch/lcd-data/mypath', 'appTarTest' ), self )
      exists_mock.assert_called_once_with( 'appTarTest' )
      copy_mock.assert_called_once_with( 'appTarTest', '/afs/cern.ch/eng/clic/data/software/appTarTest' )

  def test_upload_invalid_location( self ):
    from ILCDIRAC.Core.Utilities.FileUtils import upload
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)):
      assertDiracFails( upload( 'http://www.mypath.com', 'appTarTest' ), self )

  def test_upload_datman_upload_fails( self ):
    self.dm_mock.DataManager().putAndRegister.return_value = S_ERROR( 'dataman_upload_testerr' )
    self.ops_mock.Operations().getValue.return_value = '13984'
    from ILCDIRAC.Core.Utilities.FileUtils import upload
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)):
      assertDiracFailsWith( upload( '/some/local/path', 'something/appTarTest.tgz' ),
                            'dataman_upload_testerr', self )
      self.dm_mock.DataManager().putAndRegister.assert_called_once_with( '/some/local/path/appTarTest.tgz',
                                                                         'something/appTarTest.tgz', '13984' )
      self.ops_mock.Operations().getValue.assert_called_once_with( 'Software/BaseStorageElement', 'CERN-SRM' )

  def test_upload_requestvalidation_fails( self ):
    self.module_patcher.stop()
    req_mock = Mock()
    reqval_mock = Mock()
    reqval_mock.RequestValidator().validate.return_value = S_ERROR( 'validation_failed_testme' )
    reqclient_mock = Mock()
    op_mock = Mock()
    op_list = [ Mock(), Mock(), Mock(), Mock() ]
    file_list = [ Mock(), Mock(), Mock(), Mock() ]
    copies_at_list = [ 'MyCopySE1', 'OtherSE', 'LastSE', '' ]
    op_mock.Operation.side_effect = op_list
    op_mock.File.side_effect = file_list
    mocked_modules = { 'DIRAC.DataManagementSystem.Client.DataManager' : self.dm_mock,
                       'DIRAC.ConfigurationSystem.Client.Helpers.Operations' : self.ops_mock,
                       'DIRAC.RequestManagementSystem.Client.Request' : req_mock,
                       'DIRAC.RequestManagementSystem.private.RequestValidator' : reqval_mock,
                       'DIRAC.RequestManagementSystem.Client.ReqClient' : reqclient_mock,
                       'DIRAC.RequestManagementSystem.Client.Operation' : op_mock }
    self.module_patcher = patch.dict( sys.modules, mocked_modules )
    self.module_patcher.start()
    self.dm_mock.DataManager().putAndRegister.return_value = S_OK()
    self.ops_mock.Operations().getValue.side_effect = [ None, [ 'MyCopySE1', 'OtherSE', 'LastSE', '' ] ]
    from ILCDIRAC.Core.Utilities.FileUtils import upload
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)):
      assertDiracFailsWith( upload( '/some/local/path/', 'appTarTest.tgz' ),
                            'validation_failed_testme', self )
      assertMockCalls( self.ops_mock.Operations().getValue,
                       [ ( 'Software/BaseStorageElement', 'CERN-SRM' ), ( 'Software/CopiesAt', [] ) ], self )
      request_to_test = req_mock.Request()
      assertEqualsImproved( request_to_test.RequestName, 'copy_appTarTest', self )
      assertEqualsImproved( request_to_test.SourceComponent, 'ReplicateILCSoft', self )
      assertMockCalls( request_to_test.addOperation, op_list, self )
      assertEqualsImproved( len( op_list ), len( copies_at_list ), self )
      index = 0
      for operation, se in zip(op_list, copies_at_list):
        assertEqualsImproved( operation.Type, 'ReplicateAndRegister', self )
        assertEqualsImproved( operation.TargetSE, se, self )
        operation.addFile.assert_called_once_with( file_list[ index ] )
        assertEqualsImproved( ( file_list[ index ].LFN, file_list[ index ].GUID ),
                              ( '/some/local/path/appTarTest.tgz', '' ), self )
        index += 1

  def test_upload_putrequest_fails( self ):
    self.module_patcher.stop()
    req_mock = Mock()
    reqval_mock = Mock()
    reqval_mock.RequestValidator().validate.return_value = S_OK()
    reqclient_mock = Mock()
    reqclient_mock.ReqClient().putRequest.return_value = S_ERROR( 'ignore_test_err' )
    op_mock = Mock()
    op_list = [ Mock(), Mock(), Mock(), Mock() ]
    file_list = [ Mock(), Mock(), Mock(), Mock() ]
    copies_at_list = [ 'MyCopySE1', 'OtherSE', 'LastSE', '' ]
    op_mock.Operation.side_effect = op_list
    op_mock.File.side_effect = file_list
    mocked_modules = { 'DIRAC.DataManagementSystem.Client.DataManager' : self.dm_mock,
                       'DIRAC.ConfigurationSystem.Client.Helpers.Operations' : self.ops_mock,
                       'DIRAC.RequestManagementSystem.Client.Request' : req_mock,
                       'DIRAC.RequestManagementSystem.private.RequestValidator' : reqval_mock,
                       'DIRAC.RequestManagementSystem.Client.ReqClient' : reqclient_mock,
                       'DIRAC.RequestManagementSystem.Client.Operation' : op_mock }
    self.module_patcher = patch.dict( sys.modules, mocked_modules )
    self.module_patcher.start()
    self.dm_mock.DataManager().putAndRegister.return_value = S_OK()
    self.ops_mock.Operations().getValue.side_effect = [ None, [ 'MyCopySE1', 'OtherSE', 'LastSE', '' ] ]
    from ILCDIRAC.Core.Utilities.FileUtils import upload
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)):
      assertDiracSucceedsWith( upload( '/some/local/path/', 'appTarTest.tgz' ), 'Application uploaded', self )
      assertMockCalls( self.ops_mock.Operations().getValue,
                       [ ( 'Software/BaseStorageElement', 'CERN-SRM' ), ( 'Software/CopiesAt', [] ) ], self )
      request_to_test = req_mock.Request()
      assertEqualsImproved( request_to_test.RequestName, 'copy_appTarTest', self )
      assertEqualsImproved( request_to_test.SourceComponent, 'ReplicateILCSoft', self )
      assertMockCalls( request_to_test.addOperation, op_list, self )
      assertEqualsImproved( len( op_list ), len( copies_at_list ), self )
      index = 0
      for operation, se in zip(op_list, copies_at_list):
        assertEqualsImproved( operation.Type, 'ReplicateAndRegister', self )
        assertEqualsImproved( operation.TargetSE, se, self )
        operation.addFile.assert_called_once_with( file_list[ index ] )
        assertEqualsImproved( ( file_list[ index ].LFN, file_list[ index ].GUID ),
                              ( '/some/local/path/appTarTest.tgz', '' ), self )
        index += 1

  def test_upload_putrequest_works( self ):
    self.module_patcher.stop()
    req_mock = Mock()
    reqval_mock = Mock()
    reqval_mock.RequestValidator().validate.return_value = S_OK()
    reqclient_mock = Mock()
    reqclient_mock.ReqClient().putRequest.return_value = S_OK()
    op_mock = Mock()
    op_list = [ Mock(), Mock(), Mock(), Mock() ]
    file_list = [ Mock(), Mock(), Mock(), Mock() ]
    copies_at_list = [ 'MyCopySE1', 'OtherSE', 'LastSE', '' ]
    op_mock.Operation.side_effect = op_list
    op_mock.File.side_effect = file_list
    mocked_modules = { 'DIRAC.DataManagementSystem.Client.DataManager' : self.dm_mock,
                       'DIRAC.ConfigurationSystem.Client.Helpers.Operations' : self.ops_mock,
                       'DIRAC.RequestManagementSystem.Client.Request' : req_mock,
                       'DIRAC.RequestManagementSystem.private.RequestValidator' : reqval_mock,
                       'DIRAC.RequestManagementSystem.Client.ReqClient' : reqclient_mock,
                       'DIRAC.RequestManagementSystem.Client.Operation' : op_mock }
    self.module_patcher = patch.dict( sys.modules, mocked_modules )
    self.module_patcher.start()
    self.dm_mock.DataManager().putAndRegister.return_value = S_OK()
    self.ops_mock.Operations().getValue.side_effect = [ None, [ 'MyCopySE1', 'OtherSE', 'LastSE', '' ] ]
    from ILCDIRAC.Core.Utilities.FileUtils import upload
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)):
      assertDiracSucceedsWith( upload( '/some/local/path/', 'appTarTest.tgz' ), 'Application uploaded', self )
      assertMockCalls( self.ops_mock.Operations().getValue,
                       [ ( 'Software/BaseStorageElement', 'CERN-SRM' ), ( 'Software/CopiesAt', [] ) ], self )
      request_to_test = req_mock.Request()
      assertEqualsImproved( request_to_test.RequestName, 'copy_appTarTest', self )
      assertEqualsImproved( request_to_test.SourceComponent, 'ReplicateILCSoft', self )
      assertMockCalls( request_to_test.addOperation, op_list, self )
      assertEqualsImproved( len( op_list ), len( copies_at_list ), self )
      index = 0
      for operation, se in zip(op_list, copies_at_list):
        assertEqualsImproved( operation.Type, 'ReplicateAndRegister', self )
        assertEqualsImproved( operation.TargetSE, se, self )
        operation.addFile.assert_called_once_with( file_list[ index ] )
        assertEqualsImproved( ( file_list[ index ].LFN, file_list[ index ].GUID ),
                              ( '/some/local/path/appTarTest.tgz', '' ), self )
        index += 1

  def test_fullcopy_getallfiles( self ):
    from ILCDIRAC.Core.Utilities.FileUtils import fullCopy
    with patch('%s.gLogger' % MODULE_NAME, new=Mock()) as log_mock:
      assertDiracSucceeds( fullCopy( '/my/src/directory/', '/my/destination/dir', '   ./__!@#.~/  ' ), self )
      log_mock.error.assert_called_once_with( 'You try to get all files, that cannot happen' )

  def test_fullcopy_noitemsfound( self ):
    from ILCDIRAC.Core.Utilities.FileUtils import fullCopy
    with patch('%s.glob.glob' % MODULE_NAME, new=Mock(return_value=None)) as glob_mock:
      assertDiracFailsWith( fullCopy( '/my/src/directory/', '/my/destination/dir', './myfile123.txt  ' ),
                            'no items found', self )
      glob_mock.assert_called_once_with( '/my/src/directory/myfile123.txt' )

  def test_fullcopy_distutils_fails( self ):
    from ILCDIRAC.Core.Utilities.FileUtils import fullCopy
    with patch('%s.glob.glob' % MODULE_NAME, new=Mock(return_value=['/my/src/directory/file_1_globbed.log','/my/src/directory/other__file.stdhep', 'lastfile_in_list.txt'])), \
         patch('%s.dir_util.create_tree' % MODULE_NAME, new=Mock(side_effect=errors.DistutilsFileError('test_dist_error'))):
      assertDiracFailsWith( fullCopy( '/my/src/directory/', '/my/destination/dir', './myfile123.txt  ' ),
                            'test_dist_error', self )

  def test_fullcopy_copy1_fails( self ):
    from ILCDIRAC.Core.Utilities.FileUtils import fullCopy
    with patch('%s.glob.glob' % MODULE_NAME, new=Mock(return_value=['/my/src/directory/file_1_globbed.log','/my/src/directory/other__file.stdhep', 'lastfile_in_list.txt'])), \
         patch('%s.dir_util.create_tree' % MODULE_NAME, new=Mock()) as createtree_mock, \
         patch('%s.os.path.isfile' % MODULE_NAME, new=Mock(return_value=True)) as isfile_mock, \
         patch('%s.shutil.copy2' % MODULE_NAME, new=Mock(side_effect=(None,None,EnvironmentError('testme_copy_enverr')))) as copy_mock:
      assertDiracFailsWith( fullCopy( '/my/src/directory/', '/my/destination/dir', './myfile123.txt  ' ),
                            'testme_copy_enverr', self )
      assertMockCalls( createtree_mock, [ ( '/my/destination/dir', [ 'file_1_globbed.log' ] ),
                                          ( '/my/destination/dir', [ 'other__file.stdhep' ] ),
                                          ( '/my/destination/dir', [ 'lastfile_in_list.txt' ] ) ], self )
      assertMockCalls( isfile_mock, [ '/my/src/directory/file_1_globbed.log',
                                      '/my/src/directory/other__file.stdhep',
                                      '/my/src/directory/lastfile_in_list.txt' ], self )
      assertMockCalls( copy_mock, [
        ( '/my/src/directory/file_1_globbed.log', '/my/destination/dir/file_1_globbed.log' ),
        ( '/my/src/directory/other__file.stdhep', '/my/destination/dir/other__file.stdhep' ),
        ( '/my/src/directory/lastfile_in_list.txt', '/my/destination/dir/lastfile_in_list.txt' ) ], self )

  def test_fullcopy_copy2_fails( self ):
    from ILCDIRAC.Core.Utilities.FileUtils import fullCopy
    with patch('%s.glob.glob' % MODULE_NAME, new=Mock(return_value=['/my/src/directory/file_1_globbed.log','/my/src/directory/other__file.stdhep', 'lastfile_in_list.txt'])), \
         patch('%s.dir_util.create_tree' % MODULE_NAME, new=Mock()) as createtree_mock, \
         patch('%s.os.path.isfile' % MODULE_NAME, new=Mock(side_effect=[True,False])) as isfile_mock, \
         patch('%s.shutil.copy2' % MODULE_NAME, new=Mock()) as copy_mock, \
         patch('%s.shutil.copytree' % MODULE_NAME, new=Mock(side_effect=EnvironmentError('tree_test_copy_error'))) as copytree_mock:
      assertDiracFailsWith( fullCopy( '/my/src/directory/', '/my/destination/dir', './myfile123.txt  ' ),
                            'tree_test_copy_error', self )
      assertMockCalls( createtree_mock, [ ( '/my/destination/dir', [ 'file_1_globbed.log' ] ),
                                          ( '/my/destination/dir', [ 'other__file.stdhep' ] ) ], self )
      assertMockCalls( isfile_mock, [ '/my/src/directory/file_1_globbed.log',
                                      '/my/src/directory/other__file.stdhep' ], self )
      copy_mock.assert_called_once_with( '/my/src/directory/file_1_globbed.log', '/my/destination/dir/file_1_globbed.log' )
      copytree_mock.assert_called_once_with( '/my/src/directory/other__file.stdhep', '/my/destination/dir/other__file.stdhep' )


  def test_fullcopy( self ):
    from ILCDIRAC.Core.Utilities.FileUtils import fullCopy
    with patch('%s.glob.glob' % MODULE_NAME, new=Mock(return_value=['/my/src/directory/file_1_globbed.log','/my/src/directory/other__file.stdhep', 'lastfile_in_list.txt'])), \
         patch('%s.dir_util.create_tree' % MODULE_NAME, new=Mock()), \
         patch('%s.os.path.isfile' % MODULE_NAME, new=Mock(side_effect=[True,False,True])), \
         patch('%s.shutil.copy2' % MODULE_NAME, new=Mock()), \
         patch('%s.shutil.copytree' % MODULE_NAME, new=Mock()):
      assertDiracSucceeds( fullCopy( '/my/src/directory/', '/my/destination/dir', './myfile123.txt  ' ), self )
