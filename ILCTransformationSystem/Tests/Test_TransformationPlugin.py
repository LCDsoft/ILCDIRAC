#!/usr/local/env python
"""
 Test TransformationPlugin module

 """
 
import sys
import unittest
from mock import patch, MagicMock as Mock

from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
    assertDiracSucceeds, assertDiracSucceedsWith_equals, assertListContentEquals, assertInImproved

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.ILCTransformationSystem.Agent.TransformationPlugin'

#pylint: disable=protected-access
class TransformationPluginTestCase( unittest.TestCase ):
  """ Base class for the TransformationPlugin test cases
  """
  def setUp(self):
    """set up the objects"""
    mocked_modules = { 'DIRAC.DataManagementSystem.Client.DataManager' : Mock(),
                       'DIRAC.Resources.Catalog.FileCatalog.FileCatalog' : Mock(),
                       'DIRAC.Resources.Catalog.FileCatalog' : Mock() }
    self.module_patcher = patch.dict( sys.modules, mocked_modules )
    self.module_patcher.start()
    self.tfp = None # Stores the TransformationPlugin object

  def tearDown( self ):
    self.module_patcher.stop()

  def test_limited_add_up_to_maximum( self ):
    from ILCDIRAC.ILCTransformationSystem.Agent.TransformationPlugin import TransformationPlugin
    from DIRAC import S_OK
    dataman_mock = Mock()
    util_mock = Mock()
    util_mock.groupByReplicas.return_value = S_OK(
      [ ( 'testSE', [ 'mylfn1', 'testLFN2' ] ), ( 'testSE', [ 'other_test_lfn' ] ),
        ( 'testSE', [ 'mylfn' ] ), ( 'testSE', [ 'MyLFN' ] ) ] )
    trans_mock = Mock()
    trans_mock.getCounters.return_value = S_OK(
      [ ( { 'Status' : 'Processed' }, 3 ), ( { 'Status' : 'junk' }, 6 ), ( { 'Status' : 'Ignore_me' }, 8 ),
        ( { 'Status' : 'Assigned' }, 1 ) ] )
    util_mock.transClient = trans_mock
    self.tfp = TransformationPlugin( 'Limited', dataman_mock, trans_mock )
    self.tfp.params[ 'Status' ] = 'Processed'
    self.tfp.params[ 'MaxNumberOfTasks' ] = 6
    self.tfp.params[ 'TransformationID' ] = 78456
    self.tfp.util = util_mock
    test_data = [
      ( '/my/file/here_abc', [ 'myTestSE' ] ),
      ( '/my/file/other_file.txt', [ 'secretSE', 'StorageSE', 'TestSE' ] ),
      ( '/my/file/newfile.pdf', [ 'TestSE' ] ),
      ( '/my/file/a', [] ), ( '/dir/somefile', [ '' ] ) ]
    self.tfp.setInputData( test_data )
    assertDiracSucceedsWith_equals( self.tfp.run(), [
      ( '', [ 'mylfn1', 'testLFN2' ] ), ( '', [ 'other_test_lfn' ] ) ], self )
    util_mock.groupByReplicas.assert_called_once_with( test_data, 'Processed' )
    trans_mock.getCounters.assert_called_once_with( 'TransformationFiles', [ 'Status' ],
                                                    { 'TransformationID' : 78456 } )

  def test_limited_add_all( self ):
    from ILCDIRAC.ILCTransformationSystem.Agent.TransformationPlugin import TransformationPlugin
    from DIRAC import S_OK
    dataman_mock = Mock()
    new_tasks = [ ( 'otherSE', [ 'unique_lfn', 'testme_lfn' ] ), ( 'testSE', [ 'LFN_to_add' ] ),
                  ( 'testSE', [ 'LFN:/mydir/subdir/file.stdhep' ] ), ( 'testSE', [ 'testSE/storage/LFN.txt' ] ) ]
    util_mock = Mock()
    util_mock.groupByReplicas.return_value = S_OK( new_tasks )
    trans_mock = Mock()
    trans_mock.getCounters.return_value = S_OK(
      [ ( { 'Status' : 'Processed' }, 3 ), ( { 'Status' : 'junk' }, 6 ), ( { 'Status' : 'Ignore_me' }, 8 ),
        ( { 'Status' : 'Assigned' }, 1 ) ] )
    util_mock.transClient = trans_mock
    self.tfp = TransformationPlugin( 'Limited', dataman_mock, trans_mock )
    self.tfp.params[ 'Status' ] = 'Processed'
    self.tfp.params[ 'MaxNumberOfTasks' ] = 12
    self.tfp.params[ 'TransformationID' ] = 78456
    self.tfp.util = util_mock
    self.tfp.setInputData( [
      ( '/my/file/here_abc', [ 'myTestSE' ] ),
      ( '/my/file/other_file.txt', [ 'secretSE', 'StorageSE', 'TestSE' ] ),
      ( '/my/file/newfile.pdf', [ 'TestSE' ] ),
      ( '/my/file/a', [] ), ( '/dir/somefile', [ '' ] ) ] )
    assertDiracSucceedsWith_equals( self.tfp.run(), [ ( '', tup ) for ( _se, tup ) in new_tasks ], self )

  def test_limited_too_many( self ):
    from ILCDIRAC.ILCTransformationSystem.Agent.TransformationPlugin import TransformationPlugin
    from DIRAC import S_OK
    dataman_mock = Mock()
    trans_mock = Mock()
    trans_mock.getCounters.return_value = S_OK( [ ( { 'Status' : 'Processed' }, 2 ) ] * 23 +
                                                [ ( { 'Status' : 'Assigned' }, 1 ) ] * 15 +
                                                [ ( { 'Status' : 'junk' }, 0 ) ] * 4 )
    self.tfp = TransformationPlugin( 'Limited', trans_mock, dataman_mock )
    self.tfp.params[ 'MaxNumberOfTasks' ] = 59
    self.tfp.params[ 'TransformationID' ] = 78456
    assertDiracFailsWith( self.tfp.run(), 'too many tasks', self )

  def test_limited_getcounters_fails( self ):
    from ILCDIRAC.ILCTransformationSystem.Agent.TransformationPlugin import TransformationPlugin
    from DIRAC import S_ERROR
    dataman_mock = Mock()
    util_mock = Mock()
    trans_mock = Mock()
    trans_mock.getCounters.return_value = S_ERROR( 'my_test_getcount_err' )
    util_mock.transClient = trans_mock
    self.tfp = TransformationPlugin( 'Limited', dataman_mock, trans_mock )
    self.tfp.params[ 'Status' ] = 'Processed'
    self.tfp.params[ 'MaxNumberOfTasks' ] = 6
    self.tfp.params[ 'TransformationID' ] = 78456
    self.tfp.util = util_mock
    assertDiracFailsWith( self.tfp.run(), 'my_test_getcount_err', self )

  def test_limited_groupbyreplicas_fails( self ):
    from ILCDIRAC.ILCTransformationSystem.Agent.TransformationPlugin import TransformationPlugin
    from DIRAC import S_OK, S_ERROR
    dataman_mock = Mock()
    util_mock = Mock()
    util_mock.groupByReplicas.return_value = S_ERROR( 'group_test_err' )
    trans_mock = Mock()
    trans_mock.getCounters.return_value = S_OK(
      [ ( { 'Status' : 'Processed' }, 3 ), ( { 'Status' : 'junk' }, 6 ), ( { 'Status' : 'Ignore_me' }, 8 ),
        ( { 'Status' : 'Assigned' }, 1 ) ] )
    util_mock.transClient = trans_mock
    self.tfp = TransformationPlugin( 'Limited', dataman_mock, trans_mock )
    self.tfp.params[ 'Status' ] = 'Processed'
    self.tfp.params[ 'MaxNumberOfTasks' ] = 12
    self.tfp.params[ 'TransformationID' ] = 78456
    self.tfp.util = util_mock
    self.tfp.setInputData( [
      ( '/my/file/here_abc', [ 'myTestSE' ] ),
      ( '/my/file/other_file.txt', [ 'secretSE', 'StorageSE', 'TestSE' ] ),
      ( '/my/file/newfile.pdf', [ 'TestSE' ] ),
      ( '/my/file/a', [] ), ( '/dir/somefile', [ '' ] ) ] )
    assertDiracFailsWith( self.tfp.run(), 'group_test_err', self )

  def test_sliced( self ):
    from ILCDIRAC.ILCTransformationSystem.Agent.TransformationPlugin import TransformationPlugin
    dataman_mock = Mock()
    trans_mock = Mock()
    self.tfp = TransformationPlugin( 'Sliced', dataman_mock, trans_mock )
    self.tfp.setInputData( {
      '/my/file/here_abc' : [ 'myTestSE' ],
      '/my/file/other_file.txt' : [ 'secretSE', 'StorageSE', 'TestSE' ],
      '/my/file/newfile.pdf' : [ 'TestSE' ],
      '/my/file/a' : [], '/dir/somefile': [ '' ] } )
    result = self.tfp.run()
    assertDiracSucceeds( result, self )
    assertListContentEquals( result[ 'Value' ], [
      ( '', [ '/my/file/here_abc' ] ), ( '', [ '/my/file/other_file.txt' ] ),
      ( '', [ '/my/file/newfile.pdf' ] ), ( '', [ '/my/file/a' ] ), ( '', [ '/dir/somefile' ] ) ], self )

  def test_sliced_empty( self ):
    from ILCDIRAC.ILCTransformationSystem.Agent.TransformationPlugin import TransformationPlugin
    dataman_mock = Mock()
    trans_mock = Mock()
    self.tfp = TransformationPlugin( 'Sliced', dataman_mock, trans_mock )
    self.tfp.setInputData( {} )
    assertDiracSucceedsWith_equals( self.tfp.run(), [], self )

  def test_sliced_limited_add_up_to_maximum( self ):
    from ILCDIRAC.ILCTransformationSystem.Agent.TransformationPlugin import TransformationPlugin
    from DIRAC import S_OK
    dataman_mock = Mock()
    util_mock = Mock()
    trans_mock = Mock()
    trans_mock.getCounters.return_value = S_OK(
      [ ( { 'Status' : 'Processed' }, 4 ), ( { 'Status' : 'junk' }, 6 ), ( { 'Status' : 'Ignore_me' }, 8 ),
        ( { 'Status' : 'Assigned' }, 6 ) ] )
    util_mock.transClient = trans_mock
    self.tfp = TransformationPlugin( 'SlicedLimited', dataman_mock, trans_mock )
    self.tfp.params[ 'Status' ] = 'Processed'
    self.tfp.params[ 'MaxNumberOfTasks' ] = 12
    self.tfp.params[ 'TransformationID' ] = 78456
    self.tfp.util = util_mock
    self.tfp.setInputData( {
      '/my/file/here_abc' : [ 'myTestSE' ],
      '/my/file/other_file.txt' : [ 'secretSE', 'StorageSE', 'TestSE' ],
      '/my/file/newfile.pdf' : [ 'TestSE' ],
      '/my/file/a' : [], '/dir/somefile': [ '' ] } )
    result = self.tfp.run()
    assertDiracSucceeds( result, self )
    to_check = result[ 'Value' ]
    print result
    assertEqualsImproved(
      ( len( to_check ), to_check[0][0], to_check[1][0], len( to_check[0][1] ), len( to_check[1][1] ) ),
      ( 2, '', '', 1, 1 ), self ) # Checks that two tasks are added, that the two tasks have the form ('', [ a ])
    self.assertNotEquals( to_check[0][1][0], to_check[1][1][0] ) # Checks that the same lfn isnt added twice
    # Checks that the LFN is one of those expected (since keys() iteration is random for dictionaries)
    expected = [ '/my/file/here_abc', '/my/file/other_file.txt', '/my/file/newfile.pdf',
                 '/my/file/a', '/dir/somefile' ]
    assertInImproved( to_check[0][1][0], expected, self )
    assertInImproved( to_check[1][1][0], expected, self )
    trans_mock.getCounters.assert_called_once_with( 'TransformationFiles', [ 'Status' ],
                                                    { 'TransformationID' : 78456 } )

  def test_sliced_limited_add_all( self ):
    from ILCDIRAC.ILCTransformationSystem.Agent.TransformationPlugin import TransformationPlugin
    from DIRAC import S_OK
    dataman_mock = Mock()
    util_mock = Mock()
    trans_mock = Mock()
    trans_mock.getCounters.return_value = S_OK(
      [ ( { 'Status' : 'Processed' }, 4 ), ( { 'Status' : 'junk' }, 6 ), ( { 'Status' : 'Ignore_me' }, 8 ),
        ( { 'Status' : 'Assigned' }, 6 ) ] )
    util_mock.transClient = trans_mock
    self.tfp = TransformationPlugin( 'SlicedLimited', dataman_mock, trans_mock )
    self.tfp.params[ 'Status' ] = 'Processed'
    self.tfp.params[ 'MaxNumberOfTasks' ] = 16
    self.tfp.params[ 'TransformationID' ] = 78456
    self.tfp.util = util_mock
    self.tfp.setInputData( {
      '/my/file/here_abc' : [ 'myTestSE' ],
      '/my/file/other_file.txt' : [ 'secretSE', 'StorageSE', 'TestSE' ],
      '/my/file/newfile.pdf' : [ 'TestSE' ],
      '/my/file/a' : [], '/dir/somefile': [ '' ] } )
    result = self.tfp.run()
    assertDiracSucceeds( result, self )
    expected = [ ( '', [ '/my/file/here_abc' ] ), ( '', [ '/my/file/other_file.txt' ] ),
                 ( '', [ '/my/file/newfile.pdf' ] ), ( '', [ '/my/file/a' ] ), ( '', [ '/dir/somefile' ] ) ]
    assertListContentEquals( result[ 'Value' ], expected, self )
    trans_mock.getCounters.assert_called_once_with( 'TransformationFiles', [ 'Status' ],
                                                    { 'TransformationID' : 78456 } )

  def test_sliced_limited_too_many( self ):
    from ILCDIRAC.ILCTransformationSystem.Agent.TransformationPlugin import TransformationPlugin
    from DIRAC import S_OK
    dataman_mock = Mock()
    trans_mock = Mock()
    trans_mock.getCounters.return_value = S_OK( [ ( { 'Status' : 'Processed' }, 2 ) ] * 14 +
                                                [ ( { 'Status' : 'Assigned' }, 1 ) ] * 15 +
                                                [ ( { 'Status' : 'junk' }, 0 ) ] * 4 )
    self.tfp = TransformationPlugin( 'SlicedLimited', trans_mock, dataman_mock )
    self.tfp.params[ 'MaxNumberOfTasks' ] = 18
    self.tfp.params[ 'TransformationID' ] = 78456
    assertDiracFailsWith( self.tfp.run(), 'too many tasks', self )
    trans_mock.getCounters.assert_called_once_with( 'TransformationFiles', [ 'Status' ],
                                                    { 'TransformationID' : 78456 } )

  def test_sliced_limited_getcounters_fails( self ):
    from ILCDIRAC.ILCTransformationSystem.Agent.TransformationPlugin import TransformationPlugin
    from DIRAC import S_ERROR
    dataman_mock = Mock()
    util_mock = Mock()
    trans_mock = Mock()
    trans_mock.getCounters.return_value = S_ERROR( 'my_test_getcount_err' )
    util_mock.transClient = trans_mock
    self.tfp = TransformationPlugin( 'SlicedLimited', dataman_mock, trans_mock )
    self.tfp.params[ 'Status' ] = 'Processed'
    self.tfp.params[ 'MaxNumberOfTasks' ] = 6
    self.tfp.params[ 'TransformationID' ] = 78456
    self.tfp.util = util_mock
    assertDiracFailsWith( self.tfp.run(), 'my_test_getcount_err', self )
    trans_mock.getCounters.assert_called_once_with( 'TransformationFiles', [ 'Status' ],
                                                    { 'TransformationID' : 78456 } )

  def test_broadcast_processed_selectall( self ):
    from DIRAC import S_OK
    from ILCDIRAC.ILCTransformationSystem.Agent.TransformationPlugin import TransformationPlugin
    dataman_mock = Mock()
    trans_mock = Mock()
    util_mock = Mock()
    util_mock.fc.getFileDescendents.return_value = S_OK(
      { 'Successful' :
        { '/some/deep/dir/structure/file1.stdio' : [ 'child' ], '/file/dir/input1.txt' : [ 'child' ],
          '/dir/file.xml' : [ 'child' ], '/nodir.txt' : [ 'child' ], 'input_file.txt' : [ 'child' ],
          '/my/dir/pphys.ics' : [ 'child' ] },
        'Failed' : {} } )
    util_mock.transClient = trans_mock
    self.tfp = TransformationPlugin( 'BroadcastProcessed', dataman_mock, trans_mock )
    self.tfp.params[ 'Status' ] = ''
    self.tfp.util = util_mock
    self.tfp.setInputData( { '/some/deep/dir/structure/file1.stdio' : [ 'myTestSE1' ],
                             '/file/dir/input1.txt' : [ 'other_SE' ],
                             '/dir/file.xml' : [ 'myTestSE1', 'SecondTestSE' ],
                             '/nodir.txt' : [ 'secretSE' ],
                             'input_file.txt' : [ 'SE_to_test' ],
                             '/my/dir/pphys.ics' : [ '' ] } )
    with patch('%s.TransformationPlugin._Broadcast' % MODULE_NAME, new=Mock(return_value=S_OK(98124))):
      assertDiracSucceedsWith_equals( self.tfp.run(), 98124, self )
      assertListContentEquals( self.tfp.data,
                               [ '/some/deep/dir/structure/file1.stdio', '/file/dir/input1.txt',
                                 '/dir/file.xml', '/nodir.txt', 'input_file.txt', '/my/dir/pphys.ics' ], self )
      util_mock.fc.getFileDescendents.assert_called_once()

  def test_broadcast_processed_flush( self ):
    from DIRAC import S_OK
    from ILCDIRAC.ILCTransformationSystem.Agent.TransformationPlugin import TransformationPlugin
    dataman_mock = Mock()
    trans_mock = Mock()
    self.tfp = TransformationPlugin( 'BroadcastProcessed', dataman_mock, trans_mock )
    self.tfp.params[ 'Status' ] = 'Flush'
    with patch('%s.TransformationPlugin._Broadcast' % MODULE_NAME, new=Mock(return_value=S_OK(8124))):
      assertDiracSucceedsWith_equals( self.tfp.run(), 8124, self )

  def test_broadcast_processed_getdesc_fails( self ):
    from DIRAC import S_ERROR
    from ILCDIRAC.ILCTransformationSystem.Agent.TransformationPlugin import TransformationPlugin
    dataman_mock = Mock()
    trans_mock = Mock()
    util_mock = Mock()
    util_mock.fc.getFileDescendents.return_value = S_ERROR( 'Descendant_test_err' )
    util_mock.transClient = trans_mock
    self.tfp = TransformationPlugin( 'BroadcastProcessed', dataman_mock, trans_mock )
    self.tfp.params[ 'Status' ] = ''
    self.tfp.util = util_mock
    self.tfp.setInputData( { '/some/deep/dir/structure/file1.stdio' : [ 'myTestSE1' ],
                             '/file/dir/input1.txt' : [ 'other_SE' ],
                             '/dir/file.xml' : [ 'myTestSE1', 'SecondTestSE' ],
                             '/nodir.txt' : [ 'secretSE' ],
                             'input_file.txt' : [ 'SE_to_test' ],
                             '/my/dir/pphys.ics' : [ '' ] } )
    assertDiracFailsWith( self.tfp.run(), 'descendant_test_err', self )
    util_mock.fc.getFileDescendents.assert_called_once()

  def test_broadcast_processed_selectsome( self ):
    from DIRAC import S_OK
    from ILCDIRAC.ILCTransformationSystem.Agent.TransformationPlugin import TransformationPlugin
    dataman_mock = Mock()
    trans_mock = Mock()
    util_mock = Mock()
    util_mock.fc.getFileDescendents.return_value = S_OK(
      { 'Successful' :
        { '/some/deep/dir/structure/file1.stdio' : [ 'child' ], '/file/dir/input1.txt' : [],
          '/dir/file.xml' : [ 'child' ], '/nodir.txt' : [ 'child' ], 'input_file.txt' : [ 'child' ] },
        'Failed' : { '/my/dir/pphys.ics' : [] } } )
    util_mock.transClient = trans_mock
    self.tfp = TransformationPlugin( 'BroadcastProcessed', dataman_mock, trans_mock )
    self.tfp.params[ 'Status' ] = ''
    self.tfp.util = util_mock
    self.tfp.setInputData( { '/some/deep/dir/structure/file1.stdio' : [ 'myTestSE1' ],
                             '/file/dir/input1.txt' : [ 'other_SE' ],
                             '/dir/file.xml' : [ 'myTestSE1', 'SecondTestSE' ],
                             '/nodir.txt' : [ 'secretSE' ],
                             'input_file.txt' : [ 'SE_to_test' ],
                             '/my/dir/pphys.ics' : [ '' ] } )
    with patch('%s.TransformationPlugin._Broadcast' % MODULE_NAME, new=Mock(return_value=S_OK(98124))):
      assertDiracSucceedsWith_equals( self.tfp.run(), 98124, self )
      assertListContentEquals( self.tfp.data,
                               [ '/some/deep/dir/structure/file1.stdio', '/dir/file.xml', '/nodir.txt',
                                 'input_file.txt' ], self )
      util_mock.fc.getFileDescendents.assert_called_once()

  def test_broadcast_processed_checkchunking( self ):
    from DIRAC import S_OK
    from ILCDIRAC.ILCTransformationSystem.Agent.TransformationPlugin import TransformationPlugin
    import copy
    files = {}
    for i in xrange(0, 599):
      files[ ( '/file/dir/input%s.txt' % i ) ] = [ 'child' ]
    dataman_mock = Mock()
    trans_mock = Mock()
    util_mock = Mock()
    util_mock.fc.getFileDescendents.return_value = S_OK( { 'Successful' : files, 'Failed' : {} } )
    util_mock.transClient = trans_mock
    self.tfp = TransformationPlugin( 'BroadcastProcessed', dataman_mock, trans_mock )
    self.tfp.params[ 'Status' ] = ''
    self.tfp.util = util_mock
    self.tfp.setInputData( copy.deepcopy( files ) )
    del self.tfp.data[ '/file/dir/input542.txt' ]
    with patch('%s.TransformationPlugin._Broadcast' % MODULE_NAME, new=Mock(return_value=S_OK(98124))):
      assertDiracSucceedsWith_equals( self.tfp.run(), 98124, self )
      expected = {}
      for i in xrange(0, 599):
        if i != 542:
          expected[ ( '/file/dir/input%s.txt' % i ) ] = [ 'child' ]
      assertListContentEquals( self.tfp.data, expected, self )
    assertEqualsImproved( len( util_mock.fc.getFileDescendents.mock_calls ), 3, self )
