#!/usr/local/env python
"""
 Test ReleaseHelper module

 """
 
import unittest
from mock import patch, MagicMock as Mock

from ILCDIRAC.ILCTransformationSystem.Utilities.ReleaseHelper import killRPath, getFiles, \
  copyLibraries, resolveLinks, getLibraryPath, copyFolder, getPythonStuff, removeSystemLibraries, \
  getDependentLibraries, getGeant4DataFolders, getRootStuff, insertCSSection
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
    assertDiracSucceeds, assertMockCalls, assertListContentEquals

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.ILCTransformationSystem.Utilities.ReleaseHelper'

#pylint: disable=too-many-public-methods,no-self-use
class ReleaseHelperTestCase( unittest.TestCase ):
  """ Base class for the ReleaseHelper test cases
  """
  def setUp(self):
    """set up the objects"""
    pass

#  def tearDown( self ):
#    pass

  def test_killrpath( self ):
    walk_list = [ ( '/testRoot', 'nodirs', [ 'file1.txt', 'deletethislib.so', '', 'ignorefile.s o' ] ),
                  ( '/otherRoot/mydir/', '', [ 'lib1.so', 'subfolder/otherlib.so', 'library.dll' ] ) ]
    with patch('%s.os.walk' % MODULE_NAME, new=Mock(return_value=walk_list)) as walk_mock, \
         patch('%s.commands.getstatusoutput' % MODULE_NAME, new=Mock()) as cmd_mock:
      killRPath( 'myTest_folder' )
      walk_mock.assert_called_once_with( 'myTest_folder', followlinks=True )
      expected_files = [ '/testRoot/deletethislib.so', '/otherRoot/mydir/lib1.so',
                         '/otherRoot/mydir/subfolder/otherlib.so' ]
      assertMockCalls( cmd_mock, [ 'chrpath -d ' + x + ' ' for x in expected_files ], self )

  def test_killrpath_donothing( self ):
    with patch('%s.os.walk' % MODULE_NAME, new=Mock(return_value=[])) as walk_mock, \
         patch('%s.commands.getstatusoutput' % MODULE_NAME, new=Mock()) as cmd_mock:
      killRPath( 'myTest_folder' )
      walk_mock.assert_called_once_with( 'myTest_folder', followlinks=True )
      self.assertFalse( cmd_mock.called)

  def test_getfiles( self ):
    walk_list = [ ( '/testRoot', 'nodirs', [ 'file1.txt', 'deletethislib.so', '', 'ignorefile.stdhep' ] ),
                  ( '/otherRoot/mydir/', '', [ 'particle.stdhep', 'subfolder/otherlib.stdhep', 'library.dll' ] ) ]
    with patch('%s.os.walk' % MODULE_NAME, new=Mock(return_value=walk_list)) as walk_mock:
      res = getFiles( 'myTest_folder', '.stdhep' )
      walk_mock.assert_called_once_with( 'myTest_folder', followlinks=True )
      expected_files = [ '/testRoot/ignorefile.stdhep', '/otherRoot/mydir/particle.stdhep',
                         '/otherRoot/mydir/subfolder/otherlib.stdhep' ]
      assertListContentEquals( res, expected_files, self )

  def test_getfiles_donothing( self ):
    with patch('%s.os.walk' % MODULE_NAME, new=Mock(return_value=[])) as walk_mock:
      self.assertFalse( getFiles( 'myTest_folder', '' ) )
      walk_mock.assert_called_once_with( 'myTest_folder', followlinks = True )

  def test_copylibraries( self ):
    with patch('%s.commands.getstatusoutput' % MODULE_NAME, new=Mock(return_value=(0, ''))) as cmd_mock:
      copyLibraries( [ 'myFile.txt', 'some_dir/otherfile.so', 'copythis.log' ], 'testFolderTarget' )
      cmd_mock.assert_called_once_with( "rsync --exclude '.svn' -avzL  myFile.txt some_dir/otherfile.so copythis.log testFolderTarget " )

  def test_copylibraries_copy_error( self ):
    with patch('%s.commands.getstatusoutput' % MODULE_NAME, new=Mock(return_value=(1, ''))), \
         self.assertRaises(RuntimeError):
      copyLibraries( [ 'myFile.txt', 'some_dir/otherfile.so', 'copythis.log' ], 'testFolderTarget' )

  def test_resolvelinks( self ):
    walk_list = [ ( '/my/test/rootdir', 'ignored',
                    [ 'testfile_1.txt', 'mylib_a.so', 'subfolder/otherlib_b.so.1', 'mylib_a.so.1' ] ),
                  ( '/other/libfolder/', 'other_ignore', [ 'library.so', 'requirement.so', 'otherlib_b.so',
                                                           'requirement.so.2' ] ), ( '', '', [] ) ]
    with patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/test/dir/cwd')) as getcwd_mock, \
         patch('%s.os.chdir' % MODULE_NAME, new=Mock()) as chdir_mock, \
         patch('%s.os.walk' % MODULE_NAME, new=Mock(return_value=walk_list)) as walk_mock, \
         patch('%s.os.remove' % MODULE_NAME, new=Mock()) as remove_mock, \
         patch('%s.os.symlink' % MODULE_NAME, new=Mock()) as slink_mock:
      resolveLinks( 'my_test_dir' )
      getcwd_mock.assert_called_once_with()
      assertMockCalls( chdir_mock, [ 'my_test_dir', '/test/dir/cwd' ], self )
      assertMockCalls( remove_mock, [ 'mylib_a.so', 'otherlib_b.so', 'requirement.so' ], self )
      assertMockCalls( slink_mock, [ ( 'mylib_a.so.1', 'mylib_a.so' ), ( 'otherlib_b.so.1', 'otherlib_b.so' ),
                                     ( 'requirement.so.2', 'requirement.so' ) ], self )
      walk_mock.assert_called_once_with( 'my_test_dir', followlinks = True )

  def test_resolvelinks_none_match( self ):
    walk_list = [ ( '/my/test/rootdir', 'ignored', [ 'testfile_1.txt', 'mylib_a.so', 'subfolder/otherlib_b.so.1' ] ), ( '/other/libfolder/', 'other_ignore', [ 'library.so', 'requirement.so' ] ), ( '', '', [] ) ]
    with patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/test/dir/cwd')) as getcwd_mock, \
         patch('%s.os.chdir' % MODULE_NAME, new=Mock()) as chdir_mock, \
         patch('%s.os.walk' % MODULE_NAME, new=Mock(return_value=walk_list)), \
         patch('%s.os.remove' % MODULE_NAME, new=Mock()) as remove_mock, \
         patch('%s.os.symlink' % MODULE_NAME, new=Mock()) as slink_mock:
      resolveLinks( 'my_test_dir' )
      getcwd_mock.assert_called_once_with()
      assertMockCalls( chdir_mock, [ 'my_test_dir', '/test/dir/cwd' ], self )
      self.assertFalse( remove_mock.called )
      self.assertFalse( slink_mock.called )

  def test_getlibrarypath( self ):
    assertEqualsImproved( getLibraryPath( '/some/folder/on/the/fs' ), '/some/folder/on/the/fs/lib', self )

  def test_getlibrarypath_withslash( self ):
    assertEqualsImproved( getLibraryPath( '/some/folder/on/the/fs/' ), '/some/folder/on/the/fs/lib', self )

  def test_getlibrarypath_nopath( self ):
    assertEqualsImproved( getLibraryPath( '' ), 'lib', self )

  def test_copyfolder( self ):
    with patch('%s.os.makedirs' % MODULE_NAME, new=Mock()) as makedirs_mock, \
         patch('%s.commands.getstatusoutput' % MODULE_NAME, new=Mock(return_value=(0, ''))) as cmd_mock:
      copyFolder( 'test_base', 'test_destination/some/folders' )
      makedirs_mock.assert_called_once_with( 'test_destination/some/folders' )
      cmd_mock.assert_called_once_with( "rsync --exclude '.svn' -avzL  test_base test_destination/some/folders " )

  def test_copyfolder_ignore_oserr( self ):
    with patch('%s.os.makedirs' % MODULE_NAME, new=Mock(side_effect=OSError('test_err'))) as makedirs_mock, \
         patch('%s.commands.getstatusoutput' % MODULE_NAME, new=Mock(return_value=(0, ''))) as cmd_mock:
      copyFolder( 'test_base', 'test_destination/some/folders' )
      makedirs_mock.assert_called_once_with( 'test_destination/some/folders' )
      cmd_mock.assert_called_once_with( "rsync --exclude '.svn' -avzL  test_base test_destination/some/folders " )

  def test_copyfolder_copy_fails( self ):
    with patch('%s.os.makedirs' % MODULE_NAME, new=Mock()) as makedirs_mock, \
         patch('%s.commands.getstatusoutput' % MODULE_NAME, new=Mock(return_value=(1, ''))) as cmd_mock, \
         self.assertRaises(RuntimeError):
      copyFolder( 'test_base', 'test_destination/some/folders' )
      makedirs_mock.assert_called_once_with( 'test_destination/some/folders' )
      cmd_mock.assert_called_once_with( "rsync --exclude '.svn' -avzL  test_base test_destination/some/folders " )

  def test_getpythonstuff( self ):
    with patch('%s.copyFolder' % MODULE_NAME, new=Mock()) as copy_mock:
      getPythonStuff( 'source', 'dst' )
      copy_mock.assert_called_once_with( 'source', 'dst' )

  def test_removesyslibs( self ):
    walk_list = [ ( '/my/usr/lib/', 'ignore',
                    [ '', 'libc.so.2', 'myfile.txt', 'libc-2.5.so', 'libm.so.1', 'libpthread.so.123',
                      'ignore_me.log', 'libdl.so', 'ignore_me.so.1', 'libstdc++.so.1', 'ignore_me.so',
                      'libgcc_s.so.1' ] ),
                  ( '/lib/dir/subdir/', 'ignoretoo',
                    [ 'some_file.so', 'other_lib.so.1', 'libc.so', 'libc-2.5.so.1', 'libm.so',
                      'libpthread.so', 'libdl.so.124', 'libstdc++.so', 'libgcc_s.so.1.18' ] ) ]
    remove_vals = [ None, OSError('test_remove_failure') ] + [ None ] * 12
    with patch('%s.os.walk' % MODULE_NAME, new=Mock(return_value=walk_list)) as walk_mock, \
         patch('%s.os.remove' % MODULE_NAME, new=Mock(side_effect=remove_vals)) as remove_mock:
      removeSystemLibraries( 'my_folder_tbr' )
      walk_mock.assert_called_once_with( 'my_folder_tbr' )
      expected_remove_files_1 = [ 'libc.so.2', 'libc-2.5.so', 'libm.so.1', 'libpthread.so.123',
                                  'libdl.so', 'libstdc++.so.1', 'libgcc_s.so.1' ]
      expected_remove_files_2 = [ 'libc.so', 'libc-2.5.so.1', 'libm.so', 'libpthread.so', 'libdl.so.124',
                                  'libstdc++.so', 'libgcc_s.so.1.18' ]
      assertMockCalls( remove_mock, [ '/my/usr/lib/' + x for x in expected_remove_files_1 ] +
                       [ '/lib/dir/subdir/' + x for x in expected_remove_files_2 ], self )

  def test_getdependentlibs( self ):
    outputlines = '\tsome_string=> some_replacement (0x\n\ttest_string_deleteme=> valid_pattern         asdgf           (0x\n\n some_other_string=> invalid_replacement(0x\n'
    with patch('%s.commands.getstatusoutput' % MODULE_NAME, new=Mock(return_value=(0, outputlines))) as cmd_mock:
      res = getDependentLibraries( 'MyTest_library' )
      cmd_mock.assert_called_once_with( 'ldd MyTest_library' )
      assertEqualsImproved( res, { 'some_replacement', 'valid_pattern         asdgf          ' }, self )

  def test_getdependentlibs_fails( self ):
    outputlines = '\tsome_string=> some_replacement (0x\n\t not found 98jefeiufmn=> aufm (0x\n\ttest_string_deleteme=> valid_pattern         asdgf           (0x\n\n'
    with patch('%s.commands.getstatusoutput' % MODULE_NAME, new=Mock(return_value=(0, outputlines))), \
         self.assertRaises(RuntimeError):
      getDependentLibraries( 'MyTest_library' )

  def test_getgeant4datafolders( self ):
    import os
    with patch('%s.copyFolder' % MODULE_NAME, new=Mock()) as copy_mock, \
         patch.dict( os.environ, { 'MY_TEST_VAR' : '/my/test/path/dir' } ):
      getGeant4DataFolders( 'MY_TEST_VAR', '/test_target_folder/fold/' )
      copy_mock.assert_called_once_with( '/my/test/path/dir', '/test_target_folder/fold/' )

  def test_getrootstuff( self ):
    file_list = [ '/my_test/aim/lib/some_lib.so', '/my_test/aim/lib/my_important_lib.so.1',
                  '/my_test/aim/lib/last_lib.so.123' ]
    dependency_list = [ { 'first_dependency.so', 'base_lib.so.1' }, {}, { 'last_dependency.so.39' } ]
    with patch('%s.commands.getstatusoutput' % MODULE_NAME, new=Mock(return_value=(0, ''))) as cmd_mock, \
         patch('%s.getFiles' % MODULE_NAME, new=Mock(return_value=file_list)) as getfile_mock, \
         patch('%s.getDependentLibraries' % MODULE_NAME, new=Mock(side_effect=dependency_list)) as getlibs_mock, \
         patch('%s.copyLibraries' % MODULE_NAME, new=Mock()) as copy_mock:
      getRootStuff( 'test_rootsys', '/my_test/aim' )
      cmd_mock.assert_called_once_with( "rsync --exclude '.svn' -av test_rootsys/lib test_rootsys/etc test_rootsys/bin test_rootsys/cint  /my_test/aim")
      getfile_mock.assert_called_once_with( '/my_test/aim/lib', '.so' )
      assertMockCalls( getlibs_mock, [
        '/my_test/aim/lib/some_lib.so', '/my_test/aim/lib/my_important_lib.so.1',
        '/my_test/aim/lib/last_lib.so.123' ], self )
      copy_mock.assert_called_once_with( { 'first_dependency.so', 'base_lib.so.1', 'last_dependency.so.39' },
                                         '/my_test/aim/lib' )

  def test_getrootstuff_fails( self ):
    with patch('%s.commands.getstatusoutput' % MODULE_NAME, new=Mock(return_value=(1,''))), \
         self.assertRaises(RuntimeError):
      getRootStuff( 'rootsys', 'target' )

  def test_getcssection( self ):
    from DIRAC import S_OK
    cs_mock = Mock()
    cs_mock.setOption.return_value = S_OK()
    assertDiracSucceeds(
      insertCSSection( cs_mock, '/TestConfig/full/path', {
        'newsubsection/Parameter1' : '194851', 'newsubsection' :
        { 'newsubsubsection/ComplexParameter' : True } } ), self )
    assertMockCalls( cs_mock.setOption, [
      ( '/TestConfig/full/path/newsubsection/Parameter1', '194851' ),
      ( '/TestConfig/full/path/newsubsection/newsubsubsection/ComplexParameter', True ) ], self )
    assertMockCalls( cs_mock.createSection, [ '/TestConfig/full/path', '/TestConfig/full/path',
                                              '/TestConfig/full/path/newsubsection' ], self )

  def test_getcssection_fails( self ):
    from DIRAC import S_OK, S_ERROR
    cs_mock = Mock()
    cs_mock.setOption.side_effect = [ S_OK(), S_ERROR('set_option_failed!_testme') ]
    assertDiracFailsWith(
      insertCSSection( cs_mock, '/TestConfig/full/path',
                       { 'newsubsection/Parameter1' : '194851',
                         'newsubsection/Parameter2_invalid_value' : 'i cause an error' } ),
      'set_option_failed!_testme', self )
