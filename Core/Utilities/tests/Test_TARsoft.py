#!/usr/bin/env python
"""Test the TAR Software class"""

import unittest
import os
import sys
from mock import mock_open, patch, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, assertDiracSucceeds, assertDiracSucceedsWith_equals
from ILCDIRAC.Tests.Utilities.FileUtils import FileUtil

__RCSID__ = "$Id$"

class TestTARsoft( unittest.TestCase ):
  """ Tests the base functionality of the class """

  def setUp( self ):
    # Mock away DataManager import
    dataman_import_mock = Mock()
    dataman_import_mock.getFile.return_value = None
    sys.modules['DIRAC.DataManagementSystem.Client.DataManager'] = dataman_import_mock
    global dataman_mock
    dataman_mock = Mock()

  def test_importfails( self ):
    """ Rather stupid test. The handling of an ImportError in hashlib internally imports hashlib as well...
    """
    try:
      import builtins
    except ImportError:
      import __builtin__ as builtins
    realimport = builtins.__import__
    global importcounter
    importcounter = 0
    def myimport(name, globals=globals(), locals=locals(), fromlist=[], level=-1): #pylint: disable=C0111
      global importcounter
      if name == 'hashlib' :
        if importcounter == 4:
          importcounter = importcounter+1 # Necessary!
          raise ImportError
        else:
          importcounter = importcounter+1
      return realimport(name, globals, locals, fromlist, level)
    builtins.__import__ = myimport
    from ILCDIRAC.Core.Utilities.TARsoft import createLock

  def test_createlock( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import createLock
    file_contents = [[]]
    handles = FileUtil.getMultipleReadHandles( file_contents )
    with patch('%s.open' % MODULE_NAME, mock_open(), create=True) as mo:
      mo.side_effect = ( h for h in handles )
      assertDiracSucceeds( createLock( 'testfile.py' ), self )
      mo.assert_called_once_with( 'testfile.py', 'w' )
      expected = [ [ 'Locking this directory\n' ] ]
      self.assertEquals(len(file_contents), len(expected))
      for (index, handle) in enumerate(handles):
        cur_handle = handle.__enter__()
        self.assertEquals(len(expected[index]), handle.__enter__.return_value.write.call_count)
        for entry in expected[index]:
          cur_handle.write.assert_any_call(entry)

  def test_createlock_ioerr( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import createLock
    with patch('%s.open' % MODULE_NAME, new=Mock(side_effect=IOError('some_test_open_error')), create=True):
      assertDiracFailsWith( createLock('myfile.txt'), 'not allowed to write here: ioerror some_test_open_error', self )

  def test_check_lock_age_no_lock( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import checkLockAge
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=False)):
      result = checkLockAge( 'mylockfile.txt' )
      assertDiracSucceedsWith_equals( result, False, self )

  def test_check_lock_age_stat_fails( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import checkLockAge
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.os.stat' % MODULE_NAME, new=Mock(side_effect=OSError('some_test_stat_error'))), \
         patch('%s.wasteCPUCycles' % MODULE_NAME, new=Mock(return_value=S_OK())):
      result = checkLockAge( 'mylockfile.txt' )
      assertDiracSucceedsWith_equals( result, False, self )

  def test_check_lock_age_clear_lock_success( self ):
    import time
    from ILCDIRAC.Core.Utilities.TARsoft import checkLockAge
    stat_mock = Mock()
    stat_mock.st_atime = time.time() - 1801 # lastTouch old enough
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.os.stat' % MODULE_NAME, new=Mock(return_value=stat_mock)), \
         patch('%s.wasteCPUCycles' % MODULE_NAME, new=Mock(side_effect=[S_ERROR(), S_OK(), S_OK()])), \
         patch('%s.clearLock' % MODULE_NAME, new=Mock(side_effect=[S_ERROR(), S_OK(True)])):
      result = checkLockAge( 'mylockfile.txt' )
      assertDiracSucceedsWith_equals( result, True, self )

  def test_check_lock_age_timeout( self ):
    import time
    from ILCDIRAC.Core.Utilities.TARsoft import checkLockAge
    stat_mock = Mock()
    stat_mock.st_atime = time.time()
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.os.stat' % MODULE_NAME, new=Mock(return_value=stat_mock)), \
         patch('%s.wasteCPUCycles' % MODULE_NAME, new=Mock(return_value=S_OK())), \
         patch('%s.clearLock' % MODULE_NAME, new=Mock(return_value=S_OK(True))):
      result = checkLockAge( 'mylockfile.txt' )
      assertDiracFailsWith( result, 'buggy lock, removed: True', self )

  def test_clear_lock( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import clearLock
    with patch('%s.os.unlink' % MODULE_NAME, new=Mock(return_value=True)):
      assertDiracSucceeds( clearLock( 'mylock' ), self )

  def test_clear_lock_oserr( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import clearLock
    with patch('%s.os.unlink' % MODULE_NAME, new=Mock(side_effect=OSError('test_unlink_some_err'))):
      assertDiracFailsWith( clearLock( 'mylock' ), 'failed to clear lock: test_unlink_some_err', self )

  def test_delete_old( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import deleteOld
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=False)):
      assertDiracSucceeds( deleteOld('testfile'), self )

  def test_delete_old_file( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import deleteOld
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[True, False])) as exists_mock, \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=False)) as isdir_mock, \
         patch('%s.os.remove' % MODULE_NAME, new=Mock(return_value=True)) as remove_mock:
      assertDiracSucceeds( deleteOld('testFile_deleteMe.txt'), self )
      file_to_check = 'testFile_deleteMe.txt'
      remove_mock.assert_called_once_with( file_to_check )
      isdir_mock.assert_called_once_with( file_to_check )
      exists_mock.assert_called_with( file_to_check )

  def test_delete_old_file_fails( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import deleteOld
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[True, True])) as exists_mock, \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=False)) as isdir_mock, \
         patch('%s.os.remove' % MODULE_NAME, new=Mock(side_effect=OSError('test_remove_cannot_error'))) as remove_mock, \
         patch('%s.gLogger.error' % MODULE_NAME) as err_mock:
      assertDiracSucceeds( deleteOld('testFile_deleteMe2.txt'), self )
      file_to_check = 'testFile_deleteMe2.txt'
      remove_mock.assert_called_once_with( file_to_check )
      isdir_mock.assert_called_once_with( file_to_check )
      exists_mock.assert_called_with( file_to_check )
      err_mock.assert_any_call( 'Failed deleting testFile_deleteMe2.txt because :', 'OSError test_remove_cannot_error' )
      err_mock.assert_any_call( 'Oh Oh, something was not right, the directory testFile_deleteMe2.txt is still here' )

  def test_delete_old_dir( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import deleteOld
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[True, False])) as exists_mock, \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)) as isdir_mock, \
         patch('%s.shutil.rmtree' % MODULE_NAME, new=Mock(return_value=True)) as remove_mock:
      assertDiracSucceeds( deleteOld('/delete/this'), self )
      file_to_check = '/delete/this'
      remove_mock.assert_called_once_with( file_to_check )
      isdir_mock.assert_called_once_with( file_to_check )
      exists_mock.assert_called_with( file_to_check )

  def test_delete_old_dir_fails( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import deleteOld
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[True, True])) as exists_mock, \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)) as isdir_mock, \
         patch('%s.shutil.rmtree' % MODULE_NAME, new=Mock(side_effect=OSError('test_remove_dir_cannot_error'))) as remove_mock, \
         patch('%s.gLogger.error' % MODULE_NAME) as err_mock:
      assertDiracSucceeds( deleteOld('/delete/that'), self )
      file_to_check = '/delete/that'
      remove_mock.assert_called_once_with( file_to_check )
      isdir_mock.assert_called_once_with( file_to_check )
      exists_mock.assert_called_with( file_to_check )
      err_mock.assert_any_call( 'Failed deleting /delete/that because :', "<type 'exceptions.OSError'> test_remove_dir_cannot_error" )
      err_mock.assert_any_call( 'Oh Oh, something was not right, the directory /delete/that is still here' )

  def test_download_file( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import downloadFile
    with patch('%s.urllib.urlretrieve' % MODULE_NAME) as url_mock:
      assertDiracSucceeds( downloadFile( 'http://my/tarball/url', '/a/basename', '/my/folder/' ), self )
      url_mock.assert_called_once_with( 'http://my/tarball/url//a/basename', 'basename' )

  def test_download_file_fails( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import downloadFile
    with patch('%s.urllib.urlretrieve' % MODULE_NAME, new=Mock(side_effect=IOError('test_io_exception'))) as url_mock:
      result = downloadFile( 'http://my/other_url/', '/other/name/here', '/other_folder/' )
      url_mock.assert_called_once_with( 'http://my/other_url//other/name/here', 'here' )
      assertDiracFailsWith( result, 'exception during url retrieve: test_io_exception', self )

  def test_download_file_from_datman( self ):
    global dataman_mock
    dataman_mock.getFile.return_value=S_OK()
    with patch('%s.DataManager' % MODULE_NAME, new=Mock(return_value=dataman_mock)):
      from ILCDIRAC.Core.Utilities.TARsoft import downloadFile
      assertDiracSucceeds( downloadFile( '/my/datamanager/directory', 'app/tarball', '/other_folder/' ), self )
      dataman_mock.getFile.assert_called_once_with( '/my/datamanager/directory/app/tarball' )

  def test_download_file_from_datman_fails( self ):
    global dataman_mock
    dataman_mock.getFile.return_value=S_ERROR('datman_test_err')
    with patch('%s.DataManager' % MODULE_NAME, new=Mock(return_value=dataman_mock)):
      from ILCDIRAC.Core.Utilities.TARsoft import downloadFile
      assertDiracFailsWith( downloadFile( '/unavailable/datman/dir/', 'mytestappv2/tarball', '/other_folder/' ), 'datman_test_err', self )
      dataman_mock.getFile.assert_called_once_with( '/unavailable/datman/dir/mytestappv2/tarball' )

  def test_md5_check( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import tarMd5Check
    file_contents = [ ['849utj429foemfi84j92fno;(*ME(FOJN$EO&*R#BNOFMN(OJIm'] ]
    expected_hash = 'dab9783374461a26e100164747e84e63' # Precalculated
    handles = FileUtil.getMultipleReadHandles( file_contents )
    with patch('%s.open' % MODULE_NAME, mock_open(), create=True) as mo:
      mo.side_effect = (h for h in handles)
      result = tarMd5Check( '/my/app/tarball.tgz', expected_hash )
      mo.assert_called_once_with( '/my/app/tarball.tgz' )
      assertDiracSucceeds( result, self )

  def test_md5_check_io_err( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import tarMd5Check
    with patch('%s.open' % MODULE_NAME, new=Mock(side_effect=IOError('myerr')), create=True), \
         patch('%s.gLogger.warn' % MODULE_NAME, new=Mock()) as log_mock:
      result = tarMd5Check( '/my/app/tarball.tgz', '138974' )
      assertDiracSucceeds( result, self )
      log_mock.assert_called_once_with( 'Failed to get tar ball md5, try without' )

  def test_md5_check_hash_wrong( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import tarMd5Check
    file_contents = [['2984jt4gomrfg8924jgnm', '1938jhfo9coiemc0m90pn@O*E&HQRF(*IONU)', ')(DCIFUJE*FIUEqf9oi1mnf)']]
    expected_hash = '0981u3jr9831rkjopk,f90381'
    handles = FileUtil.getMultipleReadHandles( file_contents )
    with patch('%s.open' % MODULE_NAME, mock_open(), create=True) as mo:
      mo.side_effect = (h for h in handles)
      result = tarMd5Check( '/my/app/tarball.tgz', expected_hash )
      mo.assert_called_once_with( '/my/app/tarball.tgz' )
      assertDiracFailsWith( result, 'hash does not correspond', self )

  def test_install_deps( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import installDependencies
    with patch('%s.resolveDeps' % MODULE_NAME, new=Mock(return_value=[{ 'app' : 'myappname1', 'version' : '203.0' }, { 'app' : 'myappname2', 'version' : '138.1' }])) as dep_mock, \
         patch('%s.installInAnyArea' % MODULE_NAME) as install_mock:
      result = installDependencies( ( 'AppName', 'appvers' ), 'myconf', 'myareas' )
      assertDiracSucceeds( result, self )
      install_mock.assert_any_call( 'myareas', ['myappname1', '203.0'], 'myconf' )
      install_mock.assert_any_call( 'myareas', ['myappname2', '138.1'], 'myconf' )
      dep_mock.assert_called_once_with( 'myconf', 'appname', 'appvers' )

  def test_install_deps_nodeps( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import installDependencies
    with patch('%s.resolveDeps' % MODULE_NAME, new=Mock(return_value=[])) as dep_mock, \
         patch('%s.installInAnyArea' % MODULE_NAME, new=Mock(return_value=S_OK())) as install_mock:
      result = installDependencies( ( 'AppName', 'appvers' ), 'myconf', 'myareas' )
      assertDiracSucceeds( result, self )
      self.assertFalse( install_mock.called )
      dep_mock.assert_called_once_with( 'myconf', 'appname', 'appvers' )

  def test_install_deps_installation_fails( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import installDependencies
    with patch('%s.resolveDeps' % MODULE_NAME, new=Mock(return_value=[{ 'app' : 'myappname1', 'version' : '203.0' }, { 'app' : 'myappname2', 'version' : '138.1' }])) as dep_mock, \
         patch('%s.installInAnyArea' % MODULE_NAME, new=Mock(side_effect=[S_OK(), S_ERROR()])) as install_mock:
      result = installDependencies( ( 'AppName', 'appvers' ), 'myconf', 'myareas' )
      assertDiracFailsWith( result, "failed to install dependency: ['myappname2', '138.1']", self )
      install_mock.assert_any_call( 'myareas', ['myappname1', '203.0'], 'myconf' )
      install_mock.assert_any_call( 'myareas', ['myappname2', '138.1'], 'myconf' )
      dep_mock.assert_called_once_with( 'myconf', 'appname', 'appvers' )

  def test_install_in_any_area( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import installInAnyArea
    with patch('%s.installSinglePackage' % MODULE_NAME, new=Mock(side_effect=[S_ERROR('not here'), S_ERROR('neither here'), S_OK('Bingo')])) as install_mock:
      assertDiracSucceeds( installInAnyArea( [ 'area1', 'otherarea', 'thisonesurelyworks' ], 'myapplication2', 'jobconf' ), self )
      install_mock.assert_any_call( 'myapplication2', 'jobconf', 'area1' )
      install_mock.assert_any_call( 'myapplication2', 'jobconf', 'otherarea' )
      install_mock.assert_any_call( 'myapplication2', 'jobconf', 'thisonesurelyworks' )

  def test_install_in_any_area_fail_all( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import installInAnyArea
    with patch('%s.installSinglePackage' % MODULE_NAME, new=Mock(return_value=S_ERROR('not here'))) as install_mock:
      assertDiracFailsWith( installInAnyArea( [ 'area1', 'otherarea', 'thisonesurelyworks' ], 'myapplication3', 'jobconf' ), 'failed to install software', self )
      install_mock.assert_any_call( 'myapplication3', 'jobconf', 'area1' )
      install_mock.assert_any_call( 'myapplication3', 'jobconf', 'otherarea' )
      install_mock.assert_any_call( 'myapplication3', 'jobconf', 'thisonesurelyworks' )

  def test_install_single_package( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import installSinglePackage
    with patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='mycwd')), \
         patch('%s.installPackage' % MODULE_NAME, new=Mock(return_value=S_OK(''))) as install_mock:
      result = installSinglePackage(('appLication', 'v201'), 'jobConfiguration', 'Area51')
      assertDiracSucceeds( result, self )
      install_mock.assert_called_once_with( ('appLication', 'v201'), 'jobConfiguration', 'Area51', 'mycwd' )

  def test_install_single_package_fails( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import installSinglePackage
    with patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='mycwd')), \
         patch('%s.installPackage' % MODULE_NAME, new=Mock(return_value=S_ERROR(''))) as install_mock:
      result = installSinglePackage(('appLication', 'v202'), 'jobConfiguration', 'Area51')
      assertDiracFailsWith( result, 'failed to install here', self )
      install_mock.assert_called_once_with( ('appLication', 'v202'), 'jobConfiguration', 'Area51', 'mycwd' )

  def test_get_tarball_location( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import getTarBallLocation
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(side_effect=['tarball_loc', True, 'mymd5sum', 'mytarURL'])) as getval_mock:
      result = getTarBallLocation( ('complicated_app', 'v201'), 'complex_config', 'dummy_area' )
      assertDiracSucceedsWith_equals( result, [ 'tarball_loc', 'mytarURL', True, 'mymd5sum' ], self )
      getval_mock.assert_any_call('/AvailableTarBalls/complex_config/complicated_app/v201/TarBall', '')
      getval_mock.assert_any_call('/AvailableTarBalls/complex_config/complicated_app/v201/Overwrite', False)
      getval_mock.assert_any_call('/AvailableTarBalls/complex_config/complicated_app/v201/Md5Sum', '')
      getval_mock.assert_any_call('/AvailableTarBalls/complex_config/complicated_app/TarBallURL', '')

  def test_get_tarball_location_noapptar( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import getTarBallLocation
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(side_effect=['', True, 'mymd5sum', 'mytarURL'])):
      result = getTarBallLocation( ('complicated_app', 'v201'), 'config', 'dummy_area' )
      assertDiracFailsWith( result, 'could not find tar ball for complicated_app v201', self )

  def test_get_tarball_location_nourl( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import getTarBallLocation
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(side_effect=['tarball_loc', True, 'mymd5sum', ''])):
      result = getTarBallLocation( ('complicated_app', 'v201'), 'config', 'dummy_area' )
      assertDiracFailsWith( result, 'could not find tarballurl in cs', self )

  def test_check( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import check
    # Different treatment of with open() and open().read().....
    file_contents = [ ['HelloWorld -', 'mychecksum md5_checksum.md5', 'ef7493baf5582b41cd4fcaa25124c3d9 ./appfile1.txt', '6fe635186392d43f0ae5728e41a92f96 appfile2.ppt', 'e10dab217514cf7f2166d87fb5d04d5c ./myapp/importantfile.bin', 'a51999cbc7ee434ee95b33f1d18c7210 otherdir/main.py', 'abc libstdc++.so', 'def myapp/libgcc_s.so.1', 'ignoreme ./other_dir/libc-2.5' ] ]
    other_file_contents = [ 'appfile1r0984u3jriumfilf42890tjr742tu', 'appfile29031i4rt498jnyfouf908j248f4298fn24iuyf', 'importf90ui4j9rf41f09j14fiun41 fp1,cmic13', 'MAIN()@KJR(*@KRE)+@MOUFRIN@FR*YB^ B* @HE)J @E( HG!V!UYNEJ)(!))))' ]
    handles = FileUtil.getMultipleReadHandles( file_contents )
    with patch('%s.os.chdir' % MODULE_NAME, new=Mock(return_value=True)) as chdir_mock, \
         patch('%s.os.path.isfile' % MODULE_NAME, new=Mock(return_value=False)) as isfile_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)) as exists_mock, \
         patch('%s.open' % MODULE_NAME, mock_open(), create=True) as mo:
      mysideeff = [ handles[0] ]
      for i in xrange(0, len(other_file_contents)):
        other_read_mock = Mock()
        other_read_mock.read.return_value = other_file_contents[i]
        mysideeff.append( other_read_mock )
      mo.side_effect = tuple( mysideeff )
      result = check( ('appname', 'version'), 'mytestarea398', ['/my/basefolder', 'res_from_install[1]'] )
      expected_open_calls = [ ( '/my/basefolder/md5_checksum.md5', 'r' ), '/my/basefolder/appfile1.txt', '/my/basefolder/appfile2.ppt', '/my/basefolder/myapp/importantfile.bin', '/my/basefolder/otherdir/main.py' ]
      expected = [[]]
      FileUtil.checkFileInteractions( self, mo, expected_open_calls, expected, handles )
      assertDiracSucceedsWith_equals( result, ['/my/basefolder'], self )
      chdir_mock.assert_called_once_with( 'mytestarea398' )
      isfile_mock.assert_called_once_with( '/my/basefolder' )
      exists_mock.assert_any_call( '/my/basefolder/md5_checksum.md5' )
      exists_mock.assert_any_call( '/my/basefolder/appfile1.txt' )
      exists_mock.assert_any_call( '/my/basefolder/appfile2.ppt' )
      exists_mock.assert_any_call( '/my/basefolder/myapp/importantfile.bin' )
      exists_mock.assert_any_call( '/my/basefolder/otherdir/main.py' )
      assertEqualsImproved( len(exists_mock.mock_calls), 5, self )

  def test_check_lcsim_jar( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import check
    with patch('%s.os.path.isfile' % MODULE_NAME, new=Mock(return_value=True)) as isfile_mock, \
         patch('%s.os.chdir' % MODULE_NAME, new=Mock(return_value=True)) as chdir_mock:
      result = check( ('appname', 'version'), 'deep/area', ['mytestbasefolder', 'res_from_install[1]'] )
      assertDiracSucceedsWith_equals( result, ['mytestbasefolder'], self )
      isfile_mock.assert_called_once_with( 'mytestbasefolder' )
      chdir_mock.assert_called_once_with( 'deep/area' )

  def test_check_corrupt_checksum( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import check
    # Different treatment of with open() and open().read().....
    file_contents = [ ['HelloWorld -', 'mychecksum md5_checksum.md5', 'ef7493baf5582b41cd4fcaa25124c3d9 ./appfile1.txt', 'CORRUPT_CHECKSUM appfile2.ppt', 'e10dab217514cf7f2166d87fb5d04d5c ./myapp/importantfile.bin', 'a51999cbc7ee434ee95b33f1d18c7210 otherdir/main.py', 'abc libstdc++.so', 'def myapp/libgcc_s.so.1', 'ignoreme ./other_dir/libc-2.5' ] ]
    other_file_contents = [ 'appfile1r0984u3jriumfilf42890tjr742tu', 'appfile29031i4rt498jnyfouf908j248f4298fn24iuyf', 'importf90ui4j9rf41f09j14fiun41 fp1,cmic13', 'MAIN()@KJR(*@KRE)+@MOUFRIN@FR*YB^ B* @HE)J @E( HG!V!UYNEJ)(!))))' ]
    handles = FileUtil.getMultipleReadHandles( file_contents )
    with patch('%s.os.chdir' % MODULE_NAME, new=Mock(return_value=True)) as chdir_mock, \
         patch('%s.os.path.isfile' % MODULE_NAME, new=Mock(return_value=False)) as isfile_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)) as exists_mock, \
         patch('%s.open' % MODULE_NAME, mock_open(), create=True) as mo:
      mysideeff = [ handles[0] ]
      for i in xrange(0, len(other_file_contents)):
        other_read_mock = Mock()
        other_read_mock.read.return_value = other_file_contents[i]
        mysideeff.append( other_read_mock )
      mo.side_effect = tuple( mysideeff )
      result = check( ('appname', 'version'), 'mytestarea398', ['/my/otherfolder', 'res_from_install[1]'] )
      assertDiracFailsWith( result, 'corrupted install: file /my/otherfolder/appfile2.ppt', self )
      chdir_mock.assert_called_once_with( 'mytestarea398' )
      isfile_mock.assert_called_once_with( '/my/otherfolder' )
      exists_mock.assert_any_call( '/my/otherfolder/md5_checksum.md5' )
      exists_mock.assert_any_call( '/my/otherfolder/appfile1.txt' )
      exists_mock.assert_any_call( '/my/otherfolder/appfile2.ppt' )

  def test_check_no_checksum_file( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import check
    with patch('%s.os.chdir' % MODULE_NAME, new=Mock(return_value=True)) as chdir_mock, \
         patch('%s.os.path.isfile' % MODULE_NAME, new=Mock(return_value=False)) as isfile_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=False)) as exists_mock, \
         patch('%s.gLogger.warn' % MODULE_NAME) as warn_mock:
      result = check( ('appname', 'version'), 'mytestarea398', ['/base/', 'res_from_install[1]'] )
      assertDiracSucceedsWith_equals( result, ['/base/'], self )
      chdir_mock.assert_called_once_with( 'mytestarea398' )
      isfile_mock.assert_called_once_with( '/base/' )
      exists_mock.assert_called_once_with( '/base/md5_checksum.md5' )
      assertEqualsImproved( len(exists_mock.mock_calls), 1, self )
      warn_mock.assert_called_once_with( 'The application does not come with md5 checksum file:', ('appname', 'version'))

  def test_check_ioerr( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import check
    # Different treatment of with open() and open().read().....
    file_contents = [ [ 'ef7493baf5582b41cd4fcaa25124c3d9 ./appfile1.txt', '6fe635186392d43f0ae5728e41a92f96 appfile2.ppt', 'e10dab217514cf7f2166d87fb5d04d5c ./myapp/importantfile.bin', 'a51999cbc7ee434ee95b33f1d18c7210 otherdir/main.py', 'abc libstdc++.so', 'def myapp/libgcc_s.so.1', 'ignoreme ./other_dir/libc-2.5' ] ]
    other_file_contents = [ 'appfile1r0984u3jriumfilf42890tjr742tu', IOError('md5_read_err'), 'importf90ui4j9rf41f09j14fiun41 fp1,cmic13', 'MAIN()@KJR(*@KRE)+@MOUFRIN@FR*YB^ B* @HE)J @E( HG!V!UYNEJ)(!))))' ]
    handles = FileUtil.getMultipleReadHandles( file_contents )
    with patch('%s.os.chdir' % MODULE_NAME, new=Mock(return_value=True)) as chdir_mock, \
         patch('%s.os.path.isfile' % MODULE_NAME, new=Mock(return_value=False)) as isfile_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)) as exists_mock, \
         patch('%s.open' % MODULE_NAME, mock_open(), create=True) as mo:
      mysideeff = [ handles[0] ]
      for i in xrange(0, len(other_file_contents)):
        other_read_mock = Mock()
        if isinstance(other_file_contents[i], basestring):
          other_read_mock.read.side_effect = [ other_file_contents[i] ]
        else:
          other_read_mock.read.side_effect = other_file_contents[i]
        mysideeff.append( other_read_mock )
      mo.side_effect = tuple( mysideeff )
      result = check( ('appname', 'version'), 'mytestarea398', ['/my/basefolder', 'res_from_install[1]'] )
      expected_open_calls = [ ( '/my/basefolder/md5_checksum.md5', 'r' ), '/my/basefolder/appfile1.txt', '/my/basefolder/appfile2.ppt' ]
      expected = [[]]
      assertDiracFailsWith( result, 'failed to compute md5 sum', self )
      FileUtil.checkFileInteractions( self, mo, expected_open_calls, expected, handles )
      chdir_mock.assert_called_once_with( 'mytestarea398' )
      isfile_mock.assert_called_once_with( '/my/basefolder' )
      exists_mock.assert_any_call( '/my/basefolder/md5_checksum.md5' )
      exists_mock.assert_any_call( '/my/basefolder/appfile1.txt' )
      exists_mock.assert_any_call( '/my/basefolder/appfile2.ppt' )
      assertEqualsImproved( len(exists_mock.mock_calls), 3, self )

  def test_check_empty_checksum_file( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import check
    file_contents = [[]]
    handles = FileUtil.getMultipleReadHandles( file_contents )
    with patch('%s.os.chdir' % MODULE_NAME, new=Mock(return_value=True)) as chdir_mock, \
         patch('%s.os.path.isfile' % MODULE_NAME, new=Mock(return_value=False)) as isfile_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)) as exists_mock, \
         patch('%s.open' % MODULE_NAME, mock_open(), create=True) as mo, \
         patch('%s.gLogger.warn' % MODULE_NAME) as warn_mock:
      mo.side_effect = (h for h in handles)
      result = check( ('appname', 'version'), 'mytestarea398', ['/my/basefolder', 'res_from_install[1]'] )
      expected_open_calls = [( '/my/basefolder/md5_checksum.md5', 'r' )]
      expected = [[]]
      FileUtil.checkFileInteractions( self, mo, expected_open_calls, expected, handles )
      assertDiracSucceedsWith_equals( result, ['/my/basefolder'], self )
      chdir_mock.assert_called_once_with( 'mytestarea398' )
      self.assertFalse( warn_mock.called )
      isfile_mock.assert_called_once_with( '/my/basefolder' )
      exists_mock.assert_any_call( '/my/basefolder/md5_checksum.md5' )
      assertEqualsImproved( len(exists_mock.mock_calls), 1, self )

  def test_check_missing_file( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import check
    # Different treatment of with open() and open().read().....
    file_contents = [ ['HelloWorld -', 'mychecksum md5_checksum.md5', 'ef7493baf5582b41cd4fcaa25124c3d9 ./appfile1.txt', '6fe635186392d43f0ae5728e41a92f96 appfile2.ppt', 'e10dab217514cf7f2166d87fb5d04d5c ./myapp/importantfile.bin', 'a51999cbc7ee434ee95b33f1d18c7210 otherdir/main.py', 'abc libstdc++.so', 'def myapp/libgcc_s.so.1', 'ignoreme ./other_dir/libc-2.5' ] ]
    other_file_contents = [ 'appfile1r0984u3jriumfilf42890tjr742tu', 'appfile29031i4rt498jnyfouf908j248f4298fn24iuyf', 'importf90ui4j9rf41f09j14fiun41 fp1,cmic13', 'MAIN()@KJR(*@KRE)+@MOUFRIN@FR*YB^ B* @HE)J @E( HG!V!UYNEJ)(!))))' ]
    handles = FileUtil.getMultipleReadHandles( file_contents )
    with patch('%s.os.chdir' % MODULE_NAME, new=Mock(return_value=True)) as chdir_mock, \
         patch('%s.os.path.isfile' % MODULE_NAME, new=Mock(return_value=False)) as isfile_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[True, True, False])) as exists_mock, \
         patch('%s.open' % MODULE_NAME, mock_open(), create=True) as mo:
      mysideeff = [ handles[0] ]
      for i in xrange(0, len(other_file_contents)):
        other_read_mock = Mock()
        other_read_mock.read.return_value = other_file_contents[i]
        mysideeff.append( other_read_mock )
      mo.side_effect = tuple( mysideeff )
      result = check( ('appname', 'version'), 'mytestarea398', ['/hidden/dir', 'res_from_install[1]'] )
      expected_open_calls = [ ( '/hidden/dir/md5_checksum.md5', 'r' ), '/hidden/dir/appfile1.txt' ]
      expected = [[]]
      FileUtil.checkFileInteractions( self, mo, expected_open_calls, expected, handles )
      assertDiracFailsWith( result, 'incomplete install: the file /hidden/dir/appfile2.ppt is missing', self )
      chdir_mock.assert_called_once_with( 'mytestarea398' )
      isfile_mock.assert_called_once_with( '/hidden/dir' )
      exists_mock.assert_any_call( '/hidden/dir/md5_checksum.md5' )
      exists_mock.assert_any_call( '/hidden/dir/appfile1.txt' )
      exists_mock.assert_any_call( '/hidden/dir/appfile2.ppt' )
      assertEqualsImproved( len(exists_mock.mock_calls), 3, self )

  def test_configure_slic( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import configure
    with patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/cool/dir/')), \
         patch('%s.os.chdir' % MODULE_NAME, new=Mock(return_value=True)) as chdir_mock, \
         patch('%s.removeLibc' % MODULE_NAME, new=Mock(return_value=True)) as libc_mock, \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True))as isdir_mock, \
         patch('%s.addFolderToLdLibraryPath' % MODULE_NAME, new=Mock(return_value=True)) as addfolder_mock, \
         patch.dict( os.environ, {}, True ), \
         patch('%s.os.listdir' % MODULE_NAME, new=Mock(side_effect=[ [ 'myslicpaths' ] ,  [ 'mylcddpaths' ], [ 'myxercespaths' ]])) as listdir_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)) as exists_mock:
      result = configure( ('SLIC', 'version1000'), 'configurationArea', ['ILCApplication3002'] )
      assertDiracSucceeds( result, self )
      chdir_mock.assert_called_once_with( 'configurationArea' )
      libc_mock.assert_any_call( '/cool/dir/ILCApplication3002/LDLibs' )
      libc_mock.assert_any_call( '/cool/dir/ILCApplication3002/lib' )
      isdir_mock.assert_called_once_with( '/cool/dir/ILCApplication3002/lib' )
      addfolder_mock.assert_called_once_with( '/cool/dir/ILCApplication3002/lib' )
      assertEqualsImproved( len(addfolder_mock.mock_calls), 1, self )
      listdir_mock.assert_any_call( '/cool/dir/ILCApplication3002/packages/slic/' )
      listdir_mock.assert_any_call( '/cool/dir/ILCApplication3002/packages/lcdd/' )
      listdir_mock.assert_any_call( '/cool/dir/ILCApplication3002/packages/xerces/' )
      exists_mock.assert_called_once_with( '/cool/dir/ILCApplication3002/packages/xerces/' )
      assertEqualsImproved( len(listdir_mock.mock_calls), 3, self )
      assertEqualsImproved( os.environ[ 'SLIC_DIR' ], '/cool/dir/ILCApplication3002', self )
      assertEqualsImproved( os.environ[ 'SLIC_VERSION' ], 'myslicpaths', self )
      assertEqualsImproved( os.environ[ 'XERCES_VERSION' ], 'myxercespaths', self )
      assertEqualsImproved( os.environ[ 'LCDD_VERSION' ], 'mylcddpaths', self )
      assertEqualsImproved( len(os.environ), 4, self )

  def test_configure_slic_emptydirs( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import configure
    with patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/cool/dir/')), \
         patch('%s.os.chdir' % MODULE_NAME, new=Mock(return_value=True)) as chdir_mock, \
         patch('%s.removeLibc' % MODULE_NAME, new=Mock(return_value=True)) as libc_mock, \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=False))as isdir_mock, \
         patch('%s.addFolderToLdLibraryPath' % MODULE_NAME, new=Mock(return_value=True)) as addfolder_mock, \
         patch.dict( os.environ, {}, True ), \
         patch('%s.os.listdir' % MODULE_NAME, new=Mock(return_value=( [ '' ] ))) as listdir_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=False)) as exists_mock:
      result = configure( ('SLIC', 'version1000'), 'configurationArea', ['ILCApplication3002'] )
      assertDiracSucceeds( result, self )
      chdir_mock.assert_called_once_with( 'configurationArea' )
      libc_mock.assert_called_once_with( '/cool/dir/ILCApplication3002/LDLibs' )
      isdir_mock.assert_called_once_with( '/cool/dir/ILCApplication3002/lib' )
      self.assertFalse( addfolder_mock.called )
      listdir_mock.assert_any_call( '/cool/dir/ILCApplication3002/packages/slic/' )
      listdir_mock.assert_any_call( '/cool/dir/ILCApplication3002/packages/lcdd/' )
      exists_mock.assert_called_once_with( '/cool/dir/ILCApplication3002/packages/xerces/' )
      assertEqualsImproved( len(listdir_mock.mock_calls), 2, self )
      assertEqualsImproved( os.environ[ 'SLIC_DIR' ], '/cool/dir/ILCApplication3002', self )
      assertEqualsImproved( len(os.environ), 1, self )

  def test_configure_slic_oserr( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import configure
    with patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/cool/dir/')), \
         patch('%s.os.chdir' % MODULE_NAME, new=Mock(return_value=True)) as chdir_mock, \
         patch('%s.removeLibc' % MODULE_NAME, new=Mock(return_value=True)) as libc_mock, \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True))as isdir_mock, \
         patch('%s.addFolderToLdLibraryPath' % MODULE_NAME, new=Mock(return_value=True)) as addfolder_mock, \
         patch.dict( os.environ, {}, True ), \
         patch('%s.os.listdir' % MODULE_NAME, new=Mock(side_effect=[[ 'myslicpaths' ], OSError('test_os_listdir_err')])) as listdir_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)) as exists_mock:
      result = configure( ('SLIC', 'version1000'), 'configurationArea', ['ILCApplication3002'] )
      assertDiracSucceeds( result, self ) # FIXME: OSError in configureSlic is ignored... Intentional?
      chdir_mock.assert_called_once_with( 'configurationArea' )
      libc_mock.assert_any_call( '/cool/dir/ILCApplication3002/LDLibs' )
      libc_mock.assert_any_call( '/cool/dir/ILCApplication3002/lib' )
      isdir_mock.assert_called_once_with( '/cool/dir/ILCApplication3002/lib' )
      addfolder_mock.assert_called_once_with( '/cool/dir/ILCApplication3002/lib' )
      assertEqualsImproved( len(addfolder_mock.mock_calls), 1, self )
      listdir_mock.assert_any_call( '/cool/dir/ILCApplication3002/packages/slic/' )
      listdir_mock.assert_any_call( '/cool/dir/ILCApplication3002/packages/lcdd/' )
      self.assertFalse( exists_mock.called )
      assertEqualsImproved( len(listdir_mock.mock_calls), 2, self )
      assertEqualsImproved( os.environ[ 'SLIC_DIR' ], '/cool/dir/ILCApplication3002', self )
      assertEqualsImproved( len(os.environ), 1, self )

  def test_configure_unknownapp( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import configure
    with patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/cool/dir/')), \
         patch('%s.os.chdir' % MODULE_NAME, new=Mock(return_value=True)) as chdir_mock, \
         patch('%s.removeLibc' % MODULE_NAME, new=Mock(return_value=True)) as libc_mock, \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True))as isdir_mock, \
         patch('%s.addFolderToLdLibraryPath' % MODULE_NAME, new=Mock(return_value=True)) as addfolder_mock:
      result = configure( ('myILCApplicationTesttest123', 'version1000'), 'configurationArea', ['ILCApplication3002'] )
      assertDiracSucceeds( result, self )
      chdir_mock.assert_called_once_with( 'configurationArea' )
      libc_mock.assert_any_call( '/cool/dir/ILCApplication3002/LDLibs' )
      libc_mock.assert_any_call( '/cool/dir/ILCApplication3002/lib' )
      isdir_mock.assert_called_once_with( '/cool/dir/ILCApplication3002/lib' )
      addfolder_mock.assert_called_once_with( '/cool/dir/ILCApplication3002/lib' )

  def test_configure_root( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import configure
    with patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/current/working/dir/')), \
         patch('%s.os.chdir' % MODULE_NAME, new=Mock(return_value=True)) as chdir_mock, \
         patch('%s.removeLibc' % MODULE_NAME, new=Mock(return_value=True)) as libc_mock, \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True))as isdir_mock, \
         patch('%s.addFolderToLdLibraryPath' % MODULE_NAME, new=Mock(return_value=True)) as addfolder_mock, \
         patch.dict( os.environ, { 'PATH' : 'mypath', 'PYTHONPATH' : 'pythonic_path' }, True ):
      result = configure( ('ROOT', 'v9084u'), 'configurationArea', ['myres'] )
      assertDiracSucceeds( result, self )
      chdir_mock.assert_called_once_with( 'configurationArea' )
      libc_mock.assert_any_call( '/current/working/dir/myres/LDLibs' )
      libc_mock.assert_any_call( '/current/working/dir/myres/lib' )
      isdir_mock.assert_called_once_with( '/current/working/dir/myres/lib' )
      addfolder_mock.assert_called_with( '/current/working/dir/myres/lib' )
      assertEqualsImproved( len(addfolder_mock.mock_calls), 2, self )
      assertEqualsImproved( addfolder_mock.mock_calls[0], addfolder_mock.mock_calls[1], self )
      assertEqualsImproved( os.environ['PATH'], '/current/working/dir/myres/bin:mypath', self )
      assertEqualsImproved( os.environ['PYTHONPATH'], '/current/working/dir/myres/lib:pythonic_path', self )
      assertEqualsImproved( os.environ['ROOTSYS'], '/current/working/dir/myres', self )
      assertEqualsImproved( len(os.environ), 3, self )

  def test_configure_java( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import configure
    with patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/java/')), \
         patch('%s.os.chdir' % MODULE_NAME, new=Mock(return_value=True)) as chdir_mock, \
         patch('%s.removeLibc' % MODULE_NAME, new=Mock(return_value=True)) as libc_mock, \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True))as isdir_mock, \
         patch('%s.addFolderToLdLibraryPath' % MODULE_NAME, new=Mock(return_value=True)) as addfolder_mock, \
         patch.dict( os.environ, { 'PATH' : 'mypath' }, True ):
      result = configure( ('Java', 'v9084u'), 'configurationArea', ['res'] )
      assertDiracSucceeds( result, self )
      chdir_mock.assert_called_once_with( 'configurationArea' )
      libc_mock.assert_any_call( '/java/res/LDLibs' )
      libc_mock.assert_any_call( '/java/res/lib' )
      isdir_mock.assert_called_once_with( '/java/res/lib' )
      addfolder_mock.assert_called_with( '/java/res/lib' )
      assertEqualsImproved( len( addfolder_mock.mock_calls ), 2, self )
      assertEqualsImproved( addfolder_mock.mock_calls[0], addfolder_mock.mock_calls[1], self )
      assertEqualsImproved( os.environ['PATH'], '/java/res/bin:mypath', self )
      assertEqualsImproved( len(os.environ), 1, self )

  def test_configure_lcio( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import configure
    with patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/my/lcio/')), \
         patch('%s.os.chdir' % MODULE_NAME, new=Mock(return_value=True)) as chdir_mock, \
         patch('%s.removeLibc' % MODULE_NAME, new=Mock(return_value=True)) as libc_mock, \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True))as isdir_mock, \
         patch('%s.addFolderToLdLibraryPath' % MODULE_NAME, new=Mock(return_value=True)) as addfolder_mock, \
         patch.dict( os.environ, { 'PATH' : 'PaTH_LCIO' }, True ), \
         patch('%s.subprocess.check_call' % MODULE_NAME, new=Mock(return_value=0)) as subproc_mock:
      result = configure( ('LCIO', 'v9084u'), 'configurationArea', ['retval'] )
      assertDiracSucceeds( result, self )
      chdir_mock.assert_called_once_with( 'configurationArea' )
      libc_mock.assert_any_call( '/my/lcio/retval/LDLibs' )
      libc_mock.assert_any_call( '/my/lcio/retval/lib' )
      isdir_mock.assert_called_once_with( '/my/lcio/retval/lib' )
      addfolder_mock.assert_called_once_with( '/my/lcio/retval/lib' )
      subproc_mock.assert_called_once_with( ['java', '-Xmx1536m', '-Xms256m', '-version'] )
      assertEqualsImproved( len(os.environ), 2, self )
      assertEqualsImproved( os.environ['LCIO'], '/my/lcio/retval', self )
      assertEqualsImproved( os.environ['PATH'], '/my/lcio/retval/bin:PaTH_LCIO', self )

  def test_configure_lcio_javafails( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import configure
    with patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/cool/dir/')), \
         patch('%s.os.chdir' % MODULE_NAME, new=Mock(return_value=True)) as chdir_mock, \
         patch('%s.removeLibc' % MODULE_NAME, new=Mock(return_value=True)) as libc_mock, \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True))as isdir_mock, \
         patch('%s.addFolderToLdLibraryPath' % MODULE_NAME, new=Mock(return_value=True)) as addfolder_mock, \
         patch('%s.subprocess.check_call' % MODULE_NAME, new=Mock(return_value=1)) as subproc_mock:
      result = configure( ('LCIO', 'version1000'), 'configurationArea', ['ILCApplication3002'] )
      assertDiracFailsWith( result, 'something is wrong with java', self )
      chdir_mock.assert_called_once_with( 'configurationArea' )
      libc_mock.assert_any_call( '/cool/dir/ILCApplication3002/LDLibs' )
      libc_mock.assert_any_call( '/cool/dir/ILCApplication3002/lib' )
      isdir_mock.assert_called_once_with( '/cool/dir/ILCApplication3002/lib' )
      addfolder_mock.assert_called_once_with( '/cool/dir/ILCApplication3002/lib' )
      assertEqualsImproved( len(addfolder_mock.mock_calls), 1, self )
      subproc_mock.assert_called_once_with( ['java', '-Xmx1536m', '-Xms256m', '-version'] )

  def test_configure_lcsim( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import configure
    with patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/cool/dir/')), \
         patch('%s.os.chdir' % MODULE_NAME, new=Mock(return_value=True)) as chdir_mock, \
         patch('%s.removeLibc' % MODULE_NAME, new=Mock(return_value=True)) as libc_mock, \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True))as isdir_mock, \
         patch('%s.addFolderToLdLibraryPath' % MODULE_NAME, new=Mock(return_value=True)) as addfolder_mock, \
         patch('%s.subprocess.check_call' % MODULE_NAME, new=Mock(return_value=1)) as subproc_mock:
      result = configure( ('LCSIM', 'v9084u'), 'configurationArea', ['ILCApplication3002'] )
      assertDiracFailsWith( result, 'something is wrong with java', self )
      chdir_mock.assert_called_once_with( 'configurationArea' )
      libc_mock.assert_any_call( '/cool/dir/ILCApplication3002/LDLibs' )
      libc_mock.assert_any_call( '/cool/dir/ILCApplication3002/lib' )
      isdir_mock.assert_called_once_with( '/cool/dir/ILCApplication3002/lib' )
      addfolder_mock.assert_called_once_with( '/cool/dir/ILCApplication3002/lib' )
      subproc_mock.assert_called_once_with( ['java', '-Xmx1536m', '-Xms256m', '-version'] )

  def test_configure_stdhepcutjava( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import configure
    import subprocess
    with patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/cool/dir/')), \
         patch('%s.os.chdir' % MODULE_NAME, new=Mock(return_value=True)) as chdir_mock, \
         patch('%s.removeLibc' % MODULE_NAME, new=Mock(return_value=True)) as libc_mock, \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)) as isdir_mock, \
         patch('%s.addFolderToLdLibraryPath' % MODULE_NAME, new=Mock(return_value=True)) as addfolder_mock, \
         patch('%s.subprocess.check_call' % MODULE_NAME, new=Mock(return_value=0)) as subproc_mock:
      result = configure( ('StdHEPCutJava', 'v9084u'), 'configurationArea', ['ILCApplication3002'] )
      assertDiracSucceeds( result, self )
      chdir_mock.assert_called_once_with( 'configurationArea' )
      libc_mock.assert_any_call( '/cool/dir/ILCApplication3002/LDLibs' )
      libc_mock.assert_any_call( '/cool/dir/ILCApplication3002/lib' )
      isdir_mock.assert_called_once_with( '/cool/dir/ILCApplication3002/lib' )
      addfolder_mock.assert_called_once_with( '/cool/dir/ILCApplication3002/lib' )
      subproc_mock.assert_called_once_with( ['java', '-Xmx1536m', '-Xms256m', '-version'] )

  def test_configure_stdhepcutjava_fails( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import configure
    import subprocess
    with patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/cool/dir/')), \
         patch('%s.os.chdir' % MODULE_NAME, new=Mock(return_value=True)) as chdir_mock, \
         patch('%s.removeLibc' % MODULE_NAME, new=Mock(return_value=True)) as libc_mock, \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True))as isdir_mock, \
         patch('%s.addFolderToLdLibraryPath' % MODULE_NAME, new=Mock(return_value=True)) as addfolder_mock, \
         patch('%s.subprocess.check_call' % MODULE_NAME, new=Mock(side_effect=subprocess.CalledProcessError( 1, 'testcmd' ))) as subproc_mock:
      result = configure( ('StdHEPCutJava', 'v9084u'), 'configurationArea', ['ILCApplication3002'] )
      assertDiracFailsWith( result, 'java was not found on this machine, cannot proceed', self )
      chdir_mock.assert_called_once_with( 'configurationArea' )
      libc_mock.assert_any_call( '/cool/dir/ILCApplication3002/LDLibs' )
      libc_mock.assert_any_call( '/cool/dir/ILCApplication3002/lib' )
      isdir_mock.assert_called_once_with( '/cool/dir/ILCApplication3002/lib' )
      addfolder_mock.assert_called_once_with( '/cool/dir/ILCApplication3002/lib' )
      subproc_mock.assert_called_once_with( ['java', '-Xmx1536m', '-Xms256m', '-version'] )

  def test_clean( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import clean
    with patch('%s.os.chdir' % MODULE_NAME) as chdir_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)) as exists_mock, \
         patch('%s.os.unlink' % MODULE_NAME, new=Mock(return_value=True)) as remove_mock, \
         patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/current/dir/my')):
      result = clean( 'mytestarea51', [ 'res_from_check[0]', 'myapptarball.tgz' ] )
      assertDiracSucceeds( result, self )
      chdir_mock.assert_called_once_with( 'mytestarea51' )
      exists_mock.assert_called_once_with( '/current/dir/my/myapptarball.tgz' )
      remove_mock.assert_called_once_with( 'myapptarball.tgz' )

  def test_clean_fails( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import clean
    with patch('%s.os.chdir' % MODULE_NAME) as chdir_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)) as exists_mock, \
         patch('%s.os.unlink' % MODULE_NAME, new=Mock(side_effect=OSError('_unlink_some_err'))) as remove_mock, \
         patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/current/dir/my')), \
         patch('%s.gLogger.error' % MODULE_NAME) as err_mock:
      result = clean( 'mytestarea51', [ 'res_from_check[0]', 'myapptarball.tgz' ] )
      assertDiracSucceeds( result, self )
      chdir_mock.assert_called_once_with( 'mytestarea51' )
      exists_mock.assert_called_once_with( '/current/dir/my/myapptarball.tgz' )
      remove_mock.assert_called_once_with( 'myapptarball.tgz' )
      err_mock.assert_called_once_with( 'Could not remove tar ball:', '_unlink_some_err' )

  def test_clean_doesnt_exist( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import clean
    with patch('%s.os.chdir' % MODULE_NAME) as chdir_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=False)) as exists_mock, \
         patch('%s.os.unlink' % MODULE_NAME) as remove_mock, \
         patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/current/dir/my')), \
         patch('%s.gLogger.error' % MODULE_NAME) as err_mock:
      result = clean( 'mytestarea51', [ 'res_from_check[0]', 'myapptarball.tgz' ] )
      assertDiracSucceeds( result, self )
      chdir_mock.assert_called_once_with( 'mytestarea51' )
      exists_mock.assert_called_once_with( '/current/dir/my/myapptarball.tgz' )
      self.assertFalse( remove_mock.called )
      self.assertFalse( err_mock.called )

  def test_clean_dont_delete_jar( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import clean
    with patch('%s.os.chdir' % MODULE_NAME) as chdir_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)) as exists_mock, \
         patch('%s.os.unlink' % MODULE_NAME, new=Mock(return_value=True)) as remove_mock, \
         patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/current/dir/my')), \
         patch('%s.gLogger.error' % MODULE_NAME) as err_mock:
      result = clean( 'mytestarea51', [ 'res_from_check[0]', 'myappjarcontainer.jar' ] )
      assertDiracSucceeds( result, self )
      chdir_mock.assert_called_once_with( 'mytestarea51' )
      exists_mock.assert_called_once_with( '/current/dir/my/myappjarcontainer.jar' )
      self.assertFalse( remove_mock.called )
      self.assertFalse( err_mock.called )

  def test_addfolder_to_ldlibrary( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import addFolderToLdLibraryPath
    with patch.dict(os.environ, {}, True):
      addFolderToLdLibraryPath( 'mytestfolder123' )
      assertEqualsImproved( len(os.environ), 1, self )
      assertEqualsImproved( os.environ['LD_LIBRARY_PATH'], 'mytestfolder123', self )

  def test_addfolder_to_ldlibrary_append( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import addFolderToLdLibraryPath
    with patch.dict(os.environ, { 'LD_LIBRARY_PATH' : 'prepath' }, True):
      addFolderToLdLibraryPath( 'other/TestFoldeR' )
      assertEqualsImproved( len(os.environ), 1, self )
      assertEqualsImproved( os.environ['LD_LIBRARY_PATH'], 'other/TestFoldeR:prepath', self )

  def test_install_package( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import installPackage
    with patch('%s.getTarBallLocation' % MODULE_NAME, new=Mock(return_value=S_OK(('APP_TAR', 'http://myTar.url', False, '1234MyMd5')))) as get_mock, \
         patch('%s.os.chdir' % MODULE_NAME) as chdir_mock, \
         patch('%s.install' % MODULE_NAME, new=Mock(return_value=S_OK('res_from_install_test'))) as install_mock, \
         patch('%s.check' % MODULE_NAME, new=Mock(return_value=S_OK('res_from_check_test'))) as check_mock, \
         patch('%s.configure' % MODULE_NAME, new=Mock(return_value=S_OK())) as configure_mock, \
         patch('%s.clean' % MODULE_NAME, new=Mock(return_value=S_OK())) as clean_mock, \
         patch('%s.gLogger.error' % MODULE_NAME) as log_mock:
      result = installPackage(  ('testAppLication', 'version1.01.'), 'configToTest', 'secret_area', 'mycurrenttestdir' )
      assertDiracSucceeds( result, self )
      # change dir to current dir after every step
      chdir_mock.assert_called_with( 'mycurrenttestdir' )
      assertEqualsImproved( len(chdir_mock.mock_calls), 5, self )
      for i in xrange(0, len(chdir_mock.mock_calls) - 1):
        assertEqualsImproved( chdir_mock.mock_calls[i], chdir_mock.mock_calls[i+1], self )
      get_mock.assert_called_once_with( ('testAppLication', 'version1.01.'), 'configToTest', 'secret_area' )
      install_mock.assert_called_once_with( ('testAppLication', 'version1.01.'), 'APP_TAR', 'http://myTar.url', False, '1234MyMd5', 'secret_area' )
      check_mock.assert_called_once_with( ('testAppLication', 'version1.01.'), 'secret_area', 'res_from_install_test')
      configure_mock.assert_called_once_with( ('testAppLication', 'version1.01.'), 'secret_area', 'res_from_check_test' )
      clean_mock.assert_called_once_with( 'secret_area', 'res_from_install_test' )
      self.assertFalse( log_mock.called )

  def test_install_package_gettar_fails( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import installPackage
    with patch('%s.getTarBallLocation' % MODULE_NAME, new=Mock(return_value=S_ERROR('tarball_err'))), \
         patch('%s.gLogger.error' % MODULE_NAME) as log_mock:
      result = installPackage( ('mytestappName', 'testv12'), 'configToTest', 'teAreast', 'mycurrenttestdir' )
      assertDiracFailsWith( result, 'failed to install software', self )
      log_mock.assert_called_once_with( 'Could not install software/dependency mytestappName testv12: tarball_err')

  def test_install_package_install_fails( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import installPackage
    with patch('%s.getTarBallLocation' % MODULE_NAME, new=Mock(return_value=S_OK(('app_tar', 'tarballURL', False, 'md5sum')))), \
         patch('%s.os.chdir' % MODULE_NAME), \
         patch('%s.install' % MODULE_NAME, new=Mock(return_value=S_ERROR('install_test_err'))), \
         patch('%s.gLogger.error' % MODULE_NAME) as log_mock:
      result = installPackage( ('mytestappName', 'testv12'), 'configToTest', 'teAreast', 'mycurrenttestdir' )
      assertDiracFailsWith( result, 'failed to install software', self )
      log_mock.assert_called_once_with( 'Could not install software/dependency mytestappName testv12: install_test_err')

  def test_install_package_check_fails( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import installPackage
    with patch('%s.getTarBallLocation' % MODULE_NAME, new=Mock(return_value=S_OK(('app_tar', 'tarballURL', False, 'md5sum')))), \
         patch('%s.os.chdir' % MODULE_NAME), \
         patch('%s.install' % MODULE_NAME, new=Mock(return_value=S_OK('res_from_install'))), \
         patch('%s.check' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_check_error_test'))), \
         patch('%s.gLogger.error' % MODULE_NAME) as log_mock:
      result = installPackage( ('mytestappName', 'testv12'), 'configToTest', 'teAreast', 'mycurrenttestdir' )
      assertDiracFailsWith( result, 'failed to check integrity of software', self )
      log_mock.assert_called_with( 'Failed to check software/dependency mytestappName testv12')

  def test_install_package_configure_fails( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import installPackage
    with patch('%s.getTarBallLocation' % MODULE_NAME, new=Mock(return_value=S_OK(('app_tar', 'tarballURL', False, 'md5sum')))), \
         patch('%s.os.chdir' % MODULE_NAME), \
         patch('%s.install' % MODULE_NAME, new=Mock(return_value=S_OK('res_from_install'))), \
         patch('%s.check' % MODULE_NAME, new=Mock(return_value=S_OK('mycheckresult_test'))), \
         patch('%s.configure' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_config_err_test'))), \
         patch('%s.clean' % MODULE_NAME, new=Mock(return_value=S_OK())), \
         patch('%s.gLogger.error' % MODULE_NAME) as log_mock:
      result = installPackage( ('mytestappName', 'testv12'), 'configToTest', 'teAreast', 'mycurrenttestdir' )
      assertDiracFailsWith( result, 'failed to configure software', self )
      log_mock.assert_called_once_with( 'Failed to configure software/dependency mytestappName testv12')

  def test_install_package_clean_fails( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import installPackage
    with patch('%s.getTarBallLocation' % MODULE_NAME, new=Mock(return_value=S_OK(('app_tar', 'tarballURL', False, 'md5sum')))), \
         patch('%s.os.chdir' % MODULE_NAME), \
         patch('%s.install' % MODULE_NAME, new=Mock(return_value=S_OK('res_from_install'))), \
         patch('%s.check' % MODULE_NAME, new=Mock(return_value=S_OK())), \
         patch('%s.configure' % MODULE_NAME, new=Mock(return_value=S_OK())), \
         patch('%s.clean' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_clean_err_test'))), \
         patch('%s.gLogger.error' % MODULE_NAME) as log_mock:
      result = installPackage( ('mytestappName', 'testv12'), 'configToTest', 'teAreast', 'mycurrenttestdir' )
      assertDiracSucceeds( result, self )
      log_mock.assert_called_once_with( 'Failed to clean useless tar balls, deal with it: mytestappName testv12' )

  def test_install( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import install
    extract_mock = Mock()
    extract_mock.getmembers.return_value =  ['member0/file1.txt', 'member1']
    with patch('%s.os.chdir' % MODULE_NAME) as chdir_mock, \
         patch('%s.checkLockAge' % MODULE_NAME, new=Mock(return_value=S_OK(True))) as checklock_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[False, True])) as exists_mock, \
         patch('%s.createLock' % MODULE_NAME, new=Mock(return_value=S_OK())) as createlock_mock, \
         patch('%s.deleteOld' % MODULE_NAME, new=Mock(return_value=S_OK())) as deleteold_mock, \
         patch('%s.downloadFile' % MODULE_NAME, new=Mock(return_value=S_OK())) as download_mock, \
         patch('%s.clearLock' % MODULE_NAME, new=Mock(return_value=S_OK())) as clearlock_mock, \
         patch('%s.tarMd5Check' % MODULE_NAME, new=Mock(return_value=S_OK())) as md5check_mock, \
         patch('%s.tarfile.is_tarfile' % MODULE_NAME, new=Mock(return_value=False)) as tarcheck_mock, \
         patch('%s.os.listdir' % MODULE_NAME, new=Mock(side_effect=OSError(''))) as listdir_mock, \
         patch('%s.os.rename' % MODULE_NAME) as rename_mock, \
         patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/my/cwd/test')), \
         patch('%s.os.unlink' % MODULE_NAME) as remove_mock:
      result = install(('appname', 'appvers'), 'myapptarbase.tar.gz', 'tarballURL', False, 'Mymd5sumTest', 'mytestArea')
      assertDiracSucceedsWith_equals( result, ['myapptarbase', 'myapptarbase.tar.gz'], self )
      clearlock_mock.assert_called_once_with( 'myapptarbase.lock' )
      listdir_mock.assert_called_once_with( 'myapptarbase' )
      self.assertFalse( rename_mock.called )
      self.assertFalse( remove_mock.called )
      self.assertFalse( deleteold_mock.called )
      createlock_mock.assert_called_once_with( 'myapptarbase.lock' )
      download_mock.assert_called_once_with( 'tarballURL', 'myapptarbase.tar.gz', 'myapptarbase' )
      tarcheck_mock.assert_called_once_with( 'myapptarbase.tar.gz' )
      checklock_mock.assert_called_once_with( 'myapptarbase.lock' )
      chdir_mock.assert_called_once_with( 'mytestArea' )
      md5check_mock.assert_called_once_with( 'myapptarbase.tar.gz', 'Mymd5sumTest' )
      exists_mock.assert_any_call( 'myapptarbase' )
      exists_mock.assert_called_with( '/my/cwd/test/myapptarbase.tar.gz' )

  def test_install_checklockage_fails( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import install
    with patch('%s.os.chdir' % MODULE_NAME), \
         patch('%s.checkLockAge' % MODULE_NAME, new=Mock(return_value=S_ERROR('test_checklockage_failed'))):
      result = install(('appname', 'appvers'), 'app_tar.tar.gz', 'tarballURL', True, 'md5sum', 'area')
      assertDiracFailsWith( result, 'failed lock checks', self )

  def test_install_createlock_fails( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import install
    with patch('%s.os.chdir' % MODULE_NAME), \
         patch('%s.checkLockAge' % MODULE_NAME, new=Mock(return_value=S_OK(True))), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[False, True])), \
         patch('%s.createLock' % MODULE_NAME, new=Mock(return_value=S_ERROR('test_createlock_failed_err'))):
      result = install(('appname', 'appvers'), 'app_tar.tar.gz', 'tarballURL', True, 'md5sum', 'area')
      assertDiracFailsWith( result, 'test_createlock_failed_err', self )

  def test_install_already_installed( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import install
    with patch('%s.os.chdir' % MODULE_NAME), \
         patch('%s.checkLockAge' % MODULE_NAME, new=Mock(return_value=S_OK(False))), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)):
      result = install(('appname', 'appvers'), 'app_tar.tar.gz', 'tarballURL', False, 'md5sum', 'area')
      assertDiracSucceedsWith_equals( result, ['app_tar', 'app_tar.tar.gz'], self )

  def test_install_delete_old_fails( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import install
    with patch('%s.os.chdir' % MODULE_NAME), \
         patch('%s.checkLockAge' % MODULE_NAME, new=Mock(return_value=S_OK(False))), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.createLock' % MODULE_NAME, new=Mock(return_value=S_OK())), \
         patch('%s.deleteOld' % MODULE_NAME, new=Mock(return_value=S_ERROR('test_deleteold_file_error'))), \
         patch('%s.clearLock' % MODULE_NAME, new=Mock(return_value=True)) as clearlock_mock:
      result = install(('appname', 'appvers'), 'app_tar.tar.gz', 'tarballURL', True, 'md5sum', 'area')
      assertDiracFailsWith( result, 'test_deleteold_file_error', self )
      clearlock_mock.assert_called_once_with( 'app_tar.lock' )

  def test_install_download_fails( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import install
    with patch('%s.os.chdir' % MODULE_NAME), \
         patch('%s.checkLockAge' % MODULE_NAME, new=Mock(return_value=S_OK(False))), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=False)), \
         patch('%s.createLock' % MODULE_NAME, new=Mock(return_value=S_OK())), \
         patch('%s.downloadFile' % MODULE_NAME, new=Mock(return_value=S_ERROR('test_download_file_error'))), \
         patch('%s.clearLock' % MODULE_NAME, new=Mock(return_value=True)) as clearlock_mock:
      result = install(('appname', 'appvers'), 'app_tar.tar.gz', 'tarballURL', False, 'md5sum', 'area')
      assertDiracFailsWith( result, 'test_download_file_error', self )
      clearlock_mock.assert_called_once_with( 'app_tar.lock' )

  def test_install_downloaded_file_doesnt_exist( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import install
    with patch('%s.os.chdir' % MODULE_NAME), \
         patch('%s.checkLockAge' % MODULE_NAME, new=Mock(return_value=S_OK(False))), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=False)) as exists_mock, \
         patch('%s.createLock' % MODULE_NAME, new=Mock(return_value=S_OK())), \
         patch('%s.downloadFile' % MODULE_NAME, new=Mock(return_value=S_OK(True))), \
         patch('%s.clearLock' % MODULE_NAME, new=Mock(return_value=True)) as clearlock_mock, \
         patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/my/cwd/test')):
      result = install(('appname', 'appvers'), 'app_tar.tar.gz', 'tarballURL', False, 'md5sum', 'area')
      assertDiracFailsWith( result, 'failed to download software', self )
      clearlock_mock.assert_called_once_with( 'app_tar.lock' )
      exists_mock.assert_any_call( '/my/cwd/test/app_tar.tar.gz')

  def test_install_md5check_cannot_remove_old_file( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import install
    with patch('%s.os.chdir' % MODULE_NAME), \
         patch('%s.checkLockAge' % MODULE_NAME, new=Mock(return_value=S_OK(False))), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[False, True])), \
         patch('%s.createLock' % MODULE_NAME, new=Mock(return_value=S_OK())), \
         patch('%s.downloadFile' % MODULE_NAME, new=Mock(return_value=S_OK(True))), \
         patch('%s.clearLock' % MODULE_NAME, new=Mock(return_value=True)) as clearlock_mock, \
         patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/my/cwd/test')), \
         patch('%s.tarMd5Check' % MODULE_NAME, new=Mock(return_value=S_ERROR('test_md5check_fails'))), \
         patch('%s.deleteOld' % MODULE_NAME, new=Mock(return_value=S_ERROR('deleteold_test_error'))), \
         patch('%s.os.unlink' % MODULE_NAME, new=Mock(side_effect=OSError('my_os_err_test'))), \
         patch('%s.gLogger.error' % MODULE_NAME, new=Mock(return_value=True)) as log_mock:
      result = install(('appname', 'appvers'), 'app_tar.tar.gz', 'tarballURL', False, 'md5sum', 'area')
      assertDiracFailsWith( result, 'deleteold_test_error', self )
      log_mock.assert_called_with('Failed to clean tar ball, something bad is happening')
      clearlock_mock.assert_called_once_with( 'app_tar.lock' )

  def test_install_md5check_download_file_fails( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import install
    with patch('%s.os.chdir' % MODULE_NAME), \
         patch('%s.checkLockAge' % MODULE_NAME, new=Mock(return_value=S_OK(False))), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[False, True])), \
         patch('%s.createLock' % MODULE_NAME, new=Mock(return_value=S_OK())), \
         patch('%s.downloadFile' % MODULE_NAME, new=Mock(side_effect=[S_OK(True), S_ERROR('download_file_test_err')])), \
         patch('%s.clearLock' % MODULE_NAME, new=Mock(return_value=True)) as clearlock_mock, \
         patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/my/cwd/test')), \
         patch('%s.tarMd5Check' % MODULE_NAME, new=Mock(return_value=S_ERROR('test_md5check_fails'))), \
         patch('%s.deleteOld' % MODULE_NAME, new=Mock(return_value=S_OK(True))), \
         patch('%s.os.unlink' % MODULE_NAME):
      result = install(('appname', 'appvers'), 'app_tar.tar.gz', 'tarballURL', False, 'md5sum', 'area')
      assertDiracFailsWith( result, 'download_file_test_err', self )
      clearlock_mock.assert_called_once_with( 'app_tar.lock' )

  def test_install_second_md5check_fails( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import install
    with patch('%s.os.chdir' % MODULE_NAME), \
         patch('%s.checkLockAge' % MODULE_NAME, new=Mock(return_value=S_OK(False))), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[False, True])), \
         patch('%s.createLock' % MODULE_NAME, new=Mock(return_value=S_OK())), \
         patch('%s.downloadFile' % MODULE_NAME, new=Mock(return_value=S_OK(True))), \
         patch('%s.clearLock' % MODULE_NAME, new=Mock(return_value=True)) as clearlock_mock, \
         patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/my/cwd/test')), \
         patch('%s.tarMd5Check' % MODULE_NAME, new=Mock(return_value=S_ERROR('test_md5check_fails'))), \
         patch('%s.deleteOld' % MODULE_NAME, new=Mock(return_value=S_OK(True))), \
         patch('%s.os.unlink' % MODULE_NAME):
      result = install(('appname', 'appvers'), 'app_tar.tar.gz', 'tarballURL', False, 'md5sum', 'area')
      assertDiracFailsWith( result, 'md5 check failed', self )
      clearlock_mock.assert_called_once_with( 'app_tar.lock' )

  def test_install_tarfile_extract_fails( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import install
    from tarfile import TarError
    untar_me_mock = Mock()
    untar_me_mock.extractall.side_effect=TarError('custom_tar_test_err')
    with patch('%s.os.chdir' % MODULE_NAME), \
         patch('%s.checkLockAge' % MODULE_NAME, new=Mock(return_value=S_OK(False))), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[False, True])), \
         patch('%s.createLock' % MODULE_NAME, new=Mock(return_value=S_OK())), \
         patch('%s.downloadFile' % MODULE_NAME, new=Mock(return_value=S_OK(True))), \
         patch('%s.clearLock' % MODULE_NAME, new=Mock(return_value=True)) as clearlock_mock, \
         patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/my/cwd/test')), \
         patch('%s.tarMd5Check' % MODULE_NAME, new=Mock(return_value=S_OK(True))), \
         patch('%s.deleteOld' % MODULE_NAME, new=Mock(return_value=S_OK(True))), \
         patch('%s.os.unlink' % MODULE_NAME), \
         patch('%s.tarfile.is_tarfile' % MODULE_NAME, new=Mock(return_value=True)) as istar_mock, \
         patch('%s.tarfile.open' % MODULE_NAME, new=Mock(return_value=untar_me_mock)) as tar_open_mock:
      result = install(('appname', 'appvers'), 'app_tar.tar.gz', 'tarballURL', False, 'md5sum', 'area')
      assertDiracFailsWith( result, 'Could not extract tar ball app_tar.tar.gz because of custom_tar_test_err, cannot continue ', self )
      clearlock_mock.assert_called_once_with( 'app_tar.lock' )
      istar_mock.assert_called_once_with( 'app_tar.tar.gz' )
      tar_open_mock.assert_called_once_with( 'app_tar.tar.gz' )

  def test_install_tarfile_isslic_rename_fails( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import install
    untar_member0 = Mock()
    untar_member0.name = 'member0/file1.txt'
    untar_me_mock = Mock()
    untar_me_mock.getmembers.return_value = [untar_member0, 'member1']
    with patch('%s.os.chdir' % MODULE_NAME), \
         patch('%s.checkLockAge' % MODULE_NAME, new=Mock(return_value=S_OK(False))), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[False, True])), \
         patch('%s.createLock' % MODULE_NAME, new=Mock(return_value=S_OK())), \
         patch('%s.downloadFile' % MODULE_NAME, new=Mock(return_value=S_OK(True))), \
         patch('%s.clearLock' % MODULE_NAME, new=Mock(return_value=True)) as clearlock_mock, \
         patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/my/cwd/test')), \
         patch('%s.tarMd5Check' % MODULE_NAME, new=Mock(return_value=S_OK(True))), \
         patch('%s.deleteOld' % MODULE_NAME, new=Mock(return_value=S_OK(True))), \
         patch('%s.os.unlink' % MODULE_NAME), \
         patch('%s.tarfile.is_tarfile' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.tarfile.open' % MODULE_NAME, new=Mock(return_value=untar_me_mock)), \
         patch('%s.os.rename' % MODULE_NAME, new=Mock(side_effect=OSError('test_cannot_rename_err'))) as rename_mock:
      result = install(('appname', 'appvers'), 'slic.tar.gz', 'tarballURL', False, 'md5sum', 'area')
      assertDiracFailsWith( result, 'Could not rename slic directory', self )
      clearlock_mock.assert_called_once_with( 'slic.lock' )
      rename_mock.assert_called_once_with( 'member0', 'slic' )
      self.assertTrue( untar_me_mock.extractall.called )

  def test_install_tarfile_isslic_listdir_empty( self ):
    from ILCDIRAC.Core.Utilities.TARsoft import install
    untar_member0 = Mock()
    untar_member0.name = 'member0/file1.txt'
    untar_me_mock = Mock()
    untar_me_mock.getmembers.return_value = [untar_member0, 'member1']
    with patch('%s.os.chdir' % MODULE_NAME), \
         patch('%s.checkLockAge' % MODULE_NAME, new=Mock(return_value=S_OK(False))), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[False, True])), \
         patch('%s.createLock' % MODULE_NAME, new=Mock(return_value=S_OK())), \
         patch('%s.downloadFile' % MODULE_NAME, new=Mock(return_value=S_OK(True))), \
         patch('%s.clearLock' % MODULE_NAME, new=Mock(return_value=True)) as clearlock_mock, \
         patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/my/cwd/test')), \
         patch('%s.tarMd5Check' % MODULE_NAME, new=Mock(return_value=S_OK(True))), \
         patch('%s.deleteOld' % MODULE_NAME, new=Mock(return_value=S_OK(True))), \
         patch('%s.os.unlink' % MODULE_NAME), \
         patch('%s.tarfile.is_tarfile' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.tarfile.open' % MODULE_NAME, new=Mock(return_value=untar_me_mock)), \
         patch('%s.os.rename' % MODULE_NAME, new=Mock(return_value=True)) as rename_mock, \
         patch('%s.os.listdir' % MODULE_NAME, new=Mock(return_value=[])) as listdir_mock:
      result = install(('appname', 'appvers'), 'slic.tar.gz', 'tarballURL', False, 'md5sum', 'area')
      assertDiracFailsWith( result, 'folder ', self )
      clearlock_mock.assert_called_once_with( 'slic.lock' )
      rename_mock.assert_called_once_with( 'member0', 'slic' )
      self.assertTrue( untar_me_mock.extractall.called )
      listdir_mock.assert_called_once_with( 'slic' )

MODULE_NAME = 'ILCDIRAC.Core.Utilities.TARsoft'



