#!/usr/bin/env python
"""Test the ProcessList module"""

import unittest
from mock import patch, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Core.Utilities.ProcessList import ProcessList
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertListContentEquals, \
  assertDiracFailsWith, assertDiracSucceeds, assertDiracSucceedsWith_equals, assertMockCalls

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Core.Utilities.ProcessList'

STD_PROC_DICT = { 'TarBallCSPath' : '/test/cs/path/ball.tar', 'Detail' : 'TestNoDetails',
                  'Generator' : 'mytestGen21', 'Model' : 'testmodel3001', 'Restrictions' : '',
                  'InFile' : 'my/file.in' }


#pylint: disable=protected-access
class ProcessListSimpleTestCase( unittest.TestCase ):
  """ Test the simple methods of the class that dont need a sane CFG
  """

  def setUp( self ):
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=False)):
      self.prol = ProcessList( 'myTestProcess.list' )

  def test_constructor( self ):
    import DIRAC
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)), \
         patch.object(DIRAC.Core.Utilities.CFG.CFG, 'loadFromFile', return_value=True):
      empty_process_list = ProcessList( 'existent_location' )
      self.assertTrue( empty_process_list.cfg.existsKey('Processes') )
      self.assertTrue( empty_process_list.isOK() )

    def replace_load( self, _ ): #pylint: disable=missing-docstring
      self.createNewSection( 'myTestSection', 'testComment' )
      self.createNewSection( 'Processes', 'testProcesses' )

    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)), \
         patch.object(DIRAC.Core.Utilities.CFG.CFG, 'loadFromFile', side_effect=replace_load, autospec=True):
      other_process_list = ProcessList( 'existent_location' )
      self.assertTrue( other_process_list.cfg.existsKey('Processes') )
    self.assertFalse( self.prol.isOK() )

  def test_addentry( self ):
    self.prol.cfg.createNewSection( 'Processes' )
    self.prol.cfg.createNewSection( 'Processes/123' )
    self.prol._addEntry( '123', STD_PROC_DICT )

class ProcessListComplexTestCase( unittest.TestCase ):
  """ Test the different methods of the class, providing a usable CFG
  """

  def setUp( self ):
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=False)):
      self.prol = ProcessList( 'myTestProcess.list' )
    self.prol.cfg.createNewSection( 'Processes' )

  def test_updateproclist_and_getters( self ):
    self.prol.cfg.createNewSection( 'Processes/myTestProcDeleteMe' )
    self.prol.cfg.setOption( 'Processes/myTestProcDeleteMe/someRandomOption', True )
    dict_1 = { 'CrossSection' : 'someCross' }
    dict_1.update( STD_PROC_DICT )
    dict_2 = { 'CrossSection' : 'some_other_val' }
    dict_2.update( STD_PROC_DICT )
    process_dict = {}
    process_dict[ 'MytestProcess' ] = dict_1
    process_dict[ 'myTestProcDeleteMe' ] = dict_2
    result = self.prol.updateProcessList( process_dict )
    assertDiracSucceeds( result, self )
    conf = self.prol.cfg
    self.assertFalse( conf.existsKey( 'Processes/myTestProcDeleteMe/someRandomOption' ) )
    options = [ 'Processes/MytestProcess/CrossSection', 'Processes/myTestProcDeleteMe/CrossSection' ]
    assertEqualsImproved( ( map( conf.getOption, options ) ), ( [ 'someCross', 'some_other_val' ] ), self )
    assertEqualsImproved( ( self.prol.getCSPath( 'myTestProcDeleteMe' ),
                            self.prol.getInFile( 'myTestProcDeleteMe' ),
                            self.prol.existsProcess( 'myTestProcDeleteMe' ),
                            self.prol.existsProcess( '' ), self.prol.existsProcess( 'invalidProcess' ),
                            self.prol.existsProcess( 'myTestProcDeleteMeToo' ) ),
                          ( '/test/cs/path/ball.tar', 'my/file.in', S_OK(True), S_OK(True), S_OK(False),
                            S_OK(False) ), self )
    assertListContentEquals( self.prol.getProcesses(), [ 'myTestProcDeleteMe', 'MytestProcess' ], self )
    all_processes_dict = self.prol.getProcessesDict()
    assertEqualsImproved( len(all_processes_dict), 2, self )
    assertEqualsImproved( ('myTestProcDeleteMe' in all_processes_dict, 'MytestProcess' in all_processes_dict),
                          ( True, True ), self )
    self.prol.printProcesses()

  def test_writeproclist( self ):
    expected_write = 'Processes\n{\n  mytestprocess123\n  {\n    TarBallCSPath = /test/cs/path/bal.tarr\n    Detail = TestNoDetails\n    Generator = mytestGen21\n    Model = testmodel3001\n    Restrictions = \n    InFile = my/file.in\n    CrossSection = 0\n  }\n}\n'
    self.prol._addEntry( 'mytestprocess123', { 'TarBallCSPath' : '/test/cs/path/bal.tarr', 'Detail' :
                                               'TestNoDetails', 'Generator' : 'mytestGen21', 'Model' :
                                               'testmodel3001', 'Restrictions' : '', 'InFile' : 'my/file.in'
                                             } )
    exists_dict = { '/temp/dir' : False, '/temp/dir/mytempfile.txt' : True, '/my/folder/testpath.xml' : True }
    fhandle_mock = Mock()
    with patch('tempfile.mkstemp', new=Mock(return_value=('handle', '/temp/dir/mytempfile.txt'))), \
         patch('__builtin__.file', new=Mock(return_value=fhandle_mock)) as file_mock, \
         patch('os.makedirs') as mkdir_mock, \
         patch('os.path.exists', new=Mock(side_effect=lambda path: exists_dict[path])), \
         patch('shutil.move') as move_mock, \
         patch('os.close') as close_mock:
      assertDiracSucceedsWith_equals( self.prol.writeProcessList( '/my/folder/testpath.xml' ),
                                      '/my/folder/testpath.xml', self )
      mkdir_mock.assert_called_once_with( '/temp/dir' )
      file_mock.assert_called_once_with( '/temp/dir/mytempfile.txt', 'w' )
      fhandle_mock.write.assert_called_once_with( expected_write )
      close_mock.assert_called_once_with( 'handle' )
      move_mock.assert_called_once_with( '/temp/dir/mytempfile.txt', '/my/folder/testpath.xml' )

  def test_writeproclist_notwritten( self ):
    exists_dict = { 'myTmpNameTestme' : True }
    cfg_mock = Mock()
    cfg_mock.writeToFile.return_value = False
    self.prol.cfg = cfg_mock
    self.prol.location = '/my/folder/testpath2.txt'
    with patch('os.close') as close_mock, \
         patch('os.path.exists', new=Mock(side_effect=lambda path: exists_dict[path])), \
         patch('os.remove') as remove_mock, \
         patch('tempfile.mkstemp', new=Mock(return_value=('myhandle', 'myTmpNameTestme'))):
      assertDiracFailsWith( self.prol.writeProcessList(), 'failed to write repo', self )
      close_mock.assert_called_once_with( 'myhandle' )
      remove_mock.assert_called_once_with( 'myTmpNameTestme')

  def test_writeproclist_notwritten_noremove( self ):
    exists_dict = { 'myTmpNameTestme' : False }
    cfg_mock = Mock()
    cfg_mock.writeToFile.return_value = False
    self.prol.cfg = cfg_mock
    with patch('os.close') as close_mock, \
         patch('os.path.exists', new=Mock(side_effect=lambda path: exists_dict[path])), \
         patch('os.remove') as remove_mock, \
         patch('tempfile.mkstemp', new=Mock(return_value=('myhandle', 'myTmpNameTestme'))):
      assertDiracFailsWith( self.prol.writeProcessList( '/my/folder/testpath2.txt' ),
                            'failed to write repo', self )
      close_mock.assert_called_once_with( 'myhandle' )
      self.assertFalse( remove_mock.called )

  def test_writeproclist_move_fails( self ):
    exists_dict = { '/my/folder/testpath2.txt' : False }
    cfg_mock = Mock()
    cfg_mock.writeToFile.return_value = True
    self.prol.cfg = cfg_mock
    with patch('os.close') as close_mock, \
         patch('os.path.exists', new=Mock(side_effect=lambda path: exists_dict[path])), \
         patch('os.remove') as remove_mock, \
         patch('tempfile.mkstemp', new=Mock(return_value=('myhandle', 'myTmpNameTestme'))), \
         patch('shutil.move', new=Mock(side_effect=OSError('mytestErr_os'))):
      assertDiracFailsWith( self.prol.writeProcessList( '/my/folder/testpath2.txt' ),
                            'failed to write repo', self )
      close_mock.assert_called_once_with( 'myhandle' )
      self.assertFalse( remove_mock.called )

  def test_uploadproclist( self ):
    import sys
    import DIRAC
    datman_mock = Mock()
    datman_mock.removeFile.return_value = S_OK('something')
    datmodule_mock = Mock()
    datmodule_mock.DataManager.return_value = datman_mock
    fileutil_mock = Mock()
    fileutil_mock.upload.return_value = S_OK('something')
    conf_mock = Mock()
    conf_mock.getOption.return_value = S_OK( '/local/path/proc.list' )
    mocked_modules = { 'DIRAC.DataManagementSystem.Client.DataManager' : datmodule_mock,
                       'ILCDIRAC.Core.Utilities.FileUtils' : fileutil_mock }
    module_patcher = patch.dict( sys.modules, mocked_modules )
    module_patcher.start()
    backup_conf = DIRAC.gConfig
    DIRAC.gConfig = conf_mock
    with patch('shutil.copy') as copy_mock, \
         patch('subprocess.call') as proc_mock:
      self.prol.uploadProcessListToFileCatalog( '/my/secret/path/processlist.whiz', 'v120' )
      assertMockCalls( copy_mock, [
        ( 'myTestProcess.list', '/afs/cern.ch/eng/clic/software/whizard/whizard_195/' ),
        ( 'myTestProcess.list', '/local/path/proc.list' ) ], self )
      proc_mock.assert_called_once_with(
        [ 'svn', 'ci', '/afs/cern.ch/eng/clic/software/whizard/whizard_195/proc.list',
          "-m'Process list for whizard version v120'" ], shell=False )
    DIRAC.gConfig = backup_conf
    module_patcher.stop()

  def test_uploadproclist_remove_fails( self ):
    import sys
    import DIRAC
    datman_mock = Mock()
    datman_mock.removeFile.return_value = S_ERROR('my_test_err')
    datmodule_mock = Mock()
    datmodule_mock.DataManager.return_value = datman_mock
    fileutil_mock = Mock()
    conf_mock = Mock()
    conf_mock.getOption.return_value = S_OK( 'somepath' )
    mocked_modules = { 'DIRAC.DataManagementSystem.Client.DataManager' : datmodule_mock,
                       'ILCDIRAC.Core.Utilities.FileUtils' : fileutil_mock }
    module_patcher = patch.dict( sys.modules, mocked_modules )
    module_patcher.start()
    backup_conf = DIRAC.gConfig
    DIRAC.gConfig = conf_mock
    DIRAC.exit = abort_test
    with self.assertRaises( KeyboardInterrupt ) as ki:
      self.prol.uploadProcessListToFileCatalog( 'asd', 'v1' )
    key_interrupt = ki.exception
    assertEqualsImproved( key_interrupt.args, ( 'abort_my_test', ), self )
    DIRAC.gConfig = backup_conf
    module_patcher.stop()

  def test_uploadproclist_upload_fails( self ):
    import sys
    import DIRAC
    datman_mock = Mock()
    datman_mock.removeFile.return_value = S_OK('something')
    datmodule_mock = Mock()
    datmodule_mock.DataManager.return_value = datman_mock
    fileutil_mock = Mock()
    fileutil_mock.upload.return_value = S_ERROR('something')
    conf_mock = Mock()
    conf_mock.getOption.return_value = S_OK( 'somepath' )
    mocked_modules = { 'DIRAC.DataManagementSystem.Client.DataManager' : datmodule_mock,
                       'ILCDIRAC.Core.Utilities.FileUtils' : fileutil_mock }
    module_patcher = patch.dict( sys.modules, mocked_modules )
    module_patcher.start()
    backup_conf = DIRAC.gConfig
    DIRAC.gConfig = conf_mock
    DIRAC.exit = abort_test
    with self.assertRaises( KeyboardInterrupt ) as ki:
      self.prol.uploadProcessListToFileCatalog( 'asd', 'v1' )
    key_interrupt = ki.exception
    assertEqualsImproved( key_interrupt.args, ( 'abort_my_test', ), self )
    DIRAC.gConfig = backup_conf
    module_patcher.stop()

  def test_uploadproclist_copy_and_commit_fail( self ):
    import sys
    import DIRAC
    datman_mock = Mock()
    datman_mock.removeFile.return_value = S_OK('something')
    datmodule_mock = Mock()
    datmodule_mock.DataManager.return_value = datman_mock
    fileutil_mock = Mock()
    fileutil_mock.upload.return_value = S_OK('something')
    conf_mock = Mock()
    conf_mock.getOption.return_value = S_OK( 'somepath' )
    mocked_modules = { 'DIRAC.DataManagementSystem.Client.DataManager' : datmodule_mock,
                       'ILCDIRAC.Core.Utilities.FileUtils' : fileutil_mock }
    module_patcher = patch.dict( sys.modules, mocked_modules )
    module_patcher.start()
    backup_conf = DIRAC.gConfig
    DIRAC.gConfig = conf_mock
    DIRAC.exit = abort_test
    with patch('shutil.copy', new=Mock(side_effect=OSError('oserr_testme_keeprunning'))), \
         patch('subprocess.call', new=Mock(side_effect=OSError('subproc_test_err'))):
      self.prol.uploadProcessListToFileCatalog( '/my/secret/path/processlist.whiz', 'v120' )
    DIRAC.gConfig = backup_conf
    module_patcher.stop()

  def test_uploadproclist_skip_copy( self ):
    import sys
    import DIRAC
    datman_mock = Mock()
    datman_mock.removeFile.return_value = S_OK('something')
    datmodule_mock = Mock()
    datmodule_mock.DataManager.return_value = datman_mock
    fileutil_mock = Mock()
    fileutil_mock.upload.return_value = S_OK('something')
    conf_mock = Mock()
    conf_mock.getOption.return_value = S_OK('')
    mocked_modules = { 'DIRAC.DataManagementSystem.Client.DataManager' : datmodule_mock,
                       'ILCDIRAC.Core.Utilities.FileUtils' : fileutil_mock }
    module_patcher = patch.dict( sys.modules, mocked_modules )
    module_patcher.start()
    backup_conf = DIRAC.gConfig
    DIRAC.gConfig = conf_mock
    DIRAC.exit = abort_test
    with patch('shutil.copy', new=Mock(side_effect=IOError('dont_call_me'))), \
         patch('subprocess.call', new=Mock(side_effect=IOError('dont_call_me_either'))):
      self.prol.uploadProcessListToFileCatalog( '/my/secret/path/processlist.whiz', 'v120' )
    DIRAC.gConfig = backup_conf
    module_patcher.stop()

def abort_test( _ ):
  """ Replaces DIRACs own exit method to be testable
  """
  raise KeyboardInterrupt('abort_my_test')
