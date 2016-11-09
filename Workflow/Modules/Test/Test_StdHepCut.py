"""
Unit tests for the StdHepCut module
"""

import unittest
from mock import patch, mock_open, MagicMock as Mock

from ILCDIRAC.Tests.Utilities.FileUtils import FileUtil
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertDiracSucceeds, assertDiracSucceedsWith_equals, assertMockCalls
from ILCDIRAC.Workflow.Modules.StdHepCut import StdHepCut
from DIRAC import S_OK, S_ERROR

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Workflow.Modules.StdHepCut'

class StdHepCutTestCase( unittest.TestCase ):
  """ Contains tests for the StdHepCut class"""

  def setUp( self ):
    """set up the objects"""
    self.shc = StdHepCut()

  def test_applicationspecificinputs_nooutputfile( self ):
    with patch('%s.os.listdir' % MODULE_NAME, new=Mock(return_value=[ 'something.txt', 'something_else.stdhe', 'stdhep.io' ])):
      assertDiracFailsWith( self.shc.applicationSpecificInputs(), 'could not find suitable outputfile name', self )
      assertEqualsImproved( self.shc.OutputFile, '', self )

  def test_applicationspecificinputs( self ):
    file_contents = [[]]
    handles = FileUtil.getMultipleReadHandles(file_contents)
    self.shc.inlineCuts = 'first_line;Some more content of this file; @end \n of% fi/le'
    with patch('%s.os.listdir' % MODULE_NAME, new=Mock(return_value=[ 'something.txt', 'myoutputfile.stdhep', 'stdhep.io' ])), \
         patch('%s.open' % MODULE_NAME, mock_open(), create=True) as mo:
      mo.side_effect = ( h for h in handles )
      assertDiracSucceeds( self.shc.applicationSpecificInputs(), self )
      assertEqualsImproved( ( self.shc.OutputFile, self.shc.SteeringFile ),
                            ( 'myoutputfile_reduced.stdhep', 'cuts_local.txt' ), self )
    mo.assert_called_once_with( 'cuts_local.txt', 'w' )
    assertEqualsImproved( len( handles ), 1, self )
    handles[0].write.assert_any_call( 'first_line\nSome more content of this file\n @end \n of% fi/le' )
    assert handles[0].close.called

  def test_applicationspecificinputs_useworkflow( self ):
    """ Get the OutputFile from the workflow_commons dictionary, don't write to file """
    self.shc.workflow_commons[ 'IS_PROD' ] = True
    self.shc.workflow_commons[ 'ProductionOutputData' ] = ';/some/folder/with/afile.txt; GENfile2;/mydir/folder/file3_Gen_experiment1.stdhep;ignoreme   me too; last_file\n'
    self.shc.OutputFile = '1'
    with patch('%s.open' % MODULE_NAME, new=Mock()) as open_mock:
      assertDiracSucceeds( self.shc.applicationSpecificInputs(), self )
      assertEqualsImproved( ( self.shc.OutputFile, self.shc.SteeringFile ),
                            ( 'file3_Gen_experiment1.stdhep', '' ), self )
      self.assertFalse( open_mock.called )

  def test_applicationspecificinputs_sourcesempty( self ):
    """ Impossible to get the outputfile, use existing one """
    self.shc.workflow_commons[ 'IS_PROD' ] = True
    self.shc.workflow_commons[ 'ProductionOutputData' ] = ';/some/folder/with/afile.txt; GENfile2;ignoreme   me too; last_file\n'
    self.shc.OutputFile = 'testFile_dontchangeme'
    with patch('%s.open' % MODULE_NAME, new=Mock()) as open_mock:
      assertDiracSucceeds( self.shc.applicationSpecificInputs(), self )
      assertEqualsImproved( ( self.shc.OutputFile, self.shc.SteeringFile ),
                            ( 'testFile_dontchangeme', '' ), self )
      self.assertFalse( open_mock.called )

  def test_applicationspecificinputs_( self ):
    """ Guess outputname (using the job properties) using the ILCDIRAC method """
    self.shc.workflow_commons[ 'IS_PROD' ] = True
    self.shc.workflow_commons[ 'PRODUCTION_ID' ] = 1498
    self.shc.workflow_commons[ 'JOB_ID' ] = 134820
    self.shc.OutputFile = '1'
    with patch('%s.open' % MODULE_NAME, new=Mock()) as open_mock, \
         patch('%s.getProdFilename' % MODULE_NAME, new=Mock(return_value='myoutput_test')) as getname_mock:
      assertDiracSucceeds( self.shc.applicationSpecificInputs(), self )
      assertEqualsImproved( ( self.shc.OutputFile, self.shc.SteeringFile ),
                            ( 'myoutput_test', '' ), self )
      self.assertFalse( open_mock.called )
      getname_mock.assert_called_once_with( '1', 1498, 134820 )

  def test_preparescript( self ):
    exists_dict = { 'TestApp_vT_Run_148.sh' : True, './lib' : True }
    ( self.shc.platform, self.shc.applicationName, self.shc.applicationVersion, self.shc.STEP_NUMBER, self.shc.MaxNbEvts, self.shc.OutputFile, self.shc.SteeringFile ) = ( 'testPlatformV1', 'TestApp', 'vT', 148, 13, 'test_OF.ile', 'steer_test.file' )
    file_contents = [[]]
    handles = FileUtil.getMultipleReadHandles(file_contents)
    with patch('%s.getNewLDLibs' % MODULE_NAME, new=Mock(return_value='/my/testsoft/dir1/')) as getldlibs_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=lambda path: exists_dict[path])) as exists_mock, \
         patch('%s.os.remove' % MODULE_NAME, new=Mock()) as remove_mock, \
         patch('%s.open' % MODULE_NAME, mock_open(), create=True ) as mo:
      mo.side_effect = ( h for h in handles )
      self.shc.prepareScript( 'test_software/dir' )
      remove_mock.assert_called_once_with( 'TestApp_vT_Run_148.sh' )
      getldlibs_mock.assert_called_once_with( 'testPlatformV1', 'TestApp', 'vT' )
      assertMockCalls( exists_mock, [ './lib', 'TestApp_vT_Run_148.sh' ], self )
      mo.assert_called_once_with( 'TestApp_vT_Run_148.sh', 'w' )
      assertEqualsImproved( len( handles ), 1, self )
      assertMockCalls( handles[0].write, [
        '#!/bin/sh \n', '#####################################################################\n',
        '# Dynamically generated script to run a production or analysis job. #\n',
        '#####################################################################\n',
        'declare -x PATH=test_software/dir:$PATH\n',
        'declare -x LD_LIBRARY_PATH=./lib:test_software/dir/lib:/my/testsoft/dir1/\n',
        'env | sort >> localEnv.log\n', 'echo =============================\n',
        "stdhepCut -m 13 -o test_OF.ile -c steer_test.file  ../*.stdhep\n",
        'declare -x appstatus=$?\n', 'exit $appstatus\n' ], self )
      assert handles[0].close.called

  def test_preparescript_othercase( self ):
    exists_dict = { 'TestApp_vT_Run_148.sh' : False, './lib' : False }
    ( self.shc.platform, self.shc.applicationName, self.shc.applicationVersion, self.shc.STEP_NUMBER, self.shc.MaxNbEvts, self.shc.OutputFile, self.shc.SteeringFile ) = ( 'testPlatformV1', 'TestApp', 'vT', 148, 0, 'test_OF.ile', 'steer_test.file' )
    file_contents = [[]]
    handles = FileUtil.getMultipleReadHandles(file_contents)
    with patch('%s.getNewLDLibs' % MODULE_NAME, new=Mock(return_value='/my/testsoft/dir1/')) as getldlibs_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=lambda path: exists_dict[path])) as exists_mock, \
         patch('%s.os.remove' % MODULE_NAME, new=Mock()) as remove_mock, \
         patch('%s.open' % MODULE_NAME, mock_open(), create=True ) as mo:
      mo.side_effect = ( h for h in handles )
      self.shc.prepareScript( 'test_software/dir' )
      self.assertFalse( remove_mock.called )
      getldlibs_mock.assert_called_once_with( 'testPlatformV1', 'TestApp', 'vT' )
      assertMockCalls( exists_mock, [ './lib', 'TestApp_vT_Run_148.sh' ], self )
      mo.assert_called_once_with( 'TestApp_vT_Run_148.sh', 'w' )
      assertEqualsImproved( len( handles ), 1, self )
      assertMockCalls( handles[0].write, [
        '#!/bin/sh \n', '#####################################################################\n',
        '# Dynamically generated script to run a production or analysis job. #\n',
        '#####################################################################\n',
        'declare -x PATH=test_software/dir:$PATH\n',
        'declare -x LD_LIBRARY_PATH=test_software/dir/lib:/my/testsoft/dir1/\n',
        'env | sort >> localEnv.log\n', 'echo =============================\n',
        "stdhepCut  -o test_OF.ile -c steer_test.file  ../*.stdhep\n",
        'declare -x appstatus=$?\n', 'exit $appstatus\n' ], self )
      assert handles[0].close.called

  def test_runit_statusbad( self ):
    self.shc.workflowStatus[ 'OK' ] = True
    self.shc.workflowStatus[ 'OK' ] = False
    self.shc.applicationName = 'testApp'
    assertDiracSucceedsWith_equals( self.shc.runIt(),
                                    'testApp should not proceed as previous step did not end properly', self )
  def test_runit_nooutputfile( self ):
    assertDiracFailsWith( self.shc.runIt(), 'outputfile not specified', self )
 
  def test_runit_getswfolder_fails( self ):
    self.shc.OutputFile = 'something'
    ( self.shc.platform, self.shc.applicationName, self.shc.applicationVersion ) = ( 'testPlatform',
                                                                                     'AppTestName', 'vT' )
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(return_value=S_ERROR( 'getsw_test' ))) as getfolder_mock:
      assertDiracFailsWith( self.shc.runIt(), 'getsw_test', self )
      getfolder_mock.assert_called_once_with( 'testPlatform', 'AppTestName', 'vT' )

  def test_runit_getsteeringdir_fails( self ):
    self.shc.OutputFile = 'something'
    self.shc.SteeringFile = '/my/dir/SteerFile.testme'
    ( self.shc.platform, self.shc.applicationName, self.shc.applicationVersion ) = ( 'testPlatform',
                                                                                     'AppTestName', 'vT' )
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(return_value=S_OK('SoftDir'))), \
         patch('%s.getSteeringFileDirName' % MODULE_NAME, new=Mock(return_value=S_ERROR('getsteeringdir_err_test'))) as getsteering_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=False)) as exists_mock:
      assertDiracFailsWith( self.shc.runIt(), 'getsteeringdir_err_test', self )
      getsteering_mock.assert_called_once_with( 'testPlatform', 'AppTestName', 'vT' )
      exists_mock.assert_called_once_with( 'SteerFile.testme' )

  def test_runit_copy_steeringfile_fails( self ):
    self.shc.OutputFile = 'something'
    self.shc.SteeringFile = '/my/dir/SteerFile.testme'
    ( self.shc.platform, self.shc.applicationName, self.shc.applicationVersion ) = ( 'testPlatform',
                                                                                     'AppTestName', 'vT' )
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(return_value=S_OK('SoftDir'))), \
         patch('%s.getSteeringFileDirName' % MODULE_NAME, new=Mock(return_value=S_OK('/my/steer/dir'))), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[False,True])) as exists_mock, \
         patch('%s.shutil.copy' % MODULE_NAME, new=Mock(side_effect=EnvironmentError('copy_test_err'))) as copy_mock:
      assertDiracFailsWith( self.shc.runIt(), 'failed to access file steerfile.testme: copy_test_err', self )
      assertMockCalls( exists_mock, [ 'SteerFile.testme', '/my/steer/dir/SteerFile.testme' ], self )
      copy_mock.assert_called_once_with( '/my/steer/dir/SteerFile.testme', './SteerFile.testme' )

  def test_runit_missinglog( self ):
    exists_dict = { 'SteerFile.testme' : True, 'myAppTestLog.log' : False }
    open_mock = Mock()
    open_mock.readlines.side_effect = [ 'line1\n', 'line2\n', 'newline\n', '\n', 'ok\n' ]
    self.shc.scriptName = 'my_test_script.sh'
    self.shc.OutputFile = 'something'
    self.shc.applicationLog = 'myAppTestLog.log'
    self.shc.SteeringFile = '/my/dir/SteerFile.testme'
    ( self.shc.platform, self.shc.applicationName, self.shc.applicationVersion ) = ( 'testPlatform',
                                                                                     'AppTestName', 'vT' )
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(return_value=S_OK('SoftDir'))), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=lambda path: exists_dict[path])), \
         patch('%s.open' % MODULE_NAME, new=Mock(return_value=open_mock)) as open_mock, \
         patch('%s.os.remove' % MODULE_NAME, new=Mock()) as remove_mock, \
         patch('%s.os.chmod' % MODULE_NAME, new=Mock()) as chmod_mock, \
         patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK( ( 'status', )))) as shell_mock, \
         patch('%s.StdHepCut.prepareScript' % MODULE_NAME, new=Mock()) as prep_mock:
      assertDiracFailsWith( self.shc.runIt(), 'apptestname did not produce the expected log', self )
      self.assertFalse( remove_mock.called )
      shell_mock.assert_called_once_with( 0, 'sh -c "./my_test_script.sh"',
                                          callbackFunction = self.shc.redirectLogOutput, bufferLimit = 20971520 )
      chmod_mock.assert_called_once_with( 'my_test_script.sh', 0755 )
      prep_mock.assert_called_once_with( 'SoftDir' )

  def test_runit_missing_numbers( self ):
    exists_dict = { '/my/steer/dir/SteerFile.testme' : False, 'SteerFile.testme' : False,
                    'myAppTestLog.log' : True }
    open_mock = Mock()
    open_mock.readlines.side_effect = [ 'line1\n', 'line2\n', 'newline\n', '\n', 'ok\n' ]
    open_mock.__enter__.return_value = [ 'someline\n', 'otherline' ]
    self.shc.applicationLog = 'myAppTestLog.log'
    self.shc.OutputFile = 'something'
    self.shc.SteeringFile = '/my/dir/SteerFile.testme'
    ( self.shc.platform, self.shc.applicationName, self.shc.applicationVersion ) = ( 'testPlatform',
                                                                                     'AppTestName', 'vT' )
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(return_value=S_OK('SoftDir'))), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=lambda path: exists_dict[path])), \
         patch('%s.open' % MODULE_NAME, new=Mock(return_value=open_mock)) as open_mock, \
         patch('%s.os.remove' % MODULE_NAME, new=Mock()) as remove_mock, \
         patch('%s.os.chmod' % MODULE_NAME, new=Mock()), \
         patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK( ( 'status', )))), \
         patch('%s.StdHepCut.prepareScript' % MODULE_NAME, new=Mock()), \
         patch('%s.getSteeringFileDirName' % MODULE_NAME, new=Mock(return_value=S_OK('/my/steer/dir'))):
      assertDiracFailsWith( self.shc.runIt(), 'apptestname exited with status 1', self )
      remove_mock.assert_called_once_with( 'myAppTestLog.log' )

  def test_runit_complete( self ):
    exists_dict = { '/my/steer/dir/SteerFile.testme' : False, 'SteerFile.testme' : False,
                    'myAppTestLog.log' : True }
    open_mock = Mock()
    open_mock.readlines.side_effect = [ 'line1\n', 'line2\n', 'newline\n', '\n', 'ok\n' ]
    open_mock.__enter__.return_value = [ 'Events kept 12', 'Events passing cuts 2984', 'Events total 2996' ]
    self.shc.applicationLog = 'myAppTestLog.log'
    self.shc.OutputFile = 'something'
    self.shc.SteeringFile = '/my/dir/SteerFile.testme'
    self.shc.workflow_commons[ 'Luminosity' ] = 13.0
    ( self.shc.platform, self.shc.applicationName, self.shc.applicationVersion ) = ( 'testPlatform',
                                                                                     'AppTestName', 'vT' )
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(return_value=S_OK('SoftDir'))), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=lambda path: exists_dict[path])), \
         patch('%s.open' % MODULE_NAME, new=Mock(return_value=open_mock)) as open_mock, \
         patch('%s.os.remove' % MODULE_NAME, new=Mock()) as remove_mock, \
         patch('%s.os.chmod' % MODULE_NAME, new=Mock()), \
         patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK( ( 0, )))), \
         patch('%s.StdHepCut.prepareScript' % MODULE_NAME, new=Mock()), \
         patch('%s.getSteeringFileDirName' % MODULE_NAME, new=Mock(return_value=S_OK('/my/steer/dir'))):
      assertDiracSucceedsWith_equals( self.shc.runIt(), 'AppTestName vT Successful', self )
      remove_mock.assert_called_once_with( 'myAppTestLog.log' )
      assertEqualsImproved( self.shc.workflow_commons[ 'Luminosity' ], 13.0 * ( 1. * 12 / 2984 ), self )
      assertEqualsImproved( self.shc.workflow_commons[ 'Info' ],
                            { 'stdhepcut' : { 'Reduction' : 1. * 12 / 2984,
                                              'CutEfficiency' : 1. * 2984 / 2996 } }, self )

  def test_runit_othercases( self ):
    exists_dict = { 'SteerFile.testme' : True, 'myAppTestLog.log' : False }
    open_mock = Mock()
    open_mock.readlines.side_effect = [ 'line1\n', 'line2\n', 'newline\n', '\n', 'ok\n' ]
    open_mock.__enter__.return_value = [ 'Events kept 23', 'Events passing cuts 14', 'Events total 37' ]
    self.shc.scriptName = 'my_test_script.sh'
    self.shc.ignoreapperrors = True
    self.shc.OutputFile = 'something'
    self.shc.applicationLog = 'myAppTestLog.log'
    self.shc.SteeringFile = '/my/dir/SteerFile.testme'
    self.shc.workflow_commons[ 'Info' ] = { 'some_entry' : 'rememberMe' }
    self.shc.MaxNbEvts = 100
    ( self.shc.platform, self.shc.applicationName, self.shc.applicationVersion ) = ( 'testPlatform',
                                                                                     'AppTestName', 'vT' )
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(return_value=S_OK('SoftDir'))), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=lambda path: exists_dict[path])), \
         patch('%s.open' % MODULE_NAME, new=Mock(return_value=open_mock)) as open_mock, \
         patch('%s.os.remove' % MODULE_NAME, new=Mock()), \
         patch('%s.os.chmod' % MODULE_NAME, new=Mock()), \
         patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK( ( 0, )))), \
         patch('%s.StdHepCut.prepareScript' % MODULE_NAME, new=Mock()):
      assertDiracSucceedsWith_equals( self.shc.runIt(), 'AppTestName vT Successful', self )
      assertEqualsImproved( self.shc.workflow_commons[ 'Info'],
                            { 'some_entry' : 'rememberMe', 'stdhepcut' :
                              { 'Reduction' : 1. * 23 / 14, 'CutEfficiency' : 1. * 14 / 37 } }, self )
      assert 'Luminosity' not in self.shc.workflow_commons
