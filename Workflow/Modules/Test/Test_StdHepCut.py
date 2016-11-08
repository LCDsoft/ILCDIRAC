"""
Unit tests for the StdHepCut module
"""

import unittest
from mock import patch, mock_open, MagicMock as Mock

from ILCDIRAC.Tests.Utilities.FileUtils import FileUtil
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertDiracSucceeds, assertDiracSucceedsWith_equals, assertMockCalls
from ILCDIRAC.Workflow.Modules.StdHepCut import StdHepCut
from DIRAC import S_ERROR

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Workflow.Modules.StdHepCut'

#pylint: disable=protected-access
class StdHepCutTestCase( unittest.TestCase ):
  """ Contains tests for the StdHepCut class"""

  def setUp( self ):
    """set up the objects"""
    # Mock out modules that spawn other threads
    #sys.modules['DIRAC.DataManagementSystem.Client.DataManager'] = Mock()
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
