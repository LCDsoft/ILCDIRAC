"""
Unit tests for the StdHepSplit module
"""

import unittest
from mock import patch, MagicMock as Mock

from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertDiracSucceedsWith_equals, assertDiracSucceeds, assertMockCalls, assertListContentEquals
from ILCDIRAC.Workflow.Modules.StdHepSplit import StdHepSplit
from DIRAC import S_OK, S_ERROR

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Workflow.Modules.StdHepSplit'

#pylint: disable=protected-access
class StdHepSplitTestCase( unittest.TestCase ):
  """ Contains tests for the StdHepSplit class"""

  def setUp( self ):
    """set up the objects"""
    self.shs = StdHepSplit()

  def test_applicationspecificinputs_nooutputfile( self ):
    assertDiracFailsWith( self.shs.applicationSpecificInputs(), 'no output file defined', self )

  def test_applicationspecificinputs( self ):
    self.shs.OutputFile = 'something'
    self.shs.workflow_commons.update(
      { 'IS_PROD' : True, 'ProductionOutputData' :
        'ignore_,me;/some/dir/outputfile_gen_123.stdio;more_empty_entries.pdf;/dir/ignore;' } )
    self.shs.InputFile = []
    self.shs.InputData = [ '/myfile/1/ignoreme.txt', '/other/input/file.stdhep', 'more/ignored/', '',
                           '/add/me/input.stdhep', '/myfile/1/ignoremenot.stdhep' ]
    self.shs.step_commons[ 'listoutput' ] = [ 14897, True, 'something', {}, 'delete_this' ]
    assertDiracSucceedsWith_equals( self.shs.applicationSpecificInputs(), 'Parameters resolved', self )
    assertEqualsImproved( self.shs.OutputFile, 'outputfile_gen_123.stdio', self )
    assertEqualsImproved( self.shs.listoutput, 14897, self )

  def test_applicationspecificinputs_getoutputfile( self ):
    self.shs.OutputFile = 'something'
    self.shs.workflow_commons.update( { 'IS_PROD' : True, 'PRODUCTION_ID' : '1248', 'JOB_ID' : 173 } )
    self.shs.InputFile = 'input.stdhep'
    self.shs.InputData = []
    self.shs.listoutput = 'my_test_list'
    self.shs.step_commons[ 'listoutput' ] = []
    with patch('%s.getProdFilename' % MODULE_NAME, new=Mock(return_value='myoutputfile.stdio')) as getfile_mock:
      assertDiracSucceedsWith_equals( self.shs.applicationSpecificInputs(), 'Parameters resolved', self )
      assertEqualsImproved( self.shs.OutputFile, 'myoutputfile.stdio', self )
      assertEqualsImproved( self.shs.listoutput, 'my_test_list', self )
      getfile_mock.assert_called_once_with( 'something', 1248, 173 )

  def test_applicationspecificinputs_2( self ):
    self.shs.OutputFile = 'something'
    self.shs.workflow_commons.update( { 'IS_PROD' : True, 'PRODUCTION_ID' : '1248', 'JOB_ID' : 173 } )
    self.shs.InputFile = 'input.stdhep'
    self.shs.InputData = []
    self.shs.listoutput = 'my_test_list'
    with patch('%s.getProdFilename' % MODULE_NAME, new=Mock(return_value='myoutputfile.stdio')) as getfile_mock:
      assertDiracSucceedsWith_equals( self.shs.applicationSpecificInputs(), 'Parameters resolved', self )
      assertEqualsImproved( self.shs.OutputFile, 'myoutputfile.stdio', self )
      assertEqualsImproved( self.shs.listoutput, 'my_test_list', self )
      getfile_mock.assert_called_once_with( 'something', 1248, 173 )

  def test_applicationspecificinputs_isprod_false( self ):
    self.shs.OutputFile = 'something'
    self.shs.InputFile = 'input.stdhep'
    self.shs.InputData = []
    assertDiracSucceedsWith_equals( self.shs.applicationSpecificInputs(), 'Parameters resolved', self )
    assertEqualsImproved( self.shs.OutputFile, 'something', self )
    assertEqualsImproved( self.shs.listoutput, {}, self )

  def test_execute_no_platform( self ):
    with patch('%s.StdHepSplit.resolveInputVariables' % MODULE_NAME, new=Mock()) as resolve_mock:
      assertDiracFailsWith( self.shs.execute(), 'no ilc platform selected', self )
      resolve_mock.assert_called_once_with()

  def test_execute_resolve_input_fails( self ):
    self.shs.platform = 'TestPlatV1'
    with patch('%s.StdHepSplit.resolveInputVariables' % MODULE_NAME, new=Mock(return_value=S_ERROR('test_resolve_err'))):
      assertDiracFailsWith( self.shs.execute(), 'test_resolve_err', self )

  def test_execute_resolve_paths_fails( self ):
    self.shs.platform = 'TestPlatV1'
    self.shs.InputFile = 'something'
    with patch('%s.StdHepSplit.resolveInputVariables' % MODULE_NAME, new=Mock(return_value=S_OK())), \
         patch('%s.resolveIFpaths' % MODULE_NAME, new=Mock(return_value=S_ERROR())) as resolve_mock:
      assertDiracFailsWith( self.shs.execute(), 'missing stdhep file', self )
      resolve_mock.assert_called_once_with( 'something' )

  def test_execute_noinput( self ):
    self.shs.platform = 'TestPlatV1'
    self.shs.InputFile = ''
    with patch('%s.StdHepSplit.resolveInputVariables' % MODULE_NAME, new=Mock(return_value=S_OK())):
      assertDiracSucceedsWith_equals( self.shs.execute(), 'No files found to process', self )

  def test_execute_getswfolder_fails( self ):
    self.shs.applicationVersion = 12
    self.shs.platform = 'TestPlatV1'
    self.shs.InputFile = 'something'
    with patch('%s.StdHepSplit.resolveInputVariables' % MODULE_NAME, new=Mock(return_value=S_OK())), \
         patch('%s.resolveIFpaths' % MODULE_NAME, new=Mock(return_value=S_OK(['runonstd_test_hep']))), \
         patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(return_value=S_ERROR('test_getsw_err_dir'))) as getsw_mock:
      assertDiracFailsWith( self.shs.execute(), 'test_getsw_err_dir', self )
      getsw_mock.assert_called_once_with( 'TestPlatV1', 'stdhepsplit', 12 )

  def test_execute_no_applog( self ):
    exists_dict = { 'StdHepSplit_12_Run_4.tcl' : True, 'mytest_applog.log' : False }
    expected_script ="""
#!/bin/sh

################################################################################
# Dynamically generated script by LCIOConcatenate module                       #
################################################################################

declare -x LD_LIBRARY_PATH=mySplitDir/test/lib:/mydir/ldlibs/library

mySplitDir/test//hepsplit --infile runonstd_test_hep --nw_per_file 16 --outpref /some/dir/mytestOutput_file

exit $?

"""
    self.shs.applicationVersion = 12
    self.shs.STEP_NUMBER = 4
    self.shs.nbEventsPerSlice = 16
    self.shs.platform = 'TestPlatV1'
    self.shs.InputFile = 'something'
    self.shs.applicationLog = 'mytest_applog.log'
    open_mock = Mock()
    self.shs.OutputFile = '/some/dir/mytestOutput_file.stdhep'
    with patch('%s.StdHepSplit.resolveInputVariables' % MODULE_NAME, new=Mock(return_value=S_OK())), \
         patch('%s.resolveIFpaths' % MODULE_NAME, new=Mock(return_value=S_OK(['runonstd_test_hep']))), \
         patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(return_value=S_OK('mySplitDir/test/'))), \
         patch('%s.getNewLDLibs' % MODULE_NAME, new=Mock(return_value='/mydir/ldlibs/library')) as getlibs_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=lambda path: exists_dict[path])), \
         patch('%s.os.remove' % MODULE_NAME, new=Mock()) as remove_mock, \
         patch('%s.open' % MODULE_NAME, new=Mock(return_value=open_mock)) as file_mock, \
         patch('%s.os.chmod' % MODULE_NAME, new=Mock()) as chmod_mock, \
         patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK( ( 0, ) ))) as shell_mock:
      assertDiracFailsWith( self.shs.execute(), 'failed reading the log file', self )
      getlibs_mock.assert_called_once_with( 'TestPlatV1', 'stdhepsplit', 12 )
      file_mock.assert_called_once_with( 'StdHepSplit_12_Run_4.tcl', 'w' )
      open_mock.close.assert_called_once_with()
      open_mock.write.assert_called_once_with( expected_script )
      remove_mock.assert_called_once_with( 'StdHepSplit_12_Run_4.tcl' )
      chmod_mock.assert_called_once_with( 'StdHepSplit_12_Run_4.tcl', 0755 )
      shell_mock.assert_called_once_with( 0, '"./StdHepSplit_12_Run_4.tcl"',
                                          callbackFunction = self.shs.redirectLogOutput, bufferLimit = 20971520 )

  def test_execute_maxread( self ):
    exists_dict = { 'StdHepSplit_V3_Run_4.tcl' : True, 'mytest_applog.log' : False }
    expected_script ="""
#!/bin/sh

################################################################################
# Dynamically generated script by LCIOConcatenate module                       #
################################################################################

declare -x LD_LIBRARY_PATH=mySplitDir/test/lib:/mydir/ldlibs/library

mySplitDir/test//hepsplit --infile runonstd_test_hep --nw_per_file 16 --outpref /some/dir/mytestOutput_file --maxread 112

exit $?

"""
    self.shs.applicationVersion = 'V3'
    self.shs.STEP_NUMBER = 4
    self.shs.nbEventsPerSlice = 16
    self.shs.platform = 'TestPlatV1'
    self.shs.InputFile = 'something'
    self.shs.maxRead = 111
    self.shs.applicationLog = 'mytest_applog.log'
    open_mock = Mock()
    self.shs.OutputFile = '/some/dir/mytestOutput_file.stdhep'
    with patch('%s.StdHepSplit.resolveInputVariables' % MODULE_NAME, new=Mock(return_value=S_OK())), \
         patch('%s.resolveIFpaths' % MODULE_NAME, new=Mock(return_value=S_OK(['runonstd_test_hep']))), \
         patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(return_value=S_OK('mySplitDir/test/'))), \
         patch('%s.getNewLDLibs' % MODULE_NAME, new=Mock(return_value='/mydir/ldlibs/library')) as getlibs_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=lambda path: exists_dict[path])), \
         patch('%s.os.remove' % MODULE_NAME, new=Mock()) as remove_mock, \
         patch('%s.open' % MODULE_NAME, new=Mock(return_value=open_mock)) as file_mock, \
         patch('%s.os.chmod' % MODULE_NAME, new=Mock()) as chmod_mock, \
         patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK( ( 0, ) ))) as shell_mock:
      assertDiracFailsWith( self.shs.execute(), 'failed reading the log file', self )
      getlibs_mock.assert_called_once_with( 'TestPlatV1', 'stdhepsplit', 'V3' )
      file_mock.assert_called_once_with( 'StdHepSplit_V3_Run_4.tcl', 'w' )
      open_mock.close.assert_called_once_with()
      open_mock.write.assert_called_once_with( expected_script )
      remove_mock.assert_called_once_with( 'StdHepSplit_V3_Run_4.tcl' )
      chmod_mock.assert_called_once_with( 'StdHepSplit_V3_Run_4.tcl', 0755 )
      shell_mock.assert_called_once_with( 0, '"./StdHepSplit_V3_Run_4.tcl"',
                                          callbackFunction = self.shs.redirectLogOutput, bufferLimit = 20971520 )

  def test_execute( self ):
    exists_dict = { 'StdHepSplit_12_Run_4.tcl' : False, 'mytest_applog.log' : True }
    self.shs.applicationVersion = 12
    self.shs.STEP_NUMBER = 4
    self.shs.nbEventsPerSlice = 16
    self.shs.workflow_commons[ 'ProductionOutputData' ] = 'first_entry;/some/dir/mytestOutput_file/new_folder;a;;dontdeleteme'
    self.shs.platform = 'TestPlatV1'
    self.shs.InputFile = 'something'
    self.shs.applicationLog = 'mytest_applog.log'
    self.shs.listoutput = { 'outputPath' : 123, 'outputDataSE' : 'myTestDataSE' }
    open_mock = Mock()
    open_mock.__enter__.return_value = [ 'Open output file opfile1  ', 'Record = 41298 ',
                                         'Open output file protonpeter', 'Record = 48',
                                         'Record = 2 Output Begin Run',
                                         'Open output file     /mydir/files/run1.stdhep', 'Record=172',
                                         'Open output file       ignored_file',
                                         'Open output file /last/file.stdhep', 'Record =2941              ' ]
    self.shs.OutputFile = '/some/dir/mytestOutput_file.stdhep'
    with patch('%s.StdHepSplit.resolveInputVariables' % MODULE_NAME, new=Mock(return_value=S_OK())), \
         patch('%s.resolveIFpaths' % MODULE_NAME, new=Mock(return_value=S_OK(['runonstd_test_hep']))), \
         patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(return_value=S_OK('mySplitDir/test/'))), \
         patch('%s.getNewLDLibs' % MODULE_NAME, new=Mock(return_value='/mydir/ldlibs/library')), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=lambda path: exists_dict[path])), \
         patch('%s.os.remove' % MODULE_NAME, new=Mock()) as remove_mock, \
         patch('%s.open' % MODULE_NAME, new=Mock(return_value=open_mock)), \
         patch('%s.os.chmod' % MODULE_NAME, new=Mock()), \
         patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK( ( 0, ) ))), \
         patch('%s.StdHepSplit.listDir' % MODULE_NAME, new=Mock()):
      assertDiracSucceeds( self.shs.execute(), self )
      remove_mock.assert_called_once_with( 'mytest_applog.log' )
      assertListContentEquals( self.shs.step_commons[ 'listoutput' ], [
        { 'outputFile' : 'opfile1', 'outputPath' : 123, 'outputDataSE' : 'myTestDataSE' },
        { 'outputFile' : 'protonpeter', 'outputPath' : 123, 'outputDataSE' : 'myTestDataSE' },
        { 'outputFile' : '/mydir/files/run1.stdhep', 'outputPath' : 123, 'outputDataSE' : 'myTestDataSE' },
        { 'outputFile' : 'ignored_file', 'outputPath' : 123, 'outputDataSE' : 'myTestDataSE' },
        { 'outputFile' : '/last/file.stdhep', 'outputPath' : 123, 'outputDataSE' : 'myTestDataSE' } ], self )
      assertListContentEquals( self.shs.workflow_commons[ 'ProductionOutputData' ].split(';'),
                               'first_entry;a;;dontdeleteme;/some/dir/mytestOutput_file/ignored_file;/some/dir/mytestOutput_file/opfile1;/some/dir/mytestOutput_file/protonpeter;/last/file.stdhep;/mydir/files/run1.stdhep'.split(';'), self )
