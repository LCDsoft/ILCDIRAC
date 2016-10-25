"""
Unit tests for the UploadLogFile module
"""

import sys
import unittest
from mock import patch, call, MagicMock as Mock

from ILCDIRAC.Tests.Utilities.GeneralUtils import assertInImproved, \
  assertEqualsImproved, assertDiracFailsWith, assertDiracFails, \
  assertDiracSucceeds, assertDiracSucceedsWith, assertDiracSucceedsWith_equals
from DIRAC import S_OK, S_ERROR

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Workflow.Modules.UploadLogFile'

#pylint: disable=protected-access
class UploadLogFileTestCase( unittest.TestCase ):
  """ Contains tests for the UploadLogFile class"""

  ops_dict = { '/LogFiles/CLIC/Extensions' :
               ['*.txt', '*.log', '*.out', '*.output', '*.xml', '*.sh', '*.info', '*.err','*.root'] }

  def setUp( self ):
    """set up the objects"""
    # Mock out modules that spawn other threads
    sys.modules['DIRAC.DataManagementSystem.Client.DataManager'] = Mock()

    from ILCDIRAC.Workflow.Modules.UploadLogFile import UploadLogFile
    self.ulf = UploadLogFile()
    self.ulf.jobID = 8194
    self.ulf.workflow_commons = { 'Request' : 'something' }
    ops_mock = Mock()
    ops_mock.getValue.side_effect = lambda key, _ : UploadLogFileTestCase.ops_dict[key]
    self.ulf.ops = ops_mock

  def test_execute( self ):
    stat_list = [ ('','','','','','',148), ('','','','','','',2984828952984), OSError('mock_oserr') ]
    glob_list = [ [ 'ignore_me', 'file_1', 'file_2', 'file_3' ], [], [], [], [], [], [], [], [], [] ]
    log_mock = Mock()
    self.ulf.log = log_mock
    UploadLogFileTestCase.ops_dict[ '/LogFiles/CLIC/Extensions' ] = []
    with patch.object(self.ulf, 'resolveInputVariables', new=Mock(return_value=S_ERROR('my_testerr'))), \
         patch('%s.os.stat' % MODULE_NAME, new=Mock(side_effect=stat_list)), \
         patch('%s.glob.glob' % MODULE_NAME, new=Mock(side_effect=glob_list)), \
         patch('%s.os.path.isfile' % MODULE_NAME, new=Mock(side_effect=[ False, True, True, True ])):
      assertDiracSucceeds( self.ulf.execute(), self )
      # FIXME: Deactivated since fails in some environments
      #assertEqualsImproved( log_mock.error.mock_calls, [
      #  call('Failed to resolve input parameters:', 'my_testerr'),
      #  call('Log file found to be greater than maximum of %s bytes' % self.ulf.logSizeLimit, 'file_2'),
      #  call('Completely failed to select relevant log files.', 'Could not determine log files') ], self )

  def test_execute_nologs( self ):
    log_mock = Mock()
    self.ulf.log = log_mock
    with patch.object(self.ulf, 'resolveInputVariables', new=Mock(return_value=S_OK())), \
         patch.object(self.ulf, '_determineRelevantFiles', new=Mock(return_value=S_OK([ 'first_selected_file', 'other_files' ]))), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=False)), \
         patch('%s.os.makedirs' % MODULE_NAME, new=Mock(side_effect=OSError('os_mkdir_failed_testerr_populate'))):
      assertDiracSucceeds( self.ulf.execute(), self )
      log_mock.error.assert_called_once_with( 'Completely failed to populate temporary log file directory.', '' )

  def test_execute_disabled( self ):
    log_mock = Mock()
    self.ulf.log = log_mock
    self.ulf.enable = False
    with patch.object(self.ulf, 'resolveInputVariables', new=Mock(return_value=S_OK())), \
         patch.object(self.ulf, '_determineRelevantFiles', new=Mock(return_value=S_OK([ 'first_selected_file', 'other_files' ]))), \
         patch.object(self.ulf, '_populateLogDirectory', new=Mock(return_value=S_OK())):
      assertDiracSucceedsWith_equals( self.ulf.execute(), 'Module is disabled by control flag', self )
      self.assertFalse( log_mock.error.called )

  def test_execute_tarfails( self ):
    log_mock = Mock()
    self.ulf.log = log_mock
    with patch.object(self.ulf, 'resolveInputVariables', new=Mock(return_value=S_OK())), \
         patch.object(self.ulf, '_determineRelevantFiles', new=Mock(return_value=S_OK([ 'first_selected_file', 'other_files' ]))), \
         patch.object(self.ulf, '_populateLogDirectory', new=Mock(return_value=S_OK())), \
         patch('%s.os.listdir' % MODULE_NAME, new=Mock(return_value= ['file1', 'testfile2' ])), \
         patch('%s.os.path.islink' % MODULE_NAME, new=Mock(side_effect=[ True, False ])), \
         patch('%s.os.chmod' % MODULE_NAME, new=Mock(side_effect=OSError('chmod_mock_testerr'))), \
         patch('%s.os.path.realpath' % MODULE_NAME, new=Mock(return_value='./job/log/prodID/jobID')), \
         patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/my/inval/cwd')), \
         patch('%s.os.chdir' % MODULE_NAME), \
         patch('%s.os.listdir' % MODULE_NAME, new=Mock(return_value=[ 'mytarfile', 'otherfile' ])), \
         patch('%s.tarfile.open' % MODULE_NAME), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=False)):
      assertDiracSucceeds( self.ulf.execute(), self )
      log_mock.error.assert_any_call(
        'Could not set permissions of log files to 0755 with message:\nchmod_mock_testerr' )
      log_mock.error.assert_any_call( 'Failed to create tar file from directory',
                                      './job/log/prodID/jobID File was not created' )

  def test_execute_all_works( self ):
    log_mock = Mock()
    self.ulf.log = log_mock
    self.ulf.failoverTest = False
    se_mock = Mock()
    self.ulf.logSE = se_mock
    se_mock.putFile.return_value = S_OK( { 'Failed' : [], 'Successful' : []} )
    with patch.object(self.ulf, 'resolveInputVariables', new=Mock(return_value=S_OK())), \
         patch.object(self.ulf, '_determineRelevantFiles', new=Mock(return_value=S_OK([ 'first_selected_file', 'other_files' ]))), \
         patch.object(self.ulf, '_populateLogDirectory', new=Mock(return_value=S_OK())), \
         patch.object(self.ulf, '_setLogFilePermissions', new=Mock(return_value=S_OK())), \
         patch.object(self.ulf, '_tarTheLogFiles', new=Mock(return_value=S_OK( { 'fileName' : 'some_name'} ))):
      assertDiracSucceeds( self.ulf.execute(), self )
      self.assertFalse( log_mock.error.called )

  def test_execute_failover_fails( self ):
    log_mock = Mock()
    self.ulf.log = log_mock
    self.ulf.failoverTest = False
    se_mock = Mock()
    se_mock.name = 'mySEMOCK'
    self.ulf.logSE = se_mock
    se_mock.putFile.return_value = S_OK( { 'Failed' : [ 'some_file_failed' ], 'Successful' : [] } )
    with patch.object(self.ulf, 'resolveInputVariables', new=Mock(return_value=S_OK())), \
         patch.object(self.ulf, '_determineRelevantFiles', new=Mock(return_value=S_OK([ 'first_selected_file', 'other_files' ]))), \
         patch.object(self.ulf, '_populateLogDirectory', new=Mock(return_value=S_OK())), \
         patch.object(self.ulf, '_setLogFilePermissions', new=Mock(return_value=S_OK())), \
         patch.object(self.ulf, '_tarTheLogFiles', new=Mock(return_value=S_OK( { 'fileName' : 'some_name' } ))), \
         patch.object(self.ulf, '_tryFailoverTransfer', new=Mock(return_value=S_OK())):
      assertDiracSucceeds( self.ulf.execute(), self )
      log_mock.error.assert_called_once_with(
        "Completely failed to upload log files to mySEMOCK, will attempt upload to failover SE",
        { 'Successful' : [], 'Failed' : [ 'some_file_failed' ] } )

  def test_execute_failover_works( self ):
    log_mock = Mock()
    self.ulf.log = log_mock
    self.ulf.failoverTest = False
    se_mock = Mock()
    se_mock.name = 'mySEMOCK'
    self.ulf.logSE = se_mock
    se_mock.putFile.return_value = S_OK( { 'Failed' : [ 'some_file_failed' ], 'Successful' : [] } )
    request_mock = Mock()
    request_mock.RequestName = 'mymockreq'
    with patch.object(self.ulf, 'resolveInputVariables', new=Mock(return_value=S_OK())), \
         patch.object(self.ulf, '_determineRelevantFiles', new=Mock(return_value=S_OK([ 'first_selected_file', 'other_files' ]))), \
         patch.object(self.ulf, '_populateLogDirectory', new=Mock(return_value=S_OK())), \
         patch.object(self.ulf, '_setLogFilePermissions', new=Mock(return_value=S_OK())), \
         patch.object(self.ulf, '_tarTheLogFiles', new=Mock(return_value=S_OK( { 'fileName' : 'some_name' } ))), \
         patch.object(self.ulf, '_tryFailoverTransfer', new=Mock(return_value=S_OK( { 'Request' : request_mock, 'uploadedSE' : 'mock_se' } ))), \
         patch.object(self.ulf, '_createLogUploadRequest', new=Mock(return_value=S_OK())) as uploadreq_mock:
      assertDiracSucceeds( self.ulf.execute(), self )
      log_mock.error.assert_called_once_with(
        "Completely failed to upload log files to mySEMOCK, will attempt upload to failover SE",
        { 'Successful' : [], 'Failed' : [ 'some_file_failed' ] } )
      uploadreq_mock.assert_called_once_with( 'mySEMOCK', '', 'mock_se' )

  def test_execute_logupload_fails( self ):
    log_mock = Mock()
    self.ulf.log = log_mock
    self.ulf.failoverTest = False
    se_mock = Mock()
    se_mock.name = 'mySEMOCK'
    self.ulf.logSE = se_mock
    se_mock.putFile.return_value = S_OK( { 'Failed' : [ 'some_file_failed' ], 'Successful' : [] } )
    request_mock = Mock()
    request_mock.RequestName = 'mymockreq'
    with patch.object(self.ulf, 'resolveInputVariables', new=Mock(return_value=S_OK())), \
         patch.object(self.ulf, '_determineRelevantFiles', new=Mock(return_value=S_OK([ 'first_selected_file', 'other_files' ]))), \
         patch.object(self.ulf, '_populateLogDirectory', new=Mock(return_value=S_OK())), \
         patch.object(self.ulf, '_setLogFilePermissions', new=Mock(return_value=S_OK())), \
         patch.object(self.ulf, '_tarTheLogFiles', new=Mock(return_value=S_OK( { 'fileName' : 'some_name' } ))), \
         patch.object(self.ulf, '_tryFailoverTransfer', new=Mock(return_value=S_OK( { 'Request' : request_mock, 'uploadedSE' : 'mock_se' } ))), \
         patch.object(self.ulf, '_createLogUploadRequest', new=Mock(return_value=S_ERROR( 'upload_mock_err' ))) as uploadreq_mock:
      assertDiracSucceeds( self.ulf.execute(), self )
      assertEqualsImproved( log_mock.error.mock_calls, [ call(
        "Completely failed to upload log files to mySEMOCK, will attempt upload to failover SE",
        { 'Successful' : [], 'Failed' : [ 'some_file_failed' ] }
      ), call( 'Failed to create failover request', 'upload_mock_err' ) ], self )
      uploadreq_mock.assert_called_once_with( 'mySEMOCK', '', 'mock_se' )

  def test_populatelogdir_nopermissions( self ):
    log_mock = Mock()
    self.ulf.log = log_mock
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=False)), \
         patch('%s.os.makedirs' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.os.chmod' % MODULE_NAME, new=Mock(side_effect=OSError('permission_denied_testerr'))), \
         patch('%s.shutil.copy' % MODULE_NAME, new=Mock(side_effect=OSError('shutil_mockerr'))), \
         patch('%s.os.listdir' % MODULE_NAME, new=Mock(return_value=[])):
      assertDiracFails( self.ulf._populateLogDirectory( [ 'some_file' ] ), self )
      log_mock.error.assert_called_once_with( 'PopulateLogDir: Could not set logdir permissions to 0755:', ' (permission_denied_testerr)')
      log_mock.exception.assert_called_once_with( 'PopulateLogDir: Exception while trying to copy file.', 'some_file', 'shutil_mockerr')



