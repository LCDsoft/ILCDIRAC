#!/usr/bin/env python
""" Test the ModuleBase module """

from __future__ import print_function
from StringIO import StringIO
import sys
import unittest
from mock import patch, mock_open, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Workflow.Modules.ModuleBase import ModuleBase, generateRandomString
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, \
  assertDiracFailsWith, assertDiracSucceeds, assertDiracSucceedsWith, \
  assertDiracSucceedsWith_equals, assertMockCalls

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Workflow.Modules.ModuleBase'

class ModuleBaseTestCase( unittest.TestCase ): #pylint: disable=too-many-public-methods
  """ Test the ModuleBase module
  """
  def setUp( self ):
    # Mock out modules that spawn other threads
    mocked_modules = { 'DIRAC.DataManagementSystem.Client.DataManager' : Mock() }

    self.log_mock = Mock()

    self.patches = [
        patch.dict(sys.modules, mocked_modules),
        patch('%s.LOG' % MODULE_NAME, new=self.log_mock),
        ]

    for patcher in self.patches:
      patcher.start()

    self.moba = ModuleBase()
    # clear logging from constructor
    self.log_mock.reset_mock()

  def tearDown( self ):
    for patcher in self.patches:
      patcher.stop()

  def test_randomstring( self ):
    random_string_1 = generateRandomString()
    random_string_2 = generateRandomString()
    assertEqualsImproved( len(random_string_1), 8, self )
    assertEqualsImproved( len(random_string_2), 8, self )
    assert isinstance( random_string_1, basestring )
    assert isinstance( random_string_2, basestring )

  def test_constructor_fail(self):
    """Test constructor when failing."""
    with patch('%s.getProxyInfoAsString' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_error_testme'))):
      ModuleBase()
    self.log_mock.error.assert_called_once_with(
        'Could not obtain proxy information in module environment with message:\n', 'some_error_testme')

  def test_constructor_sucess(self):
    """Test constructor when succeeding."""
    with patch('%s.getProxyInfoAsString' % MODULE_NAME, new=Mock(return_value=S_OK('some_proxy_infos'))):
      ModuleBase()
    self.assertFalse(self.log_mock.error.called)

  def test_execute_basic( self ):
    result = self.moba.execute()
    assertDiracSucceeds( result, self )

  def test_execute_resolveinput_fails( self ):
    self.moba.isProdJob = True
    self.moba.InputData = 'myInputData.testme'
    with patch('%s.getNumberOfEvents' % MODULE_NAME, new=Mock(return_value=S_ERROR(''))) as getevts_mock:
      assertDiracFailsWith( self.moba.execute(),
                            'failed to get numberofevents from filecatalog', self )
      getevts_mock.assert_called_once_with( 'myInputData.testme' )

  def test_execute_treatsteering_fails( self ):
    self.moba.step_commons['SteeringFileVers'] = 'mySteerTestVers'
    self.moba.platform = 'TRestPlatformMine'
    with patch('%s.getSteeringFileDir' % MODULE_NAME, new=Mock(return_value=S_ERROR('test_err_filedir_steer'))) as steerdir_mock:
      assertDiracFailsWith( self.moba.execute(),
                            'failed to locate steering files mysteertestvers', self )
      steerdir_mock.assert_called_once_with( 'TRestPlatformMine', 'mySteerTestVers' )

  def test_execute_config_fails( self ):
    self.moba.platform = 'PlatformMineTest'
    self.moba.workflow_commons['ClicConfigPackage'] = 'ClicConfigv102'
    self.moba.workflow_commons['SomeOtherEntry'] = 'Args'
    self.moba.workflow_commons['AndAnotherOne'] = 'Args'
    with patch('%s.checkCVMFS' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_err'))) as cvmfs_mock, \
         patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_other_err'))) as getsoft_mock:
      assertDiracFailsWith( self.moba.execute(),
                            'Failed to locate ClicConfigv102 as config dir', self )
      cvmfs_mock.assert_called_once_with( 'PlatformMineTest',
                                          ( 'clicconfig', 'v102' ) )
      getsoft_mock.assert_called_once_with( 'PlatformMineTest',
                                            'clicconfig', 'v102' )

  def test_execute_runit_fails( self ):
    with patch('%s.ModuleBase.runIt' % MODULE_NAME, new=Mock(return_value=S_ERROR('runit_fails_test_err'))):
      assertDiracFailsWith( self.moba.execute(), 'runit_fails_test_err', self )

  def test_execute_othercases( self ):
    exists_dict = { 'steering.file' : False, './entry.steeringfile' : False,
                    './failcopyingonthis': False, './testentry' : True,
                    './lastfile.sf' : False, './ildfile.entry' : False,
                    './my.ild.file' : True, './ild_failcopyhere' : False,
                    './lastfile.ild' : False }
    isdir_dict = { 'steering/file/path/entry.steeringfile' : True,
                   'steering/file/path/lastfile.sf' : False,
                   'steering/file/path/failcopyingonthis' : True,
                   'ild/test/configpath/ildfile.entry' : True,
                   'ild/test/configpath/lastfile.ild' : False,
                   'ild/test/configpath/ild_failcopyhere' : True }
    listdir_dict = { 'steering/file/path' : [
      'entry.steeringfile', 'testentry', 'failcopyingonthis', 'lastfile.sf' ],
                     'ild/test/configpath' : [
                       'ildfile.entry', 'my.ild.file', 'ild_failcopyhere',
                       'lastfile.ild' ], '/test/cur/working/dir' : [] }
    self.moba.SteeringFile = '/dir/myfile/steering.file'
    self.moba.workflow_commons['ILDConfigPackage'] = 'ILDConfigv102'
    self.moba.step_commons['SteeringFileVers'] = 'mySteerTestVers'
    self.moba.platform = 'TRestPlatformMine'
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=lambda path: exists_dict[path])), \
         patch('%s.getSteeringFileDir' % MODULE_NAME, new=Mock(return_value=S_OK('steering/file/path'))), \
         patch('%s.os.listdir' % MODULE_NAME, new=Mock(side_effect=lambda path: listdir_dict[path])) as listdir_mock, \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(side_effect=lambda path: isdir_dict[path])) as isdir_mock, \
         patch('%s.shutil.copytree' % MODULE_NAME, new=Mock(side_effect=[ True, EnvironmentError('failed_copytree_testme' ), True, EnvironmentError('failed_copytree_ildconfig') ])) as copytree_mock, \
         patch('%s.shutil.copy2' % MODULE_NAME) as copy2_mock, \
         patch('%s.checkCVMFS' % MODULE_NAME, new=Mock(return_value=S_OK(['ild/test/configpath']))), \
         patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/test/cur/working/dir')) as getcwd_mock:
      result = self.moba.execute()
    assertDiracSucceedsWith_equals( result, None, self )
    assertMockCalls( copytree_mock, [ ( 'steering/file/path/entry.steeringfile', './entry.steeringfile' ),
                                      ( 'steering/file/path/failcopyingonthis', './failcopyingonthis' ),
                                      ( 'ild/test/configpath/ildfile.entry', './ildfile.entry'),
                                      ( 'ild/test/configpath/ild_failcopyhere', './ild_failcopyhere') ], self )
    assertMockCalls( copy2_mock, [ ( 'steering/file/path/lastfile.sf', './lastfile.sf' ),
                                   ( 'ild/test/configpath/lastfile.ild', './lastfile.ild' ) ], self )
    assertMockCalls( listdir_mock, [ 'steering/file/path', 'ild/test/configpath', '/test/cur/working/dir' ], self )
    assertMockCalls( isdir_mock, [ 'steering/file/path/entry.steeringfile',
                                   'steering/file/path/failcopyingonthis',
                                   'steering/file/path/lastfile.sf',
                                   'ild/test/configpath/ildfile.entry',
                                   'ild/test/configpath/ild_failcopyhere',
                                   'ild/test/configpath/lastfile.ild' ], self )
    getcwd_mock.assert_called_once_with()

  def test_setappstat( self ):
    self.moba.jobID = 24986
    log_mock = Mock()
    self.moba.log = log_mock
    report_mock = Mock()
    report_mock.setApplicationStatus.return_value = S_OK('mytest_success!!!')
    self.moba.workflow_commons['JobReport'] = report_mock
    assertDiracSucceedsWith_equals( self.moba.setApplicationStatus( 'my_test_status' ),
                                    'mytest_success!!!', self )
    self.assertFalse( log_mock.called )
    self.assertFalse( log_mock.warn.called )
    self.assertFalse( log_mock.err.called )
    report_mock.setApplicationStatus.assert_called_once_with( 'my_test_status', True )

  def test_setappstat_local( self ):
    self.moba.jobID = 0
    assertDiracSucceedsWith_equals( self.moba.setApplicationStatus( 'my_test_status' ),
                                    'JobID not defined', self )

  def test_setappstat_noreporter( self ):
    self.moba.jobID = 24986
    assertDiracSucceedsWith_equals( self.moba.setApplicationStatus( 'my_test_status' ),
                                    'No reporting tool given', self )

  def test_setappstat_setting_fails( self ):
    self.moba.jobID = 24986
    report_mock = Mock()
    report_mock.setApplicationStatus.return_value = S_ERROR('failed setting appstat_testme')
    self.moba.workflow_commons['JobReport'] = report_mock
    assertDiracFailsWith( self.moba.setApplicationStatus( 'my_test_status' ),
                          'failed setting appstat_testme', self )
    report_mock.setApplicationStatus.assert_called_once_with( 'my_test_status', True )
    self.log_mock.warn.assert_called_once_with('failed setting appstat_testme')

  def test_sendstoredstatinfo( self ):
    self.moba.jobID = 24986
    report_mock = Mock()
    report_mock.sendStoredStatusInfo.return_value = S_OK('mytest_success!!!')
    self.moba.workflow_commons['JobReport'] = report_mock
    assertDiracSucceedsWith_equals( self.moba.sendStoredStatusInfo(), 'mytest_success!!!', self )
    self.assertFalse(self.log_mock.called)
    self.assertFalse(self.log_mock.warn.called)
    self.assertFalse(self.log_mock.error.called)
    report_mock.sendStoredStatusInfo.assert_called_once_with()

  def test_sendstoredstatinfo_local( self ):
    self.moba.jobID = 0
    assertDiracSucceedsWith_equals( self.moba.sendStoredStatusInfo(),
                                    'JobID not defined', self )

  def test_sendstoredstatinfo_noreporter( self ):
    self.moba.jobID = 24986
    assertDiracSucceedsWith_equals( self.moba.sendStoredStatusInfo(),
                                    'No reporting tool given', self )

  def test_sendstoredstatinfo_setting_fails( self ):
    self.moba.jobID = 24986
    report_mock = Mock()
    report_mock.sendStoredStatusInfo.return_value = S_ERROR('failed setting appstat_testme')
    self.moba.workflow_commons['JobReport'] = report_mock
    assertDiracFailsWith( self.moba.sendStoredStatusInfo(), 'failed setting appstat_testme', self )
    report_mock.sendStoredStatusInfo.assert_called_once_with()
    self.log_mock.error.assert_called_once_with('failed setting appstat_testme')

  def test_setjobparameter( self ):
    self.moba.jobID = 24986
    report_mock = Mock()
    report_mock.setJobParameter.return_value = S_OK('mytest_success!!!')
    self.moba.workflow_commons['JobReport'] = report_mock
    assertDiracSucceedsWith_equals( self.moba.setJobParameter( 'mytestName', 135 ),
                                    'mytest_success!!!', self )
    self.assertFalse(self.log_mock.called)
    self.assertFalse(self.log_mock.warn.called)
    self.assertFalse(self.log_mock.error.called)
    report_mock.setJobParameter.assert_called_once_with( 'mytestName', '135', True )

  def test_setjobparameter_local( self ):
    self.moba.jobID = 0
    assertDiracSucceedsWith_equals( self.moba.setJobParameter( 'mytestName', 193 ),
                                    'JobID not defined', self )

  def test_setjobparameter_noreporter( self ):
    self.moba.jobID = 24986
    assertDiracSucceedsWith_equals( self.moba.setJobParameter( 'myTEstName', 9813 ),
                                    'No reporting tool given', self )

  def test_setjobparameter_setting_fails( self ):
    self.moba.jobID = 24986
    report_mock = Mock()
    report_mock.setJobParameter.return_value = S_ERROR('failed setting appstat_testme')
    self.moba.workflow_commons['JobReport'] = report_mock
    assertDiracFailsWith( self.moba.setJobParameter( 'parameterTestName', 984 ),
                          'failed setting appstat_testme', self )
    report_mock.setJobParameter.assert_called_once_with( 'parameterTestName', '984', True )
    self.log_mock.warn.assert_called_once_with('failed setting appstat_testme')

  def test_sendstoredjobparameters( self ):
    self.moba.jobID = 24986
    report_mock = Mock()
    report_mock.sendStoredJobParameters.return_value = S_OK('mytest_success!!!')
    self.moba.workflow_commons['JobReport'] = report_mock
    assertDiracSucceedsWith_equals( self.moba.sendStoredJobParameters(),
                                    'mytest_success!!!', self )
    self.assertFalse(self.log_mock.called)
    self.assertFalse(self.log_mock.warn.called)
    self.assertFalse(self.log_mock.error.called)
    report_mock.sendStoredJobParameters.assert_called_once_with()

  def test_sendstoredjobparameters_local( self ):
    self.moba.jobID = 0
    assertDiracSucceedsWith_equals( self.moba.sendStoredJobParameters(),
                                    'JobID not defined', self )

  def test_sendstoredjobparameters_noreporter( self ):
    self.moba.jobID = 24986
    assertDiracSucceedsWith_equals( self.moba.sendStoredJobParameters(),
                                    'No reporting tool given', self )

  def test_sendstoredjobparameters_setting_fails( self ):
    self.moba.jobID = 24986
    report_mock = Mock()
    report_mock.sendStoredJobParameters.return_value = S_ERROR('failed setting appstat_testme')
    self.moba.workflow_commons['JobReport'] = report_mock
    assertDiracFailsWith( self.moba.sendStoredJobParameters(),
                          'failed setting appstat_testme', self )
    report_mock.sendStoredJobParameters.assert_called_once_with()
    self.log_mock.error.assert_called_once_with('failed setting appstat_testme')

  def test_setfilestatus_useexistingfilereport( self ):
    report_mock = Mock()
    report_mock.setFileStatus.return_value = S_OK('my_report_test_returnval')
    self.moba.workflow_commons['FileReport'] = report_mock
    assertDiracSucceedsWith_equals( self.moba.setFileStatus( 'production', 'lfn', 'status' ),
                                    'my_report_test_returnval', self )
    assertEqualsImproved( self.moba.workflow_commons['FileReport'], report_mock, self )

  def test_setfilestatus( self ):
    report_mock = Mock()
    report_mock.setFileStatus.return_value = S_OK('other_my_report_test')
    with patch('%s.FileReport' % MODULE_NAME, new=Mock(return_value=report_mock)):
      assertDiracSucceedsWith_equals( self.moba.setFileStatus( 'production', 'lfn', 'status' ),
                                      'other_my_report_test', self )
      assertEqualsImproved( self.moba.workflow_commons['FileReport'], report_mock, self )

  def test_setfilestatus_fails_useexistingfilereport( self ):
    report_mock = Mock()
    report_mock.setFileStatus.return_value = S_ERROR('test_setfilestat_err')
    self.moba.workflow_commons['FileReport'] = report_mock
    assertDiracFailsWith( self.moba.setFileStatus( 'production', 'lfn', 'status' ),
                          'test_setfilestat_err', self )
    assertEqualsImproved( self.moba.workflow_commons['FileReport'], report_mock, self )
    self.log_mock.warn.assert_called_once_with('test_setfilestat_err')

  def test_setfilestatus_fails( self ):
    report_mock = Mock()
    report_mock.setFileStatus.return_value = S_ERROR('test_setfile_staterr')
    with patch('%s.FileReport' % MODULE_NAME, new=Mock(return_value=report_mock)):
      assertDiracFailsWith( self.moba.setFileStatus( 'production', 'lfn', 'status' ),
                            'test_setfile_staterr', self )
      assertEqualsImproved( self.moba.workflow_commons['FileReport'], report_mock, self )
      self.log_mock.warn.assert_called_once_with('test_setfile_staterr')

  def test_getcandidatefiles( self ):
    exists_dict = { 'testfile_allworks.stdhep' : True, 'testfile_notlocal.txt' : False }
    self.moba.ignoreapperrors = True
    mytest_outputlist = [ {
      'outputFile' : 'testfile_allworks.stdhep', 'outputDataSE' : 'testSE_dip4_allgood',
      'outputPath' : '/test/clic/ilc/mytestfile.txt' }, {
        'outputFile' : 'failhere', 'outputDataSE' : '' }, {
          'outputFile' : 'testfile_notlocal.txt', 'outputDataSE' : 'no_se',
          'outputPath' : '/test/clic/ilc/otherdir/newfile.txt' } ]
    mylfns = [ 'testfile_allworks.stdhep', 'ignorethis', 'testfile_notlocal.txt' ]
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=lambda path: exists_dict[path])):
      result = self.moba.getCandidateFiles( mytest_outputlist, mylfns, 'dummy_file_mask')
      assertDiracSucceedsWith_equals( result, { 'testfile_allworks.stdhep' : {
        'lfn': 'testfile_allworks.stdhep', 'path' : '/test/clic/ilc/mytestfile.txt',
        'workflowSE': 'testSE_dip4_allgood' } }, self )

  def test_getcandidatefiles_filenametoolong( self ):
    mytest_outputlist = [ {
      'outputFile' : 'eruighnegjmneroiljger89igujmnerjhvreikvnmer9fig8erjg89iuerjhguie5hgieu7hg893j4tf4iufnugfyrhbgyukbfruwjfhwiefjhuiewfjwenfiuewnfieuwhuifrweuijfiuwerjhuiwer', 'outputDataSE' : 'testSE_dip4_allgood', 'outputPath' : '/test/clic/ilc/mytestfile.txt'
    }, { 'outputFile' : 'failhere', 'outputDataSE' : '' } ]
    mylfns = [ 'eruighnegjmneroiljger89igujmnerjhvreikvnmer9fig8erjg89iuerjhguie5hgieu7hg893j4tf4iufnugfyrhbgyukbfruwjfhwiefjhuiewfjwenfiuewnfieuwhuifrweuijfiuwerjhuiwer',
               'ignorethis' ]
    assertDiracFailsWith( self.moba.getCandidateFiles(
      mytest_outputlist, mylfns, 'dummy_file_mask'), 'filename too long', self )

  def test_getcandidatefiles_lfntoolong( self ):
    mytest_outputlist = [ {
      'outputFile' : 'testfile_dirstoolong.stdhep', 'outputDataSE' : 'testSE_dip4_allgood',
      'outputPath' : '/test/clic/ilc/mytestfile.txt' }, {
        'outputFile' : 'failhere', 'outputDataSE' : '' } ]
    mylfns = [ 'esaiujf/oijkrgrmwg/oirwgjmoiwrg/oijefiouwef/dir/oiejfmwseroigfujwfguiwefmviwfweoifkmiwoe/oieujguimeosifkmespokfsoeifkjoisuejfsef/soiuejfuisejfosiekfoisejfiusejfoisekfjoisejuguisehngusefjoisefkfsefjmsi/eiujfmeiowfmefkjeoifjiuenfenfj/feiosjkfoiesfksepoflsefpolsefiokseiufnmjef/fueinsfsnejfhsnjhefsjhebfjshebfsenfseifnsoiefkjseoidejiuesjndeqniuwejqoiwjeiqwmdwkajndawnduaidjaiowdjiawd/duiwandqiuodjqiwodjqownuqnfrqujrnjqwrqweioqmwdoiqmid/testfile_dirstoolong.stdhep',
               'ignorethis' ]
    assertDiracFailsWith( self.moba.getCandidateFiles(
      mytest_outputlist, mylfns, 'dummy_file_mask'), 'lfn too long', self )

  def test_getcandidatefiles_missinglocally( self ):
    exists_dict = { 'dir/testfile_allworks.stdhep' : True, 'testfile_notlocal.txt' : False }
    self.moba.ignoreapperrors = False
    mytest_outputlist = [
      { 'outputFile' : 'dir/testfile_allworks.stdhep',
        'outputDataSE' : 'testSE_dip4_allgood',
        'outputPath' : '/test/clic/ilc/mytestfile.txt' }, {
          'outputFile' : 'failhere', 'outputDataSE' : '' }, {
            'outputFile' : 'testfile_notlocal.txt', 'outputDataSE' : 'no_se',
            'outputPath' : '/test/clic/ilc/otherdir/newfile.txt' } ]
    mylfns = [ 'testfile_allworks.stdhep', 'ignorethis', 'testfile_notlocal.txt' ]
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=lambda path: exists_dict[path])):
      result = self.moba.getCandidateFiles( mytest_outputlist, mylfns, 'dummy_file_mask')
      assertDiracFailsWith( result, 'output data not found', self )

  def test_getfilemetadata( self ):
    # Can in its current form never return S_ERROR, filedict is set when adding new element to final and GUID is set in the metadata dict for each element that might get added
    guid_dict = { 'testfile_allworks.stdhep' : 'test_myGuid_1', 'myothertest_file' : 'test_myGuid_2' }
    size_dict = { 'testfile_allworks.stdhep' : 24852, 'myothertest_file' : 948524 }
    adler_dict = { 'testfile_allworks.stdhep' : '9803531', 'myothertest_file' : 'checksum1230#' }
    with patch('%s.makeGuid' % MODULE_NAME, new=Mock(side_effect=lambda path: guid_dict[path])) as guid_mock, \
         patch('%s.os.path.getsize' % MODULE_NAME, new=Mock(side_effect=lambda path: size_dict[path])), \
         patch('%s.fileAdler' % MODULE_NAME, new=Mock(side_effect=lambda path: adler_dict[path])), \
         patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/cur/working/test/')):
      candidateFiles = { 'testfile_allworks.stdhep' : {
        'lfn': 'testfile_allworks.stdhep', 'path' : '/test/clic/ilc/mytestfile.txt',
        'workflowSE': 'testSE_dip4_allgood' }, 'myothertest_file' : {
          'lfn' : 'myothertest_file', 'path' : '/dir/clid/user/myothertestfile.txt',
          'workflowSE' : 'CERN_dip4_testme' } }
      result = self.moba.getFileMetadata( candidateFiles )
      expected_dict = {
        'testfile_allworks.stdhep': {
          'filedict': {
            'Status': 'Waiting', 'ADLER32': '9803531', 'ChecksumType': 'ADLER32',
            'Checksum': '9803531', 'LFN': 'testfile_allworks.stdhep',
            'GUID': 'test_myGuid_1', 'Addler': '9803531', 'Size': 24852
          }, 'lfn': 'testfile_allworks.stdhep',
          'localpath': '/cur/working/test//testfile_allworks.stdhep',
          'workflowSE': 'testSE_dip4_allgood', 'path': '/test/clic/ilc/mytestfile.txt',
          'GUID': 'test_myGuid_1' }, 'myothertest_file': {
            'filedict': {
              'Status': 'Waiting', 'ADLER32': 'checksum1230#',
              'ChecksumType': 'ADLER32', 'Checksum': 'checksum1230#',
              'LFN': 'myothertest_file', 'GUID': 'test_myGuid_2',
              'Addler': 'checksum1230#', 'Size': 948524 }, 'lfn': 'myothertest_file',
            'localpath': '/cur/working/test//myothertest_file',
            'workflowSE': 'CERN_dip4_testme',
            'path': '/dir/clid/user/myothertestfile.txt',
            'GUID': 'test_myGuid_2'} }
      assertDiracSucceedsWith_equals( result, expected_dict, self )
      assertMockCalls( guid_mock, [ 'testfile_allworks.stdhep', 'myothertest_file' ], self )

  def test_resolveinputvars( self ):
    mb = self.moba
    mb.workflow_commons['IS_PROD'] = True
    mb.workflow_commons['PRODUCTION_ID'] = 13412
    mb.workflow_commons['SystemConfig'] = 'myTestPlatform'
    mb.workflow_commons['StartFrom'] = 94
    mb.workflow_commons['NbOfEvts'] = 458
    mb.workflow_commons['InputData'] = 'myinputData1Test;myinputData2Test;myinputData3Test'
    mb.step_commons['InputFile'] = 'myInputTestFile1;TestinputFile2'
    mb.OutputFile = None
    mb.step_commons['OutputFile'] = 'myTestOFile.txt'
    with patch('%s.getNumberOfEvents' % MODULE_NAME, new=Mock(return_value=S_ERROR('getNbEvtsFails_test'))):
      result = mb.resolveInputVariables()
      assertDiracSucceedsWith_equals( result, 'Parameters resolved', self )

  def test_resolveinputvars_inputfile_variation( self ):
    mb = self.moba
    mb.workflow_commons['IS_PROD'] = True
    mb.workflow_commons['PRODUCTION_ID'] = 13412
    mb.workflow_commons['SystemConfig'] = 'myTestPlatform'
    mb.workflow_commons['StartFrom'] = 94
    mb.workflow_commons['InputData'] = ['myinputData1Test', 'myinputData2Test',
                                        'LFN:myinputData3Test']
    mb.step_commons['InputFile'] = ''
    mb.OutputFile = 'myTestOutputFile'
    mb.step_commons['OutputFile'] = 'myTestOFile.txt'
    with patch('%s.getNumberOfEvents' % MODULE_NAME, new=Mock(return_value=S_OK( { 'AdditionalMeta' : {}, 'nbevts' : 9824 } ))):
      result = mb.resolveInputVariables()
      assertDiracSucceedsWith_equals( result, 'Parameters resolved', self )

  def test_resolveinputvarsinputfile_var2( self ):
    mb = self.moba
    mb.workflow_commons['IS_PROD'] = True
    mb.workflow_commons['PRODUCTION_ID'] = 13412
    mb.workflow_commons['SystemConfig'] = 'myTestPlatform'
    mb.workflow_commons['StartFrom'] = 94
    mb.workflow_commons['NbOfEvts'] = 458
    mb.workflow_commons['InputData'] = ''
    # mb.workflow_commons['ParametricInputData'] = ''  test mit parametric+normal inputdata
    mb.step_commons['InputFile'] = []
    mb.OutputFile = None
    mb.step_commons['OutputFile'] = 'myTestOFile.txt'
    with patch('%s.getNumberOfEvents' % MODULE_NAME, new=Mock(return_value=S_ERROR('getNbEvtsFails_test'))):
      result = mb.resolveInputVariables()
      assertDiracSucceedsWith_equals( result, 'Parameters resolved', self )

  def test_resolveinputvars_parametricdata( self ):
    mb = self.moba
    mb.workflow_commons['IS_PROD'] = True
    mb.workflow_commons['PRODUCTION_ID'] = 13412
    mb.workflow_commons['SystemConfig'] = 'myTestPlatform'
    mb.workflow_commons['StartFrom'] = 94
    mb.workflow_commons['NbOfEvts'] = 458
    mb.workflow_commons['InputData'] = 'myinputData1Test;myinputData2Test;myinputData3Test'
    mb.workflow_commons['ParametricInputData'] = 'paramData1Test;LFN:TestParamData2;3DataTest'
    mb.OutputFile = None
    mb.step_commons['OutputFile'] = 'myTestOFile.txt'
    with patch('%s.getNumberOfEvents' % MODULE_NAME, new=Mock(return_value=S_ERROR('getNbEvtsFails_test'))), \
         patch('%s.ModuleBase.applicationSpecificInputs' % MODULE_NAME, new=Mock(return_value=S_ERROR('test_failthisappspec'))):
      result = mb.resolveInputVariables()
      assertDiracFailsWith( result, 'test_failthisappspec', self )

  def test_resolveinputvars_parametricdata_1( self ):
    mb = self.moba
    mb.workflow_commons['IS_PROD'] = True
    mb.workflow_commons['PRODUCTION_ID'] = 13412
    mb.workflow_commons['SystemConfig'] = 'myTestPlatform'
    mb.workflow_commons['StartFrom'] = 94
    mb.workflow_commons['NbOfEvts'] = 458
    mb.workflow_commons['ParametricInputData'] = ''
    mb.OutputFile = None
    mb.step_commons['OutputFile'] = 'myTestOFile.txt'
    with patch('%s.getNumberOfEvents' % MODULE_NAME, new=Mock(return_value=S_ERROR('getNbEvtsFails_test'))), \
         patch('%s.ModuleBase.applicationSpecificInputs' % MODULE_NAME, new=Mock(return_value=S_ERROR('test_failthisappspec'))):
      result = mb.resolveInputVariables()
      assertDiracFailsWith( result, 'test_failthisappspec', self )

  def test_resolveinputvars_parametricdata_2( self ):
    mb = self.moba
    mb.workflow_commons['IS_PROD'] = True
    mb.workflow_commons['PRODUCTION_ID'] = 13412
    mb.workflow_commons['SystemConfig'] = 'myTestPlatform'
    mb.workflow_commons['StartFrom'] = 94
    mb.workflow_commons['NbOfEvts'] = 458
    mb.workflow_commons['ParametricInputData'] = [ 'myParamEntry1',
                                                   'LFN:someoTherEntry',
                                                   'LFN:dontforgetme']
    mb.OutputFile = None
    mb.step_commons['OutputFile'] = 'myTestOFile.txt'
    with patch('%s.getNumberOfEvents' % MODULE_NAME, new=Mock(return_value=S_ERROR('getNbEvtsFails_test'))), \
         patch('%s.ModuleBase.applicationSpecificInputs' % MODULE_NAME, new=Mock(return_value=S_ERROR('test_failthisappspec'))):
      result = mb.resolveInputVariables()
      assertDiracFailsWith( result, 'test_failthisappspec', self )

  def test_resolveinputvars_getnbevts_fails( self ):
    mb = self.moba
    mb.workflow_commons['PRODUCTION_ID'] = 13412
    mb.workflow_commons['SystemConfig'] = 'myTestPlatform'
    mb.workflow_commons['StartFrom'] = 94
    mb.workflow_commons['InputData'] = 'myinputData1Test;myinputData2Test;myinputData3Test'
    mb.workflow_commons['ParametricInputData'] = [ 'myParamEntry1',
                                                   'LFN:someoTherEntry',
                                                   'LFN:dontforgetme']
    mb.OutputFile = None
    mb.step_commons['OutputFile'] = 'myTestOFile.txt'
    with patch('%s.getNumberOfEvents' % MODULE_NAME, new=Mock(return_value=S_ERROR('getNbEvtsFails_test'))), \
         patch('%s.ModuleBase.applicationSpecificInputs' % MODULE_NAME, new=Mock(return_value=S_OK('bla'))):
      result = mb.resolveInputVariables()
      assertDiracSucceedsWith_equals( result, 'Parameters resolved', self )
      print(self.log_mock.mock_calls)
      self.log_mock.warn.assert_called_once_with(
          'Failed to get NumberOfEvents from FileCatalog, but this is not a production job')

  def test_resolveinputvars_getnbevts_zero( self ):
    mb = self.moba
    mb.workflow_commons['PRODUCTION_ID'] = 13412
    mb.workflow_commons['SystemConfig'] = 'myTestPlatform'
    mb.workflow_commons['StartFrom'] = 94
    mb.workflow_commons['NbOfEvts'] = 458
    mb.workflow_commons['ParametricInputData'] = [ 'myParamEntry1',
                                                   'LFN:someoTherEntry',
                                                   'LFN:dontforgetme']
    mb.workflow_commons['InputData'] = 'myinputData1Test;myinputData2Test;myinputData3Test'
    mb.OutputFile = None
    mb.step_commons['OutputFile'] = 'myTestOFile.txt'
    with patch('%s.getNumberOfEvents' % MODULE_NAME, new=Mock(return_value=S_OK({ 'nbevts' : 0, 'AdditionalMeta' : {} }))), \
         patch('%s.ModuleBase.applicationSpecificInputs' % MODULE_NAME, new=Mock(return_value=S_OK('bla'))):
      result = mb.resolveInputVariables()
      assertDiracSucceedsWith_equals( result, 'Parameters resolved', self )

  def test_finalstatusreport( self ):
    self.moba.ignoreapperrors = False
    with patch('%s.ModuleBase.setApplicationStatus' % MODULE_NAME) as appstat_mock:
      assertDiracSucceedsWith( self.moba.finalStatusReport( 0 ), ' Successful', self )
      appstat_mock.assert_called_once_with( '  Successful' )
      self.assertFalse(self.log_mock.error.called)

  def test_finalstatusreport_appfailed( self ):
    self.moba.ignoreapperrors = False
    with patch('%s.ModuleBase.setApplicationStatus' % MODULE_NAME) as appstat_mock:
      assertDiracFailsWith( self.moba.finalStatusReport( 1 ), '', self )
      appstat_mock.assert_called_once_with( ' exited With Status 1' )
      self.assertTrue(self.log_mock.error.called)

  def test_finalstatusreport_ignorefail( self ):
    self.moba.ignoreapperrors = True
    with patch('%s.ModuleBase.setApplicationStatus' % MODULE_NAME) as appstat_mock:
      assertDiracSucceedsWith( self.moba.finalStatusReport( 1 ), ' exited With Status 1', self )
      appstat_mock.assert_called_once_with( ' exited With Status 1' )
      self.assertTrue(self.log_mock.error.called)

  def test_generatefailover( self ):
    container_mock = Mock()
    container_mock.__len__.return_value = 1
    container_mock.toJSON.return_value = S_OK( 'myjsonfilecontent_testme' )
    reqval_mock = Mock()
    reqval_mock.validate.return_value = S_OK('ok')
    reqval_mock.getJSONData.return_value = True
    request_mock = Mock()
    request_mock.getJSONData.return_value = True
    report_mock = Mock()
    report_mock.generateForwardDISET.return_value = S_OK(request_mock)
    report_mock.getJSONData.return_value = True
    self.moba.jobReport = report_mock
    with patch('%s.open' % MODULE_NAME, mock_open()) as mo, \
         patch('%s.RequestValidator' % MODULE_NAME, new=Mock(return_value=reqval_mock)), \
         patch('%s.ModuleBase._getRequestContainer' % MODULE_NAME, new=Mock(return_value=container_mock)):
      result = self.moba.generateFailoverFile()
      assertDiracSucceeds( result, self )
      mo.assert_any_call( '0_0_request.json', 'w' )
      mo = mo()
      mo.write.assert_called_once_with( 'myjsonfilecontent_testme' )

  def test_generatefailover_optionalfails( self ):
    container_mock = Mock()
    container_mock.__len__.return_value = 1
    container_mock.toJSON.return_value = S_OK( 'myjsonfilecontent_testme' )
    container_mock.getDigest.return_value = S_ERROR( 'bla' )
    reqval_mock = Mock()
    reqval_mock.validate.return_value = S_OK('ok')
    reqval_mock.getJSONData.return_value = True
    request_mock = Mock()
    request_mock.getJSONData.return_value = True
    report_mock = Mock()
    report_mock.generateForwardDISET.return_value = S_ERROR( 'test_faildiset' )
    report_mock.getJSONData.return_value = True
    self.moba.jobReport = report_mock
    accreport_mock = Mock()
    accreport_mock.commit.return_value = S_OK( '' )
    self.moba.workflowStatus['OK'] = False
    self.moba.workflow_commons['AccountingReport'] = accreport_mock
    self.moba.workflow_commons['ProductionOutputData'] = 'mylfn1;testmelfn2;'
    with patch('%s.open' % MODULE_NAME, mock_open()) as mo, \
         patch('%s.RequestValidator' % MODULE_NAME, new=Mock(return_value=reqval_mock)), \
         patch('%s.ModuleBase._getRequestContainer' % MODULE_NAME, new=Mock(return_value=container_mock)), \
         patch('%s.ModuleBase.setApplicationStatus' % MODULE_NAME) as appstat_mock, \
         patch('%s.ModuleBase._cleanUp' % MODULE_NAME) as cleanup_mock:
      result = self.moba.generateFailoverFile()
      assertDiracSucceeds( result, self )
      mo.assert_any_call( '0_0_request.json', 'w' )
      mo = mo()
      mo.write.assert_called_once_with( 'myjsonfilecontent_testme' )
      appstat_mock.assert_called_once_with( 'Creating Removal Requests' )
      cleanup_mock.assert_called_once_with( [ 'mylfn1', 'testmelfn2', '' ] )

  def test_generatefailover_commit_fails( self ):
    container_mock = Mock()
    container_mock.__len__.return_value = 1
    container_mock.toJSON.return_value = S_OK( 'myjsonfilecontent_testme' )
    container_mock.getDigest = S_ERROR( 'bla' )
    reqval_mock = Mock()
    reqval_mock.validate.return_value = S_OK('ok')
    reqval_mock.getJSONData.return_value = True
    request_mock = Mock()
    request_mock.getJSONData.return_value = True
    report_mock = Mock()
    report_mock.generateForwardDISET.return_value = S_ERROR( 'test_faildiset' )
    report_mock.getJSONData.return_value = True
    self.moba.jobReport = report_mock
    accreport_mock = Mock()
    accreport_mock.commit.return_value = S_ERROR( 'test_commiterr')
    self.moba.workflowStatus['OK'] = False
    self.moba.workflow_commons['AccountingReport'] = accreport_mock
    self.moba.workflow_commons['ProductionOutputData'] = 'mylfn1;testmelfn2;'
    with patch('%s.RequestValidator' % MODULE_NAME, new=Mock(return_value=reqval_mock)), \
         patch('%s.ModuleBase._getRequestContainer' % MODULE_NAME, new=Mock(return_value=container_mock)):
      result = self.moba.generateFailoverFile()
      assertDiracFailsWith( result, 'test_commiterr', self )

  def test_generatefailover_norequests( self ):
    container_mock = Mock()
    container_mock.__len__.return_value = 0
    container_mock.toJSON.return_value = S_OK( 'myjsonfilecontent_testme' )
    report_mock = Mock()
    request_mock = Mock()
    request_mock.getJSONData.return_value = True
    report_mock.generateForwardDISET.return_value = S_OK(request_mock)
    report_mock.getJSONData.return_value = True
    self.moba.jobReport = report_mock
    with patch('%s.ModuleBase._getRequestContainer' % MODULE_NAME, new=Mock(return_value=container_mock)):
      result = self.moba.generateFailoverFile()
      assertDiracSucceeds( result, self )
      self.log_mock.info.assert_any_call('No Requests to process ')

  def test_generatefailover_validatefails( self ):
    container_mock = Mock()
    container_mock.__len__.return_value = 1
    container_mock.toJSON.return_value = S_OK( 'myjsonfilecontent_testme' )
    reqval_mock = Mock()
    reqval_mock.validate.return_value = S_ERROR('test_fail-validate')
    reqval_mock.getJSONData.return_value = True
    request_mock = Mock()
    request_mock.getJSONData.return_value = True
    report_mock = Mock()
    report_mock.generateForwardDISET.return_value = S_OK(request_mock)
    report_mock.getJSONData.return_value = True
    self.moba.jobReport = report_mock
    with patch('%s.RequestValidator' % MODULE_NAME, new=Mock(return_value=reqval_mock)), \
         patch('%s.ModuleBase._getRequestContainer' % MODULE_NAME, new=Mock(return_value=container_mock)):
      with self.assertRaises( RuntimeError ) as re:
        self.moba.generateFailoverFile()
        assertEqualsImproved( re.value, 'Failover request is not valid: test_fail-validate', self ) #pylint: disable=no-member

  def test_generatefailover_tojsonfails( self ):
    container_mock = Mock()
    container_mock.__len__.return_value = 1
    container_mock.toJSON.return_value = S_ERROR( 'test_tojsonerror' )
    reqval_mock = Mock()
    reqval_mock.validate.return_value = S_OK('ok')
    reqval_mock.getJSONData.return_value = True
    request_mock = Mock()
    request_mock.getJSONData.return_value = True
    report_mock = Mock()
    report_mock.generateForwardDISET.return_value = S_OK(request_mock)
    report_mock.getJSONData.return_value = True
    self.moba.jobReport = report_mock
    with patch('%s.RequestValidator' % MODULE_NAME, new=Mock(return_value=reqval_mock)), \
         patch('%s.ModuleBase._getRequestContainer' % MODULE_NAME, new=Mock(return_value=container_mock)):
      with self.assertRaises( RuntimeError ) as re:
        self.moba.generateFailoverFile()
        assertEqualsImproved( re.value, 'test_tojsonerror', self ) #pylint: disable=no-member

#TODO Fix this/remove tests?
  def test_redirectlogoutput( self ):
    self.moba.eventstring = None
    with patch('sys.stdout', new_callable=StringIO) as print_mock, \
         patch('%s.open' % MODULE_NAME, mock_open()) as open_mock:
      self.assertIsNone( self.moba.redirectLogOutput( 1, 'mytestmessage' ) )
      #if print_mock.getvalue() not in [ 'mytestmessage\n', '' ]:
      if print_mock.getvalue() not in [ 'mytestmessage\n' ]:
        self.fail( 'Suitable output not found' )
      self.assertFalse( open_mock.called )
      assertEqualsImproved( self.moba.stdError, 'mytestmessage', self )

  def test_redirectlogoutput_emptymsg( self ):
    self.assertIsNone( self.moba.redirectLogOutput( 'fd', '' ) )

  def test_redirectlogoutput_default( self ):
    self.moba.eventstring = 'testevent123'
    with patch('sys.stdout', new_callable=StringIO) as print_mock, \
         patch('%s.open' % MODULE_NAME, mock_open()) as open_mock:
      self.assertIsNone( self.moba.redirectLogOutput(
        0, 'testevent123 has happened! quick, print it!' ) )
      #if print_mock.getvalue() not in [ 'testevent123 has happened! quick, print it!\n', '' ]:
      if print_mock.getvalue() not in [ 'testevent123 has happened! quick, print it!\n' ]:
       self.fail( 'Suitable output not found' )
      self.assertFalse( open_mock.called )
      assertEqualsImproved( self.moba.stdError, '', self )

  def test_redirectlogoutput_writetofile( self ):
    self.moba.eventstring = None
    self.moba.applicationLog = 'appLog.txt'
    self.moba.excludeAllButEventString = False
    with patch('sys.stdout', new_callable=StringIO) as print_mock, \
         patch('%s.open' % MODULE_NAME, mock_open()) as open_mock:
      self.assertIsNone( self.moba.redirectLogOutput( 1, 'mytestmessage' ) )
      if print_mock.getvalue() not in [ 'mytestmessage\n', '' ]:
        self.fail( 'Suitable output not found' )
      open_mock.assert_any_call( 'appLog.txt', 'a' )
      open_mock = open_mock()
      open_mock.write.assert_called_once_with( 'mytestmessage\n' )
      assertEqualsImproved( self.moba.stdError, 'mytestmessage', self )

  def test_redirectlogoutput_writetofile_2( self ):
    self.moba.eventstring = ''
    self.moba.applicationLog = 'appLog.txt'
    self.moba.excludeAllButEventString = True
    with patch('sys.stdout', new_callable=StringIO) as print_mock, \
         patch('%s.open' % MODULE_NAME, mock_open()) as open_mock:
      self.assertIsNone( self.moba.redirectLogOutput( 0, 'mytestmessage' ) )
      assert print_mock.getvalue() == ''
      open_mock.assert_any_call( 'appLog.txt', 'a' )
      self.assertFalse( open_mock().called )

  def test_redirectlogoutput_writetofile_3( self ):
    self.moba.eventstring = [ 'somepattern' , 'otherpattern' ]
    self.moba.applicationLog = 'appLog.txt'
    self.moba.excludeAllButEventString = True
    with patch('sys.stdout', new_callable=StringIO) as print_mock, \
         patch('%s.open' % MODULE_NAME, mock_open()) as open_mock:
      self.assertIsNone( self.moba.redirectLogOutput( 0, 'mytestmessage' ) )
      assert print_mock.getvalue() == ''
      open_mock.assert_any_call( 'appLog.txt', 'a' )
      open_mock = open_mock()
      self.assertFalse( open_mock.write.called )

  def test_redirectlogoutput_writetofile_4( self ):
    self.moba.eventstring = [ 'ignorethis', 'deletethis', 'specialTestEvent']
    self.moba.applicationLog = 'appLog.txt'
    self.moba.excludeAllButEventString = True
    with patch('sys.stdout', new_callable=StringIO) as print_mock, \
         patch('%s.open' % MODULE_NAME, mock_open()) as open_mock:
      self.assertIsNone( self.moba.redirectLogOutput( 1, '1390specialTestEvente89f' ) )
      if print_mock.getvalue() not in [ '1390specialTestEvente89f\n', '' ]:
        self.fail( 'Suitable output not found' )
      open_mock.assert_any_call( 'appLog.txt', 'a' )
      open_mock = open_mock()
      open_mock.write.assert_called_once_with( '1390specialTestEvente89f\n' )
      assertEqualsImproved( self.moba.stdError, '1390specialTestEvente89f', self )

  def test_cleanup( self ):
    file_mock = Mock()
    with patch('%s.File' % MODULE_NAME, new=Mock(return_value=file_mock)):
      result = self.moba._cleanUp( [ 'lfnlist' ] ) #pylint: disable=protected-access
      assertDiracSucceeds( result, self )
      self.assertIsNotNone( self.moba.workflow_commons[ 'Request' ] )

  def test_logworkingdirectory( self ):
    with patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK( [ 0, 'balblabal', '' ] ))):
      self.moba.logWorkingDirectory()
      self.log_mock.info.assert_any_call('balblabal')
      self.assertFalse(self.log_mock.error.called)

  def test_logworkingdirectory_fails_1( self ):
    with patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK( [ 1, 'balblabal', 'test_err' ] ))):
      self.moba.logWorkingDirectory()
      self.assertTrue(self.log_mock.error.called)
      self.assertFalse(self.log_mock.info.called)

  def test_logworkingdirectory_fails_2( self ):
    error = S_ERROR( [ 0, 'balblabal', 'myerrormsg' ] )
    error['Value'] = 'efopikif'
    with patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=error)):
      self.moba.logWorkingDirectory()
      self.assertTrue(self.log_mock.error.called)
      self.assertFalse(self.log_mock.info.called)

# TODO Check for appropriately set values
