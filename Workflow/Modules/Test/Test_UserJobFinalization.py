#!/usr/bin/env python
""" Test the UserJobFinalization module """

from __future__ import print_function
import os
import unittest
from mock import patch, call, mock_open, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Workflow.Modules.UserJobFinalization import UserJobFinalization
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, \
  assertDiracFailsWith, assertDiracSucceeds, assertDiracSucceedsWith, \
  assertDiracSucceedsWith_equals, assertMockCalls

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Workflow.Modules.UserJobFinalization'

@patch('%s.gConfig.getValue' % MODULE_NAME, new=Mock(return_value=[]))
class TestUserJobFinalization( unittest.TestCase ):
  """ Test the UserJobFinalization module
  """

  def setUp( self ):
    self.ujf = UserJobFinalization()
    self.ujf.defaultOutputSE = []

  @patch.dict(os.environ, { 'JOBID' : '123956' }, clear=True)
  def test_application_specific_inputs( self ):
    self.ujf.userOutputData = 'something1;something2'
    tmpSE = self.ujf.workflow_commons.get('UserOutputSE', '')
    self.ujf.workflow_commons['UserOutputSE'] = []
    report_mock = Mock(return_value=813412)
    self.ujf.jobReport = report_mock
    result = self.ujf.applicationSpecificInputs()
    assertDiracSucceedsWith_equals( result, 'Parameters resolved', self )
    assertEqualsImproved( self.ujf.enable, True, self )
    assertEqualsImproved( self.ujf.jobID, '123956', self )
    assertEqualsImproved( self.ujf.userOutputData, [ 'something1',
                                                     'something2' ], self )
    assertEqualsImproved( self.ujf.userOutputSE, [], self )
    assertEqualsImproved( self.ujf.userOutputPath, '', self )
    assertEqualsImproved( self.ujf.jobReport, report_mock, self )
    self.ujf.workflow_commons['UserOutputSE'] = tmpSE

  def test_application_specific_inputs_corrupt_state( self ):
    self.ujf.enable = 123
    log_mock = Mock()
    self.ujf.log = log_mock
    self.ujf.userOutputData = [ 'testentryOutputData' ]
    self.ujf.workflow_commons = {
      'UserOutputSE' : ' Some TestOutput Storage;and its brother',
      'UserOutputPath' : 'testOutputPathWwo' }
    self.ujf.jobID = 87942
    with patch.dict('os.environ', {}, clear=True):
      result = self.ujf.applicationSpecificInputs()
      assertDiracSucceedsWith_equals( result, 'Parameters resolved', self )
      assertEqualsImproved( self.ujf.jobID, 87942, self )
      assertEqualsImproved( self.ujf.enable, False, self )
      assertEqualsImproved( self.ujf.userOutputData, [ 'testentryOutputData' ],
                            self )
      assertEqualsImproved( self.ujf.userOutputSE, [ 'Some TestOutput Storage',
                                                     'and its brother' ], self )
      assertEqualsImproved( self.ujf.userOutputPath, 'testOutputPathWwo', self )

  @patch.dict(os.environ, {}, clear=True)
  def test_application_specific_inputs_jobid_zero( self ):
    self.ujf.jobID = 0
    self.ujf.enable = True
    result = self.ujf.applicationSpecificInputs()
    assertDiracSucceedsWith_equals( result, 'Parameters resolved', self )
    assertEqualsImproved( self.ujf.jobID, 0, self )
    assertEqualsImproved( self.ujf.enable, False, self )

  def test_getcurrentowner( self ):
    with patch('%s.getProxyInfo' % MODULE_NAME, new=Mock(return_value=S_OK({ 'myproxydict' : True, 'username' : 'readThisOut' }))):
      result = self.ujf.getCurrentOwner()
      assertDiracSucceedsWith( result, 'readThisOut', self )

  def test_getcurrentowner_getproxy_fails( self ):
    with patch('%s.getProxyInfo' % MODULE_NAME, new=Mock(return_value=S_ERROR('test_getproxy_fails'))):
      result = self.ujf.getCurrentOwner()
      assertDiracFailsWith( result, 'could not obtain proxy information', self )

  def test_getcurrentowner_username_missing( self ):
    with patch('%s.getProxyInfo' % MODULE_NAME, new=Mock(return_value=S_OK({ 'myproxydict' : True }))):
      result = self.ujf.getCurrentOwner()
      assertDiracFailsWith( result, 'could not get username from proxy', self )

  def test_getcurrentvo( self ):
    with patch('%s.getProxyInfo' % MODULE_NAME, new=Mock(return_value=S_OK({ 'myproxydict' : True, 'group' : 'testilc_blabla' }))):
      result = self.ujf.getCurrentVO()
      assertDiracSucceedsWith( result, 'testilc', self )

  def test_getcurrentvo_getproxy_fails( self ):
    with patch('%s.getProxyInfo' % MODULE_NAME, new=Mock(return_value=S_ERROR('test_getproxy_fails'))):
      result = self.ujf.getCurrentVO()
      assertDiracFailsWith( result, 'could not obtain proxy information', self )

  def test_getcurrentvo_group_missing( self ):
    with patch('%s.getProxyInfo' % MODULE_NAME, new=Mock(return_value=S_OK({ 'myproxydict' : True }))):
      result = self.ujf.getCurrentVO()
      assertDiracFailsWith( result, 'could not get group from proxy', self )

  def test_constructoutputlfns( self ):
    self.ujf.jobID = 0
    self.ujf.workflow_commons = {}
    self.ujf.userOutputData = 'testOutputData'
    self.ujf.userOutputPath = 'testOutputPath'
    with patch('%s.getProxyInfo' % MODULE_NAME, new=Mock(return_value=S_OK({ 'username' : 'myTestUser123', 'group' : 'TeILCst_bla'}))), \
         patch('%s.constructUserLFNs' % MODULE_NAME, new=Mock(return_value=S_OK('constructedLFNs'))) as construct_mock:
      result = self.ujf.constructOutputLFNs()
      assertDiracSucceedsWith( result, 'constructedLFNs', self )
      construct_mock.assert_called_once_with( 12345, 'TeILCst', 'myTestUser123',
                                              'testOutputData', 'testOutputPath' )
      assertEqualsImproved( self.ujf.jobID, 12345, self )

  def test_constructoutputlfns_proxy_missing( self ):
    self.ujf.jobID = 8234
    self.ujf.workflow_commons = {}
    with patch('%s.getProxyInfo' % MODULE_NAME, new=Mock(return_value=S_ERROR('proxy_err_testme'))):
      result = self.ujf.constructOutputLFNs()
      assertDiracFailsWith( result, 'could not obtain owner from proxy', self )
      assertEqualsImproved( self.ujf.jobID, 8234, self )

  def test_constructoutputlfns_vo_missing( self ):
    self.ujf.workflow_commons = {}
    with patch('%s.getProxyInfo' % MODULE_NAME, new=Mock(return_value=S_OK({ 'username' : 'user943' }))):
      result = self.ujf.constructOutputLFNs()
      assertDiracFailsWith( result, 'could not obtain VO from proxy', self )

  def test_constructoutputlfns_construction_fails( self ):
    self.ujf.jobID = 0
    self.ujf.workflow_commons = {}
    self.ujf.userOutputData = 'testOutputData'
    self.ujf.userOutputPath = 'testOutputPath'
    with patch('%s.getProxyInfo' % MODULE_NAME, new=Mock(return_value=S_OK({ 'username' : 'myTestUser123', 'group' : 'TeILCst_bla'}))), \
         patch('%s.constructUserLFNs' % MODULE_NAME, new=Mock(return_value=S_ERROR('construction_failed_testerr'))) as construct_mock:
      result = self.ujf.constructOutputLFNs()
      assertDiracFailsWith( result, 'construction_failed_testerr', self )
      construct_mock.assert_called_once_with( 12345, 'TeILCst', 'myTestUser123', 'testOutputData', 'testOutputPath' )

  def test_transfer_register_and_failover_files( self ):
    self.ujf.failoverSEs = [ 'testSite1', 'testSite2', 'myTestSite', 'privateSite4', 'etc' ]
    self.ujf.userFileCatalog = 'blaCatalogTestme'
    transfer_mock = Mock()
    transfer_mock.transferAndRegisterFileFailover.return_value = S_OK( {} )
    filesUploaded = []
    with patch('%s.random.shuffle' % MODULE_NAME) as shuffle_mock:
      result = self.ujf.transferRegisterAndFailoverFiles( transfer_mock, {
        'filename1test' : { 'metadatadict1' : True,
                            'resolvedSE' : ( 'etc', 'privateSite4' ),
                            'localpath' : '/my/local/path/Testme.txt',
                            'lfn' : 'LFN:/ilc/mytest/LFN.txt', 'filedict' : 89546 },
        'my_other_testfile' : {
          'metadatadict1' : True, 'resolvedSE' : ( 'testSite1', 'privateSite4' ),
          'localpath' : '/my/local/path/Testme2.txt', 'lfn' : 'LFN:/ilc/othertest/new_lfn.lfn',
          'filedict' : 475 } }, filesUploaded )
      assertDiracSucceeds( result, self )
      assertEqualsImproved( result['Value'], dict(cleanUp=False), self )
      assertMockCalls( shuffle_mock, [ [ 'testSite1', 'testSite2', 'myTestSite', 'privateSite4', 'etc' ],
                                       [ 'testSite1', 'testSite2', 'myTestSite', 'privateSite4', 'etc' ] ],
                       self )
      assertEqualsImproved( transfer_mock.transferAndRegisterFileFailover.mock_calls,
                            [ call( 'my_other_testfile', '/my/local/path/Testme2.txt',
                                    'LFN:/ilc/othertest/new_lfn.lfn', 'testSite1',
                                    [ 'testSite2', 'myTestSite', 'privateSite4', 'etc' ],
                                    fileCatalog='blaCatalogTestme', fileMetaDict=475 ),
                              call( 'filename1test', '/my/local/path/Testme.txt', 'LFN:/ilc/mytest/LFN.txt',
                                    'etc', [ 'testSite1', 'testSite2', 'myTestSite', 'privateSite4' ],
                                    fileCatalog='blaCatalogTestme', fileMetaDict=89546 ) ], self )
      self.assertIn( 'LFN:/ilc/mytest/LFN.txt', filesUploaded )
      self.assertIn( 'LFN:/ilc/othertest/new_lfn.lfn', filesUploaded )

  def test_transferreg_transfer_fails( self ):
    self.ujf.failoverSEs = [ 'testSite1', 'testSite2', 'myTestSite',
                             'privateSite4', 'etc' ]
    self.ujf.userFileCatalog = 'blaCatalogTestme'
    transfer_mock = Mock()
    transfer_mock.transferAndRegisterFileFailover.return_value = S_ERROR( 'some_transfer_test_err' )
    filesUploaded = []
    with patch('%s.random.shuffle' % MODULE_NAME) as shuffle_mock:
      result = self.ujf.transferRegisterAndFailoverFiles( transfer_mock, {
        'filename1test' : { 'metadatadict1' : True,
                            'resolvedSE' : ( 'nonexistant', ) ,
                            'localpath' : '/my/local/path/Testme.txt',
                            'lfn' : 'LFN:/ilc/mytest/LFN.txt', 'filedict' : 89546
                          } }, filesUploaded )
      assertDiracSucceeds( result, self )
      assertEqualsImproved( result['Value'], dict(cleanUp=True), self )
      shuffle_mock.assert_called_once_with( [ 'testSite1', 'testSite2',
                                              'myTestSite', 'privateSite4',
                                              'etc' ] )
      transfer_mock.transferAndRegisterFileFailover.assert_called_once_with(
        'filename1test', '/my/local/path/Testme.txt', 'LFN:/ilc/mytest/LFN.txt',
        'nonexistant', [ 'testSite1', 'testSite2', 'myTestSite', 'privateSite4',
                         'etc' ], fileCatalog='blaCatalogTestme',
        fileMetaDict=89546 )
      assertEqualsImproved( filesUploaded, [], self )

  def test_transferreg_failoverses_empty( self ):
    self.ujf.failoverSEs = [ 'justOneSite' ]
    self.ujf.userFileCatalog = 'blaCatalogTestme'
    transfer_mock = Mock()
    transfer_mock.transferAndRegisterFileFailover.return_value = S_ERROR( 'some_transfer_test_err' )
    filesUploaded = []
    with patch('%s.random.shuffle' % MODULE_NAME) as shuffle_mock:
      result = self.ujf.transferRegisterAndFailoverFiles( transfer_mock, {
        'filename1test' : { 'metadatadict1' : True,
                            'resolvedSE' : ( 'justOneSite', ) ,
                            'localpath' : '/my/local/path/Testme.txt',
                            'lfn' : 'LFN:/ilc/mytest/LFN.txt',
                            'filedict' : 89546 } }, filesUploaded )
      assertDiracSucceeds( result, self )
      assertEqualsImproved( result['Value'], dict(cleanUp=True), self )
      shuffle_mock.assert_called_once_with( [ 'justOneSite' ] )
      self.assertFalse( transfer_mock.transferAndRegisterFileFailover.called )
      assertEqualsImproved( filesUploaded, [], self )

  #pylint: disable=unused-argument
  def test_transferreg_some_files_fail( self ):
    self.ujf.failoverSEs = [ 'onlyAvailableSE' ]
    self.ujf.userFileCatalog = 'myCat'
    transfer_mock = Mock()
    def my_transfer_mock( fileName, localpath, lfn, targetSE, failoverSEs,
                          fileCatalog = None, fileMetaDict = None ):
      """ Return S_ERROR for the appropriate file from the dictionary and
      S_OK for the rest. Necssary since dictionary iteration order is random
      """
      return TRANSFER_AND_REGISTER_DICT[fileName]
    transfer_mock.transferAndRegisterFileFailover.side_effect = my_transfer_mock
    filesUploaded = []
    with patch('%s.random.shuffle' % MODULE_NAME) as shuffle_mock:
      result = self.ujf.transferRegisterAndFailoverFiles( transfer_mock, {
        'noMoreSEs' : { 'metadatadict1' : True,
                        'resolvedSE' : ( 'onlyAvailableSE',  ),
                        'localpath' : '/doesntmatter',
                        'lfn' : 'doesntmattereither', 'filedict' : 32 },
        'fail_transfer' : {
          'metadatadict1' : True, 'resolvedSE' : ( 'someOtherSite', ),
          'localpath' : '/matterdoesnt', 'lfn' : 'neither', 'filedict' : 92 },
        'workingFile1' : { 'resolvedSE' : ( 'someOtherSite', ),
                           'localpath' : '/my/local/first/path',
                           'lfn' : 'LFN:/ilc/some/dir/file1.txt',
                           'filedict' : 8520 }, 'thisFileWorks.too' :
        { 'resolvedSE' : ( 'someOtherOtherSite', ),
          'localpath' : '/dir/current/local.lfn',
          'lfn' : 'LFN:/ilc/mydir/file2.ppt', 'filedict' : 98453 }
      }, filesUploaded )
      assertDiracSucceeds( result, self )
      assertEqualsImproved( result['Value'], dict(cleanUp=True), self )
      assertMockCalls( shuffle_mock, [ [ 'onlyAvailableSE' ] ] * 4, self )
      assertEqualsImproved( transfer_mock.transferAndRegisterFileFailover.mock_calls, [
        call( 'workingFile1', '/my/local/first/path', 'LFN:/ilc/some/dir/file1.txt', 'someOtherSite',
              [ 'onlyAvailableSE' ], fileCatalog = 'myCat', fileMetaDict = 8520 ),
        call( 'fail_transfer', '/matterdoesnt', 'neither', 'someOtherSite', [ 'onlyAvailableSE' ],
              fileCatalog = 'myCat', fileMetaDict = 92 ),
        call( 'thisFileWorks.too', '/dir/current/local.lfn', 'LFN:/ilc/mydir/file2.ppt', 'someOtherOtherSite',
              [ 'onlyAvailableSE' ], fileCatalog = 'myCat', fileMetaDict = 98453 ) ], self )
      assertEqualsImproved( filesUploaded, [ 'LFN:/ilc/some/dir/file1.txt', 'LFN:/ilc/mydir/file2.ppt' ], self )

@patch('%s.UserJobFinalization.logWorkingDirectory' % MODULE_NAME, new=Mock())
class TestExecute( unittest.TestCase ):
  """ Test the execute() method
  """

  def setUp( self ):
    self.ujf = UserJobFinalization()
    self.ujf.defaultOutputSE = []
    self.ujf.step_commons = { 'STEP_NUMBER' : '42' }
    self.ujf.workflow_commons = { 'TotalSteps' : '42' }

  def test_execute( self ):
    exists_dict = { 'list_of.txt' : True, 'filenames.jar' : True }
    self.ujf.jobID = 512
    request_mock = Mock()
    request_mock.RequestName = 'job_512_request.xml'
    request_mock.JobID = 512
    request_mock.SourceComponent = "Job_512"
    self.ujf.workflow_commons = { 'TotalSteps' : '42', 'Request' : request_mock,
                                  'JOB_ID' : '512', 'IS_PROD' : True,
                                  'PRODUCTION_ID' : '98245',
                                  'Platform' : 'myTestPlatform',
                                  'Owner' : 'myTestOwner123RichGuy',
                                  'VO' : 'myTestVirtualOrga',
                                  'UserOutputSE' : 'myTestReceivingSE' }
    transfer_mock = Mock()
    transfer_mock.transferAndRegisterFile.return_value = S_OK( {} )
    transfer_mock.transferAndRegisterFileFailover.return_value = S_OK()
    dataman_mock = Mock()
    dataman_mock.replicateAndRegister.return_value = S_OK()
    self.ujf.workflowStatus = S_OK()
    self.ujf.stepStatus = S_OK()
    self.ujf.userOutputData = [ 'list_of.txt', 'filenames.jar' ]
    self.ujf.userOutputSE = 'thisValueIsntUsed'
    self.ujf.userOutputPath = 'my/User/OPPath'
    with patch('%s.constructUserLFNs' % MODULE_NAME, new=Mock(return_value=S_OK(['/myTestVirtualOrga/testpre/m/myTestOwner123RichGuy/my/User/OPPath/list_of.txt', '/myTestVirtualOrga/testpre/m/myTestOwner123RichGuy/my/User/OPPath/filenames.jar']))) as constructlfn_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=lambda path: exists_dict[path])) as exists_mock, \
         patch('%s.open' % MODULE_NAME, mock_open(read_data='myfilecontent,makeniceadlerchecksum')), \
         patch('%s.os.path.getsize' % MODULE_NAME, new=Mock(return_value=3048)), \
         patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/mycurdirTestMe')), \
         patch('%s.getDestinationSEList' % MODULE_NAME, new=Mock(return_value=S_OK(['CERN-DIP-4']))), \
         patch('%s.FailoverTransfer' % MODULE_NAME, new=transfer_mock), \
         patch('%s.DataManager' % MODULE_NAME, new=dataman_mock), \
         patch('%s.time.sleep' % MODULE_NAME, new=Mock(return_value=True)):
      result = self.ujf.execute()
    assertDiracSucceeds( result, self )
    constructlfn_mock.assert_called_once_with(
      512, 'myTestVirtualOrga', 'myTestOwner123RichGuy',
      [ 'list_of.txt', 'filenames.jar' ], 'my/User/OPPath' )
    transfer_mock = transfer_mock() # Necessary for the assumptions
    transfer_mock.transferAndRegisterFile.assert_any_call(
      'list_of.txt', '/mycurdirTestMe/list_of.txt',
      '/myTestVirtualOrga/testpre/m/myTestOwner123RichGuy/my/User/OPPath/list_of.txt',
      [ 'myTestReceivingSE', 'CERN-DIP-4' ], fileCatalog=['FileCatalog'],
      fileMetaDict={
        'Status' : 'Waiting', 'ADLER32' : False, 'ChecksumType' : 'ADLER32',
        'Checksum' : False,
        'LFN' : '/myTestVirtualOrga/testpre/m/myTestOwner123RichGuy/my/User/OPPath/list_of.txt',
        'GUID' : None, 'Addler' : False, 'Size' : 3048 } )
    transfer_mock.transferAndRegisterFile.assert_called_with(
      'filenames.jar', '/mycurdirTestMe/filenames.jar',
      '/myTestVirtualOrga/testpre/m/myTestOwner123RichGuy/my/User/OPPath/filenames.jar',
      [ 'myTestReceivingSE', 'CERN-DIP-4' ], fileCatalog=['FileCatalog'],
      fileMetaDict={ 'Status' : 'Waiting', 'ADLER32' : False,
                     'ChecksumType' : 'ADLER32', 'Checksum' : False,
                     'LFN' : '/myTestVirtualOrga/testpre/m/myTestOwner123RichGuy/my/User/OPPath/filenames.jar',
                     'GUID' : None, 'Addler' : False, 'Size' : 3048 } )
    assertEqualsImproved( len(transfer_mock.transferAndRegisterFile.mock_calls), 14, self )
    self.assertFalse( transfer_mock.transferAndRegisterFileFailover.called )
    dataman_mock = dataman_mock()
    assertMockCalls( dataman_mock.replicateAndRegister,
                     [ ('/myTestVirtualOrga/testpre/m/myTestOwner123RichGuy/my/User/OPPath/filenames.jar',
                        'myTestReceivingSE' ),
                       ( '/myTestVirtualOrga/testpre/m/myTestOwner123RichGuy/my/User/OPPath/list_of.txt',
                         'myTestReceivingSE' ) ], self, only_these_calls = False )
    assertEqualsImproved( len(exists_mock.mock_calls), 2, self )

  def test_execute_not_last_step( self ):
    self.ujf.step_commons = { 'STEP_NUMBER' : '1' }
    self.ujf.workflow_commons = { 'TotalSteps' : '10000' }
    result = self.ujf.execute()
    assertDiracSucceeds( result, self )

  def test_execute_resolveIV_fails( self ):
    with patch('%s.UserJobFinalization.applicationSpecificInputs' % MODULE_NAME, new=Mock(return_value=S_ERROR('ASI_err_my_test'))):
      result = self.ujf.execute()
      assertDiracFailsWith( result, 'asi_err_my_test', self )

  def test_execute_workflow_status_bad( self ):
    self.ujf.stepStatus = S_ERROR('asdjufe')
    result = self.ujf.execute()
    assertDiracSucceedsWith_equals( result, 'No output data upload attempted', self)

  def test_execute_no_data_to_upload( self ):
    self.ujf.userOutputData = None
    result = self.ujf.execute()
    assertDiracSucceedsWith_equals( result, 'No output data to upload', self)

  def test_execute_getcandidatefiles_fails( self ):
    self.ujf.workflow_commons = { 'TotalSteps' : '42', 'Owner' : 'something',
                                  'VO' : 'something_else' }
    self.ujf.userOutputData = [ 'mylist' ]
    with patch('%s.UserJobFinalization.getCandidateFiles' % MODULE_NAME, new=Mock(return_value=S_ERROR('get_candidate_files_test_err'))), \
         patch('%s.UserJobFinalization.setApplicationStatus' % MODULE_NAME) as set_app_status_mock:
      result = self.ujf.execute()
      assertDiracSucceeds( result, self )
      set_app_status_mock.assert_called_once_with( 'get_candidate_files_test_err' )

  def test_execute_getdestinationlist_fails( self ):
    self.ujf.workflow_commons = { 'TotalSteps' : '42', 'Owner' : 'something',
                                  'VO' : 'something_else' }
    self.ujf.userOutputData = [ 'mylist' ]
    with patch('%s.UserJobFinalization.setApplicationStatus' % MODULE_NAME) as set_app_status_mock, \
         patch('%s.getDestinationSEList' % MODULE_NAME, new=Mock(return_value=S_ERROR('my_destinationselist_test_err'))), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.os.path.getsize' % MODULE_NAME, new=Mock(return_value=13283)):
      result = self.ujf.execute()
      assertDiracFailsWith( result, 'my_destinationselist_test_err', self )
      set_app_status_mock.assert_called_once_with( 'Failed To Resolve OutputSE' )

  def test_execute_constructlfn_fails( self ):
    self.ujf.workflow_commons = { 'TotalSteps' : '42', 'JOB_ID' : '512',
                                  'IS_PROD' : True, 'PRODUCTION_ID' : '98245',
                                  'Platform' : 'myTestPlatform',
                                  'Owner' : 'myTestOwner123RichGuy',
                                  'VO' : 'myTestVirtualOrga' }
    self.ujf.userOutputData = [ 'list_of.txt', 'filenames.jar' ]
    with patch('%s.constructUserLFNs' % MODULE_NAME, new=Mock(return_value=S_OK(['/myTestVirtualOrga/testpre/m/myTestOwner123RichGuy/my/User/OPPath/list_of.txt', '/myTestVirtualOrga/testpre/m/myTestOwner123RichGuy/my/User/OPPath/filenames.jar']))), \
         patch('%s.UserJobFinalization.constructOutputLFNs' % MODULE_NAME, new=Mock(return_value=S_ERROR('my_construct_lfn_err_test'))):
      result = self.ujf.execute()
      assertDiracFailsWith( result, 'my_construct_lfn_err_test', self )

  def test_execute_getfilemetadata_fails( self ):
    exists_dict = { 'list_of.txt' : True, 'something' : True }
    self.ujf.jobID = 512
    request_mock = Mock()
    request_mock.RequestName = 'job_512_request.xml'
    request_mock.JobID = 512
    request_mock.SourceComponent = "Job_512"
    report_mock = Mock()
    self.ujf.jobReport = report_mock
    self.ujf.workflow_commons = { 'TotalSteps' : '42', 'Request' : request_mock,
                                  'JOB_ID' : '512', 'IS_PROD' : True,
                                  'PRODUCTION_ID' : '98245', 'Platform' : 'myTestPlatform',
                                  'Owner' : 'myTestOwner123RichGuy', 'VO' : 'myTestVirtualOrga',
                                  'JobReport' : report_mock }
    self.ujf.workflowStatus = S_OK()
    self.ujf.stepStatus = S_OK()
    self.ujf.userOutputData = [ 'something' ]
    self.ujf.userOutputSE = 'myTestReceivingSE'
    self.ujf.userOutputPath = 'my/User/OPPath'
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=lambda path: exists_dict[path])), \
         patch('%s.open' % MODULE_NAME, mock_open(read_data='myfilecontent,makeniceadlerchecksum')), \
         patch('%s.os.path.getsize' % MODULE_NAME, new=Mock(return_value=3048)), \
         patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/mycurdirTestMe')), \
         patch('%s.getDestinationSEList' % MODULE_NAME, new=Mock(return_value=S_OK(['CERN-DIP-4']))), \
         patch('%s.UserJobFinalization.getFileMetadata' % MODULE_NAME, new=Mock(return_value=S_ERROR('my_getfilemeta_test_Err'))):
      result = self.ujf.execute()
      assertDiracSucceedsWith_equals( result, None, self )
      report_mock.setApplicationStatus.assert_called_once_with(
        'my_getfilemeta_test_Err', True )

  def test_execute_getfilemetadata_empty( self ):
    exists_dict = { 'list_of.txt' : True, 'something' : True }
    self.ujf.jobID = 512
    request_mock = Mock()
    request_mock.RequestName = 'job_512_request.xml'
    request_mock.JobID = 512
    request_mock.SourceComponent = "Job_512"
    report_mock = Mock()
    self.ujf.jobReport = report_mock
    self.ujf.workflow_commons = { 'TotalSteps' : '42', 'Request' : request_mock,
                                  'JOB_ID' : '512', 'IS_PROD' : True,
                                  'PRODUCTION_ID' : '98245', 'Platform' : 'myTestPlatform',
                                  'Owner' : 'myTestOwner123RichGuy', 'VO' : 'myTestVirtualOrga',
                                  'JobReport' : report_mock }
    self.ujf.workflowStatus = S_OK()
    self.ujf.stepStatus = S_OK()
    self.ujf.userOutputData = [ 'something' ]
    self.ujf.userOutputSE = 'myTestReceivingSE'
    self.ujf.userOutputPath = 'my/User/OPPath'
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=lambda path: exists_dict[path])), \
         patch('%s.open' % MODULE_NAME, mock_open(read_data='myfilecontent,makeniceadlerchecksum')), \
         patch('%s.os.path.getsize' % MODULE_NAME, new=Mock(return_value=3048)), \
         patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/mycurdirTestMe')), \
         patch('%s.getDestinationSEList' % MODULE_NAME, new=Mock(return_value=S_OK(['CERN-DIP-4']))), \
         patch('%s.UserJobFinalization.getFileMetadata' % MODULE_NAME, new=Mock(return_value=S_OK())):
      result = self.ujf.execute()
      assertDiracSucceedsWith_equals( result, None, self )
      report_mock.setApplicationStatus.assert_called_once_with(
        'No Output Data Files To Upload', True )

  def test_execute_ignoreapperrors( self ):
    exists_dict = { 'list_of.txt' : True, 'something' : True }
    self.ujf.jobID = 512
    self.ujf.ignoreapperrors = True
    request_mock = Mock()
    request_mock.RequestName = 'job_512_request.xml'
    request_mock.JobID = 512
    request_mock.SourceComponent = "Job_512"
    self.ujf.workflow_commons = { 'TotalSteps' : '42', 'Request' : request_mock,
                                  'JOB_ID' : '512', 'IS_PROD' : True,
                                  'PRODUCTION_ID' : '98245', 'Platform' : 'myTestPlatform',
                                  'Owner' : 'myTestOwner123RichGuy', 'VO' : 'myTestVirtualOrga',
                                  'UserOutputSE' : 'myTestReceivingSE' }
    transfer_mock = Mock()
    transfer_mock.transferAndRegisterFile.return_value = S_OK( {} )
    transfer_mock.transferAndRegisterFileFailover.return_value = S_OK()
    dataman_mock = Mock()
    dataman_mock.replicateAndRegister.return_value = S_OK()
    self.ujf.workflowStatus = S_OK()
    self.ujf.stepStatus = S_OK()
    self.ujf.userOutputData = [ 'list_of.txt', 'filenames.jar' ]
    self.ujf.userOutputSE = 'thisValueIsntUsed'
    self.ujf.userOutputPath = 'my/User/OPPath'
    with patch('%s.constructUserLFNs' % MODULE_NAME, new=Mock(return_value=S_OK(['/myTestVirtualOrga/testpre/m/myTestOwner123RichGuy/my/User/OPPath/list_of.txt', '/myTestVirtualOrga/testpre/m/myTestOwner123RichGuy/my/User/OPPath/filenames.jar']))) as constructlfn_mock, \
         patch('%s.UserJobFinalization.getCandidateFiles' % MODULE_NAME, new=Mock(return_value={ 'OK' : False, 'Value' : {} })) as getcf_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=lambda path: exists_dict[path])), \
         patch('%s.open' % MODULE_NAME, mock_open(read_data='myfilecontent,makeniceadlerchecksum')), \
         patch('%s.os.path.getsize' % MODULE_NAME, new=Mock(return_value=3048)), \
         patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/mycurdirTestMe')), \
         patch('%s.getDestinationSEList' % MODULE_NAME, new=Mock(return_value=S_OK(['CERN-DIP-4']))), \
         patch('%s.FailoverTransfer' % MODULE_NAME, new=transfer_mock), \
         patch('%s.DataManager' % MODULE_NAME, new=dataman_mock), \
         patch('%s.time.sleep' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.UserJobFinalization.getFileMetadata' % MODULE_NAME, new=Mock(return_value={ 'OK' : False, 'Value' : { 'workingFile1' : { 'resolvedSE' : ( 'someOtherSite', ), 'localpath' : '/my/local/first/path', 'lfn' : 'LFN:/ilc/some/dir/file1.txt', 'filedict' : 8520 }, 'thisFileWorks.too' : { 'resolvedSE' : ( 'someOtherOtherSite', ), 'localpath' : '/dir/current/local.lfn', 'lfn' : 'LFN:/ilc/mydir/file2.ppt', 'filedict' : 98453 } } })) as getfmd_mock:
      #TODO: does this (getcandidatefiles mock, getfilemetadata mock) make sense? S_ERROR usually will not have a value, so this would actually throw an error in real environment
      result = self.ujf.execute()
    assertDiracSucceeds( result, self )
    getcf_mock.assert_called_once_with( [
      { 'outputPath': 'TXT', 'outputFile': 'list_of.txt', 'outputDataSE': ['myTestReceivingSE'] },
      {'outputPath': 'JAR', 'outputFile': 'filenames.jar', 'outputDataSE': ['myTestReceivingSE'] }
    ], [ '/myTestVirtualOrga/testpre/m/myTestOwner123RichGuy/my/User/OPPath/list_of.txt',
         '/myTestVirtualOrga/testpre/m/myTestOwner123RichGuy/my/User/OPPath/filenames.jar' ], '' )
    constructlfn_mock.assert_called_once_with(
      512, 'myTestVirtualOrga', 'myTestOwner123RichGuy',
      [ 'list_of.txt', 'filenames.jar' ], 'my/User/OPPath' )
    getfmd_mock.assert_called_once_with( {} )
    transfer_mock = transfer_mock() # Necessary for the assumptions
    transfer_mock.transferAndRegisterFile.assert_any_call(
      'workingFile1', '/my/local/first/path', 'LFN:/ilc/some/dir/file1.txt',
      ['myTestReceivingSE', 'CERN-DIP-4'], fileCatalog=['FileCatalog'],
      fileMetaDict=8520 )
    transfer_mock.transferAndRegisterFile.assert_called_with(
      'thisFileWorks.too', '/dir/current/local.lfn', 'LFN:/ilc/mydir/file2.ppt',
      ['myTestReceivingSE', 'CERN-DIP-4'], fileCatalog=['FileCatalog'],
      fileMetaDict=98453 )
    assertEqualsImproved( len(transfer_mock.transferAndRegisterFile.mock_calls),
                          14, self )
    self.assertFalse( transfer_mock.transferAndRegisterFileFailover.called )
    dataman_mock = dataman_mock()
    assertMockCalls( dataman_mock.replicateAndRegister, [ ( 'LFN:/ilc/mydir/file2.ppt', 'myTestReceivingSE' ),
                                                          ( 'LFN:/ilc/some/dir/file1.txt', 'myTestReceivingSE' ) ],
                     self, only_these_calls = False )

TRANSFER_AND_REGISTER_DICT = { 'fail_transfer' : S_ERROR( 'myrandomerror' ),
                               'workingFile1' : S_OK(),
                               'thisFileWorks.too' : S_OK() }

def main():
  """ Executes all tests from this file, useful for profiling, etc """
  app_spec_suite = unittest.TestLoader().loadTestsFromTestCase( TestUserJobFinalization )
  execute_suite = unittest.TestLoader().loadTestsFromTestCase( TestExecute )
  userjobsuite = unittest.TestSuite( [ app_spec_suite, execute_suite ] )
  print(unittest.TextTestRunner(verbosity=2).run(userjobsuite))

if __name__ == '__main__':
  main()
