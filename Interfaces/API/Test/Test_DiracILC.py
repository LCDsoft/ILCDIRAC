#!/usr/local/env python
"""
Partial tests for the DiracILC module

"""

import sys
import unittest
from mock import patch, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertDiracSucceeds, assertDiracSucceedsWith, assertDiracFailsWith_equals, assertMockCalls

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Interfaces.API.DiracILC'

#pylint: disable=protected-access,too-many-public-methods
class DiracILCTestCase( unittest.TestCase ):
  """ Base class for the DiracILC test cases
  """
  def setUp( self ):
    """set up the objects"""
    ops_mock = Mock()
    mocked_modules = { 'DIRAC.ConfigurationSystem.Client.Helpers.Operations' : ops_mock }
    self.module_patcher = patch.dict( sys.modules, mocked_modules )
    self.module_patcher.start()
    from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
    self.dilc = DiracILC()

    def setOptions(*args):
      if 'SingleReplicaSEs' in args[0]:
        return ['SE']
      if 'Minimum' in args[0]:
        return 1
      if args[0].endswith('PreferredSEs'):
        return ['Awesome-Tape-SE']

    ops_mock = Mock()
    ops_mock.getValue = Mock()
    ops_mock.getValue.side_effect = setOptions
    self.dilc.ops = ops_mock

  def tearDown( self ):
    self.module_patcher.stop()

  def test_getprocesslist( self ):
    with patch('%s.gConfig.getValue' % MODULE_NAME, new=Mock(return_value='some_gconf_testval')) as conf_mock, \
         patch('%s.ProcessList' % MODULE_NAME, new=Mock()) as pl_mock:
      res = self.dilc.getProcessList()
      pl_mock.assert_called_once_with( 'some_gconf_testval' )
      assertEqualsImproved( res, pl_mock(), self )
      conf_mock.assert_called_once_with( '/LocalSite/ProcessListPath', '' )

  def test_getprocesslist_nopath( self ):
    ops_mock = Mock()
    ops_mock.getValue.return_value = ''
    self.dilc.ops = ops_mock
    with patch('%s.gConfig.getValue' % MODULE_NAME, new=Mock(return_value='')) as conf_mock, \
         patch('%s.ProcessList' % MODULE_NAME, new=Mock()) as pl_mock:
      res = self.dilc.getProcessList()
      pl_mock.assert_called_once_with( '' )
      assertEqualsImproved( res, pl_mock(), self )
      conf_mock.assert_called_once_with( '/LocalSite/ProcessListPath', '' )
      ops_mock.getValue.assert_called_once_with( '/ProcessList/Location', '' )

  def test_presubmissionchecks_notoktosubmit( self ):
    job_mock = Mock()
    job_mock.oktosubmit = False
    assertDiracFailsWith( self.dilc.preSubmissionChecks( job_mock, None ),
                          'you should use job.submit(dirac)', self )

  def test_presubmissionchecks_checkfails( self ):
    job_mock = Mock()
    job_mock.oktosubmit = True
    with patch('%s.DiracILC._do_check' % MODULE_NAME, new=Mock(return_value=S_ERROR('mytest_check_failed'))):
      assertDiracFailsWith( self.dilc.preSubmissionChecks( job_mock, None ),
                            'mytest_check_failed', self )

  def test_presubmissionchecks_askuser_fails( self ):
    job_mock = Mock()
    job_mock.oktosubmit = True
    job_mock._askUser.return_value = S_ERROR( 'user says no' )
    self.dilc.checked = False
    with patch('%s.DiracILC._do_check' % MODULE_NAME, new=Mock(return_value=S_OK())):
      assertDiracFailsWith( self.dilc.preSubmissionChecks( job_mock, None ),
                            'user says no', self )

  def test_checkparams( self ):
    job_mock = Mock()
    job_mock.errorDict = {}
    with patch('%s.DiracILC.preSubmissionChecks' % MODULE_NAME, new=Mock(return_value=S_OK('mytest'))) as check_mock:
      assertDiracSucceedsWith( self.dilc.checkparams( job_mock ), 'mytest', self )
      check_mock.assert_called_once_with( job_mock, mode = '' )

  def test_checkparams_fails( self ):
    job_mock = Mock()
    job_mock.errorDict = { 'myerror1' : [ 'Terrible failure' ], 'last_error' : [ 'True' ] }
    assertDiracFailsWith_equals( self.dilc.checkparams( job_mock ),
                                 { 'myerror1' : [ 'Terrible failure' ], 'last_error' : [ 'True' ] }, self )

  def test_giveprocesslist( self ):
    self.dilc.processList = '13985u185r9135r'
    assertEqualsImproved( self.dilc.giveProcessList(), '13985u185r9135r', self )

  def test_giveprocesslist_empty( self ):
    self.dilc.processList = ''
    assertEqualsImproved( self.dilc.giveProcessList(), '', self )

  def test_giveprocesslist_false( self ):
    self.dilc.processList = False
    assertEqualsImproved( self.dilc.giveProcessList(), False, self )

  def test_giveprocesslist_none( self ):
    self.dilc.processList = None
    assertEqualsImproved( self.dilc.giveProcessList(), None, self )

  def test_retrievelfns_norepo( self ):
    self.dilc.jobRepo = None
    assertDiracSucceeds( self.dilc.retrieveRepositoryOutputDataLFNs(), self )

  def test_retrievelfns( self ):
    repo_mock = Mock()
    ret_dict = { '1' : { 'State' : 'Done', 'UserOutputData' : '1389' }, '2' : {},
                 '3' : { 'State' : 'secret_teststate' }, '4' : { 'State' : 'invalid_state' },
                 '5' : { 'State' : 'Done', 'UserOutputData' : 0 }, '6' : { 'ignore_me' : True },
                 '7' : { 'State' : 'secret_teststate', 'UserOutputData' : 0 },
                 '148' : { 'State' : 'Done', 1 : False, True : 941, 'values_' : 'keys' } }
    repo_mock.readRepository.return_value = S_OK( ret_dict )
    self.dilc.jobRepo = repo_mock
    with patch('%s.DiracILC.parameters' % MODULE_NAME, new=Mock(side_effect=[S_OK({'UploadedOutputData':'/my/test/lfn1'}),S_ERROR(),S_OK({}),S_OK({'some_entries':'some_values',1:True,'UploadedOutputData':'/more_lfns/append/testlfn.log'})])) as param_mock:
      assertEqualsImproved( self.dilc.retrieveRepositoryOutputDataLFNs( [ 'Done', 'secret_teststate' ] ),
                            [ '/my/test/lfn1', '/more_lfns/append/testlfn.log' ], self )
      assertMockCalls( param_mock, [ 3, 5, 7, 148 ], self )

  def test_docheck_checksandbox_fails( self ):
    job_mock = Mock()
    job_mock.inputsandbox = [ 'mysandbox', 'other_value' ]
    with patch('%s.DiracILC.checkInputSandboxLFNs' % MODULE_NAME, new=Mock(return_value=S_ERROR('test_err_sandbox'))) as check_mock:
      assertDiracFailsWith( self.dilc._do_check( job_mock ), 'test_err_sandbox', self )
      check_mock.assert_called_once_with( job_mock )

  def test_docheck_too_many_lists( self ):
    job_mock = Mock()
    job_mock.inputsandbox = [ 'mysandbox', [ 'im_a_sandbox_file.stdhep', [ 'evil_list', 'deletethis'] ] ]
    assertDiracFailsWith( self.dilc._do_check( job_mock ), 'too many lists of lists in the input sandbox', self )

  def test_docheck_checkapps_fails( self ):
    platform_mock = Mock()
    platform_mock.getValue.return_value = 'pf14081'
    apps_mock = Mock()
    apps_mock.getValue.return_value = 'Myapp1v.9.2.1;other_Appv.91.3;more_Dependencies.1;LasT_APP.0'
    param_dict = { 'Platform' : platform_mock, 'SoftwarePackages' : apps_mock }
    job_mock = Mock()
    wf_mock = Mock()
    wf_mock.findParameter.side_effect = lambda param_name : param_dict[ param_name ]
    job_mock.inputsandbox = [ 'mysandbox', 'other_value', [ 'sandbox_file1.txt', 'sandbox_file2.log',
                                                            'last.file' ] ]
    job_mock._resolveInputSandbox.return_value = [ 'resolved_file.1.txt', 'other_resolved_file.txt' ]
    job_mock.workflow = wf_mock
    with patch('%s.DiracILC.checkInputSandboxLFNs' % MODULE_NAME, new=Mock(return_value=S_OK())) as checksb_mock, \
         patch('%s.DiracILC._checkapp' % MODULE_NAME, new=Mock(side_effect=[S_OK()] * 3 + [S_ERROR('checkapp_failed_testme')])) as checkapp_mock:
      assertDiracFailsWith( self.dilc._do_check( job_mock ), 'checkapp_failed_testme', self )
      checksb_mock.assert_called_once_with( job_mock )
      job_mock._resolveInputSandbox.assert_called_once_with( [ 'mysandbox', 'other_value',
                                                               'sandbox_file1.txt', 'sandbox_file2.log',
                                                               'last.file' ] )
      job_mock._addParameter.assert_called_once_with(
        wf_mock, 'InputSandbox', 'JDL', 'resolved_file.1.txt;other_resolved_file.txt', 'Input sandbox file list' )
      assertMockCalls( checkapp_mock, [ ( 'pf14081', 'myapp1v', '9.2.1' ),
                                        ( 'pf14081', 'other_appv', '91.3' ),
                                        ( 'pf14081', 'more_dependencies', '1' ),
                                        ( 'pf14081', 'last_app', '0' ) ], self )

  def test_docheck_checkoutputpath_fails( self ):
    platform_mock = Mock()
    platform_mock.getValue.return_value = 'pf14081'
    apps_mock = Mock()
    apps_mock.getValue.return_value = 'Myapp1v.9.2.1;other_Appv.91.3;more_Dependencies.1;LasT_APP.0'
    path_mock = Mock()
    path_mock.getValue.return_value = 'path1948512895'
    param_dict = { 'Platform' : platform_mock, 'SoftwarePackages' : apps_mock, 'UserOutputPath' : path_mock }
    job_mock = Mock()
    wf_mock = Mock()
    wf_mock.findParameter.side_effect = lambda param_name : param_dict[ param_name ]
    job_mock.inputsandbox = [ 'mysandbox', 'other_value', [ 'sandbox_file1.txt', 'sandbox_file2.log',
                                                            'last.file' ] ]
    job_mock._resolveInputSandbox.return_value = [ 'resolved_file.1.txt', 'other_resolved_file.txt' ]
    job_mock.workflow = wf_mock
    with patch('%s.DiracILC.checkInputSandboxLFNs' % MODULE_NAME, new=Mock(return_value=S_OK())) as checksb_mock, \
         patch('%s.DiracILC._checkapp' % MODULE_NAME, new=Mock(return_value=S_OK())) as checkapp_mock, \
         patch('%s.DiracILC._checkoutputpath' % MODULE_NAME, new=Mock(return_value=S_ERROR('outputpath_check_testerr'))) as checkpath_mock:
      assertDiracFailsWith( self.dilc._do_check( job_mock ), 'outputpath_check_testerr', self )
      checksb_mock.assert_called_once_with( job_mock )
      job_mock._resolveInputSandbox.assert_called_once_with( [ 'mysandbox', 'other_value',
                                                               'sandbox_file1.txt', 'sandbox_file2.log',
                                                               'last.file' ] )
      job_mock._addParameter.assert_called_once_with(
        wf_mock, 'InputSandbox', 'JDL', 'resolved_file.1.txt;other_resolved_file.txt', 'Input sandbox file list' )
      assertMockCalls( checkapp_mock, [ ( 'pf14081', 'myapp1v', '9.2.1' ),
                                        ( 'pf14081', 'other_appv', '91.3' ),
                                        ( 'pf14081', 'more_dependencies', '1' ),
                                        ( 'pf14081', 'last_app', '0' ) ], self )
      checkpath_mock.assert_called_once_with( 'path1948512895' )

  def test_docheck_checkconsistency_fails( self ):
    platform_mock = Mock()
    platform_mock.getValue.return_value = 'pf14081'
    apps_mock = Mock()
    apps_mock.getValue.return_value = 'Myapp1v.9.2.1;other_Appv.91.3;more_Dependencies.1;LasT_APP.0'
    path_mock = Mock()
    path_mock.getValue.return_value = 'path1948512895'
    data_mock = Mock()
    data_mock.getValue.return_value = 'data1389518'
    param_dict = { 'Platform' : platform_mock, 'SoftwarePackages' : apps_mock,
                   'UserOutputPath' : path_mock, 'UserOutputData' : data_mock }
    job_mock = Mock()
    job_mock.addToOutputSandbox = 'job_sandbox13895'
    wf_mock = Mock()
    wf_mock.findParameter.side_effect = lambda param_name : param_dict[ param_name ]
    job_mock.inputsandbox = [ 'mysandbox', 'other_value', [ 'sandbox_file1.txt', 'sandbox_file2.log',
                                                            'last.file' ] ]
    job_mock._resolveInputSandbox.return_value = [ 'resolved_file.1.txt', 'other_resolved_file.txt' ]
    job_mock.workflow = wf_mock
    with patch('%s.DiracILC.checkInputSandboxLFNs' % MODULE_NAME, new=Mock(return_value=S_OK())) as checksb_mock, \
         patch('%s.DiracILC._checkapp' % MODULE_NAME, new=Mock(return_value=S_OK())) as checkapp_mock, \
         patch('%s.DiracILC._checkoutputpath' % MODULE_NAME, new=Mock(return_value=S_OK())) as checkpath_mock, \
         patch('%s.DiracILC._checkdataconsistency' % MODULE_NAME, new=Mock(return_value=S_ERROR('consistency_testerr'))) as checkconsistency_mock:
      assertDiracFailsWith( self.dilc._do_check( job_mock ), 'consistency_testerr', self )
      checksb_mock.assert_called_once_with( job_mock )
      job_mock._resolveInputSandbox.assert_called_once_with( [ 'mysandbox', 'other_value',
                                                               'sandbox_file1.txt', 'sandbox_file2.log',
                                                               'last.file' ] )
      job_mock._addParameter.assert_called_once_with(
        wf_mock, 'InputSandbox', 'JDL', 'resolved_file.1.txt;other_resolved_file.txt', 'Input sandbox file list' )
      assertMockCalls( checkapp_mock, [ ( 'pf14081', 'myapp1v', '9.2.1' ),
                                        ( 'pf14081', 'other_appv', '91.3' ),
                                        ( 'pf14081', 'more_dependencies', '1' ),
                                        ( 'pf14081', 'last_app', '0' ) ], self )
      checkpath_mock.assert_called_once_with( 'path1948512895' )
      checkconsistency_mock.assert_called_once_with( 'data1389518', 'job_sandbox13895' )

  def test_checkapp( self ):
    ops_mock = Mock()
    ops_mock.getValue.return_value = ''
    self.dilc.ops = ops_mock
    assertDiracFailsWith( self.dilc._checkapp( 'test_platform_341', 'testapp', 'v13.2' ),
                          'could not find the specified software testapp_v13.2 for test_platform_341, check in CS',
                          self )
    assertMockCalls( ops_mock.getValue, [
      ( '/AvailableTarBalls/test_platform_341/testapp/v13.2/TarBall', '' ),
      ( '/AvailableTarBalls/test_platform_341/testapp/v13.2/CVMFSPath', '' ) ], self )

  def test_checkoutputpath_invalidchar_1( self ):
    assertDiracFailsWith( self.dilc._checkoutputpath( 'http://www.mysitedoesnotexist3h3.abc/some/file.txt' ),
                          'invalid path', self )

  def test_checkoutputpath_invalidchar_2( self ):
    assertDiracFailsWith( self.dilc._checkoutputpath( '/my/dir/./some/file.log' ),
                          'invalid path', self )

  def test_checkoutputpath_invalidchar_3( self ):
    assertDiracFailsWith( self.dilc._checkoutputpath( '/my/dir/../dir2/somefile.txt' ),
                          'invalid path', self )

  def test_checkoutputpath_trailing_slash( self ):
    assertDiracFailsWith( self.dilc._checkoutputpath( '/my/dir/myfile.txt/  ' ),
                          'invalid path', self )

  def test_checkdataconsistency_outputdata_sandbox_equal( self ):
    assertDiracFailsWith( self.dilc._checkdataconsistency( 'same_item;something_else',
                                                           [ 'distinct_item1', 'same_item' ] ),
                          'output data and sandbox should not contain the same thing', self )

  def test_checkdataconsistency_wildcardchar( self ):
    assertDiracFailsWith( self.dilc._checkdataconsistency(
      '/test/dir/file.txt;/mydir/something/*;/file/dir/log.log',
      [ '/input/sandbox.pdf', '/other/sb/file.stdhep' ] ),
                          'wildcard character in outputdata definition', self )

  def test_checkinputsb_getreplicas_notok( self ):
    job_mock = Mock()
    job_mock.workflow.findParameter.return_value.getValue.return_value = '/some/file.txt;/other/some/file.stdhep;LFN:/my/dir/inputsandbox/in1.stdio;lfn:/my/dir/inputsandbox/in2.pdf'
    with patch('%s.DiracILC.getReplicas' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_err'))) as replica_mock:
      assertDiracFailsWith( self.dilc.checkInputSandboxLFNs( job_mock ), 'could not get replicas', self )
      replica_mock.assert_called_once_with( [ '/my/dir/inputsandbox/in1.stdio', '/my/dir/inputsandbox/in2.pdf' ] )

  def test_checkinputsb_getreplicas_fails( self ):
    job_mock = Mock()
    job_mock.workflow.findParameter.return_value.getValue.return_value = '/some/file.txt;/other/some/file.stdhep;LFN:/my/dir/inputsandbox/in1.stdio;lfn:/my/dir/inputsandbox/in2.pdf'
    ret_dict = { 'Failed' : [ '/failed/replica1', '/other/inval/replica' ], 'Successful' : {} }
    with patch('%s.DiracILC.getReplicas' % MODULE_NAME, new=Mock(return_value=S_OK(ret_dict))) as replica_mock:
      assertDiracFailsWith( self.dilc.checkInputSandboxLFNs( job_mock ), 'failed to find replicas', self )
      replica_mock.assert_called_once_with( [ '/my/dir/inputsandbox/in1.stdio', '/my/dir/inputsandbox/in2.pdf' ] )

  def test_checkinputsb( self ):
    job_mock = Mock()
    job_mock.workflow.findParameter.return_value.getValue.return_value = '/some/file.txt;/other/some/file.stdhep;LFN:/my/dir/inputsandbox/in1.stdio;lfn:/my/dir/inputsandbox/in2.pdf'
    ret_dict = {'Failed': [], 'Successful': {'/one/replica': {'SE': 'surl'}}}
    with patch('%s.DiracILC.getReplicas' % MODULE_NAME, new=Mock(return_value=S_OK(ret_dict))) as replica_mock:
      assertDiracSucceeds( self.dilc.checkInputSandboxLFNs( job_mock ), self )
      replica_mock.assert_called_once_with( [ '/my/dir/inputsandbox/in1.stdio', '/my/dir/inputsandbox/in2.pdf' ] )

  def test_checkinputsb_notInputSB(self):
    job_mock = Mock()
    job_mock.workflow.findParameter.return_value = None
    assertDiracSucceeds(self.dilc.checkInputSandboxLFNs(job_mock), self)

  def test_checkinputsb_notInputSB_Value(self):
    job_mock = Mock()
    job_mock.workflow.findParameter.return_value.getValue.return_value = ''
    assertDiracSucceeds(self.dilc.checkInputSandboxLFNs(job_mock), self)

  def test_checkinputsb_noLFNs(self):
    job_mock = Mock()
    job_mock.workflow.findParameter.return_value.getValue.return_value = '/some/file.txt;/other/some/file.stdhep'
    assertDiracSucceeds(self.dilc.checkInputSandboxLFNs(job_mock), self)

  def test_checkinputsb_noRepl(self):
    job_mock = Mock()
    job_mock.workflow.findParameter.return_value.getValue.return_value = 'LFN:/some/file.txt'
    ret_dict = {'Failed': [], 'Successful': {'/some/file.txt': {'Bad-SE': 'surl'}}}

    def setOptions(*args):
      if 'SingleReplicaSEs' in args[0]:
        return ['Awesome-Disk-SE']
      if 'Minimum' in args[0]:
        return 2
      if args[0].endswith('PreferredSEs'):
        return ['Awesome-Tape-SE']

    ops_mock = Mock()
    ops_mock.getValue = setOptions
    self.dilc.ops = ops_mock

    with patch('%s.DiracILC.getReplicas' % MODULE_NAME, new=Mock(return_value=S_OK(ret_dict))) as replica_mock:
      assertDiracFailsWith(self.dilc.checkInputSandboxLFNs(job_mock), 'Not enough replicas', self)
      replica_mock.assert_called_once_with(['/some/file.txt'])

  def test_checkinputsb_goodRepl(self):
    job_mock = Mock()
    job_mock.workflow.findParameter.return_value.getValue.return_value = 'LFN:/some/file.txt'
    ret_dict = {'Failed': [], 'Successful': {'/some/file.txt': {'Awesome-Disk-SE': 'surl'}}}

    def setOptions(*args):
      if 'SingleReplicaSEs' in args[0]:
        return ['Awesome-Disk-SE']
      if 'Minimum' in args[0]:
        return 2
      if args[0].endswith('PreferredSEs'):
        return ['Awesome-Tape-SE']

    ops_mock = Mock()
    ops_mock.getValue = setOptions
    self.dilc.ops = ops_mock

    with patch('%s.DiracILC.getReplicas' % MODULE_NAME, new=Mock(return_value=S_OK(ret_dict))) as replica_mock:
      assertDiracSucceeds(self.dilc.checkInputSandboxLFNs(job_mock), self)
      replica_mock.assert_called_once_with(['/some/file.txt'])
