#!/usr/local/env python
"""
Test UserJob module

"""

import unittest
from mock import patch, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertDiracFailsWith, \
  assertDiracSucceeds, assertDiracSucceedsWith_equals, assertMockCalls

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.UserJob'

class UserJobTestCase( unittest.TestCase ):
  """ Base class for the UserJob test cases
  """
  def setUp( self ):
    """set up the objects"""
    with patch('%s.getProxyInfo' % MODULE_NAME, new=Mock(return_value=None)):
      self.ujo = UserJob()

  def test_submit_noproxy( self ):
    self.ujo.proxyinfo = S_ERROR()
    assertDiracFailsWith( self.ujo.submit(),
                          "Not allowed to submit a job, you need a ['ilc_user', 'calice_user'] proxy", self )

  def test_submit_wrongproxygroup( self ):
    self.ujo.proxyinfo = S_OK( { 'group' : 'my_test_group.notInallowed_list' } )
    assertDiracFailsWith( self.ujo.submit(),
                          "Not allowed to submit job, you need a ['ilc_user', 'calice_user'] proxy", self )

  def test_submit_noproxygroup( self ):
    self.ujo.proxyinfo = S_OK( { 'some_key' : 'Value', True : 1, False : [], 135 : {} } )
    assertDiracFailsWith( self.ujo.submit(), 'Could not determine group, you do not have the right proxy', self )

  def test_submit_addtoworkflow_fails( self ):
    self.ujo.proxyinfo = S_OK( { 'group' : 'ilc_user' } )
    with patch('%s.UserJob._addToWorkflow' % MODULE_NAME, new=Mock(return_value=S_ERROR('workflow_testadd_error'))):
      assertDiracFailsWith( self.ujo.submit(), 'workflow_testadd_error', self )

  def test_submit_addtoworkflow_fails_2( self ):
    self.ujo.proxyinfo = S_OK( { 'group' : 'calice_user' } )
    with patch('%s.UserJob._addToWorkflow' % MODULE_NAME, new=Mock(return_value=S_ERROR('err_workflow_testadd'))):
      assertDiracFailsWith( self.ujo.submit(), 'err_workflow_testadd', self )

  def test_submit_createnew_dirac_instance( self ):
    ilc_mock = Mock()
    ilc_mock().submit.return_value = S_OK( 'test_submission_successful' )
    self.ujo.proxyinfo = S_OK( { 'group' : 'ilc_user' } )
    with patch('%s.UserJob._addToWorkflow' % MODULE_NAME, new=Mock(return_value=S_OK())), \
         patch('%s.DiracILC' % MODULE_NAME, new=ilc_mock):
      assertDiracSucceedsWith_equals( self.ujo.submit(), 'test_submission_successful', self )
      ilc_mock().submit.assert_called_once_with( self.ujo, 'wms' )
      assert self.ujo.oktosubmit

  def test_setinputdata( self ):
    assertDiracFailsWith( self.ujo.setInputData( { '/mylfn1' : True, '/mylfn2' : False } ),
                          'expected lfn string or list of lfns for input data', self )

  def test_inputsandbox( self ):
    self.ujo.inputsandbox = Mock()
    assertDiracSucceeds( self.ujo.setInputSandbox( 'LFN:/ilc/user/u/username/libraries.tar.gz' ), self )
    self.ujo.inputsandbox.extend.assert_called_once_with( [ 'LFN:/ilc/user/u/username/libraries.tar.gz' ] )

  def test_inputsandbox_dictpassed( self ):
    assertDiracFailsWith( self.ujo.setInputSandbox( { '/some/file' : True, '/my/dict' : True } ),
                          'File passed must be either single file or list of files', self )

  def test_setoutputdata_dictpassed( self ):
    assertDiracFailsWith( self.ujo.setOutputData( { '/mydict' : True } ),
                          'Expected file name string or list of file names for output data', self )

  def test_setoutputdata_nolistse( self ):
    wf_mock = Mock()
    self.ujo.workflow = wf_mock
    with patch('%s.UserJob._addParameter' % MODULE_NAME, new=Mock()) as addparam_mock:
      assertDiracFailsWith( self.ujo.setOutputData( [ 'mylfn1', 'other_lfn', 'last___lfn' ],
                                                    OutputSE = { 'mydict' : True } ),
                            'Expected string or list for OutputSE', self )
      addparam_mock.assert_called_once_with( wf_mock, 'UserOutputData', 'JDL',
                                             'mylfn1;other_lfn;last___lfn', 'List of output data files' )

  def test_setoutputdata_outputpath_nostring( self ):
    wf_mock = Mock()
    self.ujo.workflow = wf_mock
    with patch('%s.UserJob._addParameter' % MODULE_NAME, new=Mock()) as addparam_mock:
      assertDiracFailsWith( self.ujo.setOutputData( [ 'mylfn1', 'other_lfn', 'last___lfn' ],
                                                    OutputPath = { 'mydict' : True } ),
                            'Expected string for OutputPath', self )
      addparam_mock.assert_called_once_with( wf_mock, 'UserOutputData', 'JDL',
                                             'mylfn1;other_lfn;last___lfn', 'List of output data files' )

  def test_setoutputdata_invalid_outputpath_1( self ):
    wf_mock = Mock()
    self.ujo.workflow = wf_mock
    with patch('%s.UserJob._addParameter' % MODULE_NAME, new=Mock()) as addparam_mock:
      assertDiracFailsWith( self.ujo.setOutputData( [ 'mylfn1', 'other_lfn', 'last___lfn' ],
                                                    OutputPath = '//ilc/user/somedir/output.xml' ),
                            'Output path contains /ilc/user/ which is not what you want', self )
      addparam_mock.assert_called_once_with( wf_mock, 'UserOutputData', 'JDL',
                                             'mylfn1;other_lfn;last___lfn', 'List of output data files' )

  def test_setoutputdata_invalid_outputpath_2( self ):
    wf_mock = Mock()
    self.ujo.workflow = wf_mock
    with patch('%s.UserJob._addParameter' % MODULE_NAME, new=Mock()) as addparam_mock:
      assertDiracFailsWith( self.ujo.setOutputData( [ 'mylfn1', 'other_lfn', 'last___lfn' ],
                                                    OutputPath = '//some/dir/ilc/user/somedir/output.xml' ),
                            'Output path contains /ilc/user/ which is not what you want', self )
      addparam_mock.assert_called_once_with( wf_mock, 'UserOutputData', 'JDL',
                                             'mylfn1;other_lfn;last___lfn', 'List of output data files' )

  def test_setoutputdata( self ):
    wf_mock = Mock()
    self.ujo.workflow = wf_mock
    with patch('%s.UserJob._addParameter' % MODULE_NAME, new=Mock()) as addparam_mock:
      assertDiracSucceeds( self.ujo.setOutputData( [ 'mylfn1', 'other_lfn', 'last___lfn' ], OutputPath =
                                                   '//some/dir/somedir/output.xml' ), self )
      assertMockCalls( addparam_mock, [
        ( wf_mock, 'UserOutputData', 'JDL', 'mylfn1;other_lfn;last___lfn', 'List of output data files' ),
        ( wf_mock, 'UserOutputPath', 'JDL', 'some/dir/somedir/output.xml', 'User specified Output Path' ) ],
                       self )

  def test_setoutputsandbox( self ):
    wf_mock = Mock()
    self.ujo.workflow = wf_mock
    with patch('%s.UserJob._addParameter' % MODULE_NAME, new=Mock()) as addparam_mock:
      assertDiracSucceeds( self.ujo.setOutputSandbox( '/my/dir/myfile.txt' ), self )
      addparam_mock.assert_called_once_with( wf_mock, 'OutputSandbox', 'JDL',
                                             '/my/dir/myfile.txt', 'Output sandbox file' )

  def test_setoutputsandbox_dictpassed( self ):
    wf_mock = Mock()
    self.ujo.workflow = wf_mock
    with patch('%s.UserJob._addParameter' % MODULE_NAME, new=Mock()) as addparam_mock:
      assertDiracFailsWith( self.ujo.setOutputSandbox( { 'mydict' : True } ),
                            'Expected file string or list of files for output sandbox contents', self )
      self.assertFalse( addparam_mock.called )
