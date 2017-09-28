#!/usr/local/env python
"""
Test UserJob module

"""

import unittest
from mock import patch, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob

from ILCDIRAC.Interfaces.API.NewInterface.Applications import Fcc

from ILCDIRAC.Tests.Utilities.GeneralUtils import assertDiracFailsWith, \
  assertDiracSucceeds, assertDiracSucceedsWith_equals, assertMockCalls, \
  assertEqualsImproved, assertDiracSucceedsWith

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.UserJob'

#pylint: disable=protected-access

class UserJobTestCase( unittest.TestCase ):
  """ Base class for the UserJob test cases
  """
  def setUp( self ):
    """set up the objects"""
    with patch('%s.getProxyInfo' % MODULE_NAME, new=Mock(return_value=None)):
      self.ujo = UserJob()
      self.log_mock = Mock()
      self.ujo.log = self.log_mock


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

  def test_setinputdata_failed( self ):
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


  ##############################  SPLITTING STUFF : TESTS  ##############################
  @patch("%s.UserJob._toInt" % MODULE_NAME, new=Mock(return_value=1))
  @patch("%s.UserJob._checkJobConsistency" % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.UserJob._splitByData" % MODULE_NAME, new=Mock(return_value=["InputData", ["/ilc/user/u/username/data1"], True]))
  def test_split_bydata( self ):
    self.ujo.splittingOption = "byData"

    with patch("%s.UserJob.setParameterSequence" % MODULE_NAME) as mock_parametric:
  
      info_message = "Job splitting successful"
      assertDiracSucceeds( self.ujo._split(), self )
      self.log_mock.info.assert_called_with( info_message )
      mock_parametric.assert_any_call( "InputData", ["/ilc/user/u/username/data1"], True )

  @patch("%s.UserJob._toInt" % MODULE_NAME, new=Mock(return_value=1))
  @patch("%s.UserJob._checkJobConsistency" % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.UserJob._splitByEvents" % MODULE_NAME, new=Mock(return_value=['NumberOfEvents', [1, 2], 'NbOfEvts']))
  def test_split_byevents( self ):
    self.ujo.splittingOption = "byEvents"
 
    with patch("%s.UserJob.setParameterSequence" % MODULE_NAME) as mock_parametric:
  
      info_message = "Job splitting successful"
      assertDiracSucceeds( self.ujo._split(), self )
      self.log_mock.info.assert_called_with( info_message )
      mock_parametric.assert_any_call( 'NumberOfEvents', [1, 2], 'NbOfEvts' )

  @patch("%s.UserJob._toInt" % MODULE_NAME, new=Mock(return_value=1))
  @patch("%s.UserJob._checkJobConsistency" % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.UserJob._atomicSubmission" % MODULE_NAME, new=Mock(return_value=("Atomic", [], False)))
  def test_split_atomicsubmission( self ):
    self.ujo.splittingOption = None
    info_message = "Job splitting successful"
    assertDiracSucceeds( self.ujo._split(), self )
    self.log_mock.info.assert_called_with( info_message )

  @patch("%s.UserJob._toInt" % MODULE_NAME, new=Mock(return_value=False))
  def test_split_inputparameters_failed( self ):
    assertDiracFailsWith( self.ujo._split(), "Splitting: Invalid values for splitting", self )

  @patch("%s.UserJob._toInt" % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.UserJob._checkJobConsistency" % MODULE_NAME, new=Mock(return_value=False))
  def test_split_checkjobconsistency_failed( self ):
    assertDiracFailsWith( self.ujo._split(), "failed", self )
    self.log_mock.error.assert_called_once()

  @patch("%s.UserJob._toInt" % MODULE_NAME, new=Mock(return_value=1))
  @patch("%s.UserJob._checkJobConsistency" % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.UserJob._splitByData" % MODULE_NAME, new=Mock(return_value=False))
  def test_split_bydata_failed( self ):
    self.ujo.splittingOption = "byData"
    assertDiracFailsWith( self.ujo._split(), "_splitBySomething() failed", self )
    self.log_mock.error.assert_called_once()

  @patch("%s.UserJob._toInt" % MODULE_NAME, new=Mock(return_value=1))
  @patch("%s.UserJob._checkJobConsistency" % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.UserJob._splitByEvents" % MODULE_NAME, new=Mock(return_value=False))
  def test_split_byevents_failed( self ):
    self.ujo.splittingOption = "byEvents"
    assertDiracFailsWith( self.ujo._split(), "_splitBySomething() failed", self )
    self.log_mock.error.assert_called_once()
  
  @patch("%s.UserJob._toInt" % MODULE_NAME, new=Mock(return_value=1))
  @patch("%s.UserJob._checkJobConsistency" % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.UserJob._atomicSubmission" % MODULE_NAME, new=Mock(return_value=False))
  def test_split_atomicsubmission_failed( self ):
    self.ujo.splittingOption = None
    assertDiracFailsWith( self.ujo._split(), "_splitBySomething() failed", self )
    self.log_mock.error.assert_called_once()

  def test_atomicsubmission( self ):
    app1 = Fcc()
    app2 = Fcc()
    self.ujo.applicationlist = [app1, app2]
    assertEqualsImproved( self.ujo._atomicSubmission(), ("Atomic", [], False), self )

  def test_checkjobconsistency( self ):
    app1 = Fcc()
    app2 = Fcc()
    self.ujo.applicationlist = [app1, app2]
    self.ujo._switch = {"byEvents": lambda x: x }
    self.ujo.splittingOption = "byEvents"
    self.assertTrue( self.ujo._checkJobConsistency() )

  def test_checkjobconsistency_bad_split_parameter( self ):
    app1 = Fcc()
    app2 = Fcc()
    self.ujo.applicationlist = [app1, app2]
    self.ujo._switch = {"byEvents": lambda x: x }
    self.ujo.splittingOption = "byHand"
    self.assertFalse( self.ujo._checkJobConsistency() )
    self.log_mock.error.assert_called_once()

  def test_checkjobconsistency_no_same_events( self ):
    app1 = Fcc()
    app2 = Fcc()
    app1.numberOfEvents = 1
    app2.numberOfEvents = 2   
    self.ujo.applicationlist = [app1, app2]
    self.ujo._switch = {"byEvents": lambda x: x }
    self.ujo.splittingOption = "byEvents"
    self.assertTrue( self.ujo._checkJobConsistency())
    self.log_mock.warn.assert_called_once_with( "Job: Applications should all have the same number of events" )

  def test_checkjobconsistency_negative_events( self ):
    app1 = Fcc()
    app2 = Fcc()
    app1.numberOfEvents = app2.numberOfEvents = -1
    self.ujo.applicationlist = [app1, app2]
    self.ujo._switch = ["byEvents"]
    self.ujo.splittingOption = "byEvents"
    self.assertTrue( self.ujo._checkJobConsistency() )

  def test_splitbydata( self ):
    self.ujo._data = ['data1', 'data2']
    app1 = Fcc()
    app2 = Fcc()
    self.ujo.applicationlist = [app1, app2]
    assertEqualsImproved( self.ujo._splitByData(), ["InputData", [['data1'],['data2']], 'ParametricInputData'], self )

  def test_splitbydata_no_data( self ):
    self.ujo._data = None
    self.assertFalse( self.ujo._splitByData() )
    self.log_mock.error.assert_called_once()

  def test_splitbydata_incorrectparameter( self ):
    self.ujo._data = ["/path/to/data1","/path/to/data2"]
    self.ujo.numberOfFilesPerJob = 3
    self.assertFalse( self.ujo._splitByData() )
    self.log_mock.error.assert_called_once()

  def test_splitbyevents_1st_case( self ):
    app1 = Fcc()
    app2 = Fcc()
    self.ujo.applicationlist = [app1, app2]

    self.ujo.eventsPerJob = 2
    self.ujo.numberOfJobs = 3

    map_event_job = [2, 2, 2]

    assertEqualsImproved( self.ujo._splitByEvents(), ['NumberOfEvents', map_event_job, 'NbOfEvts'], self )

  def test_splitbyevents_2nd_case( self ):
    app1 = Fcc()
    app2 = Fcc()
    self.ujo.applicationlist = [app1, app2]

    self.ujo.eventsPerJob = 3
    self.ujo.totalNumberOfEvents = 5

    map_event_job = [3, 2]

    assertEqualsImproved( self.ujo._splitByEvents(), ['NumberOfEvents', map_event_job, 'NbOfEvts'], self )

  def test_splitbyevents_2nd_case_failed( self ):
    app1 = Fcc()
    app2 = Fcc()
    self.ujo.applicationlist = [app1, app2]

    self.ujo.eventsPerJob = 3
    self.ujo.totalNumberOfEvents = 2
    self.assertFalse( self.ujo._splitByEvents() )

  def test_splitbyevents_3rd_case( self ):
    app1 = Fcc()
    app2 = Fcc()
    self.ujo.applicationlist = [app1, app2]
    self.ujo.numberOfJobs = 2
    self.ujo.totalNumberOfEvents = 2
    map_event_job = [1, 1]
    assertEqualsImproved( self.ujo._splitByEvents(), ['NumberOfEvents', map_event_job, 'NbOfEvts'], self )
    
  def test_splitbyevents_3rd_case_failed( self ):
    app1 = Fcc()
    app2 = Fcc()
    self.ujo.applicationlist = [app1, app2]
    self.ujo.numberOfJobs = 2
    self.ujo.totalNumberOfEvents = None
    self.assertFalse( self.ujo._splitByEvents() )

  def test_toint( self ):
    assertEqualsImproved( self.ujo._toInt("2"), 2, self )

  def test_toint_negative( self ):
    self.assertFalse( self.ujo._toInt("-2") )
    self.log_mock.error.assert_called_once()

  def test_setsplitevents( self ):
    self.ujo.setSplitEvents( 42, 42, 126 )
    assertEqualsImproved( self.ujo.totalNumberOfEvents, 126, self )
    assertEqualsImproved( self.ujo.eventsPerJob, 42, self )
    assertEqualsImproved( self.ujo.numberOfJobs, 42, self )
    assertEqualsImproved( self.ujo.splittingOption, "byEvents", self )

  def test_setsplitInputdata( self ):
    input_data = ["/path/to/data1","/path/to/data2"]
    self.ujo.setSplitInputData( input_data )
    for data in input_data:
      self.assertIn( data, self.ujo._data )

    assertEqualsImproved( self.ujo.splittingOption, "byData", self )
    