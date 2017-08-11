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
  @patch("%s.UserJob._splitByData" % MODULE_NAME, new=Mock(return_value=["InputData", ["/ilc/user/u/username/data1"]]))
  def test_split_bydata( self ):
    self.ujo.split = "byData"

    with patch("%s.UserJob.setParameterSequence" % MODULE_NAME) as mock_parametric:
  
      info_message = "DIRAC : DIRAC submission ending"
      assertDiracSucceedsWith( self.ujo._split(), info_message, self )
      self.log_mock.info.assert_called_with( info_message )
      mock_parametric.assert_called_once_with( "InputData", ["/ilc/user/u/username/data1"] )

  @patch("%s.UserJob._toInt" % MODULE_NAME, new=Mock(return_value=1))
  @patch("%s.UserJob._checkJobConsistency" % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.UserJob._splitByEvents" % MODULE_NAME, new=Mock(return_value=["NumberOfEvents", [1, 2]]))
  def test_split_byevents( self ):
    self.ujo.split = "byEvents"
 
    with patch("%s.UserJob.setParameterSequence" % MODULE_NAME) as mock_parametric:
  
      info_message = "DIRAC : DIRAC submission ending"
      assertDiracSucceedsWith( self.ujo._split(), info_message, self )
      self.log_mock.info.assert_called_with( info_message )
      mock_parametric.assert_called_once_with( "NumberOfEvents", [1, 2] )

  @patch("%s.UserJob._toInt" % MODULE_NAME, new=Mock(return_value=1))
  @patch("%s.UserJob._checkJobConsistency" % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.UserJob._atomicSubmission" % MODULE_NAME, new=Mock(return_value=("Atomic", [])))
  def test_split_atomicsubmission( self ):
    self.ujo.split = None
    info_message = "DIRAC : DIRAC submission ending"
    assertDiracSucceedsWith( self.ujo._split(), info_message, self )
    self.log_mock.info.assert_called_with( info_message )

  @patch("%s.UserJob._toInt" % MODULE_NAME, new=Mock(return_value=False))
  def test_split_inputparameters_failed( self ):
    assertDiracFailsWith( self.ujo._split(), "Splitting : Invalid values for splitting", self )    

  @patch("%s.UserJob._toInt" % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.UserJob._checkJobConsistency" % MODULE_NAME, new=Mock(return_value=False))
  def test_split_checkjobconsistency_failed( self ):
    error_message = "DIRAC : DIRAC submission failed"
    assertDiracFailsWith( self.ujo._split(), error_message, self )
    self.log_mock.error.assert_called_once_with( error_message )  

  @patch("%s.UserJob._toInt" % MODULE_NAME, new=Mock(return_value=1))
  @patch("%s.UserJob._checkJobConsistency" % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.UserJob._splitByData" % MODULE_NAME, new=Mock(return_value=False))
  def test_split_bydata_failed( self ):
    self.ujo.split = "byData"
    error_message = "DIRAC : DIRAC submission failed"
    assertDiracFailsWith( self.ujo._split(), error_message, self )
    self.log_mock.error.assert_called_once_with( error_message )

  @patch("%s.UserJob._toInt" % MODULE_NAME, new=Mock(return_value=1))
  @patch("%s.UserJob._checkJobConsistency" % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.UserJob._splitByEvents" % MODULE_NAME, new=Mock(return_value=False))
  def test_split_byevents_failed( self ):
    self.ujo.split = "byEvents"
    error_message = "DIRAC : DIRAC submission failed"
    assertDiracFailsWith( self.ujo._split(), error_message, self )
    self.log_mock.error.assert_called_once_with( error_message )
  
  @patch("%s.UserJob._toInt" % MODULE_NAME, new=Mock(return_value=1))
  @patch("%s.UserJob._checkJobConsistency" % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.UserJob._atomicSubmission" % MODULE_NAME, new=Mock(return_value=False))
  def test_split_atomicsubmission_failed( self ):
    self.ujo.split = None
    error_message = "DIRAC : DIRAC submission failed"
    assertDiracFailsWith( self.ujo._split(), error_message, self )
    self.log_mock.error.assert_called_once_with( error_message )

  def test_saveinputdata( self ):
    input_data = ["data1", "data2"]
    assertDiracSucceedsWith( self.ujo._saveInputData(input_data), "Input Data : Input data set to :\n%s" % "\n".join(input_data), self )
    
    for data in input_data:
      self.assertIn( data, self.ujo._data )    

  def test_append( self ):

    self.ujo.split = True
    app1 = Fcc()
    
    wf_mock = Mock()
    self.ujo.workflow = wf_mock
    
    PARENT_MODULE_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.Job'
    with patch('%s.Job.append' % PARENT_MODULE_NAME) as mock_append:

      self.ujo.append(app1)
      self.log_mock.debug.assert_called_once_with( "Append : Application '%s' registering ..." % app1.__class__.__name__ )
      mock_append.assert_called_once_with( app1 )

  def test_append_failed( self ):
    self.ujo.split = True
    app1 = Fcc()
    app2 = Fcc()    
    self.ujo._userApplications = set([app1, app2])
    error_message = "Append : You try to append many times the same application, please fix it !"
    assertDiracFailsWith( self.ujo.append(app1), error_message, self )
    self.log_mock.error.assert_called_with( error_message )

  def test_atomicsubmission( self ):
    app1 = Fcc()
    app2 = Fcc()    
    self.ujo._userApplications = set([app1, app2])
    assertEqualsImproved( self.ujo._atomicSubmission(), ("Atomic", []), self )
    self.log_mock.info.assert_called_with( "Job splitting : No splitting to apply, then 'atomic submission' will be used" )

  def test_checkjobconsistency( self ):
    app1 = Fcc()
    app2 = Fcc()    
    self.ujo._userApplications = set([app1, app2])
    self.ujo._switch = ["byEvents"]
    self.ujo.split = "byEvents"
    self.assertTrue( self.ujo._checkJobConsistency() )
    info_message = "Job consistency : _checkJobConsistency() successfull"
    self.log_mock.info.assert_called_with( info_message )

  def test_checkjobconsistency_no_applications( self ):
    self.ujo._userApplications = set()
    self.assertFalse( self.ujo._checkJobConsistency() )
    error_message = (
      "Job : Your job is empty !\n"
      "You have to append at least one application\n"
      "Job consistency : _checkJobConsistency failed"
    )
    self.log_mock.error.assert_called_once_with( error_message )

  def test_checkjobconsistency_bad_split_parameter( self ):
    app1 = Fcc()
    app2 = Fcc()    
    self.ujo._userApplications = set([app1, app2])
    self.ujo._switch = ["byEvents"]
    self.ujo.split = "byHand"
    self.assertFalse( self.ujo._checkJobConsistency() )
    error_message = (
      "Job splitting : Bad split value\n"
      "Possible values are :\n"
      "- byData\n"
      "- byEvents\n"
      "- None\n"
      "Job consistency : _checkJobConsistency failed"
    )
    self.log_mock.error.assert_called_once_with( error_message )

  def test_checkjobconsistency_no_same_events( self ):
    app1 = Fcc()
    app2 = Fcc()
    app1.numberOfEvents = 1
    app2.numberOfEvents = 2   
    self.ujo._userApplications = set([app1, app2])
    self.ujo._switch = ["byEvents"]
    self.ujo.split = "byEvents"
    self.assertTrue( self.ujo._checkJobConsistency())
    self.log_mock.warn.assert_called_once_with( "Job : Applications should all have the same number of events" )

  def test_checkjobconsistency_negative_events( self ):
    app1 = Fcc()
    app2 = Fcc()
    app1.numberOfEvents = app2.numberOfEvents = -1    
    self.ujo._userApplications = set([app1, app2])
    self.ujo._switch = ["byEvents"]
    self.ujo.split = "byEvents"
    self.assertTrue( self.ujo._checkJobConsistency() )
    info_message = "Job consistency : _checkJobConsistency() successfull"
    self.log_mock.info.assert_called_with( info_message )
    warn_message = (
      "Job : You set the number of events to -1 without input data\n"
      "Was that intentional ?"
    )
    self.log_mock.warn.assert_called_once_with( warn_message )

  def test_splitbydata( self ):
    self.ujo._data = ['data1', 'data2']
    app1 = Fcc()
    app2 = Fcc()
    self.ujo._userApplications = set([app1, app2])
    assertEqualsImproved( self.ujo._splitByData(), ["InputData", self.ujo._data], self )

  def test_splitbydata_no_data( self ):
    self.ujo._data = None
    self.assertFalse( self.ujo._splitByData() )
    error_message = (
      "Job splitting : Can not continue, missing input data\n"
      "splitting 'byData' method needs input data"
    )
    self.log_mock.error.assert_called_once_with( error_message )

  def test_splitbyevents_1st_case( self ):
    app1 = Fcc()
    app2 = Fcc()
    self.ujo._userApplications = set([app1, app2])

    self.ujo.eventsPerJob = 2
    self.ujo.njobs = 2

    map_event_job = [2, 2]

    assertEqualsImproved( self.ujo._splitByEvents(), ["NumberOfEvents", map_event_job], self )

    debug_message = (
      "Job splitting : 1st case\n"
      "events per job and number of jobs have been given (easy)"
    )

    self.log_mock.debug.assert_any_call( debug_message )

    debug_message = (
      "Job splitting : Here is the 'distribution' of events over the jobs\n"
      "A list element corresponds to a job and the element value"
      " is the related number of events :\n%(map)s" % {'map':str(map_event_job)}
    )

    self.log_mock.debug.assert_any_call( debug_message )

    info_message = (
      "Job splitting : Your submission consists"
      " of %(number)d job(s)" % {'number':len(map_event_job)}
    )
    self.log_mock.info.assert_any_call( info_message )

  def test_splitbyevents_2nd_case( self ):
    app1 = Fcc()
    app2 = Fcc()
    self.ujo._userApplications = set([app1, app2])

    self.ujo.eventsPerJob = 2
    self.ujo.totalNumberOfEvents = 2

    map_event_job = [2]

    assertEqualsImproved( self.ujo._splitByEvents(), ["NumberOfEvents", map_event_job], self )

    debug_message = (
      "Job splitting : 2nd case\n"
      "Only events per job has been given but we know the total"
      " number of events, so we have to compute the number of jobs required"
    )

    self.log_mock.debug.assert_any_call( debug_message )

    debug_message = (
      "Job splitting : Here is the 'distribution' of events over the jobs\n"
      "A list element corresponds to a job and the element value"
      " is the related number of events :\n%(map)s" % {'map':str(map_event_job)}
    )

    self.log_mock.debug.assert_any_call( debug_message )   
    
    info_message = (
      "Job splitting : Your submission consists"
      " of %(number)d job(s)" % {'number':len(map_event_job)}
    )
    self.log_mock.info.assert_any_call( info_message )

  def test_splitbyevents_2nd_case_failed( self ):
    app1 = Fcc()
    app2 = Fcc()
    self.ujo._userApplications = set([app1, app2])

    self.ujo.eventsPerJob = 3
    self.ujo.totalNumberOfEvents = 2
    self.assertFalse( self.ujo._splitByEvents() )

    debug_message = (
      "Job splitting : 2nd case\n"
      "Only events per job has been given but we know the total"
      " number of events, so we have to compute the number of jobs required"
    )

    self.log_mock.debug.assert_any_call( debug_message )

    error_message = (
      "Job splitting : The number of events per job has to be"
      " lower than or equal to the total number of events"
    )
    self.log_mock.error.assert_called_once_with(  error_message )

  def test_splitbyevents_3rd_case( self ):
    app1 = Fcc()
    app2 = Fcc()
    self.ujo._userApplications = set([app1, app2])

    self.ujo.njobs = 2
    self.ujo.totalNumberOfEvents = 2

    map_event_job = [1, 1]

    assertEqualsImproved( self.ujo._splitByEvents(), ["NumberOfEvents", map_event_job], self )

    debug_message = (
      "Job splitting : 3rd case\n"
      "The number of jobs has to be given and the total number"
      " of events has to be set"
    )

    self.log_mock.debug.assert_any_call( debug_message )

    debug_message = (
      "Job splitting : Here is the 'distribution' of events over the jobs\n"
      "A list element corresponds to a job and the element value"
      " is the related number of events :\n%(map)s" % {'map':str(map_event_job)}
    )

    self.log_mock.debug.assert_any_call( debug_message )       

    info_message = (
      "Job splitting : Your submission consists"
      " of %(number)d job(s)" % {'number':len(map_event_job)}
    )
    self.log_mock.info.assert_any_call( info_message )
    
  def test_splitbyevents_3rd_case_failed( self ):
    app1 = Fcc()
    app2 = Fcc()
    self.ujo._userApplications = set([app1, app2])

    self.ujo.njobs = 2
    self.ujo.totalNumberOfEvents = None

    self.assertFalse( self.ujo._splitByEvents() )

    debug_message = (
      "Job splitting : 3rd case\n"
      "The number of jobs has to be given and the total number"
      " of events has to be set"
    )

    self.log_mock.debug.assert_any_call( debug_message )       

    error_message = (
      "Job splitting : The number of events has to be set\n"
      "It has to be greater than or equal to the number of jobs"
    )
    self.log_mock.error.assert_called_with( error_message )

  def test_toint( self ):
    assertEqualsImproved( self.ujo._toInt("2"), 2, self )

  def test_toint_negative( self ):
    self.assertFalse( self.ujo._toInt("-2") )
    error_message = (
      "Job splitting : Please, enter valid numbers :\n"
      "'events per job' and 'number of jobs' must be positive integers"
    )
    self.log_mock.error.assert_called_once_with( error_message )
