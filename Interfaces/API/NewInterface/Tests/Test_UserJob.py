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
  assertEqualsImproved

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.UserJob'
MIXIN_MODULE = 'ILCDIRAC.Interfaces.Utilities.SplitMixin'
MIXIN_CLASS = 'ILCDIRAC.Interfaces.Utilities.SplitMixin.SplitMixin'

# pylint: disable=protected-access, too-many-public-methods


class UserJobTestCase(unittest.TestCase):
  """Base class for the UserJob test cases."""

  def setUp(self):
    """Set up the objects."""
    self.log_mock = Mock(name="SubMock")
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
    ilc_mock().submitJob.return_value = S_OK('test_submission_successful')
    self.ujo.proxyinfo = S_OK( { 'group' : 'ilc_user' } )
    with patch('%s.UserJob._addToWorkflow' % MODULE_NAME, new=Mock(return_value=S_OK())), \
         patch('%s.DiracILC' % MODULE_NAME, new=ilc_mock):
      assertDiracSucceedsWith_equals( self.ujo.submit(), 'test_submission_successful', self )
      ilc_mock().submitJob.assert_called_once_with(self.ujo, 'wms')
      assert self.ujo.oktosubmit

  def test_submit_existing_dirac_instance(self):
    """Test submit with dirac instance."""
    ilc_mock = Mock()
    ilc_mock.submitJob.return_value = S_OK('test_submission_successful')
    self.ujo.proxyinfo = S_OK({'group': 'ilc_user'})
    with patch('%s.UserJob._addToWorkflow' % MODULE_NAME, new=Mock(return_value=S_OK())):
      assertDiracSucceedsWith_equals(self.ujo.submit(diracinstance=ilc_mock), 'test_submission_successful', self)
      ilc_mock.submitJob.assert_called_once_with(self.ujo, 'wms')
      assert self.ujo.oktosubmit

  def test_setinputdata_failed( self ):
    assertDiracFailsWith( self.ujo.setInputData( { '/mylfn1' : True, '/mylfn2' : False } ),
                          'expected lfn string or list of lfns for input data', self )

  def test_setinputdata(self):
    """Test setting input data."""
    assertDiracSucceeds(self.ujo.setInputData(['LFN:/mylfn1', 'LFN:/mylfn2']), self)
    self.assertEqual(self.ujo.workflow.parameters.find('InputData').getValue(), '/mylfn1;/mylfn2')
    assertDiracSucceeds(self.ujo.setInputData('/mylfn1'), self)
    self.assertEqual(self.ujo.workflow.parameters.find('InputData').getValue(), '/mylfn1')

  def test_inputsandbox( self ):
    self.ujo.inputsandbox = Mock()
    assertDiracSucceeds( self.ujo.setInputSandbox( 'LFN:/ilc/user/u/username/libraries.tar.gz' ), self )
    self.ujo.inputsandbox.extend.assert_called_once_with( [ 'LFN:/ilc/user/u/username/libraries.tar.gz' ] )

  def test_inputsandbox_dictpassed( self ):
    assertDiracFailsWith( self.ujo.setInputSandbox( { '/some/file' : True, '/my/dict' : True } ),
                          'File passed must be either single file or list of files', self )

  def test_setOutputData(self):
    """Test setting output data."""
    assertDiracSucceeds(self.ujo.setOutputData(['/myFile1', '/myFile2']), self)
    self.assertEqual(self.ujo.workflow.parameters.find('UserOutputData').getValue(), '/myFile1;/myFile2')

    assertDiracSucceeds(self.ujo.setOutputData('/myFile2'), self)
    self.assertEqual(self.ujo.workflow.parameters.find('UserOutputData').getValue(), '/myFile2')

    assertDiracSucceeds(self.ujo.setOutputData('/myFile2', OutputSE="MY-SE"), self)
    self.assertEqual(self.ujo.workflow.parameters.find('UserOutputData').getValue(), '/myFile2')
    self.assertEqual(self.ujo.workflow.parameters.find('UserOutputSE').getValue(), 'MY-SE')

    assertDiracSucceeds(self.ujo.setOutputData('/myFile2', OutputSE=["MY-SE", 'YOUR-SE']), self)
    self.assertEqual(self.ujo.workflow.parameters.find('UserOutputData').getValue(), '/myFile2')
    self.assertEqual(self.ujo.workflow.parameters.find('UserOutputSE').getValue(), 'MY-SE;YOUR-SE')


  def test_setoutputdata_dictpassed( self ):
    assertDiracFailsWith( self.ujo.setOutputData( { '/mydict' : True } ),
                          'Expected file name string or list of file names for output data', self )

  def test_setoutputdata_nolistse( self ):
    wf_mock = Mock()
    self.ujo.workflow = wf_mock
    with patch('%s.UserJob._addParameter' % MODULE_NAME, new=Mock()) as addparam_mock:
      assertDiracFailsWith(self.ujo.setOutputData(['mylfn1', 'other_lfn', 'last___lfn'],
                                                  OutputSE={'mydict': True}),
                           'Expected string or list for OutputSE', self)
      addparam_mock.assert_called_once_with(wf_mock, 'UserOutputData', 'JDL',
                                            'mylfn1;other_lfn;last___lfn', 'List of output data files')

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

  def test_setoutputsandbox_successes(self):
    """Test setting output sandbox."""
    assertDiracSucceeds(self.ujo.setOutputSandbox(['myfile.txt', 'myfile.doc']), self)
    self.assertEqual(self.ujo.workflow.parameters.find('OutputSandbox').getValue(), 'myfile.txt;myfile.doc')


  def test_setoutputsandbox_dictpassed( self ):
    wf_mock = Mock()
    self.ujo.workflow = wf_mock
    with patch('%s.UserJob._addParameter' % MODULE_NAME, new=Mock()) as addparam_mock:
      assertDiracFailsWith( self.ujo.setOutputSandbox( { 'mydict' : True } ),
                            'Expected file string or list of files for output sandbox contents', self )
      self.assertFalse( addparam_mock.called )

  def test_configs(self):
    """Test setting different config packages."""
    assertDiracSucceeds(self.ujo.setILDConfig('123.4'), self)
    self.assertEqual(self.ujo.workflow.parameters.find('ILDConfigPackage').getValue(), 'ILDConfig123.4')

    assertDiracSucceeds(self.ujo.setCLICConfig('567.8'), self)
    self.assertEqual(self.ujo.workflow.parameters.find('ClicConfigPackage').getValue(), 'ClicConfig567.8')

    self.assertIn('ildconfig', self.ujo.workflow.parameters.find('SoftwarePackages').getValue())
    self.assertIn('clicconfig', self.ujo.workflow.parameters.find('SoftwarePackages').getValue())

  def test_submit_split(self):
    """Test submitting with automatic splitting."""
    self.ujo._splittingOption = True
    self.ujo._split = Mock(return_value=S_OK())
    self.ujo.proxyinfo = S_OK({'group': 'ilc_user'})
    ilc_mock = Mock()
    ilc_mock.submitJob.return_value = S_OK('test_submission_successful')
    assertDiracSucceeds(self.ujo.submit(diracinstance=ilc_mock), self)
    ilc_mock.submitJob.assert_called_once_with(self.ujo, 'wms')

    self.ujo._splittingOption = True
    self.ujo._split = Mock(return_value=S_ERROR("Splitting InValid"))
    assertDiracFailsWith(self.ujo.submit(), "Splitting InValid", self)

  @patch("%s._checkSplitConsistency" % MIXIN_CLASS, new=Mock(return_value=S_OK()))
  def test_split_bydata(self):
    """Test splitting by data."""
    self.ujo._eventsPerJob = "1"
    self.ujo._numberOfJobs = "1"
    self.ujo._splittingOption = "byData"
    self.ujo._switch['byData'] = Mock(return_value=[("InputData", ["/ilc/user/u/username/data1"], True)])
    with patch("%s.UserJob.setParameterSequence" % MODULE_NAME) as mock_parametric, \
         patch('%s.LOG' % MIXIN_MODULE, new=self.log_mock):
      info_message = "Job splitting successful"
      assertDiracSucceeds(self.ujo._split(), self)
      self.log_mock.notice.assert_called_with(info_message)
      mock_parametric.assert_any_call("InputData", ["/ilc/user/u/username/data1"], True)

  @patch("%s.toInt" % MIXIN_MODULE, new=Mock(return_value=1))
  @patch("%s.UserJob._checkSplitConsistency" % MODULE_NAME, new=Mock(return_value=S_OK()))
  def test_split_byevents(self):
    """Test splitting by events."""
    self.ujo._splittingOption = "byEvents"
    self.ujo._switch['byEvents'] = Mock(return_value=[('NumberOfEvents', [1, 2], 'NbOfEvts')])
    with patch("%s.UserJob.setParameterSequence" % MODULE_NAME) as mock_parametric, \
         patch('%s.LOG' % MIXIN_MODULE, new=self.log_mock):
      info_message = "Job splitting successful"
      assertDiracSucceeds(self.ujo._split(), self)
      self.log_mock.notice.assert_called_with(info_message)
      mock_parametric.assert_any_call('NumberOfEvents', [1, 2], 'NbOfEvts')

  @patch("%s.toInt" % MIXIN_MODULE, new=Mock(return_value=1))
  @patch("%s.UserJob._checkSplitConsistency" % MODULE_NAME, new=Mock(return_value=S_OK()))
  def test_split_atomicsubmission(self):
    """Test splitting atomic."""
    self.ujo._splittingOption = None
    info_message = "Job splitting successful"
    with patch('%s.LOG' % MIXIN_MODULE, new=self.log_mock):
      assertDiracSucceeds(self.ujo._split(), self)
    self.log_mock.notice.assert_called_with(info_message)

  @patch("%s.toInt" % MIXIN_MODULE, new=Mock(return_value=False))
  def test_split_inputparameters_failed(self):
    """Test splitting input parameters with failure."""
    assertDiracFailsWith( self.ujo._split(), "Splitting: Invalid values for splitting", self )

  @patch("%s.toInt" % MIXIN_MODULE, new=Mock(return_value=1))
  @patch("%s._checkSplitConsistency" % MIXIN_CLASS, new=Mock(return_value=S_ERROR('failed')))
  def test_split_checkSplitConsistency_failed(self):
    """Test splitting check consistency with failure."""
    assertDiracFailsWith(self.ujo._split(), 'failed', self)

  @patch("%s.toInt" % MIXIN_MODULE, new=Mock(return_value=1))
  @patch("%s._checkSplitConsistency" % MIXIN_CLASS, new=Mock(return_value=S_OK()))
  def test_split_sequencer_fails(self):
    """Test splitting when the sequencer fails."""
    self.ujo._splittingOption = "bySequence"
    self.ujo._switch['bySequence'] = Mock(return_value=[])
    self.ujo.setParameterSequence = Mock()
    self.ujo._split()
    self.ujo.setParameterSequence.assert_not_called()

  def test_checkSplitconsistency(self):
    """Test splitting consistency check."""
    app1 = Fcc()
    app2 = Fcc()
    self.ujo.applicationlist = [app1, app2]
    self.ujo._switch = {"byEvents": lambda x: x}
    self.ujo._splittingOption = "byEvents"
    self.assertTrue(self.ujo._checkSplitConsistency())

  def test_checkjobconsistency_bad_split_parameter(self):
    """Test splitting consistency check with bad split parameters."""
    app1 = Fcc()
    app2 = Fcc()
    self.ujo.applicationlist = [app1, app2]
    self.ujo._switch = {"byEvents": lambda x: x }
    self.ujo._splittingOption = "byHand"
    self.assertFalse(self.ujo._checkSplitConsistency()['OK'])
    self.assertIn('_checkSplitConsistency', self.ujo.errorDict)

  def test_checkjobconsistency_no_same_events( self ):
    app1 = Fcc()
    app2 = Fcc()
    app1.numberOfEvents = 1
    app2.numberOfEvents = 2
    self.ujo.applicationlist = [app1, app2]
    self.ujo._switch = {"byEvents": lambda x: x }
    self.ujo._splittingOption = "byEvents"
    with patch('%s.LOG' % MIXIN_MODULE, new=self.log_mock):
      resCheck = self.ujo._checkSplitConsistency()
    self.assertFalse(resCheck['OK'])
    self.assertIn("have the same number", resCheck['Message'])

  def test_checkjobconsistency_negative_events( self ):
    app1 = Fcc()
    app2 = Fcc()
    app1.numberOfEvents = app2.numberOfEvents = -1
    self.ujo.applicationlist = [app1, app2]
    self.ujo._switch = ["byEvents"]
    self.ujo._splittingOption = "byEvents"
    self.assertTrue(self.ujo._checkSplitConsistency())

  def test_splitbydata( self ):
    self.ujo._data = ['data1', 'data2']
    app1 = Fcc()
    app2 = Fcc()
    self.ujo.applicationlist = [app1, app2]
    assertEqualsImproved(self.ujo._splitByData(), [("InputData", [['data1'], ['data2']], 'ParametricInputData')], self)

  def test_splitbydata_no_data(self):
    """Test splitting without data."""
    self.ujo._data = None
    self.assertFalse(self.ujo._splitByData())
    self.assertIn('_splitByData', self.ujo.errorDict)

  def test_splitbydata_incorrectparameter(self):
    """Test splitting with data."""
    self.ujo._data = ["/path/to/data1","/path/to/data2"]
    self.ujo._numberOfFilesPerJob = 3
    self.assertFalse(self.ujo._splitByData())
    self.assertIn('_splitByData', self.ujo.errorDict)

  def test_splitbyevents_1st_case(self):
    """Test splitting by events."""
    app1 = Fcc()
    app2 = Fcc()
    self.ujo.applicationlist = [app1, app2]
    self.ujo._eventsPerJob = 2
    self.ujo._numberOfJobs = 3
    map_event_job = [2, 2, 2]
    assertEqualsImproved(self.ujo._splitByEvents(), [('NumberOfEvents', map_event_job, 'NbOfEvts')], self)

  def test_splitbyevents_2nd_case( self ):
    app1 = Fcc()
    app2 = Fcc()
    self.ujo.applicationlist = [app1, app2]
    self.ujo._eventsPerJob = 3
    self.ujo._totalNumberOfEvents = 5
    map_event_job = [3, 2]
    assertEqualsImproved(self.ujo._splitByEvents(), [('NumberOfEvents', map_event_job, 'NbOfEvts')], self)

  def test_splitbyevents_2nd_case_failed( self ):
    app1 = Fcc()
    app2 = Fcc()
    self.ujo.applicationlist = [app1, app2]
    self.ujo._eventsPerJob = 3
    self.ujo._totalNumberOfEvents = 2
    self.assertFalse(self.ujo._splitByEvents())

  def test_splitbyevents_3rd_case(self):
    """Test splitting by events case 3."""
    app1 = Fcc()
    app2 = Fcc()
    self.ujo.applicationlist = [app1, app2]
    self.ujo._numberOfJobs = 2
    self.ujo._totalNumberOfEvents = 2
    map_event_job = [1, 1]
    assertEqualsImproved(self.ujo._splitByEvents(), [('NumberOfEvents', map_event_job, 'NbOfEvts')], self)

    self.ujo._numberOfJobs = 3
    self.ujo._totalNumberOfEvents = 5
    map_event_job = [2, 2, 1]
    assertEqualsImproved(self.ujo._splitByEvents(), [('NumberOfEvents', map_event_job, 'NbOfEvts')], self)

  def test_splitbyevents_3rd_case_failed(self):
    """Test splitting by events case 3 fails."""
    app1 = Fcc()
    app2 = Fcc()
    self.ujo.applicationlist = [app1, app2]
    self.ujo._numberOfJobs = 2
    self.ujo._totalNumberOfEvents = None
    self.assertFalse(self.ujo._splitByEvents())

  def test_setsplitevents(self):
    """Test splitting set split events."""
    self.ujo.setSplitEvents(42, 42, 126)
    assertEqualsImproved(self.ujo._totalNumberOfEvents, 126, self)
    assertEqualsImproved(self.ujo._eventsPerJob, 42, self)
    assertEqualsImproved(self.ujo._numberOfJobs, 42, self)
    assertEqualsImproved(self.ujo._splittingOption, "byEvents", self)

  def test_setsplitInputdata(self):
    """Test set split input data."""
    input_data = ["/path/to/data1", "/path/to/data2"]
    self.ujo.setSplitInputData(input_data)
    for data in input_data:
      self.assertIn(data, self.ujo._data)
    assertEqualsImproved(self.ujo._splittingOption, "byData", self)

  def test_setSplitFiles(self):
    """Test set split files over jobs."""
    self.ujo.setSplitFilesAcrossJobs('myLFN', 20, 20)
    self.assertEqual(self.ujo._data, ['myLFN'])
    self.assertEqual(self.ujo._eventsPerFile, 20)
    self.assertEqual(self.ujo._eventsPerJob, 20)

  def test_splitBySkip(self):
    """Test set split with skip."""
    self.ujo._eventsPerFile = 13
    self.ujo._eventsPerJob = 5
    self.ujo._data = ['lfn_%d' % d for d in [1, 2]]
    result = self.ujo._splitBySkip()
    self.assertEqual([('InputData', ['lfn_1', 'lfn_1', 'lfn_1', 'lfn_2', 'lfn_2', 'lfn_2'], 'InputData'),
                      ('startFrom', [0, 5, 10, 0, 5, 10], 'startFrom'),
                      ('NumberOfEvents', [5, 5, 3, 5, 5, 3], 'NbOfEvts')],
                     result)

    self.ujo._eventsPerFile = 15
    self.ujo._eventsPerJob = 5
    self.ujo._data = ['lfn_%d' % d for d in [1, 2]]
    result = self.ujo._splitBySkip()
    self.assertEqual([('InputData', ['lfn_1', 'lfn_1', 'lfn_1', 'lfn_2', 'lfn_2', 'lfn_2'], 'InputData'),
                      ('startFrom', [0, 5, 10, 0, 5, 10], 'startFrom'),
                      ('NumberOfEvents', [5, 5, 5, 5, 5, 5], 'NbOfEvts')],
                     result)

  def test_setSplittingStartIndex(self):
    """Test setting start index."""
    res = self.ujo.setSplittingStartIndex(111)
    self.assertTrue(res['OK'])
    self.assertEqual(self.ujo._startJobIndex, 111)

    self.ujo._startJobIndex = 0
    res = self.ujo.setSplittingStartIndex(-111)
    self.assertFalse(res['OK'])
    self.assertIn('setSplittingStartIndex', self.ujo.errorDict)
    self.assertEqual(self.ujo._startJobIndex, 0)

  def test_doNotAlter(self):
    """Test setting not altering the output."""
    self.ujo.setSplitDoNotAlterOutputFilename()
    self.assertIsNotNone(self.ujo.workflow.parameters.find('DoNotAlterOutputData'))
    self.assertEqual(self.ujo.workflow.parameters.find('DoNotAlterOutputData').getValue(), "True")

    self.ujo.setSplitDoNotAlterOutputFilename(False)
    self.assertIsNotNone(self.ujo.workflow.parameters.find('DoNotAlterOutputData'))
    self.assertEqual(self.ujo.workflow.parameters.find('DoNotAlterOutputData').getValue(), "False")

  def test_setSplitJobIndexList(self):
    """Test the setSplitJobIndexList function."""
    res = self.ujo.setSplitJobIndexList(range(0, 7, 3))
    self.assertTrue(res['OK'])
    self.assertEqual([0, 3, 6], self.ujo._jobIndexList)

    res = self.ujo.setSplitJobIndexList(set(range(1, 7, 3)))
    self.assertFalse(res['OK'])
    self.assertIn('Invalid argument type', res['Message'])
    self.assertEqual([0, 3, 6], self.ujo._jobIndexList)
