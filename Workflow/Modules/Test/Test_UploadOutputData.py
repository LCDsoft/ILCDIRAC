"""
Unit tests for the UploadOutputData module
"""

import unittest
from mock import patch, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Workflow.Modules.UploadOutputData import UploadOutputData 
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertDiracSucceeds, assertDiracSucceedsWith
from ILCDIRAC.Tests.Utilities.OperationsMock import createOperationsMock

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Workflow.Modules.UploadOutputData'

class UploadOutputDataTestCase( unittest.TestCase ):
  """ Base class for the UploadOutputData test cases
  """

  def setUp( self ):
    self.upod = UploadOutputData()

  def test_applicationSpecificInputs( self ):
    self.upod.step_commons['TestFailover'] = 'something'
    self.upod.workflow_commons['outputList'] = [ { 'appdict' : True, 'myOutput' : 'yes', 'outputFile' : '' },
                                                 { 'outputFile' : True } ]
    self.upod.workflow_commons['PRODUCTION_ID'] = 1834
    self.upod.workflow_commons['JOB_ID'] = 418
    self.upod.workflow_commons['outputDataFileMask'] = [ 'mycoollist' ]
    with patch('%s.getProdFilename' % MODULE_NAME, new=Mock(side_effect=[ 'myOutputF_1', 'other_file.txt' ])):
      assertDiracSucceedsWith( self.upod.applicationSpecificInputs(), 'Parameters resolved', self )
    assertEqualsImproved( self.upod.outputList, [
      { 'appdict' : True, 'myOutput' : 'yes', 'outputFile' : 'myOutputF_1' },
      { 'outputFile' : 'other_file.txt' } ], self )

  def test_execute_resolveinput_fails( self ):
    self.upod.InputData = [ 'blabal' ]
    with patch.object(self.upod, 'resolveInputVariables', new=Mock(return_value=S_ERROR('resolv_input_testerr'))):
      assertDiracFailsWith( self.upod.execute(), 'resolv_input_testerr', self )

  def test_execute_status_notok( self ):
    self.upod.stepStatus = S_ERROR( 'test_something_went_wrong' )
    assertDiracSucceedsWith( self.upod.execute(), 'No output data upload attempt', self )

  @patch('ILCDIRAC.Core.Utilities.ProductionData.Operations', new=createOperationsMock())
  def test_execute_getcandidate_fails( self ):
    self.upod.prodOutputLFNs = [ '/ilc/prod/clic/example_file' ]
    with patch.object(self.upod, 'getCandidateFiles', new=Mock(return_value=S_ERROR('cand_file_not_found_testerr'))):
      assertDiracFailsWith( self.upod.execute(), 'cand_file_not_found_testerr', self )
      assertEqualsImproved( self.upod.experiment, 'CLIC', self )

  @patch('ILCDIRAC.Core.Utilities.ProductionData.Operations', new=createOperationsMock())
  def test_execute_metadata_not_found( self ):
    self.upod.prodOutputLFNs = [ '/ilc/prod/ilc/sid/example_file' ]
    with patch.object(self.upod, 'getCandidateFiles', new=Mock(return_value=S_OK({}))), \
         patch.object(self.upod, 'getFileMetadata', new=Mock(return_value=S_ERROR( 'no_meta_test_data_err' ))):
      assertDiracFailsWith( self.upod.execute(), 'no_meta_test_data_err', self )
      assertEqualsImproved( self.upod.experiment, 'ILC_SID', self )

  @patch('ILCDIRAC.Core.Utilities.ProductionData.Operations', new=createOperationsMock())
  def test_execute_metadata_empty( self ):
    self.upod.prodOutputLFNs = [ '/ilc/prod/ilc/mc-dbd/example_file' ]
    with patch.object(self.upod, 'getCandidateFiles', new=Mock(return_value=S_OK({}))), \
         patch.object(self.upod, 'getFileMetadata', new=Mock(return_value=S_OK())):
      assertDiracSucceeds( self.upod.execute(), self )
      assertEqualsImproved( self.upod.experiment, 'ILC_ILD', self )

  @patch('ILCDIRAC.Core.Utilities.ProductionData.Operations', new=createOperationsMock())
  def test_execute_nooutputse( self ):
    self.upod.prodOutputLFNs = [ '/ilc/prod/ilc/mc-dbd/example_file' ]
    with patch.object(self.upod, 'getCandidateFiles', new=Mock(return_value=S_OK({}))), \
         patch.object(self.upod, 'getFileMetadata', new=Mock(return_value=S_OK( { 'fileTestName' : { 'workflowSE' : 'testSE', 'otherTestMetadata' : True } } ))), \
         patch('%s.getDestinationSEList' % MODULE_NAME, new=Mock(return_value=S_ERROR('getdestlist_testerr'))):
      assertDiracFailsWith( self.upod.execute(), 'getdestlist_testerr', self )
      assertEqualsImproved( self.upod.experiment, 'ILC_ILD', self )

  @patch('ILCDIRAC.Core.Utilities.ProductionData.Operations', new=createOperationsMock())
  def test_execute_disabledmodule( self ):
    self.upod.prodOutputLFNs = [ '/ilc/prod/ilc/mc-dbd/example_file' ]
    with patch.object(self.upod, 'getCandidateFiles', new=Mock(return_value=S_OK({}))), \
         patch.object(self.upod, 'getFileMetadata', new=Mock(return_value=S_OK( { 'fileTestName' : { 'workflowSE' : 'testSE', 'otherTestMetadata' : True } } ))), \
         patch('%s.getDestinationSEList' % MODULE_NAME, new=Mock(return_value=S_OK('myTestSE'))):
      assertDiracSucceedsWith( self.upod.execute(), 'Module is disabled by control flag', self )
      assertEqualsImproved( self.upod.experiment, 'ILC_ILD', self )

  @patch('ILCDIRAC.Core.Utilities.ProductionData.Operations', new=createOperationsMock())
  def test_execute_success( self ):
    trans_mock = Mock()
    trans_mock.transferAndRegisterFile.return_value = S_OK('bla')
    trans_mock.transferAndRegisterFileFailover.return_value = S_OK('bla')
    self.upod.enable = True
    self.upod.jobID = 13831
    self.upod.prodOutputLFNs = [ '/ilc/prod/ilc/mc-dbd/example_file' ]
    with patch.object(self.upod, 'getCandidateFiles', new=Mock(return_value=S_OK({}))), \
         patch.object(self.upod, 'getFileMetadata', new=Mock(return_value=S_OK( { 'fileTestName' : { 'workflowSE' : 'testSE', 'otherTestMetadata' : True, 'localpath' : None, 'lfn' : None, 'resolvedSE' : None, 'filedict' : None } } ))), \
         patch('%s.getDestinationSEList' % MODULE_NAME, new=Mock(return_value=S_OK('myTestSE'))), \
         patch('%s.FailoverTransfer' % MODULE_NAME, new=Mock(return_value=trans_mock)):
      assertDiracSucceedsWith( self.upod.execute(), 'Output data uploaded', self )
      assertEqualsImproved( self.upod.experiment, 'ILC_ILD', self )

  def test_gettreatedoutputlist_nodata( self ):
    olist = {}
    self.assertIsNone( self.upod.getTreatedOutputlistNew( [], olist, { 'outputFile' :
                                                                       '/inval/dir/myoutputfile.txt' } ) )
    self.assertFalse( olist )

  def test_gettreatedoutputlist_fullcase( self ):
    myoutputlist = {}
    self.assertIsNone( self.upod.getTreatedOutputlistNew( [ 'myoutputfile_gen' ], myoutputlist,
                                                          { 'outputFile' : 'myoutputfile_gen.stdhep' } ) )
    self.assertTrue( myoutputlist )

  def test_gettreatedoutputlist_fullcase_2( self ):
    myoutputlist = {}
    self.assertIsNone( self.upod.getTreatedOutputlistNew( [ 'myoutputfile_sim' ], myoutputlist,
                                                          { 'outputFile' : 'myoutputfile_sim.slcio' } ) )
    self.assertTrue( myoutputlist )

  def test_gettreatedoutputlist_fullcase_3( self ):
    myoutputlist = {}
    self.assertIsNone( self.upod.getTreatedOutputlistNew( [ 'myoutputfile_rec' ], myoutputlist,
                                                          { 'outputFile' : 'myoutputfile_rec.slcio' } ) )
    self.assertTrue( myoutputlist )

  def test_gettreatedoutputlist_fullcase_4( self ):
    myoutputlist = {}
    self.assertIsNone( self.upod.getTreatedOutputlistNew( [ 'myoutputfile_dst_1' ], myoutputlist,
                                                          { 'outputFile' : 'myoutputfile_dst.slcio' } ) )
    self.assertTrue( myoutputlist )

  def test_gettreatedoutputlist_fullcase_5( self ):
    myoutputlist = {}
    self.assertIsNone( self.upod.getTreatedOutputlistNew( [ 'myoutputfile_and_some_more_dst_1' ], myoutputlist,
                                                          { 'outputFile' : 'myoutputfile_dst.slcio' } ) )
    self.assertTrue( myoutputlist )

  def test_gettreatedoutputlist_sim_fails( self ):
    myoutputlist = {}
    self.assertIsNone( self.upod.getTreatedOutputlistNew( [ 'myoutputfile_sim' ], myoutputlist,
                                                          { 'outputFile' :
                                                            '/invalid/dir/myoutputfile_sim.slcio' } ) )
    self.assertFalse( myoutputlist )

  def test_gettreatedoutputlist_rec_fails( self ):
    myoutputlist = {}
    self.assertIsNone( self.upod.getTreatedOutputlistNew( [ 'myoutputfile_rec' ], myoutputlist,
                                                          { 'outputFile' :
                                                            '/invalid/dir/myoutputfile_rec.slcio' } ) )
    self.assertFalse( myoutputlist )

  def test_gettreatedoutputlist_dst_fails( self ):
    myoutputlist = {}
    self.assertIsNone( self.upod.getTreatedOutputlistNew( [ 'myoutputfile_dst_1' ], myoutputlist,
                                                          { 'outputFile' :
                                                            '/invalid/dir/myoutputfile_dst.slcio' } ) )
    self.assertFalse( myoutputlist )
