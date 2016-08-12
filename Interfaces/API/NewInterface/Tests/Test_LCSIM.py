#!/usr/local/env python
"""
Test LCSIM module

"""

import unittest
from mock import call, patch, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Interfaces.API.NewInterface.Applications import LCSIM
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertDiracSucceeds

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.Applications.LCSIM'

#pylint: disable=protected-access,too-many-public-methods
class LCSIMTestCase( unittest.TestCase ):
  """ Base class for the LCSIM test cases
  """
  def setUp(self):
    """set up the objects"""
    self.lcs = LCSIM()

  def test_setoutputrecfile( self ):
    self.assertFalse( self.lcs._errorDict )
    self.lcs.setOutputRecFile( 'myTestRECfile.rec', 'test/path/rec' )
    self.assertFalse( self.lcs._errorDict )
    assertEqualsImproved( self.lcs.outputRecFile, 'myTestRECfile.rec', self )
    assertEqualsImproved( self.lcs.outputRecPath, 'test/path/rec', self )

  def test_setoutputrecfile_argcheck_fails( self ):
    self.assertFalse( self.lcs._errorDict )
    self.lcs.setOutputRecFile( 123 )
    self.assertTrue( self.lcs._errorDict )

  def test_setoutputdstfile( self ):
    self.assertFalse( self.lcs._errorDict )
    self.lcs.setOutputDstFile( 'myTestDSTfile.dst', 'test/path/dst' )
    self.assertFalse( self.lcs._errorDict )
    assertEqualsImproved( self.lcs.outputDstFile, 'myTestDSTfile.dst', self )
    assertEqualsImproved( self.lcs.outputDstPath, 'test/path/dst', self )

  def test_setoutputdstfile_argcheck_fails( self ):
    self.assertFalse( self.lcs._errorDict )
    self.lcs.setOutputDstFile( 123 )
    self.assertTrue( self.lcs._errorDict )

  def test_setaliasproperties( self ):
    with patch('os.path.exists', new=Mock(return_value=False)):
      self.lcs.setAliasProperties( 'myAlias.Properties.Test' )
    assertEqualsImproved( self.lcs.aliasProperties, 'myAlias.Properties.Test', self )
    self.assertFalse( self.lcs._errorDict )
    assertEqualsImproved( self.lcs.inputSB, [], self )

  def test_setaliasproperties_argcheck_fails( self ):
    with patch('os.path.exists', new=Mock(return_value=True)):
      self.lcs.setAliasProperties( 8934 )
    assertEqualsImproved( self.lcs.aliasProperties, 8934, self )
    self.assertTrue( self.lcs._errorDict )
    assertEqualsImproved( self.lcs.inputSB, [ 8934 ], self )

  def test_setaliasproperties_lfn( self ):
    with patch('os.path.exists', new=Mock(return_value=False)):
      self.lcs.setAliasProperties( 'LFN:/myAlias.Properties.Test' )
    assertEqualsImproved( self.lcs.aliasProperties, 'LFN:/myAlias.Properties.Test', self )
    self.assertFalse( self.lcs._errorDict )
    assertEqualsImproved( self.lcs.inputSB, [ 'LFN:/myAlias.Properties.Test' ], self )

  def test_setdetectormodel( self ):
    with patch('os.path.exists', new=Mock(return_value=False)):
      self.lcs.setDetectorModel( 'Test_Detectorv103.clic' )
    assertEqualsImproved( self.lcs.detectorModel, 'Test_Detectorv103.clic', self )
    self.assertFalse( self.lcs._errorDict )
    assertEqualsImproved( self.lcs.inputSB, [], self )

  def test_setdetectormodel_argcheck_fails( self ):
    with patch('os.path.exists', new=Mock(return_value=True)):
      self.lcs.setDetectorModel( 2489 )
    assertEqualsImproved( self.lcs.detectorModel, 2489, self )
    self.assertTrue( self.lcs._errorDict )
    assertEqualsImproved( self.lcs.inputSB, [ 2489 ], self )

  def test_setdetectormodel_lfn( self ):
    with patch('os.path.exists', new=Mock(return_value=False)):
      self.lcs.setDetectorModel( 'LFN:/dir/other_dir/DetectorTestmeModel.ilc' )
    assertEqualsImproved( self.lcs.detectorModel, 'LFN:/dir/other_dir/DetectorTestmeModel.ilc', self )
    self.assertFalse( self.lcs._errorDict )
    assertEqualsImproved( self.lcs.inputSB, [ 'LFN:/dir/other_dir/DetectorTestmeModel.ilc' ], self )

  def test_settrackingstrategy( self ):
    with patch('os.path.exists', new=Mock(return_value=False)):
      self.lcs.setTrackingStrategy( 'Test_Strategy.trackme' )
    assertEqualsImproved( self.lcs.trackingStrategy, 'Test_Strategy.trackme', self )
    self.assertFalse( self.lcs._errorDict )
    assertEqualsImproved( self.lcs.inputSB, [], self )

  def test_settrackingstrategy_argcheck_fails( self ):
    with patch('os.path.exists', new=Mock(return_value=True)):
      self.lcs.setTrackingStrategy( 4812 )
    assertEqualsImproved( self.lcs.trackingStrategy, 4812, self )
    self.assertTrue( self.lcs._errorDict )
    assertEqualsImproved( self.lcs.inputSB, [ 4812 ], self )

  def test_settrackingstrategy_lfn( self ):
    with patch('os.path.exists', new=Mock(return_value=False)):
      self.lcs.setTrackingStrategy( 'LFN:/my/track/strat.txt' )
    assertEqualsImproved( self.lcs.trackingStrategy, 'LFN:/my/track/strat.txt', self )
    self.assertFalse( self.lcs._errorDict )
    assertEqualsImproved( self.lcs.inputSB, [ 'LFN:/my/track/strat.txt' ], self )

  def test_setextraparams( self ):
    self.assertFalse( self.lcs.willBeCut )
    self.assertFalse( self.lcs._errorDict )
    self.lcs.setExtraParams( 'myTestPar' )
    self.lcs.willRunSLICPandora()
    self.assertFalse( self.lcs._errorDict )
    assertEqualsImproved( self.lcs.extraParams, 'myTestPar', self  )
    self.assertTrue( self.lcs.willBeCut )

  def test_setextraparams_argcheck_fails( self ):
    self.assertFalse( self.lcs._errorDict )
    self.lcs.setExtraParams( [ 8914 ] )
    self.assertTrue( self.lcs._errorDict )

  def test_userjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.lcs._userjobmodules( module_mock ), self )

  def test_prodjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.lcs._prodjobmodules( module_mock ), self )

  def test_userjobmodules_fails( self ):
    with patch('%s._setUserJobFinalization' % MODULE_NAME, new=Mock(return_value=S_OK('something'))),\
         patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_test_err'))):
      assertDiracFailsWith( self.lcs._userjobmodules( None ),
                            'userjobmodules failed', self )

  def test_prodjobmodules_fails( self ):
    with patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_OK('something'))), \
         patch('%s._setOutputComputeDataList' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_other_test_err'))):
      assertDiracFailsWith( self.lcs._prodjobmodules( None ),
                            'prodjobmodules failed', self )

  def test_checkwfconsistency( self ):
    assertDiracSucceeds( self.lcs._checkWorkflowConsistency(), self )

  def test_resolveparameters( self ):
    step_mock = Mock()
    assertDiracSucceeds( self.lcs._resolveLinkedStepParameters( step_mock() ), self )

  def test_resolveparameters_setlink( self ):
    step_mock = Mock()
    self.lcs._linkedidx = 1
    self.lcs._jobsteps = [ None, Mock() ]
    assertDiracSucceeds( self.lcs._resolveLinkedStepParameters( step_mock() ), self )

  def test_checkconsistency( self ):
    import inspect
    self.lcs.version = 'v2.4'
    self.lcs.steeringFile = 'myTestSteerFile.txt'
    self.lcs.trackingStrategy = 'myTestStrat'
    self.lcs.detectorModel = 'myDetModel.wrongFormat'
    with patch('os.path.exists', new=Mock(side_effect=[ False, True, False ])) as exists_mock, \
         patch.object(inspect.getmodule(LCSIM), 'Exists', new=Mock(return_value=S_OK())), \
         patch.object(inspect.getmodule(LCSIM), 'checkXMLValidity', new=Mock(return_value=S_OK())) as xml_mock:
      assertDiracFailsWith( self.lcs._checkConsistency( 'myTestJob' ),
                            'you have to pass an existing .zip file', self )
      exists_mock.assert_called_with( 'myTestStrat' )
      assertEqualsImproved(
        ( len( exists_mock.mock_calls ), exists_mock.mock_calls[0], exists_mock.mock_calls[2] ),
        ( 3, exists_mock.mock_calls[1], call( 'myTestStrat' ) ), self )
      xml_mock.assert_called_once_with( 'myTestSteerFile.txt' )

  def test_checkconsistency_noversion( self ):
    assertDiracFailsWith( self.lcs._checkConsistency(), 'no version found', self )

  def test_checkconsistency_basic( self ):
    self.lcs.energy = 2489
    self.lcs.numberOfEvents = 8245
    self.lcs.version = 'v1.2'
    self.lcs.steeringFile = ''
    self.lcs.detectorModel = ''
    self.lcs._jobtype = 'User'
    assertDiracSucceeds( self.lcs._checkConsistency(), self )

  def test_checkconsistency_steering_Exists_fails( self ):
    import inspect
    self.lcs.version = 'v2.4'
    self.lcs.steeringFile = 'myTestSteerFile.txt'
    self.lcs.trackingStrategy = 'myTestStrat'
    self.lcs.detectorModel = 'myDetModel.wrongFormat'
    with patch('os.path.exists', new=Mock(side_effect=[ False ])), \
         patch.object(inspect.getmodule(LCSIM), 'Exists', new=Mock(return_value=S_ERROR('Exists_test_err'))):
      assertDiracFailsWith( self.lcs._checkConsistency( 'myTestJob' ), 'exists_test_err', self )

  def test_checkconsistency_invalidxml( self ):
    import inspect
    self.lcs.version = 'v2.4'
    self.lcs.steeringFile = 'myTestSteerFile.txt'
    self.lcs.trackingStrategy = 'myTestStrat'
    self.lcs.detectorModel = 'myDetModel.wrongFormat'
    with patch('os.path.exists', new=Mock(side_effect=[ True, True ])), \
         patch.object(inspect.getmodule(LCSIM), 'checkXMLValidity', new=Mock(return_value=S_ERROR('xmlcheck_failed_testme'))):
      assertDiracFailsWith( self.lcs._checkConsistency( 'myTestJob' ), 'supplied steering file cannot be'
                            ' read by xml parser: xmlcheck_failed_testme', self )

  def test_checkconsistency_tracking_Exists_fails( self ):
    import inspect
    self.lcs.version = 'v2.4'
    self.lcs.steeringFile = 'myTestSteerFile.txt'
    self.lcs.trackingStrategy = 'myTestStrat'
    self.lcs.detectorModel = 'myDetModel.wrongFormat'
    with patch('os.path.exists', new=Mock(side_effect=[ True, False, False ])), \
         patch.object(inspect.getmodule(LCSIM), 'Exists', new=Mock(return_value=S_ERROR('xmlcheck_failed_testme'))):
      assertDiracFailsWith( self.lcs._checkConsistency( 'myTestJob' ), 'xmlcheck_failed_testme', self )

  def test_checkconsistency_othercase( self ):
    import inspect
    self.lcs.version = 'v2.4'
    self.lcs.steeringFile = ''
    self.lcs.trackingStrategy = 'lfn:/myTestStrat'
    self.lcs.detectorModel = 'correctDetector.zip'
    self.lcs._jobtype = 'notUser'
    self.lcs._listofoutput = []
    app1 = Mock()
    app1.appame = 'myTestApp'
    app2 = Mock()
    app2.appname = 'marlin'
    self.lcs._inputapp = [ app1, app2 ]
    self.lcs.outputFile = ''
    self.lcs.willBeCut = False
    with patch('os.path.exists', new=Mock(side_effect=[ False ])), \
         patch.object(inspect.getmodule(LCSIM), 'Exists', new=Mock(return_value=S_OK())):
      assertDiracSucceeds( self.lcs._checkConsistency( 'myTestJob' ), self )
      expected_output_list =  [ { "outputFile" : "@{outputREC}", "outputPath" : "@{outputPathREC}",
                                  "outputDataSE" : "@{OutputSE}" }, { "outputFile" : "@{outputDST}",
                                                                      "outputPath" : "@{outputPathDST}",
                                                                      "outputDataSE" : "@{OutputSE}" } ]
      assertEqualsImproved( self.lcs._listofoutput, expected_output_list, self )
      prodparam_dict = self.lcs.prodparameters
      assertEqualsImproved( ( prodparam_dict['detectorType'], prodparam_dict['lcsim_steeringfile'],
                              prodparam_dict['lcsim_trackingstrategy'] ), ( 'SID' , '', 'lfn:/myTestStrat' ),
                            self  )
