#!/usr/local/env python
"""
Test SLICPandora module

"""

import unittest
from mock import call, patch, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Interfaces.API.NewInterface.Applications import SLICPandora
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertDiracSucceeds, assertInImproved

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.Applications.SLICPandora'

#pylint: disable=protected-access
class SLICPandoraTestCase( unittest.TestCase ):
  """ Base class for the SLICPandora test cases
  """

  def setUp(self):
    """set up the objects"""
    self.slic = SLICPandora( {} )
    self.slic.version = 941

  def test_setters( self ):
    with patch('os.path.exists', new=Mock(return_value=True)):
      self.slic.setDetectorModel( 123 )
      self.slic.setPandoraSettings( 94 )
    self.slic.setStartFrom( 'soirmgf' )
    assertInImproved( '_checkArgs', self.slic._errorDict, self )
    ( det_found, start_found, pand_found ) = ( False, False, False )
    for err in self.slic._errorDict[ '_checkArgs' ]:
      if 'detectorModel' in err:
        det_found = True
      elif 'startfrom' in err:
        start_found = True
      elif 'pandoraSettings' in err:
        pand_found = True
    assertEqualsImproved( ( True, True, True ), ( det_found, start_found, pand_found ), self )
    assertEqualsImproved( len(self.slic.inputSB), 2, self )

  def test_setdetectormodel( self ):
    self.slic.setDetectorModel( 'lfn:/some/path/testDetectorv2' )
    assertEqualsImproved( ( self.slic.detectorModel, self.slic.inputSB ),
                          ( 'lfn:/some/path/testDetectorv2', [ 'lfn:/some/path/testDetectorv2' ] ), self )
    self.assertFalse( self.slic._errorDict )

  def test_setdectormodel_notfound( self ):
    with patch('os.path.exists', new=Mock(return_value=False)):
      self.slic.setDetectorModel( 'invalid_detector' )
      assertEqualsImproved( ( self.slic.detectorModel, self.slic.inputSB ),
                            ( 'invalid_detector', [] ), self )
      self.assertFalse( self.slic._errorDict )

  def test_setpandorasettings( self ):
    self.slic.setPandoraSettings( 'lfn:/some/path/my_pand_settings.txt' )
    assertEqualsImproved( ( self.slic.pandoraSettings, self.slic.inputSB ),
                          ( 'lfn:/some/path/my_pand_settings.txt', [ 'lfn:/some/path/my_pand_settings.txt' ] ),
                          self )
    self.assertFalse( self.slic._errorDict )

  def test_setpandorasettings_notfound( self ):
    with patch('os.path.exists', new=Mock(return_value=False)):
      self.slic.setPandoraSettings( 'invalid_settings.xml' )
      assertEqualsImproved( ( self.slic.pandoraSettings, self.slic.inputSB ),
                            ( 'invalid_settings.xml', [] ), self )
      self.assertFalse( self.slic._errorDict )

  def test_applicationmodule( self ):
    result = self.slic._applicationModule()
    self.assertIsNotNone( result )

  def test_applicationmodulevalues( self ):
    module_mock = Mock()
    self.slic._applicationModuleValues( module_mock )
    self.assertTrue( module_mock.setValue.called )
    assertEqualsImproved( len(module_mock.mock_calls), 4, self )

  def test_userjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.slic._userjobmodules( module_mock ), self )

  def test_prodjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.slic._prodjobmodules( module_mock ), self )

  def test_userjobmodules_fails( self ):
    with patch('%s._setUserJobFinalization' % MODULE_NAME, new=Mock(return_value=S_OK('something'))),\
         patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_test_err'))):
      assertDiracFailsWith( self.slic._userjobmodules( None ),
                            'userjobmodules failed', self )

  def test_prodjobmodules_fails( self ):
    with patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_OK('something'))), \
         patch('%s._setOutputComputeDataList' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_other_test_err'))):
      assertDiracFailsWith( self.slic._prodjobmodules( None ),
                            'prodjobmodules failed', self )

  def test_resolveparameters( self ):
    step_mock = Mock()
    assertDiracSucceeds( self.slic._resolveLinkedStepParameters( step_mock() ), self )
    assertDiracSucceeds( self.slic._checkWorkflowConsistency(), self )

  def test_resolveparameters_setlink( self ):
    step_mock = Mock()
    self.slic._linkedidx = 1
    self.slic._jobsteps = [ None, Mock() ]
    assertDiracSucceeds( self.slic._resolveLinkedStepParameters( step_mock() ), self )

  def test_checkconsistency( self ):
    import inspect
    self.slic.version = 'v2.4'
    self.slic.steeringFile = 'myTestSteerFile.txt'
    self.slic.pandoraSettings = 'mysettings.xml'
    self.slic.startFrom = False
    self.slic._jobtype = 'notUser'
    with patch('os.path.exists', new=Mock(side_effect=[ False ])), \
         patch.object(inspect.getmodule(SLICPandora), 'Exists', new=Mock(return_value=S_OK('success!'))):
      assertDiracSucceeds( self.slic._checkConsistency( 'myTestJob' ), self )

  def test_checkconsistency_noversion( self ):
    self.slic.version = None
    assertDiracFailsWith( self.slic._checkConsistency(), 'no version found', self )

  def test_checkconsistency_steering_Exists_fails( self ):
    import inspect
    self.slic.version = 'v2.4'
    self.slic.steeringFile = 'myTestSteerFile.txt'
    self.slic.trackingStrategy = 'myTestStrat'
    self.slic.detectorModel = 'myDetModel.wrongFormat'
    with patch('os.path.exists', new=Mock(side_effect=[ False ])), \
         patch.object(inspect.getmodule(SLICPandora), 'Exists', new=Mock(return_value=S_ERROR('Exists_test_err'))):
      assertDiracFailsWith( self.slic._checkConsistency( 'myTestJob' ), 'exists_test_err', self )

  def test_checkconsistency_nosettings( self ):
    self.slic.pandoraSettings = None
    self.slic.steeringFile = None
    assertDiracFailsWith( self.slic._checkConsistency(), 'pandorasettings not set', self )

  def test_checkconsistency_othercase( self ):
    import inspect
    self.slic.version = 'v2.4'
    self.slic.steeringFile = 'myTestSteerFile.txt'
    self.slic.pandoraSettings = 'mysettings.xml'
    self.slic.startFrom = True
    self.slic._jobtype = 'User'
    with patch('os.path.exists', new=Mock(side_effect=[ True ])), \
         patch.object(inspect.getmodule(SLICPandora), 'Exists', new=Mock(side_effect=IOError('dont call me'))):
      assertDiracSucceeds( self.slic._checkConsistency( 'myTestJob' ), self )













