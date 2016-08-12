#!/usr/local/env python
"""
Test SLIC module

"""

import unittest
from mock import patch, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Interfaces.API.NewInterface.Applications import SLIC
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertDiracSucceeds, assertInImproved

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.Applications.SLIC'

#pylint: disable=protected-access
class SLICTestCase( unittest.TestCase ):
  """ Base class for the SLIC test cases
  """
  def setUp(self):
    """set up the objects"""
    self.slic = SLIC( {} )
    self.slic.version = 941

  def test_setters( self ):
    self.slic.setRandomSeed( 'someInvalidSeed' )
    self.slic.setDetectorModel( 'myDetModelTestme' )
    self.slic.setStartFrom( 'soirmgf' )
    assertInImproved( '_checkArgs', self.slic._errorDict, self )
    ( rand_found, start_found ) = ( False, False )
    for err in self.slic._errorDict[ '_checkArgs' ]:
      if 'randomSeed' in err:
        rand_found = True
      elif 'startfrom' in err:
        start_found = True
    assertEqualsImproved( ( True, True, 'myDetModelTestme' ), ( rand_found, start_found,
                                                                self.slic.detectorModel ), self )

  def test_setdetectormodel( self ):
    self.slic.setDetectorModel( 'lfn:/my/det.model' )
    assertInImproved( 'lfn:/my/det.model', self.slic.inputSB, self )
    assertEqualsImproved( self.slic.detectorModel, 'det.model', self )
    self.slic = SLIC( {} )
    with patch('os.path.exists', new=Mock(return_value=True)):
      self.slic.setDetectorModel( '/my/local/dir/detectorv212.stdhep.zip' )
      assertEqualsImproved( ( self.slic.detectorModel, self.slic.inputSB ),
                            ( 'detectorv212.stdhep',
                              [ '/my/local/dir/detectorv212.stdhep.zip' ] ), self )
    self.slic = SLIC( {} )
    with patch('os.path.exists', new=Mock(return_value=False)):
      self.slic.setDetectorModel( '/my/local/dir/detectorv212.stdhep.zip' )
      assertEqualsImproved( ( self.slic.detectorModel, self.slic.inputSB ),
                            ( 'detectorv212.stdhep', [] ), self )

  def test_userjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.slic._userjobmodules( module_mock ), self )

  def test_prodjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.slic._prodjobmodules( module_mock ), self )

  def test_userjobmodules_fails( self ):
    with patch('%s._setUserJobFinalization' % MODULE_NAME, new=Mock(return_value=S_OK('something'))),\
         patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_test_err'))):
      assertDiracFailsWith( self.slic._userjobmodules( None ), 'userjobmodules failed', self )

  def test_prodjobmodules_fails( self ):
    with patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_OK('something'))), \
         patch('%s._setOutputComputeDataList' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_other_test_err'))):
      assertDiracFailsWith( self.slic._prodjobmodules( None ), 'prodjobmodules failed', self )

  def test_checkconsistency( self ):
    self.slic._jobtype = 'User'
    self.slic.startFrom = True
    with patch('os.path.exists', new=Mock(return_value=True)):
      assertDiracSucceeds( self.slic._checkConsistency(), self )

  def test_checkconsistency_othercase( self ):
    self.slic.detectorModel = 'someModel'
    self.slic.steeringFile = 'LFN:/my/consistent/steeringfile.st'
    self.slic._jobtype = 'notUser'
    self.slic.startFrom = False
    with patch('os.path.exists', new=Mock(return_value=False)):
      assertDiracSucceeds( self.slic._checkConsistency(), self )

  def test_checkconsistency_download_succeeds( self ):
    import inspect
    self.slic.steeringFile = '/my/remote/path.st'
    self.slic._jobtype = 'notUser'
    self.slic.startFrom = False
    with patch('os.path.exists', new=Mock(return_value=False)), \
         patch.object(inspect.getmodule(SLIC), 'Exists', new=Mock(return_value=S_OK('ok'))):
      assertDiracSucceeds( self.slic._checkConsistency(), self )

  def test_checkconsistency_noversion( self ):
    self.slic.version = None
    assertDiracFailsWith( self.slic._checkConsistency(), 'no version found', self )

  def test_checkconsistency_nosteeringfile( self ):
    import inspect
    self.slic.steeringFile = 'mysteer.file'
    with patch('os.path.exists', new=Mock(return_value=False)), \
         patch.object(inspect.getmodule(SLIC), 'Exists', new=Mock(return_value=S_ERROR('failed downloading, testerr'))):
      assertDiracFailsWith( self.slic._checkConsistency(), 'failed downloading, testerr', self )

  def test_resolvestepparams( self ):
    step_mock = Mock()
    inputstep_mock = Mock()
    self.slic._linkedidx = 2
    self.slic._jobsteps = [ None, None, inputstep_mock ]
    assertDiracSucceeds( self.slic._resolveLinkedStepParameters( step_mock ), self )

    assertDiracSucceeds( self.slic._checkWorkflowConsistency(), self )

  def test_resolvestepparams_noinputstep( self ):
    self.slic._linkedidx = '139'
    self.slic._jobsteps = []
    assertDiracSucceeds( self.slic._resolveLinkedStepParameters( '' ), self )
































