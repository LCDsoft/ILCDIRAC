#!/usr/local/env python
"""
Test Mokka module

"""

import unittest
from mock import patch, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Interfaces.API.NewInterface.Applications import Mokka
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertDiracSucceeds

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.Applications.Mokka'

#pylint: disable=protected-access
class MokkaTestCase( unittest.TestCase ):
  """ Base class for the Mokka test cases
  """
  def setUp(self):
    """set up the objects"""
    self.mok = Mokka( {} )

  def test_setters( self ):
    self.assertFalse( self.mok._errorDict )
    self.assertFalse( self.mok.inputSB )
    self.mok.setRandomSeed( 'invalid_seed' )
    self.mok.setmcRunNumber( [ 'something', False, [] ] )
    self.mok.setDetectorModel( { 'bla' : True } )
    self.mok.setMacFile( 'lfn:/inval/dir/somefile.mac' )
    self.mok.setStartFrom( { 'myset', False } )
    self.mok.setProcessID( 129843 )
    self.mok.setDbSlice( 'lfn:/inval/dir/myDB.slice' )
    print self.mok._errorDict
    assertEqualsImproved( len( self.mok._errorDict['_checkArgs'] ), 5, self )
    assertEqualsImproved( self.mok.inputSB, [
      'lfn:/inval/dir/somefile.mac', 'lfn:/inval/dir/myDB.slice' ], self )
    assertEqualsImproved( ( self.mok.macFile, self.mok.dbSlice ), (
      'lfn:/inval/dir/somefile.mac', 'lfn:/inval/dir/myDB.slice' ), self )

  def test_setfiles_othercase( self ):
    with patch('os.path.exists', new=Mock(return_value=False)):
      self.assertFalse( self.mok._errorDict )
      self.mok.setMacFile( '/invalid/dir/myMac.file' )
      self.mok.setDbSlice( '/invalid/dir/someDb.sql' )
      assertEqualsImproved( ( self.mok.macFile, self.mok.dbSlice ),
                            ( '/invalid/dir/myMac.file', '/invalid/dir/someDb.sql' ), self )
      self.assertFalse( self.mok._errorDict )

  def test_userjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.mok._userjobmodules( module_mock ), self )

  def test_prodjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.mok._prodjobmodules( module_mock ), self )

  def test_userjobmodules_fails( self ):
    with patch('%s._setUserJobFinalization' % MODULE_NAME, new=Mock(return_value=S_OK('something'))),\
         patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_test_err'))):
      assertDiracFailsWith( self.mok._userjobmodules( None ),
                            'userjobmodules failed', self )

  def test_prodjobmodules_fails( self ):
    with patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_OK('something'))), \
         patch('%s._setOutputComputeDataList' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_other_test_err'))):
      assertDiracFailsWith( self.mok._prodjobmodules( None ),
                            'prodjobmodules failed', self )

  def test_checkconsistency( self ):
    self.mok.version = 8431
    self.mok.steeringFile =  'lfn:/nonvalid/dir/mysteer.stdhep'
    self.mok._jobtype = 'User'
    assertDiracSucceeds( self.mok._checkConsistency(), self )

  def test_checkconsistency_nouserjob( self ):
    with patch('os.path.exists', new=Mock(return_value=False)):
      self.mok.version = 8431
      self.mok.steeringFile =  '/nonvalid/dir/mysteer.stdhep'
      self.mok._jobtype = 'notUser'
      self.mok.detectorModel = 'myTestDetv100'
      assertDiracSucceeds( self.mok._checkConsistency(), self )
      assertEqualsImproved( self.mok._listofoutput, [
        { "outputFile":"@{OutputFile}", "outputPath":"@{OutputPath}", "outputDataSE":'@{OutputSE}' } ], self )
      assertEqualsImproved( self.mok.prodparameters, {
        'mokka_steeringfile' : '/nonvalid/dir/mysteer.stdhep', 'mokka_detectormodel' : 'myTestDetv100',
        'detectorType' : 'ILD' }, self )

  def test_checkconsistency_nouserjob_2( self ):
    with patch('os.path.exists', new=Mock(return_value=False)):
      self.mok.version = 8431
      self.mok.steeringFile =  '/nonvalid/dir/mysteer.stdhep'
      self.mok._jobtype = 'notUser'
      self.mok.detectorModel = 0
      assertDiracSucceeds( self.mok._checkConsistency(), self )
      assertEqualsImproved( self.mok._listofoutput, [ {
        "outputFile":"@{OutputFile}", "outputPath":"@{OutputPath}", "outputDataSE":'@{OutputSE}'
      } ], self )
      assertEqualsImproved( self.mok.prodparameters, {
        'mokka_steeringfile' : '/nonvalid/dir/mysteer.stdhep', 'detectorType' : 'ILD' }, self )

  def test_checkconsistency_noversion( self ):
    self.mok.version = None
    assertDiracFailsWith( self.mok._checkConsistency(), 'no version found', self )

  def test_checkconsistency_nosteeringfile( self ):
    self.mok.version = True
    self.mok.steeringFile = None
    assertDiracFailsWith( self.mok._checkConsistency(), 'no steering file', self )

  def test_resolvelinkedparams( self ):
    step_mock = Mock()
    input_mock = Mock()
    input_mock.getType.return_value = { 'abc' : False }
    self.mok._linkedidx = 3
    self.mok._jobsteps = [ None, None, None, input_mock ]
    assertDiracSucceeds( self.mok._resolveLinkedStepParameters( step_mock ), self )
    step_mock.setLink.assert_called_once_with( 'InputFile', { 'abc' : False }, 'OutputFile' )

  def test_resolvelinkedparams_noinputstep( self ):
    self.mok._linkedidx = None
    self.mok._inputappstep = []
    assertDiracSucceeds( self.mok._resolveLinkedStepParameters( None ), self )
