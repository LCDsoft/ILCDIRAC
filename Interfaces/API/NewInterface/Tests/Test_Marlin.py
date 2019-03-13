#!/usr/local/env python
"""
Test Marlin module

"""

import inspect
import unittest
from mock import patch, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Interfaces.API.NewInterface.Applications import Marlin
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertDiracSucceeds

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.Applications.Marlin'

#pylint: disable=protected-access
class MarlinTestCase( unittest.TestCase ):
  """ Base class for the Marlin test cases
  """
  def setUp( self ):
    """set up the objects"""
    self.mar = Marlin( {} )

  def test_setgear( self ):
    self.mar.setGearFile( 'lfn:/my/gear/file.txt' )
    self.assertFalse( self.mar._errorDict )
    self.assertIn( 'lfn:/my/gear/file.txt', self.mar.inputSB )

  def test_setKeepRecFile(self):
    """Tests for behaviour with KeepRecFile True/False."""
    self.assertTrue(self.mar.keepRecFile)
    self.mar.setKeepRecFile(False)
    self.assertFalse(self.mar._errorDict)
    self.mar.setKeepRecFile(True)
    self.assertTrue(self.mar.keepRecFile)
    self.assertFalse(self.mar._errorDict)
    self.mar.setKeepRecFile(123)
    self.assertIn('val = 123', str(self.mar._errorDict))

  def test_setoutputrec( self ):
    self.mar.setOutputRecFile( 'my/file.outputrec', 'mytestPath' )
    assertEqualsImproved( self.mar.outputRecPath, 'mytestPath', self )
    self.assertFalse( self.mar._errorDict )

  def test_setoutputdst( self ):
    self.mar.setOutputDstFile( 'my/file.outputdst', 'mytestPath' )
    assertEqualsImproved( self.mar.outputDstPath, 'mytestPath', self )
    self.assertFalse( self.mar._errorDict )

  def test_setproclist( self ):
    self.mar.setProcessorsToUse( [ 'proc1', 'proc2' ] )
    self.assertFalse( self.mar._errorDict )

  def test_setexcludeproclist( self ):
    self.mar.setProcessorsToExclude( [ 'proc1', 'proc2' ] )
    self.assertFalse( self.mar._errorDict )

  def test_userjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.mar._userjobmodules( module_mock ), self )

  def test_prodjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.mar._prodjobmodules( module_mock ), self )

  def test_prodjobmodules_outputpath( self ):
    module_mock = Mock()
    self.mar.outputPath = 'aef'
    assertDiracSucceeds( self.mar._prodjobmodules( module_mock ), self )
    self.assertIn( { 'OutputFile' : '@{OutputFile}', 'outputPath' : '@{OutputPath}',
                     'outputDataSE' : '@{OutputSE}'}, self.mar._listofoutput )

  def test_userjobmodules_fails( self ):
    with patch('%s._setUserJobFinalization' % MODULE_NAME, new=Mock(return_value=S_OK('something'))),\
         patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_test_err'))):
      assertDiracFailsWith( self.mar._userjobmodules( None ),
                            'userjobmodules failed', self )

  def test_prodjobmodules_fails( self ):
    with patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_OK('something'))), \
         patch('%s._setOutputComputeDataList' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_other_test_err'))):
      assertDiracFailsWith( self.mar._prodjobmodules( None ),
                            'prodjobmodules failed', self )

  def test_checkconsistency( self ):
    self.mar.version = '13'
    self.mar.steeringFile = '/mysteer/file.stdhep'
    self.mar.gearFile = None
    self.mar._jobtype = 'notUser'
    self.mar.outputFile = None
    with patch('os.path.exists', new=Mock(return_value=True)), \
         patch.object(inspect.getmodule(Marlin), 'checkXMLValidity', new=Mock(return_value=S_OK())):
      assertDiracSucceeds( self.mar._checkConsistency(), self )
      self.assertIn( { 'outputFile' : '@{outputREC}', 'outputPath' : '@{outputPathREC}',
                       'outputDataSE' : '@{OutputSE}' }, self.mar._listofoutput )
      self.assertIn( { 'outputFile' : '@{outputDST}', 'outputPath' : '@{outputPathDST}',
                       'outputDataSE' : '@{OutputSE}' }, self.mar._listofoutput )
      for keyword in [ 'detectorType', 'marlin_gearfile', 'marlin_steeringfile' ]:
        self.assertIn( keyword, self.mar.prodparameters )
      assertEqualsImproved( self.mar.gearFile, None, self )

  def test_checkconsistency_noversion( self ):
    self.mar.version = None
    assertDiracFailsWith( self.mar._checkConsistency(), 'version not set', self )

  def test_checkconsistency_invalidxml( self ):
    self.mar.version = '13'
    self.mar.steeringFile = '/mysteer/file.stdhep'
    with patch('os.path.exists', new=Mock(return_value=True)), \
         patch.object(inspect.getmodule(Marlin), 'checkXMLValidity', new=Mock(return_value=S_ERROR('mytesterrxml'))):
      assertDiracFailsWith( self.mar._checkConsistency(), 'supplied steering file cannot be read with xml', self)

  def test_checkconsistency_othercase( self ):
    self.mar.version = '13'
    self.mar.steeringFile = '/mysteer/file.stdhep'
    self.mar.gearFile = 'myGearOutput.mock'
    self.mar._jobtype = 'notUser'
    self.mar.outputFile = 'myoutput.test'
    with patch('os.path.exists', new=Mock(return_value=False)):
      assertDiracSucceeds( self.mar._checkConsistency(), self )
      self.assertNotIn( { 'outputFile' : '@{outputREC}', 'outputPath' : '@{outputPathREC}',
                          'outputDataSE' : '@{OutputSE}' }, self.mar._listofoutput )
      self.assertNotIn( { 'outputFile' : '@{outputDST}', 'outputPath' : '@{outputPathDST}',
                          'outputDataSE' : '@{OutputSE}' }, self.mar._listofoutput )
      for keyword in [ 'detectorType', 'marlin_gearfile', 'marlin_steeringfile' ]:
        self.assertIn( keyword, self.mar.prodparameters )
      assertEqualsImproved( self.mar.gearFile, 'myGearOutput.mock', self )

  def test_checkconsistency_lastcase( self ):
    self.mar.version = '13'
    self.mar.steeringFile = None
    self.mar.gearFile = 'myGearOutput.mock'
    self.mar._jobtype = 'User'
    self.mar.outputFile = 'myoutput.test'
    assertDiracSucceeds( self.mar._checkConsistency(), self )
    self.assertNotIn( { 'outputFile' : '@{outputREC}', 'outputPath' : '@{outputPathREC}',
                        'outputDataSE' : '@{OutputSE}' }, self.mar._listofoutput )
    self.assertNotIn( { 'outputFile' : '@{outputDST}', 'outputPath' : '@{outputPathDST}',
                        'outputDataSE' : '@{OutputSE}' }, self.mar._listofoutput )
    for keyword in [ 'detectorType', 'marlin_gearfile', 'marlin_steeringfile' ]:
      self.assertNotIn( keyword, self.mar.prodparameters )
    assertEqualsImproved( self.mar.gearFile, 'myGearOutput.mock', self )


















