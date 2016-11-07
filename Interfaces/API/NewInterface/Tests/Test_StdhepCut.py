#!/usr/local/env python
"""
Test StdhepCut module

"""

import unittest
from mock import patch, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Interfaces.API.NewInterface.Applications import StdhepCut
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertDiracSucceeds

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.Applications.StdhepCut'

#pylint: disable=protected-access
class StdhepCutTestCase( unittest.TestCase ):
  """ Base class for the StdhepCut test cases
  """
  def setUp(self):
    """set up the objects"""
    self.shc = StdhepCut( {} )

  def test_setnbevts( self ):
    self.assertFalse( self.shc._errorDict )
    self.shc.setNbEvtsPerFile( 92814 )
    self.assertFalse( self.shc._errorDict )
    assertEqualsImproved( self.shc.numberOfEventsPerFile, 92814, self )

  def test_setnbevts_fails( self ):
    self.assertFalse( self.shc._errorDict )
    self.shc.setNbEvtsPerFile( [ 'auegf' ] )
    self.assertIn( '_checkArgs', self.shc._errorDict )

  def test_userjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.shc._userjobmodules( module_mock ), self )

  def test_prodjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.shc._prodjobmodules( module_mock ), self )

  def test_userjobmodules_fails( self ):
    with patch('%s._setUserJobFinalization' % MODULE_NAME, new=Mock(return_value=S_OK('something'))),\
         patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_test_err'))):
      assertDiracFailsWith( self.shc._userjobmodules( None ),
                            'userjobmodules failed', self )

  def test_prodjobmodules_fails( self ):
    with patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_OK('something'))), \
         patch('%s._setOutputComputeDataList' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_other_test_err'))):
      assertDiracFailsWith( self.shc._prodjobmodules( None ),
                            'prodjobmodules failed', self )

  def test_resolvelinkedparams( self ):
    step_mock = Mock()
    input_mock = Mock()
    input_mock.getType.return_value = { 'abc' : False }
    self.shc._linkedidx = 3
    self.shc._jobsteps = [ None, None, None, input_mock ]
    assertDiracSucceeds( self.shc._resolveLinkedStepParameters( step_mock ), self )
    step_mock.setLink.assert_called_once_with( 'InputFile', { 'abc' : False }, 'OutputFile' )

  def test_resolvelinkedparams_noinputstep( self ):
    self.shc._linkedidx = None
    self.shc._inputappstep = []
    assertDiracSucceeds( self.shc._resolveLinkedStepParameters( None ), self )

  def test_checkfinalconsistency_noevents( self ):
    self.shc.numberOfEvents = 0
    assertDiracFailsWith( self.shc._checkFinalConsistency(), 'specify the number of events', self )

  def test_checkfinalconsistency_toofewevts( self ):
    self.shc.numberOfEvents = 418
    self.shc.selectionEfficiency = 4
    self.shc.maxNumberOfEvents = 1000000
    assertDiracFailsWith( self.shc._checkFinalConsistency(), "don't generate enough events", self )

  def test_checkfinalconsistency( self ):
    self.shc.numberOfEvents = 418
    self.shc.selectionEfficiency = 4
    self.shc.maxNumberOfEvents = 10
    assertDiracSucceeds( self.shc._checkFinalConsistency(), self )

  def test_checkconsistency( self ):
    self.shc.steeringFile = 'abc.xml'
    self.shc.inlineCuts = None
    self.shc.maxNumberOfEvents = 14
    self.shc.selectionEfficiency = 183
    self.shc._jobtype = 'User'
    assertDiracSucceeds( self.shc._checkConsistency(), self )
    self.assertNotIn( { 'outputFile' : '@{OutputFile}', 'outputPath' : '@{OutputPath}',
                        'outputDataSE' : '@{OutputSE}' }, self.shc._listofoutput )
    self.assertNotIn( 'nbevts', self.shc.prodparameters )
    self.assertNotIn( 'Process', self.shc.prodparameters )

  def test_checkconsistency_nocuts( self ):
    self.shc.steeringFile = None
    self.shc.inlineCuts = None
    assertDiracFailsWith( self.shc._checkConsistency(), 'cuts not specified', self )

  def test_checkconsistency_nomaxnbevts( self ):
    self.shc.steeringFile = 'abc.xml'
    self.shc.inlineCuts = 'cuts.pdf'
    self.shc.maxNumberOfEvents = None
    assertDiracFailsWith( self.shc._checkConsistency(), 'did not specify how many events', self )

  def test_checkconsistency_noefficiency( self ):
    self.shc.steeringFile = 'abc.xml'
    self.shc.inlineCuts = 'cuts.pdf'
    self.shc.maxNumberOfEvents = 14
    self.shc.selectionEfficiency = None
    assertDiracFailsWith( self.shc._checkConsistency(),
                          'need to know the selection efficiency of your cuts', self )

  def test_checkconsistency_nouserjob( self ):
    self.shc.steeringFile = 'abc.xml'
    self.shc.inlineCuts = None
    self.shc.maxNumberOfEvents = 14
    self.shc.selectionEfficiency = 38
    self.shc._jobtype = 'notUser'
    assertDiracSucceeds( self.shc._checkConsistency(), self )
    self.assertIn( { 'outputFile' : '@{OutputFile}', 'outputPath' : '@{OutputPath}',
                     'outputDataSE' : '@{OutputSE}' }, self.shc._listofoutput )
    self.assertIn( 'nbevts_kept', self.shc.prodparameters )
    self.assertIn( 'cut_file', self.shc.prodparameters )
