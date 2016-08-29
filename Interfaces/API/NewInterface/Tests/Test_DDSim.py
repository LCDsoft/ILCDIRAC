#!/usr/local/env python
"""
Test DDSim module

"""

import inspect
import unittest
from mock import patch, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Interfaces.API.NewInterface.Applications import DDSim
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertDiracSucceeds

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.Applications.DDSim'

#pylint: disable=protected-access
class DDSimTestCase( unittest.TestCase ):
  """ Base class for the DDSim test cases
  """
  def setUp(self):
    """set up the objects"""
    self.dds = DDSim( {} )

  def test_setrandomseed( self ):
    self.assertFalse( self.dds._errorDict )
    self.dds.setRandomSeed( 89421 )
    self.assertFalse( self.dds._errorDict )
    assertEqualsImproved( self.dds.randomSeed, 89421, self )

  def test_setrandomseed_fails( self ):
    self.assertFalse( self.dds._errorDict )
    self.dds.setRandomSeed( [ 'abc' ] )
    self.assertIn( '_checkArgs', self.dds._errorDict )

  def test_setstartfrom( self ):
    self.assertFalse( self.dds._errorDict )
    self.dds.setStartFrom( 89421 )
    self.assertFalse( self.dds._errorDict )
    assertEqualsImproved( self.dds.startFrom, 89421, self )

  def test_setstartfrom_fails( self ):
    self.assertFalse( self.dds._errorDict )
    self.dds.setStartFrom( 'adgiuj' )
    self.assertIn( '_checkArgs', self.dds._errorDict )

  def test_resolvelinkedparams( self ):
    step_mock = Mock()
    input_mock = Mock()
    input_mock.getType.return_value = { 'abc' : False }
    self.dds._linkedidx = 3
    self.dds._jobsteps = [ None, None, None, input_mock ]
    assertDiracSucceeds( self.dds._resolveLinkedStepParameters( step_mock ), self )
    step_mock.setLink.assert_called_once_with( 'InputFile', { 'abc' : False }, 'OutputFile' )

  def test_resolvelinkedparams_noinputstep( self ):
    self.dds._linkedidx = None
    self.dds._inputappstep = []
    assertDiracSucceeds( self.dds._resolveLinkedStepParameters( None ), self )

  def test_checkworkflow_app_missing( self ):
    self.dds._inputapp = [ 'some_depdency', 'unavailable_dependency_fail_on_this' ]
    self.dds._jobapps = [ 'myjobapp_1', 'some_dependency' ]
    assertDiracFailsWith( self.dds._checkWorkflowConsistency(), 'job order not correct', self )

  def test_checkworkflow_empty( self ):
    self.dds._inputapp = []
    self.dds._jobapps = []
    assertDiracSucceeds( self.dds._checkWorkflowConsistency(), self )

  def test_checkworkflow_success( self ):
    self.dds._inputapp = [ 'some_dependency', 'other_dependencies', 'many_more' ]
    self.dds._jobapps = [ 'ignore_me', 'many_more', 'some_dependency', 'other_dependencies' ]
    assertDiracSucceeds( self.dds._checkWorkflowConsistency(), self )

  def test_userjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.dds._userjobmodules( module_mock ), self )

  def test_prodjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.dds._prodjobmodules( module_mock ), self )

  def test_userjobmodules_fails( self ):
    with patch('%s._setUserJobFinalization' % MODULE_NAME, new=Mock(return_value=S_OK('something'))),\
         patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_test_err'))):
      assertDiracFailsWith( self.dds._userjobmodules( None ),
                            'userjobmodules failed', self )

  def test_prodjobmodules_fails( self ):
    with patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_OK('something'))), \
         patch('%s._setOutputComputeDataList' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_other_test_err'))):
      assertDiracFailsWith( self.dds._prodjobmodules( None ),
                            'prodjobmodules failed', self )

  def test_checkconsistency( self ):
    self.dds.version = '134'
    self.dds.detectorModel = 'mymodel.det'
    self.dds.outputFile = 'myoutput.file'
    self.dds._jobtype = 'User'
    assertDiracSucceeds( self.dds._checkConsistency( Mock() ), self )
    self.assertNotIn( { 'outputFile' : '@{OutputFile}', 'outputPath' : '@{OutputPath}',
                        'outputDataSE' : '@{OutputSE}' }, self.dds._listofoutput )
    self.assertNotIn( 'nbevts', self.dds.prodparameters )
    self.assertNotIn( 'Process', self.dds.prodparameters )

  def test_checkconsistency_noversion( self ):
    self.dds.version = None
    assertDiracFailsWith( self.dds._checkConsistency( Mock() ), 'no version found', self )

  def test_checkconsistency_existsfails( self ):
    self.dds.version = '134'
    self.dds.steeringFile = 'mysteer.file'
    with patch('os.path.exists', new=Mock(return_value=False)), \
         patch.object(inspect.getmodule(DDSim), 'Exists', new=Mock(return_value=S_ERROR('testerr_exists_mock'))):
      assertDiracFailsWith( self.dds._checkConsistency( Mock() ), 'testerr_exists_mock', self )

  def test_checkconsistency_userjob( self ):
    self.dds.version = '134'
    self.dds.steeringFile = 'mysteer.file'
    self.dds._jobtype = 'notUser'
    self.dds.detectorModel = 'myDetectorv200'
    with patch('os.path.exists', new=Mock(return_value=True)), \
         patch.object(inspect.getmodule(DDSim), 'Exists', new=Mock(return_value=S_ERROR('testerr_exists_mock'))):
      assertDiracSucceeds( self.dds._checkConsistency( Mock() ), self )
      self.assertIn( { 'outputFile' : '@{OutputFile}', 'outputPath' : '@{OutputPath}',
                       'outputDataSE' : '@{OutputSE}' }, self.dds._listofoutput )
      for keyword in [ 'detectorType', 'slic_detectormodel' ]:
        self.assertIn( keyword, self.dds.prodparameters )

  def test_checkconsistency_userjob_notdetmodel( self ):
    self.dds.version = '134'
    self.dds.steeringFile = 'mysteer.file'
    self.dds._jobtype = 'notUser'
    self.dds.detectorModel = True
    self.dds.setStartFrom( 148 )
    with patch('os.path.exists', new=Mock(return_value=False)), \
         patch.object(inspect.getmodule(DDSim), 'Exists', new=Mock(return_value=S_OK())):
      assertDiracSucceeds( self.dds._checkConsistency( Mock() ), self )
      self.assertIn( { 'outputFile' : '@{OutputFile}', 'outputPath' : '@{OutputPath}',
                       'outputDataSE' : '@{OutputSE}' }, self.dds._listofoutput )
      for keyword in [ 'detectorType', 'slic_detectormodel' ]:
        self.assertIn( keyword, self.dds.prodparameters )





