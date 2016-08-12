#!/usr/local/env python
"""
Test PostGenSelection module

"""

import unittest
from mock import patch, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Interfaces.API.NewInterface.Applications import PostGenSelection
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertDiracSucceeds

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.Applications.PostGenSelection'

#pylint: disable=protected-access
class PostGenSelectionTestCase( unittest.TestCase ):
  """ Base class for the PostGenSelection test cases
  """
  def setUp(self):
    """set up the objects"""
    self.pgs = PostGenSelection( {} )

  def test_setnbevts( self ):
    self.assertFalse( self.pgs._errorDict )
    self.pgs.setNbEvtsToKeep( 844 )
    assertEqualsImproved( self.pgs.numberOfEventsToKeep, 844, self )
    self.assertFalse( self.pgs._errorDict )

  def test_setnbevts_wrongtype( self ):
    self.assertFalse( self.pgs._errorDict )
    self.pgs.setNbEvtsToKeep( { 'something' : True } )
    assertEqualsImproved( len( self.pgs._errorDict[ '_checkArgs' ] ), 1, self )

  def test_userjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.pgs._userjobmodules( module_mock ), self )

  def test_prodjobmodules( self ):
    assertDiracSucceeds( self.pgs._prodjobmodules( Mock() ), self )

  def test_prodjobmodules_fails( self ):
    with patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_OK('something'))), \
         patch('%s._setOutputComputeDataList' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_other_test_err'))):
      assertDiracFailsWith( self.pgs._prodjobmodules( None ),
                            'prodjobmodules failed', self )

  def test_userjobmodules_fails( self ):
    with patch('%s._setUserJobFinalization' % MODULE_NAME, new=Mock(return_value=S_OK('something'))),\
         patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_test_err'))):
      assertDiracFailsWith( self.pgs._userjobmodules( None ),
                            'userjobmodules failed', self )

  def test_checkworkflow_app_missing( self ):
    self.pgs._inputapp = [ 'some_depdency', 'unavailable_dependency_fail_on_this' ]
    self.pgs._jobapps = [ 'myjobapp_1', 'some_dependency' ]
    assertDiracFailsWith( self.pgs._checkWorkflowConsistency(), 'job order not correct', self )

  def test_checkworkflow_empty( self ):
    self.pgs._inputapp = []
    self.pgs._jobapps = []
    assertDiracSucceeds( self.pgs._checkWorkflowConsistency(), self )

  def test_checkworkflow_success( self ):
    self.pgs._inputapp = [ 'some_dependency', 'other_dependencies', 'many_more' ]
    self.pgs._jobapps = [ 'ignore_me', 'many_more', 'some_dependency', 'other_dependencies' ]
    assertDiracSucceeds( self.pgs._checkWorkflowConsistency(), self )

  def test_resolvelinkedparams( self ):
    step_mock = Mock()
    input_mock = Mock()
    input_mock.getType.return_value = { 'abc' : False }
    self.pgs._linkedidx = 3
    self.pgs._jobsteps = [ None, None, None, input_mock ]
    assertDiracSucceeds( self.pgs._resolveLinkedStepParameters( step_mock ), self )
    step_mock.setLink.assert_called_once_with( 'InputFile', { 'abc' : False }, 'OutputFile' )

  def test_resolvelinkedparams_noinputstep( self ):
    self.pgs._linkedidx = None
    self.pgs._inputappstep = []
    assertDiracSucceeds( self.pgs._resolveLinkedStepParameters( None ), self )

  def test_checkconsistency( self ):
    self.pgs.setNbEvtsToKeep( 1847 )
    assertDiracSucceeds( self.pgs._checkConsistency(), self )
    self.assertFalse( self.pgs._errorDict )

  def test_checkconsistency_nonbevts( self ):
    assertDiracFailsWith( self.pgs._checkConsistency(), 'Number of events to keep was not given', self )







