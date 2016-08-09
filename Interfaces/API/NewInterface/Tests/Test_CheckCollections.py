#!/usr/local/env python
"""
Test CheckCollections module

"""

import unittest
from mock import patch, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Interfaces.API.NewInterface.Applications import CheckCollections
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertDiracSucceeds

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.Applications.CheckCollections'

#pylint: disable=protected-access
class CheckCollectionsTestCase( unittest.TestCase ):
  """ Base class for the CheckCollations test cases
  """
  def setUp(self):
    """set up the objects"""
    self.chcol = CheckCollections( {} )

  def test_setcollection( self ):
    self.assertFalse( self.chcol._errorDict )
    self.chcol.setCollections( [ 'mycollection', True, { 'something' : 124 } ] )
    self.assertFalse( self.chcol._errorDict )
    assertEqualsImproved( self.chcol.collections, [ 'mycollection', True, { 'something' : 124 } ], self )

  def test_setcollection_invalid( self ):
    self.assertFalse( self.chcol._errorDict )
    self.chcol.setCollections( { 'blabla', True } )
    assertEqualsImproved( len( self.chcol._errorDict[ '_checkArgs'] ), 1, self )

  def test_applicationModule( self ):
    result = self.chcol._applicationModule()
    self.assertIsNotNone( result )
    self.chcol._applicationModuleValues( result )

  def test_userjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.chcol._userjobmodules( module_mock ), self )

  def test_prodjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.chcol._prodjobmodules( module_mock ), self )

  def test_userjobmodules_fails( self ):
    with patch('%s._setUserJobFinalization' % MODULE_NAME, new=Mock(return_value=S_OK('something'))),\
         patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_test_err'))):
      assertDiracFailsWith( self.chcol._userjobmodules( None ),
                            'userjobmodules failed', self )

  def test_prodjobmodules_fails( self ):
    with patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_OK('something'))), \
         patch('%s._setOutputComputeDataList' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_other_test_err'))):
      assertDiracFailsWith( self.chcol._prodjobmodules( None ),
                            'prodjobmodules failed', self )

  def test_resolvelinkedparams( self ):
    step_mock = Mock()
    input_mock = Mock()
    input_mock.getType.return_value = { 'abc' : False }
    self.chcol._linkedidx = 3
    self.chcol._jobsteps = [ None, None, None, input_mock ]
    assertDiracSucceeds( self.chcol._resolveLinkedStepParameters( step_mock ), self )
    step_mock.setLink.assert_called_once_with( 'InputFile', { 'abc' : False }, 'OutputFile' )

  def test_resolvelinkedparams_noinputstep( self ):
    self.chcol._linkedidx = None
    self.chcol._inputappstep = []
    assertDiracSucceeds( self.chcol._resolveLinkedStepParameters( None ), self )

  def test_checkconsistency( self ):
    self.chcol.setCollections( [ 'some_entry', False, 'add_me', {} ] )
    assertDiracSucceeds( self.chcol._checkConsistency(), self )

  def test_checkconsistency_nocollection( self ):
    assertDiracFailsWith( self.chcol._checkConsistency(), 'no collections to check', self )

  def test_checkconsistency_checkfails( self ):
    self.chcol.setCollections( [ 'some_entry', False, 'add_me', {} ] )
    self.chcol._inputapp = [ 'some_depdency', 'unavailable_dependency_fail_on_this' ]
    self.chcol._jobapps = [ 'myjobapp_1', 'some_dependency' ]
    assertDiracFailsWith( self.chcol._checkConsistency(), 'job order not correct', self )
