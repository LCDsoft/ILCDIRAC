#!/usr/local/env python
"""
Test the GenericApplication module

"""

import unittest

from mock import patch, MagicMock as Mock
from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Interfaces.API.NewInterface.Applications.GenericApplication import GenericApplication
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertDiracSucceeds, assertMockCalls

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.Applications.GenericApplication'

#pylint: disable=protected-access
class GenericAppTestCase( unittest.TestCase ):
  """ Test the GenericApplication class
  """

  def setUp(self):
    """set up the objects"""
    self.gapp = GenericApplication( {} )

  def test_setters( self ):
    self.assertFalse( self.gapp._errorDict )
    assertDiracSucceeds( self.gapp.setScript( 'lfn:/my/script.sh'), self )
    assertDiracSucceeds( self.gapp.setArguments( 'arg1 arg2 -t arg3 /h?' ), self )
    assertDiracSucceeds( self.gapp.setDependency( { 'mylibrary' : 'v1.0test', 'mokka' : 'v1002' } ), self )
    self.assertFalse( self.gapp._errorDict )
    assertEqualsImproved( ( self.gapp.script, self.gapp.inputSB, self.gapp.arguments, self.gapp.dependencies ),
                          ( 'lfn:/my/script.sh', [ 'lfn:/my/script.sh' ], 'arg1 arg2 -t arg3 /h?',
                            { 'mylibrary' : 'v1.0test', 'mokka' : 'v1002' } ), self )

  def test_setters_typeerrors( self ):
    self.assertFalse( self.gapp._errorDict )
    self.gapp.setScript( '' )
    self.gapp.setArguments( {} )
    self.gapp.setDependency( [] )
    assertEqualsImproved( len( self.gapp._errorDict['_checkArgs']), 2, self )

  def test_setscript( self ):
    self.assertFalse( self.gapp._errorDict )
    self.gapp.setScript( '/invalid/dir/myscript.sh' )
    self.assertFalse( self.gapp._errorDict )
    assertEqualsImproved( ( self.gapp.script, self.gapp.inputSB ), ( '/invalid/dir/myscript.sh', [] ), self )

  def test_applicationModule( self ):
    ret_mock = Mock()
    with patch.object(self.gapp, '_createModuleDefinition', new=Mock(return_value=ret_mock)) as mod_mock:
      assertEqualsImproved( self.gapp._applicationModule(), ret_mock, self )
      self.assertTrue( mod_mock.called )
      self.assertTrue( ret_mock.addParameter.called )

  def test_applicationmodulevalues( self ):
    mod_mock = Mock()
    self.gapp._applicationModuleValues( mod_mock )
    self.assertTrue( mod_mock.setValue.called )

  def test_userjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.gapp._userjobmodules( module_mock ), self )

  def test_prodjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.gapp._prodjobmodules( module_mock ), self )

  def test_userjobmodules_fails( self ):
    with patch('%s._setUserJobFinalization' % MODULE_NAME, new=Mock(return_value=S_OK('something'))),\
         patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_test_err'))):
      assertDiracFailsWith( self.gapp._userjobmodules( None ),
                            'userjobmodules failed', self )

  def test_prodjobmodules_fails( self ):
    with patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_OK('something'))), \
         patch('%s._setOutputComputeDataList' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_other_test_err'))):
      assertDiracFailsWith( self.gapp._prodjobmodules( None ),
                            'prodjobmodules failed', self )

  def test_addparams( self ):
    assertDiracSucceeds( self.gapp._addParametersToStep( Mock() ), self )

  def test_addparams_fails( self ):
    with patch.object(self.gapp, '_addBaseParameters', new=Mock(return_value=S_ERROR('bla'))):
      assertDiracFailsWith( self.gapp._addParametersToStep( Mock() ), 'failed to set base param', self )

  def test_setstepparams( self ):
    job_mock = Mock()
    self.gapp._job = job_mock
    self.gapp.dependencies = { 'mydependency' : 'v1', 'other_library' : 'v100testme' }
    self.gapp._setStepParametersValues( Mock() )
    assertMockCalls( job_mock._addSoftware, [ ( 'mydependency', 'v1' ), ( 'other_library', 'v100testme' ) ], self )

  def test_checkconsistency( self ):
    self.gapp.script = 'lfn:/totally/valid/path.sh'
    assertDiracSucceeds( self.gapp._checkConsistency(), self )

  def test_checkconsistency_noscript( self ):
    self.gapp.script = ''
    assertDiracFailsWith( self.gapp._checkConsistency(), 'script not def', self )

  def test_checkconsistency_scriptnotfound( self ):
    self.gapp.script = '/invalid/path/dontfindme.sh'
    assertDiracFailsWith( self.gapp._checkConsistency(), 'script is not an lfn and was not found', self )
