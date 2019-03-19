#!/usr/local/env python
"""
Test the ILCDIRAC job module

"""

from __future__ import print_function
import types
import unittest

from mock import patch, MagicMock as Mock
from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Interfaces.API.NewInterface.Job import Job
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertInImproved, assertDiracFailsWith, \
  assertDiracSucceeds, assertDiracSucceedsWith_equals

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.Job'

#pylint: disable=protected-access,too-many-public-methods
class JobTestCase( unittest.TestCase ):
  """ Test the ILCDIRAC Job class
  """
  def setUp(self):
    """set up the objects"""
    import DIRAC
    objectLoaderInst = Mock(name="ObjectLoader")
    objectLoaderInst.loadObject.return_value = S_OK(lambda: S_OK(['x86_64-slc5-gcc43-opt']))
    olMock = Mock(name="ObjectLoaderModule", return_value=objectLoaderInst)
    with patch.object(DIRAC.Interfaces.API.Job, 'ObjectLoader', new=olMock):
      self.job = Job('')
    self.job.check = True

  def test_setsysconf(self):
    self.job.getDIRACPlatforms = Mock(return_value=S_OK(['myTestPlatform']))
    self.job.setPlatform('myTestPlatform')
    self.assertFalse(self.job.errorDict)

  def test_unimplemented_methods( self ):
    self.job.setInputData('')
    self.job.setInputSandbox('')
    self.job.setOutputData('')
    self.job.setOutputSandbox('')
    self.job.submit()
    assertEqualsImproved( len(self.job.errorDict), 5, self )
    expected_failures = [ 'setInputData', 'setInputSandbox', 'setOutputData', 'setOutputSandbox', 'submit' ]
    for method in expected_failures:
      assertInImproved( method, self.job.errorDict.keys(), self )

  def test_ignoreapperrors( self ):
    assertDiracSucceeds( self.job.setIgnoreApplicationErrors(), self )
    assertEqualsImproved( self.job.workflow.parameters[-1].name, 'IgnoreAppError', self )

  def test_checkparams( self ):
    app_mock = Mock()
    app_mock.appname = 'myCoolTestApp'
    app_mock._analyseJob.return_value = S_OK('')
    app_mock._checkWorkflowConsistency.return_value = S_OK('')
    app_mock._addParametersToStep.return_value = S_OK('')
    app_mock._setStepParametersValues.return_value = S_OK('')
    app_mock._resolveLinkedStepParameters.return_value = S_OK('')
    dirac_mock = Mock()
    dirac_mock.checkparams.return_value = S_OK('dirac_test_retval')
    self.job.applicationlist = [ app_mock ]
    assertDiracSucceedsWith_equals( self.job.checkparams( dirac_mock ), 'dirac_test_retval', self )
    assertEqualsImproved( self.job.workflow.parameters[-1].name, 'TotalSteps', self )

  def test_checkparams_nodirac( self ):
    assertDiracFailsWith( self.job.checkparams(), 'missing dirac', self )

  def test_checkparams_analyse_fails( self ):
    app_mock = Mock()
    app_mock.appname = 'myCoolTestApp'
    app_mock._analyseJob.return_value = S_ERROR('analyse_test_Err')
    dirac_mock = Mock()
    dirac_mock.checkparams.return_value = S_OK('dirac_test_retval')
    self.job.applicationlist = [ app_mock ]
    assertDiracFailsWith( self.job.checkparams( dirac_mock ), 'analyse_test_err', self )

  def test_checkparams_consistencycheck_fails( self ):
    app_mock = Mock()
    app_mock.appname = 'myCoolTestApp'
    app_mock._analyseJob.return_value = S_OK('')
    app_mock._checkWorkflowConsistency.return_value = S_ERROR('consistency_check_fails')
    dirac_mock = Mock()
    dirac_mock.checkparams.return_value = S_OK('dirac_test_retval')
    self.job.applicationlist = [ app_mock ]
    assertDiracFailsWith( self.job.checkparams( dirac_mock ), 'failed to check its consistency', self )

  def test_checkparams_jobspecificmodules_fails( self ):
    app_mock = Mock()
    app_mock.appname = 'myCoolTestApp'
    app_mock._analyseJob.return_value = S_OK('')
    app_mock._checkWorkflowConsistency.return_value = S_OK('')
    app_mock._addParametersToStep.side_effect = IOError('dont call me')
    app_mock._userjobmodules.return_value = S_ERROR('test_err')
    dirac_mock = Mock()
    dirac_mock.checkparams.return_value = S_OK('dirac_test_retval')
    self.job.applicationlist = [ app_mock ]
    assertDiracFailsWith( self.job.checkparams( dirac_mock ), 'failed to add module', self )

  def test_checkparams_addparam_fails( self ):
    app_mock = Mock()
    app_mock.appname = 'myCoolTestApp'
    app_mock._analyseJob.return_value = S_OK('')
    app_mock._checkWorkflowConsistency.return_value = S_OK('')
    app_mock._addParametersToStep.return_value = S_ERROR('adding of parameters failed')
    dirac_mock = Mock()
    dirac_mock.checkparams.return_value = S_OK('dirac_test_retval')
    self.job.applicationlist = [ app_mock ]
    assertDiracFailsWith( self.job.checkparams( dirac_mock ), 'failed to add parameters', self )

  def test_checkparams_resolveparams_fails( self ):
    app_mock = Mock()
    app_mock.appname = 'myCoolTestApp'
    app_mock._analyseJob.return_value = S_OK('')
    app_mock._checkWorkflowConsistency.return_value = S_OK('')
    app_mock._addParametersToStep.return_value = S_OK('')
    app_mock._setStepParametersValues.return_value = S_ERROR('resolving_failed_testerr')
    dirac_mock = Mock()
    dirac_mock.checkparams.return_value = S_OK('dirac_test_retval')
    self.job.applicationlist = [ app_mock ]
    assertDiracFailsWith( self.job.checkparams( dirac_mock ), 'failed to resolve parameters value', self )

  def test_checkparams_resolvelinks_fails( self ):
    app_mock = Mock()
    app_mock.appname = 'myCoolTestApp'
    app_mock._analyseJob.return_value = S_OK('')
    app_mock._checkWorkflowConsistency.return_value = S_OK('')
    app_mock._addParametersToStep.return_value = S_OK('')
    app_mock._setStepParametersValues.return_value = S_OK('')
    app_mock._resolveLinkedStepParameters.return_value = S_ERROR('resolve_links_failed')
    dirac_mock = Mock()
    dirac_mock.checkparams.return_value = S_OK('dirac_test_retval')
    self.job.applicationlist = [ app_mock ]
    assertDiracFailsWith( self.job.checkparams( dirac_mock ), 'failed to resolve linked parameters', self )

  def test_askuser( self ):
    with patch('%s.promptUser' % MODULE_NAME, new=Mock(return_value=S_OK(''))):
      self.job.applicationlist = [ Mock() ]
      assertDiracSucceeds( self.job._askUser(), self )

  def test_askuser_nocheck( self ):
    with patch('%s.promptUser' % MODULE_NAME, new=Mock(return_value=S_ERROR(''))):
      self.job.check = False
      assertDiracSucceeds( self.job._askUser(), self )

  def test_askuser_novalidation( self ):
    self.job.applicationlist = []
    with patch('%s.promptUser' % MODULE_NAME, new=Mock(return_value=S_ERROR(''))):
      assertDiracFailsWith( self.job._askUser(), 'user did not validate', self )

  def test_askuser_validation_denied( self ):
    self.job.applicationlist = [ Mock(), Mock() ]
    with patch('%s.promptUser' % MODULE_NAME, new=Mock(return_value=S_OK('n'))):
      assertDiracFailsWith( self.job._askUser(), 'user did not validate', self )

  def test_append( self ):
    app_mock = Mock()
    app_mock._analyseJob.return_value = S_OK('')
    app_mock._checkConsistency.return_value = S_OK('')
    app_mock._checkFinalConsistency.return_value = S_OK('')
    assertDiracSucceeds( self.job.append( app_mock ), self )

  def test_append_analysis_fails( self ):
    app_mock = Mock()
    app_mock._analyseJob.return_value = S_ERROR('analysis failed, sorry. this is a test')
    assertDiracFailsWith( self.job.append( app_mock ), 'analysis failed, sorry', self )

  def test_append_checkconsistency_fails( self ):
    app_mock = Mock()
    app_mock._analyseJob.return_value = S_OK('')
    app_mock._checkConsistency.return_value = S_ERROR('consistency check test failed')
    assertDiracFailsWith( self.job.append( app_mock ), 'failed to check its consistency', self )

  def test_append_finalconsistency_fails( self ):
    app_mock = Mock()
    app_mock._analyseJob.return_value = S_OK('')
    app_mock._checkConsistency.return_value = S_OK('')
    app_mock._checkFinalConsistency.return_value = S_ERROR('final consistency invalid')
    assertDiracFailsWith( self.job.append( app_mock ), 'failed to check its consistency', self )

  def test_append_jobspecificenergy_wrong( self ):
    self.job.energy = 2451
    app_mock = Mock()
    app_mock.energy = 214
    app_mock._analyseJob.return_value = S_OK('')
    app_mock._checkConsistency.return_value = S_OK('')
    app_mock._checkFinalConsistency.side_effect = IOError('dont call me')
    assertDiracFailsWith( self.job.append( app_mock ), 'failed job specific checks', self )

  def test_append_other_case( self ):
    app_mock = Mock()
    app_mock.numberOfEvents = 0
    app_mock.appname = ''
    app_mock.inputSB = [ 'inputsandbox1TestMe', 'other_sandbox', '' ]
    self.job.inputsandbox = [ 'other_sandbox' ]
    app_mock._analyseJob.return_value = S_OK('')
    app_mock._checkConsistency.return_value = S_OK('')
    app_mock._checkFinalConsistency.return_value = S_OK('')
    assertDiracSucceeds( self.job.append( app_mock ), self )
    assertEqualsImproved( self.job.inputsandbox, [ 'other_sandbox', 'inputsandbox1TestMe', '' ], self )

  def test_addsoftware( self ):
    param_mock = Mock()
    param_mock.getValue.return_value = 'myapp;myappnamtest.testv3/2'
    with patch.object(self.job.workflow, 'findParameter', new=Mock(return_value=param_mock)):
      self.job._addSoftware( 'myAppNamTest', 'testv3/2' )
      assertEqualsImproved( self.job.workflow.parameters[-1].name, 'SoftwarePackages', self )

  def test_addsoftware_addApp( self ):
    param_mock = Mock()
    param_mock.getValue.return_value = 'notMyApp'
    with patch.object(self.job.workflow, 'findParameter', new=Mock(return_value=param_mock)):
      self.job._addSoftware( 'myAppNamTest', 'testv3/2' )
      assertEqualsImproved( self.job.workflow.parameters[-1].name, 'SoftwarePackages', self )

#pylint: disable=protected-access
class InternalJobTestCase( unittest.TestCase ):
  """ Test the methods of the Job class that require a IntrospectJob instance
  """
  def setUp(self):
    """set up the objects"""
    import DIRAC
    with patch.object(DIRAC.ConfigurationSystem.Client.Helpers.Resources, 'getDIRACPlatforms', return_value=S_OK(['x86_64-slc5-gcc43-opt'])):
      self.job = IntrospectJob( '' )
      self.job.check = True

  def test_checkargs_1( self ):
    self.job.indirection_for_checkArgs( 246, types.IntType )
    self.assertFalse( self.job.errorDict )

  def test_checkargs_2( self ):
    self.job.indirection_for_checkArgs( 'bla', types.IntType )
    self.assertTrue( self.job.errorDict )

  def test_checkargs_3( self ):
    self.job.indirection_for_checkArgs( { True : 'blabla' }, types.DictType )
    self.assertFalse( self.job.errorDict )

  def test_checkargs_4( self ):
    self.job.indirection_for_checkArgs( False, types.DictType )
    self.assertTrue( self.job.errorDict )

  def test_checkargs_5( self ):
    self.job.indirection_for_checkArgs( True, types.BooleanType )
    self.assertFalse( self.job.errorDict )

  def test_checkargs_6( self ):
    self.job.indirection_for_checkArgs( {}, types.BooleanType )
    self.assertTrue( self.job.errorDict )

  def test_checkargs_7( self ):
    self.job.indirection_for_checkArgs( [ True, 129, '' ], types.ListType )
    self.assertFalse( self.job.errorDict )

  def test_checkargs_8( self ):
    self.job.indirection_for_checkArgs( 246, types.ListType )
    self.assertTrue( self.job.errorDict )

  def test_checkargs_9( self ):
    self.job.indirection_2_for_checkArgs( 1, types.IntType )
    self.assertTrue( self.job.errorDict )

  def test_getargsdict( self ):
    my_arg_dict = self.job.indirection_for_getargsdict( arg1=1, arg2=True, arg3='mystring' )
    assertEqualsImproved( my_arg_dict, { 'arg1' : 1, 'arg2' : True, 'arg3' : 'mystring' }, self )

class IntrospectJob( Job ):
  """ Used to easily test the introspective methods (e.g. _checkArgs)
  """

  def __init__( self, script = None ):
    import DIRAC
    objectLoaderInst = Mock(name="ObjectLoader")
    objectLoaderInst.loadObject.return_value = S_OK(lambda: S_OK(['x86_64-slc5-gcc43-opt']))
    olMock = Mock(name="ObjectLoaderModule", return_value=objectLoaderInst)
    with patch.object(DIRAC.Interfaces.API.Job, 'ObjectLoader', new=olMock):
      super(IntrospectJob, self).__init__(script)
    self.getDIRACPlatforms = Mock(return_value=S_OK(['x86_64-slc5-gcc43-opt']))

  def indirection_for_checkArgs( self, arg_to_check, argtype ):
    """ Method that uses the _checkArgs method so it can be tested.
    """
    self._checkArgs( { 'arg_to_check' : argtype } )
    print(arg_to_check)

  def indirection_2_for_checkArgs( self, arg_to_check, argtype ):
    """ Method that uses the _checkArgs method so it can be tested.
    """
    # Intentional 'typo'
    self._checkArgs( { 'arg_t_check' : argtype } )
    print(arg_to_check)

  def indirection_for_getargsdict( self, arg1, arg2, arg3 ):
    """ Method that uses the getArgsDict method so it can be tested.
    """
    print('%s %s %s' % (arg1, arg2, arg3))
    return self._getArgsDict()











