#!/usr/bin/env python
"""Test the InputDataResolution class"""

import sys
import unittest
from mock import patch, MagicMock as Mock

from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertDiracSucceeds, assertMockCalls
from DIRAC import S_OK, S_ERROR

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Core.Utilities.InputDataResolution'

#pylint: disable=too-many-public-methods
class TestInputDataResolution( unittest.TestCase ):
  """ Test the different methods of the class
  """

  def setUp( self ):
    self.ops_mock = Mock()
    self.mfac_mock = Mock()
    mocked_modules = { 'DIRAC.ConfigurationSystem.Client.Helpers.Operations' : self.ops_mock,
                       'DIRAC.Core.Utilities.ModuleFactory' : self.mfac_mock }
    self.module_patcher = patch.dict( sys.modules, mocked_modules )
    self.module_patcher.start()
    self.idr = None

  def tearDown( self ):
    self.module_patcher.stop()

  def test_execute_input_missing_fail( self ):
    module_mock = Mock()
    module_mock.execute.return_value = { 'OK' : True, 'Failed' : [ 'myReplicaSite1', 'other_failure' ],
                                         'Successful' : { 'WorkingSite' : True } }
    self.mfac_mock.ModuleFactory().getModule.return_value = S_OK( module_mock )
    from ILCDIRAC.Core.Utilities.InputDataResolution import InputDataResolution
    self.idr = InputDataResolution( { 'Job' : { 'InputDataPolicy' : [ '/myTestPolicy/some/path' ] },
                                      'Configuration': { 'SiteName' :'myTestSitename' } } )
    assertDiracFailsWith( self.idr.execute(), 'failed to access all of requested input data', self )
    self.mfac_mock.ModuleFactory().getModule.assert_called_once_with(
      '/myTestPolicy/some/path',
      { 'Job' : { 'InputDataPolicy' : [ '/myTestPolicy/some/path' ] },
        'Configuration' : { 'SiteName' : 'myTestSitename' } } )

  def test_execute_resolveinput_fails( self ):
    from ILCDIRAC.Core.Utilities.InputDataResolution import InputDataResolution
    self.idr = InputDataResolution( { 'Configuration': { 'SiteName' :'myTestSitename' } } )
    ops_mock = Mock()
    ops_mock.getOptionsDict.return_value = None
    self.idr.ops = ops_mock
    assertDiracFailsWith( self.idr.execute(), 'Could not resolve InputDataPolicy from /InputDataPolicy', self )
    ops_mock.getOptionsDict.assert_called_once_with( '/InputDataPolicy' )

  def test_execute_runmodule_fails( self ):
    self.mfac_mock.ModuleFactory().getModule.return_value = S_ERROR( 'module_test_mockerr' )
    from ILCDIRAC.Core.Utilities.InputDataResolution import InputDataResolution
    self.idr = InputDataResolution( { 'Configuration' : {},
                                      'Job' : { 'InputDataPolicy' : '/myTestPolicy/some/path' },
                                      'IgnoreMissing' : False } )
    with patch('%s.DIRAC.siteName' % MODULE_NAME, new=Mock(return_value='SiteNameTestdirac')):
      assertDiracFailsWith( self.idr.execute(), 'module_test_mockerr', self )

  def test_execute_resolveinput_none_successful( self ):
    module_mock = Mock()
    module_mock.execute.return_value = { 'OK' : True, 'Failed' : [ 'myReplicaSite1', 'other_failure' ],
                                         'Successful' : {} }
    self.mfac_mock.ModuleFactory().getModule.return_value = S_OK( module_mock )
    from ILCDIRAC.Core.Utilities.InputDataResolution import InputDataResolution
    self.idr = InputDataResolution( { 'Job' : { 'InputDataPolicy' : [ '/myTestPolicy/some/path' ] },
                                      'Configuration': { 'SiteName' :'myTestSitename' },
                                      'IgnoreMissing' : True } )
    assertDiracFailsWith( self.idr.execute(), 'could not access any requested input data', self )

  def test_execute_ignoremissing_works( self ):
    module_mock = Mock()
    module_mock.execute.side_effect = [ { 'OK' : True, 'Failed' : [ 'myReplicaSite1', 'other_failure' ],
                                          'Successful' : { 'WorkingSite1' : True, 'OtherGoodSite' : True } },
                                        { 'OK' : True, 'Failed' : [],
                                          'Successful' : { 'other_site' : True, 'TestSite1' : True } } ]
    self.mfac_mock.ModuleFactory().getModule.return_value = S_OK( module_mock )
    from ILCDIRAC.Core.Utilities.InputDataResolution import InputDataResolution
    self.idr = InputDataResolution( { 'Job' : { 'InputDataPolicy' : [ '/myTestPolicy/some/path',
                                                                      'other_policy/path' ] },
                                      'Configuration': { 'SiteName' :'myTestSitename' },
                                      'IgnoreMissing' : True } )
    res = self.idr.execute()
    assertDiracSucceeds( res, self )
    assertEqualsImproved( res, { 'OK' : True, 'Value' : None, 'Failed' : [],
                                 'Successful' : { 'WorkingSite1' : True, 'OtherGoodSite' : True,
                                                  'other_site' : True, 'TestSite1' : True } }, self )
    assertMockCalls( self.mfac_mock.ModuleFactory().getModule, [
      ( '/myTestPolicy/some/path', { 'Job' : { 'InputDataPolicy' : [ '/myTestPolicy/some/path',
                                                                     'other_policy/path' ] },
                                     'Configuration' : { 'SiteName' : 'myTestSitename' },
                                     'IgnoreMissing' : True } ),
      ( 'other_policy/path', { 'Job': { 'InputDataPolicy' : [ '/myTestPolicy/some/path', 'other_policy/path' ] },
                               'Configuration' : { 'SiteName' : 'myTestSitename' }, 'IgnoreMissing' : True } ) ],
                     self )

  def test_execute_allworks( self ):
    module_mock = Mock()
    module_mock.execute.side_effect = [ { 'OK' : True, 'Failed' : {},
                                          'Successful' : { 'some_site' : True, 'OtherGoodSite' : True } } ]
    self.mfac_mock.ModuleFactory().getModule.return_value = S_OK( module_mock )
    from ILCDIRAC.Core.Utilities.InputDataResolution import InputDataResolution
    self.idr = InputDataResolution( { 'Configuration': { 'SiteName' :'myTestSitename' },
                                      'IgnoreMissing' : True } )
    ops_mock = Mock()
    ops_mock.getOptionsDict.return_value = { 'Value' : { 'myTestSitename' :
                                                         ' module_path1,other_modpath     , lastmodule' } }
    self.idr.ops = ops_mock
    res = self.idr.execute()
    assertDiracSucceeds( res, self )
    assertEqualsImproved( res, { 'OK' : True, 'Value' : None, 'Failed' : {},
                                 'Successful' : { 'some_site' : True, 'OtherGoodSite' : True } }, self )
    self.mfac_mock.ModuleFactory().getModule.assert_called_once_with(
      'module_path1', { 'Job' : {}, 'Configuration' : { 'SiteName' : 'myTestSitename' }, 'IgnoreMissing' : True } )

  def test_execute_usedefaultoptions( self ):
    module_mock = Mock()
    module_mock.execute.side_effect = [ { 'OK' : True, 'Successful' : { 'some_site' : True,
                                                                        'OtherGoodSite' : True } } ]
    self.mfac_mock.ModuleFactory().getModule.return_value = S_OK( module_mock )
    from ILCDIRAC.Core.Utilities.InputDataResolution import InputDataResolution
    self.idr = InputDataResolution( { 'Configuration': { 'SiteName' :'myTestSitename' },
                                      'IgnoreMissing' : True } )
    ops_mock = Mock()
    ops_mock.getOptionsDict.return_value = { 'Value' : { 'wrong_sitename' :
                                                         ' module_path1,other_modpath     , lastmodule',
                                                         'Default' : 'my/_path/module/ ,  dontusethis      ' } }
    self.idr.ops = ops_mock
    res = self.idr.execute()
    assertDiracSucceeds( res, self )
    assertEqualsImproved( res, { 'OK' : True, 'Value' : None, 'Failed' : [],
                                 'Successful' : { 'some_site' : True, 'OtherGoodSite' : True } }, self )
    self.mfac_mock.ModuleFactory().getModule.assert_called_once_with(
      'my/_path/module/', { 'Job' : {}, 'Configuration' : { 'SiteName' : 'myTestSitename' },
                            'IgnoreMissing' : True } )

  def test_execute_nopolicies( self ):
    module_mock = Mock()
    module_mock.execute.side_effect = [ { 'OK' : True, 'Successful' : { 'some_site' : True,
                                                                        'OtherGoodSite' : True } } ]
    self.mfac_mock.ModuleFactory().getModule.return_value = S_OK( module_mock )
    from ILCDIRAC.Core.Utilities.InputDataResolution import InputDataResolution
    self.idr = InputDataResolution( { 'Configuration': { 'SiteName' :'myTestSitename' },
                                      'IgnoreMissing' : True } )
    ops_mock = Mock()
    ops_mock.getOptionsDict.return_value = { 'Value' : { 'wrong_sitename' :
                                                         ' module_path1,other_modpath     , lastmodule',
                                                         'NotDefault' : 'my/_path/module/ ,' } }
    self.idr.ops = ops_mock
    assertDiracFailsWith( self.idr.execute(), 'could not access any requested input data', self )
    self.assertFalse( self.mfac_mock.ModuleFactory().getModule.called )
