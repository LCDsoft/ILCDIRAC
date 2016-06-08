#!/usr/bin/env python
"""Test the Combined Software Installation class"""

import unittest
import os
from mock import patch, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import CombinedSoftwareInstallation, getSharedAreaLocation
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, assertDiracSucceeds, assertDiracSucceedsWith_equals

__RCSID__ = "$Id$"

class TestCombinedSWInstallation( unittest.TestCase ):
  """ Test the different methods of the class
  """
  STD_DICT = { 'Job' : { 'SoftwarePackages' : 'mypackagev1.0', 'SystemConfig' : 'mytestconfig', 'Platform' : 'mytestplatform' }, 'CE' : { 'CompatiblePlatforms' : 'blabla' }, 'Source' : {} }

  def setUp( self ):
    self.csi = CombinedSoftwareInstallation( {} )
    self.csi.apps = [ ('myprogram','v6765') ]

  def test_constructor( self ):
    localapps =  [('dep1', 'v4'), ['dep2', 'v5.0']]
    localapps_raw =  ['dep1.v4', 'dep2.v5.0']
    self.csi = CombinedSoftwareInstallation( { 'Job' : {'SoftwarePackages' : localapps_raw, 'Platform' : 'coolplatform123'} } )
    assertEqualsImproved( self.csi.jobConfig, 'coolplatform123', self )
    assertEqualsImproved( self.csi.apps, localapps, self )

  def test_constructor_illegal_sw_package( self ):
    import copy
    custom_job_dict = copy.deepcopy( TestCombinedSWInstallation.STD_DICT['Job'])
    custom_dict = copy.deepcopy( TestCombinedSWInstallation.STD_DICT )
    custom_job_dict['SoftwarePackages'] = 1
    del custom_job_dict['SystemConfig']
    custom_dict['Job'] = custom_job_dict
    custom_dict['CE'] = { 'CompatiblePlatforms' : [ 'iamcompatibletoo', 'here' ] }
    self.csi = CombinedSoftwareInstallation( custom_dict )
    assertEqualsImproved( self.csi.apps, [], self )
    assertEqualsImproved( self.csi.ceConfigs, ['x86_64-slc5-gcc43-opt'], self )

  def test_execute_simple( self ):
    self.csi.apps = None
    result = self.csi.execute()
    assertDiracSucceedsWith_equals( result, None, self )
    assertEqualsImproved( self.csi.jobConfig, 'x86_64-slc5-gcc43-opt', self )

  def test_execute( self ):
    with patch('%s.resolveDeps' % MODULE_NAME, new=Mock(return_value=[ { 'app' : 'dependency123', 'version' : '3.4' } ])), patch('%s.Operations.getSections' % MODULE_NAME, new=Mock(return_value=S_OK(['x86_64-slc5-gcc43-opt']))), patch('%s.installInAnyArea' % MODULE_NAME, new=Mock(return_value=S_OK())):
      self.csi = CombinedSoftwareInstallation( TestCombinedSWInstallation.STD_DICT )
      assertEqualsImproved( self.csi.ceConfigs, ['x86_64-slc5-gcc43-opt'], self )
      assertEqualsImproved( self.csi.jobConfig, 'mytestconfig', self )
      result = self.csi.execute()
      assertDiracSucceeds( result, self )
      assertEqualsImproved( self.csi.jobConfig, 'x86_64-slc5-gcc43-opt', self )

  def test_execute_noconfig( self ):
    self.csi.jobConfig = None
    assertDiracFailsWith( self.csi.execute(), 'no architecture requested', self )

  def test_execute_getsections_fails( self ):
    with patch('%s.Operations.getSections' % MODULE_NAME, new=Mock(return_value=S_ERROR('getsection_err'))):
      assertDiracFailsWith( self.csi.execute(), 'getsection_err', self )

  def test_execute_not_compatible( self ):
    with patch('%s.resolveDeps' % MODULE_NAME, new=Mock(return_value=[ { 'app' : 'dependency123', 'version' : '3.4' } ])), patch('%s.Operations.getSections' % MODULE_NAME, new=Mock(return_value=S_OK(['some_Exotic_system1', 'nope_not_this_one_either']))), patch('%s.installInAnyArea' % MODULE_NAME, new=Mock(return_value=S_OK())):
      import copy
      custom_dict = copy.deepcopy( TestCombinedSWInstallation.STD_DICT )
      custom_dict['CE'] = { 'CompatiblePlatforms' : [ 'iamcompatibletoo', 'here' ] }
      self.csi = CombinedSoftwareInstallation( custom_dict )
      result = self.csi.execute()
      assertDiracFailsWith( result, 'requested architecture not supported by ce', self )

  def test_execute_locally( self ):
    with patch('%s.resolveDeps' % MODULE_NAME, new=Mock(return_value=[ { 'app' : 'dependency123', 'version' : '3.4' } ])), patch('%s.Operations.getSections' % MODULE_NAME, new=Mock(return_value=S_OK([]))), patch('%s.installInAnyArea' % MODULE_NAME, new=Mock(return_value=S_OK())), patch('%s.checkCVMFS' % MODULE_NAME, new=Mock(side_effect=[S_OK(), S_ERROR()])), patch('%s.installInAnyArea' % MODULE_NAME, new=Mock(side_effect=[S_OK(), S_OK()])), patch('%s.createSharedArea' % MODULE_NAME, new=Mock(return_value=True)):
      import copy
      self.csi = CombinedSoftwareInstallation( TestCombinedSWInstallation.STD_DICT )
      self.csi.ceConfigs = []
      self.csi.sharedArea = ''
      result = self.csi.execute()
      assertDiracSucceeds( result, self )

  def test_execute_installfails( self ):
    with patch('%s.resolveDeps' % MODULE_NAME, new=Mock(return_value=[ { 'app' : 'dependency123', 'version' : '3.4' } ])), patch('%s.Operations.getSections' % MODULE_NAME, new=Mock(return_value=S_OK([]))), patch('%s.installInAnyArea' % MODULE_NAME, new=Mock(return_value=S_OK())), patch('%s.checkCVMFS' % MODULE_NAME, new=Mock(side_effect=[S_OK(), S_ERROR()])), patch('%s.installInAnyArea' % MODULE_NAME, new=Mock(return_value=S_ERROR('could not install my test program'))), patch('%s.createSharedArea' % MODULE_NAME, new=Mock(return_value=True)):
      import copy
      self.csi = CombinedSoftwareInstallation( TestCombinedSWInstallation.STD_DICT )
      self.csi.ceConfigs = []
      self.csi.sharedArea = ''
      result = self.csi.execute()
      assertDiracFailsWith( result, 'could not install my test program', self )

  def test_execute_install_dependency_fails( self ):
    with patch('%s.resolveDeps' % MODULE_NAME, new=Mock(return_value=[ { 'app' : 'dependency123', 'version' : '3.4' } ])), patch('%s.Operations.getSections' % MODULE_NAME, new=Mock(return_value=S_OK([]))), patch('%s.installDependencies' % MODULE_NAME, new=Mock(side_effect=[S_OK(), S_ERROR()])), patch('%s.createSharedArea' % MODULE_NAME, new=Mock(return_value=False)), patch('%s.installInAnyArea' % MODULE_NAME, new=Mock(return_value=S_OK())):
      import copy
      self.csi = CombinedSoftwareInstallation( TestCombinedSWInstallation.STD_DICT )
      self.csi.ceConfigs = []
      self.csi.sharedArea = ''
      result = self.csi.execute()
      assertDiracFailsWith( result, 'failed to install dep', self )

  def test_execute_nosharedarea( self ):
    with patch('%s.resolveDeps' % MODULE_NAME, new=Mock(return_value=[ { 'app' : 'dependency123', 'version' : '3.4' } ])), patch('%s.Operations.getSections' % MODULE_NAME, new=Mock(return_value=S_OK([]))), patch('%s.getSharedAreaLocation' % MODULE_NAME, new=Mock(return_value='')), patch('%s.createSharedArea' % MODULE_NAME, new=Mock(return_value=True)), patch('%s.installInAnyArea' % MODULE_NAME, new=Mock(return_value=S_OK())):
      import copy
      self.csi = CombinedSoftwareInstallation( TestCombinedSWInstallation.STD_DICT )
      self.csi.ceConfigs = []
      self.csi.sharedArea = ''
      result = self.csi.execute()
      assertDiracSucceeds( result, self )

  def test_listareadir_nofail( self ):
    with patch('%s.systemCall' % MODULE_NAME, new=Mock(return_value=S_OK([ 0, 'important_message', 'my_subprocess_error_msg']))), patch('%s.DIRAC.gLogger.info' % MODULE_NAME, new=Mock(side_effect=[True, KeyError('injecting this into logger call')])) as mock_log:
      from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import listAreaDirectory
      try:
        listAreaDirectory( self.csi.sharedArea )
        self.fail('Should not reach this due to KeyError being thrown')
      except KeyError as ke:
        mock_log.assert_any_call('important_message')
        assertEqualsImproved( ke.__repr__(), "KeyError('injecting this into logger call',)", self )

  def test_listareadir_fail_a_bit( self ):
    with patch('%s.systemCall' % MODULE_NAME, new=Mock(return_value=S_OK([ 1, 'entry', 'my_subprocess_error_msg']))), patch('%s.DIRAC.gLogger.error' % MODULE_NAME, new=Mock(side_effect=KeyError('injecting this into logger call 2'))) as mock_log:
      from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import listAreaDirectory
      try:
        listAreaDirectory( self.csi.sharedArea )
        self.fail('Should not reach this due to KeyError being thrown')
      except KeyError as ke:
        mock_log.assert_called_with('Failed to list the area directory', 'my_subprocess_error_msg')
        assertEqualsImproved( ke.__repr__(),  "KeyError('injecting this into logger call 2',)", self )

  def test_listareadir_fail_completely( self ):
    with patch('%s.systemCall' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_os_listdir_error'))), patch('%s.DIRAC.gLogger.error' % MODULE_NAME, new=Mock(side_effect=KeyError('injecting this into logger call 3'))) as mock_log:
      from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import listAreaDirectory
      try:
        listAreaDirectory( self.csi.sharedArea )
        self.fail('Should not reach this due to KeyError being thrown')
      except KeyError as ke:
        mock_log.assert_called_with('Failed to list the area directory', 'some_os_listdir_error')
        assertEqualsImproved( ke.__repr__(), "KeyError('injecting this into logger call 3',)", self )

class TestSharedLocation( unittest.TestCase ):
  from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import getSharedAreaLocation

  def test_getsharedarealoc( self ):
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value='mylocation123test')), patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ True ])), patch('%s.DIRAC.gConfig.getValue' % MODULE_NAME, new=Mock(side_effect=['a', 'a', '',''])), patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)):
      result = getSharedAreaLocation()
      assertEqualsImproved( result, 'mylocation123test', self )

  def test_getsharedarealoc_environvar( self ):
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value=[ 'testLocation135', '$MANY_MORE_LOCATIONS' ])), patch.dict( os.environ, { 'MANY_MORE_LOCATIONS' : '/abc/def/ghi'} ), patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ False, True ])), patch('%s.DIRAC.gConfig.getValue' % MODULE_NAME, new=Mock(side_effect=['a', 'a', '', ''])), patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)):
      result = getSharedAreaLocation()
      assertEqualsImproved( result, '/abc/def/ghi/clic', self )

  def test_getsharedarealoc_environvar_notfound( self ):
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value=[ 'testLocation135', '$I_AM_FAKE', '$MANY_MORE_LOCATIONS' ])), patch.dict( os.environ, { 'MANY_MORE_LOCATIONS' : '/abc/def/ghi'} ), patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ False, False, True ])), patch('%s.DIRAC.gConfig.getValue' % MODULE_NAME, new=Mock(side_effect=['a', 'a', '', ''])), patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)):
      result = getSharedAreaLocation()
      assertEqualsImproved( result, '/abc/def/ghi/clic', self )

  def test_getsharedarealoc_overwrite_via_config( self ):
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value=[ 'testLocation135', '$MANY_MORE_LOCATIONS' ])), patch.dict( os.environ, { 'MANY_MORE_LOCATIONS' : '/abc/def/ghi'} ), patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ False, True ])), patch('%s.DIRAC.gConfig.getValue' % MODULE_NAME, new=Mock(side_effect=['a', 'a', '/myotherpath/hereissharedarea', '/myotherpath/hereissharedarea'])), patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)):
      result = getSharedAreaLocation()
      assertEqualsImproved( result, '/myotherpath/hereissharedarea', self )

  def test_getsharedarealoc_notadir( self ):
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value=[ 'testLocation135', '$MANY_MORE_LOCATIONS' ])), patch.dict( os.environ, { 'MANY_MORE_LOCATIONS' : '/abc/def/ghi'} ), patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ False, True ])), patch('%s.DIRAC.gConfig.getValue' % MODULE_NAME, new=Mock(side_effect=['a', 'a', '', ''])), patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=False)):
      result = getSharedAreaLocation()
      assertEqualsImproved( result, '', self )

  def test_getsharedarealoc_notfound( self ):
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value=[])), patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ False, True ])), patch('%s.DIRAC.gConfig.getValue' % MODULE_NAME, new=Mock(side_effect=['a', 'a', '', ''])), patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)):
      result = getSharedAreaLocation()
      assertEqualsImproved( result, '', self )


CLASS_NAME = 'CombinedSoftwareInstallation'
MODULE_NAME = 'ILCDIRAC.Core.Utilities.%s' % CLASS_NAME
