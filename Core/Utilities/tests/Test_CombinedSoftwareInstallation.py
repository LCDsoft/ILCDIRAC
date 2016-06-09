#!/usr/bin/env python
"""Test the Combined Software Installation class"""

import unittest
import os
from mock import patch, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import CombinedSoftwareInstallation, getSharedAreaLocation, createSharedArea, getLocalAreaLocation, getSoftwareFolder, getEnvironmentScript, checkCVMFS
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
  """ Tests the sharedArea and localArea functions
  """

  def test_getsharedarealoc( self ):
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value='mylocation123test')), patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ True ])), patch('%s.DIRAC.gConfig.getValue' % MODULE_NAME, new=Mock(side_effect=['a', 'a', '',''])), patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)):
      result = getSharedAreaLocation()
      assertEqualsImproved( result, 'mylocation123test', self )

  def test_getsharedarealoc_environvar( self ):
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value=[ 'testLocation135', '$MANY_MORE_LOCATIONS' ])), patch.dict( os.environ, { 'MANY_MORE_LOCATIONS' : '/abc/def/ghi'}, True ), patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ False, True ])), patch('%s.DIRAC.gConfig.getValue' % MODULE_NAME, new=Mock(side_effect=['a', 'a', '', ''])), patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)):
      result = getSharedAreaLocation()
      assertEqualsImproved( result, '/abc/def/ghi/clic', self )

  def test_getsharedarealoc_environvar_notfound( self ):
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value=[ 'testLocation135', '$I_AM_FAKE', '$MANY_MORE_LOCATIONS' ])), patch.dict( os.environ, { 'MANY_MORE_LOCATIONS' : '/abc/def/ghi'}, True ), patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ False, False, True ])), patch('%s.DIRAC.gConfig.getValue' % MODULE_NAME, new=Mock(side_effect=['a', 'a', '', ''])), patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)):
      result = getSharedAreaLocation()
      assertEqualsImproved( result, '/abc/def/ghi/clic', self )

  def test_getsharedarealoc_overwrite_via_config( self ):
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value=[ 'testLocation135', '$MANY_MORE_LOCATIONS' ])), patch.dict( os.environ, { 'MANY_MORE_LOCATIONS' : '/abc/def/ghi'}, True ), patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ False, True ])), patch('%s.DIRAC.gConfig.getValue' % MODULE_NAME, new=Mock(side_effect=['a', 'a', '/myotherpath/hereissharedarea', '/myotherpath/hereissharedarea'])), patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)):
      result = getSharedAreaLocation()
      assertEqualsImproved( result, '/myotherpath/hereissharedarea', self )

  def test_getsharedarealoc_notadir( self ):
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value=[ 'testLocation135', '$MANY_MORE_LOCATIONS' ])), patch.dict( os.environ, { 'MANY_MORE_LOCATIONS' : '/abc/def/ghi'}, True ), patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ False, True ])), patch('%s.DIRAC.gConfig.getValue' % MODULE_NAME, new=Mock(side_effect=['a', 'a', '', ''])), patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=False)):
      result = getSharedAreaLocation()
      assertEqualsImproved( result, '', self )

  def test_getsharedarealoc_notfound( self ):
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value=[])), patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ False, True ])), patch('%s.DIRAC.gConfig.getValue' % MODULE_NAME, new=Mock(side_effect=['a', 'a', '', ''])), patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)):
      result = getSharedAreaLocation()
      assertEqualsImproved( result, '', self )

  def test_createsharedarea( self ):
    with patch.dict( os.environ, { 'VO_ILC_SW_DIR' : '/myilc/sharedarea/cooldir', 'OSG_APP' : '/appdir/shared' }, True ), patch( '%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)) as isdir_mock, patch('%s.os.path.islink' % MODULE_NAME, new=Mock(return_value=False)) as islink_mock, patch( '%s.os.remove' % MODULE_NAME, new=Mock(return_value=True) ) as remove_mock, patch( '%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=False)) as exists_mock, patch( '%s.os.makedirs' % MODULE_NAME, new=Mock(return_value=True) ) as mkdirs_mock:
      result = createSharedArea()
      self.assertTrue( result )
      self.assertFalse( remove_mock.called )
      self.assertFalse( exists_mock.called )
      self.assertFalse( mkdirs_mock.called )
      finalsharedarea = '/myilc/sharedarea/cooldir/clic'
      isdir_mock.assert_called_with( finalsharedarea )
      islink_mock.assert_called_with( finalsharedarea )

  def test_createsharedarea_noenvvars( self ):
    with patch.dict( os.environ, {}, True ), patch( '%s.DIRAC.gLogger.info' % MODULE_NAME ) as mock_log:
      result = createSharedArea()
      self.assertFalse( result )
      mock_log.assert_called_with( 'VO_ILC_SW_DIR and OSG_APP not defined.', )

  def test_createsharedarea_points_to_curdir( self ):
    with patch.dict( os.environ, { 'OSG_APP' : '.' }, True ), patch( '%s.DIRAC.gLogger.info' % MODULE_NAME ) as mock_log:
      result = createSharedArea()
      self.assertFalse( result )
      mock_log.assert_called_with( 'VO_ILC_SW_DIR or OSG_APP points to "."', )

  def test_createsharedarea_makedirs( self ):
    with patch.dict( os.environ, { 'VO_ILC_SW_DIR' : '/myilc/sharedarea/cooldir', 'OSG_APP' : '/appdir/shared' }, True ), patch( '%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)) as isdir_mock, patch('%s.os.path.islink' % MODULE_NAME, new=Mock(return_value=True)) as islink_mock, patch( '%s.os.remove' % MODULE_NAME, new=Mock(return_value=True) ) as remove_mock, patch( '%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=False)) as exists_mock, patch( '%s.os.makedirs' % MODULE_NAME, new=Mock(return_value=True) ) as mkdirs_mock:
      result = createSharedArea()
      self.assertTrue( result )
      self.assertFalse( remove_mock.called )
      finalsharedarea = '/myilc/sharedarea/cooldir/clic'
      isdir_mock.assert_called_with( finalsharedarea )
      islink_mock.assert_called_with( finalsharedarea )
      exists_mock.assert_called_with( finalsharedarea )
      mkdirs_mock.assert_called_with( finalsharedarea )

  def test_createsharedarea_remove_and_recreate( self ):
    with patch.dict( os.environ, { 'OSG_APP' : '/appdir/shared' }, True ), patch( '%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=False)) as isdir_mock, patch('%s.os.path.islink' % MODULE_NAME, new=Mock(return_value=False)) as islink_mock, patch( '%s.os.remove' % MODULE_NAME, new=Mock(return_value=True) ) as remove_mock, patch( '%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)) as exists_mock, patch( '%s.os.makedirs' % MODULE_NAME, new=Mock(return_value=True) ) as mkdirs_mock:
      result = createSharedArea()
      self.assertTrue( result )
      self.assertFalse( islink_mock.called )
      finalsharedarea = '/appdir/shared/clic'
      isdir_mock.assert_called_with( finalsharedarea )
      exists_mock.assert_called_with( finalsharedarea )
      mkdirs_mock.assert_called_with( finalsharedarea )
      remove_mock.assert_called_with( finalsharedarea )

  def test_createsharedarea_oserr( self ):
    with patch.dict( os.environ, { 'VO_ILC_SW_DIR' : '/myilc/sharedarea/cooldir', 'OSG_APP' : '/appdir/shared' }, True ), patch( '%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)) as isdir_mock, patch('%s.os.path.islink' % MODULE_NAME, new=Mock(return_value=True)) as islink_mock, patch( '%s.os.remove' % MODULE_NAME, new=Mock(side_effect=OSError('some_filesys_error')) ) as remove_mock, patch( '%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)) as exists_mock, patch( '%s.os.makedirs' % MODULE_NAME, new=Mock(return_value=True) ) as mkdirs_mock, patch( '%s.DIRAC.gLogger.error' % MODULE_NAME ) as mock_err:
      result = createSharedArea()
      self.assertFalse( result )
      self.assertFalse( mkdirs_mock.called )
      finalsharedarea = '/myilc/sharedarea/cooldir/clic'
      isdir_mock.assert_called_with( finalsharedarea )
      islink_mock.assert_called_with( finalsharedarea )
      exists_mock.assert_called_with( finalsharedarea )
      remove_mock.assert_called_with( finalsharedarea )
      mock_err.assert_called_with( 'Problem trying to create shared area', 'some_filesys_error')

  def test_getlocalarealoc_already_exists( self ):
    with patch('%s.DIRAC.gConfig.getValue' % MODULE_NAME, new=Mock(return_value='/gconfig/localarea')), patch( '%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)) as isdir_mock, patch( '%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=False)) as exists_mock, patch( '%s.os.remove' % MODULE_NAME, new=Mock(return_value=True)) as remove_mock, patch( '%s.os.mkdir' % MODULE_NAME, new=Mock(return_value=True)) as mkdir_mock:
      result = getLocalAreaLocation()
      isdir_mock.assert_called_with( '/gconfig/localarea' )
      self.assertFalse( remove_mock.called )
      self.assertFalse( mkdir_mock.called )
      self.assertFalse( exists_mock.called )
      assertEqualsImproved( result, '/gconfig/localarea', self )

  def test_getlocalarealoc_remove_old( self ):
    import DIRAC
    oldRoot = DIRAC.rootPath
    DIRAC.rootPath = '/dirac/myrootpath'
    with patch('%s.DIRAC.gConfig.getValue' % MODULE_NAME, new=Mock(return_value='')), patch( '%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=False)) as isdir_mock, patch( '%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)) as exists_mock, patch( '%s.os.remove' % MODULE_NAME, new=Mock(return_value=True)) as remove_mock:
      result = getLocalAreaLocation()
      finallocalarea = '/dirac/myrootpath/LocalArea'
      isdir_mock.assert_called_with( finallocalarea )
      exists_mock.assert_called_with( finallocalarea )
      remove_mock.assert_called_with( finallocalarea )
      assertEqualsImproved( result, finallocalarea, self )
    DIRAC.rootPath = oldRoot

  def test_getlocalarealoc_create_new_dir( self ):
    with patch('%s.DIRAC.gConfig.getValue' % MODULE_NAME, new=Mock(return_value='/gconfig/localarea')), patch( '%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=False)) as isdir_mock, patch( '%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=False)) as exists_mock, patch( '%s.os.remove' % MODULE_NAME, new=Mock(return_value=True)) as remove_mock, patch( '%s.os.mkdir' % MODULE_NAME, new=Mock(return_value=True)) as mkdir_mock:
      result = getLocalAreaLocation()
      finallocalarea = '/gconfig/localarea'
      isdir_mock.assert_called_with( finallocalarea )
      mkdir_mock.assert_called_with( finallocalarea )
      exists_mock.assert_called_with( finallocalarea )
      self.assertFalse( remove_mock.called )
      assertEqualsImproved( result, finallocalarea, self )

  def test_getlocalarealoc_remove_fails( self ):
    with patch('%s.DIRAC.gConfig.getValue' % MODULE_NAME, new=Mock(return_value='/gconfig/localarea')), patch( '%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=False)) as isdir_mock, patch( '%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)) as exists_mock, patch( '%s.os.remove' % MODULE_NAME, new=Mock(side_effect=OSError('some_remove_oserror'))) as remove_mock, patch( '%s.os.mkdir' % MODULE_NAME, new=Mock(return_value=True)) as mkdir_mock, patch('%s.DIRAC.gLogger.error' % MODULE_NAME) as mock_err:
      result = getLocalAreaLocation()
      finallocalarea = '/gconfig/localarea'
      isdir_mock.assert_called_with( finallocalarea )
      exists_mock.assert_called_with( finallocalarea )
      remove_mock.assert_called_with( finallocalarea )
      mock_err.assert_called_with( 'Cannot remove:', '/gconfig/localarea because some_remove_oserror')
      self.assertFalse( mkdir_mock.called )
      assertEqualsImproved( result, '', self )

  def test_getlocalarealoc_create_fails( self ):
    with patch('%s.DIRAC.gConfig.getValue' % MODULE_NAME, new=Mock(return_value='/gconfig/localarea')), patch( '%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=False)) as isdir_mock, patch( '%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=False)) as exists_mock, patch( '%s.os.remove' % MODULE_NAME, new=Mock(return_value=True)) as remove_mock, patch( '%s.os.mkdir' % MODULE_NAME, new=Mock(side_effect=OSError('some_mkdir_oserror'))) as mkdir_mock, patch('%s.DIRAC.gLogger.error' % MODULE_NAME) as mock_err:
      result = getLocalAreaLocation()
      finallocalarea = '/gconfig/localarea'
      isdir_mock.assert_called_with( finallocalarea )
      exists_mock.assert_called_with( finallocalarea )
      mkdir_mock.assert_called_with( finallocalarea )
      mock_err.assert_called_with( 'Cannot create:', '/gconfig/localarea because some_mkdir_oserror')
      self.assertFalse( remove_mock.called )
      assertEqualsImproved( result, '', self )

  def test_getsoftwarefolder( self ):
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value='myapparchive.test.tgz')) as getval_mock, patch('%s.getLocalAreaLocation' % MODULE_NAME, new=Mock(return_value='/mylocalarea/test')), patch('%s.getSharedAreaLocation' % MODULE_NAME, new=Mock(return_value='/testshared/area')), patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ False, False, True ])) as exists_mock:
      result = getSoftwareFolder( 'a', 'b', 'c' )
      exists_mock.assert_any_call( '/mylocalarea/test/myapparchive.test' )
      exists_mock.assert_any_call( '/testshared/area/myapparchive.test' )
      getval_mock.assert_called_with( '/AvailableTarBalls/a/b/c/TarBall', '' )
      assertEqualsImproved( len(exists_mock.mock_calls), 3, self ) #One exists call in checkCVMFS
      assertDiracSucceedsWith_equals( result, '/testshared/area/myapparchive.test', self )

  def test_getsoftwarefolder_from_cvmfs( self ):
    with patch('%s.checkCVMFS' % MODULE_NAME, new=Mock(return_value=S_OK(('mycvmfsfolder/txt', 'otherentry')))) as cvmfs_mock:
      result = getSoftwareFolder( 'a', 'b', 'c' )
      cvmfs_mock.assert_called_with( 'a', ['b', 'c'] )
      assertDiracSucceedsWith_equals( result, 'mycvmfsfolder/txt', self )

  def test_getsoftwarefolder_apptar_fails( self ):
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value='')):
      result = getSoftwareFolder( 'a', 'b', 'c' )
      assertDiracFailsWith( result, 'could not find b, c name from cs', self )

  def test_getsoftwarefolder_uselocal( self ):
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value='myapparchivev2.test.tar.gz')) as getval_mock, patch('%s.getLocalAreaLocation' % MODULE_NAME, new=Mock(return_value='/mylocalarea/test')), patch('%s.getSharedAreaLocation' % MODULE_NAME, new=Mock(return_value='/testshared/area')), patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ False, True, False ])) as exists_mock:
      result = getSoftwareFolder( 'a', 'b', 'c' )
      exists_mock.assert_called_with( '/mylocalarea/test/myapparchivev2.test' )
      getval_mock.assert_called_with( '/AvailableTarBalls/a/b/c/TarBall', '' )
      assertEqualsImproved( len(exists_mock.mock_calls), 2, self ) #One exists call in checkCVMFS
      assertDiracSucceedsWith_equals( result, '/mylocalarea/test/myapparchivev2.test', self )

  def test_getsoftwarefolder_notfound( self ):
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value='myapp_executable')) as getval_mock, patch('%s.getLocalAreaLocation' % MODULE_NAME, new=Mock(return_value='/mylocalarea/test')), patch('%s.getSharedAreaLocation' % MODULE_NAME, new=Mock(return_value='/testshared/area')), patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ False, False, False ])) as exists_mock:
      result = getSoftwareFolder( 'a', 'b', 'c' )
      exists_mock.assert_any_call( '/mylocalarea/test/myapp_executable' )
      exists_mock.assert_any_call( '/testshared/area/myapp_executable' )
      getval_mock.assert_called_with( '/AvailableTarBalls/a/b/c/TarBall', '' )
      assertEqualsImproved( len(exists_mock.mock_calls), 3, self ) #One exists call in checkCVMFS
      assertDiracFailsWith( result, 'missing installation of myapp_executable', self )

  def test_getEnvironmentScript( self ):
    with patch('%s.checkCVMFS' % MODULE_NAME, new=Mock(return_value=S_OK(('/otherfolder/otherfile', 'cvmfsfolder/myenvscript')))):
      result = getEnvironmentScript( 'a', 'b', 'c', None )
      assertDiracSucceedsWith_equals( result, '', self ) #TODO bug??

  def test_getEnvironmentScript_cvmfs_empty( self ):
    def return_my_value( platform, appname, appversion ): #pylint: disable=C0111
      return ( platform, appname, appversion )
    with patch('%s.checkCVMFS' % MODULE_NAME, new=Mock(return_value=S_OK(('entry', '')))):
      result = getEnvironmentScript( 'a', 'b', 'c', return_my_value )
      assertEqualsImproved( result, ('a', 'b', 'c'), self )

  def test_getEnvironmentScript_from_passed_func( self ):
    def return_my_value( platform, appname, appversion ): #pylint: disable=C0111
      return ( appname, platform, appversion )
    with patch('%s.checkCVMFS' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_cvmfs_error'))):
      result = getEnvironmentScript( 'a', 'b', 'c', return_my_value )
      assertEqualsImproved( result, ('b', 'a', 'c'), self )

  def test_checkcvmfs_not_found( self ):
    result = checkCVMFS( 'mytestplatform', ('appnametesttest123', 'appversionv77testp2'))
    assertDiracFailsWith( result, 'missing cvmfs', self )

  def test_checkcvmfs_exists( self ):
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(side_effect=['testcvmfspath', 'testenvscript'])), patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)):
      result = checkCVMFS( 'mytestplatform', ('appnametesttest123', 'appversionv77testp2'))
      assertDiracSucceedsWith_equals( result, ('testcvmfspath', 'testenvscript'), self )

CLASS_NAME = 'CombinedSoftwareInstallation'
MODULE_NAME = 'ILCDIRAC.Core.Utilities.%s' % CLASS_NAME
