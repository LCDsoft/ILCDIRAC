#!/usr/bin/env python
""" Test the SLICPandora module """

import unittest
from mock import patch, call, mock_open, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Workflow.Modules.SLICPandoraAnalysis import SLICPandoraAnalysis
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, assertDiracSucceeds, assertDiracSucceedsWith, assertDiracSucceedsWith_equals

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Workflow.Modules.SLICPandoraAnalysis'

class TestSLICPandora( unittest.TestCase ):
  """ Test the SLICPandora module
  """
  def setUp( self ):
    self.spa = SLICPandoraAnalysis()
    self.spa.platform = 'myTestPlatform'
    self.spa.applicationLog = 'applogFile.test'

  def test_applicationSpecificInputs( self ):
    assertDiracSucceedsWith_equals( self.spa.applicationSpecificInputs(), 'Parameters resolved', self )
    assertEqualsImproved( self.spa.pandorasettings, 'PandoraSettings.xml', self )
    assertEqualsImproved( self.spa.InputFile, [], self )
    assertEqualsImproved( self.spa.InputData, [], self )

  def test_applicationSpecificInputs_1( self ):
    self.spa.pandorasettings = 'something'
    self.spa.InputData = [ 'myfile.slcio', 'ignorethisfile', 'testPart1.slcio', 'myarchive.slcio.tar.gz' ]
    assertDiracSucceedsWith_equals( self.spa.applicationSpecificInputs(), 'Parameters resolved', self )
    assertEqualsImproved( self.spa.InputFile, [ 'myfile.slcio', 'testPart1.slcio', 'myarchive.slcio.tar.gz' ], self )

  def test_runit( self ):
    exists_dict = { 'testmodelDetector_pandora.xml' : False, '/secret/dir/testmodelDetector_pandora.xml' : True, '/secret/dir/testmodelDetector.zip' : False, 'SLICPandora__Run_465.sh' : True, './lib' : False, 'applogFile.test' : True }
    self.spa.detectorxml = '/secret/dir/testmodelDetector_pandora.xml'
    self.spa.InputFile = 'testInput.file'
    self.spa.STEP_NUMBER = 465
    ops_mock = Mock()
    ops_mock.getValue.return_value = [ 'detector_1_url_throw_error.pdf', 'detector2_unzip_fails.xml', 'working_detector_v4.xml' ]
    self.spa.ops = ops_mock
    mo = mock_open( read_data='some_log_data\nsuccessful finish :)')
    def replace_exists( path ):
      return exists_dict[path]
    with patch('%s.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=S_OK('myenvscriptpathtestme'))), \
             patch('%s.resolveIFpaths' % MODULE_NAME, new=Mock(return_value=S_OK([ 'ruinonslcio.test' ]))), \
             patch('%s.unzip_file_into_dir' % MODULE_NAME, new=Mock(side_effect=[ OSError('unzipping failed'), True ])) as unzip_mock, \
             patch('%s.os.unlink' % MODULE_NAME) as unlink_mock, \
             patch('%s.os.chmod' % MODULE_NAME) as chmod_mock, \
             patch('%s.open' % MODULE_NAME, mo ) as open_mock, \
             patch('%s.urllib.urlretrieve' % MODULE_NAME, new=Mock(side_effect=[ IOError('my_test_ioerr'), True, True ])), \
             patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK([0, 'Disabled execution', '']))) as shell_mock, \
             patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/my/curdir/test')), \
             patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=replace_exists)) as exists_mock, patch('%s.os.remove' % MODULE_NAME) as remove_mock:
      assertDiracSucceeds( self.spa.runIt(), self )
      assertEqualsImproved( exists_mock.mock_calls, [ call( 'testmodelDetector_pandora.xml' ), call( '/secret/dir/testmodelDetector.zip' ), call( '/secret/dir/testmodelDetector.zip' ), call( '/secret/dir/testmodelDetector_pandora.xml' ), call( 'SLICPandora__Run_465.sh'), call('./lib'), call( 'applogFile.test' ), call( 'applogFile.test' ) ], self )
      for opened_file in [ call( '/secret/dir/testmodelDetector.zip' ), call( 'SLICPandora__Run_465.sh', 'w'), call( 'applogFile.test', 'r')]:
        self.assertIn( opened_file, open_mock.mock_calls )
      mo = mo()
      assertEqualsImproved( unzip_mock.mock_calls, [ call( mo, '/my/curdir/test' ), call( mo, '/my/curdir/test') ], self )
      open_mock = open_mock()
      assertEqualsImproved( open_mock.write.mock_calls, [ call('#!/bin/bash \n'), call('#####################################################################\n'), call('# Dynamically generated script to run a production or analysis job. #\n'), call('#####################################################################\n'), call('source myenvscriptpathtestme\n'), call('declare -x file=./Settings/\n'), call('\nif [ -e "${file}" ]\nthen\n   declare -x PANDORASETTINGS=$file\nelse\n  if [ -d "${PANDORASETTINGSDIR}" ]\n  then\n    cp $PANDORASETTINGSDIR/*.xml .\n    declare -x PANDORASETTINGS=\n  fi\nfi\nif [ ! -e "${PANDORASETTINGS}" ]\nthen\n  echo "Missing PandoraSettings file"\n  exit 1\nfi  \n'), call('echo =============================\n'), call('echo PATH is \n'), call('echo $PATH | tr ":" "\n"  \n'), call('echo ==============\n'), call('echo =============================\n'), call('echo LD_LIBRARY_PATH is \n'), call('echo $LD_LIBRARY_PATH | tr ":" "\n"\n'), call('echo ============================= \n'), call('env | sort >> localEnv.log\n'), call('PandoraFrontend -g /secret/dir/testmodelDetector_pandora.xml -c $PANDORASETTINGS -i ruinonslcio.test -o  -r 0 \n'), call('declare -x appstatus=$?\n'), call('exit $appstatus\n') ], self )
      assertEqualsImproved( remove_mock.mock_calls, [ call('SLICPandora__Run_465.sh' ), call( 'applogFile.test') ], self )
      chmod_mock.assert_called_once_with( 'SLICPandora__Run_465.sh', 0755 )
      shell_mock.assert_called_once_with( 0, 'sh -c "./SLICPandora__Run_465.sh"', callbackFunction = self.spa.redirectLogOutput, bufferLimit = 20971520 )
      unlink_mock.assert_called_once_with( '/secret/dir/testmodelDetector.zip' )

  def test_runit_noplatform( self ):
    self.spa.platform = None
    result = self.spa.runIt()
    assertDiracFailsWith( result, 'no ilc platform selected', self )

  def test_runit_nolog( self ):
    self.spa.applicationLog = None
    result = self.spa.runIt()
    assertDiracFailsWith( result, 'no log file provided', self )

  def test_runit_workflowstatus_bad( self ):
    self.spa.workflowStatus = S_ERROR('workflow_err_testme')
    assertDiracSucceedsWith_equals( self.spa.runIt(), 'SLIC Pandora should not proceed as previous step did not end properly', self )

  def test_runit_stepstatus_bad( self ):
    self.spa.stepStatus = S_ERROR('step_err_testme')
    assertDiracSucceedsWith_equals( self.spa.runIt(), 'SLIC Pandora should not proceed as previous step did not end properly', self )

  def test_runit_getsoftwarefolder_fails( self ):
    with patch('%s.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=S_ERROR('get_envscriptI_test_error'))) as getsoft_mock:
      assertDiracFailsWith( self.spa.runIt(), 'get_envscriptI_test_error', self )
      getsoft_mock.assert_called_once_with( 'myTestPlatform', 'SLICPandora', '', self.spa.getEnvScript )

  def test_runit_resolveIFPaths_fails( self ):
    with patch('%s.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=S_OK('myenvscriptpathtestme'))) as getsoft_mock, \
         patch('%s.resolveIFpaths' % MODULE_NAME, new=Mock(return_value=S_ERROR('slcio_err_test'))), \
         patch('%s.SLICPandoraAnalysis.setApplicationStatus' % MODULE_NAME) as appstat_mock:
      assertDiracFailsWith( self.spa.runIt(), 'missing slcio file', self )
      getsoft_mock.assert_called_once_with( 'myTestPlatform', 'SLICPandora', '', self.spa.getEnvScript )
      appstat_mock.assert_called_once_with( 'SLICPandora: missing slcio file' )

  def test_runit_no_detectormodel_found( self ):
    self.spa.detectorxml = '/secret/dir/testmodelDetector.xml'
    self.spa.InputFile = 'testInput.file'
    with patch('%s.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=S_OK('myenvscriptpathtestme'))), \
             patch('%s.resolveIFpaths' % MODULE_NAME, new=Mock(return_value=S_OK([ 'ruinonslcio.test' ]))) as resolve_mock, \
             patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ True, False ])) as exists_mock:
      assertDiracFailsWith( self.spa.runIt(), 'detector model xml was not found, exiting', self )
      assertEqualsImproved( exists_mock.mock_calls, [ call( 'testmodelDetector.xml' ), call( '/secret/dir/testmodelDetector.xml' ) ], self )
      resolve_mock.assert_called_once_with( 'testInput.file' )

  def test_runit_no_applog_created( self ):
    exists_dict = { 'testmodelDetector_pandora.xml' : False, '/secret/dir/testmodelDetector_pandora.xml' : True, '/secret/dir/testmodelDetector.zip' : True, 'SLICPandora__Run_465.sh' : True, './lib' : True, 'applogFile.test' : True }
    self.spa.detectorxml = '/secret/dir/testmodelDetector_pandora.xml'
    self.spa.InputFile = 'testInput.file'
    self.spa.STEP_NUMBER = 465
    mo = mock_open()
    def replace_exists( path ):
      if path == 'applogFile.test':
        result = exists_dict[path]
        exists_dict[path] = False
        return result
      else:
        return exists_dict[path]
    with patch('%s.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=S_OK('myenvscriptpathtestme'))), \
             patch('%s.resolveIFpaths' % MODULE_NAME, new=Mock(return_value=S_OK([ 'ruinonslcio.test' ]))), \
             patch('%s.unzip_file_into_dir' % MODULE_NAME) as unzip_mock, \
             patch('%s.os.unlink' % MODULE_NAME) as unlink_mock, \
             patch('%s.os.chmod' % MODULE_NAME) as chmod_mock, \
             patch('%s.open' % MODULE_NAME, mo ) as open_mock, \
             patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK(['something']))) as shell_mock, \
             patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/my/curdir/test')), \
             patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=replace_exists)) as exists_mock, patch('%s.os.remove' % MODULE_NAME) as remove_mock:
      assertDiracFailsWith( self.spa.runIt(), 'SLICPandora did not produce the expected log', self )
      assertEqualsImproved( exists_mock.mock_calls, [ call( 'testmodelDetector_pandora.xml' ), call( '/secret/dir/testmodelDetector.zip' ), call( '/secret/dir/testmodelDetector.zip' ), call( '/secret/dir/testmodelDetector_pandora.xml' ), call( 'SLICPandora__Run_465.sh'), call('./lib'), call( 'applogFile.test' ), call( 'applogFile.test' ) ], self )
      for opened_file in [ call( '/secret/dir/testmodelDetector.zip' ), call( 'SLICPandora__Run_465.sh', 'w')]:
        self.assertIn( opened_file, open_mock.mock_calls )
      mo = mo()
      unzip_mock.assert_called_once_with( mo, '/my/curdir/test' )
      open_mock = open_mock()
      assertEqualsImproved( open_mock.write.mock_calls, [ call('#!/bin/bash \n'), call('#####################################################################\n'), call('# Dynamically generated script to run a production or analysis job. #\n'), call('#####################################################################\n'), call('source myenvscriptpathtestme\n'), call('declare -x file=./Settings/\n'), call('\nif [ -e "${file}" ]\nthen\n   declare -x PANDORASETTINGS=$file\nelse\n  if [ -d "${PANDORASETTINGSDIR}" ]\n  then\n    cp $PANDORASETTINGSDIR/*.xml .\n    declare -x PANDORASETTINGS=\n  fi\nfi\nif [ ! -e "${PANDORASETTINGS}" ]\nthen\n  echo "Missing PandoraSettings file"\n  exit 1\nfi  \n'), call('echo =============================\n'), call('echo PATH is \n'), call('echo $PATH | tr ":" "\n"  \n'), call('echo ==============\n'), call('declare -x LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH\n'), call('echo =============================\n'), call('echo LD_LIBRARY_PATH is \n'), call('echo $LD_LIBRARY_PATH | tr ":" "\n"\n'), call('echo ============================= \n'), call('env | sort >> localEnv.log\n'), call('PandoraFrontend -g /secret/dir/testmodelDetector_pandora.xml -c $PANDORASETTINGS -i ruinonslcio.test -o  -r 0 \n'), call('declare -x appstatus=$?\n'), call('exit $appstatus\n') ], self )
      assertEqualsImproved( remove_mock.mock_calls, [ call('SLICPandora__Run_465.sh' ), call( 'applogFile.test') ], self )
      chmod_mock.assert_called_once_with( 'SLICPandora__Run_465.sh', 0755 )
      shell_mock.assert_called_once_with( 0, 'sh -c "./SLICPandora__Run_465.sh"', callbackFunction = self.spa.redirectLogOutput, bufferLimit = 20971520 )
      self.assertFalse( unlink_mock.called )

  def test_runit_unzip_fails_nodetectorURL( self ):
    exists_dict = { 'testmodelDetector_pandora.xml' : False, '/secret/dir/testmodelDetector_pandora.xml' : True, '/secret/dir/testmodelDetector.zip' : True, 'SLICPandora__Run_465.sh' : False, './lib' : False, 'applogFile.test' : True }
    self.spa.detectorxml = '/secret/dir/testmodelDetector_pandora.xml'
    self.spa.InputFile = 'testInput.file'
    self.spa.STEP_NUMBER = 465
    ops_mock = Mock()
    ops_mock.getValue.return_value = [ '' ]
    self.spa.ops = ops_mock
    mo = mock_open()
    def replace_exists( path ):
      if path == '/secret/dir/testmodelDetector.zip':
        result = exists_dict[path]
        exists_dict[path] = False
        return result
      else:
        return exists_dict[path]
    with patch('%s.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=S_OK('myenvscriptpathtestme'))), \
             patch('%s.resolveIFpaths' % MODULE_NAME, new=Mock(return_value=S_OK([ 'ruinonslcio.test' ]))), \
             patch('%s.unzip_file_into_dir' % MODULE_NAME, side_effect=OSError('unable to unzip_testme')) as unzip_mock, \
             patch('%s.os.unlink' % MODULE_NAME) as unlink_mock, \
             patch('%s.os.chmod' % MODULE_NAME) as chmod_mock, \
             patch('%s.open' % MODULE_NAME, mo ) as open_mock, \
             patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK(['something']))) as shell_mock, \
             patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/my/curdir/test')), \
             patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=replace_exists)) as exists_mock, patch('%s.os.remove' % MODULE_NAME) as remove_mock:
      assertDiracFailsWith( self.spa.runIt(), 'could not find in cs the url for detector model', self )
      assertEqualsImproved( exists_mock.mock_calls, [ call( 'testmodelDetector_pandora.xml' ), call( '/secret/dir/testmodelDetector.zip' ), call( '/secret/dir/testmodelDetector.zip' ) ], self )
      unzip_mock.assert_called_once_with( mo(), '/my/curdir/test' )
      open_mock.assert_any_call( '/secret/dir/testmodelDetector.zip' )
      open_mock = open_mock()
      self.assertFalse( open_mock.write.called )
      self.assertFalse( remove_mock.called )
      self.assertFalse( chmod_mock.called )
      self.assertFalse( shell_mock.called )
      unlink_mock.assert_called_once_with( '/secret/dir/testmodelDetector.zip' )

  def test_runit_oldversion( self ):
    exists_dict = { 'secret/dir/mySuperDetector_pandora.xml' : False, 'secret/dir/mySuperDetector.zip' : False, '/my/curdir/test/secret/dir/mySuperDetector_pandora.xml' : True, 'SLICPandora_V2_Run_465.sh' : False, './lib' : False, 'applogFile.test' : False }
    self.spa.detectorxml = 'secret/dir/mySuperDetector'
    self.spa.applicationVersion = 'V2'
    self.spa.InputFile = 'testInput.file'
    self.spa.STEP_NUMBER = 465
    ops_mock = Mock()
    ops_mock.getValue.return_value = [ 'http://some_url/' ]
    self.spa.ops = ops_mock
    mo = mock_open( read_data='some_log_data\n Missing PandoraSettings file, but ignore this error :)')
    def replace_exists( path ):
      result = exists_dict[path]
      if path == 'secret/dir/mySuperDetector.zip' or path == 'applogFile.test':
        exists_dict[path] = True
      return result
    with patch('%s.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=S_OK('myenvscriptpathtestme'))), \
             patch('%s.resolveIFpaths' % MODULE_NAME, new=Mock(return_value=S_OK([ 'ruinonslcio.test' ]))), \
             patch('%s.unzip_file_into_dir' % MODULE_NAME) as unzip_mock, \
             patch('%s.os.unlink' % MODULE_NAME) as unlink_mock, \
             patch('%s.os.chmod' % MODULE_NAME) as chmod_mock, \
             patch('%s.open' % MODULE_NAME, mo ) as open_mock, \
             patch('%s.urllib.urlretrieve' % MODULE_NAME) as urlretrieve_mock, \
             patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK([0, 'Disabled execution', '']))) as shell_mock, \
             patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/my/curdir/test')), \
             patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=replace_exists)) as exists_mock, patch('%s.os.remove' % MODULE_NAME) as remove_mock:
      assertDiracSucceedsWith( self.spa.runIt(), 'SLICPandora V2 Successful', self )
      assertEqualsImproved( exists_mock.mock_calls, [ call( 'secret/dir/mySuperDetector.zip' ), call( 'secret/dir/mySuperDetector.zip' ), call( '/my/curdir/test/secret/dir/mySuperDetector_pandora.xml' ), call( 'SLICPandora_V2_Run_465.sh'), call('./lib'), call( 'applogFile.test' ), call( 'applogFile.test' ) ], self )
      for opened_file in [ call( 'SLICPandora_V2_Run_465.sh', 'w'), call( 'applogFile.test', 'r') ]:
        self.assertIn( opened_file, open_mock.mock_calls )
      mo = mo()
      #assertEqualsImproved( unzip_mock.mock_calls, [ call( mo, '/my/curdir/test' ), call( mo, '/my/curdir/test') ], self )
      open_mock = open_mock()
      assertEqualsImproved( open_mock.write.mock_calls, [ call('#!/bin/bash \n'), call('#####################################################################\n'), call('# Dynamically generated script to run a production or analysis job. #\n'), call('#####################################################################\n'), call('source myenvscriptpathtestme\n'), call('declare -x file=./Settings/\n'), call('\nif [ -e "${file}" ]\nthen\n   declare -x PANDORASETTINGS=$file\nelse\n  if [ -d "${PANDORASETTINGSDIR}" ]\n  then\n    cp $PANDORASETTINGSDIR/*.xml .\n    declare -x PANDORASETTINGS=\n  fi\nfi\nif [ ! -e "${PANDORASETTINGS}" ]\nthen\n  echo "Missing PandoraSettings file"\n  exit 1\nfi  \n'), call('echo =============================\n'), call('echo PATH is \n'), call('echo $PATH | tr ":" "\n"  \n'), call('echo ==============\n'), call('echo =============================\n'), call('echo LD_LIBRARY_PATH is \n'), call('echo $LD_LIBRARY_PATH | tr ":" "\n"\n'), call('echo ============================= \n'), call('env | sort >> localEnv.log\n'), call('PandoraFrontend /my/curdir/test/secret/dir/mySuperDetector_pandora.xml $PANDORASETTINGS ruinonslcio.test  0 \n'), call('declare -x appstatus=$?\n'), call('exit $appstatus\n') ], self )
      #assertEqualsImproved( remove_mock.mock_calls, [ call( 'applogFile.test') ], self )
      chmod_mock.assert_called_once_with( 'SLICPandora_V2_Run_465.sh', 0755 )
      shell_mock.assert_called_once_with( 0, 'sh -c "./SLICPandora_V2_Run_465.sh"', callbackFunction = self.spa.redirectLogOutput, bufferLimit = 20971520 )
      #unlink_mock.assert_called_once_with( '/secret/dir/testmodelDetector.zip' )
      self.assertFalse( urlretrieve_mock.called )
      #urlretrieve_mock.assert_called_once_with( 'http://some_url//secret/dir/mySuperDetector.zip' )
      self.assertFalse( unzip_mock.called )
      self.assertFalse( unlink_mock.called )
      self.assertFalse( remove_mock.called )

  def test_getenvscript( self ):
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(return_value=S_OK('/my/dir/test/me'))) as getsoft_mock, \
         patch('%s.removeLibc' % MODULE_NAME) as removelib_mock, \
         patch('%s.getNewLDLibs' % MODULE_NAME, new=Mock(return_value='/new/ldpath')) as getlib_mock, \
         patch('%s.getNewPATH' % MODULE_NAME, new=Mock(return_value='/new/test/path')) as getpath_mock, \
         patch('%s.open' % MODULE_NAME, mock_open()) as open_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ True ])) as exists_mock, \
         patch('%s.os.chmod' % MODULE_NAME) as chmod_mock, \
         patch('%s.os.path.abspath' % MODULE_NAME, new=Mock(return_value='/abs/test/path/SLICPandora.sh')) as abspath_mock:
      result = self.spa.getEnvScript( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
      assertDiracSucceedsWith_equals( result, '/abs/test/path/SLICPandora.sh', self )
      getsoft_mock.assert_called_once_with( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
      removelib_mock.assert_called_once_with( '/my/dir/test/me/LDLibs' )
      getlib_mock.assert_called_once_with( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
      getpath_mock.assert_called_once_with( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
      open_mock.assert_any_call( 'SLICPandora.sh', 'w' )
      open_mock = open_mock()
      assertEqualsImproved( open_mock.write.mock_calls, [ call('#!/bin/sh \n') , call('############################################################\n'), call('# Dynamically generated script to get the SLICPandora env. #\n'), call('############################################################\n'), call("declare -x PATH=/new/test/path:$PATH\n"), call('declare -x ROOTSYS=/my/dir/test/me/ROOT\n'), call('declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:/my/dir/test/me/LDLibs:/new/ldpath\n'), call('declare -x PANDORASETTINGSDIR=/my/dir/test/me/Settings\n'), call("declare -x PATH=.:$PATH\n" ) ], self )
      chmod_mock.assert_called_once_with( 'SLICPandora.sh', 0755 )
      exists_mock.assert_called_once_with( 'PandoraFrontend' )
      abspath_mock.assert_called_once_with( 'SLICPandora.sh' )

  def test_getenvscript_getsoftware_fails( self ):
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(return_value=S_ERROR('getsoftware_test_err'))), patch('%s.SLICPandoraAnalysis.setApplicationStatus' % MODULE_NAME, new=Mock(return_value=S_ERROR())):
      result = self.spa.getEnvScript( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
      assertDiracFailsWith( result, 'getsoftware_test_err', self )

  def test_getenvscript_other_prefixpath( self ):
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(return_value=S_OK('/my/dir/test/me'))) as getsoft_mock, \
         patch('%s.removeLibc' % MODULE_NAME) as removelib_mock, \
         patch('%s.getNewLDLibs' % MODULE_NAME, new=Mock(return_value='/new/ldpath')) as getlib_mock, \
         patch('%s.getNewPATH' % MODULE_NAME, new=Mock(return_value='/new/test/path')) as getpath_mock, \
         patch('%s.open' % MODULE_NAME, mock_open()) as open_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ False, True ])) as exists_mock, \
         patch('%s.os.chmod' % MODULE_NAME) as chmod_mock, \
         patch('%s.os.path.abspath' % MODULE_NAME, new=Mock(return_value='/abs/test/path/SLICPandora.sh')) as abspath_mock:
      result = self.spa.getEnvScript( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
      assertDiracSucceedsWith_equals( result, '/abs/test/path/SLICPandora.sh', self )
      getsoft_mock.assert_called_once_with( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
      removelib_mock.assert_called_once_with( '/my/dir/test/me/LDLibs' )
      getlib_mock.assert_called_once_with( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
      getpath_mock.assert_called_once_with( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
      open_mock.assert_any_call( 'SLICPandora.sh', 'w' )
      open_mock = open_mock()
      assertEqualsImproved( open_mock.write.mock_calls, [ call('#!/bin/sh \n') , call('############################################################\n'), call('# Dynamically generated script to get the SLICPandora env. #\n'), call('############################################################\n'), call("declare -x PATH=/new/test/path:$PATH\n"), call('declare -x ROOTSYS=/my/dir/test/me/ROOT\n'), call('declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:/my/dir/test/me/LDLibs:/new/ldpath\n'), call('declare -x PANDORASETTINGSDIR=/my/dir/test/me/Settings\n'), call("declare -x PATH=/my/dir/test/me/Executable:$PATH\n" ) ], self )
      chmod_mock.assert_called_once_with( 'SLICPandora.sh', 0755 )
      assertEqualsImproved( exists_mock.mock_calls, [ call('PandoraFrontend'), call('/my/dir/test/me/Executable/PandoraFrontend') ], self )
      abspath_mock.assert_called_once_with( 'SLICPandora.sh' )

  def test_getenvscript_pandorafrontend_missing( self ):
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(return_value=S_OK('/my/dir/test/me'))) as getsoft_mock, \
         patch('%s.removeLibc' % MODULE_NAME) as removelib_mock, \
         patch('%s.getNewLDLibs' % MODULE_NAME, new=Mock(return_value='/new/ldpath')) as getlib_mock, \
         patch('%s.getNewPATH' % MODULE_NAME, new=Mock(return_value='/new/test/path')) as getpath_mock, \
         patch('%s.open' % MODULE_NAME, mock_open()) as open_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ False, False ])) as exists_mock, \
         patch('%s.os.chmod' % MODULE_NAME) as chmod_mock:
      result = self.spa.getEnvScript( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
      assertDiracFailsWith( result, 'missing pandorafrontend binary', self )
      getsoft_mock.assert_called_once_with( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
      removelib_mock.assert_called_once_with( '/my/dir/test/me/LDLibs' )
      getlib_mock.assert_called_once_with( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
      getpath_mock.assert_called_once_with( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
      open_mock.assert_any_call( 'SLICPandora.sh', 'w' )
      open_mock = open_mock()
      assertEqualsImproved( open_mock.write.mock_calls, [ call('#!/bin/sh \n') , call('############################################################\n'), call('# Dynamically generated script to get the SLICPandora env. #\n'), call('############################################################\n'), call("declare -x PATH=/new/test/path:$PATH\n"), call('declare -x ROOTSYS=/my/dir/test/me/ROOT\n'), call('declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:/my/dir/test/me/LDLibs:/new/ldpath\n'), call('declare -x PANDORASETTINGSDIR=/my/dir/test/me/Settings\n') ], self )
      self.assertFalse( chmod_mock.called )
      assertEqualsImproved( exists_mock.mock_calls, [ call('PandoraFrontend'), call('/my/dir/test/me/Executable/PandoraFrontend') ], self )

