#!/usr/bin/env python
""" Test the SLICPandora module """

import unittest
import sys
from mock import patch, mock_open, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, \
  assertDiracFailsWith, assertDiracSucceeds, assertDiracSucceedsWith, \
  assertDiracSucceedsWith_equals, assertMockCalls

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Workflow.Modules.SLICPandoraAnalysis'

class TestSLICPandora( unittest.TestCase ):
  """ Test the SLICPandora module
  """
  def setUp( self ):
    # Mock out modules that spawn other threads
    sys.modules['DIRAC.DataManagementSystem.Client.DataManager'] = Mock()
    from ILCDIRAC.Workflow.Modules.SLICPandoraAnalysis import SLICPandoraAnalysis
    self.spa = SLICPandoraAnalysis()
    self.spa.platform = 'myTestPlatform'
    self.spa.applicationLog = 'applogFile.test'

  def test_applicationSpecificInputs( self ):
    assertDiracSucceedsWith_equals( self.spa.applicationSpecificInputs(),
                                    'Parameters resolved', self )
    assertEqualsImproved( self.spa.pandorasettings, 'PandoraSettings.xml', self )
    assertEqualsImproved( self.spa.InputFile, [], self )
    assertEqualsImproved( self.spa.InputData, [], self )

  def test_applicationSpecificInputs_1( self ):
    self.spa.pandorasettings = 'something'
    self.spa.InputData = [ 'myfile.slcio', 'ignorethisfile', 'testPart1.slcio',
                           'myarchive.slcio.tar.gz' ]
    assertDiracSucceedsWith_equals( self.spa.applicationSpecificInputs(),
                                    'Parameters resolved', self )
    assertEqualsImproved( self.spa.InputFile,
                          [ 'myfile.slcio', 'testPart1.slcio',
                            'myarchive.slcio.tar.gz' ], self )

  def test_runit( self ):
    exists_dict = { 'testmodelDetector_pandora.xml' : False,
                    '/secret/dir/testmodelDetector_pandora.xml' : True,
                    '/secret/dir/testmodelDetector.zip' : False,
                    'SLICPandora__Run_465.sh' : True, './lib' : False,
                    'applogFile.test' : True }
    self.spa.detectorxml = '/secret/dir/testmodelDetector_pandora.xml'
    self.spa.InputFile = 'testInput.file'
    self.spa.STEP_NUMBER = 465
    ops_mock = Mock()
    ops_mock.getValue.return_value = [ 'detector_1_url_throw_error.pdf',
                                       'detector2_unzip_fails.xml',
                                       'working_detector_v4.xml' ]
    self.spa.ops = ops_mock
    mo = mock_open( read_data='some_log_data\nsuccessful finish :)')
    with patch('%s.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=S_OK('myenvscriptpathtestme'))), \
             patch('%s.resolveIFpaths' % MODULE_NAME, new=Mock(return_value=S_OK([ 'ruinonslcio.test' ]))), \
             patch('%s.unzip_file_into_dir' % MODULE_NAME, new=Mock(side_effect=[ OSError('unzipping failed'), True ])) as unzip_mock, \
             patch('%s.os.unlink' % MODULE_NAME) as unlink_mock, \
             patch('%s.os.chmod' % MODULE_NAME) as chmod_mock, \
             patch('%s.open' % MODULE_NAME, mo ) as open_mock, \
             patch('%s.urllib.urlretrieve' % MODULE_NAME, new=Mock(side_effect=[ IOError('my_test_ioerr'), True, True ])), \
             patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK([0, 'Disabled execution', '']))) as shell_mock, \
             patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/my/curdir/test')), \
             patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=lambda path: exists_dict[path])) as exists_mock, patch('%s.os.remove' % MODULE_NAME) as remove_mock:
      assertDiracSucceeds( self.spa.runIt(), self )
      assertMockCalls( exists_mock, [ 'testmodelDetector_pandora.xml', '/secret/dir/testmodelDetector.zip',
                                      '/secret/dir/testmodelDetector.zip',
                                      '/secret/dir/testmodelDetector_pandora.xml', 'SLICPandora__Run_465.sh',
                                      './lib', 'applogFile.test', 'applogFile.test' ], self )
      assertMockCalls( open_mock, [ '/secret/dir/testmodelDetector.zip', ( 'SLICPandora__Run_465.sh', 'w' ),
                                    ( 'applogFile.test', 'r' ) ], self, only_these_calls = False )
      mo = mo()
      assertMockCalls( unzip_mock, [ ( mo, '/my/curdir/test' ), ( mo, '/my/curdir/test') ], self )
      open_mock = open_mock()
      assertMockCalls( open_mock.write, [
        '#!/bin/bash \n', '#####################################################################\n',
        '# Dynamically generated script to run a production or analysis job. #\n',
        '#####################################################################\n',
        'source myenvscriptpathtestme\n', 'declare -x file=./Settings/\n',
        '\nif [ -e "${file}" ]\nthen\n   declare -x PANDORASETTINGS=$file\nelse\n  if [ -d "${PANDORASETTINGSDIR}" ]\n  then\n    cp $PANDORASETTINGSDIR/*.xml .\n    declare -x PANDORASETTINGS=\n  fi\nfi\nif [ ! -e "${PANDORASETTINGS}" ]\nthen\n  echo "Missing PandoraSettings file"\n  exit 1\nfi  \n',
        'echo =============================\n', 'echo PATH is \n', 'echo $PATH | tr ":" "\n"  \n',
        'echo ==============\n', 'echo =============================\n', 'echo LD_LIBRARY_PATH is \n',
        'echo $LD_LIBRARY_PATH | tr ":" "\n"\n', 'echo ============================= \n',
        'env | sort >> localEnv.log\n',
        'PandoraFrontend -g /secret/dir/testmodelDetector_pandora.xml -c $PANDORASETTINGS -i ruinonslcio.test -o  -r 0 \n',
        'declare -x appstatus=$?\n', 'exit $appstatus\n' ], self )
      assertMockCalls( remove_mock, [ 'SLICPandora__Run_465.sh', 'applogFile.test' ], self )
      chmod_mock.assert_called_once_with( 'SLICPandora__Run_465.sh', 0755 )
      shell_mock.assert_called_once_with( 0, 'sh -c "./SLICPandora__Run_465.sh"',
                                          callbackFunction = self.spa.redirectLogOutput,
                                          bufferLimit = 20971520 )
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
    assertDiracSucceedsWith_equals( self.spa.runIt(),
                                    'SLIC Pandora should not proceed as previous step did not end properly',
                                    self )

  def test_runit_stepstatus_bad( self ):
    self.spa.stepStatus = S_ERROR('step_err_testme')
    assertDiracSucceedsWith_equals( self.spa.runIt(),
                                    'SLIC Pandora should not proceed as previous step did not end properly',
                                    self )

  def test_runit_getsoftwarefolder_fails( self ):
    with patch('%s.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=S_ERROR('get_envscriptI_test_error'))) as getsoft_mock:
      assertDiracFailsWith( self.spa.runIt(), 'get_envscriptI_test_error', self )
      getsoft_mock.assert_called_once_with( 'myTestPlatform', 'SLICPandora', '', self.spa.getEnvScript )

  def test_runit_resolveIFPaths_fails( self ):
    with patch('%s.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=S_OK('myenvscriptpathtestme'))) as getsoft_mock, \
         patch('%s.resolveIFpaths' % MODULE_NAME, new=Mock(return_value=S_ERROR('slcio_err_test'))), \
         patch('%s.SLICPandoraAnalysis.setApplicationStatus' % MODULE_NAME) as appstat_mock:
      assertDiracFailsWith( self.spa.runIt(), 'missing slcio file', self )
      getsoft_mock.assert_called_once_with( 'myTestPlatform', 'SLICPandora', '',
                                            self.spa.getEnvScript )
      appstat_mock.assert_called_once_with( 'SLICPandora: missing slcio file' )

  def test_runit_no_detectormodel_found( self ):
    exists_dict = { 'testmodelDetector.xml' : True, '/secret/dir/testmodelDetector.xml' : False }
    self.spa.detectorxml = '/secret/dir/testmodelDetector.xml'
    self.spa.InputFile = 'testInput.file'
    with patch('%s.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=S_OK('myenvscriptpathtestme'))), \
             patch('%s.resolveIFpaths' % MODULE_NAME, new=Mock(return_value=S_OK([ 'ruinonslcio.test' ]))) as resolve_mock, \
             patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=lambda path: exists_dict[path])) as exists_mock:
      assertDiracFailsWith( self.spa.runIt(),
                            'detector model xml was not found, exiting', self )
      assertMockCalls( exists_mock, [ 'testmodelDetector.xml', '/secret/dir/testmodelDetector.xml' ], self )
      resolve_mock.assert_called_once_with( 'testInput.file' )

  def test_runit_no_applog_created( self ):
    exists_dict = { 'testmodelDetector_pandora.xml' : False,
                    '/secret/dir/testmodelDetector_pandora.xml' : True,
                    '/secret/dir/testmodelDetector.zip' : True,
                    'SLICPandora__Run_465.sh' : True, './lib' : True,
                    'applogFile.test' : True }
    self.spa.detectorxml = '/secret/dir/testmodelDetector_pandora.xml'
    self.spa.InputFile = 'testInput.file'
    self.spa.STEP_NUMBER = 465
    mo = mock_open()
    def replace_exists( path ):
      """ Mock exists implementation """
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
      assertDiracFailsWith( self.spa.runIt(),
                            'SLICPandora did not produce the expected log',
                            self )
      assertMockCalls( exists_mock, [ 'testmodelDetector_pandora.xml', '/secret/dir/testmodelDetector.zip',
                                      '/secret/dir/testmodelDetector.zip',
                                      '/secret/dir/testmodelDetector_pandora.xml', 'SLICPandora__Run_465.sh',
                                      './lib', 'applogFile.test', 'applogFile.test' ], self )
      assertMockCalls( open_mock, [ ('/secret/dir/testmodelDetector.zip' ), ( 'SLICPandora__Run_465.sh', 'w') ],
                        self, only_these_calls = False )
      mo = mo()
      unzip_mock.assert_called_once_with( mo, '/my/curdir/test' )
      open_mock = open_mock()
      assertMockCalls( open_mock.write, [
        '#!/bin/bash \n', '#####################################################################\n',
        '# Dynamically generated script to run a production or analysis job. #\n',
        '#####################################################################\n',
        'source myenvscriptpathtestme\n', 'declare -x file=./Settings/\n',
        '\nif [ -e "${file}" ]\nthen\n   declare -x PANDORASETTINGS=$file\nelse\n  if [ -d "${PANDORASETTINGSDIR}" ]\n  then\n    cp $PANDORASETTINGSDIR/*.xml .\n    declare -x PANDORASETTINGS=\n  fi\nfi\nif [ ! -e "${PANDORASETTINGS}" ]\nthen\n  echo "Missing PandoraSettings file"\n  exit 1\nfi  \n',
        'echo =============================\n', 'echo PATH is \n',
        'echo $PATH | tr ":" "\n"  \n', 'echo ==============\n',
        'declare -x LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH\n', 'echo =============================\n',
        'echo LD_LIBRARY_PATH is \n', 'echo $LD_LIBRARY_PATH | tr ":" "\n"\n',
        'echo ============================= \n', 'env | sort >> localEnv.log\n',
        'PandoraFrontend -g /secret/dir/testmodelDetector_pandora.xml -c $PANDORASETTINGS -i ruinonslcio.test -o  -r 0 \n',
        'declare -x appstatus=$?\n', 'exit $appstatus\n' ], self )
      assertMockCalls( remove_mock, [ 'SLICPandora__Run_465.sh', 'applogFile.test' ], self )
      chmod_mock.assert_called_once_with( 'SLICPandora__Run_465.sh', 0755 )
      shell_mock.assert_called_once_with( 0, 'sh -c "./SLICPandora__Run_465.sh"',
                                          callbackFunction = self.spa.redirectLogOutput,
                                          bufferLimit = 20971520 )
      self.assertFalse( unlink_mock.called )

  def test_runit_unzip_fails_nodetectorURL( self ):
    exists_dict = { 'testmodelDetector_pandora.xml' : False,
                    '/secret/dir/testmodelDetector_pandora.xml' : True,
                    '/secret/dir/testmodelDetector.zip' : True,
                    'SLICPandora__Run_465.sh' : False, './lib' : False,
                    'applogFile.test' : True }
    self.spa.detectorxml = '/secret/dir/testmodelDetector_pandora.xml'
    self.spa.InputFile = 'testInput.file'
    self.spa.STEP_NUMBER = 465
    ops_mock = Mock()
    ops_mock.getValue.return_value = [ '' ]
    self.spa.ops = ops_mock
    mo = mock_open()
    def replace_exists( path ):
      """ Mock exists implementation """
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
      assertDiracFailsWith( self.spa.runIt(),
                            'could not find in cs the url for detector model',
                            self )
      open_mock.assert_called_once_with( '/secret/dir/testmodelDetector.zip' )
      assertMockCalls( exists_mock, [ 'testmodelDetector_pandora.xml', '/secret/dir/testmodelDetector.zip',
                                      '/secret/dir/testmodelDetector.zip' ], self )
      unzip_mock.assert_called_once_with( mo(), '/my/curdir/test' )
      open_mock = open_mock()
      self.assertFalse( open_mock.write.called )
      self.assertFalse( remove_mock.called )
      self.assertFalse( chmod_mock.called )
      self.assertFalse( shell_mock.called )
      unlink_mock.assert_called_once_with( '/secret/dir/testmodelDetector.zip' )

  def test_runit_oldversion( self ):
    exists_dict = { 'secret/dir/mySuperDetector_pandora.xml' : False,
                    'secret/dir/mySuperDetector.zip' : False,
                    '/my/curdir/test/secret/dir/mySuperDetector_pandora.xml' : True,
                    'SLICPandora_V2_Run_465.sh' : False, './lib' : False,
                    'applogFile.test' : False }
    self.spa.detectorxml = 'secret/dir/mySuperDetector'
    self.spa.applicationVersion = 'V2'
    self.spa.InputFile = 'testInput.file'
    self.spa.STEP_NUMBER = 465
    ops_mock = Mock()
    ops_mock.getValue.return_value = [ 'http://some_url/' ]
    self.spa.ops = ops_mock
    mo = mock_open( read_data='some_log_data\n Missing PandoraSettings file, but ignore this error :)')
    def replace_exists( path ):
      """ Mock implementation of exists """
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
      assertMockCalls( exists_mock, [ 'secret/dir/mySuperDetector.zip', 'secret/dir/mySuperDetector.zip',
                                      '/my/curdir/test/secret/dir/mySuperDetector_pandora.xml',
                                      'SLICPandora_V2_Run_465.sh', './lib', 'applogFile.test',
                                      'applogFile.test' ], self )
      assertMockCalls( open_mock, [ ( 'SLICPandora_V2_Run_465.sh', 'w' ), ( 'applogFile.test', 'r') ],
                       self, only_these_calls = False )
      mo = mo()
      open_mock = open_mock()
      assertMockCalls( open_mock.write, [
        '#!/bin/bash \n', '#####################################################################\n',
        '# Dynamically generated script to run a production or analysis job. #\n',
        '#####################################################################\n',
        'source myenvscriptpathtestme\n', 'declare -x file=./Settings/\n',
        '\nif [ -e "${file}" ]\nthen\n   declare -x PANDORASETTINGS=$file\nelse\n  if [ -d "${PANDORASETTINGSDIR}" ]\n  then\n    cp $PANDORASETTINGSDIR/*.xml .\n    declare -x PANDORASETTINGS=\n  fi\nfi\nif [ ! -e "${PANDORASETTINGS}" ]\nthen\n  echo "Missing PandoraSettings file"\n  exit 1\nfi  \n',
        'echo =============================\n', 'echo PATH is \n', 'echo $PATH | tr ":" "\n"  \n',
        'echo ==============\n', 'echo =============================\n', 'echo LD_LIBRARY_PATH is \n',
        'echo $LD_LIBRARY_PATH | tr ":" "\n"\n', 'echo ============================= \n',
        'env | sort >> localEnv.log\n',
        'PandoraFrontend /my/curdir/test/secret/dir/mySuperDetector_pandora.xml $PANDORASETTINGS ruinonslcio.test  0 \n',
        'declare -x appstatus=$?\n', 'exit $appstatus\n' ], self )
      chmod_mock.assert_called_once_with( 'SLICPandora_V2_Run_465.sh', 0755 )
      shell_mock.assert_called_once_with( 0, 'sh -c "./SLICPandora_V2_Run_465.sh"',
                                          callbackFunction = self.spa.redirectLogOutput,
                                          bufferLimit = 20971520 )
      self.assertFalse( urlretrieve_mock.called )
      self.assertFalse( unzip_mock.called )
      self.assertFalse( unlink_mock.called )
      self.assertFalse( remove_mock.called )

  def test_getenvscript( self ):
    def replace_abspath( path ):
      """ Mock abspath implementation """
      if path == 'SLICPandora.sh':
        return '/abs/test/path/SLICPandora.sh'
      else:
        sys.exit()
    exists_dict = { 'PandoraFrontend' : True }
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(return_value=S_OK('/my/dir/test/me'))) as getsoft_mock, \
         patch('%s.removeLibc' % MODULE_NAME) as removelib_mock, \
         patch('%s.getNewLDLibs' % MODULE_NAME, new=Mock(return_value='/new/ldpath')) as getlib_mock, \
         patch('%s.getNewPATH' % MODULE_NAME, new=Mock(return_value='/new/test/path')) as getpath_mock, \
         patch('%s.open' % MODULE_NAME, mock_open()) as open_mock:
      with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=lambda path: exists_dict[path])) as exists_mock, \
         patch('%s.os.chmod' % MODULE_NAME) as chmod_mock, \
         patch('%s.os.path.abspath' % MODULE_NAME, new=Mock(side_effect=replace_abspath)) as abspath_mock:
        result = self.spa.getEnvScript( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
        exists_mock.assert_called_once_with( 'PandoraFrontend' )
        abspath_mock.assert_called_once_with( 'SLICPandora.sh' )
        assertDiracSucceedsWith_equals( result, '/abs/test/path/SLICPandora.sh', self )
        chmod_mock.assert_called_once_with( 'SLICPandora.sh', 0755 )
      getsoft_mock.assert_called_once_with( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
      removelib_mock.assert_called_once_with( '/my/dir/test/me/LDLibs' )
      getlib_mock.assert_called_once_with( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
      getpath_mock.assert_called_once_with( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
      open_mock.assert_any_call( 'SLICPandora.sh', 'w' )
      open_mock = open_mock()
      assertMockCalls( open_mock.write, [
        '#!/bin/sh \n', '############################################################\n',
        '# Dynamically generated script to get the SLICPandora env. #\n',
        '############################################################\n',
        "declare -x PATH=/new/test/path:$PATH\n", 'declare -x ROOTSYS=/my/dir/test/me/ROOT\n',
        'declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:/my/dir/test/me/LDLibs:/new/ldpath\n',
        'declare -x PANDORASETTINGSDIR=/my/dir/test/me/Settings\n', "declare -x PATH=.:$PATH\n" ], self )

  def test_getenvscript_getsoftware_fails( self ):
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(return_value=S_ERROR('getsoftware_test_err'))), \
         patch('%s.SLICPandoraAnalysis.setApplicationStatus' % MODULE_NAME, new=Mock(return_value=S_ERROR())):
      result = self.spa.getEnvScript( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
      assertDiracFailsWith( result, 'getsoftware_test_err', self )

  def test_getenvscript_other_prefixpath( self ):
    def replace_abspath( path ):
      """ Mock implementation of os.path.abspath """
      if path == 'SLICPandora.sh':
        return '/abs/test/path/SLICPandora.sh'
      else:
        sys.exit()
    exists_dict = { 'PandoraFrontend' : False, '/my/dir/test/me/Executable/PandoraFrontend' : True }
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(return_value=S_OK('/my/dir/test/me'))) as getsoft_mock, \
         patch('%s.removeLibc' % MODULE_NAME) as removelib_mock, \
         patch('%s.getNewLDLibs' % MODULE_NAME, new=Mock(return_value='/new/ldpath')) as getlib_mock, \
         patch('%s.getNewPATH' % MODULE_NAME, new=Mock(return_value='/new/test/path')) as getpath_mock, \
         patch('%s.open' % MODULE_NAME, mock_open()) as open_mock:
      with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=lambda path: exists_dict[path])) as exists_mock, \
         patch('%s.os.chmod' % MODULE_NAME) as chmod_mock, \
         patch('%s.os.path.abspath' % MODULE_NAME, new=Mock(side_effect=replace_abspath)) as abspath_mock:
        result = self.spa.getEnvScript( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
        chmod_mock.assert_called_once_with( 'SLICPandora.sh', 0755 )
        assertMockCalls( exists_mock, [ 'PandoraFrontend', '/my/dir/test/me/Executable/PandoraFrontend' ], self )
        abspath_mock.assert_called_once_with( 'SLICPandora.sh' )
      assertDiracSucceedsWith_equals( result, '/abs/test/path/SLICPandora.sh', self )
      getsoft_mock.assert_called_once_with( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
      removelib_mock.assert_called_once_with( '/my/dir/test/me/LDLibs' )
      getlib_mock.assert_called_once_with( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
      getpath_mock.assert_called_once_with( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
      open_mock.assert_any_call( 'SLICPandora.sh', 'w' )
      open_mock = open_mock()
      assertMockCalls( open_mock.write, [
        '#!/bin/sh \n', '############################################################\n',
        '# Dynamically generated script to get the SLICPandora env. #\n',
        '############################################################\n',
        "declare -x PATH=/new/test/path:$PATH\n", 'declare -x ROOTSYS=/my/dir/test/me/ROOT\n',
        'declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:/my/dir/test/me/LDLibs:/new/ldpath\n',
        'declare -x PANDORASETTINGSDIR=/my/dir/test/me/Settings\n', "declare -x PATH=/my/dir/test/me/Executable:$PATH\n" ], self )

  def test_getenvscript_pandorafrontend_missing( self ):
    exists_dict = { 'PandoraFrontend' : False, '/my/dir/test/me/Executable/PandoraFrontend' : False }
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(return_value=S_OK('/my/dir/test/me'))) as getsoft_mock, \
         patch('%s.removeLibc' % MODULE_NAME) as removelib_mock, \
         patch('%s.getNewLDLibs' % MODULE_NAME, new=Mock(return_value='/new/ldpath')) as getlib_mock, \
         patch('%s.getNewPATH' % MODULE_NAME, new=Mock(return_value='/new/test/path')) as getpath_mock, \
         patch('%s.open' % MODULE_NAME, mock_open()) as open_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=lambda path: exists_dict[path])) as exists_mock, \
         patch('%s.os.chmod' % MODULE_NAME) as chmod_mock:
      result = self.spa.getEnvScript( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
      assertDiracFailsWith( result, 'missing pandorafrontend binary', self )
      getsoft_mock.assert_called_once_with( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
      removelib_mock.assert_called_once_with( '/my/dir/test/me/LDLibs' )
      getlib_mock.assert_called_once_with( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
      getpath_mock.assert_called_once_with( 'mytestsysconfig', 'SLIC Pandora', 'V2' )
      self.assertFalse( open_mock.called )
      self.assertFalse( chmod_mock.called )
      assertMockCalls( exists_mock, [ 'PandoraFrontend', '/my/dir/test/me/Executable/PandoraFrontend' ], self )
