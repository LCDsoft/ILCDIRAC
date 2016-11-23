#!/usr/bin/env python
""" Test the RootExecutableAnalysis module """

import unittest
from mock import patch, mock_open, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertDiracFailsWith, assertDiracSucceedsWith, assertMockCalls

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Workflow.Modules.RootExecutableAnalysis'

class TestRootExecutableAnalysis( unittest.TestCase ):
  """ Test the RootExecutableAnalysis module
  """
  def setUp( self ):
    from ILCDIRAC.Workflow.Modules.RootExecutableAnalysis import RootExecutableAnalysis
    self.rea = RootExecutableAnalysis()

  def test_applicationspecificinputs_noscript( self ):
    assertDiracFailsWith( self.rea.applicationSpecificInputs(), 'script no defined', self )

  def test_applicationspecificinputs( self ):
    self.rea.script = 'myTestscript'
    assertDiracSucceedsWith( self.rea.applicationSpecificInputs(), 'Parameters resolved', self )

  def test_runit_complete( self ):
    self.rea.platform = 'myTestPlatform'
    self.rea.applicationLog = '/my/applog/test.log'
    self.rea.script = '/my/test/my_testscript123.sh'
    self.rea.workflowStatus['OK'] = True
    self.rea.stepStatus['OK'] = True
    self.rea.applicationVersion = 'v123'
    self.rea.STEP_NUMBER = 13
    self.rea.arguments = 'mytest args 123'
    self.rea.extraCLIarguments = 'extra test_args'
    with patch('%s.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=S_OK('/mytestenvscript'))), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[False,False,False,False,True])) as exists_mock, \
         patch('%s.os.remove' % MODULE_NAME, new=Mock()) as remove_mock, \
         patch('%s.open' % MODULE_NAME, mock_open(), create=True) as open_mock, \
         patch('%s.os.chmod' % MODULE_NAME, new=Mock()) as chmod_mock, \
         patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK((0, 'my_statusmsg')))) as cmd_mock:
      assertDiracSucceedsWith( self.rea.runIt(), 'ROOT v123 Successful', self )
      assertMockCalls( exists_mock, [ 'Root_v123_Run_13.sh', './lib', 'my_testscript123.sh',
                                      '/my/applog/test.log', '/my/applog/test.log' ], self )
      self.assertFalse( remove_mock.called )
      open_mock.assert_called_once_with( 'Root_v123_Run_13.sh', 'w' )
      open_mock = open_mock()
      open_mock.close.assert_called_once_with()
      assertMockCalls( open_mock.write, [
        '#!/bin/sh \n', '#####################################################################\n',
        '# Dynamically generated script to run a production or analysis job. #\n',
        '#####################################################################\n',
        'source /mytestenvscript\n', 'echo =============================\n', 'echo LD_LIBRARY_PATH is\n',
        'echo $LD_LIBRARY_PATH | tr ":" "\n"\n', 'echo =============================\n',
        'echo PATH is\n', 'echo $PATH | tr ":" "\n"\n', 'echo =============================\n',
        'env | sort >> localEnv.log\n', 'echo =============================\n',
        'my_testscript123.sh mytest args 123 extra test_args\n', 'declare -x appstatus=$?\n',
        'exit $appstatus\n' ], self )
      chmod_mock.assert_called_once_with( 'Root_v123_Run_13.sh', 0755 )
      cmd_mock.assert_called_once_with( 0, 'sh -c "./Root_v123_Run_13.sh"',
                                        callbackFunction = self.rea.redirectLogOutput,
                                        bufferLimit = 20971520 )

  def test_runit_noplatform( self ):
    self.rea.applicationLog = 'asd'
    assertDiracFailsWith( self.rea.runIt(), 'no ilc platform selected', self )

  def test_runit_nolog( self ):
    self.rea.platform = 'asd'
    assertDiracFailsWith( self.rea.runIt(), 'no log file provided', self )

  def test_runit_getenvscript_fails( self ):
    self.rea.platform = 'myTestPlatform'
    self.rea.applicationLog = '/my/applog/test.log'
    self.rea.script = '/my/test/script.sh'
    with patch('%s.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=S_ERROR('test_scripterr'))):
      assertDiracFailsWith( self.rea.runIt(), 'test_scripterr', self )

  def test_runit_status_not_ok( self ):
    self.rea.platform = 'myTestPlatform'
    self.rea.applicationLog = '/my/applog/test.log'
    self.rea.script = '/my/test/script.sh'
    self.rea.workflowStatus['OK'] = True
    self.rea.stepStatus['OK'] = False
    with patch('%s.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=S_OK('/mytestenvscript'))):
      assertDiracSucceedsWith( self.rea.runIt(),
                               'ROOT should not proceed as previous step did not end properly', self )

  def test_runit_noscript( self ):
    self.rea.platform = 'myTestPlatform'
    self.rea.applicationLog = '/my/applog/test.log'
    self.rea.script = ''
    self.rea.workflowStatus['OK'] = True
    self.rea.stepStatus['OK'] = True
    with patch('%s.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=S_OK('/mytestenvscript'))):
      assertDiracFailsWith( self.rea.runIt(), 'executable file not defined', self )

  def test_runit_no_applog_created( self ):
    self.rea.platform = 'myTestPlatform'
    self.rea.applicationLog = '/my/applog/test.log'
    self.rea.script = '/my/test/my_testscript123.sh'
    self.rea.workflowStatus['OK'] = True
    self.rea.stepStatus['OK'] = True
    self.rea.applicationVersion = 'v123'
    self.rea.STEP_NUMBER = 13
    self.rea.arguments = 'mytest args 123'
    self.rea.extraCLIarguments = 'extra test_args'
    with patch('%s.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=S_OK('/mytestenvscript'))), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[True,True,True,True,False])) as exists_mock, \
         patch('%s.os.remove' % MODULE_NAME, new=Mock()) as remove_mock, \
         patch('%s.open' % MODULE_NAME, mock_open(), create=True) as open_mock, \
         patch('%s.os.chmod' % MODULE_NAME, new=Mock()) as chmod_mock, \
         patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK(('something', True)))) as cmd_mock:
      assertDiracFailsWith( self.rea.runIt(), 'root did not produce the expected log', self )
      assertMockCalls( exists_mock, [ 'Root_v123_Run_13.sh', './lib', 'my_testscript123.sh',
                                      '/my/applog/test.log', '/my/applog/test.log' ], self )
      assertMockCalls( remove_mock, [ 'Root_v123_Run_13.sh', '/my/applog/test.log' ], self )
      open_mock.assert_called_once_with( 'Root_v123_Run_13.sh', 'w' )
      open_mock = open_mock()
      open_mock.close.assert_called_once_with()
      assertMockCalls( open_mock.write, [
        '#!/bin/sh \n', '#####################################################################\n',
        '# Dynamically generated script to run a production or analysis job. #\n',
        '#####################################################################\n',
        'source /mytestenvscript\n', 'declare -x LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH\n',
        'echo =============================\n', 'echo LD_LIBRARY_PATH is\n',
        'echo $LD_LIBRARY_PATH | tr ":" "\n"\n', 'echo =============================\n',
        'echo PATH is\n', 'echo $PATH | tr ":" "\n"\n', 'echo =============================\n',
        'env | sort >> localEnv.log\n', 'echo =============================\n', 'chmod u+x my_testscript123.sh\n',
        './my_testscript123.sh mytest args 123 extra test_args\n', 'declare -x appstatus=$?\n',
        'exit $appstatus\n' ], self )
      chmod_mock.assert_called_once_with( 'Root_v123_Run_13.sh', 0755 )
      cmd_mock.assert_called_once_with( 0, 'sh -c "./Root_v123_Run_13.sh"',
                                        callbackFunction = self.rea.redirectLogOutput,
                                        bufferLimit = 20971520 )
