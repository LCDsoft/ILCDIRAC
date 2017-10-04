#!/usr/bin/env python
"""Test the Whizard2 WorkflowModule"""

import __builtin__
import unittest
import os
import shutil
import tempfile
from mock import patch, MagicMock as Mock, mock_open

from DIRAC import gLogger, S_OK, S_ERROR
from ILCDIRAC.Workflow.Modules.Whizard2Analysis import Whizard2Analysis
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertDiracSucceeds

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Workflow.Modules.Whizard2Analysis'
MODULEBASE_NAME = 'ILCDIRAC.Workflow.Modules.ModuleBase'
PROXYINFO_NAME = 'DIRAC.Core.Security.ProxyInfo'
#pylint: disable=too-many-public-methods, protected-access

gLogger.setLevel("ERROR")
gLogger.showHeaders(True)

def cleanup(tempdir):
  """
  Remove files after run
  """
  try:
    shutil.rmtree(tempdir)
  except OSError:
    pass

@patch("%s.getProxyInfoAsString" % MODULEBASE_NAME, new=Mock(return_value=S_OK()))
@patch("%s.getProxyInfoAsString" % PROXYINFO_NAME, new=Mock(return_value=S_OK()))
class TestWhizard2Analysis( unittest.TestCase ):
  """ test Whizard2Analysis """

  def assertIn(self, *args, **kwargs):
    """make this existing to placate pylint"""
    return super(TestWhizard2Analysis, self).assertIn(*args, **kwargs)

  @patch("%s.getProxyInfoAsString" % MODULEBASE_NAME, new=Mock(return_value=S_OK()))
  @patch("%s.getProxyInfoAsString" % PROXYINFO_NAME, new=Mock(return_value=S_OK()))
  def setUp( self ):
    self.whiz = Whizard2Analysis()
    self.curdir = os.getcwd()
    self.tempdir = tempfile.mkdtemp("", dir = "./")
    os.chdir(self.tempdir)
    self.whiz.ops = Mock()

  def tearDown( self ):
    os.chdir(self.curdir)
    cleanup(self.tempdir)

class TestWhizard2AnalysisRunit( TestWhizard2Analysis ):
  """ test Whizard2 runtIt """

  def setUp( self ):
    super(TestWhizard2AnalysisRunit, self).setUp()
    self.logFileName = "python101.log"
    with open(self.logFileName, "w") as logF:
      logF.write("logged the logging logs")

  @patch("%s.getEnvironmentScript" % MODULE_NAME, new=Mock(return_value=S_OK("setup.sh") ) )
  @patch("%s.shellCall" % MODULE_NAME, new=Mock(return_value=S_OK((0,"AllGood")) ) )
  def test_Whizard2_runIt_success(self):
    """Whizard2.runit ................................................................................."""
    self.whiz.platform = 'Windows'
    self.whiz.applicationLog = self.logFileName
    ## side effect for Script, userlibs, log, logAfter
    with patch("os.path.exists", new=Mock(side_effect=[False, False, False, True] ) ):
      res = self.whiz.runIt()
    print res
    assertDiracSucceeds( res, self )

  @patch("%s.getEnvironmentScript" % MODULE_NAME, new=Mock(return_value=S_OK("setup.sh") ) )
  @patch("%s.shellCall" % MODULE_NAME, new=Mock(return_value=S_OK((0,"AllGood")) ) )
  def test_Whizard2_runIt_failure_LogFile(self):
    """Whizard2.runit failure with applicationLog......................................................"""
    self.whiz.platform = "Windows"
    self.whiz.applicationLog = self.logFileName
    self.whiz.ignoreapperrors = False
    ## side effect for Script, userlibs, log, logAfter
    with patch("os.path.exists", new=Mock(side_effect=[False, False, False, False] ) ):
      res = self.whiz.runIt()
    self.assertIn( "did not produce the expected log", res['Message'] )

  @patch("%s.getEnvironmentScript" % MODULE_NAME, new=Mock(return_value=S_OK("setup.sh") ) )
  @patch("%s.shellCall" % MODULE_NAME, new=Mock(return_value=S_OK((0,"AllGood")) ) )
  def test_Whizard2_runIt_failure_LogFile_ignore(self):
    """Whizard2.runit failure with applicationLog but ignore..........................................."""
    self.whiz.platform = "Windows"
    self.whiz.applicationLog = self.logFileName
    self.whiz.ignoreapperrors = True
    ## side effect for Script, userlibs, log, logAfter
    with patch("os.path.exists", new=Mock(side_effect=[False, False, False, False] ) ):
      res = self.whiz.runIt()
    assertDiracSucceeds( res, self )

  @patch("%s.getEnvironmentScript" % MODULE_NAME, new=Mock(return_value=S_OK("setup.sh") ) )
  @patch("%s.shellCall" % MODULE_NAME, new=Mock(return_value=S_OK((0,"AllGood")) ) )
  def test_Whizard2_runIt_failure_NoLogFile(self):
    """Whizard2.runit failure with applicationLog not set............................................."""
    self.whiz.platform = "Windows"
    self.whiz.ignoreapperrors = True
    ## side effect for Script, userlibs, log, logAfter
    with patch("os.path.exists", new=Mock(side_effect=[False, False, False, False] ) ):
      res = self.whiz.runIt()
    self.assertIn( "No Log file provide", res['Message'] )

  @patch("%s.getEnvironmentScript" % MODULE_NAME, new=Mock(return_value=S_OK("setup.sh") ) )
  @patch("%s.shellCall" % MODULE_NAME, new=Mock(return_value=S_OK((0,"AllGood")) ) )
  def test_Whizard2_runIt_failure_NoPlatform(self):
    """Whizard2.runit failure with platform ........................................................."""
    self.whiz.applicationLog = self.logFileName
    self.whiz.ignoreapperrors = True
    ## side effect for Script, userlibs, log, logAfter
    with patch("os.path.exists", new=Mock(side_effect=[False, False, False, False] ) ):
      res = self.whiz.runIt()
    self.assertIn( "No ILC platform selected", res['Message'] )

  @patch("%s.getEnvironmentScript" % MODULE_NAME, new=Mock(return_value=S_OK("setup.sh") ) )
  @patch("%s.shellCall" % MODULE_NAME, new=Mock(return_value=S_OK((0,"AllGood")) ) )
  def test_Whizard2_runIt_success_LogAndScriptPresent(self):
    """Whizard2.runit success log and script exist..................................................."""
    self.whiz.platform = "Windows"
    self.whiz.applicationLog = self.logFileName
    self.whiz.ignoreapperrors = True
    with open("Whizard2__Run_.sh", "w") as scr:
      scr.write("content")
    with open("Whizard2__Steer_.sin", "w") as scr:
      scr.write("content")
    with open(self.logFileName, "w") as scr:
      scr.write("content")
    ## side effect for Script, userlibs, log, logAfter
    with patch("os.path.exists", new=Mock(side_effect=[True, True, False, True] ) ):
      res = self.whiz.runIt()
    assertDiracSucceeds( res, self )

  @patch("%s.getEnvironmentScript" % MODULE_NAME, new=Mock(return_value=S_OK("setup.sh") ) )
  @patch("%s.shellCall" % MODULE_NAME, new=Mock(return_value=S_OK((0,"AllGood")) ) )
  def test_Whizard2_runIt_success_sinFile(self):
    """Whizard.runit success with steeringFile........................................................."""
    self.whiz.platform = "Windows"
    self.whiz.applicationLog = self.logFileName
    self.whiz.whizard2SinFile = "whizard instructions"
    ## side effect for Steering1, Steering2, Script, userlib, log, logAfter
    with patch("os.path.exists", new=Mock(side_effect=[False, False, False, True] ) ):
      res = self.whiz.runIt()
    assertDiracSucceeds( res, self )
    self.assertEqual( self.whiz.whizard2SinFile, "whizard instructions" )
    self.assertIn( "whizard instructions", open("Whizard2__Steer_.sin").read())

  @patch("%s.getEnvironmentScript" % MODULE_NAME, new=Mock(return_value=S_OK("setup.sh") ) )
  @patch("%s.shellCall" % MODULE_NAME, new=Mock(return_value=S_OK((0,"AllGood")) ) )
  def test_Whizard2_runIt_success_numberOfEvents(self):
    """Whizard.runit success with number of events check.............................................."""
    self.whiz.platform = "Windows"
    self.whiz.applicationLog = self.logFileName
    self.whiz.whizard2SinFile = "whizard instructions"
    self.whiz.NumberOfEvents = 100
    ## side effect for Steering1, Steering2, Script, userlib, log, logAfter
    with patch("os.path.exists", new=Mock(side_effect=[False, False, False, True] ) ):
      res = self.whiz.runIt()
    assertDiracSucceeds( res, self )
    self.assertEqual( self.whiz.NumberOfEvents, 100 )
    self.assertIn( "n_events = 100", open("Whizard2__Steer_.sin").read())

  @patch("%s.getEnvironmentScript" % MODULE_NAME, new=Mock(return_value=S_OK("setup.sh") ) )
  @patch("%s.shellCall" % MODULE_NAME, new=Mock(return_value=S_OK((0,"AllGood")) ) )
  def test_Whizard2_runIt_success_randomSeed(self):
    """Whizard.runit success with random seed check.............................................."""
    self.whiz.platform = "Windows"
    self.whiz.applicationLog = self.logFileName
    self.whiz.whizard2SinFile = "whizard instructions"
    self.whiz.randomSeed = 100
    ## side effect for Steering1, Steering2, Script, userlib, log, logAfter
    with patch("os.path.exists", new=Mock(side_effect=[False, False, False, True] ) ):
      res = self.whiz.runIt()
    assertDiracSucceeds( res, self )
    self.assertEqual( self.whiz.randomSeed, 100 )
    self.assertIn( "seed = 100", open("Whizard2__Steer_.sin").read())

  @patch("%s.getEnvironmentScript" % MODULE_NAME, new=Mock(return_value=S_OK("setup.sh") ) )
  @patch("%s.shellCall" % MODULE_NAME, new=Mock(return_value=S_OK((0,"AllGood")) ) )
  def test_Whizard2_runIt_success_outputFile1(self):
    """Whizard.runit success with outputfile check 1............................................"""
    self.whiz.platform = "Windows"
    self.whiz.applicationLog = self.logFileName
    self.whiz.whizard2SinFile = "whizard instructions"
    self.whiz.OutputFile = "test.slcio"
    ## side effect for Steering1, Steering2, Script, userlib, log, logAfter
    with patch("os.path.exists", new=Mock(side_effect=[False, False, False, True] ) ):
      res = self.whiz.runIt()
    assertDiracSucceeds( res, self )
    self.assertIn( "sample_format = lcio", open("Whizard2__Steer_.sin").read())

  @patch("%s.getEnvironmentScript" % MODULE_NAME, new=Mock(return_value=S_OK("setup.sh") ) )
  @patch("%s.shellCall" % MODULE_NAME, new=Mock(return_value=S_OK((0,"AllGood")) ) )
  def test_Whizard2_runIt_success_outputFile2(self):
    """Whizard.runit success with outputfile check 2............................................"""
    self.whiz.platform = "Windows"
    self.whiz.applicationLog = self.logFileName
    self.whiz.whizard2SinFile = "whizard instructions"
    self.whiz.OutputFile = "test.stdhep"
    ## side effect for Steering1, Steering2, Script, userlib, log, logAfter
    with patch("os.path.exists", new=Mock(side_effect=[False, False, False, True] ) ):
      res = self.whiz.runIt()
    assertDiracSucceeds( res, self )
    self.assertIn( "sample_format = stdhep", open("Whizard2__Steer_.sin").read())

  def test_Whizard2_runIt_fail(self):
    """Whizard.runit fail steps......................................................................."""
    self.whiz.platform = "Windows"
    self.whiz.applicationLog = self.logFileName
    self.whiz.workflowStatus = S_ERROR( "Failed earlier")
    res = self.whiz.runIt()
    self.assertEqual( res['Value'], "Whizard2 should not proceed as previous step did not end properly" )

  @patch("%s.getEnvironmentScript" % MODULE_NAME, new=Mock(return_value=S_ERROR("missing setup.sh") ) )
  def test_Whizard2_runIt_fail_env(self):
    """Whizard.runit failed to get env................................................................"""
    self.whiz.platform = "Windows"
    self.whiz.applicationLog = self.logFileName
    res = self.whiz.runIt()
    self.assertEqual( res['Message'], "missing setup.sh" )

class TestWhizard2AnalysisASI( TestWhizard2Analysis ):
  """Whizard.ApplicationSpecificInputs """

  @patch.dict(os.environ, {"JOBID": "12345"} )
  def test_Whizard2_ASI_NoVariables( self ):
    """Whizard.applicationSpecificInputs: checks that no variables have been set after this call......"""
    gLogger.setLevel("ERROR")
    self.whiz.workflow_commons = dict()
    self.whiz.applicationSpecificInputs()
    self.assertFalse( self.whiz.jobReport or self.whiz.productionID )

  @patch.dict(os.environ, {"JOBID": "12345"} )
  def test_Whizard2_ASI_RandomSeed_Prod( self ):
    """Whizard.applicationSpecificInputs: check setting of randomseed in production..................."""
    gLogger.setLevel("ERROR")
    self.whiz.workflow_commons = dict(IS_PROD=True, PRODUCTION_ID=6666, JOB_ID=123)
    self.whiz.resolveInputVariables()
    self.whiz.applicationSpecificInputs()
    self.assertEqual( self.whiz.randomSeed, 6666123 )

  @patch.dict(os.environ, {"JOBID": "12345"} )
  def test_Whizard2_ASI_RandomSeed_Set( self ):
    """Whizard.applicationSpecificInputs: check setting of default randomseed in user jobs............"""
    gLogger.setLevel("ERROR")
    self.whiz = Whizard2Analysis()
    self.whiz.workflow_commons = dict()
    self.whiz.resolveInputVariables()
    self.whiz.applicationSpecificInputs()
    self.assertEqual( int(self.whiz.randomSeed), 12345 )

  @patch.dict(os.environ, {"JOBID": "12345"} )
  def test_Whizard2_ASI_RandomSeed_User( self ):
    """Whizard.applicationSpecificInputs: check setting of randomseed in user jobs...................."""
    gLogger.setLevel("ERROR")
    self.whiz = Whizard2Analysis()
    self.whiz.randomSeed = 654321
    self.whiz.workflow_commons = dict()
    self.whiz.resolveInputVariables()
    self.whiz.applicationSpecificInputs()
    self.assertEqual( int(self.whiz.randomSeed), 654321 )

  @patch.dict(os.environ, {"JOBID": "12345"} )
  def test_Whizard2_ASI_RandomSeed_User_Zero( self ):
    """Whizard.applicationSpecificInputs: check setting of randomseed to zero in user jobs............"""
    gLogger.setLevel("ERROR")
    self.whiz = Whizard2Analysis()
    self.whiz.randomSeed = 0
    self.whiz.workflow_commons = dict()
    self.whiz.resolveInputVariables()
    self.whiz.applicationSpecificInputs()
    self.assertEqual( int(self.whiz.randomSeed), 0)

def runTests():
  """Runs our tests"""
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestWhizard2Analysis )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestWhizard2AnalysisRunit ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestWhizard2AnalysisASI ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  print testResult

if __name__ == '__main__':
  runTests()
