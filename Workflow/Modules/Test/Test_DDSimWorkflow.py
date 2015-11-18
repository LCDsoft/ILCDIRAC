#!/usr/bin/env python
"""Test the DDSim WorkflowModule"""
__RCSID__ = "$Id$"

import unittest
import os
import shutil
import tempfile

from mock import patch, MagicMock as Mock

from DIRAC import gLogger, S_OK, S_ERROR
from ILCDIRAC.Workflow.Modules.DDSimAnalysis import DDSimAnalysis

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

class TestDDSimAnalysis( unittest.TestCase ):
  """ test DDSimAnalysis """
  def setUp( self ):
    self.ddsim = DDSimAnalysis()
    self.curdir = os.getcwd()
    self.tempdir = tempfile.mkdtemp("", dir = "./")
    os.chdir(self.tempdir)

  def tearDown( self ):
    os.chdir(self.curdir)
    cleanup(self.tempdir)

  def test_DDSim_init( self ):
    """test initialisation only ...................................................................."""
    self.assertTrue( self.ddsim.enable )

  @patch("ILCDIRAC.Workflow.Modules.DDSimAnalysis.getSoftwareFolder", new=Mock(return_value=S_OK("/win32") ) )
  def test_DDSim_getEnvScript_success( self ):
    """test getEnvScript success...................................................................."""
    platform = "Windows"
    appname = "ddsim"
    appversion = "Vista"
    res = self.ddsim.getEnvScript( platform, appname, appversion )
    self.assertEqual( res['Value'], os.path.abspath("DDSimEnv.sh") )
    self.assertTrue( os.path.exists(os.path.abspath("DDSimEnv.sh")) )

  @patch("ILCDIRAC.Workflow.Modules.DDSimAnalysis.getSoftwareFolder", new=Mock(return_value=S_OK("/win32") ) )
  @patch("os.path.exists", new=Mock(return_value=True ) )
  def test_DDSim_getEnvScript_vars( self ):
    """test getEnvScript with variables success....................................................."""

    platform = "Windows"
    appname = "ddsim"
    appversion = "Vista"
    self.ddsim.ops.getOptionsDict = Mock( return_value = S_OK( dict(KNIGHTSWORD="Ni",
                                                                    WHEN="Always"
                                                                  )
                                                             )
                                        )
    res = self.ddsim.getEnvScript( platform, appname, appversion )
    self.assertEqual( res['Value'], os.path.abspath("DDSimEnv.sh") )
    self.assertTrue( os.path.exists(os.path.abspath("DDSimEnv.sh")) )

  @patch("ILCDIRAC.Workflow.Modules.DDSimAnalysis.getSoftwareFolder", new=Mock(return_value=S_OK("/win32") ) )
  @patch("ILCDIRAC.Workflow.Modules.DDSimAnalysis.getNewLDLibs", new=Mock(return_value="") )
  @patch("os.path.exists", new=Mock(return_value=True ) )
  def test_DDSim_getEnvScript_vars2( self ):
    """test getEnvScript with variables success 2..................................................."""
    platform = "Windows"
    appname = "ddsim"
    appversion = "Vista"
    self.ddsim.ops.getOptionsDict = Mock( return_value = S_OK( dict(KNIGHTSWORK="Ni",
                                                                    WHEN="Always"
                                                                  )
                                                             )
                                        )
    res = self.ddsim.getEnvScript( platform, appname, appversion )
    self.assertEqual( res['Value'], os.path.abspath("DDSimEnv.sh") )
    self.assertTrue( os.path.exists(os.path.abspath("DDSimEnv.sh")) )



  @patch.dict(os.environ, {"JOBID": "12345"} )
  def test_DDSim_ASI_NoVariables( self ):
    """DDSim.applicationSpecificInputs: checks that no variables have been set after this call......"""
    gLogger.setLevel("ERROR")
    self.ddsim.workflow_commons = dict()
    self.ddsim.applicationSpecificInputs()
    self.assertFalse( self.ddsim.jobReport or self.ddsim.productionID )

  @patch.dict(os.environ, {"JOBID": "12345"} )
  def test_DDSim_ASI_StartFrom( self ):
    """DDSim.applicationSpecificInputs: check setting of startfrom variable........................."""
    gLogger.setLevel("ERROR")
    self.ddsim.workflow_commons = dict(StartFrom=321)
    self.ddsim.resolveInputVariables()
    self.ddsim.applicationSpecificInputs()
    self.assertEqual( self.ddsim.startFrom, 321 )

  @patch.dict(os.environ, {"JOBID": "12345"} )
  def test_DDSim_ASI_RandomSeed_Prod( self ):
    """DDSim.applicationSpecificInputs: check setting of randomseed in production..................."""
    gLogger.setLevel("ERROR")
    self.ddsim.workflow_commons = dict(StartFrom=321, IS_PROD=True, PRODUCTION_ID=6666, JOB_ID=123)
    self.ddsim.resolveInputVariables()
    self.ddsim.applicationSpecificInputs()
    self.assertEqual( self.ddsim.randomSeed, 6666123 )


  @patch.dict(os.environ, {"JOBID": "12345"} )
  def test_DDSim_ASI_RandomSeed_Set( self ):
    """DDSim.applicationSpecificInputs: check setting of default randomseed in user jobs............"""
    gLogger.setLevel("ERROR")
    self.ddsim = DDSimAnalysis()
    self.ddsim.workflow_commons = dict()
    self.ddsim.resolveInputVariables()
    self.ddsim.applicationSpecificInputs()
    self.assertEqual( int(self.ddsim.randomSeed), 12345 )

  @patch.dict(os.environ, {"JOBID": "12345"} )
  def test_DDSim_ASI_RandomSeed_User( self ):
    """DDSim.applicationSpecificInputs: check setting of randomseed in user jobs...................."""
    gLogger.setLevel("ERROR")
    self.ddsim = DDSimAnalysis()
    self.ddsim.randomSeed = 654321
    self.ddsim.workflow_commons = dict()
    self.ddsim.resolveInputVariables()
    self.ddsim.applicationSpecificInputs()
    self.assertEqual( int(self.ddsim.randomSeed), 654321 )

  @patch.dict(os.environ, {"JOBID": "12345"} )
  def test_DDSim_ASI_RandomSeed_User_Zero( self ):
    """DDSim.applicationSpecificInputs: check setting of randomseed to zero in user jobs............"""
    gLogger.setLevel("ERROR")
    self.ddsim = DDSimAnalysis()
    self.ddsim.randomSeed = 0
    self.ddsim.workflow_commons = dict()
    self.ddsim.resolveInputVariables()
    self.ddsim.applicationSpecificInputs()
    self.assertEqual( int(self.ddsim.randomSeed), 0)

  @patch.dict(os.environ, {"JOBID": "12345"} )
  def test_DDSim_ASI_InputFiles( self ):
    """DDSim.applicationSpecificInputs: check setting inputData....................................."""
    gLogger.setLevel("ERROR")
    self.ddsim = DDSimAnalysis()
    self.ddsim.InputData = ["myslcio.slcio","mystdhep.HEPEvt","my.notforsimulation"]
    self.ddsim.workflow_commons = dict()
    self.ddsim.resolveInputVariables()
    self.ddsim.applicationSpecificInputs()
    self.assertEqual( self.ddsim.InputFile, ["myslcio.slcio","mystdhep.HEPEvt"] )

def runTests():
  """Runs our tests"""
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestDDSimAnalysis )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  print testResult

if __name__ == '__main__':
  runTests()
