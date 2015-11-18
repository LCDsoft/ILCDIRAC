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

  def test_init( self ):
    """test initialisation only ...................................................................."""
    self.assertTrue( self.ddsim.enable )

  @patch("ILCDIRAC.Workflow.Modules.DDSimAnalysis.getSoftwareFolder", new=Mock(return_value=S_OK("/win32") ) )
  def test_getEnvScript_success( self ):
    """test getEnvScript success...................................................................."""
    platform = "Windows"
    appname = "ddsim"
    appversion = "Vista"
    res = self.ddsim.getEnvScript( platform, appname, appversion )
    self.assertEqual( res['Value'], os.path.abspath("DDSimEnv.sh") )
    self.assertTrue( os.path.exists(os.path.abspath("DDSimEnv.sh")) )

  @patch("ILCDIRAC.Workflow.Modules.DDSimAnalysis.getSoftwareFolder", new=Mock(return_value=S_OK("/win32") ) )
  @patch("os.path.exists", new=Mock(return_value=True ) )
  def test_getEnvScript_vars( self ):
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
  def test_getEnvScript_vars2( self ):
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

def runTests():
  """Runs our tests"""
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestDDSimAnalysis )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  print testResult

if __name__ == '__main__':
  runTests()
