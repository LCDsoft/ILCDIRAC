#!/usr/bin/env python
"""Test the OverlayInput WorkflowModule"""

import os
import shutil
import tempfile
import unittest
from mock import patch, MagicMock as Mock

from DIRAC import gLogger, S_OK
from ILCDIRAC.Workflow.Modules.OverlayInput import OverlayInput

__RCSID__ = "$Id$"

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

def createFile( *_args, **_kwargs ):
  """create a file with filename if given """
  with open("overlayFile.slcio", "w") as oFile:
    oFile.write("Somecontent")

@patch("ILCDIRAC.Workflow.Modules.ModuleBase.getProxyInfoAsString", new=Mock(return_value=S_OK()))
@patch("DIRAC.Core.Security.ProxyInfo.getProxyInfoAsString", new=Mock(return_value=S_OK()))
@patch("ILCDIRAC.Workflow.Modules.OverlayInput.FileCatalogClient", new=Mock(return_value=S_OK()))
@patch("ILCDIRAC.Workflow.Modules.OverlayInput.Operations", new=Mock(return_value=S_OK()))
@patch("ILCDIRAC.Workflow.Modules.OverlayInput.RPCClient", new=Mock(return_value=S_OK()))
@patch("ILCDIRAC.Workflow.Modules.OverlayInput.DataManager", new=Mock(return_value=S_OK()))
class TestOverlayEos( unittest.TestCase ):
  """ test Getting Overlay files from CERN EOS

  Make sure the copying command is properly formated and uses the correct path to the eos instance
  """

  def assertIn(self, *args, **kwargs):
    """make this existing to placate pylint"""
    return super(TestOverlayEos, self).assertIn(*args, **kwargs)


  def setUp( self ):
    self.tmpdir = tempfile.mkdtemp("", dir = "./")
    os.chdir(self.tmpdir)
    self.over = OverlayInput()
    self.over.applicationLog = "testOver.log"

  def tearDown( self ):
    os.chdir("../")
    cleanup(self.tmpdir)

  @patch("ILCDIRAC.Workflow.Modules.OverlayInput.shellCall", new=Mock(side_effect=createFile))
  def test_overlayinput_getEosFile_lfn_success( self ):
    """ test success when getting an lfn to copy from eos """
    testLFN = "/lfn/to/overlay/overlayFile.slcio"
    res = self.over.getEOSFile( testLFN )
    print res
    print "self result", self.over.result
    self.assertTrue( res['OK'] )
    self.assertEqual( os.path.basename( testLFN ), res['Value'] )
    with open("overlayinput.sh") as overscript:
      self.assertIn( "xrdcp -s root://eospublic.cern.ch//eos/clicdp/grid%s" % testLFN , overscript.read() )

  @patch("ILCDIRAC.Workflow.Modules.OverlayInput.shellCall", new=Mock(side_effect=createFile))
  def test_overlayinput_getEosFile_fullpath_success( self ):
    """ test that we don't predent if we get a fullpath for eos, however that might happen"""
    testLFN = "/eos/clicdp/grid/lfn/to/overlay/overlayFile.slcio"
    res = self.over.getEOSFile( testLFN )
    print res
    print "self result", self.over.result
    self.assertTrue( res['OK'] )
    self.assertEqual( os.path.basename( testLFN ), res['Value'] )
    with open("overlayinput.sh") as overscript:
      self.assertIn( "xrdcp -s root://eospublic.cern.ch/%s" % testLFN , overscript.read() )

  @patch("ILCDIRAC.Workflow.Modules.OverlayInput.shellCall", new=Mock())
  def test_overlayinput_getEosFile_lfn_failure( self ):
    """ test failure of copy command, that is no ouputfile present after copying """
    testLFN = "/lfn/to/overlay/overlayFile.slcio"
    res = self.over.getEOSFile( testLFN )
    print res
    print "self result", self.over.result
    self.assertFalse( res['OK'] )
    self.assertEqual( "Failed", res['Message'] )
    with open("overlayinput.sh") as overscript:
      self.assertIn( "xrdcp -s root://eospublic.cern.ch//eos/clicdp/grid%s" % testLFN , overscript.read() )


def runTests():
  """Runs our tests"""
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestOverlayEos )

  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  print testResult

if __name__ == '__main__':
  runTests()
