#!/usr/bin/env python
"""Test the DDSim WorkflowModule"""
__RCSID__ = "$Id$"

#pylint: disable=R0904, W0212

import unittest
import os
import shutil
import tempfile
import tarfile
from zipfile import ZipFile
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

@patch("ILCDIRAC.Workflow.Modules.ModuleBase.getProxyInfoAsString", new=Mock(return_value=S_OK()))
@patch("DIRAC.Core.Security.ProxyInfo.getProxyInfoAsString", new=Mock(return_value=S_OK()))
class TestDDSimAnalysis( unittest.TestCase ):
  """ test DDSimAnalysis """

  @patch("ILCDIRAC.Workflow.Modules.ModuleBase.getProxyInfoAsString", new=Mock(return_value=S_OK()))
  @patch("DIRAC.Core.Security.ProxyInfo.getProxyInfoAsString", new=Mock(return_value=S_OK()))
  def setUp( self ):
    self.ddsim = DDSimAnalysis()
    self.curdir = os.getcwd()
    self.tempdir = tempfile.mkdtemp("", dir = "./")
    os.chdir(self.tempdir)

  def tearDown( self ):
    os.chdir(self.curdir)
    cleanup(self.tempdir)

  @patch("ILCDIRAC.Workflow.Modules.ModuleBase.getProxyInfoAsString", new=Mock(return_value=S_OK()))
  @patch("DIRAC.Core.Security.ProxyInfo.getProxyInfoAsString", new=Mock(return_value=S_OK()))
  def test_DDSim_init( self ):
    """test initialisation only ...................................................................."""
    self.assertTrue( self.ddsim.enable )

class TestDDSimAnalysisEnv( TestDDSimAnalysis ):
  """ test DDSim getEnvScript """

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
    with patch("os.path.exists", new=Mock(return_value=True ) ):
      res = self.ddsim.getEnvScript( platform, appname, appversion )
    self.assertEqual( res['Value'], os.path.abspath("DDSimEnv.sh") )
    self.assertTrue( os.path.exists(os.path.abspath("DDSimEnv.sh")) )


class TestDDSimAnalysisASI( TestDDSimAnalysis ):
  """test DDSim ApplicationSpecificInputs """

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

class TestDDSimAnalysisDetXMLCS( TestDDSimAnalysis ):
  """tests for _getDetectorXML """

  @patch("ILCDIRAC.Workflow.Modules.DDSimAnalysis.getSoftwareFolder", new=Mock(return_value=S_OK("/win32") ) )
  def test_DDSim_getDetectorXML( self ):
    """DDSim.applicationSpecificInputs: getDetectorXML from CS......................................"""
    gLogger.setLevel("ERROR")
    xmlPath = "/path/to/camelot.xml"
    self.ddsim.detectorModel = "camelot"
    self.ddsim.ops.getOptionsDict = Mock( return_value = S_OK( dict(camelot=xmlPath ) ) )
    self.ddsim.workflow_commons = dict()
    res = self.ddsim._getDetectorXML()
    self.assertEqual( res['Value'], xmlPath )

class TestDDSimAnalysisDetXMLTar( TestDDSimAnalysis ):
  """tests for _getDetectorXML """
  def setUp( self ):
    super(TestDDSimAnalysisDetXMLTar, self).setUp()
    self.ddsim.detectorModel = "myDet"
    outputFilename = self.ddsim.detectorModel+".tar.gz"
    os.makedirs(self.ddsim.detectorModel)
    xmlPath = os.path.join(self.ddsim.detectorModel,self.ddsim.detectorModel+".xml")
    with open(xmlPath, "w") as xml:
      xml.write("myDet is awesome")
    with tarfile.open(outputFilename, "w:gz") as tar:
      tar.add(xmlPath)
    cleanup(self.ddsim.detectorModel)

  @patch("ILCDIRAC.Workflow.Modules.DDSimAnalysis.getSoftwareFolder", new=Mock(return_value=S_OK("/win32") ) )
  def test_DDSim_getDetectorXML_Local_TarGZ( self ):
    """DDSim.applicationSpecificInputs: getDetectorXML with local tar.gz............................"""
    gLogger.setLevel("ERROR")
    self.ddsim.detectorModel = "myDet"
    self.ddsim.ops.getOptionsDict = Mock( return_value = S_OK( dict(camelot="/dev/null" ) ) )
    self.ddsim.workflow_commons = dict()
    res = self.ddsim._getDetectorXML()
    gLogger.error( " res " , res )
    expectedPath = os.path.join(os.getcwd(), self.ddsim.detectorModel, self.ddsim.detectorModel+".xml" )
    self.assertEqual( res['Value'], expectedPath )
    self.assertTrue( os.path.exists( expectedPath ) )

  @patch("ILCDIRAC.Workflow.Modules.DDSimAnalysis.getSoftwareFolder", new=Mock(return_value=S_OK("/win32") ) )
  def test_DDSim_getDetectorXML_Local_TarGZ_2( self ):
    """DDSim.applicationSpecificInputs: getDetectorXML with local tar.gz run twice.................."""
    gLogger.setLevel("ERROR")
    self.ddsim.detectorModel = "myDet"
    self.ddsim.ops.getOptionsDict = Mock( return_value = S_OK( dict(camelot="/dev/null" ) ) )
    self.ddsim.workflow_commons = dict()
    res = self.ddsim._extractTar()
    res = self.ddsim._extractTar()
    gLogger.error( " res " , res )
    expectedPath = os.path.join(os.getcwd(), self.ddsim.detectorModel, self.ddsim.detectorModel+".xml" )
    self.assertEqual( res['Value'], expectedPath )
    self.assertTrue( os.path.exists( expectedPath ) )

  @patch("ILCDIRAC.Workflow.Modules.DDSimAnalysis.getSoftwareFolder", new=Mock(return_value=S_OK("/win32") ) )
  def test_DDSim_getDetectorXML_Local_TGZ_2( self ):
    """DDSim.applicationSpecificInputs: getDetectorXML with local tgz..............................."""
    gLogger.setLevel("ERROR")
    self.ddsim.detectorModel = "myDet"
    self.ddsim.ops.getOptionsDict = Mock( return_value = S_OK( dict(camelot="/dev/null" ) ) )
    self.ddsim.workflow_commons = dict()
    os.rename( "myDet.tar.gz", "myDet.tgz" )
    res = self.ddsim._getDetectorXML()
    gLogger.error( " res " , res )
    expectedPath = os.path.join(os.getcwd(), self.ddsim.detectorModel, self.ddsim.detectorModel+".xml" )
    self.assertEqual( res['Value'], expectedPath )
    self.assertTrue( os.path.exists( expectedPath ) )

class TestDDSimAnalysisDetXMLZip( TestDDSimAnalysis ):
  """tests for _getDetectorXML when a zip file exists"""

  def setUp( self ):
    super(TestDDSimAnalysisDetXMLZip, self).setUp()
    self.ddsim.detectorModel = "myDet"
    outputFilename = self.ddsim.detectorModel+".zip"
    os.makedirs(self.ddsim.detectorModel)
    xmlPath = os.path.join(self.ddsim.detectorModel,self.ddsim.detectorModel+".xml")
    with open(xmlPath, "w") as xml:
      xml.write("myDet is awesome")
    with ZipFile(outputFilename, "w") as zipF:
      zipF.write(xmlPath)
    cleanup(self.ddsim.detectorModel)

  @patch("ILCDIRAC.Workflow.Modules.DDSimAnalysis.getSoftwareFolder", new=Mock(return_value=S_OK("/win32") ) )
  def test_DDSim_getDetectorXML_Local_TarGZ( self ):
    """DDSim.applicationSpecificInputs: getDetectorXML with local zip..............................."""
    gLogger.setLevel("ERROR")
    self.ddsim.detectorModel = "myDet"
    self.ddsim.ops.getOptionsDict = Mock( return_value = S_OK( dict(camelot="/dev/null" ) ) )
    self.ddsim.workflow_commons = dict()
    res = self.ddsim._getDetectorXML()
    gLogger.error( " res " , res )
    expectedPath = os.path.join(os.getcwd(), self.ddsim.detectorModel, self.ddsim.detectorModel+".xml" )
    self.assertEqual( res['Value'], expectedPath )
    self.assertTrue( os.path.exists( expectedPath ) )

  @patch("ILCDIRAC.Workflow.Modules.DDSimAnalysis.getSoftwareFolder", new=Mock(return_value=S_OK("/win32") ) )
  def test_DDSim_getDetectorXML_Local_TarGZ_2( self ):
    """DDSim.applicationSpecificInputs: getDetectorXML with local zip run twice....................."""
    gLogger.setLevel("ERROR")
    self.ddsim.detectorModel = "myDet"
    self.ddsim.ops.getOptionsDict = Mock( return_value = S_OK( dict(camelot="/dev/null" ) ) )
    self.ddsim.workflow_commons = dict()
    res = self.ddsim._extractZip()
    res = self.ddsim._extractZip()
    gLogger.error( " res " , res )
    expectedPath = os.path.join(os.getcwd(), self.ddsim.detectorModel, self.ddsim.detectorModel+".xml" )
    self.assertEqual( res['Value'], expectedPath )
    self.assertTrue( os.path.exists( expectedPath ) )

def runTests():
  """Runs our tests"""
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestDDSimAnalysis )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestDDSimAnalysisDetXMLTar ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestDDSimAnalysisDetXMLZip ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestDDSimAnalysisDetXMLCS ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestDDSimAnalysisEnv ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestDDSimAnalysisASI ) )

  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  print testResult

if __name__ == '__main__':
  runTests()
