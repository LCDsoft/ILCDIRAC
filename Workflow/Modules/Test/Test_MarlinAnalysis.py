"""
Unit tests for the MarlinAnalysis.py file
"""

import unittest
from mock import patch, MagicMock as Mock
from ILCDIRAC.Workflow.Modules.MarlinAnalysis import MarlinAnalysis
from DIRAC import S_OK, S_ERROR


expected_diff_err = "Expected different error message"


# TODO: add case for undefined steering file
class MarlinAnalysisTestCase( unittest.TestCase ):
  """ Base class for the ProductionJob test cases
  """

  def setUp(self):
    """set up the objects"""
    self.marAna = MarlinAnalysis()
    self.marAna.OutputFile = ""

  def tearDown(self):
    del self.marAna


  def test_resolveinput_productionjob1( self ):
    self.marAna.workflow_commons[ "IS_PROD" ] = True
    outputfile1 = "/dir/a_REC_.a"
    outputfile2 = "/otherdir/b_DST_.b"
    inputfile = "/inputdir/input_SIM_.i"
    self.marAna.workflow_commons[ "ProductionOutputData" ] = ";".join([outputfile1, outputfile2, inputfile])
    self.assertEquals(S_OK("Parameters resolved"), self.marAna.applicationSpecificInputs())
    self.assertEquals((self.marAna.outputREC, self.marAna.outputDST, self.marAna.InputFile), ("a_REC_.a", "b_DST_.b", ["input_SIM_.i"]))

  def test_resolveinput_productionjob2( self ):
    self.marAna.workflow_commons[ "IS_PROD" ] = False
    self.marAna.workflow_commons[ "PRODUCTION_ID" ] = "123"
    self.marAna.workflow_commons[ "JOB_ID" ] = 456
    
    self.assertEquals(S_OK("Parameters resolved"), self.marAna.applicationSpecificInputs())

    
  def test_resolveinput_productionjob3( self ):
    self.marAna.workflow_commons[ "IS_PROD" ] = True

    self.marAna.OutputFile = "c.c"
    self.marAna.InputFile = []
    inputlist = ["a.slcio", "b.slcio", "c.exe"]
    self.marAna.InputData = inputlist
    
    self.assertEquals(S_OK("Parameters resolved"), self.marAna.applicationSpecificInputs())
    self.assertEquals([inputlist[0], inputlist[1]], self.marAna.InputFile)

  def test_runit_noplatform( self ):
    self.marAna.platform = None
    self.assertFalse( self.marAna.runIt()['OK'] )

  def test_runit_noapplog( self ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = None
    self.assertFalse( self.marAna.runIt()['OK'] )

  def test_runit_workflowbad( self ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.workflowStatus = { 'OK' : False }
    result = self.marAna.runIt()
    self.assertTrue(result['OK'])
    self.assertIn("should not proceed", result['Value'].lower(), expected_diff_err)

  def test_runit_stepbad( self ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = { 'OK' : False }
    result = self.marAna.runIt()
    self.assertTrue(result['OK'])
    self.assertIn("should not proceed", result['Value'].lower(), expected_diff_err)

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getEnvironmentScript", new=Mock(return_value=S_ERROR("failed to get env script")))
  def test_runit_getEnvScriptFails( self ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    result = self.marAna.runIt()
    self.assertFalse( result['OK'])
    msg = result['Message'].lower()
    self.assertIn('env script', msg, expected_diff_err)

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getEnvironmentScript", new=Mock(return_value=S_OK('Testpath123')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.GetInputFiles", new=Mock(return_value=S_ERROR()))
  def test_runit_getInputFilesFails( self ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    result = self.marAna.runIt()
    self.assertFalse( result['OK'] )

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getEnvironmentScript", new=Mock(return_value=S_OK('Testpath123')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.GetInputFiles", new=Mock(return_value=S_OK("testinputfiles")))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getSteeringFileDirName", new=Mock(return_value=S_OK('testdir')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.os.path.exists", new=Mock(side_effect=[False, True, False, True, False, True, False, False]))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.shutil.copy", new=Mock())
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.prepareXMLFile", new=Mock(return_value=S_OK('testdir')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.prepareMARLIN_DLL", new=Mock(return_value=S_OK('testdir')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.runMarlin", new=Mock(return_value=S_OK('testdir')))
  def test_runit_handlepandorasettings_no_log( self ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    result = self.marAna.runIt()
    # TODO: add assertion shutil.copy was called on the pandorasettings.xml
    self.assertFalse(result['OK'])
    self.assertIn('not produce the expected log', result['Message'].lower(), expected_diff_err)
    

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getEnvironmentScript", new=Mock(return_value=S_OK('Testpath123')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.GetInputFiles", new=Mock(return_value=S_OK("testinputfiles")))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getSteeringFileDirName", new=Mock(return_value=S_OK('testdir')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.os.path.exists", new=Mock(side_effect=[False, True, False, True, False, True, False]))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.shutil.copy", new=Mock())
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.prepareXMLFile", new=Mock(return_value=S_ERROR('prepxml')))
  def test_runit_xmlgenerationfails( self ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    result = self.marAna.runIt()
    self.assertFalse(result['OK'])
    self.assertIn('prepxml', result['Message'].lower(), expected_diff_err)

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getEnvironmentScript", new=Mock(return_value=S_OK('Testpath123')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.GetInputFiles", new=Mock(return_value=S_OK("testinputfiles")))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getSteeringFileDirName", new=Mock(return_value=S_OK('testdir')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.os.path.exists", new=Mock(side_effect=[False, True, False, True, False, True, False]))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.shutil.copy", new=Mock())
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.prepareXMLFile", new=Mock(return_value=S_OK('testdir')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.prepareMARLIN_DLL", new=Mock(return_value=S_ERROR('mardll')))
  def test_runit_marlindllfails( self ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    result = self.marAna.runIt()
    self.assertFalse(result['OK'])
    self.assertIn('wrong with software installation', result['Message'].lower(), expected_diff_err)

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getEnvironmentScript", new=Mock(return_value=S_OK('Testpath123')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.GetInputFiles", new=Mock(return_value=S_OK("testinputfiles")))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getSteeringFileDirName", new=Mock(return_value=S_OK('testdir')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.os.path.exists", new=Mock(side_effect=[False, True, False, True, False, True, False]))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.shutil.copy", new=Mock())
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.prepareXMLFile", new=Mock(return_value=S_OK('testdir')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.prepareMARLIN_DLL", new=Mock(return_value=S_OK('testdir')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.runMarlin", new=Mock(return_value=S_ERROR('marlin failed')))
  def test_runit_runmarlinfails( self ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    result = self.marAna.runIt()
    # TODO: add assertion shutil.copy was called on the pandorasettings.xml
    self.assertFalse(result['OK'])
    self.assertIn('failed to run', result['Message'].lower(), expected_diff_err)

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getEnvironmentScript", new=Mock(return_value=S_OK('Testpath123')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.GetInputFiles", new=Mock(return_value=S_OK("testinputfiles")))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getSteeringFileDirName", new=Mock(return_value=S_OK('testdir')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.os.path.exists", new=Mock(side_effect=[False, True, False, True, True, True, True, True]))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.shutil.copy", new=Mock())
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.prepareXMLFile", new=Mock(return_value=S_OK('testdir')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.prepareMARLIN_DLL", new=Mock(return_value=S_OK('testdir')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.runMarlin", new=Mock(return_value=S_OK([""])))
  def test_runit_complete( self ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    result = self.marAna.runIt()
    # TODO: add assertion shutil.copy was called on the pandorasettings.xml
    self.assertTrue(result['OK'])

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getSoftwareFolder", new=Mock(return_value=S_ERROR('')))
  def test_getenvscript_getsoftwarefolderfails( self ):
    self.assertFalse(self.marAna.getEnvScript(None, None, None)['OK'])

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getSoftwareFolder", new=Mock(return_value=S_OK('')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.removeLibc", new=Mock(return_value=None))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getNewLDLibs", new=Mock(return_value=None))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.os.path.exists", new=Mock(return_value=False))
  def test_getenvscript_pathexists( self ):
    result = self.marAna.getEnvScript(None, None, None)
    self.assertFalse(result['OK'])
    self.assertIn('marlin_dll folder not found', result['Message'].lower(), expected_diff_err)

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getSoftwareFolder", new=Mock(return_value=S_OK('')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.removeLibc", new=Mock(return_value=None))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getNewLDLibs", new=Mock(return_value=None))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.os.path.exists", new=Mock(return_value=True))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.os.listdir", new=Mock(return_value=['testdir']))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.open", new=Mock())
  def test_getenvscript( self ):
    result = self.marAna.getEnvScript(None, None, None)
    self.assertTrue(result['OK'])

  def test_getinputfiles_ignore( self ):
    self.marAna.ignoremissingInput = True
    self.assertTrue(self.marAna.GetInputFiles()['OK'])

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.resolveIFpaths", new=Mock(return_value=S_ERROR()))
  def test_getinputfiles_resolvepathsfails( self ):
    res = self.marAna.GetInputFiles()
    self.assertFalse(res['OK'])
    self.assertIn('missing slcio', res['Message'].lower(), expected_diff_err)

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.resolveIFpaths", new=Mock(return_value=S_OK(['1.slcio', '2.slcio'])))
  def test_getinputfiles_complete( self ):
    res = self.marAna.GetInputFiles()
    self.assertTrue(res['OK'])
    print res['Value']

    
    
    
    
  

