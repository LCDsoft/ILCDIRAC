"""
Unit tests for the MarlinAnalysis.py file
"""

import unittest
import os
from mock import mock_open, patch, MagicMock as Mock
from ILCDIRAC.Workflow.Modules.MarlinAnalysis import MarlinAnalysis
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertInImproved, assertDiracFailsWith, assertDiracSucceeds, assertDiracSucceedsWith, assertDiracSucceedsWith_equals
from DIRAC import S_OK, S_ERROR

class MarlinAnalysisFixture( object ):
  """ Contains the commonly used setUp and tearDown methods of the Tests"""
  def setUp( self ):
    """set up the objects"""
    self.marAna = MarlinAnalysis()
    self.marAna.OutputFile = ""
    self.marAna.ignoremissingInput = False

  def tearDown( self ):
    """Clean up test objects"""
    del self.marAna

# TODO: add case for undefined steering file
class MarlinAnalysisTestCase( MarlinAnalysisFixture, unittest.TestCase ):
  """ Base class for the ProductionJob test cases
  """

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
    self.assertFalse(self.marAna.InputFile or self.marAna.InputData)

    
  def test_resolveinput_productionjob3( self ):
    self.marAna.workflow_commons[ "IS_PROD" ] = True

    self.marAna.OutputFile = "c.c"
    self.marAna.InputFile = []
    inputlist = ["a.slcio", "b.slcio", "c.exe"]
    self.marAna.InputData = inputlist
    
    self.assertEquals(S_OK("Parameters resolved"), self.marAna.applicationSpecificInputs())
    self.assertEquals([inputlist[0], inputlist[1]], self.marAna.InputFile)
    
  def test_resolveinput_productionjob4( self ):
    self.marAna.workflow_commons[ "IS_PROD" ] = True
    self.marAna.OutputFile = ''
    prodId = "123"
    self.marAna.workflow_commons[ "PRODUCTION_ID" ] = prodId
    jobId = "456"
    self.marAna.workflow_commons[ "JOB_ID" ] = jobId
    self.marAna.workflow_commons.pop('ProductionOutputData', None) #default value needed, else maybe KeyError
    self.marAna.InputFile = 'in3.slcio'
    self.marAna.outputREC = 'out1.stdhep'
    self.marAna.outputDST = 'test2.stdhep'
    
    self.assertEquals(S_OK("Parameters resolved"), self.marAna.applicationSpecificInputs())
    files = [self.marAna.outputREC, self.marAna.outputDST, self.marAna.InputFile[0]]
    for filename in files:
      # TODO: check for file extension, differentiate 'test'/'test2' in filename...
      self.assertInMultiple([prodId,jobId], filename)
    self.assertInMultiple(['in3', '.slcio'], self.marAna.InputFile[0])
    self.assertInMultiple(['out1', '.stdhep'], self.marAna.outputREC)
    self.assertInMultiple(['test2', '.stdhep'], self.marAna.outputDST)
      
  def test_runit_noplatform( self ):
    self.marAna.platform = None
    result = self.marAna.runIt()
    assertDiracFailsWith( result, 'no ilc platform selected', self )

  def test_runit_noapplog( self ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = None
    result = self.marAna.runIt()
    assertDiracFailsWith( result, 'no log', self )

  def test_runit_workflowbad( self ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.workflowStatus = { 'OK' : False }
    result = self.marAna.runIt()
    assertDiracSucceedsWith( result, 'should not proceed', self )

  def test_runit_stepbad( self ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = { 'OK' : False }
    result = self.marAna.runIt()
    assertDiracSucceedsWith( result, 'should not proceed', self )

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis._getDetectorXML",
         new=Mock(return_value=S_ERROR("zis iz not camelot")))
  def test_runit_detXMLFails( self ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = { 'OK' : True }
    self.marAna.detectorModel = "notCamelot.xml"
    result = self.marAna.runIt()
    assertDiracFailsWith( result, "zis iz not camelot", self )

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getEnvironmentScript", new=Mock(return_value=S_ERROR("failed to get env script")))
  def test_runit_getEnvScriptFails( self ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    result = self.marAna.runIt()
    assertDiracFailsWith( result, 'env script', self )

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getEnvironmentScript", new=Mock(return_value=S_OK('Testpath123')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.GetInputFiles", new=Mock(return_value=S_ERROR('missing slcio file')))
  def test_runit_getInputFilesFails( self ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    result = self.marAna.runIt()
    #Only checks that the patch annotation is passed as the result
    assertDiracFailsWith( result, 'missing slcio file', self )

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getSoftwareFolder", new=Mock(return_value=S_ERROR('')))
  def test_getenvscript_getsoftwarefolderfails( self ):
    self.assertFalse(self.marAna.getEnvScript(None, None, None)['OK'])

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getSoftwareFolder", new=Mock(return_value=S_OK('')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.removeLibc", new=Mock(return_value=None))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getNewLDLibs", new=Mock(return_value=None))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.os.path.exists", new=Mock(return_value=False))
  def test_getenvscript_pathexists( self ):
    result = self.marAna.getEnvScript(None, None, None)
    assertDiracFailsWith( result, 'marlin_dll folder not found', self )

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getSoftwareFolder", new=Mock(return_value=S_OK('aFolder')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.removeLibc", new=Mock(return_value=None))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getNewLDLibs", new=Mock(return_value='bFolder'))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.os.path.exists", new=Mock(return_value=True))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.os.listdir", new=Mock(return_value=['testdir']))
  def test_getenvscript( self ):
    moduleName = "ILCDIRAC.Workflow.Modules.MarlinAnalysis"
    file_contents = []
    text_file_data = '\n'.join(file_contents)
    result = {}
    with patch('%s.open' % moduleName, mock_open(read_data=text_file_data), create=True) as file_mocker:
      result = self.marAna.getEnvScript(None, None, None)
    assertDiracSucceedsWith( result, '/MarlinEnv.sh', self )
    check_in_script = [ 'declare -x PATH=aFolder/Executable:$PATH\n', 'declare -x ROOTSYS=aFolder/ROOT\n', 'declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:aFolder/LDLibs:bFolder\n', "declare -x MARLIN_DLL=aFolder/MARLIN_DLL/testdir:\n", "declare -x PANDORASETTINGS=aFolder/Settings/PandoraSettings.xml" ]
    file_mocker.assert_called_with('MarlinEnv.sh', 'w')
    mocker_handle = file_mocker()
    for expected in check_in_script:
      mocker_handle.write.assert_any_call(expected)
    self.assertEquals(len(check_in_script), mocker_handle.__enter__.return_value.write.call_count - 4)

  def test_getinputfiles_ignore( self ):
    self.marAna.ignoremissingInput = True
    assertDiracSucceeds( self.marAna.GetInputFiles(), self )

  def test_getinputfiles_resolvepathsfails( self ):
    with patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.resolveIFpaths", new=Mock(return_value=S_ERROR())):
      result = self.marAna.GetInputFiles()
      assertDiracFailsWith( result, 'missing slcio', self )

  def test_getinputfiles_complete( self ):
    with patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.resolveIFpaths", new=Mock(return_value=S_OK(['1.slcio', '2.slcio']))):
      res = self.marAna.GetInputFiles()
      assertDiracSucceedsWith_equals( res, '1.slcio 2.slcio', self )

  def assertInMultiple( self, listOfStrings, bigString):
    """Checks if every string in listOfStrings is contained in bigString.
    """
    for string in listOfStrings:
      assertInImproved(string, bigString, self)


class MarlinAnalysisPatchTestCase( MarlinAnalysisFixture, unittest.TestCase ):
  """ Contains test that have a certain set of patches of MarlinAnalysis methods in common
  """

  def setUp( self ):
    super( MarlinAnalysisPatchTestCase, self ).setUp()
    patches = [ patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getEnvironmentScript", new=Mock(return_value=S_OK('Testpath123'))), patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.GetInputFiles", new=Mock(return_value=S_OK("testinputfiles"))), patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getSteeringFileDirName", new=Mock(return_value=S_OK('testdir'))), patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.os.path.exists", new=Mock(side_effect=[False, True, False, True, False, True, False, False])) ]
    for patcher in patches:
      patcher.start()
  #  self.addCleanup( patch.stopall() )
  def tearDown( self ):
    super( MarlinAnalysisPatchTestCase, self ).tearDown()
    patch.stopall()

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.prepareXMLFile", new=Mock(return_value=S_OK('testdir')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.prepareMARLIN_DLL", new=Mock(return_value=S_OK('testdir')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.runMarlin", new=Mock(return_value=S_OK('testdir')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.shutil.copy")
  def test_runit_handlepandorasettings_no_log( self, mock_copy ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    result = self.marAna.runIt()
    assertDiracFailsWith( result, 'not produce the expected log', self )
    mock_copy.assert_called_with('testdir/PandoraSettings.xml', '%s/PandoraSettings.xml' % os.getcwd())

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.shutil.copy", new=Mock())
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.prepareXMLFile", new=Mock(return_value=S_ERROR('prepxml')))
  def test_runit_xmlgenerationfails( self ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    result = self.marAna.runIt()
    assertDiracFailsWith( result, 'prepxml', self )

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.shutil.copy", new=Mock())
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.prepareXMLFile", new=Mock(return_value=S_OK('testdir')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.prepareMARLIN_DLL", new=Mock(return_value=S_ERROR('mardll')))
  def test_runit_marlindllfails( self ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    result = self.marAna.runIt()
    assertDiracFailsWith( result, 'wrong with software installation', self )

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.prepareXMLFile", new=Mock(return_value=S_OK('testdir')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.prepareMARLIN_DLL", new=Mock(return_value=S_OK('testdir')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.runMarlin", new=Mock(return_value=S_ERROR('marlin failed')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.shutil.copy")
  def test_runit_runmarlinfails( self, mock_copy ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    result = self.marAna.runIt()
    assertDiracFailsWith( result, 'failed to run', self )
    mock_copy.assert_called_with('testdir/PandoraSettings.xml', '%s/PandoraSettings.xml' % os.getcwd())

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.prepareXMLFile", new=Mock(return_value=S_OK('testdir')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.prepareMARLIN_DLL", new=Mock(return_value=S_OK('testdir')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.runMarlin", new=Mock(return_value=S_OK([""])))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.shutil.copy")
  def test_runit_complete( self, mock_copy ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    with patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.os.path.exists", new=Mock(side_effect=[False, True, False, True, True, True, True, True] ) ):
      result = self.marAna.runIt()
      assertDiracSucceeds( result, self )
      mock_copy.assert_called_with('testdir/PandoraSettings.xml', '%s/PandoraSettings.xml' % os.getcwd())

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getEnvironmentScript", new=Mock(return_value=S_OK('Testpath123')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.GetInputFiles", new=Mock(return_value=S_OK("testinputfiles")))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getSteeringFileDirName", new=Mock(return_value=S_OK('testdir')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.os.path.exists", new=Mock(side_effect=[False, True, False, True, True, True, True, True]))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.prepareXMLFile", new=Mock(return_value=S_OK('testdir')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.prepareMARLIN_DLL", new=Mock(return_value=S_OK('testdir')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis.runMarlin", new=Mock(return_value=S_OK([""])))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.shutil.copy")
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.MarlinAnalysis._getDetectorXML", new=Mock(return_value=S_OK("someDetector.xml")))
  def test_runit_complete_dd( self, mock_copy ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    self.marAna.detectorModel = "someDetector.xml"
    result = self.marAna.runIt()
    assertDiracSucceeds( result, self )
    mock_copy.assert_called_with('testdir/PandoraSettings.xml', '%s/PandoraSettings.xml' % os.getcwd())


  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getSoftwareFolder", new=Mock(return_value=S_ERROR('')))
  def test_getenvscript_getsoftwarefolderfails( self ):
    self.assertFalse(self.marAna.getEnvScript(None, None, None)['OK'])

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getSoftwareFolder", new=Mock(return_value=S_OK('')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.removeLibc", new=Mock(return_value=None))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getNewLDLibs", new=Mock(return_value=None))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.os.path.exists", new=Mock(return_value=False))
  def test_getenvscript_pathexists( self ):
    result = self.marAna.getEnvScript(None, None, None)
    assertDiracFailsWith( result, 'marlin_dll folder not found', self )

  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getSoftwareFolder", new=Mock(return_value=S_OK('aFolder')))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.removeLibc", new=Mock(return_value=None))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.getNewLDLibs", new=Mock(return_value='bFolder'))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.os.path.exists", new=Mock(return_value=True))
  @patch("ILCDIRAC.Workflow.Modules.MarlinAnalysis.os.listdir", new=Mock(return_value=['testdir']))
  def test_getenvscript( self ):
    moduleName = "ILCDIRAC.Workflow.Modules.MarlinAnalysis"
    file_contents = []
    text_file_data = '\n'.join(file_contents)
    result = {}
    with patch('%s.open' % moduleName, mock_open(read_data=text_file_data), create=True) as file_mocker:
      result = self.marAna.getEnvScript(None, None, None)
    assertDiracSucceedsWith( result, '/MarlinEnv.sh', self )
    check_in_script = [ 'declare -x PATH=aFolder/Executable:$PATH\n', 'declare -x ROOTSYS=aFolder/ROOT\n', 'declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:aFolder/LDLibs:bFolder\n', "declare -x MARLIN_DLL=aFolder/MARLIN_DLL/testdir:\n", "declare -x PANDORASETTINGS=aFolder/Settings/PandoraSettings.xml" ]
    file_mocker.assert_called_with('MarlinEnv.sh', 'w')
    mocker_handle = file_mocker()
    for expected in check_in_script:
      mocker_handle.write.assert_any_call(expected)
    self.assertEquals(len(check_in_script), mocker_handle.__enter__.return_value.write.call_count - 4)

  def assertInMultiple( self, listOfStrings, bigString):
    """Checks if every string in listOfStrings is contained in bigString.
    """
    for string in listOfStrings:
      assertInImproved(string, bigString, self)
