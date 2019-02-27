"""
Unit tests for the MarlinAnalysis.py file
"""

import unittest
import os
from mock import mock_open, patch, MagicMock as Mock
from ILCDIRAC.Workflow.Modules.MarlinAnalysis import MarlinAnalysis
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertInImproved, \
  assertEqualsImproved, assertDiracFailsWith, assertDiracSucceeds, \
  assertDiracSucceedsWith, assertDiracSucceedsWith_equals, assertMockCalls
from ILCDIRAC.Tests.Utilities.FileUtils import FileUtil
from DIRAC import S_OK, S_ERROR

__RCSID__ = "$Id$"

#pylint: disable=missing-docstring, protected-access

MODULE_NAME = 'ILCDIRAC.Workflow.Modules.MarlinAnalysis'

class MarlinAnalysisFixture( object ):
  """ Contains the commonly used setUp and tearDown methods of the Tests"""
  def setUp( self ):
    """set up the objects"""
    self.marAna = MarlinAnalysis()
    self.marAna.OutputFile = ""
    self.marAna.ignoremissingInput = False
    self.log_mock = Mock()
    self.patches = [patch('%s.LOG' % MODULE_NAME, new=self.log_mock)]

    for patcher in self.patches:
      patcher.start()

  def tearDown( self ):
    """Clean up test objects"""
    del self.marAna
    for patcher in self.patches:
      patcher.stop()

# TODO: add case for undefined steering file
#pylint: disable=too-many-public-methods
class MarlinAnalysisTestCase( MarlinAnalysisFixture, unittest.TestCase ):
  """ Base class for the ProductionJob test cases
  """

  def test_applicationspecificinputs( self ):
    self.marAna.InputFile = [ 'firstEntry.txt' ]
    self.marAna.workflow_commons[ 'ParametricInputSandbox' ] = \
    [ 'addthis.slcio', 'anotherFile_butignoreme', 'lastfile.slcio' ]
    assertDiracSucceeds( self.marAna.applicationSpecificInputs(), self )
    assertEqualsImproved( ( self.marAna.InputFile, self.marAna.InputData ),
                          ( [ 'firstEntry.txt', 'addthis.slcio',
                              'anotherFile_butignoreme', 'lastfile.slcio'],
                            [] ), self )

  def test_applicationspecificinputs_nolist( self ):
    self.marAna.InputFile = [ 'dontleavemealone.txt' ]
    self.marAna.workflow_commons[ 'ParametricInputSandbox' ] = 'myEntryies;another_one.slcio'
    assertDiracSucceeds( self.marAna.applicationSpecificInputs(), self )
    assertEqualsImproved( ( self.marAna.InputFile, self.marAna.InputData ),
                          ( [ 'dontleavemealone.txt', 'myEntryies',
                              'another_one.slcio' ], [] ), self )

  def test_applicationspecificinputs_emptylist( self ):
    self.marAna.InputFile = [ 'leavemealone.txt' ]
    self.marAna.workflow_commons[ 'ParametricInputSandbox' ] = []
    assertDiracSucceeds( self.marAna.applicationSpecificInputs(), self )
    assertEqualsImproved( ( self.marAna.InputFile, self.marAna.InputData ),
                          ( [ 'leavemealone.txt' ], [] ), self )

  def test_applicationspecificinputs_emptystring( self ):
    self.marAna.InputFile = [ 'leavemealone2.txt' ]
    self.marAna.workflow_commons[ 'ParametricInputSandbox' ] = ''
    assertDiracSucceeds( self.marAna.applicationSpecificInputs(), self )
    assertEqualsImproved( ( self.marAna.InputFile, self.marAna.InputData ),
                          ( ['leavemealone2.txt'], [] ), self )

  def test_resolveinput_productionjob1( self ):
    self.marAna.workflow_commons[ "IS_PROD" ] = True
    outputfile1 = "/dir/a_REC_.a"
    outputfile2 = "/otherdir/b_DST_.b"
    inputfile = "/inputdir/input_SIM_.i"
    self.marAna.workflow_commons[ "ProductionOutputData" ] = ";".join([outputfile1, outputfile2, inputfile])
    assertDiracSucceedsWith_equals( self.marAna.applicationSpecificInputs(),
                                    "Parameters resolved", self )
    assertEqualsImproved( ( self.marAna.outputREC, self.marAna.outputDST,
                            self.marAna.InputFile ),
                          ( "a_REC_.a", "b_DST_.b", [ "input_SIM_.i" ] ), self )

  def test_resolveinput_productionjob2( self ):
    self.marAna.workflow_commons[ 'ParametricInputSandbox' ] = []
    self.marAna.workflow_commons[ "IS_PROD" ] = False
    self.marAna.workflow_commons[ "PRODUCTION_ID" ] = "123"
    self.marAna.workflow_commons[ "JOB_ID" ] = 456
    assertDiracSucceedsWith_equals( self.marAna.applicationSpecificInputs(),
                                    "Parameters resolved", self )
    self.assertEquals( self.marAna.InputFile, [] )
    self.assertEquals( self.marAna.InputData, [] )

  def test_resolveinput_productionjob3( self ):
    self.marAna.workflow_commons[ "IS_PROD" ] = True
    self.marAna.OutputFile = "c.c"
    self.marAna.InputFile = []
    inputlist = [ "a.slcio", "b.slcio", "c.exe" ]
    self.marAna.InputData = inputlist
    assertDiracSucceedsWith_equals( self.marAna.applicationSpecificInputs(),
                                    "Parameters resolved", self )
    self.assertEquals([inputlist[0], inputlist[1]], self.marAna.InputFile )

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
    assertDiracSucceedsWith_equals( self.marAna.applicationSpecificInputs(),
                                    "Parameters resolved", self )
    files = [self.marAna.outputREC, self.marAna.outputDST,
             self.marAna.InputFile[0]]
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

  @patch("%s.MarlinAnalysis._getDetectorXML" % MODULE_NAME,
         new=Mock(return_value=S_ERROR("zis iz not camelot")))
  def test_runit_detXMLFails( self ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = { 'OK' : True }
    self.marAna.detectorModel = "notCamelot.xml"
    result = self.marAna.runIt()
    assertDiracFailsWith( result, "zis iz not camelot", self )

  @patch("%s.getEnvironmentScript" % MODULE_NAME, new=Mock(return_value=S_ERROR("failed to get env script")))
  def test_runit_getEnvScriptFails( self ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    result = self.marAna.runIt()
    assertDiracFailsWith( result, 'env script', self )

  @patch("%s.getEnvironmentScript" % MODULE_NAME, new=Mock(return_value=S_OK('Testpath123')))
  @patch("%s.MarlinAnalysis._getInputFiles" % MODULE_NAME, new=Mock(return_value=S_ERROR('missing slcio file')))
  def test_runit_getInputFilesFails( self ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    result = self.marAna.runIt()
    #Only checks that the patch annotation is passed as the result
    assertDiracFailsWith( result, 'missing slcio file', self )

  def test_runit_no_steeringfile( self ):
    exists_dict = { 'PandoraSettings.xml' : False, 'testGear.input' : False,
                    '/steeringdir/testGear.input' : True,
                    '/steeringdir/PandoraSettings.xml' : True, '' : True,
                    'lib': True,
                  }
    def replace_exists( path ):
      return exists_dict[path]
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    self.marAna.detectorModel = None
    self.marAna.inputGEAR = '/my/secret/path/testGear.input'
    self.marAna.SteeringFile = ''
    with patch('%s.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=S_OK('envscript_path.test'))), \
         patch('%s.MarlinAnalysis._getInputFiles' % MODULE_NAME, new=Mock(return_value=S_OK(['testslcioList']))), \
         patch('%s.getSteeringFileDirName' % MODULE_NAME, new=Mock(return_value=S_OK('/steeringdir'))), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=replace_exists)), \
         patch('%s.shutil.copy' % MODULE_NAME, new=Mock(side_effect=EnvironmentError('shutil_test_fail'))) as copy_mock, \
         patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/test/curdir')):
      result = self.marAna.runIt()
      assertDiracFailsWith( result, 'could not find steering file', self )
      self.log_mock.warn.assert_called_once_with('Could not copy PandoraSettings.xml, exception: shutil_test_fail')
      self.log_mock.error.assert_called_once_with("Steering file not defined, shouldn't happen!")
      copy_mock.assert_called_once_with( '/steeringdir/PandoraSettings.xml',
                                         '/test/curdir/PandoraSettings.xml' )

  def test_runit_no_steeringdir( self ):
    exists_dict = { 'PandoraSettings.xml' : True, 'testGear.input' : False,
                    '' : True }
    def replace_exists( path ):
      return exists_dict[path]
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    self.marAna.detectorModel = None
    self.marAna.inputGEAR = '/my/secret/path/testGear.input'
    self.marAna.SteeringFile = ''
    with patch('%s.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=S_OK('envscript_path.test'))), \
         patch('%s.MarlinAnalysis._getInputFiles' % MODULE_NAME, new=Mock(return_value=S_OK(['testslcioList']))), \
         patch('%s.getSteeringFileDirName' % MODULE_NAME, new=Mock(return_value=S_ERROR('mytesterr'))), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=replace_exists)), \
         patch('%s.shutil.copy' % MODULE_NAME, new=Mock(side_effect=EnvironmentError('shutil_test_fail'))) as copy_mock, \
         patch('%s.os.getcwd' % MODULE_NAME, new=Mock(return_value='/test/curdir')):
      result = self.marAna.runIt()
      assertDiracFailsWith( result, 'could not find steering file', self )
      self.log_mock.warn.assert_called_once_with('Could not find the steering file directory', 'mytesterr')
      self.log_mock.error.assert_called_once_with("Steering file not defined, shouldn't happen!")
      self.assertFalse( copy_mock.called )

  @patch("%s.getSoftwareFolder" % MODULE_NAME, new=Mock(return_value=S_ERROR('')))
  def test_getenvscript_getsoftwarefolderfails( self ):
    self.assertFalse(self.marAna.getEnvScript(None, None, None)['OK'])

  @patch("%s.getSoftwareFolder" % MODULE_NAME, new=Mock(return_value=S_OK('')))
  @patch("%s.removeLibc" % MODULE_NAME, new=Mock(return_value=None))
  @patch("%s.getNewLDLibs" % MODULE_NAME, new=Mock(return_value=None))
  @patch("%s.os.path.exists" % MODULE_NAME, new=Mock(return_value=False))
  def test_getenvscript_pathexists( self ):
    result = self.marAna.getEnvScript(None, None, None)
    assertDiracFailsWith( result, 'marlin_dll folder not found', self )

  @patch("%s.getSoftwareFolder" % MODULE_NAME, new=Mock(return_value=S_OK('aFolder')))
  @patch("%s.removeLibc" % MODULE_NAME, new=Mock(return_value=None))
  @patch("%s.getNewLDLibs" % MODULE_NAME, new=Mock(return_value='bFolder'))
  @patch("%s.os.path.exists" % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.os.listdir" % MODULE_NAME, new=Mock(return_value=['testdir']))
  def test_getenvscript( self ):
    file_contents = []
    text_file_data = '\n'.join(file_contents)
    result = {}
    with patch('%s.open' % MODULE_NAME, mock_open(read_data=text_file_data), create=True) as file_mocker:
      result = self.marAna.getEnvScript(None, None, None)
    assertDiracSucceedsWith( result, '/MarlinEnv.sh', self )
    check_in_script = [
      'declare -x PATH=aFolder/Executable:$PATH\n', 'declare -x ROOTSYS=aFolder/ROOT\n',
      'declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:aFolder/LDLibs:bFolder\n',
      "declare -x MARLIN_DLL=aFolder/MARLIN_DLL/testdir:\n",
      "declare -x PANDORASETTINGS=aFolder/Settings/PandoraSettings.xml" ]
    file_mocker.assert_called_with('MarlinEnv.sh', 'w')
    mocker_handle = file_mocker()
    for expected in check_in_script:
      mocker_handle.write.assert_any_call(expected)
    self.assertEquals(len(check_in_script),
                      mocker_handle.__enter__.return_value.write.call_count - 4)

  def test_getinputfiles_ignore( self ):
    self.marAna.ignoremissingInput = True
    assertDiracSucceeds( self.marAna._getInputFiles(), self )

  def test_getinputfiles_resolvepathsfails( self ):
    with patch("%s.resolveIFpaths" % MODULE_NAME, new=Mock(return_value=S_ERROR())):
      result = self.marAna._getInputFiles()
      assertDiracFailsWith( result, 'missing slcio', self )

  def test_getinputfiles_complete( self ):
    with patch("%s.resolveIFpaths" % MODULE_NAME, new=Mock(return_value=S_OK(['1.slcio', '2.slcio']))):
      res = self.marAna._getInputFiles()
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
    thesePatches = [patch("%s.getEnvironmentScript" % MODULE_NAME, new=Mock(return_value=S_OK('Testpath123'))),
                    patch("%s.MarlinAnalysis._getInputFiles" % MODULE_NAME,
                          new=Mock(return_value=S_OK("testinputfiles"))),
                    patch("%s.getSteeringFileDirName" % MODULE_NAME, new=Mock(return_value=S_OK('testdir'))),
                    patch("%s.os.path.exists" % MODULE_NAME,
                          new=Mock(side_effect=[False, True, False, True, False, True, False, False]))]
    self.patches.extend(thesePatches)
    for patcher in thesePatches:
      patcher.start()

  @patch("%s.prepareXMLFile" % MODULE_NAME, new=Mock(return_value=S_OK('testdir')))
  @patch("%s.MarlinAnalysis.prepareMARLIN_DLL" % MODULE_NAME, new=Mock(return_value=S_OK('testdir')))
  @patch("%s.MarlinAnalysis.runMarlin" % MODULE_NAME, new=Mock(return_value=S_OK('testdir')))
  @patch("%s.shutil.copy" % MODULE_NAME)
  def test_runit_handlepandorasettings_no_log( self, mock_copy ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    result = self.marAna.runIt()
    assertDiracFailsWith( result, 'not produce the expected log', self )
    mock_copy.assert_called_with('testdir/PandoraSettings.xml',
                                 '%s/PandoraSettings.xml' % os.getcwd())

  @patch("%s.shutil.copy" % MODULE_NAME, new=Mock())
  @patch("%s.prepareXMLFile" % MODULE_NAME, new=Mock(return_value=S_ERROR('prepxml')))
  def test_runit_xmlgenerationfails( self ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    result = self.marAna.runIt()
    assertDiracFailsWith( result, 'prepxml', self )

  @patch("%s.shutil.copy" % MODULE_NAME, new=Mock())
  @patch("%s.prepareXMLFile" % MODULE_NAME, new=Mock(return_value=S_OK('testdir')))
  @patch("%s.MarlinAnalysis.prepareMARLIN_DLL" % MODULE_NAME, new=Mock(return_value=S_ERROR('mardll')))
  def test_runit_marlindllfails( self ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    result = self.marAna.runIt()
    assertDiracFailsWith( result, 'wrong with software installation', self )

  @patch("%s.prepareXMLFile" % MODULE_NAME, new=Mock(return_value=S_OK('testdir')))
  @patch("%s.MarlinAnalysis.prepareMARLIN_DLL" % MODULE_NAME, new=Mock(return_value=S_OK('testdir')))
  @patch("%s.MarlinAnalysis.runMarlin" % MODULE_NAME, new=Mock(return_value=S_ERROR('marlin failed')))
  @patch("%s.shutil.copy" % MODULE_NAME)
  def test_runit_runmarlinfails( self, mock_copy ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    result = self.marAna.runIt()
    assertDiracFailsWith( result, 'failed to run', self )
    mock_copy.assert_called_with('testdir/PandoraSettings.xml',
                                 '%s/PandoraSettings.xml' % os.getcwd())

  @patch("%s.prepareXMLFile" % MODULE_NAME, new=Mock(return_value=S_OK('testdir')))
  @patch("%s.MarlinAnalysis.prepareMARLIN_DLL" % MODULE_NAME, new=Mock(return_value=S_OK('testdir')))
  @patch("%s.MarlinAnalysis.runMarlin" % MODULE_NAME, new=Mock(return_value=S_OK([""])))
  @patch("%s.shutil.copy" % MODULE_NAME)
  def test_runit_complete( self, mock_copy ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    with patch("%s.os.path.exists" % MODULE_NAME, new=Mock(side_effect=[False, True, False, True, True, True, True, True] ) ):
      result = self.marAna.runIt()
      assertDiracSucceeds( result, self )
      mock_copy.assert_called_with('testdir/PandoraSettings.xml',
                                   '%s/PandoraSettings.xml' % os.getcwd())

  def test_runit_complete_dd( self ):
    self.marAna.platform = "Testplatform123"
    self.marAna.applicationLog = "testlog123"
    self.marAna.stepStatus = S_OK()
    self.marAna.workflowStatus = S_OK()
    self.marAna.detectorModel = "someDetector.xml"
    with patch("%s.getEnvironmentScript" % MODULE_NAME, new=Mock(return_value=S_OK('Testpath123'))), \
         patch("%s.MarlinAnalysis._getInputFiles" % MODULE_NAME, new=Mock(return_value=S_OK("testinputfiles"))), \
         patch("%s.getSteeringFileDirName" % MODULE_NAME, new=Mock(return_value=S_OK('testdir'))), \
         patch("%s.os.path.exists" % MODULE_NAME, new=Mock(side_effect=[False, True, False, True, True, True, True, True])), \
         patch("%s.prepareXMLFile" % MODULE_NAME, new=Mock(return_value=S_OK('testdir'))), \
         patch("%s.MarlinAnalysis.prepareMARLIN_DLL" % MODULE_NAME, new=Mock(return_value=S_OK('testdir'))), \
         patch("%s.MarlinAnalysis.runMarlin" % MODULE_NAME, new=Mock(return_value=S_OK([""]))), \
         patch("%s.shutil.copy" % MODULE_NAME) as mock_copy, \
         patch("%s.MarlinAnalysis._getDetectorXML" % MODULE_NAME, new=Mock(return_value=S_OK("someDetector.xml"))):
      result = self.marAna.runIt()
      assertDiracSucceeds( result, self )
      mock_copy.assert_called_with('testdir/PandoraSettings.xml',
                                   '%s/PandoraSettings.xml' % os.getcwd())

  @patch("%s.getSoftwareFolder" % MODULE_NAME, new=Mock(return_value=S_ERROR('')))
  def test_getenvscript_getsoftwarefolderfails( self ):
    self.assertFalse(self.marAna.getEnvScript(None, None, None)['OK'])

  @patch("%s.getSoftwareFolder" % MODULE_NAME, new=Mock(return_value=S_OK('')))
  @patch("%s.removeLibc" % MODULE_NAME, new=Mock(return_value=None))
  @patch("%s.getNewLDLibs" % MODULE_NAME, new=Mock(return_value=None))
  @patch("%s.os.path.exists" % MODULE_NAME, new=Mock(return_value=False))
  def test_getenvscript_pathexists( self ):
    result = self.marAna.getEnvScript(None, None, None)
    assertDiracFailsWith( result, 'marlin_dll folder not found', self )

  @patch("%s.getSoftwareFolder" % MODULE_NAME, new=Mock(return_value=S_OK('aFolder')))
  @patch("%s.removeLibc" % MODULE_NAME, new=Mock(return_value=None))
  @patch("%s.getNewLDLibs" % MODULE_NAME, new=Mock(return_value='bFolder'))
  @patch("%s.os.path.exists" % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.os.listdir" % MODULE_NAME, new=Mock(return_value=['testdir']))
  def test_getenvscript( self ):
    file_contents = []
    text_file_data = '\n'.join(file_contents)
    result = {}
    with patch('%s.open' % MODULE_NAME, mock_open(read_data=text_file_data), create=True) as file_mocker:
      result = self.marAna.getEnvScript(None, None, None)
    assertDiracSucceedsWith( result, '/MarlinEnv.sh', self )
    check_in_script = [
      'declare -x PATH=aFolder/Executable:$PATH\n',
      'declare -x ROOTSYS=aFolder/ROOT\n',
      'declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:aFolder/LDLibs:bFolder\n',
      "declare -x MARLIN_DLL=aFolder/MARLIN_DLL/testdir:\n",
      "declare -x PANDORASETTINGS=aFolder/Settings/PandoraSettings.xml" ]
    file_mocker.assert_called_with('MarlinEnv.sh', 'w')
    mocker_handle = file_mocker()
    for expected in check_in_script:
      mocker_handle.write.assert_any_call(expected)
    self.assertEquals(len(check_in_script),
                      mocker_handle.__enter__.return_value.write.call_count - 4)

  def assertInMultiple( self, listOfStrings, bigString):
    """Checks if every string in listOfStrings is contained in bigString.
    """
    for string in listOfStrings:
      assertInImproved(string, bigString, self)

class MarlinAnalysisPrepareDLLCase( MarlinAnalysisFixture, unittest.TestCase ):
  ''' Tests for the prepareMARLIN_DLL method
  '''

  def test_preparemarlindll( self ):
    exists_dict = { './lib/marlin_dll' : True }
    def replace_exists( path ):
      return exists_dict[path]
    file_contents = [[]]
    handles = FileUtil.getMultipleReadHandles( file_contents )
    with patch('%s.open' % MODULE_NAME, mock_open()) as open_mock, \
         patch('%s.os.chmod' % MODULE_NAME) as chmod_mock, \
         patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK([ 0, 'MARlin_DLL/path: ', 'other_return_value_from_shell' ]))) as shell_mock, \
         patch('%s.os.remove' % MODULE_NAME, new=Mock(side_effect=[ True ])) as remove_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=replace_exists)) as exists_mock, \
         patch('%s.glob.glob' % MODULE_NAME, new=Mock(return_value=[ 'mytestlibrary.so', 'secondLibrary.veryUseful.so' ])) as glob_mock:
      open_mock.side_effect = (h for h in handles)
      result = self.marAna.prepareMARLIN_DLL( 'some_path' )
      open_mock.assert_called_once_with( 'temp.sh', 'w' )
      assertMockCalls( handles[0].__enter__().write,
                       ['#!/bin/bash\n', 'source some_path > /dev/null\necho $MARLIN_DLL'], self)
      assertEqualsImproved( len( handles ), 1, self )
      chmod_mock.assert_called_once_with('temp.sh', 0o755)
      shell_mock.assert_called_once_with( 0, './temp.sh' )
      remove_mock.assert_called_once_with( 'temp.sh' )
      exists_mock.assert_called_once_with( './lib/marlin_dll' )
      glob_mock.assert_called_once_with( './lib/marlin_dll/*.so' )
      assertDiracSucceedsWith( result, 'MARlin_DLL/path:mytestlibrary.so:secondLibrary.veryUseful.so',
                               self )

  def test_preparemarlindll_shellcall_fails( self ):
    file_contents = [[]]
    handles = FileUtil.getMultipleReadHandles( file_contents )
    with patch('%s.open' % MODULE_NAME, mock_open()) as open_mock, \
         patch('%s.os.chmod' % MODULE_NAME) as chmod_mock, \
         patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_test_err'))) as shell_mock:
      open_mock.side_effect = (h for h in handles)
      result = self.marAna.prepareMARLIN_DLL( 'some_path' )
      open_mock.assert_called_once_with( 'temp.sh', 'w' )
      assertMockCalls( handles[0].__enter__().write,
                       ['#!/bin/bash\n', 'source some_path > /dev/null\necho $MARLIN_DLL'], self)
      assertEqualsImproved( len( handles ), 1, self )
      chmod_mock.assert_called_once_with('temp.sh', 0o755)
      shell_mock.assert_called_once_with( 0, './temp.sh' )
      assertDiracFailsWith( result, 'failed getting the marlin_dll', self )

  def test_preparemarlindll_empty_marlindll( self ):
    file_contents = [[]]
    handles = FileUtil.getMultipleReadHandles( file_contents )
    with patch('%s.open' % MODULE_NAME, mock_open()) as open_mock, \
         patch('%s.os.chmod' % MODULE_NAME) as chmod_mock, \
         patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK( [0, '', 'other_value'] ))) as shell_mock, \
         patch('%s.os.remove' % MODULE_NAME, new=Mock(side_effect=EnvironmentError('something?'))) :
      open_mock.side_effect = (h for h in handles)
      result = self.marAna.prepareMARLIN_DLL( 'some_path' )
      open_mock.assert_called_once_with( 'temp.sh', 'w' )
      assertMockCalls( handles[0].__enter__().write,
                       ['#!/bin/bash\n', 'source some_path > /dev/null\necho $MARLIN_DLL'], self)
      assertEqualsImproved( len( handles ), 1, self )
      chmod_mock.assert_called_once_with('temp.sh', 0o755)
      shell_mock.assert_called_once_with( 0, './temp.sh' )
      assertDiracFailsWith( result, 'empty marlin_dll env var', self )

  def test_preparemarlindll_procstoinclude( self ):
    self.marAna.ProcessorListToUse = [ 'secondLibrary.veryUseful.so' ]
    exists_dict = { './lib/marlin_dll' : True }
    def replace_exists( path ):
      return exists_dict[path]
    file_contents = [[]]
    handles = FileUtil.getMultipleReadHandles( file_contents )
    with patch('%s.open' % MODULE_NAME, mock_open()) as open_mock, \
         patch('%s.os.chmod' % MODULE_NAME) as chmod_mock, \
         patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK([ 0, 'MARlin_DLL/path', 'other_return_value_from_shell' ]))) as shell_mock, \
         patch('%s.os.remove' % MODULE_NAME, new=Mock(side_effect=[ True ])) as remove_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=replace_exists)) as exists_mock, \
         patch('%s.glob.glob' % MODULE_NAME, new=Mock(return_value=[ 'mytestlibrary.so', 'secondLibrary.veryUseful.so', 'MARlin_DLL/path' ])) as glob_mock:
      open_mock.side_effect = (h for h in handles)
      result = self.marAna.prepareMARLIN_DLL( 'some_path' )
      open_mock.assert_called_once_with( 'temp.sh', 'w' )
      assertMockCalls( handles[0].__enter__().write,
                       ['#!/bin/bash\n', 'source some_path > /dev/null\necho $MARLIN_DLL'], self)
      assertEqualsImproved( len( handles ), 1, self )
      chmod_mock.assert_called_once_with('temp.sh', 0o755)
      shell_mock.assert_called_once_with( 0, './temp.sh' )
      remove_mock.assert_called_once_with( 'temp.sh' )
      exists_mock.assert_called_once_with( './lib/marlin_dll' )
      glob_mock.assert_called_once_with( './lib/marlin_dll/*.so' )
      assertDiracSucceedsWith( result, 'secondLibrary.veryUseful.so', self )

  def test_preparemarlindll_procstoexclude( self ):
    self.marAna.ProcessorListToExclude = [ 'secondLibrary.veryUseful.so' ]
    exists_dict = { './lib/marlin_dll' : True }
    def replace_exists( path ):
      return exists_dict[path]
    file_contents = [[]]
    handles = FileUtil.getMultipleReadHandles( file_contents )
    with patch('%s.open' % MODULE_NAME, mock_open()) as open_mock, \
         patch('%s.os.chmod' % MODULE_NAME) as chmod_mock, \
         patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK([ 0, 'MARlin_DLL/path', 'other_return_value_from_shell' ]))) as shell_mock, \
         patch('%s.os.remove' % MODULE_NAME, new=Mock(side_effect=[ True ])) as remove_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=replace_exists)) as exists_mock, \
         patch('%s.glob.glob' % MODULE_NAME, new=Mock(return_value=[ 'mytestlibrary.so', 'secondLibrary.veryUseful.so' ])) as glob_mock:
      open_mock.side_effect = (h for h in handles)
      result = self.marAna.prepareMARLIN_DLL( 'some_path' )
      open_mock.assert_called_once_with( 'temp.sh', 'w' )
      assertMockCalls( handles[0].__enter__().write,
                       ['#!/bin/bash\n', 'source some_path > /dev/null\necho $MARLIN_DLL'], self)
      assertEqualsImproved( len( handles ), 1, self )
      chmod_mock.assert_called_once_with('temp.sh', 0o755)
      shell_mock.assert_called_once_with( 0, './temp.sh' )
      remove_mock.assert_called_once_with( 'temp.sh' )
      exists_mock.assert_called_once_with( './lib/marlin_dll' )
      glob_mock.assert_called_once_with( './lib/marlin_dll/*.so' )
      assertDiracSucceedsWith( result, 'MARlin_DLL/path:mytestlibrary.so', self )

  def test_preparemarlindll_nolibs( self ):
    exists_dict = { './lib/marlin_dll' : False }
    def replace_exists( path ):
      return exists_dict[path]
    file_contents = [[]]
    handles = FileUtil.getMultipleReadHandles( file_contents )
    with patch('%s.open' % MODULE_NAME, mock_open()) as open_mock, \
         patch('%s.os.chmod' % MODULE_NAME) as chmod_mock, \
         patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK([ 0, 'MARlin_DLL/path: ', 'other_return_value_from_shell' ]))) as shell_mock, \
         patch('%s.os.remove' % MODULE_NAME, new=Mock(side_effect=[ True ])) as remove_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=replace_exists)) as exists_mock, \
         patch('%s.glob.glob' % MODULE_NAME, new=Mock(return_value=[ 'mytestlibrary.so', 'secondLibrary.veryUseful.so' ])) as glob_mock:
      open_mock.side_effect = (h for h in handles)
      result = self.marAna.prepareMARLIN_DLL( 'some_path' )
      open_mock.assert_called_once_with( 'temp.sh', 'w' )
      assertMockCalls( handles[0].__enter__().write,
                       ['#!/bin/bash\n', 'source some_path > /dev/null\necho $MARLIN_DLL'], self)
      assertEqualsImproved( len( handles ), 1, self )
      chmod_mock.assert_called_once_with('temp.sh', 0o755)
      shell_mock.assert_called_once_with( 0, './temp.sh' )
      remove_mock.assert_called_once_with( 'temp.sh' )
      exists_mock.assert_called_once_with( './lib/marlin_dll' )
      self.assertFalse( glob_mock.called )
      assertDiracSucceedsWith( result, 'MARlin_DLL/path', self )

  def test_preparemarlindll_swaplibpositions( self ):
    self.marAna.ProcessorListToExclude = [ 'mytestlibrary.so' ]
    exists_dict = { './lib/marlin_dll' : True }
    def replace_exists( path ):
      return exists_dict[path]
    file_contents = [[]]
    handles = FileUtil.getMultipleReadHandles( file_contents )
    with patch('%s.open' % MODULE_NAME, mock_open()) as open_mock, \
         patch('%s.os.chmod' % MODULE_NAME) as chmod_mock, \
         patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK([ 0, 'MARlin_DLL/path', 'other_return_value_from_shell' ]))) as shell_mock, \
         patch('%s.os.remove' % MODULE_NAME, new=Mock(side_effect=[ True ])) as remove_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=replace_exists)) as exists_mock, \
         patch('%s.glob.glob' % MODULE_NAME, new=Mock(return_value=[ 'testlibLCFIPlus.so', 'testlibLCFIVertex.1.so'  ])) as glob_mock:
      open_mock.side_effect = (h for h in handles)
      result = self.marAna.prepareMARLIN_DLL( 'some_path' )
      open_mock.assert_called_once_with( 'temp.sh', 'w' )
      assertMockCalls( handles[0].__enter__().write,
                       ['#!/bin/bash\n', 'source some_path > /dev/null\necho $MARLIN_DLL'], self)
      assertEqualsImproved( len( handles ), 1, self )
      chmod_mock.assert_called_once_with('temp.sh', 0o755)
      shell_mock.assert_called_once_with( 0, './temp.sh' )
      remove_mock.assert_called_once_with( 'temp.sh' )
      exists_mock.assert_called_once_with( './lib/marlin_dll' )
      glob_mock.assert_called_once_with( './lib/marlin_dll/*.so' )
      assertDiracSucceedsWith( result, 'MARlin_DLL/path:testlibLCFIVertex.1.so:testlibLCFIPlus.so', self )

class MarlinAnalysisRunTestCase( MarlinAnalysisFixture, unittest.TestCase ):
  ''' Tests for the runMarlin method
  '''

  def test_runmarlin( self ):
    self.marAna.STEP_NUMBER = '935'
    self.marAna.applicationVersion = 'VT'
    self.marAna.debug = True
    self.marAna.applicationLog = 'testLog.mymarlin'
    self.marAna.extraCLIarguments = 'testMyArgs'
    exists_dict = { './lib/lddlib' : True, 'Marlin_VT_Run_935.sh' : True,
                    './lib/marlin_dll' : True, 'myXML.test' : True, 'testLog.mymarlin' : False,
                    './lib/': False,
                  }
    def replace_exists( path ):
      return exists_dict[path]
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=replace_exists)) as exists_mock, \
         patch('%s.os.remove' % MODULE_NAME) as remove_mock, \
         patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK('shell succeeded'))) as shell_mock, \
         patch('%s.open' % MODULE_NAME, mock_open()) as open_mock, \
         patch('%s.MarlinAnalysis.setApplicationStatus' % MODULE_NAME) as appstat_mock, \
         patch('%s.os.chmod' % MODULE_NAME) as chmod_mock:
      result = self.marAna.runMarlin( 'myXML.test', 'TestEnvscript.path', 'marlin_dll.test.so' )
      assertDiracSucceedsWith_equals( result, 'shell succeeded', self )
      open_mock.assert_called_once_with( 'Marlin_VT_Run_935.sh', 'w' )
      open_mock = open_mock()
      assertMockCalls( open_mock.write, [
        '#!/bin/bash \n', '#####################################################################\n',
        '# Dynamically generated script to run a production or analysis job. #\n',
        '#####################################################################\n',
        'source TestEnvscript.path\n', 'declare -x MARLIN_DLL=marlin_dll.test.so\n',
        'declare -x LD_LIBRARY_PATH=./lib/lddlib:$LD_LIBRARY_PATH\n',
        'declare -x PATH=$ROOTSYS/bin:$PATH\n', 'declare -x MARLIN_DEBUG=1\n',
        '\nif [ -e "${PANDORASETTINGS}" ]\nthen\n   cp $PANDORASETTINGS .\nfi    \n',
        'echo =============================\n', 'echo LD_LIBRARY_PATH is\n',
        'echo $LD_LIBRARY_PATH | tr ":" "\n"\n',
        'echo =============================\n', 'echo PATH is\n',
        'echo $PATH | tr ":" "\n"\n', 'echo =============================\n',
        'echo MARLIN_DLL is\n', 'echo $MARLIN_DLL | tr ":" "\n"\n',
        'echo =============================\n', 'echo ldd of executable is\n',
        'ldd `which Marlin` \n', 'echo =============================\n',
        'ldd ./lib/marlin_dll/*.so \n', 'ldd ./lib/lddlib/*.so \n',
        'echo =============================\n', 'env | sort >> localEnv.log\n',
        'Marlin -c myXML.test testMyArgs\n', 'Marlin myXML.test testMyArgs\n',
        'declare -x appstatus=$?\n', 'exit $appstatus\n' ], self )
      remove_mock.assert_called_once_with( 'Marlin_VT_Run_935.sh' )
      assertMockCalls( exists_mock, [ 'Marlin_VT_Run_935.sh', './lib/lddlib', './lib/marlin_dll',
                                      './lib/lddlib', 'myXML.test', 'testLog.mymarlin', './lib/'], self )
      shell_mock.assert_called_once_with( 0, 'sh -c "./Marlin_VT_Run_935.sh"',
                                          callbackFunction=self.marAna.redirectLogOutput,
                                          bufferLimit=20971520 )
      chmod_mock.assert_called_once_with('Marlin_VT_Run_935.sh', 0o755)
      appstat_mock.assert_called_once_with( 'Marlin VT step 935')

  def test_runmarlin_nosteeringfile( self ):
    self.marAna.STEP_NUMBER = '932'
    self.marAna.applicationVersion = 'V1T'
    self.marAna.debug = False
    self.marAna.extraCLIarguments = 'testMyArgs'
    exists_dict = { './lib/lddlib' : False, 'Marlin_V1T_Run_932.sh' : False,
                    'inputxmlMy.test' : False,
                    './lib/': False,
                  }
    def replace_exists( path ):
      return exists_dict[path]
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=replace_exists)), \
         patch('%s.os.remove' % MODULE_NAME) as remove_mock, \
         patch('%s.shellCall' % MODULE_NAME) as shell_mock, \
         patch('%s.open' % MODULE_NAME, mock_open()) as open_mock, \
         patch('%s.MarlinAnalysis.setApplicationStatus' % MODULE_NAME) as appstat_mock, \
         patch('%s.os.chmod' % MODULE_NAME) as chmod_mock:
      result = self.marAna.runMarlin( 'inputxmlMy.test', 'envscript.path',
                                      'marlin_dll' )
      assertDiracFailsWith( result, 'steeringfile is missing', self )
      open_mock.assert_called_once_with( 'Marlin_V1T_Run_932.sh', 'w' )
      open_mock().close.assert_called_once_with()
      self.assertFalse( remove_mock.called )
      self.assertFalse( shell_mock.called )
      self.assertFalse( appstat_mock.called )
      self.assertFalse( chmod_mock.called )

  def test_runmarlin_shell_fails( self ):
    self.marAna.STEP_NUMBER = '935'
    self.marAna.applicationVersion = 'VT'
    self.marAna.debug = False
    self.marAna.applicationLog = 'testLog.mymarlin'
    self.marAna.extraCLIarguments = 'testMyArgs'
    exists_dict = { './lib/lddlib' : False, 'Marlin_VT_Run_935.sh' : True,
                    'myXML.test' : True, 'testLog.mymarlin' : False,
                    './lib/': True,
                  }
    def replace_exists( path ):
      return exists_dict[path]
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=replace_exists)) as exists_mock, \
         patch('%s.os.remove' % MODULE_NAME) as remove_mock, \
         patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_test_err_with_script'))) as shell_mock, \
         patch('%s.open' % MODULE_NAME, mock_open()) as open_mock, \
         patch('%s.MarlinAnalysis.setApplicationStatus' % MODULE_NAME) as appstat_mock, \
         patch('%s.os.chmod' % MODULE_NAME) as chmod_mock:
      result = self.marAna.runMarlin( 'myXML.test', 'TestEnvscript.path', 'marlin_dll.test.so' )
      assertDiracFailsWith( result, 'some_test_err_with_script', self )
      open_mock.assert_called_once_with( 'Marlin_VT_Run_935.sh', 'w' )
      open_mock = open_mock()
      assertMockCalls( open_mock.write, [
        '#!/bin/bash \n', '#####################################################################\n',
        '# Dynamically generated script to run a production or analysis job. #\n',
        '#####################################################################\n',
        'source TestEnvscript.path\n', 'declare -x MARLIN_DLL=marlin_dll.test.so\n',
        'declare -x LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH\n',
        'declare -x PATH=$ROOTSYS/bin:$PATH\n', 'declare -x MARLIN_DEBUG=1\n',
        '\nif [ -e "${PANDORASETTINGS}" ]\nthen\n   cp $PANDORASETTINGS .\nfi    \n',
        'echo =============================\n', 'echo LD_LIBRARY_PATH is\n',
        'echo $LD_LIBRARY_PATH | tr ":" "\n"\n', 'echo =============================\n',
        'echo PATH is\n', 'echo $PATH | tr ":" "\n"\n', 'echo =============================\n',
        'echo MARLIN_DLL is\n', 'echo $MARLIN_DLL | tr ":" "\n"\n', 'echo =============================\n',
        'env | sort >> localEnv.log\n', 'Marlin -c myXML.test testMyArgs\n', 'Marlin myXML.test testMyArgs\n',
        'declare -x appstatus=$?\n', 'exit $appstatus\n' ], self )
      remove_mock.assert_called_once_with( 'Marlin_VT_Run_935.sh' )
      assertMockCalls( exists_mock, [ 'Marlin_VT_Run_935.sh', './lib/lddlib', 'myXML.test',
                                      'testLog.mymarlin', './lib/' ], self )
      shell_mock.assert_called_once_with( 0, 'sh -c "./Marlin_VT_Run_935.sh"',
                                          callbackFunction=self.marAna.redirectLogOutput,
                                          bufferLimit=20971520 )
      chmod_mock.assert_called_once_with('Marlin_VT_Run_935.sh', 0o755)
      appstat_mock.assert_called_once_with( 'Marlin VT step 935' )

  def test_runmarlin_nolibs( self ):
    self.marAna.STEP_NUMBER = '935'
    self.marAna.applicationVersion = 'VT'
    self.marAna.debug = True
    self.marAna.applicationLog = 'testLog.mymarlin'
    self.marAna.extraCLIarguments = 'testMyArgs'
    exists_dict = { './lib/lddlib' : False, 'Marlin_VT_Run_935.sh' : True,
                    './lib/marlin_dll' : False, 'myXML.test' : True,
                    'testLog.mymarlin' : False,
                    './lib/': False,
                  }
    def replace_exists( path ):
      return exists_dict[path]
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=replace_exists)) as exists_mock, \
         patch('%s.os.remove' % MODULE_NAME) as remove_mock, \
         patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK('shell succeeded'))) as shell_mock, \
         patch('%s.open' % MODULE_NAME, mock_open()) as open_mock, \
         patch('%s.MarlinAnalysis.setApplicationStatus' % MODULE_NAME) as appstat_mock, \
         patch('%s.os.chmod' % MODULE_NAME) as chmod_mock:
      result = self.marAna.runMarlin( 'myXML.test', 'TestEnvscript.path', 'marlin_dll.test.so' )
      assertDiracSucceedsWith_equals( result, 'shell succeeded', self )
      open_mock.assert_called_once_with( 'Marlin_VT_Run_935.sh', 'w' )
      open_mock = open_mock()
      assertMockCalls( open_mock.write, [
        '#!/bin/bash \n', '#####################################################################\n',
        '# Dynamically generated script to run a production or analysis job. #\n',
        '#####################################################################\n',
        'source TestEnvscript.path\n', 'declare -x MARLIN_DLL=marlin_dll.test.so\n',
        'declare -x PATH=$ROOTSYS/bin:$PATH\n', 'declare -x MARLIN_DEBUG=1\n',
        '\nif [ -e "${PANDORASETTINGS}" ]\nthen\n   cp $PANDORASETTINGS .\nfi    \n',
        'echo =============================\n', 'echo LD_LIBRARY_PATH is\n',
        'echo $LD_LIBRARY_PATH | tr ":" "\n"\n', 'echo =============================\n',
        'echo PATH is\n', 'echo $PATH | tr ":" "\n"\n', 'echo =============================\n',
        'echo MARLIN_DLL is\n', 'echo $MARLIN_DLL | tr ":" "\n"\n', 'echo =============================\n',
        'echo ldd of executable is\n', 'ldd `which Marlin` \n', 'echo =============================\n',
        'echo =============================\n',
        'env | sort >> localEnv.log\n', 'Marlin -c myXML.test testMyArgs\n', 'Marlin myXML.test testMyArgs\n',
        'declare -x appstatus=$?\n', 'exit $appstatus\n' ], self )
      remove_mock.assert_called_once_with( 'Marlin_VT_Run_935.sh' )
      assertMockCalls( exists_mock, [ 'Marlin_VT_Run_935.sh', './lib/lddlib', './lib/marlin_dll',
                                      './lib/lddlib', 'myXML.test', 'testLog.mymarlin', './lib/' ], self )
      shell_mock.assert_called_once_with( 0, 'sh -c "./Marlin_VT_Run_935.sh"',
                                          callbackFunction=self.marAna.redirectLogOutput,
                                          bufferLimit=20971520 )
      chmod_mock.assert_called_once_with('Marlin_VT_Run_935.sh', 0o755)
      appstat_mock.assert_called_once_with( 'Marlin VT step 935' )


  def test_checkRunOverlay( self ):
    self.marAna.workflow_commons[ 'OI_2_eventType' ] = 'p'
    self.marAna.workflow_commons[ 'OI_1_eventType' ] = 'g'

    self.marAna.workflow_commons[ 'OI_2_eventsPerBackgroundFile' ] = 2
    self.marAna.workflow_commons[ 'OI_1_eventsPerBackgroundFile' ] = 1

    self.marAna.workflow_commons[ 'OI_2_processorName' ] = 'P'
    self.marAna.workflow_commons[ 'OI_1_processorName' ] = 'G'

    expectedTuple = [ ('g', 1, 'G'), ('p', 2, 'P') ]
    result = self.marAna._checkRunOverlay()
    assertDiracSucceedsWith_equals( result, expectedTuple, self )
