"""
Unit tests for the WhizardAnalysis module
"""

import unittest
import os
from mock import call, mock_open, patch, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Workflow.Modules.WhizardAnalysis import WhizardAnalysis
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, assertDiracSucceeds, assertDiracSucceedsWith, assertDiracSucceedsWith_equals

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Workflow.Modules.WhizardAnalysis'

class WhizardAnalysisTestCase( unittest.TestCase ):
  """ Base class for the WhizardAnalysis test cases
  """

  def setUp( self ):
    self.wha = WhizardAnalysis()
    self.wha.platform = 'myTestPlatform'
    self.wha.applicationLog = '/mydir/AppLog/test.txt'
    self.wha.workflowStatus = S_OK()
    self.wha.stepStatus = S_OK()

#  def test_obtain_processlist( self ):
#    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value='/some/path/testme.txt')) as getval_mock, \
#         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)) as exists_mock:
#      result = self.wha.obtainProcessList()
#      assertDiracSucceeds( result, self )

  def atest_runit( self ):
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(side_effect=[ '' ])) as getval_mock, \
         patch('%s.' % MODULE_NAME, new=Mock(return_value=1)), \
         patch('%s.' % MODULE_NAME, new=Mock(return_value=1)), \
         patch('%s.' % MODULE_NAME, new=Mock(return_value=1)), \
         patch('%s.' % MODULE_NAME, new=Mock(return_value=1)), \
         patch('%s.' % MODULE_NAME, new=Mock(return_value=1)), \
         patch('%s.' % MODULE_NAME, new=Mock(return_value=1)), \
         patch('%s.' % MODULE_NAME, new=Mock(return_value=1)):
      result = self.wha.runIt()
      # TO_MOCK:
      # getSoftwareFolder mit sideeffect fuer 1 call+resolveDeps
      # removeLibc
      # getNewLDLibs
      # resolveDeps
      # os.environ
      # os.path.isdir
      # Operations.getValue
      # os.path.exists
      # self.processList.getInFile
      # shutil.copy
      # os.path.exists
      # WhizardOptions, -.changeAndReturn, -.toWhizardDotIn
      # prepareWhizardFile(+ template)
      # os.remove
      # open (script from runIt, cutf from makeWhizardDotCut)
      # os.chmod
      # shellCall
      # options.getAsDict
      # glob.glob
      # os.rename

      assertDiracSucceeds( result, self )


  def test_runit_no_platform( self ):
    self.wha.platform = None
    assertDiracFailsWith( self.wha.runIt(), 'no ilc platform selected', self )

  def test_runit_no_logfile( self ):
    self.wha.applicationLog = None
    assertDiracFailsWith( self.wha.runIt(), 'no log file provided', self )

  def test_runit_workflowstatus_bad( self ):
    self.wha.workflowStatus = S_ERROR('workflow_err_testme')
    assertDiracSucceedsWith_equals( self.wha.runIt(), 'Whizard should not proceed as previous step did not end properly', self )

  def test_runit_stepstatus_bad( self ):
    self.wha.stepStatus = S_ERROR('step_err_testme')
    assertDiracSucceedsWith_equals( self.wha.runIt(), 'Whizard should not proceed as previous step did not end properly', self )

  def test_runit_getsoftwarefolder_fails( self ):
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(return_value=S_ERROR('get_software_network_test_error'))) as getsoft_mock, \
         patch('%s.WhizardAnalysis.setApplicationStatus' % MODULE_NAME) as appstat_mock:
      assertDiracFailsWith( self.wha.runIt(), 'get_software_network_test_error', self )
      appstat_mock.assert_called_once_with( 'Failed finding software' )
      getsoft_mock.assert_called_once_with( 'myTestPlatform', 'whizard', '' )

  def test_runit_getdependencies_fails( self ):
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(side_effect=[ S_OK('my/test/soft/dir'), S_OK('test/dep/folder/'), S_ERROR('something_fails_test') ])) as getsoft_mock, \
         patch('%s.removeLibc' % MODULE_NAME, new=Mock(return_value=True)) as removlib_mock, \
         patch('%s.getNewLDLibs' % MODULE_NAME, new=Mock(return_value='my/lib/path')) as getlibs_mock, \
         patch('%s.resolveDeps' % MODULE_NAME, new=Mock(return_value=[ { 'testdep1' : True, 'app' : 'mytestdep', 'version' : '4.2' }, { 'failsomethingonthisdep' : True, 'app' : 'faulty_dep_testme', 'version' : 'invalid' } ])) as resolvdep_mock, \
         patch('%s.WhizardAnalysis.setApplicationStatus' % MODULE_NAME) as setappstatus_mock:
      assertDiracFailsWith( self.wha.runIt(), 'something_fails_test', self )
      setappstatus_mock.assert_called_once_with('Failed finding software')
      getlibs_mock.assert_called_once_with( 'myTestPlatform', 'whizard', '' )
      resolvdep_mock.assert_called_once_with( 'myTestPlatform', 'whizard', '' )
      removlib_mock.assert_called_once_with( 'my/test/soft/dir/lib' )
      getsoft_mock.assert_any_call( 'myTestPlatform', 'whizard', '' )
      getsoft_mock.assert_any_call( 'myTestPlatform', 'mytestdep', '4.2' )
      getsoft_mock.assert_any_call( 'myTestPlatform', 'faulty_dep_testme', 'invalid' )
      assertEqualsImproved( len(getsoft_mock.mock_calls), 3, self )

  def test_runit_whizardin_fails( self ):
    self.wha.useGridFiles = True
    self.wha.getProcessInFile = True
    getops_mock = Mock()
    getops_mock.getValue.return_value='/my/process/list/list.txt'
    self.wha.ops = getops_mock
    method_mock = Mock()
    method_mock.getInFile.return_value = False
    proclist_mock = Mock( return_value = method_mock )
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(side_effect=[ S_OK('my/test/soft/dir'), S_OK('/my/test/dep/ignorethis'), S_OK('mygridfiles/folder'), S_OK('/spectra/files') ])), \
         patch('%s.removeLibc' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.getNewLDLibs' % MODULE_NAME, new=Mock(return_value='my/lib/path')), \
         patch('%s.resolveDeps' % MODULE_NAME, new=Mock(return_value=[ { 'app' : 'mytestdep', 'version' : '4.2' }, { 'app' : 'gridfiles', 'version' : '1.0' }, { 'app' : 'beam_spectra', 'version' : '20.3' } ])), \
         patch.dict('os.environ', {}, True), \
         patch('%s.os.listdir' % MODULE_NAME, new=Mock(side_effect=[ [ 'gridfile1.txt', 'cool/gridfile2.ppt', 'last/file/ok' ], [] ])), \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.glob.glob' % MODULE_NAME, new=Mock(side_effect=[ [ 'file1.grb', 'otherfile.grb' ], [ 'testfile.grc' ] ])), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)) as exists_mock, \
         patch('%s.ProcessList' % MODULE_NAME, new=proclist_mock), \
         patch('%s.WhizardAnalysis.setApplicationStatus' % MODULE_NAME) as appstat_mock:
      assertDiracFailsWith( self.wha.runIt(), 'error while resolving whizard input file', self )
      getops_mock.getValue.assert_called_once_with( '/ProcessList/Location', '' )
      exists_mock.assert_called_once_with( 'list.txt' )
      appstat_mock.assert_called_once_with('Whizard input file was not found')
      assertEqualsImproved( EXPECTED_TEST_ENVIRON, os.environ, self )

  def test_runit_getprocesslist_fails( self ):
    self.wha.useGridFiles = True
    self.wha.getProcessInFile = True
    getops_mock = Mock()
    getops_mock.getValue.return_value=''
    self.wha.ops = getops_mock
    method_mock = Mock()
    method_mock.getInFile.return_value = False
    proclist_mock = Mock( return_value = method_mock )
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(side_effect=[ S_OK('my/test/soft/dir'), S_OK('/my/test/dep/ignorethis'), S_OK('mygridfiles/folder'), S_OK('/spectra/files') ])), \
         patch('%s.removeLibc' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.getNewLDLibs' % MODULE_NAME, new=Mock(return_value='my/lib/path')), \
         patch('%s.resolveDeps' % MODULE_NAME, new=Mock(return_value=[ { 'app' : 'mytestdep', 'version' : '4.2' }, { 'app' : 'gridfiles', 'version' : '1.0' }, { 'app' : 'beam_spectra', 'version' : '20.3' } ])), \
         patch.dict('os.environ', {}, True), \
         patch('%s.os.listdir' % MODULE_NAME, new=Mock(side_effect=[ [ 'gridfile1.txt', 'cool/gridfile2.ppt', 'last/file/ok' ], [] ])), \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.glob.glob' % MODULE_NAME, new=Mock(side_effect=[ [ 'file1.grb', 'otherfile.grb' ], [ 'testfile.grc' ] ])), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)) as exists_mock, \
         patch('%s.ProcessList' % MODULE_NAME, new=proclist_mock), \
         patch('%s.WhizardAnalysis.setApplicationStatus' % MODULE_NAME) as appstat_mock:
      assertDiracFailsWith( self.wha.runIt(), 'no process list found', self )
      getops_mock.getValue.assert_called_once_with( '/ProcessList/Location', '' )
      self.assertFalse( exists_mock.called )
      appstat_mock.assert_called_once_with('Failed getting processlist')

  def test_runit_leshouchesfile_missing( self ):
    self.wha.useGridFiles = True
    self.wha.getProcessInFile = True
    self.wha.Model = True
    self.wha.susymodel = False
    genmodel_mock = Mock()
    genmodel_mock.hasModel.return_value = S_OK()
    genmodel_mock.getFile.return_value = S_OK( 'myTestGotFile' )
    self.wha.genmodel = genmodel_mock
    getops_mock = Mock()
    getops_mock.getValue.return_value='/my/process/list/list.txt'
    self.wha.ops = getops_mock
    method_mock = Mock()
    method_mock.getInFile.return_value = 'mywhizardTestFile.in'
    proclist_mock = Mock( return_value = method_mock )
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(side_effect=[ S_OK('my/test/soft/dir'), S_OK('/my/test/dep/ignorethis'), S_OK('mygridfiles/folder'), S_OK('/spectra/files') ])), \
         patch('%s.removeLibc' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.getNewLDLibs' % MODULE_NAME, new=Mock(return_value='my/lib/path')), \
         patch('%s.resolveDeps' % MODULE_NAME, new=Mock(return_value=[ { 'app' : 'mytestdep', 'version' : '4.2' }, { 'app' : 'gridfiles', 'version' : '1.0' }, { 'app' : 'beam_spectra', 'version' : '20.3' } ])), \
         patch.dict('os.environ', {}, True), \
         patch('%s.os.listdir' % MODULE_NAME, new=Mock(side_effect=[ [ 'gridfile1.txt', 'cool/gridfile2.ppt', 'last/file/ok' ], [] ])), \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.glob.glob' % MODULE_NAME, new=Mock(side_effect=[ [ 'file1.grb', 'otherfile.grb' ], [ 'testfile.grc' ] ])), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ True, False, False ])) as exists_mock, \
         patch('%s.ProcessList' % MODULE_NAME, new=proclist_mock), \
         patch('%s.WhizardAnalysis.setApplicationStatus' % MODULE_NAME) as appstat_mock, \
         patch('%s.shutil.copy' % MODULE_NAME, new=Mock(return_value=True)) as copy_mock:
      assertDiracFailsWith( self.wha.runIt(), 'the leshouches file was not found', self )
      getops_mock.getValue.assert_called_once_with( '/ProcessList/Location', '' )
      exists_mock.assert_any_call( 'list.txt' )
      exists_mock.assert_any_call( 'LesHouches.msugra_1.in' )
      exists_mock.assert_any_call( 'my/test/soft/dir/myTestGotFile' )
      assertEqualsImproved( len(exists_mock.mock_calls), 3, self )
      appstat_mock.assert_called_once_with( 'LesHouches file missing' )
      copy_mock.assert_called_once_with( 'my/test/soft/dir/mywhizardTestFile.in', './whizardnew.in' )
      genmodel_mock.hasModel.assert_called_once_with( True )
      genmodel_mock.getFile.assert_any_call( True )
      assertEqualsImproved( len(genmodel_mock.mock_calls), 3, self )

  def test_runit_model_undefined( self ):
    self.wha.useGridFiles = True
    self.wha.getProcessInFile = True
    self.wha.Model = True
    self.wha.susymodel = False
    genmodel_mock = Mock()
    genmodel_mock.hasModel.return_value = S_ERROR()
    genmodel_mock.getFile.return_value = S_OK( 'myTestGotFile' )
    self.wha.genmodel = genmodel_mock
    getops_mock = Mock()
    getops_mock.getValue.return_value='/my/process/list/list.txt'
    self.wha.ops = getops_mock
    method_mock = Mock()
    method_mock.getInFile.return_value = 'mywhizardTestFile.in'
    proclist_mock = Mock( return_value = method_mock )
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(side_effect=[ S_OK('my/test/soft/dir'), S_OK('/my/test/dep/ignorethis'), S_OK('mygridfiles/folder'), S_OK('/spectra/files') ])), \
         patch('%s.removeLibc' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.getNewLDLibs' % MODULE_NAME, new=Mock(return_value='my/lib/path')), \
         patch('%s.resolveDeps' % MODULE_NAME, new=Mock(return_value=[ { 'app' : 'mytestdep', 'version' : '4.2' }, { 'app' : 'gridfiles', 'version' : '1.0' }, { 'app' : 'beam_spectra', 'version' : '20.3' } ])), \
         patch.dict('os.environ', {}, True), \
         patch('%s.os.listdir' % MODULE_NAME, new=Mock(side_effect=[ [ 'gridfile1.txt', 'cool/gridfile2.ppt', 'last/file/ok' ], [] ])), \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.glob.glob' % MODULE_NAME, new=Mock(side_effect=[ [ 'file1.grb', 'otherfile.grb' ], [ 'testfile.grc' ] ])), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ True, False, False ])) as exists_mock, \
         patch('%s.ProcessList' % MODULE_NAME, new=proclist_mock), \
         patch('%s.WhizardAnalysis.setApplicationStatus' % MODULE_NAME) as appstat_mock, \
         patch('%s.shutil.copy' % MODULE_NAME, new=Mock(return_value=True)) as copy_mock:
      assertDiracFailsWith( self.wha.runIt(), 'no model true defined', self )
      getops_mock.getValue.assert_called_once_with( '/ProcessList/Location', '' )
      exists_mock.assert_any_call( 'list.txt' )
      exists_mock.assert_any_call( 'LesHouches.msugra_1.in' )
      assertEqualsImproved( len(exists_mock.mock_calls), 2, self )
      appstat_mock.assert_called_once_with( 'Model undefined' )
      copy_mock.assert_called_once_with( 'my/test/soft/dir/mywhizardTestFile.in', './whizardnew.in' )
      genmodel_mock.hasModel.assert_called_once_with( True )
      self.assertFalse( genmodel_mock.called )

  def test_runit_copy_fails( self ):
    self.wha.useGridFiles = True
    self.wha.getProcessInFile = True
    getops_mock = Mock()
    getops_mock.getValue.return_value='/my/process/list/list.txt'
    self.wha.ops = getops_mock
    method_mock = Mock()
    method_mock.getInFile.return_value = 'mywhizardTestFile.in'
    proclist_mock = Mock( return_value = method_mock )
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(side_effect=[ S_OK('my/test/soft/dir'), S_OK('/my/test/dep/ignorethis'), S_OK('mygridfiles/folder'), S_OK('/spectra/files') ])), \
         patch('%s.removeLibc' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.getNewLDLibs' % MODULE_NAME, new=Mock(return_value='my/lib/path')), \
         patch('%s.resolveDeps' % MODULE_NAME, new=Mock(return_value=[ { 'app' : 'mytestdep', 'version' : '4.2' }, { 'app' : 'gridfiles', 'version' : '1.0' }, { 'app' : 'beam_spectra', 'version' : '20.3' } ])), \
         patch.dict('os.environ', {}, True), \
         patch('%s.os.listdir' % MODULE_NAME, new=Mock(side_effect=[ [ 'gridfile1.txt', 'cool/gridfile2.ppt', 'last/file/ok' ], [] ])), \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.glob.glob' % MODULE_NAME, new=Mock(side_effect=[ [ 'file1.grb', 'otherfile.grb' ], [ 'testfile.grc' ] ])), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ True ])) as exists_mock, \
         patch('%s.ProcessList' % MODULE_NAME, new=proclist_mock), \
         patch('%s.WhizardAnalysis.setApplicationStatus' % MODULE_NAME) as appstat_mock, \
         patch('%s.shutil.copy' % MODULE_NAME, new=Mock(side_effect=EnvironmentError('some_err_msg_test'))):
      assertDiracFailsWith( self.wha.runIt(), 'failed to obtain mywhizardtestfile.in', self )
      getops_mock.getValue.assert_called_once_with( '/ProcessList/Location', '' )
      exists_mock.assert_any_call( 'list.txt' )
      assertEqualsImproved( len(exists_mock.mock_calls), 1, self )
      appstat_mock.assert_called_once_with('Failed getting whizard.in file')

  def test_runit_changeandreturn_fails( self ):
    self.wha.useGridFiles = True
    self.wha.getProcessInFile = True
    self.wha.Model = 'mytestMODEL'
    self.wha.optionsdict = 9834
    genmodel_mock = Mock()
    self.wha.genmodel = genmodel_mock
    changeandret_mock = Mock()
    changeandret_mock.changeAndReturn.return_value = S_ERROR('changeandret_err_testme')
    whiz_options_mock = Mock( return_value = changeandret_mock )
    getops_mock = Mock()
    getops_mock.getValue.return_value='/my/process/list/list.txt'
    self.wha.ops = getops_mock
    method_mock = Mock()
    method_mock.getInFile.return_value = 'mywhizardTestFile.in'
    proclist_mock = Mock( return_value = method_mock )
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(side_effect=[ S_OK('my/test/soft/dir'), S_OK('/my/test/dep/ignorethis'), S_OK('mygridfiles/folder'), S_OK('/spectra/files') ])), \
         patch('%s.removeLibc' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.getNewLDLibs' % MODULE_NAME, new=Mock(return_value='my/lib/path')), \
         patch('%s.resolveDeps' % MODULE_NAME, new=Mock(return_value=[ { 'app' : 'mytestdep', 'version' : '4.2' }, { 'app' : 'gridfiles', 'version' : '1.0' }, { 'app' : 'beam_spectra', 'version' : '20.3' } ])), \
         patch.dict('os.environ', {}, True), \
         patch('%s.os.listdir' % MODULE_NAME, new=Mock(side_effect=[ [ 'gridfile1.txt', 'cool/gridfile2.ppt', 'last/file/ok' ], [] ])), \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.glob.glob' % MODULE_NAME, new=Mock(side_effect=[ [ 'file1.grb', 'otherfile.grb' ], [ 'testfile.grc' ] ])), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ True, True ])) as exists_mock, \
         patch('%s.ProcessList' % MODULE_NAME, new=proclist_mock), \
         patch('%s.WhizardAnalysis.setApplicationStatus' % MODULE_NAME) as appstat_mock, \
         patch('%s.shutil.copy' % MODULE_NAME, new=Mock(return_value=True)) as copy_mock, \
         patch('%s.WhizardOptions' % MODULE_NAME, new=whiz_options_mock):
      assertDiracFailsWith( self.wha.runIt(), 'changeandret_err_testme', self )
      getops_mock.getValue.assert_called_once_with( '/ProcessList/Location', '' )
      exists_mock.assert_any_call( 'list.txt' )
      exists_mock.assert_any_call( 'LesHouches.msugra_1.in' )
      assertEqualsImproved( len(exists_mock.mock_calls), 2, self )
      self.assertFalse( appstat_mock.called )
      copy_mock.assert_called_once_with( 'my/test/soft/dir/mywhizardTestFile.in', './whizardnew.in' )
      self.assertFalse( genmodel_mock.called )
      changeandret_mock.changeAndReturn.assert_called_once_with( 9834 )

  def test_runit_prepareWhizardFile_fails( self ):
    self.wha.useGridFiles = True
    self.wha.getProcessInFile = True
    self.wha.Model = True
    self.wha.susymodel = False
    self.wha.optionsdict = False
    self.wha.template = False
    self.wha.evttype = 'myTestEvent'
    self.wha.energy = '100TestTeV'
    self.wha.RandomSeed = 'notRandomTestme'
    self.wha.NumberOfEvents = 'myTestNumber'
    self.wha.Lumi = 'mytestluminosity'
    genmodel_mock = Mock()
    self.wha.genmodel = genmodel_mock
    getops_mock = Mock()
    getops_mock.getValue.return_value='/my/process/list/list.txt'
    self.wha.ops = getops_mock
    method_mock = Mock()
    method_mock.getInFile.return_value = 'mywhizardTestFile.in'
    proclist_mock = Mock( return_value = method_mock )
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(side_effect=[ S_OK('my/test/soft/dir'), S_OK('/my/test/dep/ignorethis'), S_OK('mygridfiles/folder'), S_OK('/spectra/files') ])), \
         patch('%s.removeLibc' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.getNewLDLibs' % MODULE_NAME, new=Mock(return_value='my/lib/path')), \
         patch('%s.resolveDeps' % MODULE_NAME, new=Mock(return_value=[ { 'app' : 'mytestdep', 'version' : '4.2' }, { 'app' : 'gridfiles', 'version' : '1.0' }, { 'app' : 'beam_spectra', 'version' : '20.3' } ])), \
         patch.dict('os.environ', {}, True), \
         patch('%s.os.listdir' % MODULE_NAME, new=Mock(side_effect=[ [ 'gridfile1.txt', 'cool/gridfile2.ppt', 'last/file/ok' ], [] ])), \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.glob.glob' % MODULE_NAME, new=Mock(side_effect=[ [ 'file1.grb', 'otherfile.grb' ], [ 'testfile.grc' ] ])), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ True, True ])) as exists_mock, \
         patch('%s.ProcessList' % MODULE_NAME, new=proclist_mock), \
         patch('%s.WhizardAnalysis.setApplicationStatus' % MODULE_NAME) as appstat_mock, \
         patch('%s.shutil.copy' % MODULE_NAME, new=Mock(return_value=True)) as copy_mock, \
         patch('%s.prepareWhizardFile' % MODULE_NAME, new=Mock(return_value=S_ERROR('preparewhiz_no_template_test_err'))) as preparefile_mock :
      assertDiracFailsWith( self.wha.runIt(), 'something went wrong with whizard.in file generation', self )
      getops_mock.getValue.assert_called_once_with( '/ProcessList/Location', '' )
      exists_mock.assert_any_call( 'list.txt' )
      exists_mock.assert_any_call( 'LesHouches.msugra_1.in' )
      assertEqualsImproved( len(exists_mock.mock_calls), 2, self )
      appstat_mock.assert_called_once_with( 'Whizard: something went wrong with input file generation' )
      copy_mock.assert_called_once_with( 'my/test/soft/dir/mywhizardTestFile.in', './whizardnew.in' )
      self.assertFalse( genmodel_mock.called )
      preparefile_mock.assert_called_once_with('whizardnew.in', 'myTestEvent', '100TestTeV','notRandomTestme','myTestNumber','mytestluminosity', 'whizard.in' )

  def test_runit_prepareWhizardFileTemplate_fails( self ):
    self.wha.useGridFiles = True
    self.wha.getProcessInFile = True
    self.wha.Model = True
    self.wha.susymodel = False
    self.wha.optionsdict = False
    self.wha.SteeringFile = 'templateTestme123'
    self.wha.template = True
    self.wha.evttype = 'myTestEvent'
    self.wha.parameters = 'myTESTParams'
    genmodel_mock = Mock()
    self.wha.genmodel = genmodel_mock
    getops_mock = Mock()
    getops_mock.getValue.return_value='/my/process/list/list.txt'
    self.wha.ops = getops_mock
    method_mock = Mock()
    method_mock.getInFile.return_value = 'mywhizardTestFile.in'
    proclist_mock = Mock( return_value = method_mock )
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(side_effect=[ S_OK('my/test/soft/dir'), S_OK('/my/test/dep/ignorethis'), S_OK('mygridfiles/folder'), S_OK('/spectra/files') ])), \
         patch('%s.removeLibc' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.getNewLDLibs' % MODULE_NAME, new=Mock(return_value='my/lib/path')), \
         patch('%s.resolveDeps' % MODULE_NAME, new=Mock(return_value=[ { 'app' : 'mytestdep', 'version' : '4.2' }, { 'app' : 'gridfiles', 'version' : '1.0' }, { 'app' : 'beam_spectra', 'version' : '20.3' } ])), \
         patch.dict('os.environ', {}, True), \
         patch('%s.os.listdir' % MODULE_NAME, new=Mock(side_effect=[ [ 'gridfile1.txt', 'cool/gridfile2.ppt', 'last/file/ok' ], [] ])), \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.glob.glob' % MODULE_NAME, new=Mock(side_effect=[ [ 'file1.grb', 'otherfile.grb' ], [ 'testfile.grc' ] ])), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ True, True ])) as exists_mock, \
         patch('%s.ProcessList' % MODULE_NAME, new=proclist_mock), \
         patch('%s.WhizardAnalysis.setApplicationStatus' % MODULE_NAME) as appstat_mock, \
         patch('%s.shutil.copy' % MODULE_NAME, new=Mock(return_value=True)) as copy_mock, \
         patch('%s.prepareWhizardFileTemplate' % MODULE_NAME, new=Mock(return_value=S_ERROR('preparewhiz_no_template_test_err'))) as preparetemplate_mock :
      assertDiracFailsWith( self.wha.runIt(), 'something went wrong with whizard.in file generation', self )
      getops_mock.getValue.assert_called_once_with( '/ProcessList/Location', '' )
      exists_mock.assert_any_call( 'list.txt' )
      exists_mock.assert_any_call( 'LesHouches.msugra_1.in' )
      assertEqualsImproved( len(exists_mock.mock_calls), 2, self )
      appstat_mock.assert_called_once_with( 'Whizard: something went wrong with input file generation' )
      copy_mock.assert_called_once_with( 'my/test/soft/dir/mywhizardTestFile.in', './whizardnew.in' )
      self.assertFalse( genmodel_mock.called )
      preparetemplate_mock.assert_called_once_with('whizardnew.in', 'myTestEvent', 'myTESTParams', 'whizard.in')

  def test_runit_makecut1_fails( self ):
    self.wha.useGridFiles = True
    self.wha.getProcessInFile = True
    self.wha.Model = 'mytestMODEL'
    self.wha.optionsdict = 9834
    self.wha.applicationVersion = 'myTestV1'
    self.wha.STEP_NUMBER = 'testStep12'
    self.wha.genlevelcuts = { 'some_entry' : True, 'this_dict_is_not_empty' : True }
    genmodel_mock = Mock()
    self.wha.genmodel = genmodel_mock
    whizopts_mock = Mock()
    whizopts_mock.changeAndReturn.return_value = S_OK()
    whizopts_mock.toWhizardDotIn.return_value = S_OK( 'mytestprocessfindme' )
    whiz_options_mock = Mock( return_value = whizopts_mock )
    getops_mock = Mock()
    getops_mock.getValue.return_value='/my/process/list/list.txt'
    self.wha.ops = getops_mock
    method_mock = Mock()
    method_mock.getInFile.return_value = 'mywhizardTestFile.in'
    proclist_mock = Mock( return_value = method_mock )
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(side_effect=[ S_OK('my/test/soft/dir'), S_OK('/my/test/dep/ignorethis'), S_OK('mygridfiles/folder'), S_OK('/spectra/files') ])), \
         patch('%s.removeLibc' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.getNewLDLibs' % MODULE_NAME, new=Mock(return_value='my/lib/path')), \
         patch('%s.resolveDeps' % MODULE_NAME, new=Mock(return_value=[ { 'app' : 'mytestdep', 'version' : '4.2' }, { 'app' : 'gridfiles', 'version' : '1.0' }, { 'app' : 'beam_spectra', 'version' : '20.3' } ])), \
         patch.dict('os.environ', {}, True), \
         patch('%s.os.listdir' % MODULE_NAME, new=Mock(side_effect=[ [ 'gridfile1.txt', 'cool/gridfile2.ppt', 'last/file/ok' ], [] ])), \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.glob.glob' % MODULE_NAME, new=Mock(side_effect=[ [ 'file1.grb', 'otherfile.grb' ], [ 'testfile.grc' ] ])), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ True, True, True ])) as exists_mock, \
         patch('%s.ProcessList' % MODULE_NAME, new=proclist_mock), \
         patch('%s.WhizardAnalysis.setApplicationStatus' % MODULE_NAME) as appstat_mock, \
         patch('%s.shutil.copy' % MODULE_NAME, new=Mock(return_value=True)) as copy_mock, \
         patch('%s.WhizardOptions' % MODULE_NAME, new=whiz_options_mock), \
         patch('%s.os.remove' % MODULE_NAME, new=Mock(return_value=True)) as remove_mock, \
         patch('%s.open' % MODULE_NAME, mock_open()) as open_mock, \
         patch('%s.WhizardAnalysis.makeWhizardDotCut1' % MODULE_NAME, new=Mock(return_value=S_ERROR())):
      #FIXME: makeWhizardDotCut1 currently cant fail (although it operates on files?!)
      assertDiracFailsWith( self.wha.runIt(), 'could not create the cut1 file', self )
      getops_mock.getValue.assert_called_once_with( '/ProcessList/Location', '' )
      exists_mock.assert_any_call( 'list.txt' )
      exists_mock.assert_any_call( 'LesHouches.msugra_1.in' )
      exists_mock.assert_any_call( 'Whizard_myTestV1_Run_testStep12.sh' )
      assertEqualsImproved( len(exists_mock.mock_calls), 3, self )
      self.assertFalse( appstat_mock.called )
      copy_mock.assert_called_once_with( 'my/test/soft/dir/mywhizardTestFile.in', './whizardnew.in' )
      self.assertFalse( genmodel_mock.called )
      whizopts_mock.changeAndReturn.assert_called_once_with( 9834 )
      whizopts_mock.toWhizardDotIn.assert_called_once_with( 'whizard.in' )
      remove_mock.assert_called_once_with( 'Whizard_myTestV1_Run_testStep12.sh' )
      open_mock.assert_called_once_with('Whizard_myTestV1_Run_testStep12.sh', 'w')
      write_calls = open_mock().write.mock_calls
      expected_calls = [ call('#!/bin/sh \n'), call('#####################################################################\n'), call('# Dynamically generated script to run a production or analysis job. #\n'), call('#####################################################################\n'), call('declare -x PATH=my/test/soft/dir:$PATH\n'), call('declare -x LD_LIBRARY_PATH=my/test/soft/dir/lib:my/lib/path\n'), call('env | sort >> localEnv.log\n'), call('echo =============================\n'), call('echo Printing content of whizard.in \n'), call('cat whizard.in\n'), call('echo =============================\n'), call('cp  my/test/soft/dir/whizard.mdl ./\n'), call('ln -s LesHouches.msugra_1.in fort.71\n'), call('cp file1.grb ./\n'), call('cp otherfile.grb ./\n'), call('cp testfile.grc ./\n'), call('cp my/test/soft/dir/whizard.prc ./\n') ]
      assertEqualsImproved( write_calls, expected_calls, self )

  def test_runit_noapplog_created( self ):
    self.wha.useGridFiles = True
    self.wha.getProcessInFile = True
    self.wha.Model = 'mytestMODEL'
    self.wha.optionsdict = 9834
    self.wha.applicationVersion = 'myTestV1'
    self.wha.STEP_NUMBER = 'testStep12'
    self.wha.genlevelcuts = False
    genmodel_mock = Mock()
    self.wha.genmodel = genmodel_mock
    whizopts_mock = Mock()
    whizopts_mock.changeAndReturn.return_value = S_OK()
    whizopts_mock.toWhizardDotIn.return_value = S_OK( 'mytestprocessfindme' )
    whiz_options_mock = Mock( return_value = whizopts_mock )
    getops_mock = Mock()
    getops_mock.getValue.return_value='/my/process/list/list.txt'
    self.wha.ops = getops_mock
    method_mock = Mock()
    method_mock.getInFile.return_value = 'mywhizardTestFile.in'
    proclist_mock = Mock( return_value = method_mock )
    with patch('%s.getSoftwareFolder' % MODULE_NAME, new=Mock(side_effect=[ S_OK('my/test/soft/dir'), S_OK('/my/test/dep/ignorethis'), S_OK('mygridfiles/folder'), S_OK('/spectra/files') ])), \
         patch('%s.removeLibc' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.getNewLDLibs' % MODULE_NAME, new=Mock(return_value='my/lib/path')), \
         patch('%s.resolveDeps' % MODULE_NAME, new=Mock(return_value=[ { 'app' : 'mytestdep', 'version' : '4.2' }, { 'app' : 'gridfiles', 'version' : '1.0' }, { 'app' : 'beam_spectra', 'version' : '20.3' } ])), \
         patch.dict('os.environ', {}, True), \
         patch('%s.os.listdir' % MODULE_NAME, new=Mock(side_effect=[ [ 'gridfile1.txt', 'cool/gridfile2.ppt', 'last/file/ok' ], [] ])), \
         patch('%s.os.path.isdir' % MODULE_NAME, new=Mock(return_value=True)), \
         patch('%s.glob.glob' % MODULE_NAME, new=Mock(side_effect=[ [ 'file1.grb', 'otherfile.grb' ], [ 'testfile.grc' ] ])), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[ True, True, True ])) as exists_mock, \
         patch('%s.ProcessList' % MODULE_NAME, new=proclist_mock), \
         patch('%s.WhizardAnalysis.setApplicationStatus' % MODULE_NAME) as appstat_mock, \
         patch('%s.shutil.copy' % MODULE_NAME, new=Mock(return_value=True)) as copy_mock, \
         patch('%s.WhizardOptions' % MODULE_NAME, new=whiz_options_mock), \
         patch('%s.os.remove' % MODULE_NAME, new=Mock(return_value=True)) as remove_mock, \
         patch('%s.open' % MODULE_NAME, mock_open()) as open_mock, \
         patch('%s.WhizardAnalysis.makeWhizardDotCut1' % MODULE_NAME, new=Mock(return_value=S_ERROR())):
      #FIXME: makeWhizardDotCut1 currently cant fail (although it operates on files?!)
      assertDiracFailsWith( self.wha.runIt(), 'could not create the cut1 file', self )
      getops_mock.getValue.assert_called_once_with( '/ProcessList/Location', '' )
      exists_mock.assert_any_call( 'list.txt' )
      exists_mock.assert_any_call( 'LesHouches.msugra_1.in' )
      exists_mock.assert_any_call( 'Whizard_myTestV1_Run_testStep12.sh' )
      assertEqualsImproved( len(exists_mock.mock_calls), 3, self )
      self.assertFalse( appstat_mock.called )
      copy_mock.assert_called_once_with( 'my/test/soft/dir/mywhizardTestFile.in', './whizardnew.in' )
      self.assertFalse( genmodel_mock.called )
      whizopts_mock.changeAndReturn.assert_called_once_with( 9834 )
      whizopts_mock.toWhizardDotIn.assert_called_once_with( 'whizard.in' )
      remove_mock.assert_called_once_with( 'Whizard_myTestV1_Run_testStep12.sh' )
      open_mock.assert_called_once_with('Whizard_myTestV1_Run_testStep12.sh', 'w')
      write_calls = open_mock().write.mock_calls
      expected_calls = [ call('#!/bin/sh \n'), call('#####################################################################\n'), call('# Dynamically generated script to run a production or analysis job. #\n'), call('#####################################################################\n'), call('declare -x PATH=my/test/soft/dir:$PATH\n'), call('declare -x LD_LIBRARY_PATH=my/test/soft/dir/lib:my/lib/path\n'), call('env | sort >> localEnv.log\n'), call('echo =============================\n'), call('echo Printing content of whizard.in \n'), call('cat whizard.in\n'), call('echo =============================\n'), call('cp  my/test/soft/dir/whizard.mdl ./\n'), call('ln -s LesHouches.msugra_1.in fort.71\n'), call('cp file1.grb ./\n'), call('cp otherfile.grb ./\n'), call('cp testfile.grc ./\n'), call('cp my/test/soft/dir/whizard.prc ./\n') ]
      assertEqualsImproved( write_calls, expected_calls, self )





















# TODO add case with empty deps

EXPECTED_TEST_ENVIRON = { 'LUMI_LINKER': '/spectra/files/lumi_linker_000', \
                          'PHOTONS_B1': '/spectra/files/photons_beam1_linker_000', \
                          'PHOTONS_B2': '/spectra/files/photons_beam2_linker_000', \
                          'EBEAM': '/spectra/files/ebeam_in_linker_000', \
                          'PBEAM': '/spectra/files/pbeam_in_linker_000', \
                          'LUMI_EE_LINKER': '/spectra/files/lumi_ee_linker_000', \
                          'LUMI_EG_LINKER': '/spectra/files/lumi_eg_linker_000', \
                          'LUMI_GE_LINKER': '/spectra/files/lumi_ge_linker_000', \
                          'LUMI_GG_LINKER': '/spectra/files/lumi_gg_linker_000' }












