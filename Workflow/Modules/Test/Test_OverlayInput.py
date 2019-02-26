#!/usr/bin/env python
"""Test the OverlayInput WorkflowModule"""

from __future__ import print_function
import os
import shutil
import tempfile
import unittest
from mock import patch, mock_open, call, MagicMock as Mock

from DIRAC import gLogger, S_OK, S_ERROR
from ILCDIRAC.Workflow.Modules.OverlayInput import OverlayInput
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, \
  assertDiracFailsWith, assertDiracSucceeds, assertDiracSucceedsWith, \
  assertDiracSucceedsWith_equals
from ILCDIRAC.Tests.Utilities.FileUtils import FileUtil

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Workflow.Modules.OverlayInput'
MODULEBASE_NAME = 'ILCDIRAC.Workflow.Modules.ModuleBase'

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

@patch("%s.getProxyInfoAsString" % MODULEBASE_NAME, new=Mock(return_value=S_OK()))
@patch("DIRAC.Core.Security.ProxyInfo.getProxyInfoAsString", new=Mock(return_value=S_OK()))
@patch("%s.FileCatalogClient" % MODULE_NAME, new=Mock(return_value=S_OK()))
@patch("%s.Operations" % MODULE_NAME, new=Mock(return_value=S_OK()))
@patch("%s.DataManager" % MODULE_NAME, new=Mock(return_value=S_OK()))
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

  @patch("%s.shellCall" % MODULE_NAME, new=Mock(side_effect=createFile))
  def test_overlayinput_getEosFile_lfn_success( self ):
    """ test success when getting an lfn to copy from eos """
    testLFN = "/lfn/to/overlay/overlayFile.slcio"
    res = self.over.getEOSFile( testLFN )
    print(res)
    print("self result", self.over.result)
    assertDiracSucceedsWith_equals( res, os.path.basename( testLFN ), self )
    with open("overlayinput.sh") as overscript:
      self.assertIn( "xrdcp -s root://eospublic.cern.ch//eos/experiment/clicdp/grid%s" % testLFN , overscript.read() )

  @patch("%s.shellCall" % MODULE_NAME, new=Mock(side_effect=createFile))
  def test_overlayinput_getEosFile_fullpath_success( self ):
    """ test that we don't predent if we get a fullpath for eos, however that might happen"""
    testLFN = "/eos/experiment/clicdp/grid/lfn/to/overlay/overlayFile.slcio"
    res = self.over.getEOSFile( testLFN )
    print(res)
    print("self result", self.over.result)
    assertDiracSucceedsWith_equals( res, os.path.basename( testLFN ), self )
    with open("overlayinput.sh") as overscript:
      self.assertIn( "xrdcp -s root://eospublic.cern.ch/%s" % testLFN , overscript.read() )

  @patch("%s.shellCall" % MODULE_NAME, new=Mock())
  def test_overlayinput_getEosFile_lfn_failure( self ):
    """ test failure of copy command, that is no ouputfile present after copying """
    testLFN = "/lfn/to/overlay/overlayFile.slcio"
    res = self.over.getEOSFile( testLFN )
    print(res)
    print("self result", self.over.result)
    assertDiracFailsWith( res, 'Failed', self )
    with open("overlayinput.sh") as overscript:
      self.assertIn( "xrdcp -s root://eospublic.cern.ch//eos/experiment/clicdp/grid%s" % testLFN , overscript.read() )

#pylint: disable=too-many-public-methods
class TestOverlayUnittests( unittest.TestCase ):
  """ Tests the Overlayinput class
  """

  GOOD_EXIT = 'Input variables resolved'
  def setUp( self ):
    self.over = OverlayInput()
    self.over.detectormodel = 'testdetectorv2000'
    self.over.energytouse = '200TeV'
    self.over.BXOverlay = 100
    self.over.NbSigEvtsPerJob = 20

  def test_applicationSpecificInputs( self ):
    # Compare to this. Get outside of patch, else constructor fails
    reference = OverlayInput()
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value=2)):
      result = self.over.applicationSpecificInputs()
      assertDiracSucceedsWith_equals( result, TestOverlayUnittests.GOOD_EXIT, self )
      # Assert nothing has been changed, except the values in setUp (and DataManager/FileCatalogClient since theyre created anew for every object)
      assertEqualsImproved((self.over.enable, self.over.STEP_NUMBER, self.over.applicationName,
                            self.over.curdir, self.over.applicationLog, self.over.printoutflag,
                            self.over.prodid, self.over.detector, self.over.energy, self.over.nbofeventsperfile,
                            self.over.lfns, self.over.nbfilestoget, self.over.BkgEvtType, self.over.ggtohadint,
                            self.over.nbsigeventsperfile, self.over.nbinputsigfile, self.over.site,
                            self.over.useEnergyForFileLookup, self.over.machine, self.over.pathToOverlayFiles,
                            ), (reference.enable, reference.STEP_NUMBER, reference.applicationName,
                                reference.curdir, reference.applicationLog, reference.printoutflag, reference.prodid,
                                reference.detector, reference.energy, reference.nbofeventsperfile, reference.lfns,
                                reference.nbfilestoget, reference.BkgEvtType, reference.ggtohadint,
                                reference.nbsigeventsperfile, reference.nbinputsigfile, reference.site,
                                reference.useEnergyForFileLookup, reference.machine, reference.pathToOverlayFiles,
                                ), self)
      if self.over.fcc is None:
        self.fail('FCC not initialized')
      if self.over.datMan is None:
        self.fail('DataManager not initialized')

  def test_applicationSpecificInputs_nodetector( self ):
    self.over.detectormodel = ''
    assertDiracFailsWith( self.over.applicationSpecificInputs(),
                          'detector model not defined', self )

  def test_applicationSpecificInputs_noenergy( self ):
    self.over.energytouse = ''
    assertDiracFailsWith( self.over.applicationSpecificInputs(),
                          'energy not set', self )

  def test_applicationSpecificInputs_nobxoverlay( self ):
    self.over.BXOverlay = 0
    assertDiracFailsWith( self.over.applicationSpecificInputs(),
                          'bxoverlay parameter not defined', self )

  def test_applicationSpecificInputs_energyset_1( self ):
    self.over.energytouse = ''
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value=2)):
      self.over.energy = 123
      result = self.over.applicationSpecificInputs()
      assertDiracSucceedsWith_equals( result, TestOverlayUnittests.GOOD_EXIT, self )
      assertEqualsImproved( self.over.energytouse, '123gev', self )
      self.over.energy = 6800
      result = self.over.applicationSpecificInputs()
      assertDiracSucceedsWith_equals( result, TestOverlayUnittests.GOOD_EXIT, self )
      assertEqualsImproved( self.over.energytouse, '6.8tev', self )
      self.over.energy = 100000
      result = self.over.applicationSpecificInputs()
      assertDiracSucceedsWith_equals( result, TestOverlayUnittests.GOOD_EXIT, self )
      assertEqualsImproved( self.over.energytouse, '100tev', self )
      self.over.energy = 123.0
      result = self.over.applicationSpecificInputs()
      assertDiracSucceedsWith_equals( result, TestOverlayUnittests.GOOD_EXIT, self )
      assertEqualsImproved( self.over.energytouse, '123gev', self )

  def test_applicationSpecificInputs_with_setters( self ):
    tmp_dict = { 'Detector' : 'othertestdetectorv3000', 'Energy' : '10000GeV',
                 'BXOverlay' : '651', 'ggtohadint' : 9.5, 'ProdID' : 429875,
                 'NbSigEvtsPerJob' : 94, 'BkgEvtType' : 'bgoijaf',
                 'STEP_NUMBER':1,
               }
    self.over.step_commons = tmp_dict
    self.over.InputData = [ 'abc' ]
    self.over.NumberOfEvents = 15
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value=2)):
      result = self.over.applicationSpecificInputs()
      assertDiracSucceedsWith_equals( result, TestOverlayUnittests.GOOD_EXIT, self )
      assertEqualsImproved( (
        self.over.detectormodel, self.over.energytouse, self.over.BXOverlay,
        self.over.ggtohadint, self.over.prodid, self.over.NbSigEvtsPerJob,
        self.over.BkgEvtType ), ( 'othertestdetectorv3000', '10000GeV', '651',
                                  9.5, 429875, 94, 'bgoijaf' ), self )
      assertEqualsImproved( self.over.nbsigeventsperfile, 15, self )
      assertEqualsImproved( self.over.nbinputsigfile, 1, self )

  def test_applicationSpecificInputs_nonbevts( self ):
    self.over.InputData = [ 'abc' ]
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value=2)):
      result = self.over.applicationSpecificInputs()
      assertDiracFailsWith( result, 'number of events in the signal file is missing', self )

  def test_applicationSpecificInputs_nonbsigevts( self ):
    self.over.NbSigEvtsPerJob = 0
    self.over.nbsigeventsperfile = 0
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value=2)):
      result = self.over.applicationSpecificInputs()
      assertDiracFailsWith( result, 'could not determine the number of signal events per input file',
                            self )

  def test_applicationSpecificInputs_allowedBkg_rarepath( self ):
    self.over.pathToOverlayFiles = 'some_path.txt'
    self.over.energy = self.over.energytouse = 0
    self.over.detector = self.over.detectormodel = ''
    result = self.over.applicationSpecificInputs()
    assertDiracSucceeds( result, self )

  def test_applicationSpecificInputs_allowedBkgFails_1( self ):
    self.over.pathToOverlayFiles = 'some_path.txt'
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value=-1)):
      result = self.over.applicationSpecificInputs()
      assertDiracFailsWith( result, 'no background to overlay', self )

  def test_applicationSpecificInputs_allowedBkgFails_2( self ):
    self.over.pathToOverlayFiles = 'some_path.txt'
    self.over.detectormodel = ''
    self.over.detector = 'supergooddetectorv2'
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value=-1)):
      result = self.over.applicationSpecificInputs()
      assertDiracFailsWith( result, 'no background to overlay', self )

  def test_getCastorFile( self ):
    mylfn = '/ilc/user/j/jebbing/testfile.txt'
    expanded_lfn = '/castor/cern.ch/grid%s' % mylfn
    self.check_scriptwriting_method( mylfn, self.over.getCASTORFile, get_castor_lines( expanded_lfn ) )

  def test_getCastorFile_otherlfn( self ):
    mylfn = '/castor/cern.ch/grid/ilc/user/j/jebbing/testfile.txt'
    expected = get_castor_lines( mylfn )
    expected[0].append("cp %s /tmp/x509up_u%s \n" % ('mytestproxy', 'mytestuserid') )
    self.check_scriptwriting_method( mylfn , self.over.getCASTORFile, expected,
                                     'failed', [ False, False ], False,
                                     environ_dict = { 'X509_USER_PROXY' : 'mytestproxy' } )

  def test_getLyonFile( self ):
    mylfn = '/ilc/user/j/jebbing/testfile.txt'
    expanded_lfn = '/pnfs/in2p3.fr/data%s' % mylfn
    self.check_scriptwriting_method( mylfn, self.over.getLyonFile,
                                     get_lyon_lines( expanded_lfn ),
                                     environ_dict = { 'X509_USER_PROXY' : 'mytestproxy' } )

  def test_getLyonFile_otherlfn( self ):
    mylfn = '/pnfs/in2p3.fr/data/ilc/user/j/jebbing/testfile.txt'
    self.check_scriptwriting_method( mylfn , self.over.getLyonFile, get_lyon_lines( mylfn ),
                                     'failed', [ False, False ], False,
                                     environ_dict = { 'X509_USER_PROXY' : 'mytestproxy' } )

  def test_getImperialFile( self ):
    mylfn = '/ilc/user/j/jebbing/testfile.txt'
    expanded_lfn = '/pnfs/hep.ph.ic.ac.uk/data%s' % mylfn
    defaultse = 'defaultStorageElement_in_my_test'
    self.check_scriptwriting_method( mylfn, self.over.getImperialFile,
                                     get_imperial_lines( expanded_lfn, defaultse ),
                                     environ_dict = {'VO_ILC_DEFAULT_SE' : defaultse } )

  def test_getImperialFile_otherlfn( self ):
    mylfn = '/pnfs/hep.ph.ic.ac.uk/data/ilc/user/j/jebbing/testfile.txt'
    defaultse = 'defaultStorageElement_in_my_test'
    self.check_scriptwriting_method( mylfn, self.over.getImperialFile,
                                     get_imperial_lines( mylfn, defaultse, True ),
                                     'failed', [ False, False, False ], False, [[], []],
                                     [ ('overlayinput.sh', 'w'),
                                       (os.getcwd() + '/DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK', 'w') ],
                                     {'VO_ILC_DEFAULT_SE' : defaultse } )

  def test_getRALFile( self ):
    mylfn =  '/ilc/user/j/jebbing/testfile.txt'
    expanded_lfn = '/castor/ads.rl.ac.uk/prod%s' % mylfn
    with patch('%s.subprocess.Popen' % MODULE_NAME, new=Mock()) as proc_mock:
      self.check_scriptwriting_method( mylfn, self.over.getRALFile, get_RAL_lines( expanded_lfn ),
                                       environ_dict = { 'X509_USER_PROXY' : 'mytestproxy' },
                                       is_ral = True )
      self.assertTrue( proc_mock.called )

  def test_getRALFile_otherlfn( self ):
    mylfn = '/castor/ads.rl.ac.uk/prod/ilc/user/j/jebbing/testfile.txt'
    with patch('%s.subprocess.Popen' % MODULE_NAME, new=Mock()):
      self.check_scriptwriting_method( mylfn, self.over.getRALFile,
                                       get_RAL_lines( mylfn, True ), 'failed', [ False, False, False ],
                                       False, [[], []], [
                                         ('overlayinput.sh', 'w'),
                                         (os.getcwd() + '/DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK', 'w')
                                       ], is_ral = True )

  def test_getKEKFile( self ):
    mylfn =  '/ilc/user/j/jebbing/testfile.txt'
    self.check_scriptwriting_method( mylfn, self.over.getKEKFile,
                                     get_KEK_lines( '/grid%s' % mylfn ),
                                     environ_dict = { 'X509_USER_PROXY' : 'mytestproxy' } )

  def test_getKEKFile_otherlfn( self ):
    mylfn =  '/ilc/user/j/jebbing/testfile.txt'
    self.check_scriptwriting_method( mylfn, self.over.getKEKFile,
                                     get_KEK_lines( '/grid%s' % mylfn, True ), 'failed',
                                     [ False, False, False ], False, [[], []],
                                     [ ('overlayinput.sh', 'w'),
                                       (os.getcwd() + '/DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK', 'w') ] )

  #pylint: disable=too-many-arguments
  def check_scriptwriting_method( self, mylfn, scriptmethod, expected, should_fail_with = '',
                                  exists_sideeff = None, unlink_called = True,
                                  file_contents = None, expected_opens = None,
                                  environ_dict = None, is_ral = False ):
    """ Helper method that checks one of the methods provided by OverlayInput

    :param str mylfn: file path (LFN)
    :param method scriptmethod: getLyonFile etc
    :param list of list of strings expected: Expected output in the script file (list of lists with all touched files)
    :param str should_fail_with: Error message the method should return (in a S_ERROR structure)
    :param list of bool exists_sideeff: Return values of the os.path.exists method
    :param bool unlink_called: indicates whether os.unlink is expected to be called or not
    :param list of pair of string expected_opens: a list of (filename, mode) pairs of all opened files
    """
    # Set default list values
    if exists_sideeff is None:
      exists_sideeff = [ True, True, True ]
    if file_contents is None:
      file_contents = [[]]
    if environ_dict is None:
      environ_dict = {}
    if expected_opens is None:
      expected_opens = [('overlayinput.sh', 'w')]
    handles = FileUtil.getMultipleReadHandles( file_contents )
    # Variable mocks: exists return value, open values
    with patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=0)) as shell_mock, \
         patch('%s.os.unlink' % MODULE_NAME, new=Mock(return_value=True)) as remove_mock, \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=exists_sideeff)), \
         patch('%s.open' % MODULE_NAME, mock_open(), create=True) as mo, \
         patch('%s.os.chmod' % MODULE_NAME, new=Mock(return_value=True)) as chmod_mock, \
         patch.dict(os.environ, environ_dict, True), \
         patch('%s.os.getuid' % MODULE_NAME, new=Mock(return_value='mytestuserid')):
      mo.side_effect = (h for h in handles)
      result = scriptmethod( mylfn )
      if should_fail_with:
        assertDiracFailsWith( result, should_fail_with, self )
      else:
        assertDiracSucceedsWith_equals( result, 'testfile.txt', self )
      shell_mock.assert_called_with( 600, 'sh -c "./overlayinput.sh"',
                                     bufferLimit = 20971520,
                                     callbackFunction = self.over.redirectLogOutput )
      chmod_mock.assert_called_with( 'overlayinput.sh', 0755 )
      if unlink_called:
        remove_mock.assert_called_with( 'overlayinput.sh' )
      else:
        self.assertFalse( remove_mock.called )
      # Check if output to files is correct
      FileUtil.checkFileInteractions( self, mo, expected_opens, expected, handles )
      if is_ral:
        self.assertIn( 'CNS_HOST', os.environ )
        self.assertIn( 'STAGE_SVCCLASS', os.environ )
        self.assertIn( 'STAGE_HOST', os.environ )

class TestOverlayExecute( unittest.TestCase ):
  """ Tests the Execute method of the  Overlayinput class
  """
  def setUp( self ):
    self.over = OverlayInput()
    self.over.detectormodel = 'testdetectorv2000'
    self.over.energytouse = '200TeV'
    self.over.NbSigEvtsPerJob = 20
    self.over.pathToOverlayFiles = 'mytestfiles.txt'
    self.over.BXOverlay = 3
    self.over.ggtohadint = 5
    self.over.nbofeventsperfile = 21
    self.over.nbinputsigfile = 2
    self.over.site = "SomeSite"
    self.over.step_commons = dict( STEP_NUMBER=3 )


  mockretval =  S_OK({'Successful' : {'testfile1.txt' : ['CERN-DIP-4' , 'KEK'],
                                      'testfile2.ppt' : ['KEK'] }, 'Failed' : ''} )
  def test_execute( self ):
    rpc_mock = Mock()
    rpc_mock.canRun.return_value = S_OK(1)
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value=2)), \
         patch('%s.FileCatalogClient.findFilesByMetadata' % MODULE_NAME, new=Mock(return_value=S_OK(['file1.txt', 'file2.ppt']))), \
         patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value = True)), \
         patch('%s.os.remove' % MODULE_NAME, new=Mock(return_value=True)) as remove_mock, \
         patch('%s.open' % MODULE_NAME, mock_open(), create=True) as mo, \
         patch("%s.OverlaySystemClient" % MODULE_NAME, new=Mock(return_value=rpc_mock)), \
         patch('%s.os.mkdir' % MODULE_NAME, new=Mock(return_value = True)), \
         patch('%s.os.chdir' % MODULE_NAME, new=Mock(return_value = True)), \
         patch('%s.DataManager.getFile' % MODULE_NAME, new=Mock(return_value=S_OK('Nothing'))), \
         patch('%s.wasteCPUCycles' % MODULE_NAME):
      result = self.over.execute()
      assertDiracSucceedsWith_equals( result, 'OverlayInput finished successfully', self )
      assertEqualsImproved( self.over.applicationLog, os.getcwd() + '/Overlay_input.log', self )

  def test_execute_resolve_fails( self ):
    result = self.over.execute()
    assertDiracFailsWith( result, 'no background to overlay', self )

  def test_execute_status_not_ok( self ):
    log = 'my_123_log.txt'
    self.over.applicationLog = log
    self.over.workflowStatus = S_ERROR('myerror167')
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value=2)):
      result = self.over.execute()
      assertDiracSucceedsWith( result, 'OverlayInput should not proceed', self )
      assertEqualsImproved( self.over.applicationLog, os.getcwd() + '/' + log, self )

  def test_execute_getfiles_fails( self ):
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value=2)), \
         patch('%s.OverlayInput._OverlayInput__getFilesFromPath' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_getfile_error'))):
      assertDiracFailsWith( self.over.execute(), 'some_getfile_error', self )

  def test_execute_getfiles_empty( self ):
    self.over.pathToOverlayFiles = ''
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value=2)), \
         patch('%s.OverlayInput._OverlayInput__getFilesFromFC' % MODULE_NAME, new=Mock(return_value=S_OK([]))):
      assertDiracFailsWith( self.over.execute(), 'overlayprocessor got an empty list', self )

  def test_execute_getlocally_fails( self ):
    with patch('%s.Operations.getValue' % MODULE_NAME, new=Mock(return_value=2)), \
         patch('%s.OverlayInput._OverlayInput__getFilesFromPath' % MODULE_NAME, new=Mock(return_value=S_OK(['mylfn1', 'otherlfn', 'many_more_lfns.txt']))), \
         patch('%s.OverlayInput._OverlayInput__getFilesLocaly' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_local_getfile_err'))):
      assertDiracFailsWith( self.over.execute(), 'failed to get files locally', self )

  #pylint: disable=protected-access,no-member
  def test_getfcfiles( self ):
    ops_dict = { '/Overlay/clic_cdr/200TeV/testdetectorv2000/myTestBkgEvt/ProdID' : 98421,
                 '/Overlay/clic_cdr/200TeV/testdetectorv2000/myTestBkgEvt/NbEvts' : 482,
                 '/Overlay/clic_cdr/200TeV/testdetectorv2000/myTestBkgEvt/EvtType' : 'someTestEventType' }
    self.over.energy = 123
    self.over.useEnergyForFileLookup = True
    self.over.BkgEvtType = 'myTestBkgEvt'
    self.over.machine = 'clic_cdr'
    ops_mock = Mock()
    ops_mock.getValue.side_effect = lambda key, default: ops_dict[key]
    self.over.ops = ops_mock
    fcc_mock = Mock()
    fcc_mock.findFilesByMetadata.return_value = S_OK( 9824 )
    self.over.fcc = fcc_mock
    result = self.over._OverlayInput__getFilesFromFC()
    assertDiracSucceedsWith_equals( result, 9824, self )
    fcc_mock.findFilesByMetadata.assert_called_once_with(
      { 'Energy' : '123', 'EvtType' : 'someTestEventType', 'ProdID' : 98421, 'Datatype' : 'SIM',
        'DetectorModel' : 'testdetectorv2000', 'Machine' : 'clic' } )

  def test_getfcfiles_othercase( self ):
    ops_dict = { '/Overlay/ilc_dbd/TestILCDetectorv1/200TeV/otherTestEvt/ProdID' : 139,
                 '/Overlay/ilc_dbd/200TeV/TestILCDetectorv1/otherTestEvt/NbEvts' : 2145,
                 '/Overlay/ilc_dbd/200TeV/TestILCDetectorv1/otherTestEvt/EvtType' : 'ilc_evt_testme' }
    self.over.energy = 0
    self.over.useEnergyForFileLookup = False
    self.over.detectormodel = ''
    self.over.BkgEvtType = 'otherTestEvt'
    self.over.machine = 'ilc_dbd'
    self.over.detector = 'TestILCDetectorv1'
    self.over.prodid = 82492
    ops_mock = Mock()
    ops_mock.getValue.side_effect = lambda key, default: ops_dict[key]
    self.over.ops = ops_mock
    fcc_mock = Mock()
    fcc_mock.findFilesByMetadata.return_value = S_OK( 2948 )
    self.over.fcc = fcc_mock
    result = self.over._OverlayInput__getFilesFromFC()
    assertDiracSucceedsWith_equals( result, 2948, self )
    fcc_mock.findFilesByMetadata.assert_called_once_with(
      { 'EvtType' : 'ilc_evt_testme', 'ProdID' : 82492, 'Datatype' : 'SIM', 'Machine' : 'ilc' } )

  def test_getfilesfromFC( self ):
    ops_mock = Mock()
    ops_mock.getValue.side_effect = [ '1245', 2849, None ]
    self.over.ops = ops_mock
    fcc_mock = Mock()
    fcc_mock.findFilesByMetadata.return_value = S_OK( '1245' )
    self.over.fcc = fcc_mock
    self.over.energy = 9842
    self.over.useEnergyForFileLookup = True
    self.over.detectormodel = 'myTestDetectorv021'
    self.over.machine = 'clic_cdr'
    self.over.detector = 'overlaydetector'
    assertDiracSucceeds( self.over._OverlayInput__getFilesFromFC(), self )
    fcc_mock.findFilesByMetadata.assert_called_once_with(
      { 'Energy' : '9842', 'EvtType' : None, 'Datatype' : 'SIM', 'DetectorModel' : 'myTestDetectorv021',
        'Machine' : 'clic', 'ProdID' : '1245' } )

  def test_getfilesfromlyon( self ):
    import subprocess
    popen_mock = Mock()
    popen_mock.communicate.side_effect = [ ( 'myfile1\nfile1923\n813tev_collision  ', 'ignoreme' ),
                                           ( 'file1.stdhep\nsome_other_file', 'ignoreme' ), ( '', 'ignoreme' ),
                                           ( '\nlast_file.txt', 'ignoreme' ) ]
    lyon_dict = { 'ProdID' : 121345, 'Energy' : '813', 'EvtType' : 'myTestEvt',
                  'DetectorType' : 'myTestDetectorv3' }
    with patch('subprocess.Popen', new=Mock(return_value=popen_mock)) as proc_mock:
      proc_command_dir = '/ilc/prod/clic/813/myTestEvt/myTestDetectorv3/SIM/00121345/'
      assertDiracSucceedsWith_equals( self.over._OverlayInput__getFilesFromLyon( lyon_dict ),
                                      [ proc_command_dir + 'myfile1/file1.stdhep',
                                        proc_command_dir + 'myfile1/some_other_file',
                                        proc_command_dir + 'file1923/', proc_command_dir + '813tev_collision/',
                                        proc_command_dir + '813tev_collision/last_file.txt' ], self )
      assertEqualsImproved( proc_mock.mock_calls,
                            [ call( [ 'nsls', proc_command_dir ], stdout=subprocess.PIPE ),
                              call( [ 'nsls', proc_command_dir + 'myfile1' ], stdout=subprocess.PIPE ),
                              call( [ 'nsls', proc_command_dir + 'file1923' ], stdout=subprocess.PIPE ),
                              call( [ 'nsls', proc_command_dir + '813tev_collision' ],
                                    stdout=subprocess.PIPE ) ], self ) # Checks for expected calls on the mock

  def test_getfilesfromlyon_ignore_all( self ):
    import subprocess
    popen_mock = Mock()
    popen_mock.communicate.side_effect = [ ( 'mypaths/dirac_directory/some/other/stuff\nmy/dir ', 'ignoreme' ),
                                           ( 'other_file/dirac_directory', 'ignoreme' ) ]
    lyon_dict = { 'ProdID' : 121345, 'Energy' : '813', 'EvtType' : 'myTestEvt',
                  'DetectorType' : 'myTestDetectorv3' }
    with patch('subprocess.Popen', new=Mock(return_value=popen_mock)) as proc_mock:
      proc_command_dir = '/ilc/prod/clic/813/myTestEvt/myTestDetectorv3/SIM/00121345/'
      assertDiracFailsWith( self.over._OverlayInput__getFilesFromLyon( lyon_dict ), 'file list is empty',
                            self )
      assertEqualsImproved( proc_mock.mock_calls,
                            [ call( [ 'nsls', proc_command_dir ], stdout=subprocess.PIPE ),
                              call( [ 'nsls', proc_command_dir + 'my/dir' ], stdout=subprocess.PIPE ) ], self )

  def test_getfilesfromcastor( self ):
    self.over.machine = 'testMach12'
    import subprocess
    popen_mock = Mock()
    popen_mock.communicate.side_effect = [ ( 'myfile1\nfile1923\n813tev_collision  ', 'ignoreme' ),
                                           ( 'file1.stdhep\nsome_other_file', 'ignoreme' ), ( '', 'ignoreme' ),
                                           ( '\nlast_file.txt', 'ignoreme' ) ]
    castor_dict = { 'ProdID' : 121345, 'Energy' : '813', 'EvtType' : 'myTestEvt',
                    'DetectorType' : 'myTestDetectorv3' }
    with patch('subprocess.Popen', new=Mock(return_value=popen_mock)) as proc_mock:
      proc_command_dir = '/castor/cern.ch/grid/ilc/prod/testMach12/813/myTestEvt/myTestDetectorv3/SIM/00121345/'
      assertDiracSucceedsWith_equals( self.over._OverlayInput__getFilesFromCastor( castor_dict ),
                                      [ proc_command_dir + 'myfile1/file1.stdhep',
                                        proc_command_dir + 'myfile1/some_other_file',
                                        proc_command_dir + 'file1923/', proc_command_dir + '813tev_collision/',
                                        proc_command_dir + '813tev_collision/last_file.txt' ], self )
      assertEqualsImproved( proc_mock.mock_calls,
                            [ call( [ 'nsls', proc_command_dir ], stdout=subprocess.PIPE ),
                              call( [ 'nsls', proc_command_dir + 'myfile1' ], stdout=subprocess.PIPE ),
                              call( [ 'nsls', proc_command_dir + 'file1923' ], stdout=subprocess.PIPE ),
                              call( [ 'nsls', proc_command_dir + '813tev_collision' ],
                                    stdout=subprocess.PIPE ) ], self )

  def test_getfilesfromcastor_ignoreall( self ):
    import subprocess
    popen_mock = Mock()
    popen_mock.communicate.side_effect = [ ( 'mypaths/dirac_directory/some/other/stuff\nmy/dir ', 'ignoreme' ),
                                           ( 'other_file/dirac_directory', 'ignoreme' ) ]
    castor_dict = { 'ProdID' : 121345, 'Energy' : '813', 'EvtType' : 'myTestEvt',
                    'DetectorType' : 'myTestDetectorv3' }
    with patch('subprocess.Popen', new=Mock(return_value=popen_mock)) as proc_mock:
      proc_command_dir = '/castor/cern.ch/grid/ilc/prod/clic_cdr/813/myTestEvt/myTestDetectorv3/SIM/00121345/'
      assertDiracFailsWith( self.over._OverlayInput__getFilesFromCastor( castor_dict ), 'file list is empty',
                            self )
      assertEqualsImproved( proc_mock.mock_calls,
                            [ call( [ 'nsls', proc_command_dir ], stdout=subprocess.PIPE ),
                              call( [ 'nsls', proc_command_dir + 'my/dir' ], stdout=subprocess.PIPE ) ], self )

def get_castor_lines( expanded_lfn ):
  result = [ [
    '#!/bin/sh \n', '###############################\n', '# Dynamically generated scrip #\n',
    '###############################\n', 'declare -x STAGE_SVCCLASS=ilcdata\n',
    'declare -x STAGE_HOST=castorpublic\n',
    r"xrdcp -s root://castorpublic.cern.ch/%s ./ -OSstagerHost=castorpublic\&svcClass=ilcdata\n" % expanded_lfn,
    """
if [ ! -s %s ]; then
  echo "Using rfcp instead"
  rfcp %s ./
fi\n""" % ( 'testfile.txt', expanded_lfn ), 'declare -x appstatus=$?\n',
    'exit $appstatus\n'] ]
  return result

def get_lyon_lines( expanded_lfn ):
  result = [ [
    '#!/bin/sh \n', '###############################\n', '# Dynamically generated scrip #\n',
    '###############################\n', "cp %s /tmp/x509up_u%s \n" % ( 'mytestproxy', 'mytestuserid'),
    ". /afs/in2p3.fr/grid/profiles/lcg_env.sh\n",
    "xrdcp root://ccdcacsn179.in2p3.fr:1094%s ./ -s\n" % expanded_lfn,
    'declare -x appstatus=$?\n', 'exit $appstatus\n' ] ]
  return result

def get_imperial_lines( expanded_lfn, defaultse, with_watchdog = False ):
  result = []
  if with_watchdog:
    result.append( [ 'Dont look at cpu' ] )
  result.append( [ '#!/bin/sh \n', '###############################\n',
                   '# Dynamically generated scrip #\n', '###############################\n',
                   "dccp dcap://%s%s ./\n" % ( defaultse, expanded_lfn ),
                   'declare -x appstatus=$?\n', 'exit $appstatus\n' ] )
  return result

def get_RAL_lines( expanded_lfn, with_watchdog = False ):
  result = []
  if with_watchdog:
    result.append( [ 'Dont look at cpu' ] )
  result.append( [ '#!/bin/sh \n', '###############################\n',
                   '# Dynamically generated scrip #\n', '###############################\n',
                   "/usr/bin/rfcp 'rfio://cgenstager.ads.rl.ac.uk:9002?svcClass=ilcTape&path=%s' %s\n" % ( expanded_lfn, 'testfile.txt' ),
                   'declare -x appstatus=$?\n', 'exit $appstatus\n' ] )
  return result

def get_KEK_lines( expanded_lfn, with_watchdog = False ):
  result = []
  if with_watchdog:
    result.append( [ 'Dont look at cpu' ] )
  result.append( [ '#!/bin/sh \n', '###############################\n', '# Dynamically generated scrip #\n',
                   '###############################\n',"cp %s ./ -s\n" % expanded_lfn,
                   'declare -x appstatus=$?\n','exit $appstatus\n' ] )
  return result

def runTests():
  """Runs our tests"""
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestOverlayEos )

  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  print(testResult)

  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestOverlayUnittests )

  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  print(testResult)

  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestOverlayExecute )

  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  print(testResult)


if __name__ == '__main__':
  runTests()
