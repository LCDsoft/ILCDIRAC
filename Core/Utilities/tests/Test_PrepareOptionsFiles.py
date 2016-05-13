"""
tests for PrepareOptionFiles

"""
__RCSID__ = "$Id$"
from DIRAC import S_OK, S_ERROR
from mock import mock_open, patch, MagicMock as Mock
import unittest
import os
import filecmp
import re
import copy
import collections

from ILCDIRAC.Core.Utilities import PrepareOptionFiles
from ILCDIRAC.Core.Utilities.PrepareOptionFiles import prepareMacFile
from ILCDIRAC.Tests.Utilities.FileUtils import FileUtil
from ILCDIRAC.Tests.Utilities.GeneralUtils import assert_equals_xml, assertEqualsImproved
import xml.etree.ElementTree as ET

#TODO split up in separate classes


# pylint: disable=E1101
# pylint: disable=R0904
class TestPrepareOptionsFile( unittest.TestCase ):
  """tests of PrepareOptionsFile"""
  def setUp(self):
    """
    Make fake files for the test
    """
    self.referenceMacfile = "ref.mac"
    self.inputmac = "input.mac"
    self.outputmac = "output.mac"
    self.stdhep = "dummy.stdhep"
    self.nbevts = 123
    self.startfrom = 12

  def tearDown(self):
    """ Remove the fake files
    """
    for rFile in ( self.outputmac, self.inputmac, self.referenceMacfile ):
      try:
        os.remove( rFile )
      except OSError as e:
        print "Error removing %s: %s " % ( rFile, str(e) )

  def createInputMacFile(self):
    """create the input macfile for the test"""
    lines = []
    lines.append("/generator/filename asads")
    lines.append("/generator/skipEvents 123")
    lines.append("/random/seed 321")
    lines.append("/run/beamOn 456")
    lines.append("/lcdd/url http://google.com")
    lines.append("/lcio/filename outfile.wrong")
    lines.append("/some/other/command")
    lines.append("/do/not/remove/this")

    with open(self.inputmac, "w") as ifile:
      ifile.write( "\n".join(lines) )
      ifile.write( "\n" )

  def createReferenceMacFile(self, startfrom=0, nbevents=10, outputfile=None, detector=None, randomseed=0):
    """create the input macfile for the test"""
    lines = []
    if detector:
      lines.append("/lcdd/url %s.lcdd" % detector)
    if outputfile:
      lines.append("/lcio/filename %s" % outputfile)
    lines.append("/lcio/runNumber %d" % randomseed)
    if not detector:
      lines.append("/lcdd/url http://google.com")
      lines.append("")
    if not outputfile:
      lines.append("/lcio/filename outfile.wrong")
      lines.append("")
    lines.append("/some/other/command")
    lines.append("")
    lines.append("/do/not/remove/this")
    lines.append("")
    lines.append("/generator/filename dummy.stdhep")
    lines.append("/generator/skipEvents %d" % startfrom )
    lines.append("/random/seed %d" % randomseed)
    lines.append("/run/beamOn %d" % nbevents )

    with open(self.referenceMacfile, "w") as ifile:
      ifile.write( "\n".join(lines) )
      ifile.write( "\n" )

  def compareMacFiles(self):
    """create the new macfile with the expected one"""
    res = filecmp.cmp( self.outputmac, self.referenceMacfile )
    print "Compare mac files" , res
    return res

  def test_prepMacFile1(self):
    """test with start, events, stdhep.............................................................."""

    self.createInputMacFile()
    startfrom = 10
    nbevents = 10
    stdhep = "dummy.stdhep"
    self.createReferenceMacFile( startfrom=startfrom, nbevents=nbevents)
    prepareMacFile(self.inputmac, self.outputmac, stdhep=stdhep, nbevts=nbevents, startfrom=startfrom)
    self.assertTrue( self.compareMacFiles() )

  def test_prepMacFile2(self):
    """test with start, events, stdhep and outputfile..............................................."""
    self.createInputMacFile()
    startfrom = 10
    nbevents = 10
    stdhep = "dummy.stdhep"
    outputfile = "out1.slcio"
    self.createReferenceMacFile( startfrom=startfrom, nbevents=nbevents, outputfile=outputfile)
    prepareMacFile( self.inputmac, self.outputmac, stdhep=stdhep, nbevts=nbevents,
                    startfrom=startfrom,
                    outputlcio=outputfile
                  )
    self.assertTrue( self.compareMacFiles() )

  def test_prepMacFile3(self):
    """test with start, events, stdhep and detector................................................."""
    self.createInputMacFile()
    startfrom = 10
    nbevents = 10
    stdhep = "dummy.stdhep"
    outputfile = "out1.slcio"
    detector = "CLIC_SID_CDR"
    self.createReferenceMacFile( startfrom=startfrom,
                                 nbevents=nbevents,
                                 outputfile=outputfile,
                                 detector=detector
                               )
    prepareMacFile( self.inputmac, self.outputmac, stdhep=stdhep, nbevts=nbevents,
                    startfrom=startfrom,
                    outputlcio=outputfile,
                    detector=detector
                  )
    self.assertTrue( self.compareMacFiles() )


  def test_prepMacFile4(self):
    """test with start, events, stdhep and randomseed..............................................."""
    self.createInputMacFile()
    startfrom = 10
    nbevents = 10
    stdhep = "dummy.stdhep"
    randomseed = 123
    self.createReferenceMacFile( startfrom=startfrom,
                                 nbevents=nbevents,
                                 randomseed=randomseed
                               )
    prepareMacFile( self.inputmac, self.outputmac, stdhep=stdhep, nbevts=nbevents,
                    startfrom=startfrom,
                    randomseed=randomseed
                  )
    self.assertTrue( self.compareMacFiles() )

  dep1 = { 'app' : True, 'version' : True }
  dep2 = { 'app' : True, 'version' : True }
  dep3 = { 'app' : True, 'version' : True }

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.resolveDeps", new=Mock(return_value=[dep1, dep2, dep3]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getSoftwareFolder", new=Mock(side_effect=[S_ERROR(), S_OK('aFolder'), S_OK('bFolder')]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.os.path.exists", new=Mock(side_effect=[True, False, False, True]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.removeLibc")
  def test_getnewldlibs_cornercase( self, mock_removelibc ):
    # TODO: Understand method: Currently this method ignores every library path except the last one in the list and just ignores if getSoftwareFolder fails
    reference = os.environ['LD_LIBRARY_PATH']
    mock_removelibc.return_value=True
    self.assertEquals("%s:%s" % ('bFolder/LDLibs', reference), PrepareOptionFiles.getNewLDLibs(None, None, None))
    mock_removelibc.assert_any_call("aFolder/lib")
    mock_removelibc.assert_any_call("bFolder/LDLibs")

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.resolveDeps", new=Mock(return_value=[dep1, dep2, dep3]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getSoftwareFolder", new=Mock(side_effect=[S_ERROR(), S_OK(''), S_OK('')]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.os.path.exists", new=Mock(side_effect=[False, False, False, False]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.removeLibc", new=Mock(return_value=True))
  def test_getnewldlibs_nochange( self ):
    reference = os.environ['LD_LIBRARY_PATH']
    self.assertEquals(reference, PrepareOptionFiles.getNewLDLibs(None, None, None))

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.resolveDeps", new=Mock(return_value=[dep1, dep2, dep3]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getSoftwareFolder", new=Mock(side_effect=[S_ERROR(), S_OK('basetest'), S_ERROR()]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.os.path.exists", new=Mock(side_effect=[True, False, False, False]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.removeLibc", new=Mock(return_value=True))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.os.environ", {})
  def test_getnewldlibs_noldlibpath( self ):
    self.assertEquals('basetest/lib', PrepareOptionFiles.getNewLDLibs(None, None, None))

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.resolveDeps", new=Mock(return_value=[dep1, dep2]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getSoftwareFolder", new=Mock(side_effect=[S_ERROR(), S_OK('bFolder')]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.os.path.exists", new=Mock(side_effect=[True, False, False, True]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.removeLibc", new=Mock(return_value=True))
  def test_getnewpath_cornercase( self ):
    # TODO: Understand method: Currently this method ignores every path except the last one in the list and just ignores if getSoftwareFolder fails
    reference = os.environ['PATH']
    self.assertEquals("%s:%s" % ('bFolder/bin', reference), PrepareOptionFiles.getNewPATH(None, None, None))

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.resolveDeps", new=Mock(return_value=[dep1, dep2, dep3]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getSoftwareFolder", new=Mock(side_effect=[S_ERROR(), S_OK(''), S_OK('')]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.os.path.exists", new=Mock(side_effect=[False, False]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.removeLibc", new=Mock(return_value=True))
  def test_getnewpath_nochange( self ):
    reference = os.environ['PATH']
    self.assertEquals(reference, PrepareOptionFiles.getNewPATH(None, None, None))

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.resolveDeps", new=Mock(return_value=[dep1, dep2, dep3]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getSoftwareFolder", new=Mock(side_effect=[S_ERROR(), S_OK("testfolder"), S_ERROR()]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.os.path.exists", new=Mock(side_effect=[True, False]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.removeLibc", new=Mock(return_value=True))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.os.environ", {})
  def test_getnewpath_nokey( self ):
    self.assertEquals("testfolder/bin", PrepareOptionFiles.getNewPATH(None, None, None))

  def test_prepareWhizFile( self ):
    moduleName = "ILCDIRAC.Core.Utilities.PrepareOptionFiles"
    file_contents = [['asdseed123', '314s.sqrtsfe89u', 'n_events143417', 'write_events_file', 'processidprocess_id"123', '98u243jrui4fg4289fjh2487rh13urhi'], []]
    handles = FileUtil.get_multiple_read_handles(file_contents)
    with patch('%s.open' % moduleName, mock_open(), create=True) as file_mocker:
      file_mocker.side_effect = (h for h in handles)
      result = PrepareOptionFiles.prepareWhizardFile("in", "typeA", "1tev", "89741", "50", False, "out")
      assertEqualsImproved(S_OK(True), result, self)
    tuples = [('in', 'r'), ('out', 'w')]
    expected = [[], [' seed = 89741\n', ' sqrts = 1tev\n', ' n_events = 50\n', ' write_events_file = "typeA" \n', 'processidprocess_id"123', '98u243jrui4fg4289fjh2487rh13urhi']]
    FileUtil.check_file_interactions( self, file_mocker, tuples, expected, handles )

  def test_prepareWhizFile_noprocessid( self ):
    moduleName = "ILCDIRAC.Core.Utilities.PrepareOptionFiles"
    file_contents = [['asdseed123', '314s.sqrtsfe89u', 'n_events143417', 'write_events_file', 'processidprocess_id1"', '98u243jrui4fg4289fjh2487rh13urhi'], []]
    handles = FileUtil.get_multiple_read_handles(file_contents)
    with patch('%s.open' % moduleName, mock_open(), create=True) as file_mocker:
      file_mocker.side_effect = (h for h in handles)
      result = PrepareOptionFiles.prepareWhizardFile("in", "typeA", "1tev", "89741", "50", False, "out")
      assertEqualsImproved(S_OK(False), result, self)
    tuples = [('in', 'r'), ('out', 'w')]
    expected = [[], [' seed = 89741\n', ' sqrts = 1tev\n', ' n_events = 50\n', ' write_events_file = "typeA" \n', 'processidprocess_id1"', '98u243jrui4fg4289fjh2487rh13urhi']]
    FileUtil.check_file_interactions( self, file_mocker, tuples, expected, handles )

  def test_prepareWhizFile_luminosity( self ):
    moduleName = "ILCDIRAC.Core.Utilities.PrepareOptionFiles"
    file_contents = ['asdseed123', '314s.sqrtsfe89u', 'n_events143417', 'write_events_file', 'processidprocess_id"123', '98u243jrui4fg4289fjh2487rh13urhi', 'luminosity']
    text_file_data = '\n'.join(file_contents)
    with patch('%s.open' % moduleName, mock_open(read_data=text_file_data), create=True) as file_mocker:
      file_mocker.return_value.__iter__.return_value = text_file_data.splitlines()
      result = PrepareOptionFiles.prepareWhizardFile("in", "typeA", "1tev", "89741", "50", "684", "out")
      assertEqualsImproved(S_OK(True), result, self)
    file_mocker.assert_any_call('in', 'r')
    file_mocker.assert_any_call('out', 'w')
    mocker_handle = file_mocker()
    expected = [' seed = 89741\n', ' sqrts = 1tev\n', 'n_events143417', ' write_events_file = "typeA" \n', 'processidprocess_id"123', '98u243jrui4fg4289fjh2487rh13urhi', ' luminosity = 684\n']
    for entry in expected:
      mocker_handle.write.assert_any_call(entry)
    assertEqualsImproved(len(expected), mocker_handle.__enter__.return_value.write.call_count, self)

  def test_prepareWhizFileTemplate( self ):
    parameters = { }
    parameters['SEED'] = '135431'
    parameters['ENERGY'] = '1tev'
    parameters['RECOIL'] = '134'
    parameters['NBEVTS'] = '23'
    parameters['LUMI'] = '13'
    parameters['INITIALS'] = 'JE'
    parameters['PNAME1'] = 'electron_hans'
    parameters['PNAME2'] = 'proton_peter'
    parameters['POLAB1'] = 'plus'
    parameters['POLAB2'] = 'minus'
    parameters['USERB1'] = 'spectrumA'
    parameters['USERB2'] = 'SpectrumB'
    parameters['ISRB1'] = 'PSDL'
    parameters['ISRB2'] = 'FVikj'
    parameters['EPAB1'] = '234'
    parameters['EPAB2'] = 'asf31'

    moduleName = "ILCDIRAC.Core.Utilities.PrepareOptionFiles"
    file_contents = [ x+x for x in parameters.keys() ] #Fill file contents with template strings
    # Template strings are the keys of the parameter dictionary concatenated with themselves, e.g. SEEDSEED for the entry 'SEED' : 135431

    parameters['USERSPECTRUM'] = 'mode1234'

    file_contents += ['USERSPECTRUMB1', 'USERSPECTRUMB2']

    file_contents += ['write_events_file', 'processidaisuydhprocess_id"35', 'efiuhifuoejf', '198734y37hrunffuydj82']
    text_file_data = '\n'.join(file_contents)
    with patch('%s.open' % moduleName, mock_open(read_data=text_file_data), create=True) as file_mocker:
      file_mocker.return_value.__iter__.return_value = text_file_data.splitlines()
      result = PrepareOptionFiles.prepareWhizardFileTemplate("in", "typeA", parameters, "out")
      assertEqualsImproved(S_OK(True), result, self)
    file_mocker.assert_any_call('in', 'r')
    file_mocker.assert_any_call('out', 'w')
    mocker_handle = file_mocker()
    expected = [' seed = 135431\n', ' sqrts = 1tev\n', ' beam_recoil = 134\n', ' n_events = 23\n', ' luminosity=13\n', ' keep_initials = JE\n', " particle_name = 'electron_hans'\n", " particle_name = 'proton_peter'\n", ' polarization = plus\n', ' polarization = minus\n', ' USER_spectrum_on = spectrumA\n', ' USER_spectrum_on = SpectrumB\n', ' USER_spectrum_mode = mode1234\n', ' USER_spectrum_mode = -mode1234\n', ' ISR_on = PSDL\n', ' ISR_on = FVikj\n', ' EPA_on = 234\n', ' EPA_on = asf31\n', ' write_events_file = "typeA" \n', 'processidaisuydhprocess_id"35', 'efiuhifuoejf', '198734y37hrunffuydj82']
    for entry in expected:
      mocker_handle.write.assert_any_call(entry)
    assertEqualsImproved(len(expected), mocker_handle.__enter__.return_value.write.call_count, self)


  def test_prepareWhizFileTemplate_noprocessid( self ):
    parameters = { }
    parameters['SEED'] = '135431'
    parameters['ENERGY'] = '1tev'
    parameters['RECOIL'] = '134'
    parameters['NBEVTS'] = '23'
    parameters['LUMI'] = '13'
    parameters['INITIALS'] = 'JE'
    parameters['PNAME1'] = 'electron_hans'
    parameters['PNAME2'] = 'proton_peter'
    parameters['POLAB1'] = 'plus'
    parameters['POLAB2'] = 'minus'
    parameters['USERB1'] = 'spectrumA'
    parameters['USERB2'] = 'SpectrumB'
    parameters['ISRB1'] = 'PSDL'
    parameters['ISRB2'] = 'FVikj'
    parameters['EPAB1'] = '234'
    parameters['EPAB2'] = 'asf31'

    moduleName = "ILCDIRAC.Core.Utilities.PrepareOptionFiles"
    file_contents = [ x+x for x in parameters.keys() ] #Fill file contents with template strings
    # Template strings are the keys of the parameter dictionary concatenated with themselves, e.g. SEEDSEED for the entry 'SEED' : 135431

    parameters['USERSPECTRUM'] = 'mode1234'

    file_contents += ['USERSPECTRUMB1', 'USERSPECTRUMB2']

    file_contents += ['write_events_file', 'processidaisuydhprocess_id"', 'efiuhifuoejf', '198734y37hrunffuydj82']
    text_file_data = '\n'.join(file_contents)
    with patch('%s.open' % moduleName, mock_open(read_data=text_file_data), create=True) as file_mocker:
      file_mocker.return_value.__iter__.return_value = text_file_data.splitlines()
      result = PrepareOptionFiles.prepareWhizardFileTemplate("in", "typeA", parameters, "out")
      assertEqualsImproved(S_OK(False), result, self)
    file_mocker.assert_any_call('in', 'r')
    file_mocker.assert_any_call('out', 'w')
    mocker_handle = file_mocker()
    expected = [' seed = 135431\n', ' sqrts = 1tev\n', ' beam_recoil = 134\n', ' n_events = 23\n', ' luminosity=13\n', ' keep_initials = JE\n', " particle_name = 'electron_hans'\n", " particle_name = 'proton_peter'\n", ' polarization = plus\n', ' polarization = minus\n', ' USER_spectrum_on = spectrumA\n', ' USER_spectrum_on = SpectrumB\n', ' USER_spectrum_mode = mode1234\n', ' USER_spectrum_mode = -mode1234\n', ' ISR_on = PSDL\n', ' ISR_on = FVikj\n', ' EPA_on = 234\n', ' EPA_on = asf31\n', ' write_events_file = "typeA" \n', 'processidaisuydhprocess_id"', 'efiuhifuoejf', '198734y37hrunffuydj82']
    for entry in expected:
      mocker_handle.write.assert_any_call(entry)
    assertEqualsImproved(len(expected), mocker_handle.__enter__.return_value.write.call_count, self)

  # TODO Write test when getoverlayfiles is empty
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.open", mock_open(), create=True)
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getOverlayFiles", new=Mock(return_value='test'))
  def test_prepareXMLFile( self ):
    #pylint: disable=W0613
    def parseModified( self, source, parser=None ):
      """Exchanges the current xmltree object with the one generated by the method"""
      self._root = createXMLTreeForXML().getroot()

    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=parseModified):
      result = PrepareOptionFiles.prepareXMLFile( 'finalxml', 'inputxml', 'inputGEAR', ['input slcio file list'], 1, 'outputfile', 'outputREC', 'outputdst', True )
      assertEqualsImproved(result, S_OK(True), self)

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=Mock(side_effect=IOError))
  def test_prepareXMLFile_parsefails( self ):
    result = PrepareOptionFiles.prepareXMLFile( 'finalxml', 'inputxml', 'inputGEAR', ['input slcio file list'], 1, 'outputfile', 'outputREC', 'outputdst', True )
    self.assertFalse(result['OK'])
    self.assertIn('found exception ', result['Message'].lower())

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.open", mock_open(), create=True)
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getOverlayFiles", new=Mock(return_value='test'))
  def test_prepareXMLFile_slciotypeerror( self ):
    #pylint: disable=W0613
    def parseModified( self, source, parser=None ):
      """Exchanges the current xmltree object with the one generated by the method"""
      self._root = createXMLTreeForXML().getroot()
    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=parseModified):
      result = PrepareOptionFiles.prepareXMLFile( 'finalxml', 'inputxml', 'inputGEAR', 1, 1, 'outputfile', 'outputREC', 'outputdst', True )
      self.assertFalse(result['OK'])
      self.assertIn('inputslcio is neither string nor list!', result['Message'].lower())
      self.assertIn("actual type is <type 'int'>", result['Message'].lower())

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.open", mock_open(), create=True)
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getOverlayFiles", new=Mock(return_value='test'))
  def test_prepareXMLFile_nodebug( self ):
    #TODO: Change so that lciolistfound=False
    #pylint: disable=W0613
    def parseModified( self, source, parser=None ):
      """Exchanges the current xmltree object with the one generated by the method"""
      self._root = createXMLTreeForXML().getroot()

    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=parseModified):
      result = PrepareOptionFiles.prepareXMLFile( 'finalxml', 'inputxml', 'inputGEAR', 'input slcio file list', 1, '', 'outputREC', 'outputdst', False )
      assertEqualsImproved(result, S_OK(True), self)

  def test_prepareSteeringFile_full( self ):
    # Any open() call removes the first element of this list and uses it as its content
    file_contents = [[], ["/Mokka/init/initialMacroFile", "ewoqijfoifemf/Mokka/init/BatchModeadsifkojmf", "asdioj/Mokka/init/randomSeedasdki", "13490ielcioFilename12894eu14", "8r9f2u4jikmelf8/Mokka/init/detectorModelasdiojuaf934i", "9d0i3198ji31i", "nextline", "901-l[doc,193dkdnfba"], []]
    args = ['input.intest', 'output.outtest', "TestdetectormodelClicv302", "stdhepfiletest", "", 41, 2, 561351, 8654]
    tuples = [('mokkamac.mac', 'w'), ('input.intest', 'r'), ('output.outtest', 'w')]

    # expected[i] is the expected output to file i (files are numbered in the order they are opened in the method that is being tested)
    expected = [['/generator/generator stdhepfiletest\n', '/run/beamOn 41\n'], [], ['9d0i3198ji31i', 'nextline', '901-l[doc,193dkdnfba', '#Set detector model to value specified\n', '/Mokka/init/detectorModel TestdetectormodelClicv302\n', '#Set debug level to 1\n', '/Mokka/init/printLevel 1\n', '#Set batch mode to true\n', '/Mokka/init/BatchMode true\n', '#Set mac file to the one created on the site\n', '/Mokka/init/initialMacroFile mokkamac.mac\n', '#Setting random seed\n', '/Mokka/init/randomSeed 561351\n', "13490ielcioFilename12894eu14", '#Setting run number, same as seed\n', '/Mokka/init/mcRunNumber 8654\n', '#Set event start number to value given as job parameter\n', '/Mokka/init/startEventNumber 2\n']]
    exp_retval = S_OK(True)
    self.helper_test_prepareSteeringFile(file_contents, args, tuples, expected, exp_retval)

  def test_prepareSteeringFile_nostdhepfile( self ):
    # Any open() call removes the first element of this list and uses it as its content
    file_contents = [[], ["/Mokka/init/initialMacroFile", "ewoqijfoifemf/Mokka/init/BatchModeadsifkojmf", "asdioj/Mokka/init/randomSeedasdki", "13490ielcioFilename12894eu14", "8r9f2u4jikmelf8/Mokka/init/detectorModelasdiojuaf934i", "9d0i3198ji31i", "nextline", "901-l[doc,193dkdnfba"], []]
    args = ['input.intest', 'output.outtest', "TestdetectormodelClicv302", "", "", 41, 2, 561351, 8654, None, True]
    tuples = [('mokkamac.mac', 'w'), ('input.intest', 'r'), ('output.outtest', 'w')]

    # expected[i] is the expected output to file i (files are numbered in the order they are opened in the method that is being tested)
    expected = [['/run/beamOn 41\n'], [], ['9d0i3198ji31i', 'nextline', '901-l[doc,193dkdnfba', '#Set detector model to value specified\n', '/Mokka/init/detectorModel TestdetectormodelClicv302\n', '#Set batch mode to true\n', '/Mokka/init/BatchMode true\n', '#Set mac file to the one created on the site\n', '/Mokka/init/initialMacroFile mokkamac.mac\n', '#Setting random seed\n', '/Mokka/init/randomSeed 561351\n', "13490ielcioFilename12894eu14", '#Setting run number, same as seed\n', '/Mokka/init/mcRunNumber 8654\n', '#Set event start number to value given as job parameter\n', '/Mokka/init/startEventNumber 2\n']]
    exp_retval = S_OK(True)
    self.helper_test_prepareSteeringFile(file_contents, args, tuples, expected, exp_retval)

  def test_prepareSteeringFile_othercases( self ):
    # Any open() call removes the first element of this list and uses it as its content
    file_contents = [ ["/Mokka/init/initialMacroFile", "ewoqijfoifemf/Mokka/init/BatchModeadsifkojmf", "asdioj/Mokka/init/randomSeedasdki", "13490ielcioFilename12894eu14", "8r9f2u4jikmelf8/Mokka/init/detectorModelasdiojuaf934i", "9d0i3198ji31i", "nextline", "901-l[doc,193dkdnfba"], []]
    metadict = {'GenProcessID' : 'testgenID', 'CrossSection' : '1.3', 'Energy' : '186', 'PolarizationB1' : '', 'PolarizationB2' : 'L' }
    args = ['input.intest', 'output.outtest', "TestdetectormodelClicv302", "stdhepfiletest", "testmacname", 41, 2, 561351, 8654, "testprocessid", False, "testslciooutput.te", metadict ]
    tuples = [('input.intest', 'r'), ('output.outtest', 'w')]

    # expected[i] is the expected output to file i (files are numbered in the order they are opened in the method that is being tested)
    expected = [[], ['8r9f2u4jikmelf8/Mokka/init/detectorModelasdiojuaf934i', '9d0i3198ji31i', 'nextline', '901-l[doc,193dkdnfba', '#Set detector model to value specified\n', '/Mokka/init/detectorModel TestdetectormodelClicv302\n', '#Set debug level to 1\n', '/Mokka/init/printLevel 1\n', '#Set batch mode to true\n', '/Mokka/init/BatchMode true\n', '#Set mac file to the one created on the site\n', '/Mokka/init/initialMacroFile testmacname\n', '#Setting random seed\n', '/Mokka/init/randomSeed 561351\n', '#Setting run number, same as seed\n', '/Mokka/init/mcRunNumber 8654\n', '#Set event start number to value given as job parameter\n', '/Mokka/init/startEventNumber 2\n', '#Set outputfile name to job specified\n', '/Mokka/init/lcioFilename testslciooutput.te\n', "#Set processID as event parameter\n", "/Mokka/init/lcioEventParameter string Process testprocessid\n", "/Mokka/init/lcioEventParameter float CrossSection_fb 1.3\n", "/Mokka/init/lcioEventParameter float Energy 186.0\n", "/Mokka/init/lcioEventParameter float Pol_ep 0.0\n", "/Mokka/init/lcioEventParameter float Pol_em -1.0\n"]]
    exp_retval = S_OK(True)
    self.helper_test_prepareSteeringFile(file_contents, args, tuples, expected, exp_retval)

  def test_prepareSteeringFile_polarization1( self ):
    # Any open() call removes the first element of this list and uses it as its content
    file_contents = [ ["/Mokka/init/initialMacroFile", "ewoqijfoifemf/Mokka/init/BatchModeadsifkojmf", "asdioj/Mokka/init/randomSeedasdki", "13490ielcioFilename12894eu14", "8r9f2u4jikmelf8/Mokka/init/detectorModelasdiojuaf934i", "9d0i3198ji31i", "nextline", "901-l[doc,193dkdnfba"], []]
    metadict = {'GenProcessID' : 'testgenID', 'CrossSection' : '9.2', 'Energy' : '-91', 'PolarizationB1' : 'R', 'PolarizationB2' : '' }
    args = ['input.intest', 'output.outtest', "TestdetectormodelClicv302", "stdhepfiletest", "testmacname", 41, 2, 561351, 8654, '', False, "testslciooutput.te", metadict ]
    tuples = [('input.intest', 'r'), ('output.outtest', 'w')]

    # expected[i] is the expected output to file i (files are numbered in the order they are opened in the method that is being tested)
    expected = [[], ['8r9f2u4jikmelf8/Mokka/init/detectorModelasdiojuaf934i', '9d0i3198ji31i', 'nextline', '901-l[doc,193dkdnfba', '#Set detector model to value specified\n', '/Mokka/init/detectorModel TestdetectormodelClicv302\n', '#Set debug level to 1\n', '/Mokka/init/printLevel 1\n', '#Set batch mode to true\n', '/Mokka/init/BatchMode true\n', '#Set mac file to the one created on the site\n', '/Mokka/init/initialMacroFile testmacname\n', '#Setting random seed\n', '/Mokka/init/randomSeed 561351\n', '#Setting run number, same as seed\n', '/Mokka/init/mcRunNumber 8654\n', '#Set event start number to value given as job parameter\n', '/Mokka/init/startEventNumber 2\n', '#Set outputfile name to job specified\n', '/Mokka/init/lcioFilename testslciooutput.te\n', "#Set processID as event parameter\n", "/Mokka/init/lcioEventParameter string Process testgenID\n", "/Mokka/init/lcioEventParameter float CrossSection_fb 9.2\n", "/Mokka/init/lcioEventParameter float Energy -91.0\n", "/Mokka/init/lcioEventParameter float Pol_ep 1.0\n", "/Mokka/init/lcioEventParameter float Pol_em 0.0\n"]]
    exp_retval = S_OK(True)
    self.helper_test_prepareSteeringFile(file_contents, args, tuples, expected, exp_retval)


  def test_prepareSteeringFile_polarization2( self ):
    # Any open() call removes the first element of this list and uses it as its content
    file_contents = [ ["/Mokka/init/initialMacroFile", "ewoqijfoifemf/Mokka/init/BatchModeadsifkojmf", "asdioj/Mokka/init/randomSeedasdki", "13490ielcioFilename12894eu14", "8r9f2u4jikmelf8/Mokka/init/detectorModelasdiojuaf934i", "9d0i3198ji31i", "nextline", "901-l[doc,193dkdnfba"], []]
    metadict = {'GenProcessID' : 'testgenID', 'CrossSection' : '5.7', 'Energy' : '0', 'PolarizationB1' : 'L', 'PolarizationB2' : 'R' }
    args = ['input.intest', 'output.outtest', "TestdetectormodelClicv302", "stdhepfiletest", "testmacname", 41, 2, 561351, 8654, "testprocessid", False, "testslciooutput.te", metadict ]
    tuples = [('input.intest', 'r'), ('output.outtest', 'w')]

    # expected[i] is the expected output to file i (files are numbered in the order they are opened in the method that is being tested)
    expected = [[], ['8r9f2u4jikmelf8/Mokka/init/detectorModelasdiojuaf934i', '9d0i3198ji31i', 'nextline', '901-l[doc,193dkdnfba', '#Set detector model to value specified\n', '/Mokka/init/detectorModel TestdetectormodelClicv302\n', '#Set debug level to 1\n', '/Mokka/init/printLevel 1\n', '#Set batch mode to true\n', '/Mokka/init/BatchMode true\n', '#Set mac file to the one created on the site\n', '/Mokka/init/initialMacroFile testmacname\n', '#Setting random seed\n', '/Mokka/init/randomSeed 561351\n', '#Setting run number, same as seed\n', '/Mokka/init/mcRunNumber 8654\n', '#Set event start number to value given as job parameter\n', '/Mokka/init/startEventNumber 2\n', '#Set outputfile name to job specified\n', '/Mokka/init/lcioFilename testslciooutput.te\n', "#Set processID as event parameter\n", "/Mokka/init/lcioEventParameter string Process testprocessid\n", "/Mokka/init/lcioEventParameter float CrossSection_fb 5.7\n", "/Mokka/init/lcioEventParameter float Energy 0.0\n", "/Mokka/init/lcioEventParameter float Pol_ep -1.0\n", "/Mokka/init/lcioEventParameter float Pol_em 1.0\n" ]]
    exp_retval = S_OK(True)
    self.helper_test_prepareSteeringFile(file_contents, args, tuples, expected, exp_retval)


  def test_prepareSteeringFile_polarization3( self ):
    # Any open() call removes the first element of this list and uses it as its content
    file_contents = [ ["/Mokka/init/initialMacroFile", "ewoqijfoifemf/Mokka/init/BatchModeadsifkojmf", "asdioj/Mokka/init/randomSeedasdki", "13490ielcioFilename12894eu14", "8r9f2u4jikmelf8/Mokka/init/detectorModelasdiojuaf934i", "9d0i3198ji31i", "nextline", "901-l[doc,193dkdnfba"], []]
    metadict = {'GenProcessID' : 'testgenID', 'CrossSection' : '0.0', 'Energy' : '3.7', 'PolarizationB1' : 'L13', 'PolarizationB2' : 'R98' }
    args = ['input.intest', 'output.outtest', "TestdetectormodelClicv302", "stdhepfiletest", "testmacname", 41, 2, 561351, 8654, "testprocessid", False, "testslciooutput.te", metadict ]
    tuples = [('input.intest', 'r'), ('output.outtest', 'w')]

    # expected[i] is the expected output to file i (files are numbered in the order they are opened in the method that is being tested)
    expected = [[], ['8r9f2u4jikmelf8/Mokka/init/detectorModelasdiojuaf934i', '9d0i3198ji31i', 'nextline', '901-l[doc,193dkdnfba', '#Set detector model to value specified\n', '/Mokka/init/detectorModel TestdetectormodelClicv302\n', '#Set debug level to 1\n', '/Mokka/init/printLevel 1\n', '#Set batch mode to true\n', '/Mokka/init/BatchMode true\n', '#Set mac file to the one created on the site\n', '/Mokka/init/initialMacroFile testmacname\n', '#Setting random seed\n', '/Mokka/init/randomSeed 561351\n', '#Setting run number, same as seed\n', '/Mokka/init/mcRunNumber 8654\n', '#Set event start number to value given as job parameter\n', '/Mokka/init/startEventNumber 2\n', '#Set outputfile name to job specified\n', '/Mokka/init/lcioFilename testslciooutput.te\n', "#Set processID as event parameter\n", "/Mokka/init/lcioEventParameter string Process testprocessid\n", "/Mokka/init/lcioEventParameter float CrossSection_fb 0.0\n", "/Mokka/init/lcioEventParameter float Energy 3.7\n", "/Mokka/init/lcioEventParameter float Pol_ep -0.13\n", "/Mokka/init/lcioEventParameter float Pol_em 0.98\n"]]
    exp_retval = S_OK(True)
    self.helper_test_prepareSteeringFile(file_contents, args, tuples, expected, exp_retval)


  def test_prepareSteeringFile_outputlcio_det_model1( self ):
    # Tests with outputlcio set and detmodel not set
    # Any open() call removes the first element of this list and uses it as its content
    file_contents = [ ["/Mokka/init/initialMacroFile", "ewoqijfoifemf/Mokka/init/BatchModeadsifkojmf", "asdioj/Mokka/init/randomSeedasdki", "13490ielcioFilename12894eu14", "8r9f2u4jikmelf8/Mokka/init/detectorModelasdiojuaf934i", "9d0i3198ji31i", "nextline", "901-l[doc,193dkdnfba"], []]
    metadict = {'GenProcessID' : 'testgenID', 'CrossSection' : '1.3', 'Energy' : '186', 'PolarizationB1' : '', 'PolarizationB2' : 'L' }
    args = ['input.intest', 'output.outtest', '', "stdhepfiletest", "testmacname", 41, 2, 561351, 8654, "testprocessid", False, "testslciooutput.te", metadict ]
    tuples = [('input.intest', 'r'), ('output.outtest', 'w')]

    # expected[i] is the expected output to file i (files are numbered in the order they are opened in the method that is being tested)
    expected = [[], ['8r9f2u4jikmelf8/Mokka/init/detectorModelasdiojuaf934i', '9d0i3198ji31i', 'nextline', '901-l[doc,193dkdnfba', '#Set debug level to 1\n', '/Mokka/init/printLevel 1\n', '#Set batch mode to true\n', '/Mokka/init/BatchMode true\n', '#Set mac file to the one created on the site\n', '/Mokka/init/initialMacroFile testmacname\n', '#Setting random seed\n', '/Mokka/init/randomSeed 561351\n', '#Setting run number, same as seed\n', '/Mokka/init/mcRunNumber 8654\n', '#Set event start number to value given as job parameter\n', '/Mokka/init/startEventNumber 2\n', '#Set outputfile name to job specified\n', '/Mokka/init/lcioFilename testslciooutput.te\n', "#Set processID as event parameter\n", "/Mokka/init/lcioEventParameter string Process testprocessid\n", "/Mokka/init/lcioEventParameter float CrossSection_fb 1.3\n", "/Mokka/init/lcioEventParameter float Energy 186.0\n", "/Mokka/init/lcioEventParameter float Pol_ep 0.0\n", "/Mokka/init/lcioEventParameter float Pol_em -1.0\n"]]
    exp_retval = S_OK(True)
    self.helper_test_prepareSteeringFile(file_contents, args, tuples, expected, exp_retval)


  def test_prepareSteeringFile_outputlcio_det_model2( self ):
    # Tests with outputlcio set and detmodel not set
    # Any open() call removes the first element of this list and uses it as its content
    file_contents = [ ["/Mokka/init/initialMacroFile", "ewoqijfoifemf/Mokka/init/BatchModeadsifkojmf", "asdioj/Mokka/init/randomSeedasdki", "13490ielcioFilename12894eu14", "8r9f2u4jikmelf8/Mokka/init/detectorModelasdiojuaf934i", "9d0i3198ji31i", "nextline", "901-l[doc,193dkdnfba"], []]
    metadict = {'GenProcessID' : 'testgenID', 'CrossSection' : '1.3', 'Energy' : '186', 'PolarizationB1' : '', 'PolarizationB2' : 'L' }
    args = ['input.intest', 'output.outtest', '', "stdhepfiletest", "testmacname", 41, 2, 561351, 8654, "testprocessid", False, '', metadict ]
    tuples = [('input.intest', 'r'), ('output.outtest', 'w')]

    # expected[i] is the expected output to file i (files are numbered in the order they are opened in the method that is being tested)
    expected = [[], ['13490ielcioFilename12894eu14', '8r9f2u4jikmelf8/Mokka/init/detectorModelasdiojuaf934i', '9d0i3198ji31i', 'nextline', '901-l[doc,193dkdnfba', '#Set debug level to 1\n', '/Mokka/init/printLevel 1\n', '#Set batch mode to true\n', '/Mokka/init/BatchMode true\n', '#Set mac file to the one created on the site\n', '/Mokka/init/initialMacroFile testmacname\n', '#Setting random seed\n', '/Mokka/init/randomSeed 561351\n', '#Setting run number, same as seed\n', '/Mokka/init/mcRunNumber 8654\n', '#Set event start number to value given as job parameter\n', '/Mokka/init/startEventNumber 2\n', "#Set processID as event parameter\n", "/Mokka/init/lcioEventParameter string Process testprocessid\n", "/Mokka/init/lcioEventParameter float CrossSection_fb 1.3\n", "/Mokka/init/lcioEventParameter float Energy 186.0\n", "/Mokka/init/lcioEventParameter float Pol_ep 0.0\n", "/Mokka/init/lcioEventParameter float Pol_em -1.0\n"]]
    exp_retval = S_OK(True)
    self.helper_test_prepareSteeringFile(file_contents, args, tuples, expected, exp_retval)

  def helper_test_prepareSteeringFile( self, file_contents, args, expected_file_tuples, expected, expected_return_value ):
    """Helper function to test prepareSteeringFile.

    :param list file_contents: List of lists containing the mocked file contents. i-th element is a list whose j-th element is the j-th line of the file it represents. No \n necessary
    :param list args: Arguments for the call of prepareSteeringFile
    :param list expected_file_tuples: List of tuples with the filename and mode of opened files. Has to be in order
    :param list expected: The expected output of the file operations. List of lists, the i-th element represents the output to the i-th file. Lines have to end with \n
    :param expected_return_value: The value the call should return
    """
    assertEqualsImproved(len(file_contents), len(expected), self)

    moduleName = "ILCDIRAC.Core.Utilities.PrepareOptionFiles"
    mymock = Mock()
    handles = get_multiple_read_handles(file_contents)
    with patch('%s.open' % moduleName, mock_open(mymock), create=True) as file_mocker:
      file_mocker.side_effect = (h for h in handles)
      args.extend([None] * (13-len(args)))
      result = PrepareOptionFiles.prepareSteeringFile(*args)
    for (filename, mode) in expected_file_tuples:
      file_mocker.assert_any_call(filename, mode)

    for (index, handle) in enumerate(handles):
      cur_handle = handle.__enter__()
      assertEqualsImproved(len(expected[index]), handle.__enter__.return_value.write.call_count, self)
      for entry in expected[index]:
        cur_handle.write.assert_any_call(entry)
    assertEqualsImproved(expected_return_value, result, self)

  current_tree = None

  #@patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.open", mock_open(), create=True)
  # For ease of testing, assert library method write() is correct and instead traverse the xml tree to check for correctness
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getOverlayFiles", new=Mock(return_value=['overlaytestfile1', 'testfile2.txt']))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.write", new=Mock(return_value=True))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.allowedBkg", new=Mock(return_value=S_OK()))
  def test_prepareLCSIM( self ):
    #pylint: disable=W0613
    def parseModified( self, source, parser=None ):
      """Exchanges the current xmltree object with the one generated by the method"""
      self._root = xml_lcsim_1().getroot()

    trackstrat = 'trackstrategy'
    def checkstrat( self, driver ):
      elem_to_check = driver.find('strategyFile')
      expected_element = ET.Element('strategyFile')
      expected_element.text = trackstrat
      assert_equals_xml( elem_to_check, expected_element, self )
      return True

    slcio_list = ['list of slcio files', 'anotherEntry.txt']
    jar_list = ['list of', 'jar files']
    amEvents = 1
    cachedir = 'cachedir'
    outputfile = 'outputfile'

    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=parseModified):
      result = PrepareOptionFiles.prepareLCSIMFile( 'inputlcsim', 'outputlcsim', amEvents, trackstrat, slcio_list, jar_list, cachedir, outputfile, 'outputrec', 'outputdst', False )
      #TODO make another test in which getoverlayfiles fails
      self.assertEquals(result, S_OK('testtext'))
      current_tree = TestPrepareOptionsFile.current_tree
      # clear is called
      assert_equals_xml(current_tree.find('inputFiles'), ET.Element('inputFiles'), self)
      # element is created for each entry in slcio list
      xml_file_list = current_tree.findall('inputFiles/file')
      assertEqualsImproved(len(slcio_list), len(xml_file_list), self)
      for (slcio_string, treeelem) in zip(slcio_list, xml_file_list):
        expected_element = ET.Element('file')
        expected_element.text = slcio_string
        assert_equals_xml(treeelem, expected_element, self)
      assert_equals_xml(current_tree.find('classpath'), ET.Element('classpath'), self)
      xml_jar_list = current_tree.findall('classpath/jar')
      assertEqualsImproved(len(jar_list), len(xml_jar_list), self)
      for (jar_string, treeelem) in zip(jar_list, xml_jar_list):
        expected_element = ET.Element('jar')
        expected_element.text = jar_string
        assert_equals_xml(treeelem, expected_element, self)
      nbEvents = current_tree.find('control/numberOfEvents')
      assertEqualsImproved(str(amEvents), nbEvents.text, self)
      cachdir = current_tree.find('control/cacheDirectory')
      assertEqualsImproved(cachdir.text, cachedir, self)
      evInterv = current_tree.findall('drivers/driver/eventInterval')
      for ev_int in evInterv:
        assertEqualsImproved(ev_int.text, '1', self)
      drivers = current_tree.findall('drivers/driver')
      ovNameFound = False
      ofpNameFound = False
      for d in drivers:
        if d.attrib.has_key('type'):
          self.assertTrue(not d.attrib['type'] == 'org.lcsim.recon.tracking.seedtracker.steeringwrappers.SeedTrackerWrapper' or checkstrat(self, d)) # note: short circuit operator
          if d.attrib['type'] == 'org.lcsim.util.OverlayDriver':
            ovNameFound = True
            expected_element = ET.Element('overlayFiles')
            expected_element.text = 'overlaytestfile1\ntestfile2.txt'
            assert_equals_xml(d.find('overlayFiles'), expected_element, self)
          if d.attrib['type'] == 'org.lcsim.util.loop.LCIODriver':
            if d.attrib['name'] == 'Writer':
              ofpNameFound = True
              expected_element = ET.Element('outputFilePath')
              expected_element.text = outputfile
              assert_equals_xml(d.find('outputFilePath'), expected_element, self)
      assertEqualsImproved(ovNameFound, ofpNameFound, self)

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getOverlayFiles", new=Mock(return_value=['overlaytestfile1', 'testfile2.txt']))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.write", new=Mock(return_value=True))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.allowedBkg", new=Mock(return_value=S_OK()))
  def test_prepareLCSIM_RECWriter( self ):
    #pylint: disable=W0613
    def parseModified( self, source, parser=None ):
      """Exchanges the current xmltree object with the one generated by the method"""
      self._root = xml_lcsim_5().getroot()

    trackstrat = 'trackstrategy'
    slcio_list = ['list of slcio files', 'anotherEntry.txt']
    jar_list = ['list of', 'jar files']
    amEvents = 1
    cachedir = 'cachedir'
    outputrec = 'outputrec'

    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=parseModified):
      result = PrepareOptionFiles.prepareLCSIMFile( 'inputlcsim', 'outputlcsim', amEvents, trackstrat, slcio_list, jar_list, cachedir, 'outputfile', outputrec, 'outputdst', False )
      self.assertEquals(result, S_OK('testtext'))
      current_tree = TestPrepareOptionsFile.current_tree
      drivers = current_tree.findall('drivers/driver')
      flag = False
      print 'in test'
      print drivers
      for d in drivers:
        if d.attrib.has_key('type') and d.attrib['type'] == 'org.lcsim.util.loop.LCIODriver' and d.attrib['name'] == 'RECWriter':
          expected_element = ET.Element('outputFilePath')
          expected_element.text = outputrec
          assert_equals_xml(d.find('outputFilePath'), expected_element, self)
          flag = True
      self.assertTrue(flag)


  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getOverlayFiles", new=Mock(return_value=['overlaytestfile1', 'testfile2.txt']))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.write", new=Mock(return_value=True))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.allowedBkg", new=Mock(return_value=S_OK()))
  def test_prepareLCSIM_DSTWriter( self ):
    #pylint: disable=W0613
    def parseModified( self, source, parser=None ):
      """Exchanges the current xmltree object with the one generated by the method"""
      self._root = xml_lcsim_6().getroot()

    trackstrat = 'trackstrategy'
    slcio_list = ['list of slcio files', 'anotherEntry.txt']
    jar_list = ['list of', 'jar files']
    amEvents = 1
    cachedir = 'cachedir'
    outputdst = 'outputdst'
    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=parseModified):
      result = PrepareOptionFiles.prepareLCSIMFile( 'inputlcsim', 'outputlcsim', amEvents, trackstrat, slcio_list, jar_list, cachedir, 'outputfile', 'outputrec', outputdst, False )
      self.assertEquals(result, S_OK('testtext'))
      current_tree = TestPrepareOptionsFile.current_tree
      drivers = current_tree.findall('drivers/driver')
      flag = False
      for d in drivers:
        if d.attrib.has_key('type') and d.attrib['type'] == 'org.lcsim.util.loop.LCIODriver' and d.attrib['name'] == 'DSTWriter':
          expected_element = ET.Element('outputFilePath')
          expected_element.text = outputdst
          assert_equals_xml(d.find('outputFilePath'), expected_element, self)
          flag = True
      self.assertTrue(flag)

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getOverlayFiles", new=Mock(return_value=['overlaytestfile1', 'testfile2.txt']))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.write", new=Mock(return_value=True))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.allowedBkg", new=Mock(return_value=S_OK()))
  def test_prepareLCSIM_nooutput( self ):
    #pylint: disable=W0613
    def parseModified( self, source, parser=None ):
      """Exchanges the current xmltree object with the one generated by the method"""
      self._root = xml_lcsim_7().getroot()

    trackstrat = 'trackstrategy'
    slcio_list = ['list of slcio files', 'anotherEntry.txt']
    jar_list = ['list of', 'jar files']
    amEvents = 1
    cachedir = 'cachedir'
    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=parseModified):
      result = PrepareOptionFiles.prepareLCSIMFile( 'inputlcsim', 'outputlcsim', amEvents, trackstrat, slcio_list, jar_list, cachedir, 'outputfile', '', '', False )
      self.assertEquals(result, S_OK('testtext'))
      current_tree = TestPrepareOptionsFile.current_tree
      ex = current_tree.find('execute/driver')
      assert_equals_xml(ex, ET.Element('driver', name='Writer'), self)

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getOverlayFiles", new=Mock(return_value=['overlaytestfile1', 'testfile2.txt']))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.write", new=Mock(return_value=True))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.allowedBkg", new=Mock(return_value=S_OK()))
  def test_prepareLCSIM_differentoutput( self ):
    #pylint: disable=W0613
    def parseModified( self, source, parser=None ):
      """Exchanges the current xmltree object with the one generated by the method"""
      self._root = xml_lcsim_8().getroot()

    trackstrat = 'trackstrategy'
    slcio_list = ['list of slcio files', 'anotherEntry.txt']
    jar_list = ['list of', 'jar files']
    amEvents = 1
    cachedir = 'cachedir'
    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=parseModified):
      result = PrepareOptionFiles.prepareLCSIMFile( 'inputlcsim', 'outputlcsim', amEvents, trackstrat, slcio_list, jar_list, cachedir, 'outputfile', '', '', False )
      self.assertEquals(result, S_OK('LCSIM'))
      current_tree = TestPrepareOptionsFile.current_tree
      drivers = current_tree.findall('drivers/driver')
      flag = False
      for d in drivers:
        if d.attrib.has_key('type') and d.attrib['type'] == 'org.lcsim.job.EventMarkerDriver':
          expected_element = ET.Element('marker')
          expected_element.text = 'LCSIM'
          assert_equals_xml(d.find('marker'), expected_element, self)
          flag = True
      self.assertTrue(flag)


  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getOverlayFiles", new=Mock(return_value=['overlaytestfile1', 'testfile2.txt']))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.write", new=Mock(return_value=True))
  def test_prepareLCSIM_lcsimempty( self ):
    #pylint: disable=W0613
    def parseModified( self, source, parser=None ):
      """Exchanges the current xmltree object with the one generated by the method"""
      self._root = xml_lcsim_2().getroot()

    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=parseModified):
      result = PrepareOptionFiles.prepareLCSIMFile( 'inputlcsim', 'outputlcsim', 0, 'trackstrategy', ['list of slcio files'], [], '', 'outputfile', 'outputrec', 'outputdst', True )
      assertEqualsImproved(result, S_OK('testtext'), self)
      # New Elem is created
      assert_equals_xml(TestPrepareOptionsFile.current_tree.find('inputFiles'), ET.Element('inputFiles'), self)

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getOverlayFiles", new=Mock(return_value=[]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.write", new=Mock(return_value=True))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.allowedBkg", new=Mock(return_value=S_OK()))
  def test_prepareLCSIM_overlayempty( self ):
    #pylint: disable=W0613
    def parseModified( self, source, parser=None ):
      """Exchanges the current xmltree object with the one generated by the method"""
      self._root = xml_lcsim_1().getroot()

    trackstrat = 'trackstrategy'
    slcio_list = ['list of slcio files', 'anotherEntry.txt']
    jar_list = ['list of', 'jar files']
    amEvents = 1
    cachedir = 'cachedir'

    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=parseModified):
      result = PrepareOptionFiles.prepareLCSIMFile( 'inputlcsim', 'outputlcsim', amEvents, trackstrat, slcio_list, jar_list, cachedir, 'outputfile', 'outputrec', 'outputdst', False )
      #TODO check if jar elements are added to classpath
      #TODO make another test in which getoverlayfiles fails
      self.assertFalse(result['OK'])
      self.assertIn('could not find any overlay files', result['Message'].lower())


# check classpath elem is cleared
# assert_equals_xml(TestPrepareOptionsFile.current_tree.find('classpath'), ET.Element('classpath'), self)

  def test_prepareLCSIM_ioerr( self ):
    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", side_effect=IOError('')):
      result = PrepareOptionFiles.prepareLCSIMFile( 'inputlcsim', 'outputlcsim', 1, 'trackstrategy', ['list of slcio files'], ['list of', 'jar files'], 'cachedir', 'outputfile', 'outputrec', 'outputdst', False )
      self.assertFalse(result['OK'])
      self.assertIn('found exception', result['Message'].lower())

  def test_prepareLCSIM_inputlistempty( self ):
    #pylint: disable=W0613
    def parseModified( self, source, parser=None ):
      """Exchanges the current xmltree object with the one generated by the method"""
      self._root = xml_lcsim_1().getroot()

    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=parseModified):
      result = PrepareOptionFiles.prepareLCSIMFile( 'inputlcsim', 'outputlcsim', 1, 'trackstrategy', [], ['list of', 'jar files'], 'cachedir', 'outputfile', 'outputdst', 'outputrec', False )
      self.assertFalse(result['OK'])
      self.assertIn('empty input file list', result['Message'].lower())

  def test_prepareLCSIM_emptytree( self ):
    #pylint: disable=W0613
    def parseModified( self, source, parser=None ):
      """Exchanges the current xmltree object with the one generated by the method"""
      self._root = xml_lcsim_3().getroot()

    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=parseModified):
      result = PrepareOptionFiles.prepareLCSIMFile( 'inputlcsim', 'outputlcsim', 1, 'trackstrategy', ['list of slcio files'], ['list of', 'jar files'], 'cachedir', 'outputfile', 'outputrec', 'outputdst', False )
      self.assertFalse(result['OK'])
      self.assertIn('invalid lcsim file structure', result['Message'].lower())

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getOverlayFiles", new=Mock(return_value=['overlaytestfile1', 'testfile2.txt']))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.write", new=Mock(return_value=True))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.allowedBkg", new=Mock(return_value=S_OK()))
  def test_prepareLCSIM_rarecases( self ):
    #pylint: disable=W0613
    def parseModified( self, source, parser=None ):
      """Exchanges the current xmltree object with the one generated by the method"""
      self._root = xml_lcsim_9().getroot()

    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=parseModified):
      result = PrepareOptionFiles.prepareLCSIMFile( 'inputlcsim', 'outputlcsim', 1, 'trackstrategy', ['list of slcio files', 'anotherEntry.txt'], [], 'cachedir', 'outputfile', 'outputrec', 'outputdst', True )
      assertEqualsImproved(S_OK('testtext'), result, self)
      current_tree = TestPrepareOptionsFile.current_tree
      expected_element = ET.Element('numberOfEvents')
      expected_element.text = '1'
      assert_equals_xml(current_tree.find('control/numberOfEvents'), expected_element, self)
      self.assertTrue(current_tree.find('control/verbose').text == 'true')
      self.assertTrue(current_tree.find('control/cacheDirectory').text == 'cachedir')
      drivers = current_tree.findall('drivers/driver')
      flag = False
      for d in drivers:
        if d.attrib.has_key("type") and d.attrib["type"] == "org.lcsim.job.EventMarkerDriver":
          expected_element = ET.Element('eventInterval')
          expected_element.text = '1'
          assert_equals_xml(d.find('eventInterval'), expected_element, self)
          flag = True
      self.assertTrue(flag)




  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getOverlayFiles", new=Mock(return_value=['overlaytestfile1', 'testfile2.txt']))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.write", new=Mock(return_value=True))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.allowedBkg", new=Mock(return_value=S_OK()))
  def test_prepareLCSIM_evtintervalwrong( self ):
    #pylint: disable=W0613
    def parseModified( self, source, parser=None ):
      """Exchanges the current xmltree object with the one generated by the method"""
      self._root = xml_lcsim_10().getroot()

    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=parseModified):
      result = PrepareOptionFiles.prepareLCSIMFile( 'inputlcsim', 'outputlcsim', 1, '', ['list of slcio files', 'anotherEntry.txt'], [], 'cachedir', 'outputfile', 'outputrec', 'outputdst', False )
      assertEqualsImproved(S_OK('testtext'), result, self)
      current_tree = TestPrepareOptionsFile.current_tree
      assertEqualsImproved(current_tree.find("drivers/driver/eventInterval").text, '135851', self)

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getOverlayFiles", new=Mock(return_value=['overlaytestfile1', 'testfile2.txt']))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.write", new=Mock(return_value=True))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.allowedBkg", new=Mock(return_value=S_OK()))
  def test_prepareLCSIM_noexec( self ):
    #pylint: disable=W0613
    def parseModified( self, source, parser=None ):
      """Exchanges the current xmltree object with the one generated by the method"""
      self._root = xml_lcsim_11().getroot()

    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=parseModified):
      result = PrepareOptionFiles.prepareLCSIMFile( 'inputlcsim', 'outputlcsim', 1, 'trackstrategy', ['list of slcio files', 'anotherEntry.txt'], [], 'cachedir', 'outputfile', 'outputrec', 'outputdst', False )
      assertEqualsImproved(S_OK('testtext'), result, self)
      current_tree = TestPrepareOptionsFile.current_tree
      drivers = current_tree.findall('drivers/driver')
      flag = False
      for d in drivers:
        if d.attrib.has_key('type') and d.attrib['type'] == 'org.lcsim.job.EventMarkerDriver' and d.attrib['name'] == 'evtMarker':
          assertEqualsImproved(d.find('eventInterval').text, '1', self)
          flag = True
      self.assertTrue(flag)

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getOverlayFiles", new=Mock(return_value=['overlaytestfile1', 'testfile2.txt']))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.write", new=Mock(return_value=True))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.allowedBkg", new=Mock(return_value=S_OK()))
  def test_prepareLCSIM_withexec( self ):
    #pylint: disable=W0613
    def parseModified( self, source, parser=None ):
      """Exchanges the current xmltree object with the one generated by the method"""
      self._root = xml_lcsim_12().getroot()

    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=parseModified):
      result = PrepareOptionFiles.prepareLCSIMFile( 'inputlcsim', 'outputlcsim', 1, 'trackstrategy', ['list of slcio files', 'anotherEntry.txt'], [], 'cachedir', 'outputfile', 'outputrec', 'outputdst' , False )
      assertEqualsImproved(S_OK('testtext'), result, self)
      current_tree = TestPrepareOptionsFile.current_tree
      drivers = current_tree.findall('drivers/driver')
      flag = False
      for d in drivers:
        if d.attrib.has_key('type') and d.attrib['type'] == 'org.lcsim.job.EventMarkerDriver' and d.attrib['name'] == 'evtMarker':
          assertEqualsImproved(d.find('eventInterval').text, '1', self)
          flag = True
      self.assertTrue(flag)
      assertEqualsImproved(current_tree.find('execute/driver').attrib['name'], 'evtMarker', self)

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getOverlayFiles", new=Mock(return_value=['overlaytestfile1', 'testfile2.txt']))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.write", new=Mock(return_value=True))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.allowedBkg", new=Mock(return_value=S_OK()))
  def test_prepareLCSIM_noclasspath( self ):
    #pylint: disable=W0613
    def parseModified( self, source, parser=None ):
      """Exchanges the current xmltree object with the one generated by the method"""
      self._root = xml_lcsim_4().getroot()

    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=parseModified):
      result = PrepareOptionFiles.prepareLCSIMFile( 'inputlcsim', 'outputlcsim', 1, 'trackstrategy', ['list of slcio files', 'anotherEntry.txt'], ['jarfile'], 'cachedir', 'outputfile', 'outputrec', 'outputdst', False )
      assertEqualsImproved(S_OK('testtext'), result, self)
      current_tree = TestPrepareOptionsFile.current_tree
      assert_equals_xml(current_tree.find('classpath'), ET.Element('classpath'), self)

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getOverlayFiles", new=Mock(return_value=['overlaytestfile1', 'testfile2.txt']))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.write", new=Mock(return_value=True))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.allowedBkg", new=Mock(return_value=S_ERROR('bkgtesterror')))
  def test_prepareLCSIM_bkgfails( self ):
    #pylint: disable=W0613
    def parseModified( self, source, parser=None ):
      """Exchanges the current xmltree object with the one generated by the method"""
      self._root = xml_lcsim_1().getroot()

    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=parseModified):
      result = PrepareOptionFiles.prepareLCSIMFile( 'inputlcsim', 'outputlcsim', 1, 'trackstrategy', ['list of slcio files', 'anotherEntry.txt'], [], 'cachedir', 'outputfile', 'outputrec', 'outputdst', False )
      assertEqualsImproved(S_ERROR('bkgtesterror'), result, self)

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getOverlayFiles", new=Mock(return_value=['overlaytestfile1', 'testfile2.txt']))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.write", new=Mock(return_value=True))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.allowedBkg", new=Mock(return_value=S_ERROR('dontthrowme')))
  def test_prepareLCSIM_noovname( self ):
    #pylint: disable=W0613
    def parseModified( self, source, parser=None ):
      """Exchanges the current xmltree object with the one generated by the method"""
      self._root = xml_lcsim_13().getroot()

    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=parseModified):
      result = PrepareOptionFiles.prepareLCSIMFile( 'inputlcsim', 'outputlcsim', 1, 'trackstrategy', ['list of slcio files', 'anotherEntry.txt'], [], 'cachedir', 'outputfile', 'outputrec', 'outputdst', False )
      assertEqualsImproved(S_OK('testtext'), result, self)

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.write", new=Mock(return_value=True))
  def test_prepareTomato_stdcase( self ):
    #pylint: disable=W0613
    def parseModified( self, source, parser=None ):
      """Exchanges the current xmltree object with the one generated by the method"""
      self._root = xml_salad_1().getroot()

    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=parseModified):
      self.assertTrue(PrepareOptionFiles.prepareTomatoSalad('inputxml', 'outputxml', 'inputslcio', 'ofs', 'coll')['OK'])
      current_tree = TestPrepareOptionsFile.current_tree
      it = current_tree.find('global').iter()
      it.next()
      assert_equals_xml(it.next(), ET.Comment("input file list changed"), self)
      assert_equals_xml(it.next(), ET.Comment("input file list changed"), self)
      for pa in current_tree.findall('global/parameter'):
        if pa.attrib.has_key('name') and pa.attrib['name'] == 'LCIOInputFiles':
          assertEqualsImproved(pa.text, 'inputslcio', self)
      params = current_tree.findall('processor/parameter')
      for pa in params:
        if pa.attrib.has_key('name'):
          if pa.attrib['name'] == 'OutputFile':
            assertEqualsImproved(pa.text, 'ofs', self)
          elif pa.attrib['name'] == 'MCCollectionName':
            assertEqualsImproved(pa.text, 'coll', self)

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.write", new=Mock(return_value=True))
  def test_prepareTomato_othercase( self ):
    #pylint: disable=W0613
    def parseModified( self, source, parser=None ):
      """Exchanges the current xmltree object with the one generated by the method"""
      self._root = xml_salad_2().getroot()

    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=parseModified):
      self.assertTrue(PrepareOptionFiles.prepareTomatoSalad('inputxml', 'outputxml', 'inputslcio', 'outputFile', 'collection')['OK'])
      current_tree = TestPrepareOptionsFile.current_tree
      expected_element = ET.Element('parameter', name='LCIOInputFiles')
      expected_element.text = 'inputslcio'
      assert_equals_xml(current_tree.findall('global/parameter')[-1], expected_element, self)

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.write", new=Mock(return_value=True))
  def test_prepareTomato_parsefails( self ):
    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=Mock(side_effect=IOError(''))):
      result = PrepareOptionFiles.prepareTomatoSalad('inputxml', 'outputxml', 'inputslcio', 'outputFile', 'collection')
      self.assertFalse(result['OK'])
      self.assertIn('found exception', result['Message'].lower())

  teststr = """
<?xml version="1.0" encoding="us-ascii"?>
<!-- ?xml-stylesheet type="text/xsl" href="http://ilcsoft.desy.de/marlin/marlin.xsl"? -->
<!-- ?xml-stylesheet type="text/xsl" href="marlin.xsl"? -->

<marlin xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://ilcsoft.desy.de/marlin/marlin.xsd">

   <execute>
      <processor name="MyTomatoProcessor"/>
   </execute>

   <global>
      <parameter name="Verbosity" value="ERROR"/>
   </global>

 <processor name="MyTomatoProcessor" type="TomatoProcessor">
 <!--Automated analysis-->
  <!--Name of the MCParticle collection-->
  <parameter name="MCCollectionName" type="string" lcioInType="MCParticle"> MCParticle </parameter>
  <!--Root OutputFile-->
  <parameter name="OutputFile" type="string" value="tomato.root"/>
  <!--verbosity level of this processor ("DEBUG0-4,MESSAGE0-4,WARNING0-4,ERROR0-4,SILENT")-->
  <!--parameter name="Verbosity" type="string" value=""/-->
</processor>

</marlin>      
    """
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.write", new=Mock(return_value=True))
  def test_prepareTomato_noinputfile( self ):
    #pylint: disable=W0613
    def parseModified( self, source, parser=None ):
      """Exchanges the current xmltree object with the one generated by the method"""
      self._root = xml_salad_2().getroot()

    file_contents = [ [] ]
    expected = [[ TestPrepareOptionsFile.teststr ]] # Means 2 files will be opened, nothing is written to first file, and 'firstlineentry' and 'line100' are written (in different calls and exactly these strings) to the second file. If more/less is written this fails!
    handles = FileUtil.get_multiple_read_handles(file_contents)
    moduleName = "ILCDIRAC.Core.Utilities.PrepareOptionFiles"
    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=parseModified), patch('%s.open' % moduleName, mock_open(), create=True) as mo:
      mo.side_effect = (h for h in handles)
      self.assertTrue(PrepareOptionFiles.prepareTomatoSalad(None, 'outputxml', 'inputslcio', 'outputFile', 'collection')['OK'])
      current_tree = TestPrepareOptionsFile.current_tree
      expected_element = ET.Element('parameter', name='LCIOInputFiles')
      expected_element.text = 'inputslcio'
      assert_equals_xml(current_tree.findall('global/parameter')[-1], expected_element, self)
      expected_tuples = [('default.xml', 'w')]
      FileUtil.check_file_interactions( self, mo, expected_tuples, expected, handles )

def get_multiple_read_handles(file_contents):
  full_file_contents = ['\n'.join(x) for x in file_contents]
  gens = []
  for filecontent in file_contents:
    gens.append((f for f in filecontent))
  amount_of_files = len(gens)
  handles = []
  for i in range(0, amount_of_files):
    curhandle = Mock()
    curhandle.__enter__.return_value.read.side_effect = lambda: full_file_contents.pop(0)
    curhandle.__enter__.return_value.__iter__.return_value = gens[i]
    handles.append(curhandle)
  return handles

def createXMLTreeForXML():
  """Creates a XML Tree to test prepareXMLFile()"""
  import xml.etree.ElementTree as ET
  root = ET.Element("root")
  ex = ET.SubElement(root, 'execute')
  ET.SubElement(ex, 'processor', name='OVERLAYTIMING')
  ET.SubElement(ex, 'processor', name='BGoverlAY')
  glob = ET.SubElement(root, 'global')
  ET.SubElement(glob, 'parameter', name='LCIOInputFiles')
  ET.SubElement(glob, 'parameter', name='MaxRecordNumber', value='2')
  # TODO: Check for change + inserted comment
  ET.SubElement(glob, 'parameter', name='GearXMLFile', value='a')
  # TODO: Check for change + inserted comment
  ET.SubElement(glob, 'parameter', name='GearXMLFile')
  ET.SubElement(glob, 'parameter', name='Verbosity')
  # TODO Check for change + inserted comment (if not debug)
  mlop = ET.SubElement(root, 'processor', name='MyLCIOOutputProcessor')
  ET.SubElement(mlop, 'parameter', name='LCIOOutputFile')
  dst = ET.SubElement(root, 'processor', name='DSTOutput')
  ET.SubElement(dst, 'parameter', name='LCIOOutputFile')
  olt = ET.SubElement(root, 'processor', name='OVERLAYTIMING')
  bgo = ET.SubElement(root, 'processor', name='BGoverlAY')
  ET.SubElement(olt, 'parameter', name='NumberBackground', value = '0.0') # TODO Both with value 0.0 and without
  ET.SubElement(olt, 'parameter', name='NBunchtrain', value = '0') # TODO Both with value 0 and with different val
  ET.SubElement(bgo, 'parameter', name='expBG', value = '0.0') # TODO Also with 0.0
  ET.SubElement(bgo, 'parameter', name='NBunchtrain', value = '0') # TODO Both with value 0 and with different val
  ET.SubElement(olt, 'parameter', name='BackgroundFileNames')
  ET.SubElement(bgo, 'parameter', name='InputFileNames')

  #TODO Create tree without LCIOInputFiles
  result = ET.ElementTree(root)
  TestPrepareOptionsFile.currenttree = result
  return result

stdargs = collections.defaultdict(bool) # returns false by default

def xml_lcsim_1():
  """Creates standard xml tree"""
  return createXMLTreeForLCSIM(stdargs)

def xml_lcsim_2():
  """Creates xml tree without inputFiles element """
  customargs = copy.deepcopy(stdargs)
  customargs['noInputFiles'] = True
  return createXMLTreeForLCSIM(customargs)

def xml_lcsim_3():
  """Creates empty xml tree """
  customargs = copy.deepcopy(stdargs)
  customargs['returnEmptyTree'] = True
  return createXMLTreeForLCSIM(customargs)

def xml_lcsim_4():
  """ Creates xml tree """
  customargs = copy.deepcopy(stdargs)
  customargs['with_classpath'] = True
  return createXMLTreeForLCSIM(customargs)

def xml_lcsim_5():
  """ Creates xml tree """
  customargs = copy.deepcopy(stdargs)
  customargs['use_rec_writer'] = True
  return createXMLTreeForLCSIM(customargs)

def xml_lcsim_6():
  """ Creates xml tree """
  customargs = copy.deepcopy(stdargs)
  customargs['use_dst_writer'] = True
  return createXMLTreeForLCSIM(customargs)

def xml_lcsim_7():
  """ Creates xml tree """
  customargs = copy.deepcopy(stdargs)
  customargs['no_writer'] = True
  return createXMLTreeForLCSIM(customargs)

def xml_lcsim_8():
  """ Creates xml tree """
  customargs = copy.deepcopy(stdargs)
  customargs['different_marker'] = True
  return createXMLTreeForLCSIM(customargs)

def xml_lcsim_9():
  """ Creates xml tree """
  customargs = copy.deepcopy(stdargs)
  customargs['with_control_subelems'] = True
  return createXMLTreeForLCSIM(customargs)

def xml_lcsim_10():
  """ Creates xml tree """
  customargs = copy.deepcopy(stdargs)
  customargs['invalid_nbEvts'] = True
  return createXMLTreeForLCSIM(customargs)

def xml_lcsim_11():
  """ Creates xml tree """
  customargs = copy.deepcopy(stdargs)
  customargs['with_root_execute'] = True
  return createXMLTreeForLCSIM(customargs)

def xml_lcsim_12():
  """ Creates xml tree """
  customargs = copy.deepcopy(stdargs)
  customargs['with_evt_interval'] = True
  return createXMLTreeForLCSIM(customargs)

def xml_lcsim_13():
  """ Creates xml tree """
  customargs = copy.deepcopy(stdargs)
  customargs['with_overlaydriver_overlayname_elem'] = True
  return createXMLTreeForLCSIM(customargs)


def createXMLTreeForLCSIM( flags ):
  """Creates a XML Tree to test prepareXMLFile()"""
  if flags['returnEmptyTree']:
    return ET.ElementTree()
  root = ET.Element("root")
  if flags['noInputFiles']:
    ET.SubElement(root, 'inputFiles', testarg1='2', testarg2='deleteMe')
  if not flags['with_classpath']:
    ET.SubElement(root, 'classpath', testarg1='2', testarg2='deleteMe')

  co = ET.SubElement(root, 'control')
  if not flags['with_control_subelems']:
    ve = ET.SubElement(co, 'verbose')
    ve.text='t'
    cd = ET.SubElement(co, 'cacheDirectory')
    cd.text='t'

  dr = ET.SubElement(root, 'drivers')
  dri = ET.SubElement(dr, 'driver', type='org.lcsim.recon.tracking.seedtracker.steeringwrappers.SeedTrackerWrapper')
  ET.SubElement(dri, 'strategyFile', testarg1='deletemepls', testparam='143')
  if not flags['with_control_subelems'] and not flags['with_root_execute'] and not flags['with_evt_interval']:
    ev = ET.SubElement(dri, 'eventInterval')
    ev.text='7'
  if flags['invalid_nbEvts']:
    ev.text='135851'
  if not flags['with_root_execute']:
    ET.SubElement(root, 'execute')

  if flags['with_control_subelems']:
    ET.SubElement(co, 'numberOfEvents')
    ET.SubElement(dr, 'driver', type='org.lcsim.job.EventMarkerDriver')

  dri1 = ET.SubElement(dr, 'driver', type='org.lcsim.recon.tracking.seedtracker.steeringwrappers.SeedTrackerWrapper')
  ET.SubElement(dri1, 'strategyFile')
  ma = ET.SubElement(dri1, 'marker')
  ma.text='testtext'
  dri2 = ET.SubElement(dr, 'driver', type='org.lcsim.util.OverlayDriver')
  if not flags['with_overlaydriver_overlayname_elem']:
    ol = ET.SubElement(dri2, 'overlayName')
    ol.text='bkgtestname'
  ET.SubElement(dri2, 'overlayFiles')

  ET.SubElement(dr, 'driver')
  dri4 = ET.SubElement(dr, 'driver', type='org.lcsim.util.loop.LCIODriver', name='Writer')
  ET.SubElement(dri4, 'outputFilePath', testparam1='1', asd='123')

  if flags['use_rec_writer']:
    dri5 = ET.SubElement(dr, 'driver', type='org.lcsim.util.loop.LCIODriver', name='RECWriter')
    ET.SubElement(dri5, 'outputFilePath', testparam1='1', asd='123')

  if flags['use_dst_writer']:
    dri6 = ET.SubElement(dr, 'driver', type='org.lcsim.util.loop.LCIODriver', name='DSTWriter')
    ET.SubElement(dri6, 'outputFilePath', testparam1='1', asd='123')

  if flags['no_writer']:
    dr.remove(dri4)

  if flags['different_marker']:
    dri1.remove(ma)
    ET.SubElement(dr, 'driver', type='org.lcsim.job.EventMarkerDriver')

  result = ET.ElementTree(root)
  TestPrepareOptionsFile.current_tree = result
  return result

def createXMLTreeForSalad( flags ):
  """Creates a XML Tree to test prepareTomatoSalad()"""
  root = ET.Element("root")
  glob = ET.SubElement(root, 'global')
  ET.SubElement(glob, 'parameter')
  if not flags[0]:
    ET.SubElement(glob, 'parameter', name='LCIOInputFiles')
    ET.SubElement(glob, 'parameter', name='LCIOInputFiles')
  ET.SubElement(glob, 'parameter', name='testme123')
  ET.SubElement(glob, 'parameter')
  ET.SubElement(glob, 'parameter')
  ET.SubElement(glob, 'parameter')

  proc = ET.SubElement(root, 'processor', type='TomatoProcessor')
  ET.SubElement(root, 'processor')

  ET.SubElement(proc, 'parameter')
  ET.SubElement(proc, 'parameter', name='OutputFile')
  ET.SubElement(proc, 'parameter', name='MCCollectionName')

  result = ET.ElementTree(root)
  TestPrepareOptionsFile.current_tree = result
  return result


def xml_salad_1():
  """ Creates standard xml tree """
  return createXMLTreeForSalad(stdargs)

def xml_salad_2():
  """ Creates xml tree """
  customargs = copy.deepcopy(stdargs)
  customargs[0] = True
  return createXMLTreeForSalad(customargs)


if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( TestPrepareOptionsFile )
  TESTRESULT = unittest.TextTestRunner( verbosity = 2 ).run( SUITE )
  print TESTRESULT
