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

from ILCDIRAC.Core.Utilities.PrepareOptionFiles import prepareMacFile


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
    from ILCDIRAC.Core.Utilities import PrepareOptionFiles
    self.assertEquals("%s:%s" % ('bFolder/LDLibs', reference), PrepareOptionFiles.getNewLDLibs(None, None, None))
    mock_removelibc.assert_any_call("aFolder/lib")
    mock_removelibc.assert_any_call("bFolder/LDLibs")

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.resolveDeps", new=Mock(return_value=[dep1, dep2, dep3]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getSoftwareFolder", new=Mock(side_effect=[S_ERROR(), S_OK(''), S_OK('')]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.os.path.exists", new=Mock(side_effect=[False, False, False, False]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.removeLibc", new=Mock(return_value=True))
  def test_getnewldlibs_nochange( self ):
    reference = os.environ['LD_LIBRARY_PATH']
    from ILCDIRAC.Core.Utilities import PrepareOptionFiles
    self.assertEquals(reference, PrepareOptionFiles.getNewLDLibs(None, None, None))


  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.resolveDeps", new=Mock(return_value=[dep1, dep2]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getSoftwareFolder", new=Mock(side_effect=[S_ERROR(), S_OK('bFolder')]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.os.path.exists", new=Mock(side_effect=[True, False, False, True]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.removeLibc", new=Mock(return_value=True))
  def test_getnewpath_cornercase( self ):
    # TODO: Understand method: Currently this method ignores every path except the last one in the list and just ignores if getSoftwareFolder fails
    reference = os.environ['PATH']
    from ILCDIRAC.Core.Utilities import PrepareOptionFiles
    self.assertEquals("%s:%s" % ('bFolder/bin', reference), PrepareOptionFiles.getNewPATH(None, None, None))

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.resolveDeps", new=Mock(return_value=[dep1, dep2, dep3]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getSoftwareFolder", new=Mock(side_effect=[S_ERROR(), S_OK(''), S_OK('')]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.os.path.exists", new=Mock(side_effect=[False, False]))
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.removeLibc", new=Mock(return_value=True))
  def test_getnewpath_nochange( self ):
    reference = os.environ['PATH']
    from ILCDIRAC.Core.Utilities import PrepareOptionFiles
    self.assertEquals(reference, PrepareOptionFiles.getNewPATH(None, None, None))

  def test_prepareWhizFile( self ):
    from ILCDIRAC.Core.Utilities import PrepareOptionFiles
    moduleName = "ILCDIRAC.Core.Utilities.PrepareOptionFiles"
    file_contents = ['asdseed123', '314s.sqrtsfe89u', 'n_events143417', 'write_events_file', 'processidprocess_id"123', '98u243jrui4fg4289fjh2487rh13urhi']
    text_file_data = '\n'.join(file_contents)
    with patch('%s.open' % moduleName, mock_open(read_data=text_file_data), create=True) as file_mocker:
      file_mocker.return_value.__iter__.return_value = text_file_data.splitlines()
      result = PrepareOptionFiles.prepareWhizardFile("in", "typeA", "1tev", "89741", "50", False, "out")
      self.assertEquals(S_OK(True), result)
    file_mocker.assert_any_call('in', 'r')
    file_mocker.assert_any_call('out', 'w')
    mocker_handle = file_mocker()
    expected = [' seed = 89741\n', ' sqrts = 1tev\n', ' n_events = 50\n', ' write_events_file = "typeA" \n', 'processidprocess_id"123', '98u243jrui4fg4289fjh2487rh13urhi']
    for entry in expected:
      mocker_handle.write.assert_any_call(entry)
    self.assertEquals(len(expected), mocker_handle.__enter__.return_value.write.call_count)

  def test_prepareWhizFile_luminosity( self ):
    from ILCDIRAC.Core.Utilities import PrepareOptionFiles
    moduleName = "ILCDIRAC.Core.Utilities.PrepareOptionFiles"
    file_contents = ['asdseed123', '314s.sqrtsfe89u', 'n_events143417', 'write_events_file', 'processidprocess_id"123', '98u243jrui4fg4289fjh2487rh13urhi', 'luminosity']
    text_file_data = '\n'.join(file_contents)
    with patch('%s.open' % moduleName, mock_open(read_data=text_file_data), create=True) as file_mocker:
      file_mocker.return_value.__iter__.return_value = text_file_data.splitlines()
      result = PrepareOptionFiles.prepareWhizardFile("in", "typeA", "1tev", "89741", "50", "684", "out")
      self.assertEquals(S_OK(True), result)
    file_mocker.assert_any_call('in', 'r')
    file_mocker.assert_any_call('out', 'w')
    mocker_handle = file_mocker()
    expected = [' seed = 89741\n', ' sqrts = 1tev\n', 'n_events143417', ' write_events_file = "typeA" \n', 'processidprocess_id"123', '98u243jrui4fg4289fjh2487rh13urhi', ' luminosity = 684\n']
    for entry in expected:
      mocker_handle.write.assert_any_call(entry)
    self.assertEquals(len(expected), mocker_handle.__enter__.return_value.write.call_count)


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

    from ILCDIRAC.Core.Utilities import PrepareOptionFiles
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
      self.assertEquals(S_OK(True), result)
    file_mocker.assert_any_call('in', 'r')
    file_mocker.assert_any_call('out', 'w')
    mocker_handle = file_mocker()
    expected = [' seed = 135431\n', ' sqrts = 1tev\n', ' beam_recoil = 134\n', ' n_events = 23\n', ' luminosity=13\n', ' keep_initials = JE\n', " particle_name = 'electron_hans'\n", " particle_name = 'proton_peter'\n", ' polarization = plus\n', ' polarization = minus\n', ' USER_spectrum_on = spectrumA\n', ' USER_spectrum_on = SpectrumB\n', ' USER_spectrum_mode = mode1234\n', ' USER_spectrum_mode = -mode1234\n', ' ISR_on = PSDL\n', ' ISR_on = FVikj\n', ' EPA_on = 234\n', ' EPA_on = asf31\n', ' write_events_file = "typeA" \n', 'processidaisuydhprocess_id"35', 'efiuhifuoejf', '198734y37hrunffuydj82']
    for entry in expected:
      mocker_handle.write.assert_any_call(entry)
    self.assertEquals(len(expected), mocker_handle.__enter__.return_value.write.call_count)
    #self.mocked_calls_match_expected_automated(expected, mocker_handle.mock_calls)

  #TODO construct return value of parse()
  # TODO Write test when getoverlayfiles is empty
  from ILCDIRAC.Core.Utilities import PrepareOptionFiles
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.open", mock_open(), create=True)
  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getOverlayFiles", new=Mock(return_value='test'))
  def test_prepareXMLFile( self ):
    def parseModified( self, source, parser=None ):
      """Exchanges the current xmltree object with the one generated by the method"""
      self._root = createXMLTestTree().getroot()

    from ILCDIRAC.Core.Utilities import PrepareOptionFiles
    resulttree = createXMLTestTree()
    import xml.etree.ElementTree as ET
    print resulttree.getroot()
    print ET.tostring(resulttree.getroot())
    #with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree", autospec=True) as et_mock:
    #  et_mock.__new__.return_value=resulttree
    #  et_mock.parse.return_value=1
    #  result = PrepareOptionFiles.prepareXMLFile( 'finalxml', 'inputxml', 'inputGEAR', ['input slcio file list'], 1, 'outputfile', 'outputREC', 'outputdst', True )
    with patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=parseModified):
      result = PrepareOptionFiles.prepareXMLFile( 'finalxml', 'inputxml', 'inputGEAR', ['input slcio file list'], 1, 'outputfile', 'outputREC', 'outputdst', True )
      self.assertEquals(result, S_OK(True))

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.ElementTree.parse", new=Mock(side_effect=IOError))
  def test_prepareXMLFile_parsefails( self ):
    from ILCDIRAC.Core.Utilities import PrepareOptionFiles
    result = PrepareOptionFiles.prepareXMLFile( 'finalxml', 'inputxml', 'inputGEAR', ['input slcio file list'], 1, 'outputfile', 'outputREC', 'outputdst', True )
    self.assertFalse(result['OK'])
    self.assertIn('found exception ', result['Message'].lower())


  def test_prepareSteeringFile_full( self ):
    # Any open() call removes the first element of this list and uses it as its content
    file_contents = [[], ["/Mokka/init/initialMacroFile", "ewoqijfoifemf/Mokka/init/BatchModeadsifkojmf", "asdioj/Mokka/init/randomSeedasdki", "13490ielcioFilename12894eu14", "8r9f2u4jikmelf8/Mokka/init/detectorModelasdiojuaf934i", "9d0i3198ji31i", "nextline", "901-l[doc,193dkdnfba"], []]
    args = ['input.intest', 'output.outtest', "TestdetectormodelClicv302", "stdhepfiletest", "", 41, 2, 561351, 8654]
    tuples = [('mokkamac.mac', 'w'), ('input.intest', 'r'), ('output.outtest', 'w')]

    # expected[i] is the expected output to file i (files are numbered in the order they are opened in the method that is being tested)
    expected = [['/generator/generator stdhepfiletest\n', '/run/beamOn 41\n'], [], ['9d0i3198ji31i', 'nextline', '901-l[doc,193dkdnfba', '#Set detector model to value specified\n', '/Mokka/init/detectorModel TestdetectormodelClicv302\n', '#Set debug level to 1\n', '/Mokka/init/printLevel 1\n', '#Set batch mode to true\n', '/Mokka/init/BatchMode true\n', '#Set mac file to the one created on the site\n', '/Mokka/init/initialMacroFile mokkamac.mac\n', '#Setting random seed\n', '/Mokka/init/randomSeed 561351\n', "13490ielcioFilename12894eu14", '#Setting run number, same as seed\n', '/Mokka/init/mcRunNumber 8654\n', '#Set event start number to value given as job parameter\n', '/Mokka/init/startEventNumber 2\n']]
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
    from ILCDIRAC.Core.Utilities import PrepareOptionFiles
    moduleName = "ILCDIRAC.Core.Utilities.PrepareOptionFiles"
    mymock = Mock()
    handles = get_multiple_read_handles(file_contents)
    with patch('%s.open' % moduleName, mock_open(mymock), create=True) as file_mocker:
      file_mocker.side_effect = (h for h in handles)
      for j in range(len(args), 13):
        args.append(None)
      i = 0
      result = PrepareOptionFiles.prepareSteeringFile(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9], args[10], args[11], args[12])
    for (filename, mode) in expected_file_tuples:
      file_mocker.assert_any_call(filename, mode)

    for i in range(0, len(file_contents)):
      cur_handle = handles[i].__enter__()
      #self.mocked_calls_match_expected_automated(expected[i], handles[i].mock_calls)
      self.assertEquals(len(expected[i]), handles[i].__enter__.return_value.write.call_count)
      for entry in expected[i]:
        cur_handle.write.assert_any_call(entry)
    self.assertEquals(expected_return_value, result)

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

def createXMLTestTree():
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
  ET.SubElement(root, 'processor', name='MyLCIOOutputProcessor')
  ET.SubElement(root, 'parameter', name='LCIOOutputFile')
  ET.SubElement(root, 'processor', name='DSTOutput')
  ET.SubElement(root, 'parameter', name='NumberBackground', value = '0.0') # TODO Both with value 0.0 and without
  ET.SubElement(root, 'parameter', name='NBunchtrain', value = '0') # TODO Both with value 0 and with different val
  ET.SubElement(root, 'parameter', name='expBG', value = '0') # TODO Also with 0.0

  #TODO Create tree without LCIOInputFiles
  result = ET.ElementTree(root)
  return result


if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( TestPrepareOptionsFile )
  TESTRESULT = unittest.TextTestRunner( verbosity = 2 ).run( SUITE )
  print TESTRESULT
