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
    print "Calls: %s" % mock_removelibc.mock_calls
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
      # mock_open doesn't properly handle iterating over the open file with for line in file:
      # but if we set the return value like this, it works.
      file_mocker.return_value.__iter__.return_value = text_file_data.splitlines()
      from ILCDIRAC.Core.Utilities import PrepareOptionFiles
      result = PrepareOptionFiles.prepareWhizardFile("in", "typeA", "1tev", "89741", "50", False, "out")
      self.assertEquals(S_OK(True), result)
    file_mocker.assert_any_call('in', 'r')
    file_mocker.assert_any_call('out', 'w')
    mocker_handle = file_mocker()
    expected = [' seed = 89741\n', ' sqrts = 1tev\n', ' n_events = 50\n', ' write_events_file = "typeA" \n', 'processidprocess_id"123', '98u243jrui4fg4289fjh2487rh13urhi']
    print "Mocked write calls: %s" % mocker_handle.write.mock_calls
    for entry in expected:
      mocker_handle.write.assert_any_call(entry)


def splitkeepsep(line, sep):
  """Splits a line at a given separator sep but keeps the separator in the returned list
  """
  return reduce(lambda acc, elem: acc[:-1] + [acc[-1] + elem] if elem == sep else acc + [elem], re.split("(%s)" % re.escape(sep), line), [])

if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( TestPrepareOptionsFile )
  TESTRESULT = unittest.TextTestRunner( verbosity = 2 ).run( SUITE )
  print TESTRESULT
