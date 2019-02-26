"""
tests for InputFilesUtilities

"""
from __future__ import print_function
import unittest
from mock import MagicMock as Mock, patch
from ILCDIRAC.Core.Utilities.InputFilesUtilities import getNumberOfEvents
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertDiracSucceedsWith_equals

from DIRAC import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Core.Utilities.InputFilesUtilities'

class TestgetNumberOfEvents( unittest.TestCase ):
  """tests of getNumberOfEvents"""
  def setUp(self):
    """
    Make fake files for the test
    """
    self.inputfile = "/ilc/prod/ilc/mc-dbd/generated/500-TDR_ws/6f_eeWW/v01-16-p05_500/00005160/000/E500-TDR_ws.I108640.P6f_eexyev.eL.pR_gen_5160_2_065.stdhep"
    self.inputfiles = [ self.inputfile, self.inputfile ]
    gLogger.setLevel("DEBUG")

  def tearDown(self):
    """ Remove the fake files
    """
    pass

  def test_getNumberOfEvents(self):
    """test getNumberOfEvents Single File Success..................................................."""
    fcMock = Mock()
    fcMock.getDirectoryUserMetadata = Mock(return_value=S_OK({"NumberOfEvents":500}))
    fcMock.getFileUserMetadata = Mock(return_value=S_OK({"NumberOfEvents":500}))

    with patch( "%s.FileCatalogClient" % MODULE_NAME, new=Mock(return_value=fcMock)):
      res = getNumberOfEvents(self.inputfiles)
    self.assertEqual(res['Value']['nbevts'],1000, res.get("Message",''))

  def test_getNumberOfEvents_2(self):
    """test getNumberOfEvents Multiple File Success................................................."""
    fcMock = Mock()
    fcMock.getDirectoryUserMetadata = Mock(return_value=S_OK({"NumberOfEvents":500}))
    fcMock.getFileUserMetadata = Mock(return_value=S_OK({"NumberOfEvents":500}))
    with patch( "%s.FileCatalogClient" % MODULE_NAME, new=Mock(return_value=fcMock)):
      res = getNumberOfEvents([self.inputfile])
    self.assertEqual(res['Value']['nbevts'],500, res.get("Message",''))

  def test_getNumberOfEvents_Fail(self):
    """test getNumberOfEvents Single File Failure..................................................."""
    fcMock = Mock()
    fcMock.getDirectoryUserMetadata = Mock(return_value=S_ERROR("No Such File"))
    fcMock.getFileUserMetadata = Mock(return_value=S_ERROR("No Such File"))
    with patch( "%s.FileCatalogClient" % MODULE_NAME, new=Mock(return_value=fcMock)):
      res = getNumberOfEvents(['/no/such/file'])
    self.assertFalse(res['OK'], res.get("Message",''))

  def test_getNumberOfEvents_Fail2(self):
    """test getNumberOfEvents Multiple File Failure................................................."""
    fcMock = Mock()
    fcMock.getDirectoryUserMetadata = Mock(return_value=S_ERROR("No Such File"))
    fcMock.getFileUserMetadata = Mock(return_value=S_ERROR("No Such File"))
    with patch( "%s.FileCatalogClient" % MODULE_NAME, new=Mock(return_value=fcMock)):
      res = getNumberOfEvents(['/no/such/file', '/no/such2/file2'])
    self.assertFalse(res['OK'], res.get("Message",''))

  def test_getnumberofevents_othercases( self ):
    # Expected behavior:
    # If one file in a directory, get its tags, if Number of events defined go to next entry
    # Else go to directory and check there. if nbevts go to next
    # Else iterate over all files in directory.
    # If numberevents defined nowhere, method fails
    file_meta_dict = { '/unique/dir/file3' : S_OK( { 'Luminosity' : '49.2' } ),
                       '/one/file/myfile' : S_OK( { 'NumberOfEvents' : '14' } ),
                       '/other/myfile2' : S_OK( { 'Luminosity' : 1489, 'NumberOfEvents' : 941.2 } ),
                       '/a/b/c/Dir1/someFile' : S_OK( { 'NumberOfEvents' : '14' } ),
                       '/a/b/c/Dir1/other_file' : S_OK( { 'NumberOfEvents' : '14' } ),
                       '/a/b/c/Dir1/dontforget_me' : S_OK( { 'NumberOfEvents' : '14' } ) }
    directory_meta_dict = { '/a/b/c/Dir1' : S_OK( { 'Luminosity' : 84.1, 'evttype' : 'testEvt' } ),
                            '/unique/dir' : S_OK( { 'NumberOfEvents' : 814, 'Luminosity' : None } ),
                            '/other' : S_OK( { 'NumberOfEvents' : None, 'Luminosity' : None } ),
                            '/one/file' : S_OK( {} ) }
    fcMock = Mock()
    fcMock.getDirectoryUserMetadata = Mock(side_effect=lambda path: directory_meta_dict[path])
    fcMock.getFileUserMetadata = Mock(side_effect=lambda filename : file_meta_dict[filename] )
    with patch( "%s.FileCatalogClient" % MODULE_NAME, new=Mock(return_value=fcMock)):
      assertDiracSucceedsWith_equals( getNumberOfEvents( [ '/a/b/c/Dir1/someFile', '', '/a/b/c/Dir1/other_file',
                                                           '/unique/dir/file3', '', '', '',
                                                           '/a/b/c/Dir1/dontforget_me', '/one/file/myfile',
                                                           '/other/myfile2' ] ),
                                      { 'AdditionalMeta': { 'evttype' : 'testEvt' }, 'EvtType' : '',
                                        'lumi' : 1790.5, 'nbevts' : 1811 }, self )

  def test_getnumberofevents_rarecase( self ):
    file_meta_dict = { '/a/b/c/Dir1/someFile' : S_OK( { 'NumberOfEvents' : '14' } ),
                       '/a/b/c/Dir1/other_file' : S_OK( { 'Luminosity' : '14.5' } ),
                       '/a/b/c/Dir1/dontforget_me' : S_OK( { 'NumberOfEvents' : '14' } ) }
    directory_meta_dict = { '/a/b/c/Dir1' : S_ERROR( { 'Luminosity' : 84.25, 'evttype' : 'testEvt' } ),
                            '/unique/dir' : S_ERROR( { 'NumberOfEvents' : 814, 'Luminosity' : None } ),
                            '/other' : S_ERROR( { 'NumberOfEvents' : None, 'Luminosity' : None } ),
                            '/one/file' : S_OK( {} ) }
    fcMock = Mock()
    fcMock.getDirectoryUserMetadata = Mock(side_effect=lambda path: directory_meta_dict[path])
    fcMock.getFileUserMetadata = Mock(side_effect=lambda filename : file_meta_dict[filename] )
    with patch( "%s.FileCatalogClient" % MODULE_NAME, new=Mock(return_value=fcMock)):
      assertDiracSucceedsWith_equals( getNumberOfEvents( [ '/a/b/c/Dir1/someFile', '', '/a/b/c/Dir1/other_file',
                                                           '', '', '', '/a/b/c/Dir1/dontforget_me' ] ),
                                      { 'AdditionalMeta': {}, 'EvtType' : '', 'lumi' : 14.5, 'nbevts' : 28 },
                                      self )

if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( TestgetNumberOfEvents )
  TESTRESULT = unittest.TextTestRunner( verbosity = 2 ).run( SUITE )
  print(TESTRESULT)
