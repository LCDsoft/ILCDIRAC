"""
tests for InputFilesUtilities

"""
import unittest
from mock import MagicMock as Mock, patch
from ILCDIRAC.Core.Utilities.InputFilesUtilities import getNumberOfEvents

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

if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( TestgetNumberOfEvents )
  TESTRESULT = unittest.TextTestRunner( verbosity = 2 ).run( SUITE )
  print TESTRESULT
