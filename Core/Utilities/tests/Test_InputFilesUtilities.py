"""
tests for InputFilesUtilities

"""
__RCSID__ = "$Id$"
import unittest
import importlib
from mock import Mock
from ILCDIRAC.Core.Utilities.InputFilesUtilities import getNumberOfEvents

from DIRAC import gLogger, S_OK, S_ERROR

class TestgetNumberOfEvents( unittest.TestCase ):
  """tests of getNumberOfEvents"""
  def setUp(self):
    """
    Make fake files for the test
    """
    self.inputfile = "/ilc/prod/ilc/mc-dbd/generated/500-TDR_ws/6f_eeWW/v01-16-p05_500/00005160/000/E500-TDR_ws.I108640.P6f_eexyev.eL.pR_gen_5160_2_065.stdhep"
    self.utils = importlib.import_module( 'ILCDIRAC.Core.Utilities.InputFilesUtilities' )
    self.utils.FileCatalogClient.getDirectoryUserMetadata = Mock(return_value=S_OK({"NumberOfEvents":500}))
    self.utils.FileCatalogClient.getFileUserMetadata = Mock(return_value=S_OK({"NumberOfEvents":500}))
    #self.utils.FileCatalogClient.getDirectoryUserMetadata.return_value(S_OK())
    self.inputfiles = [ self.inputfile, self.inputfile ]
    gLogger.setLevel("DEBUG")

  def tearDown(self):
    """ Remove the fake files
    """
    pass

  def test_getNumberOfEvents(self):
    """test getNumberOfEvents Single File Success..................................................."""
    res = getNumberOfEvents(self.inputfiles)
    self.assertEqual(res['Value']['nbevts'],1000)

  def test_getNumberOfEvents_2(self):
    """test getNumberOfEvents Multiple File Success................................................."""
    res = getNumberOfEvents([self.inputfile])
    self.assertEqual(res['Value']['nbevts'],500)

  def test_getNumberOfEvents_Fail(self):
    """test getNumberOfEvents Single File Failure..................................................."""
    self.utils.FileCatalogClient.getDirectoryUserMetadata = Mock(return_value=S_ERROR("No Such File"))
    self.utils.FileCatalogClient.getFileUserMetadata = Mock(return_value=S_ERROR("No Such File"))
    res = getNumberOfEvents(['/no/such/file'])
    self.assertFalse(res['OK'])

  def test_getNumberOfEvents_Fail2(self):
    """test getNumberOfEvents Multiple File Failure................................................."""
    self.utils.FileCatalogClient.getDirectoryUserMetadata = Mock(return_value=S_ERROR("No Such File"))
    self.utils.FileCatalogClient.getFileUserMetadata = Mock(return_value=S_ERROR("No Such File"))
    res = getNumberOfEvents(['/no/such/file', '/no/such2/file2'])
    self.assertFalse(res['OK'])

if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( TestgetNumberOfEvents )
  TESTRESULT = unittest.TextTestRunner( verbosity = 2 ).run( SUITE )
  print TESTRESULT
