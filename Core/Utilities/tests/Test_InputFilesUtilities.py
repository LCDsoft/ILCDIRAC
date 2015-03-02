"""
tests for InputFilesUtilities

"""
__RCSID__ = "$Id$"
import unittest

from ILCDIRAC.Core.Utilities.InputFilesUtilities import getNumberOfEvents

from DIRAC import gLogger
from DIRAC.Core.Base import Script
Script.parseCommandLine()


class TestgetNumberOfEvents( unittest.TestCase ):
  """tests of getNumberOfEvents"""
  def setUp(self):
    """
    Make fake files for the test
    """
    self.inputfile = "/ilc/prod/ilc/mc-dbd/generated/250-TDR_ws/higgs/qqh/0/v01-16-p10_250/00004343/000/rv01-16-p10_250.sv01-14-01-p00.E250-TDR_ws.I999.Pqqh_gen_4343_1_013.stdhep"

    self.inputfiles = [ self.inputfile, self.inputfile ]
    gLogger.setLevel("INFO")

  def tearDown(self):
    """ Remove the fake files
    """
    pass

  def test_getNumberOfEvents(self):
    res = getNumberOfEvents(self.inputfiles)
    self.assertEqual(res['Value']['nbevts'],400)

  def test_getNumberOfEvents_2(self):
    res = getNumberOfEvents([self.inputfile])
    self.assertEqual(res['Value']['nbevts'],200)


  def test_getNumberOfEvents_Fail(self):
    res = getNumberOfEvents(['/no/such/file'])
    self.assertFalse(res['OK'])

  def test_getNumberOfEvents_Fail2(self):
    res = getNumberOfEvents(['/no/such/file', '/no/such2/file2'])
    self.assertFalse(res['OK'])

if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( TestgetNumberOfEvents )
  TESTRESULT = unittest.TextTestRunner( verbosity = 2 ).run( SUITE )
  print TESTRESULT
