#!/usr/local/env python

"""
Test user jobfinalization

"""
__RCSID__ = "$Id$"

import unittest
from mock import MagicMock as Mock, patch

from DIRAC import gLogger, S_OK, S_ERROR

from ILCDIRAC.Interfaces.API.NewInterface.Applications import DDSim

gLogger.setLevel("DEBUG")
gLogger.showHeaders(True)

class TestDDSim( unittest.TestCase ):
  """tests for the DDSim interface"""

  def setUp( self ):
    pass

  def tearDown( self ):
    """cleanup any files"""
    pass


  @patch( "ILCDIRAC.Interfaces.API.NewInterface.Applications.DDSim.getKnownDetectorModels",
          new = Mock(return_value=S_OK(['CLIC_o2_v03'])))
  def test_setDetectorModel1( self ):
    """test DDSIm setDetectorModel part of software................................................."""
    detModel = "CLIC_o2_v03"
    ddsim = DDSim()
    ddsim.setDetectorModel( detModel )
    self.assertEqual( ddsim.detectorModel, detModel )

  @patch( "ILCDIRAC.Interfaces.API.NewInterface.Applications.DDSim.getKnownDetectorModels",
          new = Mock(return_value=S_ERROR("No known models")))
  def test_setDetectorModel2( self ):
    """test DDSIm setDetectorModel part of software failure........................................."""
    detModel = "CLIC_o2_v03"
    ddsim = DDSim()
    res = ddsim.setDetectorModel( detModel )
    self.assertEqual( res['Message'], "No known models" )

  @patch( "os.path.exists", new = Mock(return_value=True ) )
  def test_setDetectorModel_TB_success( self ):
    """test DDSIm setDetectorModel tarBall success.................................................."""
    detModel = "CLIC_o2_v03"
    ext = ".tar.gz"
    ddsim = DDSim()
    ddsim.setDetectorModel( detModel+ext )
    self.assertEqual( ddsim.detectorModel, detModel )
    self.assertTrue( detModel+ext in ddsim.inputSB )

  @patch( "os.path.exists", new = Mock(return_value=False))
  def test_setDetectorModel_TB_notLocal( self ):
    """test DDSIm setDetectorModel tarBall notLocal................................................."""
    detModel = "CLIC_o2_v03"
    ddsim = DDSim()
    ddsim.setDetectorModel( detModel )
    self.assertEqual( ddsim.inputSB, [] )

  def test_setDetectorModel_LFN_succcess( self ):
    """test DDSIm setDetectorModel lfn success......................................................"""
    detModel = "lfn:/ilc/user/s/sailer/CLIC_o2_v03.tar.gz"
    ddsim = DDSim()
    ddsim.setDetectorModel( detModel )
    self.assertEqual( ddsim.detectorModel, "CLIC_o2_v03" )
    self.assertTrue( detModel in ddsim.inputSB )

  def test_setStartFrom1( self ):
    """test DDSIm setStartFrom 1...................................................................."""
    ddsim = DDSim()
    ddsim.setStartFrom( "Arg")
    self.assertTrue( ddsim._errorDict )

  def test_setStartFrom2( self ):
    """test DDSIm setStartFrom 2...................................................................."""
    ddsim = DDSim()
    ddsim.setStartFrom( 42 )
    self.assertEqual( ddsim.startFrom, 42 )


def runTests():
  """Runs our tests"""
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestDDSim )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  print testResult


if __name__ == '__main__':
  runTests()
