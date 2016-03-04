#!/usr/local/env python

"""
Test user jobfinalization

"""

import unittest
from mock import MagicMock as Mock, patch, create_autospec

from DIRAC import gLogger, S_OK, S_ERROR

from ILCDIRAC.Interfaces.API.NewInterface.Applications import DDSim

__RCSID__ = "$Id$"

gLogger.setLevel("DEBUG")
gLogger.showHeaders(True)

#pylint: disable=protected-access

class TestDDSim( unittest.TestCase ):
  """tests for the DDSim interface"""

  def setUp( self ):
    pass

  def tearDown( self ):
    """cleanup any files"""
    pass


  @patch( "ILCDIRAC.Interfaces.API.NewInterface.Applications.DDSim.getKnownDetectorModels",
          new = Mock(return_value=S_OK({'CLIC_o2_v03':"/some/path"})))
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

  @patch( "ILCDIRAC.Interfaces.API.NewInterface.Applications.DDSim.getKnownDetectorModels",
          new = Mock(return_value=S_OK({'CLIC_o2_v04':"/some/path"})))
  def test_setDetectorModel3( self ):
    """test DDSIm setDetectorModel is not known....................................................."""
    detModel = "ATLAS"
    ddsim = DDSim()
    ret = ddsim.setDetectorModel( detModel )
    self.assertEqual( ddsim.detectorModel, '' )
    self.assertFalse( ret['OK'] )
    self.assertIn( "Unknown detector model in ddsim: ATLAS", ret['Message'] )

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
    ext = ".tgz"
    ddsim = DDSim()
    ddsim.setDetectorModel( detModel+ext )
    self.assertEqual( ddsim.inputSB, [] )
    self.assertEqual( ddsim.detectorModel, detModel )

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

  def test_getKnownDetModels1( self ):
    """test getKnownDetectorModels failure no version..............................................."""
    ddsim = DDSim()
    ret = ddsim.getKnownDetectorModels()
    self.assertFalse( ret['OK'] )
    self.assertEqual( "No software version defined", ret['Message'] )

  def test_getKnownDetModels2( self ):
    """test getKnownDetectorModels success.........................................................."""
    ddsim = DDSim()
    ddsim.version = "test"
    import DIRAC
    ddsim._ops = create_autospec(DIRAC.ConfigurationSystem.Client.Helpers.Operations.Operations, spec_set=True)
    ddsim._ops.getOptionsDict.return_value = S_OK({"detModel1":"/path", "detModel2":"/path2"})
    ret = ddsim.getKnownDetectorModels()
    self.assertIn( "detModel1", ret['Value'] )
    self.assertTrue( ret['OK'] )

def runTests():
  """Runs our tests"""
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestDDSim )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  print testResult


if __name__ == '__main__':
  runTests()
