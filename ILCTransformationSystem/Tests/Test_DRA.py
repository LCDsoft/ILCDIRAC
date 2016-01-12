"""Test the DataRecoveryAgent"""

import unittest
import sys
from StringIO import StringIO

from mock import MagicMock as Mock, patch

from DIRAC import S_OK, S_ERROR

from ILCDIRAC.ILCTransformationSystem.Agent.DataRecoveryAgent import DataRecoveryAgent

__RCSID__ = "$Id$"

TINFOMOCK = Mock()

class TestDRA( unittest.TestCase ):
  """Test the DataRecoveryAgent"""
  dra = None


  @patch("DIRAC.Core.Base.AgentModule.PathFinder", new=Mock())
  @patch("DIRAC.ConfigurationSystem.Client.PathFinder.getSystemInstance", new=Mock() )
  @patch("ILCDIRAC.ILCTransformationSystem.Agent.DataRecoveryAgent.ReqClient", new=Mock() )
  def setUp ( self ):
    self.dra = DataRecoveryAgent( agentName="ILCTransformationSystem/DataRecoveryAgent", loadName="TestDRA" )
    self.dra.reqClient = Mock()
    self.dra.tClient = Mock()
    self.dra.fcClient = Mock()
    self.dra.jobMon = Mock()

  def tearDown ( self ):
    pass

  @patch("DIRAC.Core.Base.AgentModule.PathFinder", new=Mock())
  @patch("DIRAC.ConfigurationSystem.Client.PathFinder.getSystemInstance", new=Mock() )
  @patch("ILCDIRAC.ILCTransformationSystem.Agent.DataRecoveryAgent.ReqClient", new=Mock() )
  def test_init( self ):
    """test for DataRecoveryAgent initialisation...................................................."""
    res = DataRecoveryAgent( agentName="ILCTransformationSystem/DataRecoveryAgent", loadName="TestDRA" )
    self.assertIsInstance( res , DataRecoveryAgent )

  def test_beginExecution( self ):
    """test for DataRecoveryAgent beginExecution...................................................."""
    res = self.dra.beginExecution()
    self.assertIn( "MCReconstruction", self.dra.transformationTypes )
    self.assertFalse( self.dra.enabled )
    self.assertTrue( res['OK'] )

  def test_getEligibleTransformations_success( self ):
    """test for DataRecoveryAgent getEligibleTransformations success................................"""
    self.dra.tClient.getTransformations = Mock(
      return_value=S_OK( [dict(TransformationID=1234, TransformationName="TestProd12", Type="TestProd")] ) )

    res = self.dra.getEligibleTransformations( status="Active", typeList=['TestProds'] )
    self.assertTrue( res['OK'] )
    self.assertIsInstance( res['Value'], dict )
    vals = res['Value']
    self.assertIn( "1234", vals )
    self.assertIsInstance( vals['1234'], tuple )
    self.assertEqual( ("TestProd", "TestProd12"), vals["1234"] )

  def test_getEligibleTransformations_failed( self ):
    """test for DataRecoveryAgent getEligibleTransformations failure................................"""
    self.dra.tClient.getTransformations = Mock( return_value=S_ERROR( "No can Do" ) )
    res = self.dra.getEligibleTransformations( status="Active", typeList=['TestProds'] )
    self.assertFalse( res['OK'] )
    self.assertEqual( "No can Do", res['Message'] )

  def test_treatProduction1( self ):
    """test for DataRecoveryAgent treatProduction success1.........................................."""
    getJobMock = Mock( name = "getJobMOck" )
    getJobMock.getJobs.return_value = ( Mock( name = "jobsMOck" ), 50, 50 )
    tinfoMock = Mock( name = "infoMock", return_value = getJobMock )
    self.dra.checkAllJobs = Mock()
    with patch("ILCDIRAC.ILCTransformationSystem.Agent.DataRecoveryAgent.TransformationInfo", new=tinfoMock ):
      self.dra.treatProduction( prodID=1234, transName="TestProd12", transType="MCGeneration" ) ##returns None

  def test_treatProduction2( self ):
    """test for DataRecoveryAgent treatProduction success2.........................................."""
    getJobMock = Mock( name = "getJobMOck" )
    getJobMock.getJobs.return_value = ( Mock( name = "jobsMock" ), 50, 50 )
    tinfoMock = Mock( name = "infoMock", return_value = getJobMock )
    self.dra.checkAllJobs = Mock()
    with patch("ILCDIRAC.ILCTransformationSystem.Agent.DataRecoveryAgent.TransformationInfo", new=tinfoMock ):
      self.dra.treatProduction( prodID=1234, transName="TestProd12", transType="MCReconstruction" ) ##returns None


  def test_treatProduction3( self ):
    """test for DataRecoveryAgent treatProduction skip.............................................."""
    getJobMock = Mock( name = "getJobMOck" )
    getJobMock.getJobs.return_value = ( Mock( name = "jobsMock" ), 50, 50 )
    tinfoMock = Mock( name = "infoMock", return_value = getJobMock )
    self.dra.checkAllJobs = Mock()
    self.dra.jobCache[1234] = (50, 50)
    #catch the printout to check path taken
    out = StringIO()
    sys.stdout = out

    with patch("ILCDIRAC.ILCTransformationSystem.Agent.DataRecoveryAgent.TransformationInfo", new=tinfoMock ):
      self.dra.treatProduction( prodID=1234, transName="TestProd12", transType="MCReconstruction" ) ##returns None
    self.assertIn( "Skipping production 1234", out.getvalue().strip().splitlines()[0] )


if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( TestDRA )
  TESTRESULT = unittest.TextTestRunner( verbosity = 2 ).run( SUITE )
