"""Test the dirac-ilc-moving-transformation script"""

import unittest

from mock import MagicMock as Mock, patch

from DIRAC import S_OK, S_ERROR

from ILCDIRAC.ILCTransformationSystem.Utilities.DataTransformation import createDataTransformation, checkDatatype
from ILCDIRAC.ILCTransformationSystem.Utilities.DataParameters import Params

__RCSID__ = "$Id$"


def getProxyMock(success=True):
  """ return value for getProxy """
  if success:
    return Mock(return_value=S_OK({'group': 'ilc_prod'}))

  return Mock(return_value=S_ERROR("Failed"))


def OpMock():
  """ return mock for config operations """
  opMock = Mock()
  opMock.getOptionsDict.return_value = S_OK({'REC': 'MCReconstruction_Overlay'})
  return Mock(return_value=opMock)

class TestMoving( unittest.TestCase ):
  """Test the creation of moving transformation"""

  def setUp ( self ):
    self.tClientMock = Mock()
    self.tClientMock.createTransformationInputDataQuery.return_value = S_OK()
    self.tMock = Mock( return_value=self.tClientMock )


  def tearDown ( self ):
    pass

  def test_createMoving_1( self ):
    """ test creating transformation """
    tSE = "Target-SRM"
    sSE = "Source-SRM"
    prodID = 12345
    dType = "DNS"
    module_name = "ILCDIRAC.ILCTransformationSystem.Utilities.DataTransformation"
    trmodule = "DIRAC.TransformationSystem.Client.Transformation.Transformation"
    with patch(trmodule+".getTransformation", new=Mock(return_value=S_OK({}))), \
         patch(trmodule+".addTransformation", new=Mock(return_value=S_OK())), \
         patch(trmodule+"._Transformation__setSE", new=Mock(return_value=S_OK())), \
         patch(module_name + ".checkDatatype", new=Mock(return_value=S_OK())), \
         patch("%s.TransformationClient" % module_name, new=self.tMock ):
      ret = createDataTransformation('Moving', tSE, sSE, prodID, dType)
    self.assertTrue( ret['OK'], ret.get('Message', "") )


  def test_createMoving_2( self ):
    """ test creating transformation """
    tSE = "Target-SRM"
    sSE = "Source-SRM"
    prodID = 12345
    dType = "DNS"
    module_name = "ILCDIRAC.ILCTransformationSystem.Utilities.DataTransformation"
    trmodule = "DIRAC.TransformationSystem.Client.Transformation.Transformation"
    with patch(trmodule+".getTransformation", new=Mock(return_value=S_OK({}))), \
         patch(trmodule+".addTransformation", new=Mock(return_value=S_OK())), \
         patch(trmodule+"._Transformation__setSE", new=Mock(return_value=S_OK())), \
         patch(module_name + ".checkDatatype", new=Mock(return_value=S_OK())), \
         patch("%s.TransformationClient" % module_name, new=self.tMock ):
      ret = createDataTransformation('Moving', tSE, sSE, prodID, dType, "extraName")
    self.assertTrue( ret['OK'], ret.get('Message', "") )


  def test_createMoving_SEFail_1( self ):
    """ test creating transformation """
    tSE = "Target-SRM"
    sSE = "Source-SRM"
    prodID = 12345
    dType = "DNS"
    module_name = "ILCDIRAC.ILCTransformationSystem.Utilities.DataTransformation"
    trmodule = "DIRAC.TransformationSystem.Client.Transformation.Transformation"
    with patch(trmodule+".getTransformation", new=Mock(return_value=S_OK({}))), \
         patch(trmodule+".addTransformation", new=Mock(return_value=S_OK())), \
         patch(trmodule+"._Transformation__setSE", new=Mock(side_effect=(S_OK(), S_ERROR()))), \
         patch(module_name + ".checkDatatype", new=Mock(return_value=S_OK())), \
         patch("%s.TransformationClient" % module_name, new=self.tMock ):
      ret = createDataTransformation('Moving', tSE, sSE, prodID, dType)
    self.assertFalse( ret['OK'], str(ret) )
    self.assertIn( "TargetSE not valid", ret['Message'] )

  def test_createMoving_SEFail_2( self ):
    """ test creating transformation """
    tSE = "Target-SRM"
    sSE = "Source-SRM"
    prodID = 12345
    dType = "DNS"
    module_name = "ILCDIRAC.ILCTransformationSystem.Utilities.DataTransformation"
    trmodule = "DIRAC.TransformationSystem.Client.Transformation.Transformation"
    with patch(trmodule+".getTransformation", new=Mock(return_value=S_OK({}))), \
         patch(trmodule+".addTransformation", new=Mock(return_value=S_OK())), \
         patch(trmodule+"._Transformation__setSE", new=Mock(side_effect=(S_ERROR(), S_ERROR()))), \
         patch(module_name + ".checkDatatype", new=Mock(return_value=S_OK())), \
         patch("%s.TransformationClient" % module_name, new=self.tMock ):
      ret = createDataTransformation('Moving', tSE, sSE, prodID, dType)
    self.assertFalse( ret['OK'], str(ret) )
    self.assertIn( "SourceSE not valid", ret['Message'] )


  def test_createMoving_addTrafoFail_( self ):
    """ test creating transformation """
    tSE = "Target-SRM"
    sSE = "Source-SRM"
    prodID = 12345
    dType = "DNS"
    module_name = "ILCDIRAC.ILCTransformationSystem.Utilities.DataTransformation"
    trmodule = "DIRAC.TransformationSystem.Client.Transformation.Transformation"
    with patch(trmodule+".getTransformation", new=Mock(return_value=S_OK({}))), \
         patch(trmodule+".addTransformation", new=Mock(return_value=S_ERROR("Cannot add Trafo"))), \
         patch(trmodule+"._Transformation__setSE", new=Mock(return_value=S_OK())), \
         patch(module_name + ".checkDatatype", new=Mock(return_value=S_OK())), \
         patch("%s.TransformationClient" % module_name, new=self.tMock ):
      ret = createDataTransformation('Moving', tSE, sSE, prodID, dType)
    self.assertFalse( ret['OK'], str(ret) )
    self.assertIn( "Cannot add Trafo", ret['Message'] )


  def test_createMoving_createTrafoFail_( self ):
    """ test creating transformation """
    tSE = "Target-SRM"
    sSE = "Source-SRM"
    prodID = 12345
    dType = "DNS"
    self.tClientMock.createTransformationInputDataQuery.return_value = S_ERROR("Failed to create IDQ")

    module_name = "ILCDIRAC.ILCTransformationSystem.Utilities.DataTransformation"
    trmodule = "DIRAC.TransformationSystem.Client.Transformation.Transformation"
    with patch(trmodule+".getTransformation", new=Mock(return_value=S_OK({}))), \
         patch(trmodule+".addTransformation", new=Mock(return_value=S_OK())), \
         patch(trmodule+"._Transformation__setSE", new=Mock(return_value=S_OK())), \
         patch(module_name + ".checkDatatype", new=Mock(return_value=S_OK())), \
         patch("%s.TransformationClient" % module_name, new=self.tMock ):
      ret = createDataTransformation('Moving', tSE, sSE, prodID, dType)
    self.assertFalse( ret['OK'], str(ret) )
    self.assertIn( "Failed to create transformation:Failed to create IDQ", ret['Message'] )


class TestMovingParams( unittest.TestCase ):
  """Test the parameters for the moving creation script"""

  def setUp ( self ):
    self.arguments = []
    self.sMock = Mock()
    self.sMock.getPositionalArgs.return_value = self.arguments
    self.params = Params()


  def tearDown ( self ):
    pass

  @patch("ILCDIRAC.ILCTransformationSystem.Utilities.DataParameters.getProxyInfo", new=getProxyMock())
  def test_checkSettings( self ):
    self.arguments = [ 12345, "TargetSE", "SourceSE", "GEN" ]
    self.sMock.getPositionalArgs.return_value = self.arguments
    ret = self.params.checkSettings( self.sMock )
    self.assertTrue( ret['OK'], ret.get( "Message", "") )
    self.assertEqual( self.params.prodIDs, [12345] )
    self.assertEqual( self.params.sourceSE, "SourceSE" )
    self.assertEqual( self.params.targetSE, ["TargetSE"] )
    self.assertEqual( self.params.datatype, "GEN" )

  @patch("ILCDIRAC.ILCTransformationSystem.Utilities.DataParameters.getProxyInfo", new=getProxyMock())
  def test_checkSettings_FailData( self ):
    self.arguments = [ 12345, "TargetSE", "SourceSE", "DNA" ]
    self.sMock.getPositionalArgs.return_value = self.arguments
    ret = self.params.checkSettings( self.sMock )
    self.assertFalse( ret['OK'], str(ret) )
    self.assertTrue( any( "ERROR: Unknown Datatype" in msg for msg in self.params.errorMessages ) )

  @patch("ILCDIRAC.ILCTransformationSystem.Utilities.DataParameters.getProxyInfo", new=getProxyMock())
  def test_checkSettings_FailArgumentSize( self ):
    self.arguments = [ 12345, "TargetSE", "SourceSE" ]
    self.sMock.getPositionalArgs.return_value = self.arguments
    ret = self.params.checkSettings( self.sMock )
    self.assertFalse( ret['OK'], str(ret) )
    self.assertTrue( any( "ERROR: Not enough arguments" in msg for msg in self.params.errorMessages ) )

  @patch("ILCDIRAC.ILCTransformationSystem.Utilities.DataParameters.getProxyInfo", new=getProxyMock(False))
  def test_FailProxy( self ):
    self.arguments = [ 12345, "TargetSE", "SourceSE", "GEN" ]
    self.sMock.getPositionalArgs.return_value = self.arguments
    ret = self.params.checkSettings( self.sMock )
    self.assertFalse( ret['OK'], str(ret) )
    self.assertTrue(any("ERROR: No Proxy" in msg for msg in self.params.errorMessages), str(self.params.errorMessages))


  @patch( "ILCDIRAC.Core.Utilities.CheckAndGetProdProxy.checkAndGetProdProxy", new = Mock( return_value=S_OK()) )
  def test_setExtraName( self ):
    ret = self.params.setExtraname( "extraName" )
    self.assertTrue( ret['OK'], ret.get('Message',"") )
    self.assertEqual( "extraName", self.params.extraname )

  @patch( "ILCDIRAC.Core.Utilities.CheckAndGetProdProxy.checkAndGetProdProxy", new = Mock( return_value=S_OK()) )
  def test_checkDatatype( self ):
    tMock = Mock()
    tMock.getTransformations.return_value= S_OK( [dict(Type="MCReconstruction_Overlay")] )
    module_name = "ILCDIRAC.ILCTransformationSystem.Utilities.DataTransformation"

    with patch(module_name + ".TransformationClient", new=Mock(return_value=tMock)), \
         patch(module_name + ".Operations", new=OpMock()):
      ret = checkDatatype(123, "REC")
      self.assertTrue( ret['OK'], ret.get('Message',"") )


  @patch( "ILCDIRAC.Core.Utilities.CheckAndGetProdProxy.checkAndGetProdProxy", new = Mock( return_value=S_OK()) )
  def test_checkDatatype_fail0( self ):
    tMock = Mock()
    tMock.getTransformations.return_value= S_OK( [dict(Type="MCReconstruction_Overlay")] )
    with patch( "DIRAC.TransformationSystem.Client.TransformationClient.TransformationClient",
                new = Mock( return_value=tMock) ):
      ret = checkDatatype(123, "Gen")
      self.assertFalse( ret['OK'], ret.get('Message',"") )


  @patch( "ILCDIRAC.Core.Utilities.CheckAndGetProdProxy.checkAndGetProdProxy", new = Mock( return_value=S_OK()) )
  def test_checkDatatype_fail1( self ):
    tMock = Mock()
    tMock.getTransformations.return_value= S_OK( [ dict(Type="MCReconstruction_Overlay"),
                                                   dict(Type="MCReconstruction_Overlay"),
                                                 ] )
    with patch( "DIRAC.TransformationSystem.Client.TransformationClient.TransformationClient",
                new = Mock( return_value=tMock) ):
      ret = checkDatatype(123, "REC")
      self.assertFalse( ret['OK'], ret.get('Message',"") )


  @patch( "ILCDIRAC.Core.Utilities.CheckAndGetProdProxy.checkAndGetProdProxy", new = Mock( return_value=S_OK()) )
  def test_checkDatatype_fail2( self ):
    tMock = Mock()
    tMock.getTransformations.return_value= S_ERROR( 'Failed to do something' )
    with patch( "DIRAC.TransformationSystem.Client.TransformationClient.TransformationClient",
                new = Mock( return_value=tMock) ):
      ret = checkDatatype(123, "REC")
      self.assertFalse( ret['OK'], ret.get('Message',"") )


if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( TestMoving )
  TESTRESULT = unittest.TextTestRunner( verbosity = 3 ).run( SUITE )
