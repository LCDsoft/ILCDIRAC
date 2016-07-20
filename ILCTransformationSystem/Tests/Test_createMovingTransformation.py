"""Test the dirac-ilc-moving-transformation script"""

import unittest

from mock import MagicMock as Mock, patch

from DIRAC import S_OK, S_ERROR


from ILCDIRAC.ILCTransformationSystem.Utilities.MovingTransformation import createMovingTransformation

__RCSID__ = "$Id$"

class TestMoving( unittest.TestCase ):
  """Test the DataRecoveryAgent"""
  
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
    module_name = "ILCDIRAC.ILCTransformationSystem.Utilities.MovingTransformation"
    trmodule = "DIRAC.TransformationSystem.Client.Transformation.Transformation"
    with patch(trmodule+".getTransformation", new=Mock(return_value=S_OK({}))), \
         patch(trmodule+".addTransformation", new=Mock(return_value=S_OK())), \
         patch(trmodule+"._Transformation__setSE", new=Mock(return_value=S_OK())), \
         patch("%s.TransformationClient" % module_name, new=self.tMock ):
      ret = createMovingTransformation( tSE, sSE, prodID, dType )
    self.assertTrue( ret['OK'], ret.get('Message', "") )


  def test_createMoving_2( self ):
    """ test creating transformation """
    tSE = "Target-SRM"
    sSE = "Source-SRM"
    prodID = 12345
    dType = "DNS"
    module_name = "ILCDIRAC.ILCTransformationSystem.Utilities.MovingTransformation"
    trmodule = "DIRAC.TransformationSystem.Client.Transformation.Transformation"
    with patch(trmodule+".getTransformation", new=Mock(return_value=S_OK({}))), \
         patch(trmodule+".addTransformation", new=Mock(return_value=S_OK())), \
         patch(trmodule+"._Transformation__setSE", new=Mock(return_value=S_OK())), \
         patch("%s.TransformationClient" % module_name, new=self.tMock ):
      ret = createMovingTransformation( tSE, sSE, prodID, dType, "extraName" )
    self.assertTrue( ret['OK'], ret.get('Message', "") )


  def test_createMoving_SEFail_1( self ):
    """ test creating transformation """
    tSE = "Target-SRM"
    sSE = "Source-SRM"
    prodID = 12345
    dType = "DNS"
    module_name = "ILCDIRAC.ILCTransformationSystem.Utilities.MovingTransformation"
    trmodule = "DIRAC.TransformationSystem.Client.Transformation.Transformation"
    with patch(trmodule+".getTransformation", new=Mock(return_value=S_OK({}))), \
         patch(trmodule+".addTransformation", new=Mock(return_value=S_OK())), \
         patch(trmodule+"._Transformation__setSE", new=Mock(side_effect=(S_OK(), S_ERROR()))), \
         patch("%s.TransformationClient" % module_name, new=self.tMock ):
      ret = createMovingTransformation( tSE, sSE, prodID, dType )
    self.assertFalse( ret['OK'], str(ret) )
    self.assertIn( "TargetSE not valid", ret['Message'] )

  def test_createMoving_SEFail_2( self ):
    """ test creating transformation """
    tSE = "Target-SRM"
    sSE = "Source-SRM"
    prodID = 12345
    dType = "DNS"
    module_name = "ILCDIRAC.ILCTransformationSystem.Utilities.MovingTransformation"
    trmodule = "DIRAC.TransformationSystem.Client.Transformation.Transformation"
    with patch(trmodule+".getTransformation", new=Mock(return_value=S_OK({}))), \
         patch(trmodule+".addTransformation", new=Mock(return_value=S_OK())), \
         patch(trmodule+"._Transformation__setSE", new=Mock(side_effect=(S_ERROR(), S_ERROR()))), \
         patch("%s.TransformationClient" % module_name, new=self.tMock ):
      ret = createMovingTransformation( tSE, sSE, prodID, dType )
    self.assertFalse( ret['OK'], str(ret) )
    self.assertIn( "SourceSE not valid", ret['Message'] )


  def test_createMoving_addTrafoFail_( self ):
    """ test creating transformation """
    tSE = "Target-SRM"
    sSE = "Source-SRM"
    prodID = 12345
    dType = "DNS"
    module_name = "ILCDIRAC.ILCTransformationSystem.Utilities.MovingTransformation"
    trmodule = "DIRAC.TransformationSystem.Client.Transformation.Transformation"
    with patch(trmodule+".getTransformation", new=Mock(return_value=S_OK({}))), \
         patch(trmodule+".addTransformation", new=Mock(return_value=S_ERROR("Cannot add Trafo"))), \
         patch(trmodule+"._Transformation__setSE", new=Mock(return_value=S_OK())), \
         patch("%s.TransformationClient" % module_name, new=self.tMock ):
      ret = createMovingTransformation( tSE, sSE, prodID, dType )
    self.assertFalse( ret['OK'], str(ret) )
    self.assertIn( "Cannot add Trafo", ret['Message'] )


  def test_createMoving_createTrafoFail_( self ):
    """ test creating transformation """
    tSE = "Target-SRM"
    sSE = "Source-SRM"
    prodID = 12345
    dType = "DNS"

    self.tClientMock.createTransformationInputDataQuery.return_value = S_ERROR("Failed to create IDQ")

    module_name = "ILCDIRAC.ILCTransformationSystem.Utilities.MovingTransformation"
    trmodule = "DIRAC.TransformationSystem.Client.Transformation.Transformation"
    with patch(trmodule+".getTransformation", new=Mock(return_value=S_OK({}))), \
         patch(trmodule+".addTransformation", new=Mock(return_value=S_OK())), \
         patch(trmodule+"._Transformation__setSE", new=Mock(return_value=S_OK())), \
         patch("%s.TransformationClient" % module_name, new=self.tMock ):
      ret = createMovingTransformation( tSE, sSE, prodID, dType )
    self.assertFalse( ret['OK'], str(ret) )
    self.assertIn( "Failed to create transformation:Failed to create IDQ", ret['Message'] )



if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( TestMoving )
  TESTRESULT = unittest.TextTestRunner( verbosity = 3 ).run( SUITE )
