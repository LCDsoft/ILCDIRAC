"""Test the dirac-ilc-moving-transformation script"""

import unittest
import importlib
import pytest

from mock import MagicMock as Mock, patch

from DIRAC import S_OK, S_ERROR

from ILCDIRAC.ILCTransformationSystem.Utilities.DataParameters import Params, checkDatatype
THE_SCRIPT = 'ILCDIRAC.ILCTransformationSystem.scripts.dirac-ilc-moving-transformation'

__RCSID__ = "$Id$"


def getProxyMock(success=True):
  """ return value for getProxy """
  if success:
    return Mock(return_value=S_OK({'groupProperties': ['ProductionManagement']}))

  return Mock(return_value=S_ERROR("Failed"))


def OpMock():
  """ return mock for config operations """
  opMock = Mock()
  opMock.getOptionsDict.return_value = S_OK({'REC': 'MCReconstruction_Overlay'})
  return Mock(return_value=opMock)


class TestMovingParams( unittest.TestCase ):
  """Test the parameters for the moving creation script"""

  def setUp ( self ):
    self.arguments = []
    self.sMock = Mock()
    self.sMock.getPositionalArgs.return_value = self.arguments
    self.params = Params()

  def tearDown ( self ):
    pass

  @patch("DIRAC.TransformationSystem.Utilities.ReplicationCLIParameters.getVOMSVOForGroup",
         new=Mock(return_value="VOMSED_VO"))
  @patch("DIRAC.TransformationSystem.Utilities.ReplicationCLIParameters.getProxyInfo",
         new=getProxyMock())
  def test_checkSettings( self ):
    self.arguments = ['12345', "TargetSE", "SourceSE", "GEN"]
    self.sMock.getPositionalArgs.return_value = self.arguments
    ret = self.params.checkSettings( self.sMock )
    self.assertTrue( ret['OK'], ret.get( "Message", "") )
    self.assertEqual(self.params.metaValues, [12345])
    self.assertEqual(self.params.sourceSE, ["SourceSE"])
    self.assertEqual( self.params.targetSE, ["TargetSE"] )
    self.assertEqual( self.params.datatype, "GEN" )

  def test_checkSettings_FailData( self ):
    self.arguments = ['12345', "TargetSE", "SourceSE", "DNA"]
    self.sMock.getPositionalArgs.return_value = self.arguments
    ret = self.params.checkSettings( self.sMock )
    self.assertFalse( ret['OK'], str(ret) )
    self.assertTrue( any( "ERROR: Unknown Datatype" in msg for msg in self.params.errorMessages ) )

  def test_checkSettings_FailArgumentSize( self ):
    self.arguments = ['12345', "TargetSE", "SourceSE"]
    self.sMock.getPositionalArgs.return_value = self.arguments
    ret = self.params.checkSettings( self.sMock )
    self.assertFalse( ret['OK'], str(ret) )
    self.assertTrue( any( "ERROR: Not enough arguments" in msg for msg in self.params.errorMessages ) )

  @patch("DIRAC.TransformationSystem.Utilities.ReplicationCLIParameters.getProxyInfo",
         new=getProxyMock(False))
  def test_FailProxy( self ):
    self.arguments = ['12345', "TargetSE", "SourceSE", "GEN"]
    self.sMock.getPositionalArgs.return_value = self.arguments
    ret = self.params.checkSettings( self.sMock )
    self.assertFalse( ret['OK'], str(ret) )
    self.assertTrue(any("ERROR: No Proxy" in msg for msg in self.params.errorMessages), str(self.params.errorMessages))


  def test_setExtraName( self ):
    ret = self.params.setExtraname( "extraName" )
    self.assertTrue( ret['OK'], ret.get('Message',"") )
    self.assertEqual( "extraName", self.params.extraname )

  def test_checkDatatype( self ):
    tMock = Mock()
    tMock.getTransformations.return_value= S_OK( [dict(Type="MCReconstruction_Overlay")] )
    module_name = "ILCDIRAC.ILCTransformationSystem.Utilities.DataParameters"
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


@pytest.fixture
def ParamFix():
  pInstance = Mock()
  pClass = Mock(return_value=pInstance)
  pClass.pInstance = pInstance
  pInstance.metaKey = 'ProdID'
  return pClass


@pytest.fixture
def movingModule(mocker, ParamFix):
  """Fixture for the script module, mocking some parts."""
  mocker.patch('DIRAC.TransformationSystem.Utilities.ReplicationCLIParameters.getProxyInfo', new=getProxyMock())
  mocker.patch('DIRAC.TransformationSystem.Utilities.ReplicationCLIParameters.getVOMSVOForGroup',
               new=Mock(return_value='VOMSED_VO'))
  theScript = importlib.import_module(THE_SCRIPT)
  theScript.Script = Mock(name='ScriptMock')
  theScript.Script.parseCommandLine = Mock(name='pclMock')
  theScript.getTransformationGroup = Mock(return_value='someGroup')
  theScript.checkDatatype = Mock(return_value=S_OK('SIM'))
  theScript.createDataTransformation = Mock(return_value=S_OK())
  theScript.Params = ParamFix
  return theScript


def test_createTrafo(movingModule):
  movingModule.Script.getPositionalArgs.return_value = ['12345', 'TargetSE', 'SourceSE', 'SIM']
  movingModule.Params.pInstance.checkSettings = Mock(name='RetVal', return_value=S_OK())
  movingModule.Params.pInstance.metaValues = [12345]
  movingModule.Params.pInstance.targetSE = ['TargetSE']
  movingModule.Params.pInstance.sourceSE = ['SourceSE']
  movingModule.Params.pInstance.forcemoving = False
  movingModule.Params.pInstance.enable = False
  movingModule.Params.pInstance.datatype = 'SIM'
  movingModule.Params.pInstance.extraname = ''
  movingModule.Params.pInstance.extraname = ''
  parDict = dict(flavour='Moving',
                 targetSE=['TargetSE'],
                 sourceSE=['SourceSE'],
                 metaKey='ProdID',
                 metaValue=12345,
                 extraData={'Datatype': 'SIM'},
                 extraname='',
                 plugin='BroadcastProcessed',
                 groupSize=10,
                 tGroup='someGroup',
                 enable=False,
               )
  assert movingModule._createTrafo() == 0
  movingModule.createDataTransformation.assert_called_with(**parDict)


def test_createTrafo_force(movingModule):
  movingModule.Script.getPositionalArgs.return_value = ['12345', 'TargetSE', 'SourceSE', 'SIM']
  movingModule.Params.pInstance.checkSettings = Mock(name='RetVal', return_value=S_OK())
  movingModule.Params.pInstance.metaValues = [12345]
  movingModule.Params.pInstance.targetSE = ['TargetSE']
  movingModule.Params.pInstance.sourceSE = ['SourceSE']
  movingModule.Params.pInstance.forcemoving = True
  movingModule.Params.pInstance.enable = False
  movingModule.Params.pInstance.datatype = 'SIM'
  movingModule.Params.pInstance.extraname = ''
  movingModule.Params.pInstance.extraname = ''
  parDict = dict(flavour='Moving',
                 targetSE=['TargetSE'],
                 sourceSE=['SourceSE'],
                 metaKey='ProdID',
                 metaValue=12345,
                 extraData={'Datatype': 'SIM'},
                 extraname='',
                 plugin='Broadcast',
                 groupSize=10,
                 tGroup='someGroup',
                 enable=False,
               )
  assert movingModule._createTrafo() == 0
  movingModule.createDataTransformation.assert_called_with(**parDict)


def test_createTrafo_dtypeFail(movingModule):
  movingModule.Params.pInstance.checkSettings = Mock(name='RetVal', return_value=S_OK())
  movingModule.Params.pInstance.metaValues = [12345]
  movingModule.checkDatatype = Mock(name='CheckDType', return_value=S_ERROR())
  assert movingModule._createTrafo() == 1


def test_createTrafo_createFail(movingModule):
  movingModule.Params.pInstance.checkSettings = Mock(name='RetVal', return_value=S_OK())
  movingModule.Params.pInstance.metaValues = [12345]
  movingModule.createDataTransformation = Mock(name='createT', return_value=S_ERROR())
  assert movingModule._createTrafo() == 1


def test_createTrafo_notEnougParameters(movingModule):
  movingModule.Params.pInstance.checkSettings = Mock(name='RetVal', return_value=S_ERROR())
  assert movingModule._createTrafo() == 1
