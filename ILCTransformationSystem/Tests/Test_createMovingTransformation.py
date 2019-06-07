"""Test the dirac-ilc-moving-transformation script"""

import unittest
import importlib
import pytest

from mock import MagicMock as Mock, patch

from DIRAC import S_OK, S_ERROR

from ILCDIRAC.ILCTransformationSystem.Utilities.DataParameters import Params, checkDatatype
THE_SCRIPT = 'ILCDIRAC.ILCTransformationSystem.scripts.dirac-ilc-moving-transformation'
THE_REPL_SCRIPT = 'ILCDIRAC.ILCTransformationSystem.scripts.dirac-ilc-replication-transformation'

__RCSID__ = "$Id$"

PARAMS = dict(flavour='Moving')


class MyParams(Params):
  """Replacement to set some parameters for Param class"""
  def __init__(self):
    self.metaKey = 'MyKey'
    self.forcemoving = True
    self.errorMessages = []
    self.groupName = ''
    self.extraname = ''
    self.enable = False
    for name, val in PARAMS.items():
      setattr(self, name, val)


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
def mockProxy(mocker):
  mocker.patch('DIRAC.TransformationSystem.Utilities.ReplicationCLIParameters.getProxyInfo', new=getProxyMock())
  mocker.patch('DIRAC.TransformationSystem.Utilities.ReplicationCLIParameters.getVOMSVOForGroup',
               new=Mock(return_value='VOMSED_VO'))
  return


@pytest.fixture
def movingModule(mockProxy):
  """Fixture for the script module, mocking some parts."""
  theScript = importlib.import_module(THE_SCRIPT)
  theScript.Script = Mock(name='ScriptMock')
  theScript.Script.parseCommandLine = Mock(name='pclMock')
  theScript.getTransformationGroup = Mock(return_value='someGroup')
  theScript.checkDatatype = Mock(return_value=S_OK('SIM'))
  theScript.createDataTransformation = Mock(return_value=S_OK())
  return theScript


@pytest.fixture
def replModule(mockProxy):
  """Fixture for the script module, mocking some parts."""
  theScript = importlib.import_module(THE_REPL_SCRIPT)
  theScript.Script = Mock(name='ScriptMock')
  theScript.Script.parseCommandLine = Mock(name='pclMock')
  theScript.getTransformationGroup = Mock(return_value='someGroup')
  theScript.checkDatatype = Mock(return_value=S_OK('SIM'))
  theScript.createDataTransformation = Mock(return_value=S_OK())
  return theScript


def test_moving_createTrafo(movingModule, mocker):
  movingModule.Script.getPositionalArgs.return_value = ['12345', 'TargetSE', 'SourceSE', 'SIM']
  parDict = dict(flavour='Moving',
                 targetSE=['TargetSE'],
                 sourceSE=['SourceSE'],
                 metaKey='TransformationID',
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


def test_moving_createTrafo_force(movingModule, mocker):
  movingModule.Script.getPositionalArgs.return_value = ['12345', 'TargetSE', 'SourceSE', 'SIM']
  mocker.patch(THE_SCRIPT + '.Params', spec='ILCDIRAC.ILCTransformationSystem.Utilities.DataParameters.Params',
               new=MyParams)
  PARAMS['flavour'] = 'Moving'
  parDict = dict(flavour='Moving',
                 targetSE=['TargetSE'],
                 sourceSE=['SourceSE'],
                 metaKey='MyKey',
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


def test_moving_createTrafo_dtypeFail(movingModule):
  movingModule.Script.getPositionalArgs.return_value = ['12345', 'TargetSE', 'SourceSE', 'REC']
  movingModule.checkDatatype = Mock(name='CheckDType', return_value=S_ERROR())
  assert movingModule._createTrafo() == 1
  movingModule.checkDatatype.assert_called_once()


def test_moving_createTrafo_createFail(movingModule):
  movingModule.Script.getPositionalArgs.return_value = ['12345', 'TargetSE', 'SourceSE', 'SIM']
  movingModule.createDataTransformation = Mock(name='createT', return_value=S_ERROR())
  assert movingModule._createTrafo() == 1
  movingModule.createDataTransformation.assert_called_once()


def test_moving_createTrafo_notEnougParameters(movingModule):
  movingModule.Script.getPositionalArgs.return_value = ['12345', 'TargetSE', 'SourceSE']
  assert movingModule._createTrafo() == 1


def test_repl_createTrafo(replModule, mocker):
  replModule.Script.getPositionalArgs.return_value = ['12345', 'TargetSE', 'SourceSE', 'SIM']
  mocker.patch(THE_REPL_SCRIPT + '.Params', spec='ILCDIRAC.ILCTransformationSystem.Utilities.DataParameters.Params',
                        new=MyParams)
  PARAMS['flavour'] = 'Replication'
  PARAMS['plugin'] = 'Broadcast'
  parDict = dict(flavour='Replication',
                 targetSE=['TargetSE'],
                 sourceSE=['SourceSE'],
                 metaKey='MyKey',
                 metaValue=12345,
                 extraData={'Datatype': 'SIM'},
                 extraname='',
                 plugin='Broadcast',
                 groupSize=10,
                 tGroup='someGroup',
                 enable=False,
               )
  assert replModule._createTrafo() == 0
  replModule.createDataTransformation.assert_called_with(**parDict)


def test_repl_createTrafo_createFail(replModule):
  replModule.Script.getPositionalArgs.return_value = ['12345', 'TargetSE', 'SourceSE', 'SIM']
  replModule.createDataTransformation = Mock(name='createT', return_value=S_ERROR())
  assert replModule._createTrafo() == 1
  replModule.createDataTransformation.assert_called_once()


def test_repl_createTrafo_notEnougParameters(replModule):
  assert replModule._createTrafo() == 1
