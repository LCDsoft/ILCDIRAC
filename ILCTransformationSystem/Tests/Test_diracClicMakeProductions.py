"""Test the dirac-ilc-make-productions script."""

import os
import importlib
import ConfigParser
from collections import defaultdict

import pytest
from mock import MagicMock as Mock, patch

from DIRAC import S_OK, S_ERROR

from ILCDIRAC.ILCTransformationSystem.Utilities.Utilities import Task

# pylint: disable=protected-access, invalid-name, missing-docstring, redefined-outer-name
THE_SCRIPT = "ILCDIRAC.ILCTransformationSystem.scripts.dirac-clic-make-productions"
theScript = importlib.import_module(THE_SCRIPT)
SCP = "ILCDIRAC.ILCTransformationSystem.scripts.dirac-clic-make-productions.ConfigParser.SafeConfigParser"
__RCSID__ = "$Id$"

CONFIG_DICT = {}


def test_environ():
  """Ensure we are not running optimized python.

  Some assertion in the pytest code are otherwise ignored.
  """
  assert "PYTHONOPTIMIZE" not in os.environ


def configDict():
  """Return dictionary for dirac configuration system."""
  return {'prodGroup': "myProdGroup",
          'detectorModel': 'myDetectorModel',
          'softwareVersion': 'mySoftwareVersion',
          'configVersion': 'my',
          'configPackage': 'ClicConfig',
          'processes': 'process1, process2',
          'energies': '100, 200',
          'eventsPerJobs': '1000, 2000',
          'productionloglevel': 'DEBUGLEVEL3',
          'outputSE': 'CERN-CASTOR',
          'finalOutputSE': 'VAULT-101',
          'additionalName': 'waitForIt',
          'prodIDs': '123, 456',
          'eventsInSplitFiles': '5000, 6000',
          'ProdTypes': 'Gen, RecOver',
          'MoveTypes': '',
          'MoveStatus': 'Active',
          'MoveGroupSize': '11',
          'overlayEvents': '',
          'overlayEventType': '',
          'cliReco': '--Config.Tracking=Tracked',
          'whizard2Version': 'myWhizardVersion',
          'whizard2SinFile': 'myWhizardSinFile1, myWhizardSinFile2',
          'numberOfTasks': '1, 2',
          'ignoreMetadata': '',
          'taskNames': 'taskA, taskB',
          }


@pytest.fixture
def opsMock():
  """Return fixture for Operations."""
  def mockOpsConfig(*args, **kwargs):  # pylint: disable=unused-argument
    """Mock the operations getValue calls."""
    opsDict = {'DefaultDetectorModel': 'detModel',
               'DefaultConfigVersion': 'Config',
               'DefaultConfigPackage': 'Click',
               'DefaultSoftwareVersion': 'Software',
               'FailOverSE': 'FAIL=SRM',
               'DefaultWhizard2Version': '1.9.5',
               'BasePath': '',
              }
    for opName, value in opsDict.items():
      if args[0].endswith(opName):
        return value
    assert args[0] == opsDict
    return None
  theOps = Mock(name='OpsMock')
  theOps.getValue = mockOpsConfig
  return theOps


@pytest.fixture
def pMockMod():
  """Return Module for ProductionJob."""
  pMockMod = Mock()
  pjMock = Mock(name="ProductionJob")
  pMockMod.return_value = pjMock
  pjMock.getMetadata.return_value = {}
  return pMockMod


@pytest.fixture
def theChain(opsMock):
  """Return production chain fixture."""
  params = Mock()
  params.additionalName = ''
  params.dryRun = True
  with patch("ILCDIRAC.ILCTransformationSystem.scripts.dirac-clic-make-productions.CLICDetProdChain.loadParameters",
             new=Mock()), \
       patch("DIRAC.ConfigurationSystem.Client.Helpers.Operations.Operations",
             new=Mock(return_value=opsMock)):
    chain = theScript.CLICDetProdChain(params)

  return chain


@pytest.fixture
def aTask():
  """Return aTask with minimal content."""
  task = Task({'Energy': 300.0}, {}, 333)
  return task


@pytest.fixture
def cpMock():
  """Return a Mock for the ConfigParser."""
  theCPMock = Mock()
  theCPMock.thisConfigDict = dict(configDict())

  def hasMock(*args, **kwargs):  # pylint: disable=unused-argument
    """Mock the configparser.has_option function."""
    return theCPMock.thisConfigDict.get(args[1])

  def mockConfig(*args, **kwargs):  # pylint: disable=unused-argument
    """Mock the configparser object."""
    assert args[0] == theScript.PP
    return theCPMock.thisConfigDict[args[1]]

  theCPMock.read = Mock()
  theCPMock.get = mockConfig
  theCPMock.has_option = hasMock
  return theCPMock


def test_meta(theChain):
  """Test meta data."""
  ret = theChain.meta(123, 'process', 555.5)
  assert {'ProdID': '123',
          'EvtType': 'process',
          'Energy': '555.5',
          'Machine': 'clic'} == ret


@pytest.mark.parametrize('eInput, eOutput',
                         [(123.0, '123'),
                          (123.03, '123.03'),
                          (123.235, '123.23'),
                          ('123.235', '123.235'),
                          ])
def test_metaEnergy(theChain, eInput, eOutput):
  """Test meta data."""
  assert theChain.metaEnergy(eInput) == eOutput


def test_overlayParameter(theChain):
  """Test overlayPamareters."""
  assert theChain.checkOverlayParameter('300GeV') == '300GeV'
  assert theChain.checkOverlayParameter('3TeV') == '3TeV'
  assert theChain.checkOverlayParameter('') == ''

  with pytest.raises(RuntimeError, match="does not end with unit"):
    theChain.checkOverlayParameter('3000')

  with pytest.raises(RuntimeError, match="does not end with unit"):
    theChain.checkOverlayParameter('3tev')


def test_loadParameters(theChain, cpMock):
  """Test load parameters."""
  parameter = Mock()
  parameter.prodConfigFilename = None
  parameter.dumpConfigFile = None
  theChain.loadParameters(parameter)
  c = theChain

  parameter.prodConfigFilename = 'filename'

  with patch(SCP, new=Mock(return_value=cpMock)):
    c.loadParameters(parameter)
  assert c.prodGroup == "myProdGroup"
  assert c.detectorModel == "myDetectorModel"
  assert c.prodIDs == [123, 456]
  assert c.energies == [100, 200]
  assert c.eventsPerJobs == [1000, 2000]
  assert c.eventsInSplitFiles == [5000, 6000]

  assert c.whizard2Version == "myWhizardVersion"
  assert c.whizard2SinFile == ['myWhizardSinFile1', 'myWhizardSinFile2']

  assert c.moveStatus == 'Active'
  assert c.moveGroupSize == '11'

  cpMock.thisConfigDict['prodIDs'] = "123, 456, 789"
  with patch(SCP, new=Mock(return_value=cpMock)), \
       pytest.raises(AttributeError, match="Lengths of Processes"):
    c.loadParameters(parameter)

  cpMock.thisConfigDict['prodIDs'] = ''
  cpMock.has_option = Mock()
  cpMock.has_option.return_value = False
  with patch(SCP, new=Mock(return_value=cpMock)):
    c.loadParameters(parameter)
  assert c.prodIDs == [1, 1]
  assert c.cliRecoOption == '--Config.Tracking=Tracked'

  cpMock.thisConfigDict['eventsInSplitFiles'] = "1000"
  c._flags._spl = True
  with patch(SCP, new=Mock(return_value=cpMock)), \
       pytest.raises(AttributeError, match="Length of eventsInSplitFiles"):
    c.loadParameters(parameter)

  parameter.prodConfigFilename = None
  parameter.dumpConfigFile = True
  with patch(SCP, new=Mock(return_value=cpMock)), \
    pytest.raises(RuntimeError, match="^$"):
    c.loadParameters(parameter)

  parameter.prodConfigFilename = 'filename'
  parameter.dumpConfigFile = False
  cpMock.thisConfigDict['MoveStatus'] = 'Foo'
  with patch(SCP, new=Mock(return_value=cpMock)), \
       pytest.raises(AttributeError, match='MoveStatus can only be'):
    c.loadParameters(parameter)


def test_createMarlinApplication(theChain, aTask, cpMock):
  """Test creating the marlin application."""
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import Marlin

  parameter = Mock()
  parameter.prodConfigFilename = 'filename'
  parameter.dumpConfigFile = False
  with patch(SCP, new=Mock(return_value=cpMock)):
    theChain.loadParameters(parameter)

  ret = theChain.createMarlinApplication(aTask, over=True)
  assert isinstance(ret, Marlin)
  assert ret.detectortype == 'myDetectorModel'
  assert ret.steeringFile == 'clicReconstruction.xml'
  assert theChain.cliRecoOption == '--Config.Tracking=Tracked'
  assert ret.extraCLIArguments == '--Config.Tracking=Tracked  --Config.Overlay=300GeV'

  with patch(SCP, new=Mock(return_value=cpMock)):
    theChain.loadParameters(parameter)
  theChain._flags._over = False

  ret = theChain.createMarlinApplication(aTask, over=False)
  assert isinstance(ret, Marlin)
  assert ret.detectortype == 'myDetectorModel'
  assert ret.steeringFile == 'clicReconstruction.xml'
  assert theChain.cliRecoOption == '--Config.Tracking=Tracked'
  assert ret.extraCLIArguments == '--Config.Tracking=Tracked'


def test_createWhizard2Application(theChain, aTask, cpMock, opsMock):
  """Test creating the whizard2 application."""
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import Whizard2

  parameter = Mock(name="ParameterMock")
  parameter.whizard2SinFile = 'filename'
  parameter.dumpConfigFile = False
  with patch(SCP, new=Mock(return_value=cpMock)), \
       patch("DIRAC.ConfigurationSystem.Client.Helpers.Operations.Operations",
             new=Mock(return_value=opsMock)):
    theChain.loadParameters(parameter)

  aTask.meta = {'ProdID': '123', 'EvtType': 'process', 'Energy': '555', 'Machine': 'clic'}
  aTask.eventsPerJob = 100
  aTask.sinFile = 'sinFile'

  ret = theChain.createWhizard2Application(aTask)
  assert isinstance(ret, Whizard2)
  assert ret.version == 'myWhizardVersion'


def test_createDDSimApplication(theChain, aTask, cpMock, opsMock):
  """Test creating the ddsim application."""
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import DDSim
  parameter = Mock()
  parameter.prodConfigFilename = 'filename'
  parameter.dumpConfigFile = False
  with patch(SCP, new=Mock(return_value=cpMock)), \
       patch("DIRAC.ConfigurationSystem.Client.Helpers.Operations.Operations",
             new=Mock(return_value=opsMock)):
    theChain.loadParameters(parameter)

  ret = theChain.createDDSimApplication(aTask)
  assert isinstance(ret, DDSim)
  assert ret.steeringFile == 'clic_steer.py'


def test_createSplitApplication(theChain, cpMock):
  """Test creating the splitting application."""
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import StdHepSplit

  parameter = Mock()
  parameter.prodConfigFilename = 'filename'
  parameter.dumpConfigFile = False
  with patch(SCP, new=Mock(return_value=cpMock)):
    theChain.loadParameters(parameter)

  ret = theChain.createSplitApplication(100, 1000, 'stdhep')
  assert isinstance(ret, StdHepSplit)
  assert ret.datatype == 'gen'
  assert ret.maxRead == 1000
  assert ret.numberOfEventsPerFile == 100


def test_createOverlayApplication(theChain, aTask, cpMock):
  """Test creating the overlay application."""
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import OverlayInput
  parameter = Mock()
  parameter.prodConfigFilename = 'filename'
  parameter.dumpConfigFile = False
  with patch(SCP, new=Mock(return_value=cpMock)):
    theChain.loadParameters(parameter)
  aTask.meta['Energy'] = 350
  ret = theChain.createOverlayApplication(aTask)
  assert isinstance(ret, OverlayInput)
  assert ret.machine == 'clic_opt'
  aTask.meta['Energy'] = 355
  with pytest.raises(RuntimeError, match='No overlay parameters'):
    ret = theChain.createOverlayApplication(aTask)


def test_createSplitProduction(theChain, pMockMod):
  """Test creating the splitting production."""
  task = Task(metaInput={'ProdID': '23', 'Energy': '350'},
              parameterDict=theChain.getParameterDictionary('MI6')[0],
              eventsPerJob=0o07,
              eventsPerBaseFile=700,
              )
  assert task.meta['NumberOfEvents'] == 700
  with patch("ILCDIRAC.Interfaces.API.NewInterface.ProductionJob.ProductionJob", new=pMockMod):
    retMeta = theChain.createSplitProduction(task)
  assert retMeta == {}


def test_createRecoProduction(theChain, pMockMod):
  """Test creating the reco production."""
  theChain._flags._over = True
  assert theChain._flags.over
  theChain.overlayEvents = '1.4TeV'
  task = Task(metaInput={'ProdID': '23', 'Energy': '350'},
              parameterDict=theChain.getParameterDictionary('MI6')[0],
              eventsPerJob=321,
              )
  with patch("ILCDIRAC.Interfaces.API.NewInterface.ProductionJob.ProductionJob", new=pMockMod):
    retMeta = theChain.createReconstructionProduction(task, over=False)
  assert retMeta == {}
  assert theChain.cliRecoOption == ''


def test_createSimProduction(theChain, pMockMod):
  """Test creating the simulation production."""
  task = Task(metaInput={'ProdID': '23', 'Energy': '350'},
              parameterDict=theChain.getParameterDictionary('MI6')[0],
              eventsPerJob=333,
              )
  with patch("ILCDIRAC.Interfaces.API.NewInterface.ProductionJob.ProductionJob", new=pMockMod):
    retMeta = theChain.createSimulationProduction(task)
  assert retMeta == {}


def test_createGenProduction(theChain, pMockMod):
  """Test creating the generation production."""
  with patch("ILCDIRAC.Interfaces.API.NewInterface.ProductionJob.ProductionJob", new=pMockMod):
    task = Task(metaInput={'ProdID': '23', 'Energy': '350', 'EvtType': 'ttBond'},
                parameterDict=theChain.getParameterDictionary('MI6')[0],
                eventsPerJob=10,
                nbTasks='10',
                sinFile='myWhizardSinFile')
    retMeta = theChain.createGenerationProduction(task)
  assert retMeta == {}


def test_createMovingTransformation(theChain):
  """Test creating the moving productions."""
  theChain.outputSE = "Source"
  theChain.finalOutputSE = "Target"
  theChain.moveGroupSize = 13
  theChain._flags._rec = True
  theChain._flags._sim = True
  theChain._flags._moveDst = True
  theChain._flags._moveRec = False
  theChain._flags._moveSim = True
  theChain._flags._moves = True
  theChain._flags._dryRun = False
  with patch("DIRAC.TransformationSystem.Utilities.ReplicationTransformation.createDataTransformation") as moveMock:
    theChain.createMovingTransformation({'ProdID': 666}, 'MCReconstruction')
    parDict = dict(flavour='Moving',
                   targetSE='Target',
                   sourceSE='Source',
                   plugin='Broadcast',
                   metaKey='ProdID',
                   metaValue=666,
                   extraData={'Datatype': 'DST'},
                   tGroup='several',
                   groupSize=13,
                   enable=True,
                   )
    moveMock.assert_called_once_with(**parDict)

  with patch("DIRAC.TransformationSystem.Utilities.ReplicationTransformation.createDataTransformation") as moveMock:
    theChain.createMovingTransformation({'ProdID': 666}, 'MCSimulation')
    parDict = dict(flavour='Moving',
                   targetSE='Target',
                   sourceSE='Source',
                   plugin='BroadcastProcessed',
                   metaKey='ProdID',
                   metaValue=666,
                   extraData={'Datatype': 'SIM'},
                   tGroup='several',
                   groupSize=13,
                   enable=True,
                   )
    moveMock.assert_called_once_with(**parDict)

  theChain._flags._rec = True
  theChain._flags._moves = False
  theChain._flags._dryRun = False
  with patch("DIRAC.TransformationSystem.Utilities.ReplicationTransformation.createDataTransformation") as moveMock:
    theChain.createMovingTransformation({'ProdID': 666}, 'MCReconstruction')
    moveMock.assert_not_called()

  with pytest.raises(RuntimeError, match='ERROR creating Moving'):
    theChain.createMovingTransformation({'ProdID': 666}, "Split")


def test_setApplicationOptions(theChain, aTask):
  """Test setting the application options."""
  application = Mock()
  application.setSomeParameter = Mock()
  aTask.applicationOptions = {'foo': 'bar'}
  theChain.applicationOptions['AppName'] = {'SomeParameter': 'SomeValue', 'FE.foo': ['bar', 'baz'],
                                            'C_Repl': 'longValueWeDoNotwantToRepeat'}
  theChain._setApplicationOptions('AppName', application, aTask.applicationOptions)
  application.setSomeParameter.assert_called_once_with('SomeValue')
  application.setfoo.assert_called_once_with('bar')

  from ILCDIRAC.Interfaces.API.NewInterface.Applications import Marlin
  application = Marlin()
  theChain.applicationOptions['AppName'] = {'SomeOtherParameter': 'SomeValue'}
  with pytest.raises(AttributeError, match='Cannot set'):
    theChain._setApplicationOptions('AppName', application)


def test_getProdInfoFromIDs(theChain):
  """Test getting theproduction information."""
  # successful
  theChain.prodIDs = [12345]
  trClientMock = Mock(name='trClient')
  trClientMock.getTransformation.return_value = S_OK({'EventsPerTask': 123})
  trMock = Mock(return_value=trClientMock)
  fcClientMock = Mock(name='fcClient')
  fcClientMock.findFilesByMetadata.return_value = S_OK(['/path/to/file'])
  fcClientMock.getDirectoryUserMetadata.return_value = S_OK({'EvtType': 'haha', 'Energy': 321})
  fcMock = Mock(return_value=fcClientMock)
  with patch('DIRAC.TransformationSystem.Client.TransformationClient.TransformationClient', new=trMock), \
       patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient', new=fcMock):
    theChain._getProdInfoFromIDs()
  assert theChain.eventsPerJobs == [123]
  assert theChain.processes == ['haha']
  assert theChain.energies == [321]

  # first exception
  theChain.prodIDs = []
  with pytest.raises(AttributeError, match='No prodIDs'):
    theChain._getProdInfoFromIDs()

  # second exception
  theChain.prodIDs = [12345]
  trClientMock = Mock(name='trClient')
  trClientMock.getTransformation.return_value = S_ERROR('No such prod')
  trMock = Mock(return_value=trClientMock)
  with patch('DIRAC.TransformationSystem.Client.TransformationClient.TransformationClient', new=trMock), \
       pytest.raises(AttributeError, match='No prodInfo found'):
    theChain._getProdInfoFromIDs()

  # third exception
  theChain.prodIDs = [12345]
  trClientMock = Mock(name='trClient')
  trClientMock.getTransformation.return_value = S_OK({'EventsPerTask': 123})
  trMock = Mock(return_value=trClientMock)
  fcClientMock = Mock(name='fcClient')
  fcClientMock.findFilesByMetadata.return_value = S_ERROR('No files found')
  fcMock = Mock(return_value=fcClientMock)
  with patch('DIRAC.TransformationSystem.Client.TransformationClient.TransformationClient', new=trMock), \
       patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient', new=fcMock), \
       pytest.raises(AttributeError, match='Could not find file'):
    theChain._getProdInfoFromIDs()


def test_createTransformations(theChain):
  """Test replcation transformation creation."""
  theChain.createMovingTransformation = Mock(name='MovingTrafo')
  taskDict = dict(MOVE_GEN=[{'move': 'gen'}],
                  MOVE_SPLIT=[{'move': 'split'}],
                  MOVE_SIM=[{'move': 'sim'}],
                  MOVE_REC=[{'move': 'rec'}],
                  MOVE_OVER=[{'move': 'over'}],
                )
  theChain.createTransformations(taskDict)
  theChain.createMovingTransformation.assert_any_call({'move': 'gen'}, 'MCGeneration')
  theChain.createMovingTransformation.assert_any_call({'move': 'split'}, 'MCGeneration')
  theChain.createMovingTransformation.assert_any_call({'move': 'sim'}, 'MCSimulation')
  theChain.createMovingTransformation.assert_any_call({'move': 'rec'}, 'MCReconstruction')
  theChain.createMovingTransformation.assert_any_call({'move': 'over'}, 'MCReconstruction_Overlay')


def test_createTransformations_2(theChain):
  """Test workflow transformation creation."""
  theChain.createMovingTransformation = Mock(name='MovingTrafo')
  theChain.createGenerationProduction = Mock(name='GenTrafo', return_value={'ret': 'gen'})
  theChain.createSimulationProduction = Mock(name='SimTrafo', return_value={'ret': 'sim'})
  theChain.createReconstructionProduction = Mock(name='RecTrafo', return_value={'ret': 'rec'})
  theChain.addSimTask = Mock(name='AddSim')
  theChain.addRecTask = Mock(name='AddRec')

  task = Task(metaInput={'ProdID': '23', 'Energy': '350'},
              parameterDict=theChain.getParameterDictionary('MI6')[0],
              eventsPerJob=0o07,
              eventsPerBaseFile=700,
              )
  taskDict = defaultdict(list)
  taskDict['GEN'].append(task)
  theChain.createTransformations(taskDict)
  theChain.createGenerationProduction.assert_any_call(task)
  theChain.addSimTask.assert_called_with(taskDict, {'ret': 'gen'}, originalTask=task)

  # SIM transformation, off
  theChain.createMovingTransformation.reset_mock()
  theChain.createGenerationProduction.reset_mock()
  theChain.addSimTask.reset_mock()

  taskDict = defaultdict(list)
  taskDict['SIM'].append(task)
  theChain._flags._sim = False
  theChain.createTransformations(taskDict)
  theChain.createSimulationProduction.assert_not_called()
  theChain.addRecTask.assert_not_called()

  # SIM transformation, on
  theChain.createMovingTransformation.reset_mock()
  theChain.createGenerationProduction.reset_mock()
  theChain.addRecTask.reset_mock()

  taskDict = defaultdict(list)
  taskDict['SIM'].append(task)
  theChain._flags._sim = True
  theChain.createTransformations(taskDict)
  theChain.createSimulationProduction.assert_called_with(task)
  theChain.addRecTask.assert_called_with(taskDict, {'ret': 'sim'}, originalTask=task)

  # REC transformation, no over
  taskDict = defaultdict(list)
  taskDict['REC'].append(task)
  theChain._flags._rec = True
  theChain._flags._over = False
  theChain.createTransformations(taskDict)
  theChain.createReconstructionProduction.assert_called_once_with(task, over=False)

  # REC transformation, over
  theChain.createReconstructionProduction.reset_mock()
  taskDict = defaultdict(list)
  taskDict['REC'].append(task)
  theChain._flags._rec = False
  theChain._flags._over = True
  theChain.createTransformations(taskDict)
  theChain.createReconstructionProduction.assert_called_once_with(task, over=True)


def test_addSimTask(theChain):
  """Test adding sim task."""
  taskDict = defaultdict(list)
  theChain.addSimTask(taskDict, metaInput={'ProdID': '23', 'Energy': '350'}, originalTask=Task({}, {}, 123))
  assert len(taskDict['SIM']) == 1

  taskDict = defaultdict(list)
  theChain.applicationOptions['DDSim']['FE.steeringFile'] = ['a.py', 'b.py']
  theChain.applicationOptions['DDSim']['FE.additionalName'] = ['APY', 'BPY']
  theChain.addSimTask(taskDict, metaInput={'ProdID': '23', 'Energy': '350'}, originalTask=Task({}, {}, 123))
  assert len(taskDict['SIM']) == 2
  assert taskDict['SIM'][0].applicationOptions['steeringFile'] == 'a.py'
  assert taskDict['SIM'][1].applicationOptions['steeringFile'] == 'b.py'
  assert taskDict['SIM'][0].taskName == 'APY'
  assert taskDict['SIM'][1].taskName == 'BPY'


def test_addRecTask(theChain):
  """Test adding rec task."""
  taskDict = defaultdict(list)
  theChain.addRecTask(taskDict, metaInput={'ProdID': '23', 'Energy': '350'}, originalTask=Task({}, {}, 123))
  assert len(taskDict['REC']) == 1

  taskDict = defaultdict(list)
  theChain.applicationOptions['Marlin']['FE.steeringFile'] = ['a.xml', 'b.xml']
  theChain.applicationOptions['Marlin']['FE.QueryLanguage'] = ['EN', 'DE']
  theChain.applicationOptions['Marlin']['FE.cliReco'] = ['--Option=Value0', '--Option=Value1']
  theChain.addRecTask(taskDict, metaInput={'ProdID': '23', 'Energy': '350'}, originalTask=Task({}, {}, 123))
  assert len(taskDict['REC']) == 2
  assert taskDict['REC'][0].applicationOptions['steeringFile'] == 'a.xml'
  assert taskDict['REC'][1].applicationOptions['steeringFile'] == 'b.xml'
  assert taskDict['REC'][0].meta['Language'] == 'EN'
  assert taskDict['REC'][1].meta['Language'] == 'DE'
  assert taskDict['REC'][0].cliReco == '--Option=Value0'
  assert taskDict['REC'][1].cliReco == '--Option=Value1'


def test_addGenTask(theChain):
  """Test adding gen task."""
  taskDict = defaultdict(list)
  theChain.addGenTask(taskDict, originalTask=Task({}, {}, 123))
  assert len(taskDict['GEN']) == 1

  taskDict = defaultdict(list)
  theChain.applicationOptions['Whizard2']['FE.additionalName'] = ['1', '2']
  theChain.applicationOptions['Whizard2']['steeringFile'] = ['original.sin']
  theChain.addGenTask(taskDict, originalTask=Task({}, {}, 123))
  assert len(taskDict['GEN']) == 2
  assert taskDict['GEN'][0].taskName == '1'
  assert taskDict['GEN'][1].taskName == '2'


def test_createTaskDict_none(theChain):
  """Test createTaskDict function."""
  taskDict = theChain.createTaskDict(123456, 'ee_qq', 5000, 333, sinFile='file.sin', nbTasks=222,
                                     eventsPerBaseFile=None, taskName='')
  for pType in ['GEN', 'SIM', 'REC', 'SPLIT']:
    assert not taskDict[pType]


def test_createTaskDict_gen(theChain):
  """Test createTaskDict function."""
  theChain._flags._gen = True
  taskDict = theChain.createTaskDict(123456, 'ee_qq', 5000, 333, sinFile='file.sin', nbTasks=222,
                                     eventsPerBaseFile=None, taskName='')
  assert len(taskDict['GEN']) == 1
  assert taskDict['GEN'][0].sinFile == 'file.sin'
  assert taskDict['GEN'][0].nbTasks == 222
  assert taskDict['GEN'][0].meta == {'EvtType': 'ee_qq', 'ProdID': '123456',
                                     'Machine': 'clic',
                                     'NumberOfEvents': 333,
                                     'Energy': '5000',
                                     }


def test_createTaskDict_sim(theChain):
  """Test createTaskDict function."""
  theChain._flags._sim = True
  taskDict = theChain.createTaskDict(123456, 'ee_qqqq', 5000, 333, sinFile=None,
                                     nbTasks=None, eventsPerBaseFile=None, taskName='')
  assert len(taskDict['SIM']) == 1
  assert taskDict['SIM'][0].sinFile is None
  assert taskDict['SIM'][0].nbTasks is None
  assert taskDict['SIM'][0].eventsPerJob == 333


def test_createTaskDict_sim_split(theChain):
  """Test createTaskDict function."""
  theChain._flags._sim = True
  theChain._flags._spl = True
  # no need to split
  taskDict = theChain.createTaskDict(123456, 'ee_qqqq', 5000, 25, sinFile=None, nbTasks=None,
                                     eventsPerBaseFile=25, taskName='')
  assert not taskDict['SPLIT']
  assert len(taskDict['SIM']) == 1
  # need to split
  taskDict = theChain.createTaskDict(123456, 'ee_qqqq', 5000, 25, sinFile=None, nbTasks=None,
                                     eventsPerBaseFile=400, taskName='')
  assert len(taskDict['SPLIT']) == 1
  assert not taskDict['SIM']


def test_createTaskDict_nosplit(theChain):
  """Test createTaskDict function."""
  theChain._flags._spl = True
  taskDict = theChain.createTaskDict(123456, 'ee_qqqq', 5000, 25, sinFile=None, nbTasks=None,
                                     eventsPerBaseFile=25, taskName='')
  assert not taskDict['SPLIT']
  assert not taskDict['SIM']


def test_createTaskDict_rec(theChain):
  """Test createTaskDict function."""
  theChain._flags._rec = True
  theChain.applicationOptions['Marlin']['FE.QueryMachine'] = 'fccee, clic'
  taskDict = theChain.createTaskDict(123456, 'ee_qqqq', 5000, 25, sinFile=None, nbTasks=None,
                                     eventsPerBaseFile=400, taskName='')
  assert len(taskDict['REC']) == 2
  assert taskDict['REC'][0].meta['Machine'] == 'fccee'
  assert taskDict['REC'][1].meta['Machine'] == 'clic'


@pytest.fixture
def theFlags():
  """Return the flags fixture."""
  return theScript.CLICDetProdChain.Flags()


def test_flags_init(theFlags):
  """Returns the flags constructor."""
  f = theFlags
  assert f._dryRun
  assert not f._gen
  assert not f._spl
  assert not f._sim
  assert not f._rec
  assert not f._over
  assert not f._moves
  assert not f._moveGen
  assert not f._moveSim
  assert not f._moveRec
  assert not f._moveDst


def test_flags_properties(theFlags):
  """Test the flags properties."""
  f = theFlags
  f._gen = True
  f._spl = True
  f._sim = True
  f._rec = False
  f._over = True
  assert f.dryRun
  assert f.gen
  assert f.spl
  assert f.sim
  assert not f.rec
  assert f.over
  f._dryRun = True
  f._moves = True
  f._moveRec = True
  assert f.move
  assert not f.moveGen
  assert not f.moveSim
  assert f.moveRec
  assert not f.moveDst
  f._dryRun = False
  f._moveGen = True
  f._moveSim = True
  f._moveRec = True
  f._moveDst = False
  assert f.move
  assert f.moveGen
  assert f.moveSim
  assert f.moveRec
  assert not f.moveDst


def test_flags_str(theFlags):
  """Test the flags string representation."""
  theFlags._gen = True
  theFlags._sim = True
  theFlags._rec = False
  theFlags._over = True
  flagStr = str(theFlags)
  assert flagStr == """

#Productions to create: Gen, Split, Sim, Rec, RecOver
ProdTypes = Gen, Sim, RecOver

move = False

#Datatypes to move: Gen, Sim, Rec, Dst
MoveTypes = \n"""


def test_loadFlags(theFlags):
  """Test loading the flags from config."""
  myConfig = ConfigParser.SafeConfigParser()
  myConfig.add_section(theScript.PRODUCTION_PARAMETERS)
  myConfig.set(theScript.PRODUCTION_PARAMETERS, 'ProdTypes', 'Gen, Sim,Rec')
  myConfig.set(theScript.PRODUCTION_PARAMETERS, 'move', 'False')
  myConfig.set(theScript.PRODUCTION_PARAMETERS, 'MoveTypes', 'gen, dst')
  theFlags.loadFlags(myConfig)
  f = theFlags
  assert f.gen
  assert f.sim
  assert f.rec
  assert not f.over
  assert f._moveGen
  assert not f._moveSim
  assert not f._moveRec
  assert f._moveDst
  myConfig.set(theScript.PRODUCTION_PARAMETERS, 'MoveTypes', 'gen, dst, badType')
  with pytest.raises(AttributeError, match='badType'):
    theFlags.loadFlags(myConfig)


@pytest.fixture
def theParams():
  """Return the Params fixture."""
  return theScript.Params()


def test_params_init(theParams):
  """Test the Params constructor."""
  assert theParams.prodConfigFilename is None
  assert not theParams.dumpConfigFile
  assert theParams.dryRun
  assert theParams.additionalName == ''


def test_params_settters(theParams):
  """Test the Params setters."""
  with patch("%s.os.path.exists" % THE_SCRIPT, new=Mock(return_value=True)):
    assert theParams.setProdConf('myconf')['OK']

  assert theParams.prodConfigFilename, 'myconf'
  assert theParams.setDumpConf('_')['OK']
  assert theParams.dumpConfigFile
  assert theParams.setEnable('_')['OK']
  assert not theParams.dryRun
  assert theParams.setAddName('addName')['OK']
  assert theParams.additionalName, 'addName'
