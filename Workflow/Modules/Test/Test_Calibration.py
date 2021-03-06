"""Unit tests for the Calibration.py file."""

from __future__ import print_function
import pytest
import os
import shutil
from mock import MagicMock as Mock
from ILCDIRAC.Workflow.Modules.Calibration import Calibration
from DIRAC import S_OK, S_ERROR
from ILCDIRAC.CalibrationSystem.Utilities.functions import searchFilesWithPattern

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Workflow.Modules.Calibration'


@pytest.fixture
def calib():
  """Create calibration run."""
  calib = Calibration()
  calib.applicationName = "Testing"
  calib.applicationVersion = "vTest"
  calib.debug = True
  calib.platform = 'dummy_platform'
  calib.applicationLog = 'dummy_applicationLog'
  calib.log = Mock()
  calib.detectorModel = 'FCCee_o1_v03'

  import ILCDIRAC as ilcdirac
  calib.SteeringFile = os.path.join(ilcdirac.__path__[0], 'Testfiles/clicReconstruction.xml')

  yield calib  # provide the fixture value

  dirName = os.path.dirname(os.path.realpath(__file__))
  filesToRemove = searchFilesWithPattern(dirName, 'marlinSteeringFile_*.xml')
  filesToRemove += searchFilesWithPattern(dirName, '%s_%s_Run_*.sh' % (calib.applicationName, calib.applicationVersion))

  potentialFilesToRemove = ['temp.sh', 'localEnv.log']
  for iFile in potentialFilesToRemove:
    fullFileName = os.path.join(dirName, iFile)
    if os.path.exists(fullFileName):
      filesToRemove += [fullFileName]

  for iFile in filesToRemove:
    try:
      os.remove(iFile)
    except EnvironmentError, e:
      print("Failed to delete file: %s" % iFile, str(e))

  del calib
  #  assert False


def test_missingConfigurations(calib, mocker):
  """Test cases when some configuration is missing."""
  calib.platform = None
  res = calib.runIt()
  assert not res['OK']
  assert res['Message'] == 'No ILC platform selected'
  calib.platform = 'dummy'

  calib.applicationLog = None
  res = calib.runIt()
  assert not res['OK']
  assert res['Message'] == 'No Log file provided'
  calib.applicationLog = 'dummy'

  calib.detectorModel = None
  res = calib.runIt()
  assert not res['OK']
  calib.detectorModel = 'dummy'

  mocker.patch.object(calib, 'prepareMARLIN_DLL', new=Mock(return_value=S_ERROR('')))
  res = calib.runIt()
  assert not res['OK']


def test_runScript_properInputArguments(calib, mocker):
  """Test CalibrationRun.runScrip function."""
  mocker.patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK()))
  mocker.patch('os.remove', new=Mock(return_value=True))
  inputxml = 'file1.xml'
  env_script_path = '/dummy/env/path'
  marlin_dll = '/dummy/marlin/dll'
  errorMessageToTest = "steeringfile is missing: %s" % inputxml
  res = calib.runScript(inputxml, env_script_path, marlin_dll)
  assert not res['OK']
  assert res['Message'].lower() == errorMessageToTest.lower()

  mocker.patch('%s.os.path.exists' % MODULE_NAME, return_value=True)
  res = calib.runScript(inputxml, env_script_path, marlin_dll)
  assert res['OK']


paramDictList = [
    {'OK': True, 'Value': {'currentStep': 0, 'currentStage': 1, 'currentPhase': 0, 'parameters': {
        "processor[@name='MyDDMarlinPandora']/parameter[@name='ECalToHadGeVCalibrationEndCap']": 'ECALTOHAD_YYYY'},
        'calibrationIsFinished': False}},
    {'OK': True, 'Value': {'currentStep': 1, 'currentStage': 1,
                           'currentPhase': 1, 'parameters': {}, 'calibrationIsFinished': False}},
    {'OK': True, 'Value': {'currentStep': 2, 'currentStage': 1,
                           'currentPhase': 2, 'parameters': {}, 'calibrationIsFinished': False}},
    {'OK': True, 'Value': {'currentStep': 3, 'currentStage': 1,
                           'currentPhase': 3, 'parameters': {}, 'calibrationIsFinished': False}},
    {'OK': True, 'Value': {'currentStep': 4, 'currentStage': 1,
                           'currentPhase': 4, 'parameters': {}, 'calibrationIsFinished': False}},
    {'OK': True, 'Value': {'currentStep': 5, 'currentStage': 2,
                           'currentPhase': 5, 'parameters': {}, 'calibrationIsFinished': False}},
    {'OK': True, 'Value': {'currentStep': 6, 'currentStage': 3,
                           'currentPhase': 0, 'parameters': {}, 'calibrationIsFinished': False}},
    {'OK': True, 'Value': {'currentStep': 7, 'currentStage': 3,
                           'currentPhase': 1, 'parameters': {}, 'calibrationIsFinished': False}},
    {'OK': True, 'Value': {'currentStep': 8, 'currentStage': 3,
                           'currentPhase': 2, 'parameters': {}, 'calibrationIsFinished': False}},
    {'OK': True, 'Value': {'currentStep': 9, 'currentStage': 3,
                           'currentPhase': 3, 'parameters': {}, 'calibrationIsFinished': False}},
    {'OK': True, 'Value': {'currentStep': 10, 'currentStage': 3,
                           'currentPhase': 4, 'parameters': {}, 'calibrationIsFinished': False}},
    {'OK': True, 'Value': {'currentStep': 11, 'currentStage': 3,
                           'currentPhase': 4, 'parameters': {}, 'calibrationIsFinished': False}},
    {'OK': True, 'Value': {'currentStep': 12, 'currentStage': 3, 'currentPhase': 4, 'parameters': {},
     'calibrationIsFinished': True}}]


@pytest.fixture
def helper_copyFiles():
  """Copy steering file to local test directory."""
  dirsToCopy = [os.path.join(os.environ['DIRAC'], "ILCDIRAC", "Testfiles", "fcceeConfig", "CalibrationPandoraSettings"),
                os.path.join(os.environ['DIRAC'], "ILCDIRAC", "Testfiles", "fcceeConfig", "PandoraSettingsFCCee")]
  for iDir in dirsToCopy:
    try:
      shutil.copytree(iDir, iDir.split('/')[-1])
    except EnvironmentError as e:
      print('ERROR Could not copy dir: %s, exception: %s' % (iDir, e))

  yield helper_copyFiles
  if os.path.exists('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK'):
    os.remove('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK')

  for iDir in dirsToCopy:
    iDir = iDir.split('/')[-1]

    if os.path.exists(iDir):
      try:
        shutil.rmtree(iDir, './')
      except EnvironmentError as e:
        print('ERROR Could not delete dir: %s, exception: %s' % (iDir, e))


def test_runIt_simple(calib, mocker):
  """Test runIt function."""
  mocker.patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK()))
  mocker.patch('%s.os.remove' % MODULE_NAME, return_value=True)
  mocker.patch('%s.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value={'OK': True, 'Value': 'dummy_EnvScript'}))
  mocker.patch('%s.updateSteeringFile' % MODULE_NAME, new=Mock(return_value={'OK': True, 'Value': None}))
  mocker.patch('%s.os.path.exists' % MODULE_NAME, return_value=True)

  calibClientMock = Mock(name='mock1')
  calibClientMock.requestNewParameters = Mock(side_effect=paramDictList, name='mock3')
  calibClientMock.requestNewPhotonLikelihood.return_value = Mock(return_value='dummy_NewPhotonLikelihood')
  calibClientMock.reportResult.return_value = Mock(return_value=True)
  mocker.patch('%s.CalibrationClient' % MODULE_NAME, new=Mock(return_value=calibClientMock, name='mock2'))

  calib.prepareMARLIN_DLL = Mock(return_value={'OK': True, 'Value': 'dummy_MARLIN_DLL'})
  calib.resolveInputSlcioFilesAndAddToParameterDict = Mock(return_value={'OK': True, 'Value': {}})

  res = calib.runIt()
  assert res['OK']
  print('currentStep: %s' % calib.currentStep)
  assert calib.currentStep == 11


def test_runIt_updatingSteeringFile(helper_copyFiles, calib, mocker):
  """Test runtIt and updateSteeringFile functions."""
  mocker.patch('%s.shellCall' % MODULE_NAME, new=Mock(return_value=S_OK()))
  mocker.patch('%s.os.remove' % MODULE_NAME, return_value=True)
  mocker.patch('%s.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value={'OK': True, 'Value': 'dummy_EnvScript'}))
  mocker.patch('%s.os.path.exists' % MODULE_NAME, return_value=True)

  calibClientMock = Mock(name='mock1')
  calibClientMock.requestNewParameters = Mock(side_effect=paramDictList, name='mock3')
  calibClientMock.requestNewPhotonLikelihood.return_value = Mock(return_value='dummy_NewPhotonLikelihood')
  calibClientMock.reportResult.return_value = Mock(return_value=True)
  mocker.patch('%s.CalibrationClient' % MODULE_NAME, new=Mock(return_value=calibClientMock, name='mock2'))

  calib.prepareMARLIN_DLL = Mock(return_value={'OK': True, 'Value': 'dummy_MARLIN_DLL'})
  calib.resolveInputSlcioFilesAndAddToParameterDict = Mock(return_value={'OK': True, 'Value': {}})

  res = calib.runIt()
  assert res['OK']
  print('currentStep: %s' % calib.currentStep)
  assert calib.currentStep == 11


initialParameterDict = {".//global/parameter[@name='MaxRecordNumber']": "10",
                        ".//global/parameter[@name='SkipNEvents']": "1",
                        ".//processor[@name='MyDDCaloDigi_10ns']/parameter[@name='ECALBarrelTimeWindowMax']": ' 10 ',
                        ".//global/parameter[@name='LCIOInputFiles']": '\n      /afs/cern.ch/work/e/eleogran/public/'
                                                                       'mu_sim/mu_validation_50kevents.slcio\n    ',
                        ".//processor[@name='MyDDMarlinPandora_10ns']/parameter[@name='MuonToMipCalibration']":
                        '20703.9',
                        ".//processor[@name='MyDDCaloDigi_10ns']/parameter[@name='ECALEndcapCorrectionFactor']":
                        '1.03245503522',
                        ".//processor[@name='MyDDMarlinPandora_10ns']/parameter[@name='ECalToHadGeVCalibrationEndCap']":
                        '1.11490774181',
                        ".//processor[@name='MyDDMarlinPandora_10ns']/parameter[@name='HCalToMipCalibration']":
                        '45.6621',
                        ".//processor[@name='MyDDMarlinPandora_10ns']/parameter[@name='HCalToEMGeVCalibration']":
                        '1.01776966108',
                        ".//processor[@name='MyDDMarlinPandora_10ns']/parameter[@name='ECalToHadGeVCalibrationBarrel']":
                        '1.11490774181',
                        ".//processor[@name='MyDDMarlinPandora_10ns']/parameter[@name='HCalToHadGeVCalibration']":
                        '1.00565042407',
                        ".//processor[@name='MyDDMarlinPandora_10ns']/parameter[@name='PandoraSettingsXmlFile']":
                        ' PandoraSettingsFCCee/PandoraSettingsDefault.xml ',
                        ".//processor[@name='MyDDMarlinPandora_10ns']/parameter[@name='ECalToMipCalibration']":
                        '175.439',
                        ".//processor[@name='MyDDMarlinPandora_10ns']/parameter[@name='ECalToEMGeVCalibration']":
                        '1.01776966108',
                        ".//processor[@name='MyDDCaloDigi_10ns']/parameter[@name='HCALEndcapTimeWindowMax']": ' 10 ',
                        ".//processor[@name='MyDDCaloDigi_10ns']/parameter[@name='CalibrHCALBarrel']": '45.9956826061',
                        ".//processor[@name='MyDDCaloDigi_10ns']/parameter[@name='CalibrHCALEndcap']": '46.9252540291',
                        ".//processor[@name='MyDDCaloDigi_10ns']/parameter[@name='CalibrECAL']":
                        '37.5227197175 37.5227197175',
                        ".//processor[@name='MyDDCaloDigi_10ns']/parameter[@name='CalibrHCALOther']": '57.4588011802',
                        ".//parameter[@name='MaxClusterEnergyToApplySoftComp']": 0,
                        ".//processor[@type='InitializeDD4hep']/parameter[@name='DD4hepXMLFile']":
                        '\n      /cvmfs/clicdp.cern.ch/iLCSoft/builds/nightly/x86_64-slc6-gcc62-opt/lcgeo/HEAD/FCCee/'
                        'compact/FCCee_o1_v04/FCCee_o1_v04.xml\n    ',
                        ".//processor[@name='MyDDMarlinPandora_10ns']/parameter[@name='MaxHCalHitHadronicEnergy']":
                        '10000000.',
                        ".//processor[@name='MyDDCaloDigi_10ns']/parameter[@name='ECALEndcapTimeWindowMax']": ' 10 ',
                        ".//processor[@name='MyDDCaloDigi_10ns']/parameter[@name='HCALBarrelTimeWindowMax']": ' 10 ',
                        ".//processor[@name='RootFile']": None}


def test_resolveInputSlcioFilesAndAddToParameterDict(calib, mocker):
  """Test resolveInputSlcioFilesAndAddToParameterDict function."""
  tmpDataDict = {'zuds': (['zuds.slcio'], 1, 24), 'gamma': (['gamma.slcio'], 2, 20),
                 'muon': (['muon.slcio'], 5, 28), 'kaon': (['kaon.slcio'], 8, 31)}
  tmpListOfFiles = ['zuds.slcio', 'gamma.slcio', 'muon.slcio', 'kaon.slcio']
  calibClientMock = Mock(name='mock1')
  calibClientMock.getInputDataDict.return_value = S_OK(tmpDataDict)
  calib.cali = calibClientMock

  calib.currentPhase = 2
  dataFile = 'muon.slcio'
  mocker.patch('%s.resolveIFpaths' % MODULE_NAME, new=Mock(return_value=S_OK([dataFile])))

  calib.currentStage = 1
  res = calib.resolveInputSlcioFilesAndAddToParameterDict(tmpListOfFiles, initialParameterDict)
  print(res)
  assert res['OK']
  assert res['Value'][calib.getKey(res['Value'], 'LCIOInputFiles')] == dataFile
  assert res['Value'][calib.getKey(res['Value'], 'SkipNEvents')] == tmpDataDict['muon'][1]
  assert res['Value'][calib.getKey(res['Value'], 'MaxRecordNumber')] == tmpDataDict['muon'][2]
  assert res['Value'][calib.getKey(res['Value'], 'PandoraSettingsXmlFile')
                      ] == 'PandoraSettingsFCCee/PandoraSettingsDefault.xml'
  assert res['Value'][calib.getKey(res['Value'], 'RootFile')] == 'pfoAnalysis.root'

  calib.currentStage = 2
  res = calib.resolveInputSlcioFilesAndAddToParameterDict(tmpListOfFiles, initialParameterDict)
  print(res)
  assert res['OK']
  assert res['Value'][calib.getKey(res['Value'], 'LCIOInputFiles')] == dataFile
  assert res['Value'][calib.getKey(res['Value'], 'PandoraSettingsXmlFile')
                      ] == 'CalibrationPandoraSettings/PandoraSettingsPhotonTraining.xml'
  assert res['Value'][calib.getKey(res['Value'], 'RootFile')] == 'dummy.root'
  #  assert False
