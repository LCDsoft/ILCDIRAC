"""
Unit tests for the CalibrationService
"""

import unittest
import pytest
import os
import shutil
import time
from datetime import datetime
from datetime import timedelta
from xml.etree import ElementTree as et
from shutil import copyfile
from DIRAC import S_OK, S_ERROR, gLogger
from mock import call, patch, MagicMock as Mock
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from ILCDIRAC.CalibrationSystem.Service.CalibrationHandler import CalibrationHandler
from ILCDIRAC.CalibrationSystem.Service.CalibrationRun import CalibrationRun
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertInImproved, \
    assertEqualsImproved, assertDiracFailsWith, assertDiracSucceeds, \
    assertDiracSucceedsWith, assertDiracSucceedsWith_equals, assertMockCalls, \
    assertDiracFails
from ILCDIRAC.CalibrationSystem.Client.DetectorSettings import createCalibrationSettings

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.CalibrationSystem.Service.CalibrationHandler'


@pytest.fixture
def readParameterDict():
  from ILCDIRAC.CalibrationSystem.Utilities.functions import readParameterDict
  import os

  fileDir = os.path.join(os.environ['DIRAC'], "ILCDIRAC", "CalibrationSystem", "Utilities", "testing")
  fileToRead = os.path.join(fileDir, 'parameterListMarlinSteeringFile.txt')
  parDict = readParameterDict(fileToRead)
  for iKey in parDict.keys():
    if 'CalibrECAL' in iKey:
      parDict[iKey] = '1.0 1.0'
    else:
      parDict[iKey] = 1.0
  return parDict


@pytest.fixture
def calibHandler():
  CalibrationHandler.initializeHandler(None)
  RequestHandler._rh__initializeClass(Mock(), Mock(), Mock(), Mock())
  calibHandler = CalibrationHandler({}, Mock())
  calibHandler.initialize()
  yield calibHandler

  if os.path.exists('status'):
    os.remove('status')
  # clean up output directory
  for iCalID in list(CalibrationHandler.activeCalibrations.keys()):
    try:
      dirToDelete = 'calib%s' % iCalID
      shutil.rmtree(dirToDelete)
    except EnvironmentError as e:
      print("Failed to delete directory: %s" % dirToDelete, str(e))
      assert False
  CalibrationHandler.activeCalibrations = {}
  CalibrationHandler.calibrationCounter = 0


@pytest.fixture
def copiedFccSteeringFile():
  calibID = 1
  workdirName = 'calib%s' % calibID
  if not os.path.exists(workdirName):
      os.makedirs(workdirName)
  src = '/cvmfs/clicdp.cern.ch/iLCSoft/builds/2019-04-17/x86_64-slc6-gcc62-opt/ClicPerformance/HEAD/fcceeConfig/fccReconstruction.xml'
  copyfile(src, '%s/fccReconstruction.xml' % workdirName)
  yield workdirName
  try:
    shutil.rmtree(workdirName)
  except EnvironmentError as e:
    print("Failed to delete directory: %s; ErrMsg: %s" % (workdirName, str(e)))
    assert False


def addPfoAnalysisProcessor(mainSteeringMarlinRecoFile):
  mainTree = et.ElementTree()
  mainTree.parse(mainSteeringMarlinRecoFile)
  mainRoot = mainTree.getroot()

  #FIXME TODO properly find path to the file
  # this file should only contains PfoAnalysis processor
  import ILCDIRAC.CalibrationSystem.Utilities as utilities
  pfoAnalysisProcessorFile = os.path.join(utilities.__path__[0], 'testing/pfoAnalysis.xml')
  if not os.path.exists(pfoAnalysisProcessorFile):
    return S_ERROR("cannot find xml file with pfoAnalysis processor")
  tmpTree = et.parse(pfoAnalysisProcessorFile)
  elementToAdd = tmpTree.getroot()

  if 'MyPfoAnalysis' not in (iEl.attrib['name'] for iEl in mainRoot.iter('processor')):
    tmp1 = mainRoot.find('execute')
    c = et.Element("processor name=\"MyPfoAnalysis\"")
    tmp1.append(c)
    mainRoot.append(elementToAdd)
    #  mainTree.write(mainSteeringMarlinRecoFile)
    root = mainTree.getroot()
    root_str = et.tostring(root)
    # TODO FIXME why write to "test_<fileName>" file???
    #  with open('test_' + mainSteeringMarlinRecoFile, "w") as of:
    with open(mainSteeringMarlinRecoFile, "w") as of:
      of.write(root_str)
  return S_OK()


def test_readInitialParameterDict(copiedFccSteeringFile, mocker):
  calibSetting = createCalibrationSettings('CLD')
  calibSetting.settingsDict['DDPandoraPFANewProcessorName'] = 'MyDDMarlinPandora_10ns'
  calibSetting.settingsDict['DDCaloDigiName'] = 'MyDDCaloDigi_10ns'
  tmpMock = Mock(name='instance')
  tmpMock.getFile.return_value = S_OK()
  mocker.patch('ILCDIRAC.CalibrationSystem.Service.CalibrationRun.DataManager',
               new=Mock(return_value=tmpMock, name='Class'))
  calibID = int(copiedFccSteeringFile.split('calib')[-1])
  newRun = CalibrationRun(calibID, {'dummy': ['dummy_inputFiles1', 'dummy_inputFiles2']}, calibSetting.settingsDict)
  res = newRun.readInitialParameterDict()
  assert res['OK']

def test_initializeHandler(mocker):
  mocker.patch('%s.glob.glob' % MODULE_NAME, new=Mock(return_value=['calib2', 'calib4', 'calib78']))
  mocker.patch('%s.loadCalibrationRun' % MODULE_NAME, new=Mock())
  mocker.patch.object(CalibrationHandler, 'loadStatus', new=Mock(return_value=0))

  res = CalibrationHandler.initializeHandler(None)
  assert not res['OK']

  mocker.patch.object(CalibrationHandler, 'loadStatus', new=Mock(return_value=78))
  res = CalibrationHandler.initializeHandler(None)
  assert res['OK']

def mimic_convert_and_execute(inList, _=''):
  from ILCDIRAC.CalibrationSystem.Client.CalibrationClient import CalibrationPhase
  if True in ['ECal_Digi_Extract.py' in str(iEl) for iEl in inList] and True in ['Mean' in str(iEl) for iEl in inList]:
    return S_OK((0, '%s\n' % CalibrationPhase.sampleEnergyFromPhase(CalibrationPhase.ECalDigi)))
  elif True in ['HCal_Digi_Extract.py' in str(iEl) for iEl in inList] and True in ['Mean' in str(iEl) for iEl in inList]:
    return S_OK((0, '%s\n' % CalibrationPhase.sampleEnergyFromPhase(CalibrationPhase.HCalDigi)))
  elif True in ['EM_Extract.py' in str(iEl) for iEl in inList] and True in ['Mean' in str(iEl) for iEl in inList]:
    return S_OK((0, '%s\n' % CalibrationPhase.sampleEnergyFromPhase(CalibrationPhase.ElectroMagEnergy)))
  elif True in ['Had_Extract.py' in str(iEl) for iEl in inList] and True in ['FOM' in str(iEl) for iEl in inList]:
    return S_OK((0, '%s\n' % CalibrationPhase.sampleEnergyFromPhase(CalibrationPhase.HadronicEnergy)))
  elif _ != '':
    return 6.66
  else:
    return S_OK((0, '6.66\n'))


def test_endCurrentStepBasicWorkflow(copiedFccSteeringFile, readParameterDict, mocker):
  opsMock = Mock(name='instance')
  opsMock.getValue.return_value = 'dummy'
  mocker.patch('ILCDIRAC.CalibrationSystem.Service.CalibrationRun.Operations',
               new=Mock(return_value=opsMock, name='Class'))
  #  mocker.patch('ILCDIRAC.CalibrationSystem.Service.CalibrationRun.updateSteeringFile', new=Mock(return_value=S_OK()))
  mocker.patch('ILCDIRAC.CalibrationSystem.Service.CalibrationRun.convert_and_execute',
               side_effect=mimic_convert_and_execute)
  mocker.patch('ILCDIRAC.CalibrationSystem.Service.CalibrationRun.saveCalibrationRun', new=Mock(return_value=S_OK()))

  calibSetting = createCalibrationSettings('CLD')
  calibSetting.settingsDict['DDPandoraPFANewProcessorName'] = 'MyDDMarlinPandora_10ns'
  calibSetting.settingsDict['DDCaloDigiName'] = 'MyDDCaloDigi_10ns'
  tmpMock = Mock(name='instance')
  tmpMock.getFile.return_value = S_OK()
  mocker.patch('ILCDIRAC.CalibrationSystem.Service.CalibrationRun.DataManager',
               new=Mock(return_value=tmpMock, name='Class'))
  steeringFileName = copiedFccSteeringFile
  calibID = int(steeringFileName.split('calib')[-1])
  addPfoAnalysisProcessor('%s/%s' % (steeringFileName, calibSetting.settingsDict['steeringFile']))
  newRun = CalibrationRun(calibID, {'dummy': ['dummy_inputFiles1', 'dummy_inputFiles2']}, calibSetting.settingsDict)
  res = newRun.readInitialParameterDict()
  newRun.calibrationConstantsDict[".//processor[@type='PfoAnalysis']/parameter[@name='RootFile']"] = 'pfoAnalysis.root'

  mocker.patch.object(CalibrationRun, '_CalibrationRun__mergePandoraLikelihoodXmlFiles',
                      new=Mock(return_value={'OK': True}))
  stageIDSequence = []
  phaseIDSequence = []
  stepIDSequence = []
  calibFinishedSequence = []
  for _ in range(0, 13):
    stepOutcome = 'stage: %s, phase: %s, step: %s,\tcalibFinished: %s' % (
        newRun.currentStage, newRun.currentPhase, newRun.currentStep, newRun.calibrationFinished)
    print(stepOutcome)
    stageIDSequence.append(newRun.currentStage)
    phaseIDSequence.append(newRun.currentPhase)
    stepIDSequence.append(newRun.currentStep)
    calibFinishedSequence.append(newRun.calibrationFinished)
    newRun.endCurrentStep()
  assert stageIDSequence == [1, 1, 1, 1, 1, 2, 3, 3, 3, 3, 3, 3, 3]
  assert phaseIDSequence == [0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 4, 4]
  assert stepIDSequence == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 11]
  assert calibFinishedSequence == [False, False, False, False,
                                   False, False, False, False, False, False, False, True, True]


def test_export_checkForStepIncrement(calibHandler, mocker):
  calibSetting = createCalibrationSettings('CLIC')
  calibRun = CalibrationRun(27, {'dummy': ['dummy_inputFiles1', 'dummy_inputFiles2']}, calibSetting.settingsDict)
  calibRun.calibrationFinished = True
  calibRun.calibrationEndTime = datetime.now() - timedelta(minutes=9, seconds=59, milliseconds=10)
  CalibrationHandler.activeCalibrations[27] = calibRun

  mocker.patch.object(calibRun, 'copyResultsToEos', new=Mock(return_value=S_OK()))
  mocker.patch('%s.shutil' % MODULE_NAME, new=Mock())

  assert len(CalibrationHandler.activeCalibrations) == 1
  res = calibHandler.export_checkForStepIncrement()
  assert res['OK']
  assert len(CalibrationHandler.activeCalibrations) == 1
  time.sleep(1)
  res = calibHandler.export_checkForStepIncrement()
  assert res['OK']
  assert len(CalibrationHandler.activeCalibrations) == 0


def test_finalInterimResultReceived(calibHandler, mocker):
  calibSetting = createCalibrationSettings('CLIC')
  calibSetting.settingsDict['numberOfJobs'] = 200
  calibRun = CalibrationRun(1, {'dummy': ['dummy_inputFiles1', 'dummy_inputFiles2']}, calibSetting.settingsDict)
  nJobsTmp = int(calibSetting.settingsDict['numberOfJobs'] *
                 calibSetting.settingsDict['fractionOfFinishedJobsNeededToStartNextStep'] - 1)
  for i in range(0, nJobsTmp):
    calibRun.stepResults[calibRun.currentStep].addResult(i, 'dummy')
  assert not calibHandler.finalInterimResultReceived(calibRun, calibRun.currentStep)
  calibRun.stepResults[calibRun.currentStep].addResult(nJobsTmp, 'dummy')
  assert calibHandler.finalInterimResultReceived(calibRun, calibRun.currentStep)

def test_regroupInputFile(calibHandler, mocker):
  inputFileDir = {'muon': ['muon1', 'muon2', 'muon3', 'muon4', 'muon5'], 'kaon': ['kaon1', 'kaon2', 'kaon3', 'kaon4', 'kaon5'], 'gamma': [
      'gamma1', 'gamma2', 'gamma3', 'gamma4', 'gamma5'], 'zuds': ['zuds1', 'zuds2', 'zuds3', 'zuds4', 'zuds5']}

  numberOfJobs = 4
  res = calibHandler._CalibrationHandler__regroupInputFile(inputFileDir, numberOfJobs)
  assert res['OK']
  groupedDict = res['Value']
  for iKey in inputFileDir.keys():
    assert len(groupedDict[0][iKey]) == 2
    assert len(groupedDict[1][iKey]) == 1

  numberOfJobs = 2
  res = calibHandler._CalibrationHandler__regroupInputFile(inputFileDir, numberOfJobs)
  assert res['OK']
  groupedDict = res['Value']
  for iKey in inputFileDir.keys():
    assert len(groupedDict[0][iKey]) == 3
    assert len(groupedDict[1][iKey]) == 2

def test_export_submitResult(calibHandler, mocker):
  mocker.patch.object(CalibrationRun, 'submitJobs', new=Mock())
  mocker.patch.object(calibHandler, '_CalibrationHandler__regroupInputFile',
                      new=Mock(return_value={'OK': True, 'Value': []}))
  mocker.patch.object(calibHandler, '_getUsernameAndGroup', new=Mock(
      return_value={'OK': True, 'Value': {'username': 'oviazlo', 'group': 'ilc_users'}}))

  calibSettings = createCalibrationSettings('CLIC')
  res = calibHandler.export_createCalibration(
      {'muon': [], 'kaon': [], 'gamma': [], 'zuds': []}, calibSettings.settingsDict)
  if not res['OK']:
    print('Error message:\t%s' % res['Message'])
    assert False

  print('CalibrationHandler.activeCalibrations.keys(): %s' % CalibrationHandler.activeCalibrations.keys())

  import ILCDIRAC.CalibrationSystem.Utilities as utilities
  fileDir = utilities.__path__[0]
  fileToRead = os.path.join(fileDir, 'testing/pfoAnalysis.xml')
  from ILCDIRAC.CalibrationSystem.Utilities.fileutils import binaryFileToString
  tmpFile = binaryFileToString(fileToRead)

  calibID = 1
  stageID = 2
  phaseID = 0
  stepID = 0
  workerID = 8234

  res = calibHandler.export_submitResult(calibID, stageID, phaseID, stepID, workerID, tmpFile)
  if not res['OK']:
    print res
    assert False
  assert res['OK'] == True

  outFile = CalibrationHandler.activeCalibrations[calibID].stepResults[stepID].results[workerID]
  assert os.path.exists(outFile)
  print(outFile)

  import filecmp
  assert filecmp.cmp(fileToRead, outFile)

def test_mergePandoraLikelihoodXmlFiles(calibHandler, mocker):
  import ILCDIRAC.CalibrationSystem.Utilities as utilities
  fileDir = utilities.__path__[0]

  mocker.patch.object(CalibrationRun, 'submitJobs', new=Mock())
  opsMock = Mock(name='instance')
  opsMock.getValue.return_value = os.path.join(fileDir, 'testing')
  mocker.patch('ILCDIRAC.CalibrationSystem.Service.CalibrationRun.Operations',
               new=Mock(return_value=opsMock, name='Class'))
  mocker.patch.object(calibHandler, '_CalibrationHandler__regroupInputFile',
                      new=Mock(return_value={'OK': True, 'Value': []}))
  mocker.patch.object(calibHandler, '_getUsernameAndGroup', new=Mock(
      return_value={'OK': True, 'Value': {'username': 'oviazlo', 'group': 'ilc_users'}}))

  calibSettings = createCalibrationSettings('CLIC')
  print('calibSettings.settingsDict: %s' % calibSettings.settingsDict)

  res = calibHandler.export_createCalibration(
      {'muon': [], 'kaon': [], 'gamma': [], 'zuds': []}, calibSettings.settingsDict)
  print('calibHandler.export_createCalibration: %s' % res)
  if not res['OK']:
    print(res['Message'])
    assert False

  fileToRead = os.path.join(fileDir, 'testing/PandoraLikelihoodData9EBin.xml')
  if not os.path.exists(fileToRead):
    assert False
  from ILCDIRAC.CalibrationSystem.Utilities.fileutils import binaryFileToString
  tmpFile = binaryFileToString(fileToRead)

  calibID = 1
  stageID = 2
  phaseID = 0
  stepID = 0
  workerID = 654

  CalibrationHandler.activeCalibrations[calibID].currentStage = stageID
  CalibrationHandler.activeCalibrations[calibID].currentPhase = phaseID
  CalibrationHandler.activeCalibrations[calibID].currentStep = stepID

  res = calibHandler.export_submitResult(calibID, stageID, phaseID, stepID, workerID, tmpFile)
  if not res['OK']:
    print res
    assert False

  nFilesToMerge = 3
  for _ in range(0, nFilesToMerge - 1):
    workerID += 1
    res = calibHandler.export_submitResult(calibID, stageID, phaseID, stepID, workerID, tmpFile)
    if not res['OK']:
      print(res)
      assert False

  res = CalibrationHandler.activeCalibrations[calibID]._CalibrationRun__mergePandoraLikelihoodXmlFiles()
  if not res['OK']:
    print(res)
    assert False

  mergedFile = 'calib%s/newPandoraLikelihoodData.xml' % calibID
  assert os.path.exists(mergedFile)

  from ILCDIRAC.CalibrationSystem.Utilities.functions import searchFilesWithPattern
  inFileList = searchFilesWithPattern('calib%s/stage%s' % (calibID, stageID), '*.xml')

  import re
  diffLines = None
  with open(mergedFile) as file1:
    with open(inFileList[0]) as file2:
      diffLines = set(file1).symmetric_difference(file2)
  diffLines = list(diffLines)
  diffLines = [re.split('\>|\<', iLine) for iLine in diffLines]

  # since we merge a few copies of the same file, likelihood functions has to be identical in input and output files
  # the only difference has to be in the number of events in NSignalEvents and NBackgroundEvents fields (lines)
  # this is why there should be 2 unique lines in input and output files --> 4 lines in total
  assert len(diffLines) == 4

  # sum numbers from NSignalEvents and NBackgroundEvents nodes for input and output files
  nSignalEvents = []
  nBackgroundEvents = []
  for iList in diffLines:
    if iList[1] == 'NSignalEvents':
      nSignalEvents.append(sum([int(iEl) for iEl in iList[2].split()]))
    elif iList[1] == 'NBackgroundEvents':
      nBackgroundEvents.append(sum([int(iEl) for iEl in iList[2].split()]))
    else:
      pass

  # NSignalEvents_output = nFilesToMerge * NSignalEvents_input
  # NBackgroundEvents_output = nFilesToMerge * NBackgroundEvents_input
  assert ((nSignalEvents[0] == nFilesToMerge * nSignalEvents[1])
          or (nSignalEvents[1] == nFilesToMerge * nSignalEvents[0]))
  assert ((nBackgroundEvents[0] == nFilesToMerge * nBackgroundEvents[1])
          or (nBackgroundEvents[1] == nFilesToMerge * nBackgroundEvents[0]))

  #  print('nSignalEvents: %s' % nSignalEvents)
  #  print('nBackgroundEvents: %s' % nBackgroundEvents)

# TODO this function has decorator... which one need to mock
#  def test_submitJobs(calibHandler, mocker):
#    calibSetting = createCalibrationSettings('CLIC')
#    calibSetting.settingsDict['numberOfJobs'] = 4
#
#    inputFileDir = {'muon': ['muon1', 'muon2', 'muon3', 'muon4', 'muon5'], 'kaon': ['kaon1', 'kaon2', 'kaon3', 'kaon4', 'kaon5'], 'gamma': ['gamma1', 'gamma2', 'gamma3', 'gamma4', 'gamma5'], 'zuds': ['zuds1', 'zuds2', 'zuds3', 'zuds4', 'zuds5']}
#    res = calibHandler._CalibrationHandler__regroupInputFile(inputFileDir, calibSetting.settingsDict['numberOfJobs'])
#    groupedDict = res['Value']
#
#    mocker.patch('ILCDIRAC.CalibrationSystem.Service.CalibrationRun.saveCalibrationRun', new=Mock(return_value=S_OK()))
#    mocker.patch('ILCDIRAC.CalibrationSystem.Service.CalibrationRun.DiracILC', new=Mock(return_value=S_OK()))
#    testRun  = CalibrationRun(1, groupedDict, calibSetting.settingsDict)
#
#    userJobMock = Mock(name='instance')
#    userJobMock.submit.return_value = 'dummy'
#    userJobMock.append.return_value = {'OK': True}
#
#    res = testRun.submitJobs([])
#    assert res == []
#    assert False

#pylint: disable=protected-access,too-many-public-methods,,no-member


class CalibrationHandlerTest(unittest.TestCase):
  """ Tests the implementation of the methods of the CalibrationService classes """

  @classmethod
  def setUpClass(cls):
    CalibrationHandler.initializeHandler(None)

  def setUp(self):
    """ Create a CalibrationHandler instance so we can check some basic functionality. """
    self.transport_mock = Mock()
    RequestHandler._rh__initializeClass(Mock(), Mock(), Mock(), Mock())
    self.calh = CalibrationHandler({}, self.transport_mock)
    self.calh.initialize()
    # TODO mock this call:
    #  self.ops.getValue("/AvailableTarBalls/%s/%s/%s/pandoraAnalysisHeadBin" % (self.platform,
    #                                 'pandora_calibration_scripts', self.appversion), None)

  def tearDown(self):
    CalibrationHandler.activeCalibrations = {}
    CalibrationHandler.calibrationCounter = 0

# FIXME: Change tests to reflect new way of starting calibration creation

  #
  #  def test_submitresult_old_stepid( self ):
  #    with patch.object( CalibrationRun, 'submitJobs', new=Mock()):
  #      for _ in xrange( 0, 50 ): #creates Calibrations with IDs 1-50
  #        self.calh.export_createCalibration( '', '', [], 0, '', '' )
  #    CalibrationHandler.activeCalibrations[ 27 ].currentStep = 13
  #    assertDiracSucceeds( self.calh.export_submitResult( 27, 12, 9841, [ 5, 6, 2, 1, 7 ] ), self )
  #    for i in xrange( 0, 30 ):
  #      assertEqualsImproved( CalibrationHandler.activeCalibrations[ 27 ].stepResults[ i ].getNumberOfResults(),
  #                            0, self )
  #
  #  def test_submitresult_wrong_calibrationID( self ):
  #    with patch.object( CalibrationRun, 'submitJobs', new=Mock()):
  #      for _ in xrange( 0, 50 ): #creates Calibrations with IDs 1-50
  #        self.calh.export_createCalibration( '', '', [], 0, '', '' )
  #    res = self.calh.export_submitResult( 54, 1, 9841, [ 5, 6, 2, 1, 7 ] )
  #    assertDiracFailsWith( res, 'Calibration with id 54 not found', self )
  #    for i in xrange( 0, 50 ):
  #      assertEqualsImproved( CalibrationHandler.activeCalibrations[ 27 ].stepResults[ i ].getNumberOfResults(),
  #                            0, self )

  #  def test_createcalibration( self ):
  #    CalibrationHandler.calibrationCounter = 834 - 1 # newly created Calibration gets ID 834
  #    job_mock = Mock()
  #    with patch.object( CalibrationRun, 'submitJobs', new=job_mock ):
  #      result = self.calh.export_createCalibration( 'steeringfile', 'version', [ 'inputfile1', 'inputfile2' ],
  #                                                   12, '', '' )
  #    assertDiracSucceedsWith_equals( result, ( 834, job_mock() ), self )
  #    testRun = CalibrationHandler.activeCalibrations[ 834 ]
  #    assertEqualsImproved(
  #      ( testRun.steeringFile, testRun.softwareVersion, testRun.inputFiles, testRun.numberOfJobs ),
  #      ( 'steeringfile', 'version', [ 'inputfile1', 'inputfile2' ], 12 ), self )
  #    assertEqualsImproved( CalibrationHandler.calibrationCounter, 834, self ) # next calibration gets ID 835
  #
  #  def test_resubmitjobs( self ):
  #    calIDsWorkIDs = [ ( 138, 1249 ), ( 123, 1357 ), ( 498626, 4368 ) ]
  #    CalibrationHandler.activeCalibrations[ 138 ] = CalibrationRun(1, '', '', [], 0 )
  #    CalibrationHandler.activeCalibrations[ 123 ] = CalibrationRun(2, '', '', [], 0 )
  #    CalibrationHandler.activeCalibrations[ 498626 ] = CalibrationRun(3, '', '', [], 0 )
  #    with patch.object( CalibrationRun, 'resubmitJob', new=Mock()) as resubmit_mock:
  #      assertDiracSucceeds( self.calh.export_resubmitJobs( calIDsWorkIDs ), self )
  #      assertEqualsImproved( resubmit_mock.mock_calls, [ call( 1249, proxyUserGroup = '', proxyUserName = '' ),
  #                                                        call( 1357, proxyUserGroup = '', proxyUserName = '' ),
  #                                                        call( 4368, proxyUserGroup = '', proxyUserName = '' ) ],
  #                            self )
  #
  #  def test_resubmitjobs_fails( self ):
  #    calIDsWorkIDs = [ ( 138, 1249 ), ( 198735, 1357 ), ( 498626, 4368 ) ]
  #    CalibrationHandler.activeCalibrations[ 138 ] = CalibrationRun( '', '', [], 0 )
  #    CalibrationHandler.activeCalibrations[ 123 ] = CalibrationRun( '', '', [], 0 )
  #    CalibrationHandler.activeCalibrations[ 498626 ] = CalibrationRun( '', '', [], 0 )
  #    with patch.object( CalibrationRun, 'resubmitJob', new=Mock() ):
  #      res = self.calh.export_resubmitJobs( calIDsWorkIDs )
  #      assertDiracFails( res, self )
  #      assertInImproved( 'Could not resubmit all jobs', res[ 'Message' ], self )
  #      assertInImproved( '[(198735, 1357)]', res[ 'Message' ], self )
  #      assertEqualsImproved( [ ( 198735, 1357 ) ], res[ 'failed_pairs' ], self )

  def test_getnumberofjobs(self):
    with patch.object(CalibrationRun, 'submitJobs', new=Mock()):
      with patch.object(self.calh, '_CalibrationHandler__regroupInputFile', new=Mock(return_value={'OK': True, 'Value': []})):
        with patch.object(self.calh, '_getUsernameAndGroup',
                          new=Mock(return_value={'OK': True, 'Value': {'username': 'oviazlo', 'group': 'ilc_users'}})):
          tmpDict = {782145: 815, 72453: 421, 189455: 100, 954692: 0, 29485: 1040}
          for iCalID in tmpDict.keys():  # creates Calibrations with IDs 1-50
            calibSettings = createCalibrationSettings('CLIC')
            calibSettings.settingsDict['numberOfJobs'] = tmpDict[iCalID]
            iCalibRun = CalibrationRun(iCalID, {'muon': [], 'kaon': [], 'gamma': [],
                                                'zuds': []}, calibSettings.settingsDict)
            CalibrationHandler.activeCalibrations[iCalID] = iCalibRun
          result = self.calh.export_getNumberOfJobsPerCalibration()
          assertDiracSucceeds(result, self)
          assertEqualsImproved(result['Value'], tmpDict, self)

  #  def test_getnewparams_calculationfinished( self ):
  #    # TODO rewrite this test
  #    calibSetting = createCalibrationSettings('CLIC')
  #    testRun  = CalibrationRun(1, {'dummy': ['dummy_inputFiles1', 'dummy_inputFiles2']}, calibSetting.settingsDict)
  #    testRun.calibrationFinished = True
  #    CalibrationHandler.activeCalibrations[ 2489 ] = testRun
  #    assertDiracSucceedsWith( self.calh.export_getNewParameters( 2489, 193 ),
  #                             'Calibration finished! End job now', self )

  #  def test_getnewparams_nonewparamsyet( self ):
  #    calibSetting = createCalibrationSettings('CLIC')
  #    testRun  = CalibrationRun(1, {'dummy': ['dummy_inputFiles1', 'dummy_inputFiles2']}, calibSetting.settingsDict)
  #    testRun.currentStep = 149
  #    CalibrationHandler.activeCalibrations[ 2489 ] = testRun
  #    assertDiracSucceedsWith_equals(self.calh.export_getNewParameters( 2489, 149 ),
  #                                   None, self)

  def test_getnewparams_newparams(self):
    calibSetting = createCalibrationSettings('CLIC')
    testRun = CalibrationRun(1, {'dummy': ['dummy_inputFiles1', 'dummy_inputFiles2']}, calibSetting.settingsDict)
    testRun.currentStep = 36
    testRun.currentParameterSet = {'dummy': 2435}
    CalibrationHandler.activeCalibrations[2489] = testRun
    assertDiracSucceedsWith_equals(self.calh.export_getNewParameters(2489, 35),
                                   testRun.currentParameterSet, self)

  def test_getnewparams_inactive_calibration(self):
    with patch.object(CalibrationRun, 'submitJobs', new=Mock()):
      with patch.object(self.calh, '_CalibrationHandler__regroupInputFile', new=Mock(return_value={'OK': True, 'Value': []})):
        with patch.object(self.calh, '_getUsernameAndGroup',
                          new=Mock(return_value={'OK': True, 'Value': {'username': 'oviazlo', 'group': 'ilc_users'}})):
          for _ in xrange(0, 50):  # creates Calibrations with IDs 1-50
            calibSettings = createCalibrationSettings('CLIC')
            res = self.calh.export_createCalibration(
                {'muon': [], 'kaon': [], 'gamma': [], 'zuds': []}, calibSettings.settingsDict)
            if not res['OK']:
              print(res['Message'])
              assert False
          assertDiracFailsWith(self.calh.export_getNewParameters(135, 913),
                               'CalibrationID is not in active calibrations: 135', self)

  #  @patch('DIRAC.ConfigurationSystem.Client.Helpers.Operations')
  #  def test_endcurrentstep( self, opsMock ):
  #    from ILCDIRAC.CalibrationSystem.Service.CalibrationHandler import CalibrationRun
  #    #  opsMock = Mock(name='OpsMock')
  #    opsMock.getValue.return_value = 'dummy'
  #    #  with patch('ILCDIRAC.CalibrationSystem.Service.CalibrationHandler.Operations', new=Mock(return_value=opsMock)):
  #    newRun = CalibrationRun(1, 'dummy_steeringFile', 'dummy_ilcsoftPath', ['dummy_inputFiles1', 'dummy_inputFiles2'], 1)
  #    newRun.dumpSelfArguments()
  #    newRun.endCurrentStep()
  #    newRun.dumpSelfArguments()
  #    self.assertTrue( True, 'dummy' )


  #  def test_calibrun_init_mock( self ):
  #    instanceMock = Mock(name='instanceMock')
  #    instanceMock.getValue.return_value = 'dummy'
  #    from ILCDIRAC.CalibrationSystem.Service.CalibrationHandler import CalibrationRun
  #    with patch('ILCDIRAC.CalibrationSystem.Service.CalibrationHandler.Operations', new=Mock(return_value=instanceMock)):
  #      newRun = CalibrationRun(1, 'dummy_steeringFile', 'dummy_ilcsoftPath', ['dummy_inputFiles1', 'dummy_inputFiles2'], 1)
  #      assert newRun.ops.getValue() == 'dummy'
  #
  #  @patch('ILCDIRAC.CalibrationSystem.Service.CalibrationHandler.Operations', name='OpsMock')
  #  def test_calibrun_init_mock_v2( self, classMock ):
  #    classMock.return_value=Mock(name='instanceMock')
  #    from ILCDIRAC.CalibrationSystem.Service.CalibrationHandler import CalibrationRun
  #    classMock.return_value.getValue.return_value = 'dummy'
  #    newRun = CalibrationRun(1, 'dummy_steeringFile', 'dummy_ilcsoftPath', ['dummy_inputFiles1', 'dummy_inputFiles2'], 1)
  #    assert newRun.ops.getValue() == 'dummy'


  #  @patch('DIRAC.ConfigurationSystem.Client.Helpers.Operations.getValue', return_value='dummyReturnString')
  #  def test_endcurrentstep( self, mock_operations ):
  #    from ILCDIRAC.CalibrationSystem.Service.CalibrationHandler import CalibrationResult
  #    with patch.object( CalibrationRun, 'submitJobs', new=Mock()):
  #      self.calh.export_createCalibration('', '', [], 0, '', '' )
  #    self.calh.activeCalibrations[ 1 ].currentStep = 15
  #    result1 = [ 1, 2.3, 5 ]
  #    result2 = [ 0, 0.2, -0.5 ]
  #    result3 = [ -10, -5.4, 2 ]
  #    res = CalibrationResult()
  #    res.addResult( 2384, result1 )
  #    res.addResult( 742, result2 )
  #    res.addResult( 9354, result3 )
  #    self.calh.activeCalibrations[ 1 ].stepResults[ 15 ] = res
  #    self.calh.activeCalibrations[ 1 ].endCurrentStep()
  #    self.assertTrue( self.calh.activeCalibrations[ 1 ].calibrationFinished, 'Expecting calibration to be finished' )
  #
  #  @patch('DIRAC.ConfigurationSystem.Client.Helpers.Operations.getValue', return_value='dummyReturnString')
  #  def test_endcurrentstep_not_finished( self ):
  #    from ILCDIRAC.CalibrationSystem.Service.CalibrationHandler import CalibrationResult
  #    with patch.object( CalibrationRun, 'submitJobs', new=Mock()):
  #      self.calh.export_createCalibration('', '', [], 0, '', '' )
  #    self.calh.activeCalibrations[ 1 ].currentStep = 14
  #    result1 = [ 1, 2.3, 5 ]
  #    result2 = [ 0, 0.2, -0.5 ]
  #    result3 = [ -10, -5.4, 2 ]
  #    res = CalibrationResult()
  #    res.addResult( 2384, result1 )
  #    res.addResult( 742, result2 )
  #    res.addResult( 9354, result3 )
  #    self.calh.activeCalibrations[ 1 ].stepResults[ 14 ] = res
  #    self.calh.activeCalibrations[ 1 ].endCurrentStep()
  #    self.assertFalse( self.calh.activeCalibrations[ 1 ].calibrationFinished,
  #                      'Expecting calibration to be finished' )

  def test_addlists_work( self ):
    # Simple case
    test_list_1 = [1, 148]
    test_list_2 = [-3, 0.2]
    calibSetting = createCalibrationSettings('CLIC')
    testobj = CalibrationRun(1, {'dummy': ['dummy_inputFiles1', 'dummy_inputFiles2']}, calibSetting.settingsDict)
    res = testobj._CalibrationRun__addLists(test_list_1, test_list_2)
    assertEqualsImproved([-2, 148.2], res, self)

  def test_addlists_work_2(self):
    # More complex case
    test_list_1 = [9013, -137.25, 90134, 4278, -123, 'abc', ['a', False]]
    test_list_2 = [0, 93, -213, 134, 98245, 'aifjg', ['some_entry', {}]]
    calibSetting = createCalibrationSettings('CLIC')
    testobj = CalibrationRun(1, {'dummy': ['dummy_inputFiles1', 'dummy_inputFiles2']}, calibSetting.settingsDict)
    res = testobj._CalibrationRun__addLists(test_list_1, test_list_2)
    assertEqualsImproved([9013, -44.25, 89921, 4412, 98122, 'abcaifjg',
                          ['a', False, 'some_entry', {}]], res, self)

  def test_addlists_empty(self):
    test_list_1 = []
    test_list_2 = []
    calibSetting = createCalibrationSettings('CLIC')
    testobj = CalibrationRun(1, {'dummy': ['dummy_inputFiles1', 'dummy_inputFiles2']}, calibSetting.settingsDict)
    res = testobj._CalibrationRun__addLists(test_list_1, test_list_2)
    assertEqualsImproved([], res, self)

  def test_addlists_incompatible(self):
    test_list_1 = [1, 83, 0.2, -123]
    test_list_2 = [1389, False, '']
    calibSetting = createCalibrationSettings('CLIC')
    testobj = CalibrationRun(1, {'dummy': ['dummy_inputFiles1', 'dummy_inputFiles2']}, calibSetting.settingsDict)
    with pytest.raises(ValueError) as ve:
      testobj._CalibrationRun__addLists(test_list_1, test_list_2)
    assertInImproved('the two lists do not have the same number of elements', ve.__str__().lower(), self)

  def atest_resubmitJob(self):
    pass  # FIXME: Finish atest once corresponding method is written
