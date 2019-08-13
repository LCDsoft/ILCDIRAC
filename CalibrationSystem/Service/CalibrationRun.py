"""CalibrationRun class."""

import glob
import os
import threading
from collections import defaultdict
from datetime import datetime
import shutil

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.Proxy import executeWithUserProxy
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
import ILCDIRAC.CalibrationSystem.Utilities as utilities
from ILCDIRAC.CalibrationSystem.Utilities.fileutils import binaryFileToString
from ILCDIRAC.CalibrationSystem.Utilities.functions import convert_and_execute
from ILCDIRAC.CalibrationSystem.Utilities.functions import searchFilesWithPattern
from ILCDIRAC.CalibrationSystem.Utilities.functions import readParameterDict
from ILCDIRAC.CalibrationSystem.Utilities.functions import readParametersFromSteeringFile
from ILCDIRAC.CalibrationSystem.Utilities.functions import updateSteeringFile
from ILCDIRAC.CalibrationSystem.Utilities.functions import saveCalibrationRun
from ILCDIRAC.CalibrationSystem.Utilities.functions import addParameterToProcessor
from ILCDIRAC.CalibrationSystem.Utilities.mergePandoraLikelihoodData import mergeLikelihoods
from ILCDIRAC.CalibrationSystem.Client.CalibrationClient import CalibrationPhase
from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob
from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
from ILCDIRAC.Interfaces.API.NewInterface.Applications.Calibration import Calibration

# TODO do we need it here (since there is one in calibrationHandler file). What is this for?
__RCSID__ = "$Id$"
LOG = gLogger.getSubLogger(__name__)


# pylint: disable=no-self-use
class CalibrationResult(object):
  """Wrapper class to store information about calibration computation interim results.

  Stores results from all worker nodes from a single step.
  """

  def __init__(self):
    """Initialize."""
    self.results = dict()

  def addResult(self, workerID, result):
    """Add a result from a given worker to the object.

    :param int workerID: ID of the worker providing the result
    :param result: list of floats representing the returned histogram
    :type result: `python:list`
    :returns: None
    """
    self.results[workerID] = result

  def getNumberOfResults(self):
    """Return number of interim results stored in this wrapper.

    :returns: Number of histograms stored in this object
    :rtype: int
    """
    return len(self.results)


class CalibrationRun(object):
  """Object that stores information about a single run of the calibration software.

  Includes files, current parameter set, software version, the workers running as well as
  the results of each step.
  """

  def __init__(self, calibrationID, inputFiles, calibSettingsDict):
    """Initialize."""
    self.lock = threading.Lock()
    self.calibrationID = calibrationID
    self.settings = calibSettingsDict
    self.inputFiles = inputFiles
    self.log = LOG.getSubLogger('[%s]' % calibrationID)
    if self.settings['steeringFile'].lower().startswith('lfn:'):
      self.settings['steeringFile'] = self.settings['steeringFile'].split(':')[1]
    self.localSteeringFile = os.path.join("calib%s/" % self.calibrationID,
                                          os.path.basename(self.settings['steeringFile']))
    self.stepResults = defaultdict(CalibrationResult)
    self.currentStage = self.settings['startStage']
    self.currentPhase = self.settings['startPhase']
    self.currentStep = 0
    self.currentParameterSet = defaultdict()
    # TODO temporary field in the settings. for testing only
    self.calibrationFinished = self.settings['startCalibrationFinished']
    #  self.calibrationFinished = False
    self.resultsSuccessfullyCopiedToEos = False
    self.calibrationStartTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    self.calibrationEndTime = None
    self.newPhotonLikelihood = None
    self.ops = Operations()
    self.calibrationConstantsDict = None
    self.proxyUserName = ''
    self.proxyUserGroup = ''
    # DEBUG properties:
    self.stopStage = self.settings['stopStage']
    self.stopPhase = self.settings['stopPhase']
    self.nFailedJobs = 0
    self.calibrationRunStatus = 'Starting'

    # self.workerJobs = [] ##FIXME: Disabled because not used? Maybe in submit initial jobs
    # self.activeWorkers = dict() ## dict between calibration and worker node? ##FIXME:Disabled because not used?
    # FIXME: Probably need to store a mapping workerID -> part of calibration that worker is working on. This then needs
    # to be accessed by the agent in the case of resubmission

  def __getstate__(self):
    """Copy the object's state from self.__dict__ which contains all instance attributes.

    This implementatin is required to dump instance of calibrationRun to local file.
    """
    state = self.__dict__.copy()
    # Remove the unpicklable entries.
    state.pop('log', None)
    state.pop('ops', None)
    state.pop('lock', None)
    return state

  def __setstate__(self, state):
    """Restore the object's state from self.__dict__ which contains all instance attributes.

    This implementatin is required to dump instance of calibrationRun to local file.
    """
    self.__dict__.update(state)
    # Restore unpicklable entries
    self.ops = Operations()
    self.lock = threading.Lock()
    self.log = LOG.getSubLogger('[%s]' % self.calibrationID)

  def getCurrentStatus(self):
    """Return dict with calibration current status."""
    outDict = {}
    outDict['calibrationRunStatus'] = self.calibrationRunStatus
    outDict['calibrationID'] = self.calibrationID
    outDict['currentStage'] = self.currentStage
    outDict['currentPhase'] = self.currentPhase
    outDict['currentStep'] = self.currentStep
    outDict['calibrationStartTime'] = self.calibrationStartTime
    outDict['calibrationFinished'] = self.calibrationFinished
    outDict['resultsSuccessfullyCopiedToEos'] = self.resultsSuccessfullyCopiedToEos
    if self.calibrationEndTime is not None:
      outDict['calibrationEndTime'] = self.calibrationEndTime.strftime("%Y-%m-%d %H:%M:%S")
    return outDict

  def checkForDebugFlagsToStopCalibration(self):
    """Check if calibration reached stopPhase and stopStage values."""
    if (self.stopStage <= self.currentParameterSet['currentStage']
            and self.stopPhase <= self.currentParameterSet['currentPhase']):
      return True
    else:
      return False

  def readInitialParameterDict(self):
    """Read initial parameters for calibration."""
    self.log.info('running readInitialParameterDict')

    dataMan = DataManager()
    res = dataMan.getFile(self.settings['steeringFile'], destinationDir='calib%s/' % self.calibrationID)

    if not res['OK'] or not os.path.exists(self.localSteeringFile):
      errMsg = 'Cannot copy Marlin steering file. res: %s' % res
      self.log.error(errMsg)
      return S_ERROR(errMsg)

    try:
      shutil.copyfile(self.localSteeringFile, "%s_INPUT" % self.localSteeringFile)
    except IOError:
      self.log.error("Cannot make a copy of input steering file: %s_INPUT. This doesn't affect operation." %
                     self.localSteeringFile)

    # FIXME this path will be different in production version probably... update it
    parListFileName = os.path.join(utilities.__path__[0], 'auxiliaryFiles/parameterListMarlinSteeringFile.txt')

    parDict = readParameterDict(parListFileName)
    for key in list(parDict.keys()):
      newKey = None
      if 'MyDDMarlinPandora' in key:
        newKey = key.replace('MyDDMarlinPandora', self.settings['DDPandoraPFANewProcessorName'])
      if 'MyDDCaloDigi' in key:
        newKey = key.replace('MyDDCaloDigi', self.settings['DDCaloDigiName'])
      if newKey is not None:
        parDict[newKey] = parDict.pop(key)

    res = readParametersFromSteeringFile(self.localSteeringFile, parDict, ['PfoAnalysis'])
    if not res['OK']:
      self.log.error('Failed to read parameters from steering file:', res['Message'])
      return S_ERROR('Failed to read parameters from steering file')

    if self.settings['disableSoftwareCompensation']:
      tmpKey = (".//processor[@name='%s']/parameter[@name='MaxClusterEnergyToApplySoftComp']"
                % self.settings['DDPandoraPFANewProcessorName'])
      parDict[tmpKey] = 0.0
      tmpDict = {tmpKey: None}
      res = readParametersFromSteeringFile(self.localSteeringFile, tmpDict)
      # if no such parameter in the steering file --> add it
      if not res['OK']:
        res = addParameterToProcessor(self.localSteeringFile, self.settings['DDPandoraPFANewProcessorName'], {
                                      'name': 'MaxClusterEnergyToApplySoftComp', 'type': 'float', 'value': '0'})
        print("self.settings['DDPandoraPFANewProcessorName']: %s" % self.settings['DDPandoraPFANewProcessorName'])
        print('out of addParameterToProcessor: %s' % res)
        print('self.localSteeringFile: %s' % self.localSteeringFile)
        if not res['OK']:
          self.log.error('Message from addParameterToProcessor function: %s' % res['Message'])

    self.calibrationConstantsDict = parDict

    self.currentParameterSet['currentStage'] = self.currentStage
    self.currentParameterSet['currentPhase'] = self.currentPhase
    self.currentParameterSet['currentStep'] = self.currentStep
    self.currentParameterSet['parameters'] = self.calibrationConstantsDict
    self.currentParameterSet['calibrationIsFinished'] = self.calibrationFinished

    if self.settings['nEcalThickLayers'] > 0:
      tmpKey = ".//processor[@name='%s']/parameter[@name='ECALLayers']" % self.settings['DDCaloDigiName']
      # +1 is just for safety
      valToSetup = '%s %s' % (self.settings['nEcalThinLayers'],
                              self.settings['nEcalThinLayers'] + self.settings['nEcalThickLayers'] + 1)
      tmpDict = {tmpKey: None}
      res = readParametersFromSteeringFile(self.localSteeringFile, tmpDict)
      # if no such parameter in the steering file --> add it
      if not res['OK']:
        res = addParameterToProcessor(self.localSteeringFile, self.settings['DDCaloDigiName'],
                                      {'name': 'ECALLayers', 'type': 'IntVec', 'value': valToSetup})
        if not res['OK']:
          self.log.error('Message from addParameterToProcessor function: %s' % res['Message'])
      else:
        res = updateSteeringFile(self.localSteeringFile, self.localSteeringFile,
                                 {tmpKey: valToSetup})
        if not res['OK']:
          self.log.error('Error while updating local steering file. Error message: %s' % res['Message'])
          return res

    return S_OK()

  def getCalibrationID(self):
    """Get calibration ID."""
    return self.calibrationID

  @executeWithUserProxy
  def submitJobs(self, idsOfWorkerNodesToSubmitTo=None):
    """Submit the calibration jobs to the workers for the first time.

    :param idsOfWorkerNodesToSubmitTo: list of integers representing IDs of worker nodes to submit jobs to;
                                       if None submit to all allocated nodes
    :returns: S_OK or S_ERROR
    :rtype: dict
    """
    self.log.info('running submitJobs')
    res = self.readInitialParameterDict()
    self.log.info('read initial parameter dict')
    if not res['OK']:
      errMsg = 'Cannot read initial parameter dict. Message: %s' % res['Message']
      self.log.error(errMsg)
      return res

    dirac = DiracILC(True, 'calib%s/job_repository.rep' % self.calibrationID)
    results = []

    listOfNodesToSubmitTo = xrange(0, self.settings['numberOfJobs'])
    if idsOfWorkerNodesToSubmitTo is not None:
      listOfNodesToSubmitTo = idsOfWorkerNodesToSubmitTo

    key = CalibrationPhase.fileKeyFromPhase(self.currentPhase)
    self.log.verbose('fileKeyFromPhase: %s' % key)

    dirName = "calib%s/stage%s/phase%s/step%s" % (self.calibrationID,
                                                  self.currentStage, self.currentPhase, self.currentStep)
    if not os.path.exists(dirName):
      try:
        os.makedirs(dirName)
      except OSError as e:
        errMsg = 'Cannot create directories. Current working directory: %s. Error message: %s' % (os.getcwd(), e)
        self.log.error(errMsg)
        return S_ERROR(errMsg)

    for curWorkerID in listOfNodesToSubmitTo:
      # get input files
      fileList = []
      for _, iList in self.inputFiles[curWorkerID].iteritems():
        fileList += iList[0]
      lcioFiles = fileList

      # create user job
      curJob = UserJob()
      curJob.check = False  # Necessary to turn off user confirmation
      curJob.setName('PandoraCaloCalibration_calid_%s_workerid_%s' % (self.calibrationID, curWorkerID))
      curJob.setJobGroup('PandoraCaloCalibration_calid_%s' % self.calibrationID)
      if 'CLIC' in self.settings['detectorModel']:
        # needed to copy files form ClicPerformance package
        curJob.setCLICConfig(self.settings['marlinVersion'].rsplit("_", 1)[0])
      elif 'FCC' in self.settings['detectorModel']:
        appName = 'FcceeConfig'
        version = self.settings['marlinVersion'].rsplit("_", 1)[0]
        curJob._addSoftware(appName.lower(), version)
        curJob._addParameter(curJob.workflow, appName + 'Package', 'JDL', appName + version, 'FCCee Config package')

      # TODO implement using line below - choose of tracking, time window, etc.
      #  calib.setExtraCLIArguments(" --Config.Overlay="+overlayParameterValue+"  --Config.Tracking="+trackingType+"
      #                             --Output_DST.LCIOOutputFile="+outputFile+"
      #                             --constant.CalorimeterIntegrationTimeWindow="+str(calorimeterIntegrationTimeWindow))
      # FIXME use default CPU time limit?
      curJob.setCPUTime(24 * 60 * 60)
      # FIXME allow user to specify xml-files. CLIC detector have different name of PandoraLikelihhod file than CLD
      #  inputSB = ['GearOutput.xml', 'PandoraSettingsDefault.xml', 'PandoraLikelihoodData9EBin.xml']
      curJob.setInputSandbox(['LFN:' + self.settings['steeringFile']])
      curJob.setInputData(lcioFiles)
      # TODO files to redirect for output: newPhotonLikelihood.xml, finalSteeringFile
      curJob.setOutputSandbox(['*.log', '*.xml', '*.txt'])

      # create calibration workflow
      calib = Calibration()
      calib.setCalibrationID(self.calibrationID)
      calib.setWorkerID(curWorkerID)
      calib.setVersion(self.settings['marlinVersion'])
      calib.setDetectorModel(self.settings['detectorModel'])
      #  calib.setNbEvts(nEvts+1)
      #  calib.setProcessorsToUse([])
      calib.setSteeringFile(os.path.basename(self.settings['steeringFile']))
      res = curJob.append(calib)
      if not res['OK']:
        self.log.error('Append calib module to UserJob: error_msg: %s' % res['Message'])
        return S_ERROR('Failed to setup Calibration worklow module. CalibrationID = %s; WorkerID = %s'
                       % (self.calibrationID, curWorkerID))

      # submit jobs
      # FIXME we use local mode only for testing...
      # res = curJob.submit(dirac, mode='local')
      res = curJob.submit(dirac, mode='wms')
      results.append(res)

    self.calibrationRunStatus = 'Running'
    saveCalibrationRun(self)

    return results

  def addResult(self, stepID, workerID, result):
    """Add a reconstruction result to the list of other results.

    :param int stepID: ID of the step
    :param int workerID: ID of the worker providing the result
    :param result: reconstruction histogram from the worker node
    :type result: `python:list`
    :returns: None
    """
    self.lock.acquire()
    self.stepResults[stepID].addResult(workerID, result)
    saveCalibrationRun(self)
    self.lock.release()
    # FIXME: Do we add old step results? Current status is no, ensured in CalibrationHandler
    # FIXME: Do we delete old interim results?

  def getNewParameters(self, stepIDOnWorker):
    """Return current parameters.

    :param int stepIDOnWorker: The ID of the step the worker just completed.
    :returns: If the computation is finished, returns S_OK containing a success message string. If there is a new
              parameter set, a S_OK dict containing the updated parameter set. Else a S_ERROR
    :rtype: dict
    """
    if self.currentStep > stepIDOnWorker:
      return S_OK(dict(self.currentParameterSet))
    else:
      self.log.verbose('No new parameter set available yet. Current step in service: %s, step on worker: %s'
                       % (self.currentStep, stepIDOnWorker))
      return S_OK()

  def getNewPhotonLikelihood(self):
    """Return new photon likelihood."""
    if self.newPhotonLikelihood:
      return S_OK(self.newPhotonLikelihood)
    else:
      errMsg = ('No new parameter set available yet. Current step in service: %s'
                % (self.currentStep))
      self.log.verbose(errMsg)
      return S_ERROR(errMsg)

  #  def __addLists(self, list1, list2):
  #    """Add two lists together by adding the first element, second element, and so on. Throws an exception
  #    if the lists have a different number of elements.
  #
  #    :param list1: List that should be added element-wise to another
  #    :type list1: `python:list`
  #    :param list2: Other list that should be added element-wise
  #    :type list2: `python:list`
  #    :returns: The list [list1[0]+list2[0], list1[1]+list2[1], ...]
  #    :rtype: list
  #    """
  #    if len(list1) != len(list2):
  #      raise ValueError('The two lists do not have the same number of elements. \n List 1: %s \n List 2: %s'
  #                       % (list1, list2))
  #    result = []
  #    for first_elem, second_elem in zip(list1, list2):
  #      result.append(first_elem + second_elem)
  #    return result

  def __mergePandoraLikelihoodXmlFiles(self):
    self.log.info('SASHA __mergePandoraLikelihoodXmlFiles')
    folder = "calib%s/stage%s/phase%s/" % (self.calibrationID, self.currentStage, self.currentPhase)
    if not os.path.exists(folder):
      return S_ERROR('no directory found: %s' % folder)

    filesToMerge = searchFilesWithPattern(folder, '*.xml')
    self.log.info('SASHA filesToMerge: %s' % filesToMerge)
    outFileName = "calib%s/newPandoraLikelihoodData.xml" % (self.calibrationID)
    self.log.info('SASHA outFileName: %s' % outFileName)

    res = mergeLikelihoods(filesToMerge, outFileName)
    if not res['OK']:
      return res

    if os.path.exists(outFileName):
      self.newPhotonLikelihood = binaryFileToString(outFileName)
      return S_OK()
    else:
      return S_ERROR('Failed to merge photon likelihoods')

  def getKey(self, pattern):
    """Return key from calibrationConstantsDict which contain input pattern."""
    for iKey in self.calibrationConstantsDict:
      if pattern in iKey:
        return iKey
    self.log.error('Cannot find XPath inside the parameter dict which contains pattern: %s' % pattern)
    return None

  def endCurrentStep(self):
    """Calculate new parameter set and prepare the object for the next step.

    :returns: None
    """
    self.log.info('Start execution of endCurrentStep. Stage %s; Phase: %s; Step: %s' %
                  (self.currentStage, self.currentPhase, self.currentStep))

    if self.calibrationFinished:
      return S_ERROR('Calibration is finished. Do not call endCurrentStep() anymore!')

    fileNamePattern = 'pfoanalysis_w*.root'
    fileDir = "calib%s/stage%s/phase%s/step%s/" % (self.calibrationID, self.currentStage, self.currentPhase,
                                                   self.currentStep)
    inputFilesPattern = os.path.join(fileDir, fileNamePattern)
    inputFilesPattern = '"%s"' % inputFilesPattern  # needed for proper handling of wildcards
    fileDir = "calib%s/" % (self.calibrationID)
    calibrationFile = os.path.join(fileDir, "Calibration.txt")  # as hardcoded in calibration binaries

    self.log.info('calibrationFile: %s' % calibrationFile)

    scriptPath = self.ops.getValue("/AvailableTarBalls/%s/pandora_calibration_scripts/%s/%s"
                                   % (self.settings['platform'], self.settings['marlinVersion_CS'], "PandoraAnalysis"),
                                   None)
    ilcSoftInitScript = self.ops.getValue("/AvailableTarBalls/%s/pandora_calibration_scripts/%s/%s"
                                          % (self.settings['platform'], self.settings['marlinVersion_CS'],
                                             "CVMFSEnvScript"), None)

    pythonReadScriptPath = os.path.join(utilities.__path__[0], 'Python_Read_Scripts')

    truthEnergy = CalibrationPhase.sampleEnergyFromPhase(self.currentPhase)

    #  print('self.currentPhase', self.currentPhase)
    #  print('CalibrationPhase.ECalDigi', CalibrationPhase.ECalDigi)
    #  print('convert_and_execute', convert_and_execute())

    if self.currentPhase == CalibrationPhase.ECalDigi:
      binary = os.path.join(scriptPath, 'ECalDigitisation_ContainedEvents')

      res = convert_and_execute([binary, '-a', inputFilesPattern, '-b', truthEnergy,
                                 '-c', self.settings['digitisationAccuracy'], '-d', fileDir, '-e', '90', '-g', 'Barrel',
                                 '-i', self.settings['ecalBarrelCosThetaRange'][0], '-j',
                                 self.settings['ecalBarrelCosThetaRange'][1]], ilcSoftInitScript)

      res = convert_and_execute([binary, '-a', inputFilesPattern, '-b', truthEnergy,
                                 '-c', self.settings['digitisationAccuracy'], '-d', fileDir, '-e', '90', '-g', 'EndCap',
                                 '-i', self.settings['ecalEndcapCosThetaRange'][0], '-j',
                                 self.settings['ecalEndcapCosThetaRange'][1]], ilcSoftInitScript)

      # this parameter can be written in format "value value" in the xml steering file in case of ECAL layout which has
      # layers with different thicknesses
      prevStepCalibConstBarrel = float(self.calibrationConstantsDict[self.getKey('CalibrECAL')].split()[0])
      prevStepCalibConstEndcap = prevStepCalibConstBarrel * float(self.calibrationConstantsDict[
          self.getKey('ECALEndcapCorrectionFactor')])

      pythonReadScript = os.path.join(pythonReadScriptPath, 'ECal_Digi_Extract.py')
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 truthEnergy, prevStepCalibConstBarrel, 'Calibration_Constant',
                                 'Barrel'])
      calibConstBarrel = float(res['Value'][1].split('\n')[0])
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 truthEnergy, prevStepCalibConstEndcap, 'Calibration_Constant',
                                 'Endcap'])
      calibConstEndcap = float(res['Value'][1].split('\n')[0])
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 truthEnergy, prevStepCalibConstBarrel, 'Mean', 'Barrel'])
      meanBarrel = float(res['Value'][1].split('\n')[0])
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 truthEnergy, prevStepCalibConstEndcap, 'Mean', 'Endcap'])
      meanEndcap = float(res['Value'][1].split('\n')[0])

      self.calibrationConstantsDict[self.getKey('CalibrECAL')] = (
          '%s %s' % (calibConstBarrel, self.settings['ecalResponseCorrectionForThickLayers'] * calibConstBarrel))
      self.calibrationConstantsDict[self.getKey('ECALEndcapCorrectionFactor')] = (
          calibConstEndcap / calibConstBarrel)

      fractionalError = max(abs(meanBarrel - truthEnergy), abs(meanEndcap - truthEnergy)) / truthEnergy
      if fractionalError < self.settings['digitisationAccuracy']:
        self.currentPhase = self.currentPhase + 1

    elif self.currentPhase == CalibrationPhase.HCalDigi:
      binary = os.path.join(scriptPath, 'HCalDigitisation_ContainedEvents')
      kineticEnergy = truthEnergy - 0.497672

      res = convert_and_execute([binary, '-a', inputFilesPattern, '-b', truthEnergy,
                                 '-c', self.settings['digitisationAccuracy'], '-d', fileDir, '-e', '90', '-g', 'Barrel',
                                 '-i', self.settings['hcalBarrelCosThetaRange'][0], '-j',
                                 self.settings['hcalBarrelCosThetaRange'][1]], ilcSoftInitScript)

      res = convert_and_execute([binary, '-a', inputFilesPattern, '-b', truthEnergy,
                                 '-c', self.settings['digitisationAccuracy'], '-d', fileDir, '-e', '90', '-g', 'EndCap',
                                 '-i', self.settings['hcalEndcapCosThetaRange'][0], '-j',
                                 self.settings['hcalEndcapCosThetaRange'][1]], ilcSoftInitScript)

      prevStepCalibConstBarrel = float(self.calibrationConstantsDict[
          self.getKey('CalibrHCALBarrel')])
      prevStepCalibConstEndcap = float(self.calibrationConstantsDict[
          self.getKey('CalibrHCALEndcap')])

      pythonReadScript = os.path.join(pythonReadScriptPath, 'HCal_Digi_Extract.py')
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 kineticEnergy, prevStepCalibConstBarrel, 'Barrel',
                                 'Calibration_Constant'])
      calibConstBarrel = float(res['Value'][1].split('\n')[0])
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 kineticEnergy, prevStepCalibConstEndcap, 'EndCap',
                                 'Calibration_Constant'])
      calibConstEndcap = float(res['Value'][1].split('\n')[0])
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 kineticEnergy, prevStepCalibConstBarrel, 'Barrel', 'Mean'])
      meanBarrel = float(res['Value'][1].split('\n')[0])
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 kineticEnergy, prevStepCalibConstEndcap, 'EndCap', 'Mean'])
      meanEndcap = float(res['Value'][1].split('\n')[0])

      self.calibrationConstantsDict[
          self.getKey('CalibrHCALBarrel')] = calibConstBarrel
      self.calibrationConstantsDict[
          self.getKey('CalibrHCALEndcap')] = calibConstEndcap

      fractionalError = max(abs(meanBarrel - kineticEnergy), abs(meanEndcap - kineticEnergy)) / truthEnergy
      if fractionalError < self.settings['digitisationAccuracy']:
        self.currentPhase = self.currentPhase + 1

    elif self.currentPhase == CalibrationPhase.MuonAndHCalOtherDigi:
      binary = os.path.join(scriptPath, 'PandoraPFACalibrate_MipResponse')
      kineticEnergy = truthEnergy - 0.497672
      res = convert_and_execute([binary, '-a', inputFilesPattern, '-b', truthEnergy,
                                 '-c', fileDir],
                                ilcSoftInitScript)

      pythonReadScript = os.path.join(pythonReadScriptPath, 'Extract_GeVToMIP.py')
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 truthEnergy, 'ECal'])
      ecalGevToMip = float(res['Value'][1].split('\n')[0])
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 truthEnergy, 'HCal'])
      hcalGevToMip = float(res['Value'][1].split('\n')[0])
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 truthEnergy, 'Muon'])
      muonGevToMip = float(res['Value'][1].split('\n')[0])
      # constants below were not used anywhere but they were calculated in the previous calibration procedure
      #  ecalMipMpv = float(convert_and_execute(['python', 'Extract_SimCaloHitMIPMPV.py', calibrationFile,
      #                                          'ECal']))
      #  hcalMipMpv = float(convert_and_execute(['python', 'Extract_SimCaloHitMIPMPV.py', calibrationFile,
      #                                          'HCal']))

      self.calibrationConstantsDict[self.getKey('ECalToMipCalibration')] = (
          ecalGevToMip)
      self.calibrationConstantsDict[self.getKey('HCalToMipCalibration')] = (
          hcalGevToMip)
      self.calibrationConstantsDict[self.getKey('MuonToMipCalibration')] = (
          muonGevToMip)

      binary = os.path.join(scriptPath, 'SimCaloHitEnergyDistribution')
      res = convert_and_execute([binary, '-a', inputFilesPattern, '-b', truthEnergy,
                                 '-c', fileDir],
                                ilcSoftInitScript)

      pythonReadScript = os.path.join(pythonReadScriptPath, 'HCal_Ring_Digi_Extract.py')
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 truthEnergy])
      mipPeakRatio = float(res['Value'][1].split('\n')[0])

      # this binary need to access kaon files --> refer to previous stage and step
      kaonTruthEnergy = CalibrationPhase.sampleEnergyFromPhase(self.currentPhase - 1)
      kaonInputFilesPattern = os.path.join("calib%s/stage%s/phase%s/step%s/"
                                           % (self.calibrationID, self.currentStage, self.currentPhase - 1,
                                              self.currentStep - 1), fileNamePattern)
      binary = os.path.join(scriptPath, 'HCalDigitisation_DirectionCorrectionDistribution')
      res = convert_and_execute([binary, '-a', kaonInputFilesPattern, '-b', kaonTruthEnergy,
                                 '-c', fileDir],
                                ilcSoftInitScript)

      pythonReadScript = os.path.join(pythonReadScriptPath, 'HCal_Direction_Corrections_Extract.py')
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 kaonTruthEnergy])
      directionCorrectionRatio = float(res['Value'][1].split('\n')[0])
      calibHcalEndcap = float(self.calibrationConstantsDict[
          self.getKey('CalibrHCALEndcap')])
      # one need to access hcalMeanEndcap again (as in previous phase. Read it again from calibration file...
      # but it will also write output from script execution to the file again
      pythonReadScript = os.path.join(pythonReadScriptPath, 'HCal_Digi_Extract.py')
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 kaonTruthEnergy, calibHcalEndcap, 'Endcap', 'Mean'])
      hcalMeanEndcap = float(res['Value'][1].split('\n')[0])

      # TODO deal with these hardcoded values
      Absorber_Thickness_EndCap = 20.0
      Scintillator_Thickness_Ring = 3.0
      Absorber_Thickness_Ring = 20.0
      Scintillator_Thickness_EndCap = 3.0

      Absorber_Scintillator_Ratio = ((Absorber_Thickness_EndCap * Scintillator_Thickness_Ring)
                                     / (Absorber_Thickness_Ring * Scintillator_Thickness_EndCap))

      kaonKineticEnergy = CalibrationPhase.sampleEnergyFromPhase(CalibrationPhase.HCalDigi) - 0.49767
      calibHcalOther = (directionCorrectionRatio * mipPeakRatio * Absorber_Scintillator_Ratio * calibHcalEndcap
                        * kaonKineticEnergy / hcalMeanEndcap)

      self.calibrationConstantsDict[
          self.getKey('CalibrHCALOther')] = calibHcalOther

      self.currentPhase = self.currentPhase + 1

    elif self.currentPhase == CalibrationPhase.ElectroMagEnergy:
      binary = os.path.join(scriptPath, 'PandoraPFACalibrate_EMScale')

      res = convert_and_execute([binary, '-a', inputFilesPattern, '-b', truthEnergy,
                                 '-c', self.settings['pandoraPFAAccuracy'], '-d', fileDir, '-e', '90'],
                                ilcSoftInitScript)

      prevStepCalibConstEcalToEm = float(self.calibrationConstantsDict[
          self.getKey('ECalToEMGeVCalibration')])
      pythonReadScript = os.path.join(pythonReadScriptPath, 'EM_Extract.py')
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 truthEnergy, prevStepCalibConstEcalToEm, 'Calibration_Constant'])
      ecalToEm = float(res['Value'][1].split('\n')[0])
      hcalToEm = ecalToEm
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 truthEnergy, prevStepCalibConstEcalToEm, 'Mean'])
      emMean = float(res['Value'][1].split('\n')[0])

      self.calibrationConstantsDict[
          self.getKey('ECalToEMGeVCalibration')] = ecalToEm
      self.calibrationConstantsDict[
          self.getKey('HCalToEMGeVCalibration')] = hcalToEm

      fractionalError = abs(truthEnergy - emMean) / truthEnergy
      if fractionalError < self.settings['pandoraPFAAccuracy']:
        self.currentPhase += 1

    elif self.currentPhase == CalibrationPhase.HadronicEnergy:
      binary = os.path.join(scriptPath, 'PandoraPFACalibrate_HadronicScale_ChiSquareMethod')

      res = convert_and_execute([binary, '-a', inputFilesPattern, '-b', truthEnergy,
                                 '-c', self.settings['pandoraPFAAccuracy'], '-d', fileDir, '-e',
                                 self.settings['nHcalLayers']], ilcSoftInitScript)

      pythonReadScript = os.path.join(pythonReadScriptPath, 'Had_Extract.py')
      prevStepCalibConstHcalToHad = float(self.calibrationConstantsDict[
          self.getKey('HCalToHadGeVCalibration')])
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 truthEnergy, 'HCTH', prevStepCalibConstHcalToHad,
                                 'Calibration_Constant', 'CSM'])
      hcalToHad = float(res['Value'][1].split('\n')[0])
      prevStepCalibConstEcalToHad = float(self.calibrationConstantsDict[
          self.getKey('ECalToHadGeVCalibrationBarrel')])
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 truthEnergy, 'ECTH', prevStepCalibConstEcalToHad,
                                 'Calibration_Constant', 'CSM'])
      ecalToHad = float(res['Value'][1].split('\n')[0])

      self.calibrationConstantsDict[
          self.getKey('HCalToHadGeVCalibration')] = hcalToHad
      self.calibrationConstantsDict[
          self.getKey('ECalToHadGeVCalibrationBarrel')] = ecalToHad
      self.calibrationConstantsDict[
          self.getKey('ECalToHadGeVCalibrationEndCap')] = ecalToHad

      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 truthEnergy, 'HCTH', hcalToHad, 'FOM', 'CSM'])
      hcalToHadFom = float(res['Value'][1].split('\n')[0])
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 truthEnergy, 'ECTH', ecalToHad, 'FOM', 'CSM'])
      ecalToHadFom = float(res['Value'][1].split('\n')[0])

      kineticEnergy = truthEnergy - 0.497672
      fractionalError = max(abs(hcalToHadFom - kineticEnergy), abs(ecalToHadFom - kineticEnergy)) / kineticEnergy

      if fractionalError < self.settings['pandoraPFAAccuracy']:
        if self.currentStage == 1:
          self.currentStage += 1
          self.currentPhase += 1
        elif self.currentStage == 3:
          self.calibrationFinished = True
          self.calibrationRunStatus = "Calibration is succesfully finished."
          self.calibrationEndTime = datetime.now()
          self.log.info('The last step of calibration has been finished')
        else:
          return S_ERROR('%s' % self.currentStage)
    elif self.currentPhase == CalibrationPhase.PhotonTraining and self.currentStage == 2:
      res = self.__mergePandoraLikelihoodXmlFiles()
      if not res['OK']:
        return res
      self.currentStage += 1
      self.currentPhase = CalibrationPhase.ECalDigi
    else:
      return S_ERROR('Error in the execution sequence. Dump current ids\nstageID: %s, phaseID: %s, stepID: %s'
                     % (self.currentStage, self.currentPhase, self.currentStep))

    self.currentStep += 1

    self.currentParameterSet['currentStage'] = self.currentStage
    self.currentParameterSet['currentPhase'] = self.currentPhase
    self.currentParameterSet['currentStep'] = self.currentStep
    self.currentParameterSet['parameters'] = self.calibrationConstantsDict
    self.currentParameterSet['calibrationIsFinished'] = self.calibrationFinished

    if self.checkForDebugFlagsToStopCalibration():
      self.currentParameterSet['calibrationIsFinished'] = True
      self.calibrationFinished = True
      self.calibrationEndTime = datetime.now()
      self.calibrationRunStatus = ("Calibration is succesfully finished. It reached requested stopStage: % and"
                                   " stopPhase: %s" % (self.stopStage, self.stopPhase))

    # update local steering file after every step. This file will be used if calibration service will be restarted and
    # some calibrations are still are not finished
    res = updateSteeringFile(self.localSteeringFile, self.localSteeringFile,
                             self.calibrationConstantsDict, ["PfoAnalysis"])
    saveCalibrationRun(self)

    return S_OK()

  @executeWithUserProxy
  def copyResultsToEos(self):
    """Copy output files from the calibration to EOS."""
    res = updateSteeringFile(self.localSteeringFile, self.localSteeringFile,
                             self.calibrationConstantsDict, ["PfoAnalysis"])
    if not res['OK']:
      self.log.error('Error while updating local steering file. Error message: %s' % res['Message'])
      return res

    filesToCopy = []
    filesToCopy.append(self.localSteeringFile)
    if os.path.exists("calib%s/newPandoraLikelihoodData.xml" % (self.calibrationID)):
      filesToCopy.append("calib%s/newPandoraLikelihoodData.xml" % (self.calibrationID))
    filesToCopy.append("calib%s/Calibration.txt" % (self.calibrationID))
    filesToCopy += glob.glob("calib%s/*.C" % (self.calibrationID))
    filesToCopy += glob.glob("calib%s/*.png" % (self.calibrationID))
    filesToCopy += glob.glob("calib%s/*_INPUT" % (self.calibrationID))

    self.log.info('Start copying output of the calibration to user directory : %s' % self.settings['outputPath'])
    self.log.info('Files to copy: %s' % filesToCopy)

    dm = DataManager()
    for iFile in filesToCopy:
      if not os.path.exists(iFile):
        errMsg = "File %s must exist locally" % iFile
        self.log.error(errMsg)
        return(S_ERROR(errMsg))
      if not os.path.isfile(iFile):
        errMsg = "%s is not a file" % iFile
        self.log.error(errMsg)
        return(S_ERROR(errMsg))

      lfn = os.path.join(self.settings['outputPath'], "calib%s" % (self.calibrationID), os.path.basename(iFile))
      localFile = iFile
      res = dm.putAndRegister(lfn, localFile, 'CERN-DST-EOS', None, overwrite=True)
      if not res['OK']:
        errMsg = 'Error while uploading results to EOS. Error message: %s' % res['Message']
        self.log.error(errMsg)
        return res

    self.log.info('Copying is finished')
    return S_OK()
