"""
The CalibrationRun ???
"""

import glob
import os
from collections import defaultdict

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.Proxy import executeWithUserProxy
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from ILCDIRAC.CalibrationSystem.Utilities.fileutils import binaryFileToString
from ILCDIRAC.CalibrationSystem.Utilities.functions import convert_and_execute
from ILCDIRAC.CalibrationSystem.Utilities.functions import searchFilesWithPattern
from ILCDIRAC.CalibrationSystem.Utilities.functions import readParameterDict
from ILCDIRAC.CalibrationSystem.Utilities.functions import readParametersFromSteeringFile
from ILCDIRAC.CalibrationSystem.Utilities.functions import updateSteeringFile
import ILCDIRAC.CalibrationSystem.Utilities as utilities
from ILCDIRAC.CalibrationSystem.Client.CalibrationClient import CalibrationPhase
from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob
from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
from ILCDIRAC.Interfaces.API.NewInterface.Applications.Calibration import Calibration

# TODO do we need it here (since there is one in calibrationHandler file). What is this for?
__RCSID__ = "$Id$"
LOG = gLogger.getSubLogger(__name__)


#pylint: disable=no-self-use
class CalibrationResult(object):
  """ Wrapper class to store information about calibration computation interim results. Stores results
  from all worker nodes from a single step. """

  def __init__(self):
    self.results = dict()

  def addResult(self, workerID, result):
    """ Adds a result from a given worker to the object

    :param int workerID: ID of the worker providing the result
    :param result: list of floats representing the returned histogram
    :type result: `python:list`
    :returns: None
    """
    self.results[workerID] = result

  def getNumberOfResults(self):
    """ Return number of interim results stored in this wrapper

    :returns: Number of histograms stored in this object
    :rtype: int
    """
    return len(self.results)


class CalibrationRun(object):
  """ Object that stores information about a single run of the calibration software.
  Includes files, current parameter set, software version, the workers running as well as
  the results of each step.
  """

  def __init__(self, calibrationID, inputFiles, calibSettingsDict):
    self.calibrationID = calibrationID
    self.settings = calibSettingsDict
    self.inputFiles = inputFiles
    self.log = LOG.getSubLogger('[%s]' % calibrationID)
    if 'LFN:' in self.settings['steeringFile']:
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
    self.newPhotonLikelihood = None
    self.ops = Operations()
    self.calibrationConstantsDict = None
    self.proxyUserName = ''
    self.proxyUserGroup = ''

    #self.workerJobs = [] ##FIXME: Disabled because not used? Maybe in submit initial jobs
    #self.activeWorkers = dict() ## dict between calibration and worker node? ##FIXME:Disabled because not used?
    #FIXME: Probably need to store a mapping workerID -> part of calibration that worker is working on. This then needs
    #to be accessed by the agent in the case of resubmission

  def readInitialParameterDict(self):
    self.log.info('running readInitialParameterDict')

    dataMan = DataManager()
    res = dataMan.getFile(self.settings['steeringFile'], destinationDir='calib%s/' % self.calibrationID)

    if not res['OK'] or not os.path.exists(self.localSteeringFile):
      errMsg = 'Cannot copy Marlin steering file. res: %s' % res
      self.log.error(errMsg)
      return S_ERROR(errMsg)

    # FIXME this path will be different in production version probably... update it
    parListFileName = os.path.join(utilities.__path__[0], 'testing/parameterListMarlinSteeringFile.txt')
    parDict = readParameterDict(parListFileName)
    res = readParametersFromSteeringFile(self.localSteeringFile, parDict)
    if not res['OK']:
      self.log.error('Failed to read parameters from steering file:', res['Message'])
      return S_ERROR('Failed to read parameters from steering file')

    self.calibrationConstantsDict = parDict

    self.currentParameterSet['currentStage'] = self.currentStage
    self.currentParameterSet['currentPhase'] = self.currentPhase
    self.currentParameterSet['currentStep'] = self.currentStep
    self.currentParameterSet['parameters'] = self.calibrationConstantsDict
    self.currentParameterSet['calibrationIsFinished'] = self.calibrationFinished

    return S_OK()

  def getCalibrationID(self):
    return self.calibrationID

  # TODO this function is only for debugging purpose
  def dumpSelfArguments(self):
    for iEl in dir(self):
      if "__" not in iEl:
        iElVal = eval("self." + iEl)
        if isinstance(iElVal, (float, int, basestring, list, dict, tuple)):
          print("%s: %s" % (iEl, eval("self." + iEl)))

  @executeWithUserProxy
  def submitJobs(self, idsOfWorkerNodesToSubmitTo=None):
    """ Submit the calibration jobs to the workers for the first time.
    Use a specially crafted application that runs repeated Marlin reconstruction steps

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

    for curWorkerID in listOfNodesToSubmitTo:
      # get input files
      fileList = []
      for _, iList in self.inputFiles[curWorkerID].iteritems():
        fileList += iList
      lcioFiles = fileList

      # create user job
      curJob = UserJob()
      curJob.check = False  # Necessary to turn off user confirmation
      curJob.setName('CalibrationService_calid_%s_workerid_%s' % (self.calibrationID, curWorkerID))
      curJob.setJobGroup('CalibrationService_calib_job')
      # needed to copy files form ClicPerformance package
      curJob.setCLICConfig(self.settings['marlinVersion'].rsplit("_", 1)[0])
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

    return results

  def addResult(self, stepID, workerID, result):
    """ Add a reconstruction result to the list of other results

    :param int stepID: ID of the step
    :param int workerID: ID of the worker providing the result
    :param result: reconstruction histogram from the worker node
    :type result: `python:list`
    :returns: None
    """
    self.stepResults[stepID].addResult(workerID, result)
    #FIXME: Do we add old step results? Current status is no, ensured in CalibrationHandler
    #FIXME: Do we delete old interim results?

  def getNewParameters(self, stepIDOnWorker):
    """ Returns the current parameters

    :param int stepIDOnWorker: The ID of the step the worker just completed.
    :returns: If the computation is finished, returns S_OK containing a success message string. If there is a new
              parameter set, a S_OK dict containing the updated parameter set. Else a S_ERROR
    :rtype: dict
    """
    if self.currentStep > stepIDOnWorker:
      return S_OK(dict(self.currentParameterSet))
    else:
      self.log.info('No new parameter set available yet. Current step in service: %s, step on worker: %s'
                    % (self.currentStep, stepIDOnWorker))
      return S_OK()

  def getNewPhotonLikelihood(self):
    if self.newPhotonLikelihood:
      return S_OK(self.newPhotonLikelihood)
    else:
      return S_ERROR('No new photon likelihood file available yet. Current stage and phase in service: %s, %s'
                     % (self.currentStage, self.currentPhase))

  def __addLists(self, list1, list2):
    """ Adds two lists together by adding the first element, second element, and so on. Throws an exception
    if the lists have a different number of elements.

    :param list1: List that should be added element-wise to another
    :type list1: `python:list`
    :param list2: Other list that should be added element-wise
    :type list2: `python:list`
    :returns: The list [list1[0]+list2[0], list1[1]+list2[1], ...]
    :rtype: list
    """
    if len(list1) != len(list2):
      raise ValueError('The two lists do not have the same number of elements. \n List 1: %s \n List 2: %s'
                       % (list1, list2))
    result = []
    for first_elem, second_elem in zip(list1, list2):
      result.append(first_elem + second_elem)
    return result

  def __mergePandoraLikelihoodXmlFiles(self):
    self.log.info('SASHA __mergePandoraLikelihoodXmlFiles')
    folder = "calib%s/stage%s/phase%s/" % (self.calibrationID, self.currentStage, self.currentPhase)
    if not os.path.exists(folder):
      return S_ERROR('no directory found: %s' % folder)

    filesToMerge = searchFilesWithPattern(folder, '*.xml')
    self.log.info('SASHA filesToMerge: %s' % filesToMerge)
    outFileName = "calib%s/newPandoraLikelihoodData.xml" % (self.calibrationID)
    self.log.info('SASHA outFileName: %s' % outFileName)

    from ILCDIRAC.CalibrationSystem.Utilities.mergePandoraLikelihoodData import mergeLikelihoods
    res = mergeLikelihoods(filesToMerge, outFileName)
    if not res['OK']:
      return res

    if os.path.exists(outFileName):
      self.newPhotonLikelihood = binaryFileToString(outFileName)
      return S_OK()
    else:
      return S_ERROR('Failed to merge photon likelihoods')

  def endCurrentStep(self):
    """ Calculates the new parameter set based on the results from the computations and prepares the object
    for the next step. (StepCounter increased, ...)

    :returns: None
    """
    self.log.info('Start execution of endCurrentStep')

    if self.calibrationFinished:
      return S_ERROR('Calibration is finished. Do not call endCurrentStep() anymore!')

    fileNamePattern = 'pfoanalysis_w*.root'
    fileDir = "calib%s/stage%s/phase%s/step%s/" % (self.calibrationID, self.currentStage, self.currentPhase,
                                                   self.currentStep)
    inputFilesPattern = os.path.join(fileDir, fileNamePattern)
    fileDir = "calib%s/" % (self.calibrationID)
    calibrationFile = os.path.join(fileDir, "Calibration.txt")  # as hardcoded in calibration binaries

    self.log.info('calibrationFile: %s' % calibrationFile)

    scriptPath = self.ops.getValue("/AvailableTarBalls/%s/pandora_calibration_scripts/%s/%s" % (self.settings['platform'],
                                                                                                self.settings['marlinVersion_CS'], "PandoraAnalysis"), None)
    ilcSoftInitScript = self.ops.getValue("/AvailableTarBalls/%s/pandora_calibration_scripts/%s/%s" % (self.settings['platform'],
                                                                                                       self.settings['marlinVersion_CS'], "CVMFSEnvScript"), None)

    import ILCDIRAC.CalibrationSystem.Utilities as utilities
    pythonReadScriptPath = os.path.join(utilities.__path__[0], 'Python_Read_Scripts')

    truthEnergy = CalibrationPhase.sampleEnergyFromPhase(self.currentPhase)

    #  print('self.currentPhase', self.currentPhase)
    #  print('CalibrationPhase.ECalDigi', CalibrationPhase.ECalDigi)
    #  print('convert_and_execute', convert_and_execute())

    if self.currentPhase == CalibrationPhase.ECalDigi:
      binary = os.path.join(scriptPath, 'ECalDigitisation_ContainedEvents')

      res = convert_and_execute([binary, '-a', inputFilesPattern, '-b', truthEnergy,
                                 '-c', self.settings['digitisationAccuracy'], '-d', fileDir, '-e', '90', '-g', 'Barrel',
                                 '-i', self.settings['ecalBarrelCosThetaRange'][0], '-j', self.settings['ecalBarrelCosThetaRange'][1]],
                                ilcSoftInitScript)

      res = convert_and_execute([binary, '-a', inputFilesPattern, '-b', truthEnergy,
                                 '-c', self.settings['digitisationAccuracy'], '-d', fileDir, '-e', '90', '-g', 'EndCap',
                                 '-i', self.settings['ecalEndcapCosThetaRange'][0], '-j', self.settings['ecalEndcapCosThetaRange'][1]],
                                ilcSoftInitScript)

      # this parameter is written in format "value value" in the xml steering file
      prevStepCalibConstBarrel = float(self.calibrationConstantsDict[
          "processor[@name='MyDDCaloDigi']/parameter[@name='CalibrECAL']"].split()[0])
      prevStepCalibConstEndcap = prevStepCalibConstBarrel * float(self.calibrationConstantsDict[
          "processor[@name='MyDDCaloDigi']/parameter[@name='ECALEndcapCorrectionFactor']"])

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

      self.calibrationConstantsDict["processor[@name='MyDDCaloDigi']/parameter[@name='CalibrECAL']"] = (
          '%s %s' % (calibConstBarrel, calibConstBarrel))
      self.calibrationConstantsDict["processor[@name='MyDDCaloDigi']/parameter[@name='ECALEndcapCorrectionFactor']"] = (
          calibConstEndcap / calibConstBarrel)

      fractionalError = max(abs(meanBarrel - truthEnergy), abs(meanEndcap - truthEnergy)) / truthEnergy
      if fractionalError < self.settings['digitisationAccuracy']:
        self.currentPhase = self.currentPhase + 1

    elif self.currentPhase == CalibrationPhase.HCalDigi:
      binary = os.path.join(scriptPath, 'HCalDigitisation_ContainedEvents')

      res = convert_and_execute([binary, '-a', inputFilesPattern, '-b', truthEnergy,
                                 '-c', self.settings['digitisationAccuracy'], '-d', fileDir, '-e', '90', '-g', 'Barrel',
                                 '-i', self.settings['hcalBarrelCosThetaRange'][0], '-j', self.settings['hcalBarrelCosThetaRange'][1]],
                                ilcSoftInitScript)

      res = convert_and_execute([binary, '-a', inputFilesPattern, '-b', truthEnergy,
                                 '-c', self.settings['digitisationAccuracy'], '-d', fileDir, '-e', '90', '-g', 'EndCap',
                                 '-i', self.settings['hcalEndcapCosThetaRange'][0], '-j', self.settings['hcalEndcapCosThetaRange'][1]],
                                ilcSoftInitScript)

      prevStepCalibConstBarrel = float(self.calibrationConstantsDict[
          "processor[@name='MyDDCaloDigi']/parameter[@name='CalibrHCALBarrel']"])
      prevStepCalibConstEndcap = float(self.calibrationConstantsDict[
          "processor[@name='MyDDCaloDigi']/parameter[@name='CalibrHCALEndcap']"])

      pythonReadScript = os.path.join(pythonReadScriptPath, 'HCal_Digi_Extract.py')
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 truthEnergy, prevStepCalibConstBarrel, 'Barrel',
                                 'Calibration_Constant'])
      calibConstBarrel = float(res['Value'][1].split('\n')[0])
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 truthEnergy, prevStepCalibConstEndcap, 'EndCap',
                                 'Calibration_Constant'])
      calibConstEndcap = float(res['Value'][1].split('\n')[0])
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 truthEnergy, prevStepCalibConstBarrel, 'Barrel', 'Mean'])
      meanBarrel = float(res['Value'][1].split('\n')[0])
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 truthEnergy, prevStepCalibConstEndcap, 'EndCap', 'Mean'])
      meanEndcap = float(res['Value'][1].split('\n')[0])

      self.calibrationConstantsDict[
          "processor[@name='MyDDCaloDigi']/parameter[@name='CalibrHCALBarrel']"] = calibConstBarrel
      self.calibrationConstantsDict[
          "processor[@name='MyDDCaloDigi']/parameter[@name='CalibrHCALEndcap']"] = calibConstEndcap

      fractionalError = max(abs(meanBarrel - truthEnergy), abs(meanEndcap - truthEnergy)) / truthEnergy
      if fractionalError < self.settings['digitisationAccuracy']:
        self.currentPhase = self.currentPhase + 1

    elif self.currentPhase == CalibrationPhase.MuonAndHCalOtherDigi:
      binary = os.path.join(scriptPath, 'PandoraPFACalibrate_MipResponse')
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

      self.calibrationConstantsDict["processor[@name='MyDDMarlinPandora']/parameter[@name='ECalToMipCalibration']"] = (
          ecalGevToMip)
      self.calibrationConstantsDict["processor[@name='MyDDMarlinPandora']/parameter[@name='HCalToMipCalibration']"] = (
          hcalGevToMip)
      self.calibrationConstantsDict["processor[@name='MyDDMarlinPandora']/parameter[@name='MuonToMipCalibration']"] = (
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
          "processor[@name='MyDDCaloDigi']/parameter[@name='CalibrHCALEndcap']"])
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

      calibHcalOther = (directionCorrectionRatio * mipPeakRatio * Absorber_Scintillator_Ratio * calibHcalEndcap *
                        kaonTruthEnergy / hcalMeanEndcap)

      self.calibrationConstantsDict[
          "processor[@name='MyDDCaloDigi']/parameter[@name='CalibrHCALOther']"] = calibHcalOther

      self.currentPhase = self.currentPhase + 1

    elif self.currentPhase == CalibrationPhase.ElectroMagEnergy:
      binary = os.path.join(scriptPath, 'PandoraPFACalibrate_EMScale')

      res = convert_and_execute([binary, '-a', inputFilesPattern, '-b', truthEnergy,
                                 '-c', self.settings['pandoraPFAAccuracy'], '-d', fileDir, '-e', '90'],
                                ilcSoftInitScript)

      prevStepCalibConstEcalToEm = float(self.calibrationConstantsDict[
          "processor[@name='MyDDMarlinPandora']/parameter[@name='ECalToEMGeVCalibration']"])
      pythonReadScript = os.path.join(pythonReadScriptPath, 'EM_Extract.py')
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 truthEnergy, prevStepCalibConstEcalToEm, 'Calibration_Constant'])
      ecalToEm = float(res['Value'][1].split('\n')[0])
      hcalToEm = ecalToEm
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 truthEnergy, prevStepCalibConstEcalToEm, 'Mean'])
      emMean = float(res['Value'][1].split('\n')[0])

      self.calibrationConstantsDict[
          "processor[@name='MyDDMarlinPandora']/parameter[@name='ECalToEMGeVCalibration']"] = ecalToEm
      self.calibrationConstantsDict[
          "processor[@name='MyDDMarlinPandora']/parameter[@name='HCalToEMGeVCalibration']"] = hcalToEm

      fractionalError = abs(truthEnergy - emMean) / truthEnergy
      if fractionalError < self.settings['pandoraPFAAccuracy']:
        self.currentPhase += 1

    elif self.currentPhase == CalibrationPhase.HadronicEnergy:
      binary = os.path.join(scriptPath, 'PandoraPFACalibrate_HadronicScale_ChiSquareMethod')

      res = convert_and_execute([binary, '-a', inputFilesPattern, '-b', truthEnergy,
                                 '-c', self.settings['pandoraPFAAccuracy'], '-d', fileDir, '-e', self.settings['nHcalLayers']],
                                ilcSoftInitScript)

      pythonReadScript = os.path.join(pythonReadScriptPath, 'Had_Extract.py')
      prevStepCalibConstHcalToHad = float(self.calibrationConstantsDict[
          "processor[@name='MyDDMarlinPandora']/parameter[@name='HCalToHadGeVCalibration']"])
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 truthEnergy, 'HCTH', prevStepCalibConstHcalToHad,
                                 'Calibration_Constant', 'CSM'])
      hcalToHad = float(res['Value'][1].split('\n')[0])
      prevStepCalibConstEcalToHad = float(self.calibrationConstantsDict[
          "processor[@name='MyDDMarlinPandora']/parameter[@name='ECalToHadGeVCalibrationBarrel']"])
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 truthEnergy, 'ECTH', prevStepCalibConstEcalToHad,
                                 'Calibration_Constant', 'CSM'])
      ecalToHad = float(res['Value'][1].split('\n')[0])

      self.calibrationConstantsDict[
          "processor[@name='MyDDMarlinPandora']/parameter[@name='HCalToHadGeVCalibration']"] = hcalToHad
      self.calibrationConstantsDict[
          "processor[@name='MyDDMarlinPandora']/parameter[@name='ECalToHadGeVCalibrationBarrel']"] = ecalToHad
      self.calibrationConstantsDict[
          "processor[@name='MyDDMarlinPandora']/parameter[@name='ECalToHadGeVCalibrationEndCap']"] = ecalToHad

      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 truthEnergy, 'HCTH', hcalToHad, 'FOM', 'CSM'])
      hcalToHadFom = float(res['Value'][1].split('\n')[0])
      res = convert_and_execute(['python', pythonReadScript, calibrationFile,
                                 truthEnergy, 'ECTH', ecalToHad, 'FOM', 'CSM'])
      ecalToHadFom = float(res['Value'][1].split('\n')[0])

      fractionalError = max(abs(hcalToHadFom - truthEnergy), abs(ecalToHadFom - truthEnergy)) / truthEnergy

      if fractionalError < self.settings['pandoraPFAAccuracy']:
        if self.currentStage == 1:
          self.currentStage += 1
          self.currentPhase += 1
        elif self.currentStage == 3:
          self.calibrationFinished = True
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

    # update local steering file after every step. This file will be used if calibration service will be restarted and some calibrations are still are not finished
    res = updateSteeringFile(self.localSteeringFile, self.localSteeringFile, self.calibrationConstantsDict)

    return S_OK()

  @executeWithUserProxy
  def copyResultsToEos(self):

    res = updateSteeringFile(self.localSteeringFile, self.localSteeringFile, self.calibrationConstantsDict)
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

    self.log.info('Start copying output of the calibration to user directory : %s' % self.settings['outputPath'])
    self.log.info('Files to copy: %s' % filesToCopy)

    from DIRAC.DataManagementSystem.Client.DataManager import DataManager
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
