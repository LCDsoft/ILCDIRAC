"""Run Pandora Calorimeter calibration.

Runs on each worker node.
ILCDIRAC.Workflow.Modules.Calibration called by Job Agent.

:since: March, 2019

:author: Jan Hendrik Ebbing
:author: Oleksandr Viazlo
"""

import os
import shutil

from ILCDIRAC.Core.Utilities.WasteCPU import wasteCPUCycles
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC import S_OK, S_ERROR, gLogger

from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import getEnvironmentScript
from ILCDIRAC.Core.Utilities.resolvePathsAndNames import resolveIFpaths
from ILCDIRAC.Core.Utilities.FindSteeringFileDir import getSteeringFileDirName
from ILCDIRAC.Workflow.Modules.MarlinAnalysis import MarlinAnalysis
from ILCDIRAC.CalibrationSystem.Client.CalibrationClient import CalibrationClient
from ILCDIRAC.CalibrationSystem.Service.CalibrationRun import CalibrationPhase
from ILCDIRAC.CalibrationSystem.Utilities.functions import updateSteeringFile
from ILCDIRAC.CalibrationSystem.Utilities.functions import readValueFromSteeringFile
from ILCDIRAC.CalibrationSystem.Utilities.functions import addPfoAnalysisProcessor
from ILCDIRAC.CalibrationSystem.Utilities.fileutils import stringToBinaryFile
from ILCDIRAC.CalibrationSystem.Utilities.functions import readParametersFromSteeringFile
from ILCDIRAC.CalibrationSystem.Utilities.functions import addParameterToProcessor

__RCSID__ = '$Id$'


class Calibration(MarlinAnalysis):
  """Define the Calibration part of the workflow."""

  def __init__(self):
    """Initialize."""
    super(Calibration, self).__init__()
    self.applicationName = "Calibration"
    self.currentStep = None
    self.currentPhase = None
    self.currentStage = None
    self.calibrationID = None
    self.workerID = None
    self.cali = None
    self.log = None

  def runIt(self):
    """Execute.

    Called by Job Agent

    Execute the following:
      1) resolve input data and environment
      2) request parameters from the service
      3) prepare Marlin steering file
      4) run Marlin and catch the exit code
      5) report results back to service
      6) repeat steps 2-5 until calibration is finished

    :return: S_OK(), S_ERROR()
    """
    self.setApplicationStatus('PandoraCalib_%s: setting up jobs' % self.calibrationID)

    if self.cali is None:
      self.cali = CalibrationClient(self.calibrationID, self.workerID)
    if self.log is None:
      self.log = gLogger.getSubLogger('%s_wid_%s' % (__name__, self.workerID))

    self.result = S_OK()
    if not self.platform:
      self.result = S_ERROR('No ILC platform selected')
    elif not self.applicationLog:
      self.result = S_ERROR('No Log file provided')
    if not self.result['OK']:
      self.log.error("Failed to resolve input parameters:", self.result["Message"])
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('%s should not proceed as previous step did not end properly' % self.applicationName)

    # FIXME this file is needed to disable watchdog check to prevent killing jobs
    self.log.info('creating DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK file to disable watchdog')
    fopen = open('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK', 'w')
    fopen.close()

    # get the path to the detector model, either local or from the software
    compactFile = None
    if self.detectorModel:
      resXML = self._getDetectorXML()
      if not resXML['OK']:
        self.log.error("Could not obtain the detector XML file: ", resXML["Message"])
        return resXML
      compactFile = resXML['Value']
    else:
      self.log.error('no detectorModel specified! use model which is provided in the template steering file')

    res = getEnvironmentScript(self.platform, "marlin", self.applicationVersion, self.getEnvScript)
    if not res['OK']:
      self.log.error("Failed to get the env script")
      return res
    env_script_path = res["Value"]

    res = resolveIFpaths(self.InputData)
    if not res['OK']:
      self.log.error("Failed to resolve path to input slcio files: %s" % res)
      return res
    listofslcio = res['Value']
    self.log.info('SASHA resolved input data: %s' % listofslcio)

    steeringfiledirname = ''
    res = getSteeringFileDirName(self.platform, "marlin", self.applicationVersion)
    if res['OK']:
      steeringfiledirname = res['Value']
    else:
      self.log.warn('Could not find the steering file directory', res['Message'])

    # Handle PandoraSettings.xml
    # TODO directory below is detector dependent... implement it
    pandorasettings = 'PandoraSettings/PandoraSettings.xml'
    if 'FCC' in self.detectorModel:
      pandorasettings = 'PandoraSettingsFCCee/PandoraSettings.xml'
    if not os.path.exists(pandorasettings):
      if steeringfiledirname and os.path.exists(os.path.join(steeringfiledirname, pandorasettings)):
        try:
          shutil.copy(os.path.join(steeringfiledirname, pandorasettings),
                      os.path.join(os.getcwd(), pandorasettings))
        except EnvironmentError as x:
          self.log.warn('Could not copy PandoraSettings.xml, exception: %s' % x)

    # Handle PandoraSettingsPhotonTraining.xml which is used for photon training stage
    pandorasettings = 'CalibrationPandoraSettings/PandoraSettingsPhotonTraining.xml'
    if not os.path.exists(pandorasettings):
      # FIXME is this path wrong?
      photontrainingfiledirname = os.path.join(steeringfiledirname, '../CalibrationPandoraSettings/')
      if photontrainingfiledirname and os.path.exists(os.path.join(photontrainingfiledirname, pandorasettings)):
        try:
          fullPathPandoraSettings = os.path.join(os.getcwd(), pandorasettings)
          shutil.copy(os.path.join(photontrainingfiledirname, pandorasettings),
                      fullPathPandoraSettings)
        except EnvironmentError as x:
          self.log.warn('Could not copy and prepare PandoraSettingsPhotonTraining.xml, exception: %s' % x)
    # rename output xml-file from photon training stage
    updateSteeringFile(pandorasettings, pandorasettings,
                       {"algorithm[@type='PhotonReconstruction']/HistogramFile":
                        'PandoraLikelihoodDataPhotonTraining.xml'})

    if not os.path.exists(self.SteeringFile):
      if steeringfiledirname:
        if os.path.exists(os.path.join(steeringfiledirname, self.SteeringFile)):
          self.SteeringFile = os.path.join(steeringfiledirname, self.SteeringFile)
          # default steering file doesn't have PfoAnalysis processor
    if not self.SteeringFile:
      self.log.error("Steering file not defined, this shouldn't happen!")
      return S_ERROR("Could not find steering file")
    if not os.path.exists(self.SteeringFile):
      self.log.error("Steering is not found!")
      return S_ERROR("Could not find steering file")
    self.log.info("Steering file: %s" % self.SteeringFile)

    # check if steering file contains PfoAnalysis processor needed for calibration
    if readValueFromSteeringFile(self.SteeringFile, ".//processor[@type='PfoAnalysis']") is None:
      addPfoAnalysisProcessor(self.SteeringFile)

    res = self.prepareMARLIN_DLL(env_script_path)
    if not res['OK']:
      self.log.error('Failed building MARLIN_DLL:', res['Message'])
      self.setApplicationStatus('Failed to setup MARLIN_DLL')
      return S_ERROR('Something wrong with software installation')
    marlin_dll = res["Value"]

    while True:
      res = self.cali.requestNewParameters()
      if not res['OK']:
        self.log.error('Stop executing calibration workflow. Error when requesting new parameters from the calibration'
                       ' service. Error message: %s' % res['Message'])
        return res

      while res['Value'] is None:
        self.log.notice("Waiting for new parameters set")
        wasteCPUCycles(10)
        res = self.cali.requestNewParameters()
        if not res['OK']:
          self.log.error('Stop executing calibration workflow. Error when requesting new parameters from the'
                         ' calibration service. Error message: %s' % res['Message'])
          return res

      calibrationParameters = res['Value']
      if calibrationParameters['calibrationIsFinished']:
        self.log.notice("Calibration finished")
        break

      self.currentPhase = calibrationParameters['currentPhase']
      self.currentStage = calibrationParameters['currentStage']
      self.currentStep = calibrationParameters['currentStep']
      parameterDict = calibrationParameters['parameters']

      # list of parameters which can be not present in the steering file be default
      # if they are not found - add them
      parametersToCheck = [('MaxClusterEnergyToApplySoftComp', 'float'), ('ECALLayers', 'IntVec')]
      for iParName, iParType in parametersToCheck:
        tmpKey = self.getKey(parameterDict, iParName)
        if tmpKey is not None:
          res = readParametersFromSteeringFile(self.SteeringFile, {tmpKey: None})
          if not res['OK']:
            #  expect key of following format:
            #    ".//processor[@name=%s]/parameter[@name='MaxClusterEnergyToApplySoftComp']"
            processorName = tmpKey.split('=')[1].split(']')[0]
            processorName = processorName.replace('\'', '')
            processorName = processorName.replace('\"', '')
            res = addParameterToProcessor(self.SteeringFile, processorName,
                                          {'name': iParName, 'type': iParType,
                                           'value': parameterDict[tmpKey]})

      self.setApplicationStatus('PandoraCalib_%s: stage: %s; phase: %s; step: %s' %
                                (self.calibrationID, self.currentStage, self.currentPhase, self.currentStep))

      res = self.resolveInputSlcioFilesAndAddToParameterDict(listofslcio, parameterDict)
      if res['OK']:
        parameterDict = res['Value']
      else:
        self.log.error('Problem while executing resolveInputSlcioFilesAndAddToParameterDict: %s' % res['Message'])
        return res

      steeringFileToRun = 'marlinSteeringFile_%s_%s_%s.xml' % (self.currentStage, self.currentPhase, self.currentStep)
      res = updateSteeringFile(self.SteeringFile, steeringFileToRun, parameterDict)
      if not res['OK']:
        self.log.error('Error while updateing steering file. Error message: %s' % res['Message'])
        return res

      # update path to the detector model, either local or from the software, if specified in settings
      if compactFile:
        res = updateSteeringFile(steeringFileToRun, steeringFileToRun,
                                 {".//processor[@name='InitDD4hep']/parameter[@name='DD4hepXMLFile']": compactFile})
        if not res['OK']:
          self.log.error('Error while updateing steering file. Error message: %s' % res['Message'])
          return res

      # FIXME for debug purposes:
      if (self.currentStage == 3) and (not os.path.exists('newPhotonLikelihood.xml')) and (self.currentStep == 0):
        # there is no newPhotonLikelihood.xml file -> create empty dummy and do not use it
        open('newPhotonLikelihood.xml', 'a').close()

      if self.currentStage == 3 and not os.path.exists('newPhotonLikelihood.xml'):
        newPhotonLikelihood = self.cali.requestNewPhotonLikelihood()
        while newPhotonLikelihood is None:
          self.log.notice("Waiting for new photon likelihood file for stage 3")
          wasteCPUCycles(10)
          newPhotonLikelihood = self.cali.requestNewPhotonLikelihood()
        stringToBinaryFile(newPhotonLikelihood, 'newPhotonLikelihood.xml')
        # TODO this depends from the name inside steering file... And it's even more difficult for FCCee case
        pandoraSettingsFile = readValueFromSteeringFile(
            self.SteeringFile, self.getKey(parameterDict, 'PandoraSettingsXmlFile'))
        pandoraSettingsFile = pandoraSettingsFile.strip()

        updateSteeringFile(
            pandoraSettingsFile, pandoraSettingsFile,
            {"algorithm[@type='PhotonReconstruction']/HistogramFile":
             "newPhotonLikelihood.xml"})

      # TODO clean up Marlin steering file - we don't need a lot of processors for calibration
      self.log.notice("new set of calibration parameters: %r" % parameterDict)

      self.result = self.runScript(steeringFileToRun, env_script_path, marlin_dll)
      if not self.result['OK']:
        self.log.error('Something wrong during running:', self.result['Message'])
        self.setApplicationStatus('Error during running %s' % self.applicationName)
        return S_ERROR('Failed to run %s' % self.applicationName)

      # FIXME make sure that runScript function return tuple of the same format as used below
      # self.result = {'OK':True,'Value':(0,'Disabled Execution','')}
      resultTuple = self.result['Value']
      if not os.path.exists(self.applicationLog):
        self.log.error("Something went terribly wrong, the log file is not present")
        self.setApplicationStatus('%s failed terribly, you are doomed!' % (self.applicationName))
        if not self.ignoreapperrors:
          return S_ERROR('%s did not produce the expected log' % (self.applicationName))

      # FIXME is result tuple correspond to the status?
      status = resultTuple
      # stdOutput = resultTuple[1]
      # stdError = resultTuple[2]
      self.log.info("Status after the application execution is:", str(status))

      outFile = "pfoAnalysis.root"
      if self.currentStage == 2:
        outFile = 'PandoraLikelihoodDataPhotonTraining.xml'

      self.cali.reportResult(outFile)

      self.setApplicationStatus('PandoraCalib_%s: step %s is finished. Waiting for other jobs' %
                                (self.calibrationID, self.currentStep))

    # TODO implement me
    #  return self.finalStatusReport(status)

    self.setApplicationStatus('PandoraCalib_%s: Calibration is finished.' % self.calibrationID)
    return S_OK()

  def getKey(self, parameterDict, pattern):
    """Return key from parameterDict which satisfy pattern."""
    for iKey in parameterDict:
      if pattern in iKey:
        return iKey
    self.log.error('Cannot find XPath inside the parameter dict which contains pattern: %s' % pattern)
    #  try:
    #    if pattern in iKey:
    #      return iKey
    #  except:
    #    self.log.error('Cannot find XPath inside the parameter dict which contains pattern: %s' % pattern)
    return None

  def resolveInputSlcioFilesAndAddToParameterDict(self, allSlcioFiles, parameterDict):
    """Resolve pathes to input files.

    Add PandoraSettings-file and input slcio files which corresponds to current currentStage and currentPhase to the
    parameterDict.

    :param list basestring allSlcioFiles: List of all slcio-files in the node
    :param dict parameterDict: dict of parameters and their values

    :returns: S_OK or S_ERROR
    :rtype: dict
    """
    pandoraSettingsFile = ''
    if self.currentStage in [1, 3]:  # FIXME hardcoded values are bad...
      pandoraSettingsFile = 'PandoraSettings/PandoraSettingsDefault.xml'
      if 'FCC' in self.detectorModel:
        pandoraSettingsFile = 'PandoraSettingsFCCee/PandoraSettingsDefault.xml'
    else:
      pandoraSettingsFile = 'CalibrationPandoraSettings/PandoraSettingsPhotonTraining.xml'

    iType = CalibrationPhase.fileKeyFromPhase(self.currentPhase).lower()
    self.log.info('SASHA iType: %s' % iType)
    res = self.cali.getInputDataDict()
    print('res: %s' % res)
    if not res['OK']:
      errorMessageConst = 'Somemething went wrong during retrieveing inputDataDict!'
      errorMessageVariable = 'Msg: %s' % (res['Message'])
      self.log.error(errorMessageConst, errorMessageVariable)
      return S_ERROR(errorMessageConst + errorMessageVariable)

    self.log.info('SASHA self.cali.getInputDataDict() %s' % res)

    inputDataDict = res['Value']
    print('iType: %s' % iType)
    print('inputDataDict.keys(): %s' % inputDataDict.keys())
    if iType not in inputDataDict.keys():
      errorMessage = 'Corrupted inputDataDict! No files for process: %s' % (iType)
      self.log.error(errorMessage)
      return S_ERROR(errorMessage)

    lfnList = inputDataDict[iType][0]
    numberOfEventsToSkip = inputDataDict[iType][1]
    numberOfEventsToProcess = inputDataDict[iType][2]

    parameterDict[self.getKey(parameterDict, 'SkipNEvents')] = numberOfEventsToSkip
    parameterDict[self.getKey(parameterDict, 'MaxRecordNumber')] = numberOfEventsToProcess

    res = resolveIFpaths(lfnList)
    if not res['OK']:
      self.log.error("Failed to resolve path to input slcio files: %s" % res)
      return res
    filesToRunOn = res['Value']
    if len(filesToRunOn) == 0:
      errorMessage = 'Corrupted inputDataDict! No files for process: %s' % (iType)
      self.log.error(errorMessage)
      return S_ERROR(errorMessage)
    if not set(filesToRunOn).issubset(allSlcioFiles):
      errorMessage = ('Cannot find all input data on the worker. Needed files for phase: %s; All copied files: %s'
                      % (filesToRunOn, allSlcioFiles))
      self.log.error(errorMessage)
      return S_ERROR(errorMessage)

    parameterDict[self.getKey(parameterDict, 'LCIOInputFiles')] = ' '.join(filesToRunOn)
    parameterDict[self.getKey(parameterDict, 'PandoraSettingsXmlFile')] = pandoraSettingsFile

    # TODO should one use different steering file for photon training? if no one need to append line below during all
    # steps
    if self.currentStage in [1, 3]:
      parameterDict[self.getKey(parameterDict, 'RootFile')] = 'pfoAnalysis.root'
    else:
      parameterDict[self.getKey(parameterDict, 'RootFile')] = 'dummy.root'
    return S_OK(parameterDict)

  def runScript(self, marlinSteeringFile, env_script_path, marlin_dll):
    """Actual bit of code running Marlin and PandoraAnalysis.

    :param marlinSteeringFile: steering file to use for Marlin reconstruction. E.g.: 'fccReconstruction.xml'
    :param string env_script_path: path to the setup environment scripts
    :param string marlin_dll: string containing path to marlin libraries

    :returns: FIXME S_OK or S_ERROR
    :rtype: dict
    """
    res = self._prepareRunScript(marlinSteeringFile, env_script_path, marlin_dll)
    if not res['OK']:
      return res

    scriptName = res['Value']

    if os.path.exists(self.applicationLog):
      os.remove(self.applicationLog)

    os.chmod(scriptName, 0755)
    comm = 'sh -c "./%s"' % (scriptName)
    self.setApplicationStatus('Running: stage: %s; phase: %s; step: %s'
                              % (self.currentStage, self.currentPhase, self.currentStep))
    self.stdError = ''
    res = shellCall(0, comm, callbackFunction=self.redirectLogOutput, bufferLimit=20971520)
    return res

  def _prepareRunScript(self, marlinSteeringFile, env_script_path, marlin_dll):
    """Return current parameters.

    :param marlinSteeringFile: steering file to use for Marlin reconstruction. E.g.: 'fccReconstruction.xml'
    :param string env_script_path: path to the setup environment scripts
    :param string marlin_dll: string containing path to marlin libraries

    :returns: S_OK or S_ERROR
    :rtype: dict
    """
    scriptName = '%s_%s_Run_%s.sh' % (self.applicationName, self.applicationVersion, self.currentStep)
    if os.path.exists(scriptName):
      os.remove(scriptName)
    script = open(scriptName, 'w')
    script.write('#!/bin/bash \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')
    script.write("source %s\n" % env_script_path)
    script.write("declare -x MARLIN_DLL=%s\n" % marlin_dll)
    if os.path.exists("./lib/lddlib"):
      script.write('declare -x LD_LIBRARY_PATH=./lib/lddlib:$LD_LIBRARY_PATH\n')
    script.write('declare -x PATH=$ROOTSYS/bin:$PATH\n')
    script.write('declare -x MARLIN_DEBUG=1\n')  # Needed for recent version of marlin (from 03 april 2013)
    # We need to make sure the PandoraSettings is in the current directory
    script.write("""
if [ -e "${PANDORASETTINGS}" ]
then
   cp $PANDORASETTINGS .
fi
""")
    script.write('echo =============================\n')
    script.write('echo LD_LIBRARY_PATH is\n')
    script.write('echo $LD_LIBRARY_PATH | tr ":" "\n"\n')
    script.write('echo =============================\n')
    script.write('echo PATH is\n')
    script.write('echo $PATH | tr ":" "\n"\n')
    script.write('echo =============================\n')
    script.write('echo MARLIN_DLL is\n')
    script.write('echo $MARLIN_DLL | tr ":" "\n"\n')
    script.write('echo =============================\n')
    if self.debug:
      script.write('echo ldd of executable is\n')
      script.write('ldd `which Marlin` \n')
      script.write('echo =============================\n')
      if os.path.exists('./lib/marlin_dll'):
        script.write('ldd ./lib/marlin_dll/*.so \n')
      if os.path.exists('./lib/lddlib'):
        script.write('ldd ./lib/lddlib/*.so \n')
      script.write('echo =============================\n')
    script.write('env | sort >> localEnv.log\n')

    if not os.path.exists(marlinSteeringFile):
      script.close()
      self.log.error("Steering file missing: %s" % (marlinSteeringFile))
      return S_ERROR("SteeringFile is missing: %s" % (marlinSteeringFile))
    # check
    script.write('Marlin -c %s %s\n' % (marlinSteeringFile, self.extraCLIarguments))
    # real run
    script.write('Marlin %s %s\n' % (marlinSteeringFile, self.extraCLIarguments))
    script.write('declare -x appstatus=$?\n')
    script.write('exit $appstatus\n')
    script.close()
    return S_OK(scriptName)
