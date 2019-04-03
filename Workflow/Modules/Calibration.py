'''
Run Pandora Calorimeter calibration

ILCDIRAC.Workflow.Modules.Calibration Called by Job Agent.

:since: March, 2019

:author: Jan Hendrik Ebbing
:author: Oleksandr Viazlo
'''

import glob
import os
import shutil

from xml.etree import ElementTree as et
from ILCDIRAC.Core.Utilities.WasteCPU import wasteCPUCycles
from collections import defaultdict
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC import S_OK, S_ERROR, gLogger

from ILCDIRAC.Core.Utilities.WasteCPU import wasteCPUCycles
from collections import defaultdict
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC import S_OK, S_ERROR, gLogger

from ILCDIRAC.Workflow.Modules.ModuleBase import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import getSoftwareFolder, getEnvironmentScript
from ILCDIRAC.Core.Utilities.PrepareOptionFiles import prepareXMLFile, getNewLDLibs
from ILCDIRAC.Core.Utilities.resolvePathsAndNames import resolveIFpaths, getProdFilename
from ILCDIRAC.Core.Utilities.PrepareLibs import removeLibc
from ILCDIRAC.Core.Utilities.FindSteeringFileDir import getSteeringFileDirName
from ILCDIRAC.Workflow.Utilities.DD4hepMixin import DD4hepMixin
from ILCDIRAC.Workflow.Modules.MarlinAnalysis import MarlinAnalysis
from ILCDIRAC.CalibrationSystem.Client.CalibrationClient import CalibrationClient, CalibrationPhase
from ILCDIRAC.CalibrationSystem.Utilities.functions import xml_generate, updateSteeringFile
from ILCDIRAC.CalibrationSystem.Utilities.functions import readParameterDict, readValueFromSteeringFile
from ILCDIRAC.CalibrationSystem.Utilities.functions import readParametersFromSteeringFile
from ILCDIRAC.CalibrationSystem.Utilities.fileutils import stringToBinaryFile

__RCSID__ = '$Id$'
LOG = gLogger.getSubLogger(__name__)

class Calibration(MarlinAnalysis):
  """Define the Calibration part of the workflow
  """

  def __init__(self):
    super(Calibration, self).__init__()
    self.applicationName = "Calibration"
    self.currentStep = -1  # internal counter of worker node of how much times Marlin was run
    self.currentPhase = None
    self.currentStage = None
    self.calibrationID = None
    self.workerID = None
    self.cali = None

  def runIt(self):
    """
    Called by Job Agent
    
    Execute the following:
      - resolve where the soft was installed
      - prepare the list of file to feed Marlin with
      - create the XML file on which Marlin has to run, done by :any:`prepareXMLFile`
      - run Marlin and catch the exit code

    :return: S_OK(), S_ERROR()
    """

    if self.cali is None:
      self.cali = CalibrationClient(self.calibrationID, self.workerID)

    self.result = S_OK()
    if not self.platform:
      self.result = S_ERROR('No ILC platform selected')
    elif not self.applicationLog:
      self.result = S_ERROR('No Log file provided')
    if not self.result['OK']:
      LOG.error("Failed to resolve input parameters:", self.result["Message"])
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      LOG.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('%s should not proceed as previous step did not end properly' % self.applicationName)

    #get the path to the detector model, either local or from the software
    compactFile = None
    if self.detectorModel:
      resXML = self._getDetectorXML()
      if not resXML['OK']:
        LOG.error("Could not obtain the detector XML file: ", resXML["Message"])
        return resXML
      compactFile = resXML['Value']

    res = getEnvironmentScript(self.platform, "marlin", self.applicationVersion, self.getEnvScript)
    if not res['OK']:
      LOG.error("Failed to get the env script")
      return res
    env_script_path = res["Value"]

    res = resolveIFpaths(self.InputData)
    if not res['OK']:
      LOG.error("Failed to resolve path to input slcio files: %s" % res)
      return res
    listofslcio = res['Value']
    LOG.info('SASHA resolved input data: %s' % listofslcio)

    steeringfiledirname = ''
    res = getSteeringFileDirName(self.platform, "marlin", self.applicationVersion)
    if res['OK']:
      steeringfiledirname = res['Value']
    else:
      LOG.warn('Could not find the steering file directory', res['Message'])

    ##Handle PandoraSettings.xml
    # TODO directory below is detector dependent... implement it
    pandorasettings = 'PandoraSettings/PandoraSettings.xml'
    if not os.path.exists(pandorasettings):
      if steeringfiledirname and os.path.exists(os.path.join(steeringfiledirname, pandorasettings)):
        try:
          shutil.copy(os.path.join(steeringfiledirname, pandorasettings),
                      os.path.join(os.getcwd(), pandorasettings))
        except EnvironmentError, x:
          LOG.warn('Could not copy PandoraSettings.xml, exception: %s' % x)

    ##Handle PandoraSettingsPhotonTraining.xml which is used for photon training stage
    pandorasettings = 'CalibrationPandoraSettings/PandoraSettingsPhotonTraining.xml'
    if not os.path.exists(pandorasettings):
      # FIXME is this path wrong?
      photontrainingfiledirname = os.path.join(steeringfiledirname, '../CalibrationPandoraSettings/')
      if photontrainingfiledirname and os.path.exists(os.path.join(photontrainingfiledirname, pandorasettings)):
        try:
          fullPathPandoraSettings = os.path.join(os.getcwd(), pandorasettings)
          shutil.copy(os.path.join(photontrainingfiledirname, pandorasettings),
                      fullPathPandoraSettings)
        except EnvironmentError, x:
          LOG.warn('Could not copy and prepare PandoraSettingsPhotonTraining.xml, exception: %s' % x)
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
      LOG.error("Steering file not defined, this shouldn't happen!")
      return S_ERROR("Could not find steering file")
    if not os.path.exists(self.SteeringFile):
      LOG.error("Steering is not found!")
      return S_ERROR("Could not find steering file")
    LOG.info("Steering file: %s" % self.SteeringFile)

    # check if steering file contains PfoAnalysis processor needed for calibration
    if readValueFromSteeringFile(self.SteeringFile, "processor[@type='PfoAnalysis']") is None:
      self.addPfoAnalysisProcessor(self.SteeringFile)

    res = self.prepareMARLIN_DLL(env_script_path)
    if not res['OK']:
      LOG.error('Failed building MARLIN_DLL:', res['Message'])
      self.setApplicationStatus('Failed to setup MARLIN_DLL')
      return S_ERROR('Something wrong with software installation')
    marlin_dll = res["Value"]

    while True:
      calibrationParameters = self.cali.requestNewParameters()
      while calibrationParameters is None:
        LOG.notice("Waiting for new parameters set")
        wasteCPUCycles(10)
        calibrationParameters = self.cali.requestNewParameters()

      if calibrationParameters['calibrationIsFinished']:
        LOG.notice("Calibration finished")
        break

      #  print('\nstepID: %s' % self.currentStep)
      #  print(calibrationParameters)


      self.currentPhase = calibrationParameters['currentPhase']
      self.currentStage = calibrationParameters['currentStage']
      self.currentStep = self.currentStep + 1
      parameterDict = calibrationParameters['parameters']

      print('DEBUG_CALIB: parameterDict BEFORE resolve: %s' % parameterDict)
      res = self.resolveInputSlcioFilesAndAddToParameterDict(listofslcio, parameterDict)
      if res['OK']:
        parameterDict = res['Value']
      else:
        LOG.error('Problem while executing resolveInputSlcioFilesAndAddToParameterDict: %s' % res['Message'])
        return res
      print('DEBUG_CALIB: parameterDict AFTER resolve: %s' % parameterDict)

      steeringFileToRun = 'marlinSteeringFile_%s_%s_%s.xml' % (self.currentStage, self.currentPhase, self.currentStep)
      res = updateSteeringFile(self.SteeringFile, steeringFileToRun, parameterDict)
      if not res['OK']:
        LOG.error('Error while updateing steering file. Error message: %s' % res['Message'])
        return res

      if self.currentStage == 3 and not os.path.exists('newPhotonLikelihood.xml'):
        newPhotonLikelihood = self.cali.requestNewPhotonLikelihood()
        while newPhotonLikelihood is None:
          LOG.notice("Waiting for new photon likelihood file for stage 3")
          wasteCPUCycles(10)
          newPhotonLikelihood = self.cali.requestNewPhotonLikelihood()
        stringToBinaryFile(newPhotonLikelihood, 'newPhotonLikelihood.xml')
        # TODO this depends from the name inside steering file... And it's even more difficult for FCCee case
        pandoraSettingsFile = readValueFromSteeringFile(
            self.SteeringFile, "processor[@name='MyDDMarlinPandora']/parameter[@name='PandoraSettingsXmlFile']")
        updateSteeringFile(
            pandoraSettingsFile, pandoraSettingsFile,
            {"processor[@name='MyDDMarlinPandora']/parameter[@name='PandoraSettingsXmlFile']":
             "newPhotonLikelihood.xml"})

      #TODO clean up Marlin steering file - we don't need a lot of processors for calibration
      LOG.notice("new set of calibration parameters: %r" % parameterDict)

      self.result = self.runScript(steeringFileToRun, env_script_path, marlin_dll)
      if not self.result['OK']:
        LOG.error('Something wrong during running:', self.result['Message'])
        self.setApplicationStatus('Error during running %s' % self.applicationName)
        return S_ERROR('Failed to run %s' % self.applicationName)

      #FIXME make sure that runScript function return tuple of the same format as used below
      #self.result = {'OK':True,'Value':(0,'Disabled Execution','')}
      resultTuple = self.result['Value']
      if not os.path.exists(self.applicationLog):
        LOG.error("Something went terribly wrong, the log file is not present")
        self.setApplicationStatus('%s failed terribly, you are doomed!' % (self.applicationName))
        if not self.ignoreapperrors:
          return S_ERROR('%s did not produce the expected log' % (self.applicationName))

      # FIXME is result tuple correspond to the status?
      status = resultTuple
      # stdOutput = resultTuple[1]
      # stdError = resultTuple[2]
      LOG.info("Status after the application execution is:", str(status))

      outFile = "pfoAnalysis.root"
      if self.currentStage == 2:
        outFile = 'PandoraLikelihoodDataPhotonTraining.xml'

      self.cali.reportResult(outFile)

    # TODO implement me
    #  return self.finalStatusReport(status)
    return S_OK()

  def addPfoAnalysisProcessor(self, mainSteeringMarlinRecoFile):
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
      with open('test_' + mainSteeringMarlinRecoFile, "w") as of:
        of.write(root_str)

    return S_OK()

  def resolveInputSlcioFilesAndAddToParameterDict(self, allSlcioFiles, parameterDict):
    """ Add PandoraSettings-file and input slcio files which corresponds to current currentStage and currentPhase to the parameterDict

    :param list basestring allSlcioFiles: List of all slcio-files in the node
    :param dict parameterDict: dict of parameters and their values

    :returns: S_OK or S_ERROR
    :rtype: dict
    """
    #FIXME TODO implementation assumes that slcio file names should containt a specific word among: ['muon','kaon','gamma','zuds']
    #           this require adding functionality of renaming of the input files at some point

    patternToSearchFor = ''
    pandoraSettingsFile = ''
    patternToSearchFor = CalibrationPhase.fileKeyFromPhase(self.currentPhase).lower()
    if self.currentStage in [1, 3]:  # FIXME hardcoded values are bad...
      pandoraSettingsFile = 'PandoraSettings/PandoraSettingsDefault.xml'
    else:
      pandoraSettingsFile = 'CalibrationPandoraSettings/PandoraSettingsPhotonTraining.xml'

    # FIXME I don't need to search for any pattern here. I already preselect files when submit jobs!!!
    filesToRunOn = [x for x in allSlcioFiles if patternToSearchFor in x.lower()]
    if len(filesToRunOn) == 0:
      errorMessage = 'Empty list of or incorrectly named input slcio-files. Search pattern: <%s>. nInputFile: %d' % (
          patternToSearchFor, len(allSlcioFiles))
      LOG.error(errorMessage)
      return S_ERROR(errorMessage)

    parameterDict["global/parameter[@name='LCIOInputFiles']"] = ' '.join(filesToRunOn)
    parameterDict["processor[@name='MyDDMarlinPandora']/parameter[@name='PandoraSettingsXmlFile']"] = pandoraSettingsFile

    #TODO should one use different steering file for photon training? if no one need to append line below during all steps
    if self.currentStage in [1, 3]: 
      parameterDict["processor[@name='MyPfoAnalysis']/parameter[@name='RootFile']"] = 'pfoAnalysis.root'
    return S_OK(parameterDict)

  def runScript(self, marlinSteeringFile, env_script_path, marlin_dll):
    """ Actual bit of code running Marlin and PandoraAnalysis.

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
    self.setApplicationStatus('%s %s step %s' % (self.applicationName, self.applicationVersion, self.currentStep))
    self.stdError = ''
    res = shellCall(0, comm, callbackFunction=self.redirectLogOutput, bufferLimit=20971520)
    return res

  def _prepareRunScript(self, marlinSteeringFile, env_script_path, marlin_dll):
    """ Returns the current parameters

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
    #We need to make sure the PandoraSettings is in the current directory
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
      LOG.error("Steering file missing: %s" % (marlinSteeringFile))
      return S_ERROR("SteeringFile is missing: %s" % (marlinSteeringFile))
    #check
    script.write('Marlin -c %s %s\n' % (marlinSteeringFile, self.extraCLIarguments))
    #real run
    script.write('Marlin %s %s\n' % (marlinSteeringFile, self.extraCLIarguments))
    script.write('declare -x appstatus=$?\n')
    script.write('exit $appstatus\n')
    script.close()
    return S_OK(scriptName)
