'''
Run Marlin

ILCDIRAC.Workflow.Modules.MarlinAnalysis Called by Job Agent.

:since: Feb 9, 2010

:author: Stephane Poss
:author: Przemyslaw Majewski
'''

import glob
import os
import shutil

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
from ILCDIRAC.CalibrationSystem.Utilities.fileutils import stringToBinaryFile

__RCSID__ = '$Id$'
LOG = gLogger.getSubLogger(__name__)

class Calibration(MarlinAnalysis):
  """Define the Calibration part of the workflow
  """

  def __init__(self, _calibrationID, _workerID):
    super(Calibration, self).__init__()
    self.applicationName = "Calibration"
    self.currentStep = -1  # internal counter of worker node of how much times Marlin was run
    self.currentPhase = None
    self.currentStage = None
    self.cali = CalibrationClient(_calibrationID, _workerID)

  def runIt(self):
    """
    Called by Agent
    
    Execute the following:
      - resolve where the soft was installed
      - prepare the list of file to feed Marlin with
      - create the XML file on which Marlin has to run, done by :any:`prepareXMLFile`
      - run Marlin and catch the exit code

    :return: S_OK(), S_ERROR()
    """
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

    res = self._getInputFiles()
    if not res['OK']:
      LOG.error("Failed getting input files:", res['Message'])
      return res
    listofslcio = res['Value']

    steeringfiledirname = ''
    res = getSteeringFileDirName(self.platform, "marlin", self.applicationVersion)
    if res['OK']:
      steeringfiledirname = res['Value']
    else:
      LOG.warn('Could not find the steering file directory', res['Message'])

    ##Handle PandoraSettings.xml
    pandorasettings = 'PandoraSettings.xml'
    if not os.path.exists(pandorasettings):
      if steeringfiledirname and os.path.exists(os.path.join(steeringfiledirname, pandorasettings)):
        try:
          shutil.copy(os.path.join(steeringfiledirname, pandorasettings),
                      os.path.join(os.getcwd(), pandorasettings))
        except EnvironmentError, x:
          LOG.warn('Could not copy PandoraSettings.xml, exception: %s' % x)

    ##Handle PandoraSettingsPhotonTraining.xml which is used for photon training stage
    pandorasettings = 'PandoraSettingsPhotonTraining.xml'
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
    tree = et.parse(pandorasettings)
    tree.find("algorithm[@type='PhotonReconstruction']/HistogramFile").text = 'PandoraLikelihoodDataPhotonTraining.xml'
    tree.write(fullPathPandoraSettings)

    if self.inputGEAR:
      self.inputGEAR = os.path.basename(self.inputGEAR)
      if self.inputGEAR and not os.path.exists(self.inputGEAR) and steeringfiledirname \
         and os.path.exists(os.path.join(steeringfiledirname, self.inputGEAR)):
        self.inputGEAR = os.path.join(steeringfiledirname, self.inputGEAR)

    self.SteeringFile = os.path.basename(self.SteeringFile)
    if not os.path.exists(self.SteeringFile):
      if steeringfiledirname:
        if os.path.exists(os.path.join(steeringfiledirname, self.SteeringFile)):
          self.SteeringFile = os.path.join(steeringfiledirname, self.SteeringFile)
          # default steering file doesn't have PfoAnalysis processor
          self.addPfoAnalysisProcessor(self.SteeringFile)
    if not self.SteeringFile:
      LOG.error("Steering file not defined, shouldn't happen!")
      return S_ERROR("Could not find steering file")

    #  eventsPerBackgroundFile=self.workflow_commons.get("OI_eventsPerBackgroundFile", 0)
    #  LOG.info( "Number of Events per BackgroundFile: %d " % eventsPerBackgroundFile )
    #
    #  res = prepareXMLFile(self.baseSteeringFile, self.SteeringFile, self.inputGEAR, listofslcio,
    #                       self.NumberOfEvents, self.OutputFile, self.outputREC, self.outputDST,
    #                       self.debug,
    #                       dd4hepGeoFile=compactFile,
    #                       eventsPerBackgroundFile=eventsPerBackgroundFile,
    #                      )
    #  if not res['OK']:
    #    LOG.error('Something went wrong with XML generation because %s' % res['Message'])
    #    self.setApplicationStatus('Marlin: something went wrong with XML generation')
    #    return res

    res = self.prepareMARLIN_DLL(env_script_path)
    if not res['OK']:
      LOG.error('Failed building MARLIN_DLL:', res['Message'])
      self.setApplicationStatus('Failed to setup MARLIN_DLL')
      return S_ERROR('Something wrong with software installation')
    marlin_dll = res["Value"]

    while True:
      calibrationParameters = defaultdict()
      if self.currentStep == -1:
        calibrationParameters['currentPhase'] = CalibrationPhase.ECalDigi
        calibrationParameters['currentStage'] = 1
        # TODO copy this file to workdir first
        parListFileName = 'parameterListMarlinSteeringFile.txt'
        parDict = readParameterDict(parListFileName)
        readParametersFromSteeringFile(self.SteeringFile, parDict)
        calibrationParameters['parameters'] = parDict
      else:
        calibrationParameters = self.cali.requestNewParameters()
      while calibrationParameters is None:
        LOG.notice("Waiting for new parameters set")
        wasteCPUCycles(10)
        calibrationParameters = self.cali.requestNewParameters()

      if calibrationParameters['OK']:  # dict will contain element with key 'OK' only when calibration is finishd
        LOG.notice("Calibration finished")
        break

      self.currentPhase = calibrationParameters['currentPhase']
      self.currentStage = calibrationParameters['currentStage']
      self.currentStep = self.currentStep + 1
      parameterDict = calibrationParameters['parameters']
      resolveInputSlcioFilesAndAddToParameterDict(listofslcio, parameterDict)

      steeringFileToRun = 'marlinSteeringFile_%s_%s_%s.xml' % (self.currentStage, self.currentPhase, self.currentStep)
      updateSteeringFile(self.SteeringFile, steeringFileToRun, parameterDict)

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

      status = resultTuple[0]
      # stdOutput = resultTuple[1]
      # stdError = resultTuple[2]
      LOG.info("Status after the application execution is:", str(status))

      outFile = "pfoAnalysis.root"
      if self.currentStage == 2:
        outFile = 'PandoraLikelihoodDataPhotonTraining.xml'

      self.cali.reportResult(outFile)

    return self.finalStatusReport(status)

  def addPfoAnalysisProcessor(self, mainSteeringMarlinRecoFile):
    mainTree = et.parse(mainSteeringMarlinRecoFile)
    mainRoot = mainTree.getroot()

    #FIXME TODO properly find path to the file
    # this file should contain only PfoAnalysis processor
    pfoAnalysisProcessorFile = '/afs/cern.ch/user/v/viazlo/pyDevs/dirac_developerGuide/ILCDIRAC/CalibrationSystem/Utilities/testing/pfoAnalysis.xml'
    tmpTree = et.parse(pfoAnalysisProcessorFile)
    elementToAdd = tmpTree.getroot()

    if 'MyPfoAnalysis' not in (iEl.attrib['name'] for iEl in mainRoot.iter('processor')):
      tmp1 = mainRoot.find('execute')
      c = et.Element("processor name=\"MyPfoAnalysis\"")
      tmp1.append(c)
      mainRoot.append(elementToAdd)
      mainTree.write(mainSteeringMarlinRecoFile)

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
  patternToSearchFor = fileKeyFromPhase(self.currentPhase).lower()
  if self.currentStage in [1, 3]:  # FIXME hardcoded values are bad...
      pandoraSettingsFile = 'PandoraSettings.xml'
    else:
      pandoraSettingsFile = 'PandoraSettingsPhotonTraining.xml'

    filesToRunOn = [x for x in allSlcioFiles if patternToSearchFor in x.lower()]
    if len(filesToRunOn) == 0:
      return s_ERROR('empty list of input slcio-files')

    parameterDict["global/parameter[@name='LCIOInputFiles']"] = ', '.join(filesToRunOn)
    parameterDict["processor[@name='MyDDMarlinPandora']/parameter[@name='PandoraSettingsXmlFile']"] = pandoraSettingsFile

    #TODO should one use different steering file for photon training? if no one need to append line below during all steps
    if self.currentStage in [1, 3]: 
      parameterDict["processor[@name='MyPfoAnalysis']/parameter[@name='RootFile']"] = 'pfoAnalysis.root'
    return S_OK()

  def prepareMARLIN_DLL(self, env_script_path):
    """ Prepare the run time environment: MARLIN_DLL in particular.
    """
    #to fix the MARLIN_DLL, we need to get it first
    with open("temp.sh", 'w') as script:
      script.write("#!/bin/bash\n")
      lines = []
      lines.append("source %s" % env_script_path)
      lines.append('echo $MARLIN_DLL')
      script.write("\n".join(lines))
    os.chmod("temp.sh", 0755)
    res = shellCall(0, "./temp.sh")
    if not res['OK']:
      LOG.error("Could not get the MARLIN_DLL env")
      return S_ERROR("Failed getting the MARLIN_DLL")
    marlindll = res["Value"][1].rstrip()
    marlindll = marlindll.rstrip(":")
    try:
      os.remove('temp.sh')
    except EnvironmentError, e:
      LOG.warn("Failed to delete the temp file", str(e))

    if not marlindll:
      return S_ERROR("Empty MARLIN_DLL env variable!")
    #user libs
    userlib = ""

    if os.path.exists("./lib/marlin_dll"):
      for library in glob.glob("./lib/marlin_dll/*.so"):
        userlib = userlib + library + ":"

    userlib = userlib.rstrip(":")

    temp = marlindll.split(':')
    temp2 = userlib.split(":")
    lib1d = {}
    libuser = {}
    for lib in temp:
      lib1d[os.path.basename(lib)] = lib
    for lib in temp2:
      libuser[os.path.basename(lib)] = lib

    for lib1, path1 in lib1d.items():
      if lib1 in libuser:
        LOG.verbose("Duplicated lib found, removing %s" % path1)
        try:
          temp.remove(path1)
        except ValueError:
          pass

    marlindll = "%s:%s" % (":".join(temp), userlib)  # Here we concatenate the default MarlinDLL with the user's stuff
    finallist = []
    items = marlindll.split(":")
    #Care for user defined list of processors, useful when someone does not want to run the full reco
    if len(self.ProcessorListToUse):
      for processor in self.ProcessorListToUse:
        for item in items:
          if item.count(processor):
            finallist.append(item)
    else:
      finallist = items
    items = finallist
    #Care for user defined excluded list of processors, useful when someone does not want to run the full reco
    if len(self.ProcessorListToExclude):
      for item in items:
        for processor in self.ProcessorListToExclude:
          if item.count(processor):
            finallist.remove(item)
    else:
      finallist = items

    ## LCFIPlus links with LCFIVertex, LCFIVertex needs to go first in the MARLIN_DLL
    plusPos = 0
    lcfiPos = 0
    for position, lib in enumerate(finallist):
      if 'libLCFIPlus' in lib:
        plusPos = position
      if 'libLCFIVertex' in lib:
        lcfiPos = position
    if plusPos < lcfiPos:  # if lcfiplus is before lcfivertex
      #swap the two entries
      finallist[plusPos], finallist[lcfiPos] = finallist[lcfiPos], finallist[plusPos]

    marlindll = ":".join(finallist)
    LOG.verbose("Final MARLIN_DLL is:", marlindll)
    
    return S_OK(marlindll)

  def runScript(self, marlinSteeringFile, env_script_path, marlin_dll):
    """ Actual bit of code running Marlin and PandoraAnalysis.

    :param marlinSteeringFile: steering file to use for Marlin reconstruction. E.g.: 'fccReconstruction.xml'
    :param string env_script_path: path to the setup environment scripts
    :param string marlin_dll: string containing path to marlin libraries

    :returns: FIXME S_OK or S_ERROR
    :rtype: dict
    """
    res = self._prepareMarlinPartOfTheScript(marlinSteeringFile, env_script_path, marlin_dll)
    if not res['OK']:
      return res
    res = self._preparePandoraPartOfTheScript()
    if not res['OK']:
      return res

    scriptName = '%s_%s_Run_%s.sh' % (self.applicationName, self.applicationVersion, self.STEP_NUMBER)

    if os.path.exists(self.applicationLog):
      os.remove(self.applicationLog)

    os.chmod(scriptName, 0755)
    comm = 'sh -c "./%s"' % (scriptName)
    self.setApplicationStatus('%s %s step %s' % (self.applicationName, self.applicationVersion, self.STEP_NUMBER))
    self.stdError = ''
    res = shellCall(0, comm, callbackFunction=self.redirectLogOutput, bufferLimit=20971520)
    return res

  def _prepareMarlinPartOfTheScript(self, marlinSteeringFile, env_script_path, marlin_dll):
    """ Returns the current parameters

    :param marlinSteeringFile: steering file to use for Marlin reconstruction. E.g.: 'fccReconstruction.xml'
    :param string env_script_path: path to the setup environment scripts
    :param string marlin_dll: string containing path to marlin libraries

    :returns: S_OK or S_ERROR
    :rtype: dict
    """
    scriptName = '%s_%s_Run_%s.sh' % (self.applicationName, self.applicationVersion, self.STEP_NUMBER)
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
    script.close()
    return S_OK()

  def _preparePandoraPartOfTheScript(self):
    scriptName = '%s_%s_Run_%s.sh' % (self.applicationName, self.applicationVersion, self.STEP_NUMBER)
    script = open(scriptName, 'a')

    # TODO implement Pandora related stuff
    if self.currentStage in [1,3]:
      pass

    # TODO rename output root or xml file

    script.write('declare -x appstatus=$?\n')
    script.write('exit $appstatus\n')
    script.close()
    return S_OK()
