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

from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC import S_OK, S_ERROR, gLogger

from ILCDIRAC.Workflow.Modules.MarlinAnalysis import MarlinAnalysis
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import getSoftwareFolder, getEnvironmentScript
from ILCDIRAC.Core.Utilities.PrepareOptionFiles import prepareXMLFile, getNewLDLibs
from ILCDIRAC.Core.Utilities.PrepareLibs import removeLibc
from ILCDIRAC.Core.Utilities.FindSteeringFileDir import getSteeringFileDirName
from ILCDIRAC.CalibrationSystem.Client.CalibrationClient import CalibrationClient, CalibrationPhase
from ILCDIRAC.CalibrationSystem.Utilities.functions import xml_generate, updateSteeringFile
from ILCDIRAC.Core.Utilities.WasteCPU import wasteCPUCycles


__RCSID__ = "$Id$"


class Calibration(MarlinAnalysis):
  """Define the Marlin analysis part of the workflow
  """

  def __init__(self):
    super(Calibration, self).__init__()
    self.enable = True
    self.stepId = ''
    self.phaseId = ''
    self.iterationNumber = 0
    self.log = gLogger.getSubLogger("Calibration")
    self.result = S_ERROR()
    self.applicationName = "Calibration"
    self.eventstring = ['ProgressHandler', 'event']
    self.envdict = {}
    self.detectorModel = None
    self.calibrationID = 0
    self.workerID = 0
    self.baseSteeringFile = None
    self.cali = CalibrationClient(self.calibrationID, self.workerID)

  def applicationSpecificInputs(self):
    """ Resolve all input variables for the module here.

    :return: S_OK()
    """

    if 'ParametricInputSandbox' in self.workflow_commons:
      paramsb = self.workflow_commons['ParametricInputSandbox']
      if not isinstance(paramsb, list):
        if len(paramsb):
          paramsb = paramsb.split(";")
        else:
          paramsb = []

      self.InputFile += paramsb

    if not len(self.InputFile) and len(self.InputData):
      for files in self.InputData:
        if files.lower().find(".slcio") > -1:
          self.InputFile.append(files)

    return S_OK('Parameters resolved')

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
      self.log.error("Failed to resolve input parameters:", self.result["Message"])
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('%s should not proceed as previous step did not end properly' % self.applicationName)

    #get the path to the detector model, either local or from the software
    compactFile = None
    if self.detectorModel:
      resXML = self._getDetectorXML()
      if not resXML['OK']:
        self.log.error("Could not obtain the detector XML file: ", resXML["Message"])
        return resXML
      compactFile = resXML['Value']

    res = getEnvironmentScript(self.platform, "marlin", self.applicationVersion, self.getEnvScript)
    if not res['OK']:
      self.log.error("Failed to get the env script")
      return res
    env_script_path = res["Value"]

    res = self.GetInputFiles()
    if not res['OK']:
      self.log.error("Failed getting input files:", res['Message'])
      return res
    listofslcio = res['Value']

    steeringfiledirname = ''
    res = getSteeringFileDirName(self.platform, "marlin", self.applicationVersion)
    if res['OK']:
      steeringfiledirname = res['Value']
    else:
      self.log.warn('Could not find the steering file directory', res['Message'])

    ##Handle PandoraSettings.xml
    pandorasettings = 'PandoraSettings.xml'
    if not os.path.exists(pandorasettings):
      if steeringfiledirname and os.path.exists(os.path.join(steeringfiledirname, pandorasettings)):
        try:
          shutil.copy(os.path.join(steeringfiledirname, pandorasettings),
                      os.path.join(os.getcwd(), pandorasettings))
        except EnvironmentError, x:
          self.log.warn('Could not copy PandoraSettings.xml, exception: %s' % x)

    ##Handle PandoraSettingsPhotonTraining.xml for photon training step
    # FIXME to test this part of the code
    pandorasettings = 'PandoraSettingsPhotonTraining.xml'
    photontrainingfiledirname = os.path.join(steeringfiledirname, '../CalibrationPandoraSettings/')
    if not os.path.exists(pandorasettings):
      if photontrainingfiledirname and os.path.exists(os.path.join(photontrainingfiledirname, pandorasettings)):
        try:
          shutil.copy(os.path.join(photontrainingfiledirname, pandorasettings),
                      os.path.join(os.getcwd(), pandorasettings))
        except EnvironmentError, x:
          self.log.warn('Could not copy PandoraSettingsPhotonTraining.xml, exception: %s' % x)

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
    if not self.SteeringFile:
      self.log.error("Steering file not defined, shouldn't happen!")
      return S_ERROR("Could not find steering file")

    eventsPerBackgroundFile = self.workflow_commons.get("OI_eventsPerBackgroundFile", 0)
    self.log.info("Number of Events per BackgroundFile: %d " % eventsPerBackgroundFile)

    res = prepareXMLFile(self.baseSteeringFile, self.SteeringFile, self.inputGEAR, listofslcio,
                         self.NumberOfEvents, self.OutputFile, self.outputREC, self.outputDST,
                         self.debug,
                         dd4hepGeoFile=compactFile,
                         eventsPerBackgroundFile=eventsPerBackgroundFile,
                         )
    if not res['OK']:
      self.log.error('Something went wrong with XML generation because %s' % res['Message'])
      self.setApplicationStatus('Marlin: something went wrong with XML generation')
      return res

    res = self.prepareMARLIN_DLL(env_script_path)
    if not res['OK']:
      self.log.error('Failed building MARLIN_DLL:', res['Message'])
      self.setApplicationStatus('Failed to setup MARLIN_DLL')
      return S_ERROR('Something wrong with software installation')
    marlin_dll = res["Value"]

    # such loop has to be done for each phase
    # each phase will have its own set of input slcio files (listofslcio)
    while True:

      calibrationParameters = self.cali.requestNewParameters()
      while calibrationParameters is None:
        self.log.notice("Waiting for new parameters set")
        wasteCPUCycles(10)
        calibrationParameters = self.cali.requestNewParameters()

      if not calibrationParameters['OK']:
        self.log.notice("Calibration finished")
        break

      self.phaseID = calibrationParameters['phaseID']
      self.stepID = calibrationParameters['stepID']
      self.iterationNumber = self.iterationNumber + 1
      parameterList = calibrationParameters['parameters']
      resolveInputSlcioFilesAndAddToParameterList(self.stepID, self.phaseID, parameterList)

      steeringFileToRun = 'marlinSteeringFile_%s_%s_%s.xml' % (self.stepID, self.phaseID, self.iterationNumber)
      updateSteeringFile(self.SteeringFile, steeringFileToRun, parameterList)
      # TODO additionaly clean up Marlin file - a lot of processors we don't need for calibration
      self.log.notice("new set of calibration parameters: %r" % parameterList)

      finalXML = xml_generate(self.baseSteeringFile, self.workerID, listofslcio, *parameters)

      self.result = self.runScript(finalXML, env_script_path, marlin_dll)
      if not self.result['OK']:
        self.log.error('Something wrong during running:', self.result['Message'])
        self.setApplicationStatus('Error during running %s' % self.applicationName)
        return S_ERROR('Failed to run %s' % self.applicationName)

      #self.result = {'OK':True,'Value':(0,'Disabled Execution','')}
      resultTuple = self.result['Value']
      if not os.path.exists(self.applicationLog):
        self.log.error("Something went terribly wrong, the log file is not present")
        self.setApplicationStatus('%s failed terribly, you are doomed!' % (self.applicationName))
        if not self.ignoreapperrors:
          return S_ERROR('%s did not produce the expected log' % (self.applicationName))

      status = resultTuple[0]
      # stdOutput = resultTuple[1]
      # stdError = resultTuple[2]
      self.log.info("Status after the application execution is:", str(status))

      # FIXME TODO report correct root file or xml-file (in case of PhotonTraining step)
      pfoAnalysisRootFile = "pfoAnalysis.root"

      self.cali.reportResult(pfoAnalysisRootFile)

    return self.finalStatusReport(status)

  def resolveInputSlcioFilesAndAddToParameterList(allSlcioFiles, stepID, phaseID, parameterList):
  """ Add PandoraSettings-file and input slcio files which corresponds to current stepID and phaseID to the parameterList

  :param list basestring allSlcioFiles: List of all slcio-files in the node
  :param int stepID: current stepID
  :param int phaseID: current phaseID
  :param list basestring parameterList: list of parameters and their values

  :returns: S_OK or S_ERROR
  :rtype: dict
  """
  #FIXME TODO implementation assumes that slcio file names should containt a specific word among: ['muon','kaon','gamma','zuds']
  #           this require adding functionality of renaming of the input files at some point

  patternToSearchFor = ''
  pandoraSettingsFile = ''
   if stepID in [1, 3]:  # FIXME hardcoded values are bad...
      patternToSearchFor = fileKeyFromPhase(phaseID).lower()
      pandoraSettingsFile = 'PandoraSettings.xml'
    else:
      patternToSearchFor = 'zuds'
      pandoraSettingsFile = 'PandoraSettingsPhotonTraining.xml'

    filesToRunOn = [x for x in allSlcioFiles if patternToSearchFor in x.lower()]
    if len(filesToRunOn) == 0:
      return s_ERROR('empty list of input slcio-files')

    parameterList.append('global,None,LCIOInputFiles,%s' % (filesToRunOn))
    parameterList.append('processor,MyDDMarlinPandora,PandoraSettingsXmlFile' % (pandoraSettingsFile))
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
      self.log.error("Could not get the MARLIN_DLL env")
      return S_ERROR("Failed getting the MARLIN_DLL")
    marlindll = res["Value"][1].rstrip()
    marlindll = marlindll.rstrip(":")
    try:
      os.remove('temp.sh')
    except EnvironmentError, e:
      self.log.warn("Failed to delete the temp file", str(e))

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
        self.log.verbose("Duplicated lib found, removing %s" % path1)
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
    self.log.verbose("Final MARLIN_DLL is:", marlindll)

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
      self.log.error("Steering file missing: %s" % (marlinSteeringFile))
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
    #      - need to rename output root or xml files


    script.write('declare -x appstatus=$?\n')
    script.write('exit $appstatus\n')
    script.close()
    return S_OK()

  def GetInputFiles(self):
    """ Resolve the input files. But not if in the application definition it was decided
    that it should forget about the input.
    """
    if self.ignoremissingInput:
      return S_OK("")
    res = resolveIFpaths(self.InputFile)
    if not res['OK']:
      self.setApplicationStatus('%s: missing slcio file' % self.applicationName)
      return S_ERROR('Missing slcio file!')
    runonslcio = res['Value']

    listofslcio = " ".join(runonslcio)

    return S_OK(listofslcio)

  def getEnvScript(self, sysconfig, appname, appversion):
    """ Called if CVMFS is not available
    """
    res = getSoftwareFolder(sysconfig, appname, appversion)
    if not res['OK']:
      self.setApplicationStatus('Marlin: Could not find neither local area not shared area install')
      return res

    myMarlinDir = res['Value']

    ##Remove libc
    removeLibc(myMarlinDir + "/LDLibs")

    ##Need to fetch the new LD_LIBRARY_PATH
    new_ld_lib_path = getNewLDLibs(sysconfig, "marlin", appversion)

    marlindll = ""
    if os.path.exists("%s/MARLIN_DLL" % myMarlinDir):
      for library in os.listdir("%s/MARLIN_DLL" % myMarlinDir):
        marlindll = marlindll + "%s/MARLIN_DLL/%s" % (myMarlinDir, library) + ":"
      marlindll = "%s" % (marlindll)
    else:
      self.log.error('MARLIN_DLL folder not found, cannot proceed')
      return S_ERROR('MARLIN_DLL folder not found in %s' % myMarlinDir)

    env_script_name = "MarlinEnv.sh"
    script = open(env_script_name, "w")
    script.write("#!/bin/sh\n")
    script.write('##########################################################\n')
    script.write('# Dynamically generated script to create env for Marlin. #\n')
    script.write('##########################################################\n')
    script.write("declare -x PATH=%s/Executable:$PATH\n" % myMarlinDir)
    script.write('declare -x ROOTSYS=%s/ROOT\n' % (myMarlinDir))
    script.write('declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:%s/LDLibs:%s\n' % (myMarlinDir, new_ld_lib_path))
    script.write("declare -x MARLIN_DLL=%s\n" % marlindll)
    # FIXME line below will be incorrect at PhotonTraining step
    script.write("declare -x PANDORASETTINGS=%s/Settings/PandoraSettings.xml" % myMarlinDir)
    script.close()
    #os.chmod(env_script_name, 0755)
    return S_OK(os.path.abspath(env_script_name))
