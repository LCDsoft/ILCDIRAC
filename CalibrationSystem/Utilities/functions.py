""" functions from the calibration system """

import re
import os
import fnmatch
import tempfile
import pickle
from xml.etree import ElementTree as et

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.Subprocess import shellCall

LOG = gLogger.getSubLogger(__name__)

#pylint: disable=invalid-name,too-many-locals,too-many-statements


def printSet(inSet):
  if isinstance(inSet, set):
    return re.split('\(|\)', str(inSet))[1]
  else:
    return ''


def xml_generate(baseFileName='CLIC_PfoAnalysis_AAAA_SN_BBBB.xml',
                 workerID=0,
                 inputFile=None,
                 *args
                 ):
  """ replace parameters in XML steering file used for calibration
  run on a single XML file, as now used in each job

  """
  print "Shall run Xml_Generate.py with these arguments of length", len(args)

  Energy = args[1]

  Particle = args[2]

  ECALTOEM = args[3]
  HCALTOEM = args[4]
  ECALTOHAD = args[5]
  HCALTOHAD = args[6]

  CALIBR_ECAL_INPUT = args[7]
  CALIBR_ECAL_INPUT2 = 2 * float(CALIBR_ECAL_INPUT)

  CALIBR_ECAL = str(CALIBR_ECAL_INPUT) + ' ' + str(CALIBR_ECAL_INPUT2)
  #Of the form: <parameter name="CalibrECAL" type="FloatVec">42.91 85.82 </parameter>

  CALIBR_HCAL_BARREL = args[8]
  CALIBR_HCAL_ENDCAP = args[9]
  CALIBR_HCAL_OTHER = args[10]

  MHHHE = args[11]

  ##Slcio_Path = args[12]

  #Slcio_Format = args[13]

  Gear_File_And_Path = args[14]

  Pandora_Settings_File = args[15]

  ECalGeVToMIP = args[16]
  HCalGeVToMIP = args[17]
  MuonGeVToMIP = args[18]

  HCALBarrelTimeWindowMax = args[19]
  HCALEndcapTimeWindowMax = args[20]
  ECALBarrelTimeWindowMax = args[21]
  ECALEndcapTimeWindowMax = args[22]
  dd4hep_compactxml_File = args[23]

  #===========================
  # Slcio_Format = re.sub('ENERGY',Energy,Slcio_Format)
  # Slcio_Format = re.sub('PARTICLE',Particle,Slcio_Format)

  job_name = Energy + '_GeV_Energy_' + Particle

  ECAL_MIP_THRESHOLD = '0.3'
  HCAL_MIP_THRESHOLD = '0.5'

  Marlin_Path = os.getcwd()

  with open(baseFileName, 'r') as base:
    newContent = base.read()

  baseFileName = re.sub('AAAA', job_name, baseFileName)

  SN = workerID  # workerID ??

  #print SN
  newContent = re.sub('ECALTOEM_XXXX', ECALTOEM, newContent)
  newContent = re.sub('HCALTOEM_XXXX', HCALTOEM, newContent)
  newContent = re.sub('ECALTOHAD_XXXX', ECALTOHAD, newContent)
  newContent = re.sub('HCALTOHAD_XXXX', HCALTOHAD, newContent)
  newContent = re.sub('MHHHE_XXXX', MHHHE, newContent)
  newContent = re.sub('slcio_XXXX', inputFile, newContent)
  newContent = re.sub('Gear_XXXX', Gear_File_And_Path, newContent)
  newContent = re.sub('CALIBR_ECAL_XXXX', CALIBR_ECAL, newContent)
  newContent = re.sub('CALIBR_HCAL_BARREL_XXXX', CALIBR_HCAL_BARREL, newContent)
  newContent = re.sub('CALIBR_HCAL_ENDCAP_XXXX', CALIBR_HCAL_ENDCAP, newContent)
  newContent = re.sub('CALIBR_HCAL_OTHER_XXXX', CALIBR_HCAL_OTHER, newContent)
  newContent = re.sub('EMT_XXXX', ECAL_MIP_THRESHOLD, newContent)
  newContent = re.sub('HMT_XXXX', HCAL_MIP_THRESHOLD, newContent)
  newContent = re.sub('ECalGeVToMIP_XXXX', ECalGeVToMIP, newContent)
  newContent = re.sub('HCalGeVToMIP_XXXX', HCalGeVToMIP, newContent)
  newContent = re.sub('MuonGeVToMIP_XXXX', MuonGeVToMIP, newContent)
  newContent = re.sub('HCALBarrelTimeWindowMax_XXXX', HCALBarrelTimeWindowMax, newContent)
  newContent = re.sub('HCALEndcapTimeWindowMax_XXXX', HCALEndcapTimeWindowMax, newContent)
  newContent = re.sub('ECALBarrelTimeWindowMax_XXXX', ECALBarrelTimeWindowMax, newContent)
  newContent = re.sub('ECALEndcapTimeWindowMax_XXXX', ECALEndcapTimeWindowMax, newContent)

  newContent = re.sub('PSF_XXXX', Pandora_Settings_File, newContent)
  newContent = re.sub('COMPACTXML_XXXX', dd4hep_compactxml_File, newContent)

  newContent = re.sub('pfoAnalysis_XXXX.root ', 'pfoAnalysis_' + job_name + '_SN_' + SN + '.root', newContent)

  newFileName = re.sub('BBBB', str(SN), baseFileName)

  Fullpath = os.path.join(Marlin_Path, newFileName)

  with open(Fullpath, 'w') as newXML:
    newXML.write(newContent)

  return newFileName


def validateExistenceOfWordsInFile(inFile, words):
  """ TODO

  """
  with open(inFile) as iFile:
    for iWord in words:
      if iWord not in iFile:
        return S_ERROR('Word %s is not found in template file: %s' % (iWord, inFile))
  return S_OK()


def validateMarlinRecoTemplateFile(marlinRecoTemplateFileName):
  """ TODO

  """
  parametersToValidate = ['ECALTOEM_XXXX', 'HCALTOEM_XXXX', 'ECALTOHAD_XXXX', 'HCALTOHAD_XXXX', 'MHHHE_XXXX',
                          'slcio_XXXX', 'Gear_XXXX', 'CALIBR_ECAL_XXXX', 'CALIBR_HCAL_BARREL_XXXX',
                          'CALIBR_HCAL_ENDCAP_XXXX', 'CALIBR_HCAL_OTHER_XXXX', 'EMT_XXXX', 'HMT_XXXX',
                          'ECalGeVToMIP_XXXX', 'HCalGeVToMIP_XXXX']
  return validateExistenceOfWordsInFile(marlinRecoTemplateFileName, parametersToValidate)


def validatePandoraSettingsFile(pandoraSettingsFileName):
  """ TODO

  """
  parametersToValidate = ['PANDORALIKELIHOOD_XXXX']
  return validateExistenceOfWordsInFile(pandoraSettingsFileName, parametersToValidate)


def updateSteeringFile(inFileName, outFileName, parametersToSetup, exceptions=None):
  """ Read input xml-file, update values given be dictionary and write result to a new file

  :param basestring inFileName: name of input xml-file
  :param basestring outFileName: name of output xml-file
  :param dict parametersToSetup: dict which contains values which have to be updated. Keys of dictionary are XPath-string. E.g.: {"processor/[@name='OuterPlanarDigiProcessor']/parameter[@name='IsStrip']": True}

  :returns: S_OK or S_ERROR
  :rtype: dict
    """
  if exceptions is None:
    exceptions = []
  tree = et.parse(inFileName)

  #FIXME redirect log messegage to LOG class?
  LOG.info("Updating following values for %s:" % outFileName)
  for iPar, iVal in parametersToSetup.items():
    skipParameter = False
    for iEx in exceptions:
      if iEx in iPar:
        skipParameter = True
    if skipParameter:
      continue
    iElement = tree.find(iPar)
    if iElement is None or iVal is None:
      errMsg = ("Cannot update parameter in the steering file! Parameter: %s; Value: %s; inFileName: %s; outFileName: %s"
                % (iPar, iVal, inFileName, outFileName))
      LOG.error(errMsg)
      return S_ERROR(errMsg)
    else:
      if isinstance(iVal, (float, int)):
        iVal = str(iVal)

      if iElement.text is None:
        if not iVal in iElement.get('value'):
          LOG.info('%s:\t"%s" --> "%s"' % (iPar, iElement.get('value'), iVal))
      else:
        if not iVal in iElement.text:
          LOG.info('%s:\t"%s" --> "%s"' % (iPar, iElement.text, iVal))
      # remove value attribute since we write value to the text field
      if 'value' in iElement.attrib:
        del iElement.attrib['value']
      iElement.text = iVal

  tree.write(outFileName)
  return S_OK()

def readValueFromSteeringFile(fileName, xPath):
  """ Read value of the node from xml-file

  :param basestring fileName: name of xml-file to read
  :param basestring xPath: xParh of the node to read. E.g.: "processor/[@name='OuterPlanarDigiProcessor']/parameter[@name='IsStrip']"

  :returns: basestring or None
  :rtype: basestring 
    """
  tree = et.parse(fileName)
  iElement = tree.find(xPath)
  if iElement is not None:
    if iElement.text is None:
      return iElement.get('value')
    else:
      return iElement.text
  else:
    return None

#  def readParameterDict(inFile='testing/parameterListMarlinSteeringFile.txt'):


def readParameterDict(inFile='DEFAULT_VALUE'):
  if inFile == 'DEFAULT_VALUE':
    import ILCDIRAC.CalibrationSystem.Utilities as utilities
    inFile = os.path.join(utilities.__path__[0], 'testing/parameterListMarlinSteeringFile.txt')
  outList = {}
  with open(inFile, 'r') as f:
    for iLine in f:
      outList[iLine.split('\n')[0]] = None
  return outList


def readParametersFromSteeringFile(inFileName, parameterDict, exceptions=None):
  if exceptions is None:
    exceptions = []
  tree = et.parse(inFileName)

  for iPar in parameterDict.keys():
    skipParameter = False
    for iEx in exceptions:
      if iEx in iPar:
        skipParameter = True
    if skipParameter:
      continue
    iElement = tree.find(str(iPar))
    if iElement is None:
      return S_ERROR("Cannot read parameter from the steering file! Parameter: %s; inFileName: %s" % (iPar, inFileName))
    else:
      if iElement.text is None:
        parameterDict[iPar] = iElement.get('value')
      else:
        parameterDict[iPar] = iElement.text

  return S_OK()

def testUpdateOfSteeringFileWithNewParameters():
  inFileName = 'testing/in1.xml'


  parDict = readParameterDict()
  print parDict
  currentParameters = readParametersFromSteeringFile(inFileName, parDict)
  print currentParameters

  outFileName = 'testing/out1.xml'
  res = updateSteeringFile(inFileName, outFileName, {
                           "processor[@name='MyPfoAnalysis']/parameter[@name='RootFile']": "dummyRootFile.root", "global/parameter[@name='LCIOInputFiles']": "in1.slcio, in2.slcio"})
  print res

  res = updateSteeringFile(inFileName, outFileName, {"processor[@name='MyPfoAnalysis']/parameter[@name='RootFile']": "dummyRootFile.root",
                                                     "global/parameter[@name='LCIOInputFiles']": "in1.slcio, in2.slcio", "processor[@name='MyPfoAnalysis2']/parameter[@name='RootFile']": "wrong.root"})
  print res


def convert_and_execute(command_list, fileToSource=''):
  """ Takes a list, casts every entry of said list to string and executes it in a subprocess.

  :param list command_list: List for a subprocess to execute, that may contain castable non-strings
  :param basestring fileToSource: file which will be sourced before running command
  :returns: S_OK or S_ERROR
  :rtype: S_OK or S_ERROR
  """
  callString = ''
  for iWord in command_list:
    callString += str(iWord)
    callString += ' '
  callString += '\n'

  tmpFile = tempfile.NamedTemporaryFile(delete=False)
  if fileToSource != '':
    tmpFile.write("source %s\n" % fileToSource)
  tmpFile.write(callString)
  tmpFile.close()

  os.chmod(tmpFile.name, 0755)
  comm = 'sh -c "%s"' % (tmpFile.name)
  res = shellCall(0, comm)
  os.unlink(tmpFile.name)
  return res


def searchFilesWithPattern(dirName, filePattern):
  matches = []
  for root, _, filenames in os.walk(dirName):
    for filename in fnmatch.filter(filenames, filePattern):
      matches.append(os.path.join(root, filename))
  return matches


def saveCalibrationRun(calibRun):
  fileName = "calib%s/calibRun_bak.pkl" % (calibRun.calibrationID)
  with open(fileName, 'wb') as f:
    pickle.dump(calibRun, f, pickle.HIGHEST_PROTOCOL)


def loadCalibrationRun(calibrationID):
  fileName = "calib%s/calibRun_bak.pkl" % (calibrationID)
  if os.path.exists(fileName):
    with open(fileName, 'rb') as f:
      return pickle.load(f)
  else:
    return None


def addPfoAnalysisProcessor(mainSteeringMarlinRecoFile):
  if not os.path.exists(mainSteeringMarlinRecoFile):
    return S_ERROR("cannot find input steering file: %s" % mainSteeringMarlinRecoFile)

  mainTree = et.ElementTree()
  try:
    mainTree.parse(mainSteeringMarlinRecoFile)
  except et.ParseError as e:
    return S_ERROR("cannot parse input steering file: %s. errMsg: %s" % (mainSteeringMarlinRecoFile, e))
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
    c = et.Element("processor", {"name": "MyPfoAnalysis"})
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


def addParameterToProcessor(mainSteeringMarlinRecoFile, processorName, parameterDict):
  if not os.path.exists(mainSteeringMarlinRecoFile):
    return S_ERROR("cannot find input steering file: %s" % mainSteeringMarlinRecoFile)

  mainTree = et.ElementTree()
  try:
    mainTree.parse(mainSteeringMarlinRecoFile)
  except et.ParseError as e:
    return S_ERROR("cannot parse input steering file: %s. errMsg: %s" % (mainSteeringMarlinRecoFile, e))

  if not 'name' in parameterDict.keys():
    return S_ERROR("parameter dict should have key 'name'")

  mainRoot = mainTree.getroot()

  # each processors is mentioned twixe in the steering file. Once in the execute (just name) and once in the body (with name and type tags).
  procElement = [iEl for iEl in mainRoot.iter('processor') if iEl.attrib['name']
                 == processorName and 'type' in iEl.keys()]
  if len(procElement) > 1:
    return S_ERROR('Multiple processors with given names are found: %s' % procElement)
  elif len(procElement) == 0:
    return S_ERROR("Can't find processor with a name %s in the file %s" % (processorName, mainSteeringMarlinRecoFile))
  else:
    procElement = procElement[0]
  for iSubEl in list(procElement):
    if 'name' in iSubEl.attrib.keys():
      if parameterDict['name'] == iSubEl.attrib['name']:
        return S_ERROR("parameter with name %s already exists in the processor %s" % (parameterDict['name'], processorName))
  procElement.append(et.Element("parameter", parameterDict))

  root_str = et.tostring(mainRoot)
  with open(mainSteeringMarlinRecoFile, "w") as of:
    of.write(root_str)

  return S_OK()


def splitFilesAcrossJobs(inputFiles, nEventsPerFile, nJobs):
  """ Function to regroup inputFiles dict according to number of jobs.
  Output dict will have a format:
  - arguments of dict: [iJob][iFileType]
  - values of dict: tuple (ordered list of files, startFromEventNumber, nEventsToProcess)

  :param inputFiles: Input list of files for the calibration. Dictionary.
  :type inputFiles: `python:dict`
  :param nEventsPerFile: number of events per file
  :type nEventsPerFile: `python:dict`
  :param int nJobs: Number of jobs to run
  :returns: S_OK with 'Value' element being a new regroupped dict or S_ERROR
  :rtype: dict
  """

  tmpDict = {}
  for iKey, iList in inputFiles.iteritems():
    nEventsPerJob = int(len(iList) * nEventsPerFile[iKey] / nJobs)

    newDict = {}
    for i in range(0, nJobs):
      newDict[i] = []
      indexOfFirstEventInJob = nEventsPerJob * i
      indexOfLastEventInJob = nEventsPerJob * (i + 1) - 1
      indexOfFirstFile = int((indexOfFirstEventInJob) / nEventsPerFile[iKey])
      indexOfLastFile = int((indexOfLastEventInJob) / nEventsPerFile[iKey])
      fileListForJob = iList[indexOfFirstFile:indexOfLastFile + 1]
      startFromEventNumber = indexOfFirstEventInJob - indexOfFirstFile * nEventsPerFile[iKey]
      newDict[i] += (fileListForJob, startFromEventNumber, nEventsPerJob)

    tmpDict[iKey] = newDict

  outDict = {}
  for iJob in range(0, nJobs):
    newDict = {}
    for iType in inputFiles.keys():
      newDict[iType] = tmpDict[iType][iJob]
    outDict[iJob] = newDict

  return outDict
