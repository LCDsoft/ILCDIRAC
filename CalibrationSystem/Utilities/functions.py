"""Helper functions for calibration system."""

import re
import os
import fnmatch
import tempfile
import pickle
from xml.etree import ElementTree as et

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.Subprocess import shellCall

LOG = gLogger.getSubLogger(__name__)

# pylint: disable=invalid-name,too-many-locals,too-many-statements


def printSet(inSet):
  """Print set."""
  if isinstance(inSet, set):
    return re.split(r'\(|\)', str(inSet))[1]
  else:
    return ''


def updateSteeringFile(inFileName, outFileName, parametersToSetup, exceptions=None):
  """Read input xml-file, update values given be dictionary and write result to a new file.

  :param basestring inFileName: name of input xml-file
  :param basestring outFileName: name of output xml-file
  :param dict parametersToSetup: dict which contains values which have to be updated.
                                 Keys of dictionary are XPath-string.
                                 E.g.: {"processor/[@name='OuterPlanarDigiProcessor']/parameter[@name='IsStrip']": True}

  :returns: S_OK or S_ERROR
  :rtype: dict
  """
  if exceptions is None:
    exceptions = []
  tree = et.parse(inFileName)

  # FIXME redirect log messegage to LOG class?
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
      errMsg = ("Cannot update parameter in the steering file! Parameter: %s; Value: %s; inFileName: %s;"
                " outFileName: %s" % (iPar, iVal, inFileName, outFileName))
      LOG.error(errMsg)
      return S_ERROR(errMsg)
    else:
      if isinstance(iVal, (float, int)):
        iVal = str(iVal)

      if iElement.text is None:
        if iVal not in iElement.get('value'):
          LOG.info('%s:\t"%s" --> "%s"' % (iPar, iElement.get('value'), iVal))
      else:
        if iVal not in iElement.text:
          LOG.info('%s:\t"%s" --> "%s"' % (iPar, iElement.text, iVal))
      # remove value attribute since we write value to the text field
      if 'value' in iElement.attrib:
        iElement.attrib.pop('value', None)
      iElement.text = iVal

  tree.write(outFileName)
  return S_OK()


def readValueFromSteeringFile(fileName, xPath):
  """Read value of the node from xml-file.

  :param basestring fileName: name of xml-file to read
  :param basestring xPath: xParh of the node to read.
                           E.g.: "processor/[@name='OuterPlanarDigiProcessor']/parameter[@name='IsStrip']"

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


def readParameterDict(inFile='DEFAULT_VALUE'):
  """Read parameter dict from file."""
  if inFile == 'DEFAULT_VALUE':
    import ILCDIRAC.CalibrationSystem.Utilities as utilities
    inFile = os.path.join(utilities.__path__[0], 'auxiliaryFiles/parameterListMarlinSteeringFile.txt')
  outList = {}
  with open(inFile, 'r') as f:
    for iLine in f:
      outList[iLine.split('\n')[0]] = None
  return outList


def readParametersFromSteeringFile(inFileName, parameterDict, exceptions=None):
  """Read parameters (provided in the input dict) from file."""
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


def convert_and_execute(command_list, fileToSource=''):
  """Take a list, cast every entry of said list to string and executes it in a subprocess.

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
  """Return list of files which satisfy provided pattern."""
  matches = []
  for root, _, filenames in os.walk(dirName):
    for filename in fnmatch.filter(filenames, filePattern):
      matches.append(os.path.join(root, filename))
  return matches


def saveCalibrationRun(calibRun):
  """Dump instance of calibrationRun class to file."""
  fileName = "calib%s/calibRun_bak.pkl" % (calibRun.calibrationID)
  with open(fileName, 'wb') as f:
    pickle.dump(calibRun, f, pickle.HIGHEST_PROTOCOL)


def loadCalibrationRun(calibrationID):
  """Recover instance of calibrationRun class from file."""
  fileName = "calib%s/calibRun_bak.pkl" % (calibrationID)
  if os.path.exists(fileName):
    with open(fileName, 'rb') as f:
      return pickle.load(f)
  else:
    return None


def addPfoAnalysisProcessor(mainSteeringMarlinRecoFile):
  """Add pfoAnalysis processor (needed for calibration) to Marlin steering file."""
  if not os.path.exists(mainSteeringMarlinRecoFile):
    return S_ERROR("cannot find input steering file: %s" % mainSteeringMarlinRecoFile)

  mainTree = et.ElementTree()
  try:
    mainTree.parse(mainSteeringMarlinRecoFile)
  except et.ParseError as e:
    return S_ERROR("cannot parse input steering file: %s. errMsg: %s" % (mainSteeringMarlinRecoFile, e))
  mainRoot = mainTree.getroot()

  # FIXME TODO properly find path to the file
  # this file should only contains PfoAnalysis processor
  import ILCDIRAC.CalibrationSystem.Utilities as utilities
  pfoAnalysisProcessorFile = os.path.join(utilities.__path__[0], 'auxiliaryFiles/pfoAnalysis.xml')
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
  """Add parameter to processor inside Marlin steering file."""
  if not os.path.exists(mainSteeringMarlinRecoFile):
    return S_ERROR("cannot find input steering file: %s" % mainSteeringMarlinRecoFile)

  mainTree = et.ElementTree()
  try:
    mainTree.parse(mainSteeringMarlinRecoFile)
  except et.ParseError as e:
    return S_ERROR("cannot parse input steering file: %s. errMsg: %s" % (mainSteeringMarlinRecoFile, e))

  if 'name' not in parameterDict.keys():
    return S_ERROR("parameter dict should have key 'name'")

  mainRoot = mainTree.getroot()

  # each processors is mentioned twixe in the steering file. Once in the execute (just name) and once in the body
  # (with name and type tags).
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
        return S_ERROR("parameter with name %s already exists in the processor %s" % (parameterDict['name'],
                                                                                      processorName))
  procElement.append(et.Element("parameter", parameterDict))

  root_str = et.tostring(mainRoot)
  with open(mainSteeringMarlinRecoFile, "w") as of:
    of.write(root_str)

  return S_OK()


def splitFilesAcrossJobs(inputFiles, nEventsPerFile, nJobs):
  """Regroup inputFiles dict according to number of jobs.

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


def convert_to_int_list(non_int_list):
  """Take a list and converts each entry to an integer, returning this new list.

  :param list non_int_list: List that contains entries that may not be integers but can be cast
  :returns: List that only contains integers.
  :rtype: list
  """
  result = []
  for entry in non_int_list:
    result.append(int(entry))
  return result


def calibration_creation_failed(results):
  """Return whether or not the creation of all calibration jobs was successful.

  :param results: List of S_OK/S_ERROR dicts that were returned by the submission call
  :returns: True if everything was successful, False otherwise
  :rtype: bool
  """
  success = True
  for job_result in results:
    success = success and job_result['OK']
  return not success
