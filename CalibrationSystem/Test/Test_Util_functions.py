"""
Unit tests for the CalibrationSystem/Utilities/functions.py
"""

import pytest
import os
import unittest
import string
import random
import tempfile
import shutil

from ILCDIRAC.CalibrationSystem.Utilities.functions import readParameterDict
from ILCDIRAC.CalibrationSystem.Utilities.functions import readParametersFromSteeringFile
from ILCDIRAC.CalibrationSystem.Utilities.functions import updateSteeringFile
from ILCDIRAC.CalibrationSystem.Utilities.functions import addParameterToProcessor

__RCSID__ = "$Id$"
MODULE_NAME = 'ILCDIRAC.CalibrationSystem.Utilities.functions'


@pytest.fixture
def copyFccSteeringFile():
  calibID = 1
  workdirName = 'calib%s' % calibID
  if not os.path.exists(workdirName):
      os.makedirs(workdirName)
  src = '/cvmfs/clicdp.cern.ch/iLCSoft/builds/2019-04-17/x86_64-slc6-gcc62-opt/ClicPerformance/HEAD/fcceeConfig/fccReconstruction.xml'
  shutil.copyfile(src, '%s/fccReconstruction.xml' % workdirName)
  yield '%s/fccReconstruction.xml' % workdirName
  try:
    shutil.rmtree(workdirName)
  except EnvironmentError as e:
    print("Failed to delete directory: %s; ErrMsg: %s" % (workdirName, str(e)))
    assert False


@pytest.fixture
def produceRandomTextFile():
  f = tempfile.NamedTemporaryFile(delete=False)
  nLines = random.randint(2, 20)
  for iLine in range(0, nLines):
    nSymbolsInLine = random.randint(0, 120)
    line = ''
    for iSymbol in range(0, nSymbolsInLine):
      line += random.choice(string.ascii_letters + '       ')
    f.write(line)
  f.close()
  yield f.name
  os.unlink(f.name)


def test_addParameterToProcessor(produceRandomTextFile, copyFccSteeringFile, mocker):
  # non-existing input file
  res = addParameterToProcessor('dummy.xml', 'dummyProc', {'name': 'dummyValue'})
  assert not res['OK']
  assert "cannot find input" in res['Message']
  # non-xml input file
  randomFile = produceRandomTextFile
  res = addParameterToProcessor(randomFile, 'dummyProc', {'name': 'dummyValue'})
  assert not res['OK']
  assert "cannot parse input" in res['Message']
  # good input file, non-existing processor
  steeringFile = copyFccSteeringFile
  res = addParameterToProcessor(steeringFile, 'dummyProc', {'name': 'dummyValue'})
  assert not res['OK']
  assert "Can't find processor" in res['Message']
  # good input file, good processor name, no 'name' key in the parameter dict
  steeringFile = copyFccSteeringFile
  res = addParameterToProcessor(steeringFile, 'dummyProc', {'dummy': 'dummyValue'})
  assert not res['OK']
  assert "parameter dict should have key 'name'" in res['Message']
  # good input file, good processor name
  res = addParameterToProcessor(steeringFile, 'MyAIDAProcessor', {'name': 'dummyValue'})
  assert res['OK']
  # good input file, good processor name, second append of the parameter with the same name
  res = addParameterToProcessor(steeringFile, 'MyAIDAProcessor', {'name': 'dummyValue'})
  assert not res['OK']
  assert ("parameter with name %s already exists" % 'dummyValue') in res['Message']

class TestsFileUtilsFunctions(unittest.TestCase):
  """ Tests the utilities/functions for the CalibrationSystem """

  def setUp(self):
    """set up the objects"""
    import ILCDIRAC.CalibrationSystem.Utilities as utilities
    self.fileDir = os.path.join(utilities.__path__[0], 'testing')

    inFileName = os.path.join(self.fileDir, 'parameterListMarlinSteeringFile.txt')
    self.parDict = readParameterDict(inFileName)
    for iKey in self.parDict.keys():
      if 'RootFile' in iKey:
        del self.parDict[iKey]


  def tearDown(self):
    """ tear down the objects """
    fileName = os.path.join(self.fileDir, 'out1.xml')
    if os.path.exists(fileName):
      os.remove(fileName)
    pass

  def test_readParameterDict(self):
    self.assertTrue(not '' in self.parDict.keys(), "entry with empty key in the dictionary")

    allValuesAreNone = True
    for iKey, iVal in self.parDict.iteritems():
      if iVal is not None:
        allValuesAreNone = False
    self.assertTrue(allValuesAreNone, "all values in dict has to be None")

  def test_readParametersFromSteeringFile(self):
    inFileName = os.path.join(self.fileDir, 'clicReconstruction_2019-04-17.xml')
    res = readParametersFromSteeringFile(inFileName, self.parDict)
    print(res)
    self.assertTrue(res['OK'], "function didn't return S_OK")

    someValuesAreNone = False
    for iKey, iVal in self.parDict.iteritems():
      if iVal is None:
        someValuesAreNone = True
    self.assertTrue(not someValuesAreNone, "all read values has to be not None")

  def test_updateSteeringFile(self):
    initialParDict = self.parDict

    parDict1 = dict(initialParDict)
    inFileName = os.path.join(self.fileDir, 'clicReconstruction_2019-04-17.xml')
    res = readParametersFromSteeringFile(inFileName, parDict1)
    #  key1 = "processor[@name='MyPfoAnalysis']/parameter[@name='RootFile']"
    #  parDict1[key1] = "dummyDummyRootFile.root"
    #  key2 = "global/parameter[@name='LCIOInputFiles']"
    #  parDict1[key2] = "in1.slcio, in2.slcio"
    #  self.assertTrue(len(parDict1) == len(initialParDict), "two dictionaries have to be the same size. len1: %s; len2: %s" % (len(parDict1), len(initialParDict)))

    outFileName = os.path.join(self.fileDir, 'out1.xml')
    res = updateSteeringFile(inFileName, outFileName, parDict1)
    self.assertTrue(res['OK'], "function didn't return S_OK")

    parDict2 = dict(initialParDict)
    res = readParametersFromSteeringFile(outFileName, parDict2)
    self.assertTrue(len(parDict1) == len(parDict2), "two dictionaries have to be the same size")

    notEqualValues = False
    for iKey in initialParDict:
      if parDict1[iKey] != parDict2[iKey]:
        notEqualValues = True
    self.assertTrue(not notEqualValues, "two dictionaries have to be identical")
