"""
Unit tests for the CalibrationSystem/Utilities/functions.py
"""

import os
import unittest

from ILCDIRAC.CalibrationSystem.Utilities.functions import readParameterDict
from ILCDIRAC.CalibrationSystem.Utilities.functions import readParametersFromSteeringFile
from ILCDIRAC.CalibrationSystem.Utilities.functions import updateSteeringFile


__RCSID__ = "$Id$"
MODULE_NAME = 'ILCDIRAC.CalibrationSystem.Utilities.functions'


class TestsFileUtilsFunctions(unittest.TestCase):
  """ Tests the utilities/functions for the CalibrationSystem """

  def setUp(self):
    """set up the objects"""
    self.fileDir = os.path.join(os.environ['DIRAC'], "ILCDIRAC", "CalibrationSystem", "Utilities", "testing")
    pass

  def tearDown(self):
    """ tear down the objects """
    fileName = os.path.join(self.fileDir, 'out1.xml')
    if os.path.exists(fileName):
      os.remove(fileName)
    pass

  def test_readParameterDict(self):
    inFileName = os.path.join(self.fileDir, 'parameterListMarlinSteeringFile.txt')
    parDict = readParameterDict(inFileName)
    self.assertTrue(len(parDict) == 22, "wrong number of items are read")
    allValuesAreNone = True

    for iKey, iVal in parDict.iteritems():
      if iVal is not None:
        allValuesAreNone = False
    self.assertTrue(allValuesAreNone, "all values in dict has to be None")

  def test_readParametersFromSteeringFile(self):
    inFileName = os.path.join(self.fileDir, 'parameterListMarlinSteeringFile.txt')
    parDict = readParameterDict(inFileName)

    inFileName = os.path.join(self.fileDir, 'in1.xml')
    res = readParametersFromSteeringFile(inFileName, parDict)
    self.assertTrue(res['OK'], "function didn't return S_OK")

    someValuesAreNone = False
    for iKey, iVal in parDict.iteritems():
      if iVal is None:
        someValuesAreNone = True
    self.assertTrue(not someValuesAreNone, "all read values has to be not None")

  def test_updateSteeringFile(self):
    inFileName = os.path.join(self.fileDir, 'parameterListMarlinSteeringFile.txt')
    initialParDict = readParameterDict(inFileName)

    parDict1 = dict(initialParDict)
    inFileName = os.path.join(self.fileDir, 'in1.xml')
    res = readParametersFromSteeringFile(inFileName, parDict1)
    key1 = "processor[@name='MyPfoAnalysis']/parameter[@name='RootFile']"
    parDict1[key1] = "dummyDummyRootFile.root"
    key2 = "global/parameter[@name='LCIOInputFiles']"
    parDict1[key2] = "in1.slcio, in2.slcio"
    self.assertTrue(len(parDict1) == len(initialParDict), "two dictionaries have to be the same size")

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
