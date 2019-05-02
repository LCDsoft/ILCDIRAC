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
    self.assertTrue(len(self.parDict) == 22, "wrong number of items are read")
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
