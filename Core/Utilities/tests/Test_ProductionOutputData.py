"""Tests for .Core.Utilities.ProductionData.constructProductionLFNs"""

import unittest
from ILCDIRAC.Core.Utilities.ProductionData import constructProductionLFNs

__RCSID__ = "$Id$"

class ProductionOutputDataTestCase( unittest.TestCase ):
  """ Base class for the test cases
  """
  def setUp( self ):
    pass
  def tearDown( self ):
    pass
  
  def test_contructProductionLFNBad(self):
    """test ProductionOutputData construct Bad LFN.................................................."""
    commons = {}
    result = constructProductionLFNs(commons)
    self.assertEqual( result['OK'], False)
    
  def test_contructProductionLFNstdhep(self):
    """test ProductionOutputData construct stdhep LFN..............................................."""
    commons = {}
    commons['PRODUCTION_ID'] = 12345
    commons['JOB_ID'] = 1234
    commons['outputList'] = [{'outputFile':"something_gen.stdhep",
                              'outputPath':'/ilc/prod/clic/test/gen'}]
    result = constructProductionLFNs(commons)

    self.assertEqual( result['OK'], True)


  def test_contructProductionLFNsim(self):
    """test ProductionOutputData construct sim LFN.................................................."""
    commons = {}
    commons['PRODUCTION_ID'] = 12345
    commons['JOB_ID'] = 1234
    commons['outputList'] = [{'outputFile':"something_sim.slcio",
                              'outputPath':'/ilc/prod/clic/test/SIM'}]
    result = constructProductionLFNs(commons)
    self.assertEqual( result['OK'], True)
    
  def test_contructProductionLFNrec(self):
    """test ProductionOutputData construct rec LFN.................................................."""
    commons = {}
    commons['PRODUCTION_ID'] = 12345
    commons['JOB_ID'] = 1234
    commons['outputList'] = [{'outputFile':"something_rec.slcio",
                              'outputPath':'/ilc/prod/clic/test/REC'},
                             {'outputFile':"something_dst.slcio",
                              'outputPath':'/ilc/prod/clic/test/DST'}]
    result = constructProductionLFNs(commons)
    self.assertEqual( result['OK'], True)

  def test_contructProductionLFNoutput(self):
    """test ProductionOutputData construct out LFN.................................................."""
    commons = {}
    commons['PRODUCTION_ID'] = 12345
    commons['JOB_ID'] = 1234
    commons['outputList'] = [{'outputFile':"something_gen.stdhep",
                              'outputPath':'/ilc/prod/clic/test/gen'}]
    result = constructProductionLFNs(commons)
    res = {'ProductionOutputData' : ["/ilc/prod/clic/test/gen/00012345/001/something_gen_12345_1234.stdhep"], 
           'LogFilePath' : ["/ilc/prod/clic/test/gen/00012345/LOG/001"],
           'LogTargetPath' : ["/ilc/prod/clic/test/gen/LOG/00012345/00012345_1234.tar"]}
    for key in res.keys():
      self.assertEqual( result['Value'][key], res[key])
        
    
def runTests():
  """runs all the tests"""
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ProductionOutputDataTestCase )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  print testResult

if __name__ == '__main__':
  runTests()
