"""Tests for .Core.Utilities.ProductionData.constructProductionLFNs"""
__RCSID__ = "$Id$"
#pylint: disable=R0904
import unittest
from ILCDIRAC.Core.Utilities.ProductionData import constructProductionLFNs

class ProductionOutputDataTestCase( unittest.TestCase ):
  """ Base class for the test cases
  """
  def setUp( self ):
    pass
  def tearDown( self ):
    pass
  
  def test_contructProductionLFNBad(self):
    commons = {}
    result = constructProductionLFNs(commons)
    self.assertEqual( result['OK'], False)
    
  def test_contructProductionLFNstdhep(self):
    commons = {}
    commons['PRODUCTION_ID'] = 12345
    commons['JOB_ID'] = 1234
    commons['outputList'] = [{'outputFile':"something_gen.stdhep",
                              'outputPath':'/ilc/prod/clic/test/gen'}]
    result = constructProductionLFNs(commons)

    self.assertEqual( result['OK'], True)


  def test_contructProductionLFNsim(self):
    commons = {}
    commons['PRODUCTION_ID'] = 12345
    commons['JOB_ID'] = 1234
    commons['outputList'] = [{'outputFile':"something_sim.slcio",
                              'outputPath':'/ilc/prod/clic/test/SIM'}]
    result = constructProductionLFNs(commons)
    self.assertEqual( result['OK'], True)
    
  def test_contructProductionLFNrec(self):
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
    commons = {}
    commons['PRODUCTION_ID'] = 12345
    commons['JOB_ID'] = 1234
    commons['outputList'] = [{'outputFile':"something_gen.stdhep",
                              'outputPath':'/ilc/prod/clic/test/gen'}]
    result = constructProductionLFNs(commons)
    res = {'ProductionOutputData' : ["/ilc/prod/clic/test/gen/00012345/001/something_gen_12345_1234.stdhep"], 
           'LogFilePath' : ["/ilc/prod/clic/test/gen/00012345/LOG/001"],
           'LogTargetPath' : ["/ilc/prod/clic/test/gen/00012345/LOG/00012345_1234.tar"],
           'DebugLFNs' : []}
    for key in res.keys():
      self.assertEqual( result['Value'][key], res[key])
        
    
def runTests():
  """runs all the tests"""
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ProductionOutputDataTestCase )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  print testResult

if __name__ == '__main__':
  runTests()
