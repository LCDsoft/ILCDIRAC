#!/usr/env python

"""
Test generateFailoverFile
"""
__RCSID__ = "$Id$"
#pylint: disable=W0212
import unittest, copy, os
from mock import MagicMock as Mock

from DIRAC import gLogger, S_ERROR, S_OK

from DIRAC.Core.Base import Script
Script.parseCommandLine()
from ILCDIRAC.Workflow.Modules.ModuleBase import ModuleBase
from DIRAC.RequestManagementSystem.Client.Request import Request

from ILCDIRAC.Core.Utilities.ProductionData import getLogPath
from ILCDIRAC.Workflow.Modules.FailoverRequest import FailoverRequest
from ILCDIRAC.Workflow.Modules.UploadOutputData import UploadOutputData
from ILCDIRAC.Workflow.Modules.UploadLogFile import UploadLogFile


from DIRAC.Workflow.Modules.test.Test_Modules import ModulesTestCase as DiracModulesTestCase
#import DIRAC.Workflow.Modules.test.Test_Modules as Test_Modules
gLogger.setLevel("Notice")
gLogger.showHeaders(True)
class ModulesTestCase ( DiracModulesTestCase ):
  """ ILCDirac version of Workflow module tests"""
  def setUp( self ):
    """Set up the objects"""
    super(ModulesTestCase, self).setUp()
    self.log = gLogger.getSubLogger("MODULEBASE")
    self.mb = ModuleBase()
    self.mb.rm = self.rm_mock
    self.mb.request = self.rc_mock
    self.mb.jobReport = self.jr_mock
    self.mb.fileReport = self.fr_mock
    self.mb.workflow_commons = self.wf_commons[0]
    self.mb.workflow_commons['LogFilePath'] = "/ilc/user/s/sailer/test/dummy/folder"
    self.mb.log = gLogger.getSubLogger("ModuleBaseTest")
    self.mb.log.showHeaders(True)
    
    self.uod = UploadOutputData()
    self.uod.workflow_commons = self.mb.workflow_commons

    self.fr = FailoverRequest()
    self.fr.workflow_commons = self.mb.workflow_commons

    self.ulf = UploadLogFile()

    
class TestModuleBase( ModulesTestCase ):
  """ Test the generateFailoverFile function"""

    
  def test_generateFailoverFile( self ):
    """run the generateFailoverFile function and see what happens"""
    dummy_res = self.mb.generateFailoverFile()
    #print res


#############################################################################
# UploadLogFile.py
#############################################################################

class TestUploadLogFile( ModulesTestCase ):
  """ test UploadLogFile """

  def test_NoLogFiles( self ):
    self.ulf = UploadLogFile()
    self.ulf.workflow_commons = copy.deepcopy(self.mb.workflow_commons)
    self.ulf.log = gLogger.getSubLogger("ULF-NoLogFiles")
    self.ulf.log.setLevel("INFO")

    self.ulf.resolveInputVariables = Mock(return_value=S_OK())
    self.ulf._determineRelevantFiles = Mock(return_value=S_OK([]))
    self.ulf.applicationSpecificInputs()
    res = self.ulf.execute()
    self.assertTrue( res['OK'] )

  def test_OneLogFile( self ):
    self.ulf = UploadLogFile()
    self.ulf.log = gLogger.getSubLogger("ULF-OneLogFile")
    self.ulf.workflow_commons = copy.deepcopy(self.wf_commons[0])
    self.ulf._determineRelevantFiles = Mock(return_value=S_OK(['MyLogFile.log']))
    self.ulf.logSE.putDirectory = Mock(return_value=S_OK(dict(Failed=['MyLogFile.log'],Message="Ekke Ekke Ekke Ekke")))
    self.ulf.logLFNPath = getLogPath(self.ulf.workflow_commons)['Value']['LogTargetPath'][0]
    self.ulf._tryFailoverTransfer = Mock(return_value = S_OK({'Request': self.rc_mock,
                                                              'uploadedSE': 'CERN-SRM'}))
    self.ulf.applicationSpecificInputs()
    res = self.ulf.execute()
    self.assertTrue( res['OK'] )


  def test_FailedFailover( self ):
    self.ulf = UploadLogFile()
    self.ulf.log = gLogger.getSubLogger("ULF-OneLogFile")
    self.ulf.workflow_commons = copy.deepcopy(self.wf_commons[0])
    self.ulf._determineRelevantFiles = Mock(return_value=S_OK(['MyLogFile.log']))
    self.ulf.logSE.putDirectory = Mock(return_value=S_OK(dict(Failed=['MyLogFile.log'],Message="Ekke Ekke Ekke Ekke")))
    self.ulf.logLFNPath = getLogPath(self.ulf.workflow_commons)['Value']['LogTargetPath'][0]
    self.ulf._tryFailoverTransfer = Mock(return_value = S_OK())
    self.ulf.applicationSpecificInputs()
    res = self.ulf.execute()
    self.assertTrue( res['OK'] )

  def test_LogFileGone( self ):
    self.ulf = UploadLogFile()
    self.ulf.workflow_commons = copy.deepcopy(self.wf_commons[0])
    self.ulf.log = gLogger.getSubLogger("ULF-LogFileGone")

    self.ulf.logLFNPath = getLogPath(self.ulf.workflow_commons)['Value']['LogTargetPath'][0]
    self.ulf._determineRelevantFiles = Mock(return_value=S_OK(['std.out']))
    self.assertRaises( IOError, self.ulf.execute )

  def test_Request( self ):
    self.ulf.workflow_commons = copy.deepcopy(self.wf_commons[0])
    self.ulf.log = gLogger.getSubLogger("ULF-RequestTest")
    self.ulf._determineRelevantFiles = Mock(return_value=S_OK(['MyLogFile.log','MyOtherLogFile.log']))
    self.ulf.logSE.putDirectory = Mock(return_value=S_OK(dict(Failed=['MyLogFile.log', 'MyOtherLogFile.log'],
                                                              Message="Ekke Ekke Ekke Ekke")))
    self.mb.workflow_commons['Request']  = Request()
    self.ulf._tryFailoverTransfer = Mock(return_value = S_OK({'Request': self.mb.workflow_commons['Request'],
                                                              'uploadedSE': 'CERN-SRM'}))
    self.ulf.logLFNPath = getLogPath(self.ulf.workflow_commons)['Value']['LogTargetPath'][0]
    self.ulf.applicationSpecificInputs()
    res = self.ulf.execute()
    self.assertTrue( res['OK'] )

#############################################################################
# UploadOutputData.py
#############################################################################

    
class UploadOutputDataSuccess( ModulesTestCase ):
  """ test UploadLogFile """
  def test_execute( self ):
    """ tests execute function"""
    pass

class UploadOutputDataFailure( ModulesTestCase ):
  """ test UploadLogFile """
  def test_execute( self ):
    """ tests execute function"""
    pass

#############################################################################
# FailoverRequest.py
#############################################################################

class TestFailoverRequest( ModulesTestCase ):
  """ test UploadLogFile """
  def setUp( self ):
    super(TestFailoverRequest, self).setUp()
    self.frq = None

  def test_execute( self ):
    pass

  def test_ASI_Enabled( self ):
    """applicationSpecificInputs: control flag is enabled......................................."""
    self.frq = FailoverRequest()
    self.frq.workflow_commons = dict( )
    self.frq.log = gLogger.getSubLogger("testASI")
    os.environ['JOBID']="12345"
    self.frq.applicationSpecificInputs()
    del os.environ['JOBID']
    self.assertTrue ( self.frq.enable )

  def test_ASI_Disable( self ):
    """applicationSpecificInputs: control flag is enabled......................................."""
    self.frq = FailoverRequest()
    self.frq.workflow_commons = dict( )
    self.frq.log = gLogger.getSubLogger("testASI")
    os.environ['JOBID']="12345"
    self.frq.step_commons = dict( Enable = "arg")
    self.frq.applicationSpecificInputs()
    del os.environ['JOBID']
    self.assertFalse ( self.frq.enable )

  def test_ASI_Disabled( self ):
    """applicationSpecificInputs: control flag is disabled......................................"""
    self.frq = FailoverRequest()
    self.frq.workflow_commons = dict( )
    self.frq.log = gLogger.getSubLogger("testASI")

    self.frq.applicationSpecificInputs()
    self.assertTrue ( self.frq.enable == False )

  def test_ASI_AllVariables( self ):
    """applicationSpecificInputs: checks if all variables have been properly set after this call"""
    self.frq = FailoverRequest()
    self.frq.workflow_commons = dict( JobReport = self.jr_mock, FileReport = self.fr_mock, PRODUCTION_ID=43321, JOB_ID = 12345 )
    os.environ['JOBID']="12345"
    self.frq.applicationSpecificInputs()
    del os.environ['JOBID']
    self.assertTrue( self.frq.jobReport and self.frq.fileReport and
                     self.frq.productionID and self.frq.prodJobID and self.frq.enable )

  def test_ASI_NoVariables( self ):
    """applicationSpecificInputs: checks that no variables have been set after this call........"""
    self.frq = FailoverRequest()
    self.frq.workflow_commons = dict()
    os.environ['JOBID']="12345"
    self.frq.applicationSpecificInputs()
    del os.environ['JOBID']
    self.assertFalse( self.frq.jobReport or self.frq.fileReport or
                      self.frq.productionID or self.frq.prodJobID )


#############################################################################
# Run Tests
#############################################################################
def runTests():
  """Runs our tests"""
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ModulesTestCase )

  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestUploadLogFile ) )
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestModuleBase ) )
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestUploadOutputData ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestFailoverRequest ) )
  
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  print testResult

  ## Test from Dirac Proper
  # suite = unittest.defaultTestLoader.loadTestsFromTestCase( DiracModulesTestCase )
  # suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( Test_Modules.ModuleBaseSuccess ) )
  # suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( Test_Modules.FailoverRequestSuccess ) )
  # testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

if __name__ == '__main__':
  runTests()
  