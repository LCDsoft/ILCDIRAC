#!/usr/env python

"""
Test generateFailoverFile
"""
__RCSID__ = "$Id$"
#pylint: disable=W0212
import unittest, copy
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
gLogger.setLevel("DEBUG")
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

    
class ModuleBaseFailure( ModulesTestCase ):
  """ Test the generateFailoverFile function"""

    
  def test_generateFailoverFile( self ):
    """run the generateFailoverFile function and see what happens"""
    dummy_res = self.mb.generateFailoverFile()
    #print res


#############################################################################
# UploadLogFile.py
#############################################################################

class UploadLogFileSuccess( ModulesTestCase ):
  """ test UploadLogFile """

  def test_execute( self ):
    """ tests UploadLogFile execute function"""
    pass

class UploadLogFileFailure( ModulesTestCase ):
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
    self.ulf.logSE.putDirectory = Mock(return_value=S_OK(dict(Failed=['MyLogFile.log'],Message="MockingJay")))
    self.ulf.logLFNPath = getLogPath(self.ulf.workflow_commons)['Value']['LogTargetPath'][0]
    self.ulf._tryFailoverTransfer = Mock(return_value = S_OK({'Request': self.rc_mock,
                                                              'uploadedSE': 'CERN-SRM'}))
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
                                                              Message="MockingJay")))
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

class FailoverRequestSuccess( ModulesTestCase ):
  """ test UploadLogFile """
  def test_execute( self ):
    """ tests execute function"""
    pass

class FailoverRequestFailure( ModulesTestCase ):
  """ test UploadLogFile """
  def test_execute( self ):
    """ tests execute function"""
    pass



#############################################################################
# Run Tests
#############################################################################
def runTests():
  """Runs our tests"""
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ModulesTestCase )
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ModuleBaseFailure ) )

  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( UploadOutputDataFailure ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( UploadLogFileFailure ) )
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( FailoverRequestFailure ) )
  
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( FailoverRequestSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  print testResult

  ## Test from ILCDirac Proper
  # suite = unittest.defaultTestLoader.loadTestsFromTestCase( DiracModulesTestCase )
  # suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( Test_Modules.ModuleBaseSuccess ) )
  # suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( Test_Modules.FailoverRequestSuccess ) )
  # testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

if __name__ == '__main__':
  runTests()
  