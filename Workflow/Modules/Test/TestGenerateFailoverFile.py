#!/usr/env python

"""
Test generateFailoverFile
"""
__RCSID__ = "$Id$"

import unittest
from mock import MagicMock as Mock

from DIRAC import gLogger, S_ERROR, S_OK

from DIRAC.Core.Base import Script
Script.parseCommandLine()
from ILCDIRAC.Workflow.Modules.ModuleBase import ModuleBase

from DIRAC.Workflow.Modules.test.Test_Modules import ModulesTestCase as DiracModulesTestCase
#import DIRAC.Workflow.Modules.test.Test_Modules as Test_Modules

class ModulesTestCase ( DiracModulesTestCase ):
  """ ILCDirac version of Workflow module tests"""
  def setUp( self ):
    """Set up the objects"""
    super(ModulesTestCase, self).setUp()
    gLogger.setLevel("DEBUG")
    self.log = gLogger.getSubLogger("ModuleBaseTest")
    self.log.showHeaders(True)
    self.log.setLevel("ERROR")
    self.mb = ModuleBase()
    self.mb.rm = self.rm_mock
    self.mb.request = self.rc_mock
    self.mb.jobReport = self.jr_mock
    self.mb.fileReport = self.fr_mock
    self.mb.workflow_commons = self.wf_commons[0]
    self.mb.workflow_commons['LogFilePath'] = "/ilc/test/dummy/folder"
    
    from ILCDIRAC.Workflow.Modules.UploadOutputData import UploadOutputData
    self.uod = UploadOutputData()
    self.uod.workflow_commons = self.mb.workflow_commons
    
    from ILCDIRAC.Workflow.Modules.UploadLogFile import UploadLogFile
    self.ulf = UploadLogFile()
    self.ulf.resolveInputVariables = Mock(return_value=S_OK())
    self.ulf.tryFailoverTransfer = Mock(return_value = S_OK({'Request': self.rc_mock,
                                                             'uploadedSE': 'CERN-SRM'}))
    
    self.ulf.workflow_commons = self.mb.workflow_commons

    from ILCDIRAC.Workflow.Modules.FailoverRequest import FailoverRequest
    self.fr = FailoverRequest()
    self.fr.workflow_commons = self.mb.workflow_commons


    
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
    self.ulf.resolveInputVariables = Mock(return_value=S_OK())
    self.ulf.determineRelevantFiles = Mock(return_value=S_OK([]))
    res = self.ulf.execute()
    self.assertTrue( res['OK'] )

  def test_OneLogFile( self ):
    self.ulf.log.setLevel("DEBUG")
    self.ulf.determineRelevantFiles = Mock(return_value=S_OK(['MyLogFile.log']))
    self.ulf.logSE.putDirectory = Mock(return_value=S_OK(dict(Failed=['MyLogFile.log'],Message="MockingJay")))
    from ILCDIRAC.Core.Utilities.ProductionData import getLogPath
    self.ulf.logLFNPath = getLogPath(self.ulf.workflow_commons)['Value']['LogTargetPath'][0]
    self.log.info("LOGPATH: %s: " % self.ulf.logLFNPath)
    self.ulf.applicationSpecificInputs()
    res = self.ulf.execute()
    self.assertTrue( res['OK'] )

  def test_LogFileGone( self ):
    self.ulf.log.setLevel("DEBUG")
    self.ulf.determineRelevantFiles = Mock(return_value=S_OK(['std.out']))
    self.assertRaises( IOError, self.ulf.execute )

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
  