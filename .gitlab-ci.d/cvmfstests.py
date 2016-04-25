#!/usr/bin/env python
'''
Run many different applications as a test. Creates a temp directory and runs in there.
Stops at any error.

:since: Nov 8, 2013

:author: sposs
'''

import unittest
import os
from mock import patch, MagicMock as Mock
from DIRAC.Core.Base import Script
from DIRAC import S_OK, gConfig
from ILCDIRAC.Interfaces.API.NewInterface.Tests.LocalTestObjects import TestCreater, CLIParams


__RCSID__ = "$Id$"


class JobTestCase( unittest.TestCase ):
  """ Base class for the ProductionJob test cases
  """
  
  @classmethod
  def setUpClass(cls):
    """Read in parameters etc."""
    #clip = CLIParams()
    #clip.registerSwitches()
    Script.parseCommandLine()
  
  def setUp(self):
    """set up the objects"""
    super(JobTestCase, self).setUp()
    clip = CLIParams()
    clip.testOverlay=True
    clip.testChain = True
    clip.testMokka = True
    clip.testInputData = True
    clip.testWhizard = True
    clip.testUtilities = True
    overlayrun = clip.testOverlay
    clip.testRoot = True
    myMarlinSteeringFile = "bbudsc_3evt_stdreco.xml"
    myLCSimPreSteeringFile = "clic_cdr_prePandoraOverlay_1400.0.lcsim" if overlayrun else "clic_cdr_prePandora.lcsim"
    myLCSimPostSteeringFile = "clic_cdr_postPandoraOverlay.lcsim"
    parameterDict = dict( mokkaVersion="ILCSoft-01-17-06",
                          mokkaSteeringFile="bbudsc_3evt.steer",
                          detectorModel="ILD_o1_v05",
                          machine="ilc_dbd",
                          backgroundType="aa_lowpt",
                          energy=350.0,
                          marlinVersion="ILCSoft-01-17-06",
                          marlinSteeringFile=myMarlinSteeringFile,
                          alwaysOverlay = True,
                          marlinInputData="/ilc/user/s/sailer/testILDsim.slcio",
                          ildConfig = "v01-16-p10_250",
                          gearFile='GearOutput.xml',
                          lcsimPreSteeringFile=myLCSimPreSteeringFile,
                          lcsimPostSteeringFile=myLCSimPostSteeringFile,
                          rootVersion="ILCSoft-01-17-08"
                        )

    self.myTests = TestCreater(clip, parameterDict)
    # Differentiate between local execution and execution in docker
    localsitelocalarea = ''
    if os.path.exists("/home/jebbing/"):
      localsitelocalarea = "/home/jebbing/cvmfstests"
    else:
      localsitelocalarea = os.path.join(os.getcwd(), "cvmfstests" )
    gConfig.setOptionValue( '/LocalSite/LocalArea', localsitelocalarea)
    gConfig.setOptionValue( '/LocalSite/LocalSE', "CERN-DIP-4" )
    gConfig.setOptionValue( '/Operations/Defaults/AvailableTarBalls/x86_64-slc5-gcc43-opt/steeringfiles/V16/Overwrite', 'False' )
    gConfig.setOptionValue( '/Operations/Defaults/AvailableTarBalls/x86_64-slc5-gcc43-opt/steeringfiles/V18/Overwrite', 'False' )
    gConfig.setOptionValue( '/Operations/Defaults/AvailableTarBalls/x86_64-slc5-gcc43-opt/stdhepcutjava/1.0/Overwrite', 'False' )
    gConfig.setOptionValue( '/Resources/Countries/local/AssignedTo' , 'ch' )

  
  #@unittest.skip("Temporarily disabled due to length")
  @patch("ILCDIRAC.Workflow.Modules.ModuleBase.getProxyInfoAsString", new=Mock(return_value=S_OK()))
  @patch("ILCDIRAC.Interfaces.API.NewInterface.UserJob.getProxyInfo", new=Mock(return_value=S_OK({"group":"ilc_user"})))
  @patch("ILCDIRAC.Interfaces.API.NewInterface.UserJob.UserJob.setPlatform", new=Mock(return_value=S_OK()))
  #@patch("DIRAC.Core.Utilities.Network.getFQDN", new=Mock(return_value='pclcd32docker.cern.ch'))
  #@patch("ILCDIRAC.Workflow.Modules.UserJobFinalization.siteName", new=Mock(return_value='MyCustomValue'))
  #@patch("DIRAC.siteName", new=Mock(return_value='MyCustomValue'))
  #@patch("DIRAC.Core.Utilities.Network.socket.getfqdn", new=Mock(return_value='pclcd32docker.cern.ch'))
  #@patch("socket.getfqdn", new=Mock(return_value='pclcd32docker.cern.ch'))
  #@patch("socket.gethostbyaddr", new=Mock(return_value=('pclcd32docker.cern.ch', [], ['172.17.0.1'])))
  def test_mokka(self):
    """create test for mokka"""
    jobs = self.myTests.createMokkaTest()
    self.assertTrue ( jobs['OK'] )
    thisJob = jobs['Value']
    res = self.myTests.runJobLocally(thisJob, "Mokka")
    self.assertTrue ( res['OK'] )

  #@unittest.skip("Temporarily disabled due to length")
  @patch("ILCDIRAC.Workflow.Modules.ModuleBase.getProxyInfoAsString", new=Mock(return_value=S_OK()))
  @patch("ILCDIRAC.Interfaces.API.NewInterface.UserJob.getProxyInfo", new=Mock(return_value=S_OK({"group":"ilc_user"})))
  @patch("ILCDIRAC.Interfaces.API.NewInterface.UserJob.UserJob.setPlatform", new=Mock(return_value=S_OK()))
  def test_marlin(self):
    """create test for marlin"""
    jobs = self.myTests.createMarlinTest()
    self.assertTrue ( jobs['OK'] )
    thisJob = jobs['Value']
    res = self.myTests.runJobLocally(thisJob, "Marlin")
    self.assertTrue ( res['OK'] )
  
  #@unittest.skip("Temporarily disabled due to length")
  @patch("ILCDIRAC.Workflow.Modules.ModuleBase.getProxyInfoAsString", new=Mock(return_value=S_OK()))
  @patch("ILCDIRAC.Interfaces.API.NewInterface.UserJob.getProxyInfo", new=Mock(return_value=S_OK({"group":"ilc_user"})))
  @patch("ILCDIRAC.Interfaces.API.NewInterface.UserJob.UserJob.setPlatform", new=Mock(return_value=S_OK()))
  def test_whizard(self):
    """create test for whizard"""
    jobs = self.myTests.createWhizardTest()
    self.assertTrue ( jobs['OK'] )
    theseJobs = jobs['Value']
    for thisJob in theseJobs:
      res = self.myTests.runJobLocally(thisJob,"Whizard")
      self.assertTrue ( res['OK'] )

  #@unittest.skip("Temporarily disabled due to length")
  @patch("ILCDIRAC.Workflow.Modules.ModuleBase.getProxyInfoAsString", new=Mock(return_value=S_OK()))
  @patch("ILCDIRAC.Interfaces.API.NewInterface.UserJob.getProxyInfo", new=Mock(return_value=S_OK({"group":"ilc_user"})))
  @patch("ILCDIRAC.Interfaces.API.NewInterface.UserJob.UserJob.setPlatform", new=Mock(return_value=S_OK()))
  def test_utilities(self):
    """create test for utilities"""
    print "Creating Test"
    jobs = self.myTests.createUtilityTests()
    self.assertTrue ( jobs['OK'] )
    print "Creation OK"
    theseJobs = jobs['Value']
    for thisJob in theseJobs:
      print "Running a Job"
      res = self.myTests.runJobLocally(thisJob,"Utility")
      self.assertTrue ( res['OK'] )
      print "Job executed correctly"
      
  #@unittest.skip("Temporarily disabled due to length")
  @patch("ILCDIRAC.Workflow.Modules.ModuleBase.getProxyInfoAsString", new=Mock(return_value=S_OK()))
  @patch("ILCDIRAC.Interfaces.API.NewInterface.UserJob.getProxyInfo", new=Mock(return_value=S_OK({"group":"ilc_user"})))
  @patch("ILCDIRAC.Interfaces.API.NewInterface.UserJob.UserJob.setPlatform", new=Mock(return_value=S_OK()))
  def test_root(self):
    """create test for root 1"""
    jobs = self.myTests.createRootScriptTest()
    self.assertTrue ( jobs['OK'] )
    thisJob = jobs['Value']
    res = self.myTests.runJobLocally(thisJob, "Root")
    self.assertTrue ( res['OK'] )

  #@unittest.skip("Temporarily disabled due to length")
  @patch("ILCDIRAC.Workflow.Modules.ModuleBase.getProxyInfoAsString", new=Mock(return_value=S_OK()))
  @patch("ILCDIRAC.Interfaces.API.NewInterface.UserJob.getProxyInfo", new=Mock(return_value=S_OK({"group":"ilc_user"})))
  @patch("ILCDIRAC.Interfaces.API.NewInterface.UserJob.UserJob.setPlatform", new=Mock(return_value=S_OK()))
  def test_root2(self):
    """create test for root 2"""
    jobs = self.myTests.createRootHaddTest()
    self.assertTrue ( jobs['OK'] )
    thisJob = jobs['Value']
    res = self.myTests.runJobLocally(thisJob, "Root")
    self.assertTrue ( res['OK'] )

  #@unittest.skip("Temporarily disabled due to length") 
  @patch("ILCDIRAC.Workflow.Modules.ModuleBase.getProxyInfoAsString", new=Mock(return_value=S_OK()))
  @patch("ILCDIRAC.Interfaces.API.NewInterface.UserJob.getProxyInfo", new=Mock(return_value=S_OK({"group":"ilc_user"})))
  @patch("ILCDIRAC.Interfaces.API.NewInterface.UserJob.UserJob.setPlatform", new=Mock(return_value=S_OK()))
  def test_root3(self):
    """create test for root 3"""
    jobs = self.myTests.createRootMacroTest()
    self.assertTrue ( jobs['OK'] )
    thisJob = jobs['Value']
    res = self.myTests.runJobLocally(thisJob, "Root")
    self.assertTrue ( res['OK'] )

def runTests():
  """runs the tests"""
  #clip = CLIParams()
  #clip.registerSwitches()
  #Script.parseCommandLine()

  suite = unittest.defaultTestLoader.loadTestsFromTestCase( JobTestCase )
  testResult = unittest.TextTestRunner( verbosity = 1 ).run( suite )
  print testResult


def runUtilitiesTest():
  """runs the utilities test only"""
  #clip = CLIParams()
  #clip.registerSwitches()
  #Script.parseCommandLine()

  suite = unittest.TestSuite()
  suite.addTest(JobTestCase('test_utilities'))
  testResult = unittest.TextTestRunner( verbosity = 1 ).run( suite )
  print testResult

def runMokkaTest():
  """runs the utilities test only"""
  #clip = CLIParams()
  #clip.registerSwitches()
  #Script.parseCommandLine()

  suite = unittest.TestSuite()
  suite.addTest(JobTestCase('test_mokka'))
  testResult = unittest.TextTestRunner( verbosity = 1 ).run( suite )
  print testResult
  

if __name__ == '__main__':
  runTests()
  #runUtilitiesTest()
  #runMokkaTest()
  