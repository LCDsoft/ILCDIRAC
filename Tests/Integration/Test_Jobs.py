#!/usr/bin/env python
'''
Run many different applications as a test. Creates a temp directory and runs in there.
Stops at any error.

:since: Nov 8, 2013

:author: sposs
'''

from __future__ import print_function
import unittest
import os
import pwd
from mock import patch, MagicMock as Mock

import pytest
from DIRAC import S_OK
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertDiracSucceeds, running_on_docker

__RCSID__ = "$Id$"

MODULEBASE_NAME = 'ILCDIRAC.Workflow.Modules.ModuleBase'
USERJOB_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.UserJob'


@pytest.mark.integration
class JobTestCase(unittest.TestCase):
  """ Base class for the ProductionJob test cases
  """

  @classmethod
  def setUpClass(cls):
    """Read in parameters etc."""
    #clip = CLIParams()       # Already in setUp
    #clip.registerSwitches()
    from DIRAC.Core.Base import Script
    Script.parseCommandLine()  # Perform only once, multiple invocations can cause small issues

  def tearDown(self):
    os.chdir(self.basedir)

  def setUp(self):
    """set up the objects"""
    super(JobTestCase, self).setUp()
    from ILCDIRAC.Tests.Utilities.JobTestUtils import CLIParams

    self.basedir = os.getcwd()

    clip = CLIParams()
    clip.testOverlay = True
    clip.testChain = True
    clip.testMokka = True
    clip.testInputData = True
    clip.testWhizard = True
    clip.testUtilities = True
    overlayrun = clip.testOverlay
    clip.testRoot = True
    clip.testFccSw = True
    clip.testFccAnalysis = True
    myMarlinSteeringFile = "bbudsc_3evt_stdreco.xml"
    myLCSimPreSteeringFile = "clic_cdr_prePandoraOverlay_1400.0.lcsim" if overlayrun else "clic_cdr_prePandora.lcsim"
    myLCSimPostSteeringFile = "clic_cdr_postPandoraOverlay.lcsim"
    myFccSwSteeringFile = os.path.join(os.environ['DIRAC'], 'ILCDIRAC', 'Testfiles', 'geant_fastsim.py')
    myFccAnalysisSteeringFile = '/cvmfs/fcc.cern.ch/sw/0.8.1/fcc-physics/0.2.1/x86_64-slc6-gcc62-opt/share/ee_ZH_Zmumu_Hbb.txt'
    myFccSwPath = "/cvmfs/fcc.cern.ch/sw/0.8.1/fccsw/0.8.1/x86_64-slc6-gcc62-opt"

    parameterDict = dict(mokkaVersion='ILCSoft-02-01_gcc82',
                         mokkaSteeringFile="bbudsc_3evt.steer",
                         detectorModel="ILD_o1_v05",
                         machine="ilc_dbd",
                         backgroundType="aa_lowpt",
                         energy=350.0,
                         marlinVersion='ILCSoft-02-01_gcc82',
                         marlinSteeringFile=myMarlinSteeringFile,
                         alwaysOverlay=True,
                         marlinInputData="/ilc/user/s/sailer/testILDsim.slcio",
                         ildConfig='v02-01',
                         gearFile='GearOutput.xml',
                         lcsimPreSteeringFile=myLCSimPreSteeringFile,
                         lcsimPostSteeringFile=myLCSimPostSteeringFile,
                         ddsimVersion='ILCSoft-2020-02-07_gcc62',
                         ddsimDetectorModel='CLIC_o2_v04',
                         ddsimInputFile="Muon_50GeV_Fixed_cosTheta0.7.stdhep",
                         inputFilesPath='LFN:/ilc/user/s/simoniel/stdhep_files/ttbar_3TeV/',
                         rootVersion='ILCSoft-02-01_gcc82',

                         fccSwSteeringFile=myFccSwSteeringFile,
                         fccAnalysisSteeringFile=myFccAnalysisSteeringFile,
                         fccSwPath=myFccSwPath,

                         whizard2Version="2.3.1",
                         whizard2SinFile="Testfiles/whizard2_sample.sin",

                         )
    from ILCDIRAC.Tests.Utilities.JobTestUtils import JobCreater
    self.myTests = JobCreater(clip, parameterDict)
    # Differentiate between local execution and execution in docker
    localsitelocalarea = ''
    uid = os.getuid()
    user_info = pwd.getpwuid(uid)
    homedir = os.path.join(os.sep + 'home', user_info.pw_name)
    cvmfstestsdir = 'cvmfstests'
    if running_on_docker():
      localsitelocalarea = os.path.join(os.getcwd(), cvmfstestsdir)
    else:
      localsitelocalarea = os.path.join(homedir, cvmfstestsdir)
    from DIRAC import gConfig
    gConfig.setOptionValue('/LocalSite/LocalArea', localsitelocalarea)
    gConfig.setOptionValue('/LocalSite/LocalSE', "CERN-DIP-4")
    #gConfig.setOptionValue( '/Operations/Defaults/AvailableTarBalls/x86_64-slc5-gcc43-opt/steeringfiles/V16/Overwrite', 'False' )
    #gConfig.setOptionValue( '/Operations/Defaults/AvailableTarBalls/x86_64-slc5-gcc43-opt/steeringfiles/V18/Overwrite', 'False' )
    #gConfig.setOptionValue( '/Operations/Defaults/AvailableTarBalls/x86_64-slc5-gcc43-opt/stdhepcutjava/1.0/Overwrite', 'False' )
    gConfig.setOptionValue('/Resources/Countries/local/AssignedTo', 'ch')

  #@unittest.skip("Temporarily disabled due to length")

  @patch("%s.getProxyInfoAsString" % MODULEBASE_NAME, new=Mock(return_value=S_OK()))
  @patch("%s.getProxyInfo" % USERJOB_NAME, new=Mock(return_value=S_OK({"group": "ilc_user"})))
  @patch("%s.UserJob.setPlatform" % USERJOB_NAME, new=Mock(return_value=S_OK()))
  def test_mokka(self):
    """create test for mokka"""
    print("mokka test")
    jobs = self.myTests.createMokkaTest()
    assertDiracSucceeds(jobs, self)
    thisJob = jobs['Value']
    res = self.myTests.runJobLocally(thisJob, "Mokka")
    assertDiracSucceeds(res, self)

  #@unittest.skip("Temporarily disabled due to length")
  @patch("%s.getProxyInfoAsString" % MODULEBASE_NAME, new=Mock(return_value=S_OK()))
  @patch("%s.getProxyInfo" % USERJOB_NAME, new=Mock(return_value=S_OK({"group": "ilc_user"})))
  @patch("%s.UserJob.setPlatform" % USERJOB_NAME, new=Mock(return_value=S_OK()))
  def test_ddsim(self):
    """create tests for ddsim"""
    print("ddsimtest")
    # First run, all files available
    jobs = self.myTests.createDDSimTest()
    assertDiracSucceeds(jobs, self)
    thisJob = jobs['Value']
    res = self.myTests.runJobLocally(thisJob, "DDSim")
    assertDiracSucceeds(res, self)

    ddsimInputFile = "Muon_50GeV_Fixed_cosTheta0.7.stdhep"
    ddsimTarball = "FCalTB.tar.gz"

    # Replace inputfile with 00.stdhep
    jobs = self.myTests.createDDSimTest(ddsimInputFile, ddsimTarball)
    assertDiracSucceeds(jobs, self)
    thisJob = jobs['Value']
    res = self.myTests.runJobLocally(thisJob, "DDSim")
    assertDiracSucceeds(res, self)

  #@unittest.skip("Temporarily disabled due to length")
  @patch("%s.getProxyInfoAsString" % MODULEBASE_NAME, new=Mock(return_value=S_OK()))
  @patch("%s.getProxyInfo" % USERJOB_NAME, new=Mock(return_value=S_OK({"group": "ilc_user"})))
  @patch("%s.UserJob.setPlatform" % USERJOB_NAME, new=Mock(return_value=S_OK()))
  def test_marlin(self):
    """create test for marlin"""
    print("marlin test")
    jobs = self.myTests.createMarlinTest()
    assertDiracSucceeds(jobs, self)
    thisJob = jobs['Value']
    res = self.myTests.runJobLocally(thisJob, "Marlin")
    assertDiracSucceeds(res, self)

  #@unittest.skip("Temporarily disabled due to length")
  @patch("%s.getProxyInfoAsString" % MODULEBASE_NAME, new=Mock(return_value=S_OK()))
  @patch("%s.getProxyInfo" % USERJOB_NAME, new=Mock(return_value=S_OK({"group": "ilc_user"})))
  @patch("%s.UserJob.setPlatform" % USERJOB_NAME, new=Mock(return_value=S_OK()))
  def test_marlin2(self):
    """create test for marlin"""
    print("marlin test2")
    jobs = self.myTests.createMarlinTest(True)
    assertDiracSucceeds(jobs, self)
    thisJob = jobs['Value']
    res = self.myTests.runJobLocally(thisJob, "Marlin")
    assertDiracSucceeds(res, self)

  #@unittest.skip("Temporarily disabled due to length")
  @patch("%s.getProxyInfoAsString" % MODULEBASE_NAME, new=Mock(return_value=S_OK()))
  @patch("%s.getProxyInfo" % USERJOB_NAME, new=Mock(return_value=S_OK({"group": "ilc_user"})))
  @patch("%s.UserJob.setPlatform" % USERJOB_NAME, new=Mock(return_value=S_OK()))
  def test_whizard(self):
    """create test for whizard"""
    print("whizard test")
    jobs = self.myTests.createWhizardTest()
    assertDiracSucceeds(jobs, self)
    theseJobs = jobs['Value']
    for thisJob in theseJobs:
      res = self.myTests.runJobLocally(thisJob, "Whizard")
      assertDiracSucceeds(res, self)

  #@unittest.skip("Temporarily disabled due to length")
  @patch("%s.getProxyInfoAsString" % MODULEBASE_NAME, new=Mock(return_value=S_OK()))
  @patch("%s.getProxyInfo" % USERJOB_NAME, new=Mock(return_value=S_OK({"group": "ilc_user"})))
  @patch("%s.UserJob.setPlatform" % USERJOB_NAME, new=Mock(return_value=S_OK()))
  def test_utilities(self):
    """create test for utilities"""
    print("Utilities test")
    jobs = self.myTests.createUtilityTests()
    assertDiracSucceeds(jobs, self)
    theseJobs = jobs['Value']
    for thisJob in theseJobs:
      res = self.myTests.runJobLocally(thisJob, "Utility")
      assertDiracSucceeds(res, self)

  #@unittest.skip("Temporarily disabled due to length")
  @patch("%s.getProxyInfoAsString" % MODULEBASE_NAME, new=Mock(return_value=S_OK()))
  @patch("%s.getProxyInfo" % USERJOB_NAME, new=Mock(return_value=S_OK({"group": "ilc_user"})))
  @patch("%s.UserJob.setPlatform" % USERJOB_NAME, new=Mock(return_value=S_OK()))
  def test_root(self):
    """create test for root 1"""
    print("test root")
    jobs = self.myTests.createRootScriptTest()
    assertDiracSucceeds(jobs, self)
    thisJob = jobs['Value']
    res = self.myTests.runJobLocally(thisJob, "Root")
    assertDiracSucceeds(res, self)

  #@unittest.skip("Temporarily disabled due to length")
  @patch("%s.getProxyInfoAsString" % MODULEBASE_NAME, new=Mock(return_value=S_OK()))
  @patch("%s.getProxyInfo" % USERJOB_NAME, new=Mock(return_value=S_OK({"group": "ilc_user"})))
  @patch("%s.UserJob.setPlatform" % USERJOB_NAME, new=Mock(return_value=S_OK()))
  def test_root2(self):
    """create test for root 2"""
    print("test root2")
    jobs = self.myTests.createRootHaddTest()
    assertDiracSucceeds(jobs, self)
    thisJob = jobs['Value']
    res = self.myTests.runJobLocally(thisJob, "Root")
    assertDiracSucceeds(res, self)

  #@unittest.skip("Temporarily disabled due to length")
  @patch("%s.getProxyInfoAsString" % MODULEBASE_NAME, new=Mock(return_value=S_OK()))
  @patch("%s.getProxyInfo" % USERJOB_NAME, new=Mock(return_value=S_OK({"group": "ilc_user"})))
  @patch("%s.UserJob.setPlatform" % USERJOB_NAME, new=Mock(return_value=S_OK()))
  def test_root3(self):
    """create test for root 3"""
    print("test root3")
    jobs = self.myTests.createRootMacroTest()
    assertDiracSucceeds(jobs, self)
    thisJob = jobs['Value']
    res = self.myTests.runJobLocally(thisJob, "Root")
    assertDiracSucceeds(res, self)

  #@unittest.skip("Temporarily disabled due to length")
  @patch("%s.getProxyInfoAsString" % MODULEBASE_NAME, new=Mock(return_value=S_OK()))
  @patch("%s.getProxyInfo" % USERJOB_NAME, new=Mock(return_value=S_OK({"group": "ilc_user"})))
  @patch("%s.UserJob.setPlatform" % USERJOB_NAME, new=Mock(return_value=S_OK()))
  def test_fccsw(self):
    """create test for fccsw"""
    print("fccsw test")
    jobs = self.myTests.createFccSwTest()
    assertDiracSucceeds(jobs, self)
    thisJob = jobs['Value']
    res = self.myTests.runJobLocally(thisJob, "FccSw")
    assertDiracSucceeds(res, self)

  #@unittest.skip("Temporarily disabled due to length")
  @patch("%s.getProxyInfoAsString" % MODULEBASE_NAME, new=Mock(return_value=S_OK()))
  @patch("%s.getProxyInfo" % USERJOB_NAME, new=Mock(return_value=S_OK({"group": "ilc_user"})))
  @patch("%s.UserJob.setPlatform" % USERJOB_NAME, new=Mock(return_value=S_OK()))
  def test_fccanalysis(self):
    """create test for fccanalysis"""
    print("fccanalysis test")
    jobs = self.myTests.createFccAnalysisTest()
    assertDiracSucceeds(jobs, self)
    thisJob = jobs['Value']
    res = self.myTests.runJobLocally(thisJob, "FccAnalysis")

  #@unittest.skip("Temporarily disabled due to length")
  @patch("%s.getProxyInfoAsString" % MODULEBASE_NAME, new=Mock(return_value=S_OK()))
  @patch("%s.getProxyInfo" % USERJOB_NAME, new=Mock(return_value=S_OK({"group": "ilc_user"})))
  @patch("%s.UserJob.setPlatform" % USERJOB_NAME, new=Mock(return_value=S_OK()))
  def test_whizard2(self):
    """create tests for whizard2"""
    print("whizard2test")
    # First run, all files available
    jobs = self.myTests.createWhizard2Test()
    assertDiracSucceeds(jobs, self)
    thisJob = jobs['Value']
    res = self.myTests.runJobLocally(thisJob, "Whizard2")
    assertDiracSucceeds(res, self)


def runTests():
  """runs the tests"""
  #clip = CLIParams()          # See setUpClass
  #clip.registerSwitches()
  #Script.parseCommandLine()

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(JobTestCase)
  testResult = unittest.TextTestRunner(verbosity=1).run(suite)
  print(testResult)


def runUtilitiesTest():
  """runs the utilities test only"""
  #clip = CLIParams()
  #clip.registerSwitches()
  #Script.parseCommandLine()

  suite = unittest.TestSuite()
  suite.addTest(JobTestCase('test_utilities'))
  testResult = unittest.TextTestRunner(verbosity=1).run(suite)
  print(testResult)


def runMokkaTest():
  """runs the utilities test only"""
  #clip = CLIParams()
  #clip.registerSwitches()
  #Script.parseCommandLine()

  suite = unittest.TestSuite()
  suite.addTest(JobTestCase('test_mokka'))
  testResult = unittest.TextTestRunner(verbosity=1).run(suite)
  print(testResult)


def runDDSimTest():
  """runs the ddsim test only"""
  #Script.parseCommandLine()
  suite = unittest.TestSuite()
  suite.addTest(JobTestCase('test_ddsim'))
  testResult = unittest.TextTestRunner(verbosity=1).run(suite)
  print(testResult)

# TO UNCOMMENT when Detector folder of FCCSW will be on CVMFS
#def runFccSwTest():
#  """runs the fccsw test only"""
#  #Script.parseCommandLine()
#  suite = unittest.TestSuite()
#  suite.addTest(JobTestCase('test_fccsw'))
#  testResult = unittest.TextTestRunner( verbosity = 1 ).run( suite )
#  print testResult


def runFccAnalysisTest():
  """runs the fccanalysis test only"""
  #Script.parseCommandLine()
  suite = unittest.TestSuite()
  suite.addTest(JobTestCase('test_fccanalysis'))


def runWhizard2Test():
  """runs the Whizard test only"""
  #Script.parseCommandLine()
  suite = unittest.TestSuite()
  suite.addTest(JobTestCase('test_whizard2'))
  testResult = unittest.TextTestRunner(verbosity=1).run(suite)
  print(testResult)


if __name__ == '__main__':
  runTests()
  #runUtilitiesTest()
  #runMokkaTest()
  #runDDSimTest()
  #runWhizard2Test()
