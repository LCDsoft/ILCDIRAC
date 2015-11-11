#!/bin/env python
'''
Run many different applications as a test. Creates a temp directory and runs in there.
Stops at any error.

:since: Nov 8, 2013

:author: sposs
'''
__RCSID__ = "$Id$"

import unittest
from DIRAC.Core.Base import Script

from ILCDIRAC.Interfaces.API.NewInterface.Tests.LocalTestObjects import TestCreater, CLIParams

class JobTestCase( unittest.TestCase ):
  """ Base class for the ProductionJob test cases
  """

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
                          energy=350,
                          marlinVersion="ILCSoft-01-17-06",
                          marlinSteeringFile=myMarlinSteeringFile,
                          alwaysOverlay = True,
                          marlinInputData="/ilc/user/s/sailer/testILDsim.slcio",
                          ildConfig = "v01-16-p10_250",
                          gearFile='GearOutput.xml',
                          lcsimPreSteeringFile=myLCSimPreSteeringFile,
                          lcsimPostSteeringFile=myLCSimPostSteeringFile,
                          rootVersion="ILCSoft-01-17-08",
                        )

    self.myTests = TestCreater(clip, parameterDict)

  def test_mokka(self):
    """create test for mokka"""
    jobs = self.myTests.createMokkaTest()
    self.assertTrue ( jobs['OK'] )
    thisJob = jobs['Value']
    res = self.myTests.runJobLocally(thisJob, "Mokka")
    self.assertTrue ( res['OK'] )

  def test_marlin(self):
    """create test for marlin"""
    jobs = self.myTests.createMarlinTest()
    self.assertTrue ( jobs['OK'] )
    thisJob = jobs['Value']
    res = self.myTests.runJobLocally(thisJob, "Marlin")
    self.assertTrue ( res['OK'] )

  def test_whizard(self):
    """create test for whizard"""
    jobs = self.myTests.createWhizardTest()
    self.assertTrue ( jobs['OK'] )
    theseJobs = jobs['Value']
    for thisJob in theseJobs:
      res = self.myTests.runJobLocally(thisJob,"Whizard")
      self.assertTrue ( res['OK'] )

  def test_utilities(self):
    """create test for utilities"""
    jobs = self.myTests.createUtilityTests()
    self.assertTrue ( jobs['OK'] )
    theseJobs = jobs['Value']
    for thisJob in theseJobs:
      res = self.myTests.runJobLocally(thisJob,"Utility")
      self.assertTrue ( res['OK'] )

  def test_root(self):
    """create test for mokka"""
    jobs = self.myTests.createRootScriptTest()
    self.assertTrue ( jobs['OK'] )
    thisJob = jobs['Value']
    res = self.myTests.runJobLocally(thisJob, "Root")
    self.assertTrue ( res['OK'] )

  def test_root2(self):
    jobs = self.myTests.createRootHaddTest()
    self.assertTrue ( jobs['OK'] )
    thisJob = jobs['Value']
    res = self.myTests.runJobLocally(thisJob, "Root")
    self.assertTrue ( res['OK'] )

def runTests():
  """runs the tests"""
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( JobTestCase )
  testResult = unittest.TextTestRunner( verbosity = 1 ).run( suite )
  print testResult


if __name__ == '__main__':
  CLIP = CLIParams()
  CLIP.registerSwitches()
  Script.parseCommandLine()

  runTests()
