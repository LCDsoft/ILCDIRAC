#!/bin/env python
'''
Run many different applications as a test. Creates a temp directory and runs in there.
Stops at any error.

@since: Nov 8, 2013

@author: sposs
'''
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC import exit as dexit

from ILCDIRAC.Interfaces.API.NewInterface.Tests.LocalTestObjects import TestCreater, CLIParams

def runTests():
  """runs the tests"""
  clip = CLIParams()
  clip.registerSwitches()
  Script.parseCommandLine()

  overlayrun = clip.testOverlay
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
                        lcsimPostSteeringFile=myLCSimPostSteeringFile
                      )

  myTests = TestCreater(clip, parameterDict)
  res = myTests.checkForTests()
  if not res['OK']:
    dexit(1)
  myTests.run()

  return

if __name__ == '__main__':
  runTests()
