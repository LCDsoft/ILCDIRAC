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
  myMarlinSteeringFile = "clic_ild_cdr_steering_overlay_1400.0.xml" if overlayrun else "clic_ild_cdr_steering.xml"

  myLCSimPreSteeringFile = "clic_cdr_prePandoraOverlay_1400.0.lcsim" if overlayrun else "clic_cdr_prePandora.lcsim"
  myLCSimPostSteeringFile = "clic_cdr_postPandoraOverlay.lcsim"
  parameterDict = dict( mokkaVersion="0706P08",
                        mokkaSteeringFile="clic_ild_cdr.steer",
                        detectorModel="CLIC_ILD_CDR",
                        steeringFileVersion="V22",
                        machine="clic_cdr",
                        backgroundType="gghad",
                        energy=1400,
                        marlinVersion="v0111Prod",
                        rootVersion="5.34",
                        marlinSteeringFile=myMarlinSteeringFile,
                        marlinInputdata = "/ilc/user/s/sailer/testFiles/prod_clic_ild_e2e2_o_sim_2214_26.slcio",
                        gearFile='clic_ild_cdr.gear',
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
