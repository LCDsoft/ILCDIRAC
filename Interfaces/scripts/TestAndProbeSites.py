#!/bin/env python

from DIRAC.Core.Base import Script

if __name__=="__main__":
  Script.parseCommandLine()
  
  from DIRAC import gLogger, exit as dexit
  
  gLogger.notice("This will probe the sites.")
  from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import CheckWNs
  from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
  
  dexit(0)

