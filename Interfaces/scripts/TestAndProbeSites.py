#!/bin/env python

from DIRAC.Core.Base import Script
from DIRAC import S_OK

class Params(object):
  def __init__(self):
    self.site = None
    self.ce = None
    
  def setSite(self, opt):
    self.site = opt
    return S_OK()
  
  def setCE(self,opt):
    self.ce = opt
    return S_OK()
  
  def registerswitches(self):
    Script.registerSwitch("S:", "Site=", "Site to probe", self.setSite)
    Script.registerSwitch("C:", "CE=","Computing Element to probe", self.setCE)
    Script.setUsageMessage("%s --Site LCG.CERN.ch" % Script.scriptName)
    
if __name__=="__main__":
  cli = Params()
  cli.registerswitches()
  Script.parseCommandLine()
  
  from DIRAC import gLogger, exit as dexit
  
  from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import CheckWNs
  from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
  
  
  from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getQueues
  
  res = getQueues(siteList = cli.site, ceList = cli.ce)
  if not res['OK']:
    gLogger.error("Failed getting the queues", res['Message'])
    dexit(1)
  
  sitedict = res['Value']
  CEs = []

  for ces in sitedict.values():
    CEs.extend(ces.keys())

  gLogger.notice("Found %s CEs to look at." % len(CEs))

  
  d = DiracILC(True, "SiteProbe.rep")

  for CE in CEs:
    j = UserJob()
    j.setDestinationCE(CE)
    c = CheckWNs()
    res = j.append(c)
    if not res['OK']:
      gLogger.error(res['Message'])
      continue
    j.setOutputSandbox("*.log")
    j.setCPUTime(30000)
    j.dontPromptMe()
    res = j.submit(d)
    if not res['OK']:
      gLogger.error("Failed to submit job, aborting")
      dexit(1)
  
  dexit(0)

