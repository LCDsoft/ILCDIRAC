#!/bin/env python
""" submit jobs to all sites and checks the worker nodes for functionality"""
__RCSID__ = "$Id$"
from DIRAC.Core.Base import Script
from DIRAC import S_OK

from DIRAC import gLogger

LOG = gLogger.getSubLogger(__name__)

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
  
  def registerSwitches(self):
    Script.registerSwitch("S:", "Site=", "Site to probe", self.setSite)
    Script.registerSwitch("C:", "CE=","Computing Element to probe", self.setCE)
    Script.setUsageMessage("%s --Site LCG.CERN.ch" % Script.scriptName)
    
def testAndProbeSites():
  """submits jobs to test sites"""
  clip = Params()
  clip.registerSwitches()
  Script.parseCommandLine()
  
  from DIRAC import exit as dexit
  
  from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import CheckWNs
  from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
  
  
  from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getQueues
  
  res = getQueues(siteList = clip.site, ceList = clip.ce)
  if not res['OK']:
    LOG.error("Failed getting the queues", res['Message'])
    dexit(1)
  
  sitedict = res['Value']
  CEs = []

  for ces in sitedict.values():
    CEs.extend(ces.keys())

  LOG.notice("Found %s CEs to look at." % len(CEs))

  
  d = DiracILC(True, "SiteProbe.rep")

  for CE in CEs:
    j = UserJob()
    j.setDestinationCE(CE)
    c = CheckWNs()
    res = j.append(c)
    if not res['OK']:
      LOG.error(res['Message'])
      continue
    j.setOutputSandbox("*.log")
    j.setCPUTime(30000)
    j.dontPromptMe()
    res = j.submit(d)
    if not res['OK']:
      LOG.error("Failed to submit job, aborting")
      dexit(1)
  
  dexit(0)

if __name__=="__main__":
  testAndProbeSites()
