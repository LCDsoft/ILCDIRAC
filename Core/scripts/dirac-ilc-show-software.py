#!/bin/env python
'''
Get all software registered in the Configuration service

Created on Dec 17, 2010

@author: sposs
'''
from DIRAC.Core.Base import Script
from DIRAC import S_OK

class Params(object):
  def __init__(self):
    self.all = False
    self.platform = 'x86_64-slc5-gcc43-opt'
    self.software = ''
    
  def setAll(self,opt):
    self.all = True
    return S_OK()
  
  def setPlatform(self,opt):
    self.platform = opt
    return S_OK()
  def setSoftware(self,opt):
    self.software = opt.lower()
    return S_OK()
  
  def registerSwitches(self):
    Script.registerSwitch("a", "all", "Show all available", self.setAll)
    Script.registerSwitch("S:","Software=","Software to lookup",self.setSoftware)
    Script.registerSwitch("P:","Platform=","Platform (ex. %s)"%self.platform,self.setPlatform)
    
    Script.setUsageMessage("%s" % Script.scriptName)
    

if __name__=="__main__":
  cli_p = Params()
  cli_p.registerSwitches()
  Script.parseCommandLine()
  from DIRAC import gConfig, gLogger, exit as dexit
  base = '/Operations/Defaults/AvailableTarBalls'
  platforms = gConfig.getSections(base)['Value']
  if not cli_p.all:
    if cli_p.platform in platforms:
      platforms = [cli_p.platform]
    else:
      gLogger.error("Cannot use this platform, it's not defined.")
      dexit(1)
      
  for platform in platforms:
    gLogger.notice("For platform %s, here is the available software:" % platform)
    apps = gConfig.getSections(base + "/" + platform)
    for app in apps['Value']:
      if not cli_p.all and cli_p.software:
        if cli_p.software != app:
          continue
      gLogger.notice("   - %s" % app)
      versions = gConfig.getSections(base + "/" + platform + "/" + app)
      for vers in  versions['Value']:
        gLogger.notice("     * %s" % vers)
        depsb = gConfig.getSections(base + "/" + platform + "/" + app + "/" + vers)
        if len(depsb['Value']):
          gLogger.notice("       Depends on")
          deps = gConfig.getSections(base + "/" + platform + "/" + app + "/" + vers + "/Dependencies")
          for dep in deps['Value']:
            depversions = gConfig.getOption(base + "/" + platform + "/" + app + "/" + vers + "/Dependencies/" + dep + "/version")
            gLogger.notice("         %s %s" % (dep, depversions['Value']))
                      
      if not len(versions['Value']):
        gLogger.notice("      No version available")

