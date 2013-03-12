#!/bin/env python
'''
Created on Dec 17, 2010

@author: sposs
'''
from DIRAC.Core.Base import Script



if __name__=="__main__":
  Script.parseCommandLine()
  from DIRAC import gConfig, gLogger
  base = '/Operations/Defaults/AvailableTarBalls'

  platforms = gConfig.getSections(base)
  
  for platform in platforms['Value']:
    print "For platform %s, here are the available software" % platform
    apps = gConfig.getSections(base + "/" + platform)
    for app in apps['Value']:
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

