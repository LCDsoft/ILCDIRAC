'''
Created on Dec 17, 2010

@author: sposs
'''
from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC import gConfig

import string
base = '/Operations/Defaults/AvailableTarBalls'

platforms = gConfig.getSections(base)

for platform in platforms['Value']:
  print "For platform %s, here are the available software" % platform
  apps = gConfig.getSections(base + "/" + platform)
  for app in apps['Value']:
    print "   - %s" % app
    versions = gConfig.getSections(base + "/" + platform + "/" + app)
    for vers in  versions['Value']:
      print "     * %s" % vers
      depsb = gConfig.getSections(base + "/" + platform + "/" + app + "/" + vers)
      if len(depsb['Value']):
        print "       Depends on"
        deps = gConfig.getSections(base + "/" + platform + "/" + app + "/" + vers + "/Dependencies")
        for dep in deps['Value']:
          depversions = gConfig.getOption(base + "/" + platform + "/" + app + "/" + vers + "/Dependencies/" + dep + "/version")
          print "         %s %s" % (dep, depversions['Value'])
                    
    if not len(versions['Value']):
      print "      No version available"

