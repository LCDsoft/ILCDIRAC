"""Print all applications and their versions registered in the configuration service.

Options:

   -S, --software software       show versions for this application only
   -A, --apps                    show only available Applications, not their versions

:since: Dec 17, 2010
:author: sposs
"""
__RCSID__ = "$Id$"

import os

from DIRAC.Core.Base import Script
from DIRAC import S_OK

class _Params(object):
  """Parameter Object"""
  def __init__(self):
    self.software = ''
    self.appsOnly = False
  def setSoftware(self,opt):
    self.software = opt
    return S_OK()

  def setAppsOnly(self,_):
    self.appsOnly = True
    return S_OK()

  def registerSwitches(self):
    Script.registerSwitch("S:", "software=", "show versions for this software", self.setSoftware)
    Script.registerSwitch("A", "apps", "show only available Applications, not their versions", self.setAppsOnly)
    Script.setUsageMessage("""%s """ % ("dirac-ilc-show-software",) )


def _showSoftware():
  """Show available software"""
  clip = _Params()
  clip.registerSwitches()
  Script.parseCommandLine()
  from DIRAC import gConfig, gLogger

  base = '/Operations/Defaults/AvailableTarBalls'
  platforms = gConfig.getSections(base)
  
  for platform in platforms['Value']:
    gLogger.notice("For platform %s, here are the available software" % platform)
    apps = gConfig.getSections(base + "/" + platform)
    for app in apps['Value']:
      if clip.software and app.lower() != clip.software.lower():
        continue
      gLogger.notice("   - %s" % app)
      versions = gConfig.getSections(base + "/" + platform + "/" + app)
      if clip.appsOnly:
        continue
      for vers in  versions['Value']:
        gLogger.notice("     * %s" % vers)
        depsb = gConfig.getSections(base + "/" + platform + "/" + app + "/" + vers)
        if 'Dependencies' in depsb['Value']:
          gLogger.notice("       Depends on")
          deps = gConfig.getSections( os.path.join( base, platform,  app,  vers , "Dependencies") )
          for dep in deps['Value']:
            depversions = gConfig.getOption(base + "/" + platform + "/" + app + "/" + vers + "/Dependencies/" + dep + "/version")
            gLogger.notice("         %s %s" % (dep, depversions['Value']))
                      
      if not len(versions['Value']):
        gLogger.notice("      No version available")

if __name__=="__main__":
  _showSoftware()
