'''
ILCDIRAC.Core.Utilities.ResolveDependencies

Set of functions used to resolve the applications' dependencies, looking into the CS

Works recursively

@since: Apr 26, 2010

@author: Stephane Poss
'''

from DIRAC import gConfig, gLogger

def resolveDeps(sysconfig, appli, appversion):
  """ Resolve the dependencies
  
  @param sysconfig: system configuration
  @type sysconfig: string
  @param appli: application name
  @type appli: string
  @param appversion: application version
  @type appversion: string
  
  @return: array of dictionaries
  """
  deps = gConfig.getSections('/Operations/AvailableTarBalls/%s/%s/%s/Dependencies' % (sysconfig, appli, appversion), '')
  depsarray = []
  if deps['OK']:
    for dep in deps['Value']:
      vers = gConfig.getOption('/Operations/AvailableTarBalls/%s/%s/%s/Dependencies/%s/version' % (sysconfig, appli, appversion, dep), '')
      depvers = ''
      if vers['OK']:
        depvers = vers['Value']
      else:
        gLogger.error("Retrieving dependency version for %s failed, skipping to next !" % (dep))
        continue
      gLogger.verbose("Found dependency %s %s" % (dep, depvers))
      depdict = {}
      depdict["app"] = dep
      depdict["version"] = depvers
      depsarray.append(depdict)
      ##resolve recursive dependencies
      depsofdeps = resolveDeps(sysconfig, dep, depvers)
      depsarray.extend(depsofdeps)
  else:
    gLogger.verbose("Could not find any dependency for %s %s, ignoring" % (appli, appversion))
  return depsarray

def resolveDepsTar(sysconfig, appli, appversion):
  """ Return the dependency tar ball name, if available
  
  Uses same parameters as L{resolveDeps}.
  @return: array of strings
  """
  deparray = resolveDeps(sysconfig, appli, appversion)
  depsarray = []
  for dep in deparray:
    dep_tar = gConfig.getOption('/Operations/AvailableTarBalls/%s/%s/%s/TarBall' % (sysconfig, dep["app"], dep["version"]), '')
    if dep_tar['OK']:
      depsarray.append(dep_tar["Value"])
    else:
      gLogger.error("Dependency %s version %s is not defined in CS, please check !" % (dep["app"], dep["version"]))         
  return depsarray
