'''
ILCDIRAC.Core.Utilities.ResolveDependencies

Set of functions used to resolve the applications' dependencies, looking into the CS

Works recursively

@since: Apr 26, 2010

@author: Stephane Poss
'''

from DIRAC import gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations            import Operations

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
  ops = Operations()
  deps = ops.getSections('/AvailableTarBalls/%s/%s/%s/Dependencies' % (sysconfig, appli, 
                                                                       appversion), '')
  depsarray = []
  if deps['OK']:
    for dep in deps['Value']:
      vers = ops.getValue('/AvailableTarBalls/%s/%s/%s/Dependencies/%s/version' % (sysconfig, appli, 
                                                                                    appversion, dep), '')
      depvers = ''
      if vers:
        depvers = vers
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
  ops = Operations()
  deparray = resolveDeps(sysconfig, appli, appversion)
  depsarray = []
  for dep in deparray:
    dep_tar = ops.getValue('/AvailableTarBalls/%s/%s/%s/TarBall' % (sysconfig, dep["app"], 
                                                                    dep["version"]), '')
    if dep_tar:
      depsarray.append(dep_tar)
    else:
      gLogger.error("Dependency %s version %s is not defined in CS, please check !" % (dep["app"], dep["version"]))         
  return depsarray
