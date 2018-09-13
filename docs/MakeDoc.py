#!/usr/bin/env python
"""Create rst files for documentation of ILCDIRAC."""

import os
import shutil
import logging

logging.basicConfig()
LOG = logging.getLogger(name="MakeDoc")
LOG.setLevel(logging.INFO)

ORIGDIR = os.getcwd()
DIRACPATH = os.environ.get("DIRAC")
ILCDIRACPATH = os.path.join(DIRACPATH, "ILCDIRAC")
BASEPATH = os.path.join(ORIGDIR, "DOC")

IGNORE_FOLDERS = ('productions', 'source', 'test')


def mkdir(folder):
  """Create a folder, ignore if it exists."""
  try:
    os.mkdir(os.path.join(os.getcwd(), folder))
  except OSError as e:
    LOG.debug("Exception when creating dir %s: %r", folder, e)


def mkPackageRst(filename, packagename, fullmodulename, subpackages=None, modules=None):
  """Make an rst file for a package."""
  if packagename == "scripts":
    packagefinal = fullmodulename.split(".")[-2] + " Scripts"
  else:
    packagefinal = packagename

  lines = []
  lines.append("%s" % packagefinal)
  lines.append("=" * len(packagefinal))
  lines.append(".. module:: %s " % fullmodulename)
  lines.append("")

  if subpackages or modules:
    lines.append(".. toctree::")
    lines.append("   :maxdepth: 1")
    lines.append("")

  if subpackages:
    for package in subpackages:
      lines.append("   %s/index.rst" % package)

  if modules:
    for module in sorted(modules):
      lines.append("   %s.rst" % (module.split("/")[-1],))

  with open(filename, 'w') as rst:
    rst.write("\n".join(lines))

    
def mkModuleRest( classname, fullclassname ):
  """ create rst file for class"""
  LOG.info("Creating RST file for %s", fullclassname)
  filename = classname+".rst"
  with open(filename, 'w') as rst:
    lines = []
    lines.append("%s" % classname)
    lines.append("="*len(classname))

    # if "-" not in classname:
    #   lines.append(".. autosummary::" )
    #   lines.append("   :toctree: %sGen" % classname )
    #   lines.append("")
    #   lines.append("   %s " % fullclassname )
    #   lines.append("")

    ## These diagrams look aweful, need to install graphiz package and enable extensions
    # lines.append(".. inheritance-diagram:: %s" % fullclassname )
    # lines.append("")

    lines.append(".. automodule:: %s" % fullclassname )
    lines.append("   :members:" )
    lines.append("   :inherited-members:" )
    lines.append("   :undoc-members:" )
    lines.append("   :show-inheritance:" )
    if classname.startswith("_") or any( name == classname for name in ('UserJob', 'Application' ) ):
      lines.append( "   :private-members:" )
    rst.write("\n".join(lines))


def getsubpackages(abspath, direc):
  """Return list of subpackages with full path."""
  packages = []
  for dire in direc:
    if "test" in dire.lower():
      continue
    LOG.debug("Found __init__ %s", os.path.join(DIRACPATH, abspath, dire, "__init__.py"))
    if os.path.exists(os.path.join(DIRACPATH, abspath, dire, "__init__.py")) and \
       dire.lower() not in IGNORE_FOLDERS:
      packages.append( os.path.join( dire) )
  if packages:
    packages = sorted(packages)
    LOG.info("In %r found package(s): %s", abspath, ", ".join(packages))
  return packages

def getmodules( _abspath, _direc, files ):
  """return list of subpackages with full path"""
  packages = []
  for filename in files:
    if any( part in filename.lower() for part in ('test', ) ):
      continue
    if filename != "__init__.py":
      packages.append( filename.split(".py")[0] )

  return packages


def createDoc():
  """create the rst files for all the things we want them for"""
  mkdir(BASEPATH)
  os.chdir(BASEPATH)

  for root, direc, files in os.walk(ILCDIRACPATH):
    configTemplate = [os.path.join(root, _) for _ in files if _ == 'ConfigTemplate.cfg']

    files = [ _ for _ in files if _.endswith(".py") ]
    if "__init__.py" not in files:
      continue
    if any("/%s" % dire in root.lower() for dire in IGNORE_FOLDERS):
      LOG.warn("Ignoring folder %s", root)
      continue
    #print root, direc, files
    modulename = root.split("/")[-1]
    abspath = root.split(DIRACPATH)[1].strip("/")
    fullmodulename = ".".join(abspath.split("/"))
    packages = getsubpackages(abspath, direc)
    if abspath:
      mkdir( abspath )
      os.chdir( abspath )

    LOG.info("Creating RST file %s/index.rst for %s", abspath, fullmodulename)
    mkPackageRst("index.rst", modulename, fullmodulename, subpackages=packages,
                 modules=getmodules(abspath, direc, files))

    for filename in files:
      if "test" in filename.lower():
        continue
      if filename == "__init__.py":
        continue
      if not filename.endswith(".py"):
        continue
      fullclassname = ".".join(abspath.split("/")+[filename])
      mkModuleRest( filename.split(".py")[0], fullclassname.split(".py")[0] )

    if configTemplate:
      LOG.debug("Copying %s to %s", configTemplate[0], os.path.join(BASEPATH, abspath))
      shutil.copy(configTemplate[0], os.path.join(BASEPATH, abspath))

    os.chdir(BASEPATH)
  return 0


    
# for Module in Interfaces Workflow RequestManagement ILCTransformationSystem WorkloadManagementSystem DataManagementSystem
# do

# done


# #sphinx-apidoc -T -F -H ILCDIRAC -V 23.0 -R 10 -e -M -d 1 -f -o source/rsts/Interfaces ../Interfaces/API
# sphinx-apidoc -T -F -H ILCDIRAC -V 23.0 -R 10 -e -M -d 1 -f -o source/rsts/ILCDIRAC ../


# rm source/rsts/ILCDIRAC/ILCDIRAC.Interfaces.API.NewInterface.Tests.ProductionJobTests.rst

# #sphinx-apidoc -F -H Interfaces -V 23.0 -R 10 -e -M -d 1 -f -o source/rsts/Interfaces ../Interfaces/API/NewInterface/

if __name__ == "__main__":
  exit(createDoc())
