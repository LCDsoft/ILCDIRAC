#!/usr/bin/env python
""" create rst files for documentation of ILCDIRAC """
import os

BASEPATH = "DOC"
ILCDIRACPATH = os.path.join(os.environ.get("DIRAC"), "ILCDIRAC" )
DIRACPATH = os.environ.get("DIRAC")

ORIGDIR = os.getcwd()

BASEPATH = os.path.join( ORIGDIR, BASEPATH )


def mkdir( folder ):
  """create a folder, ignore if it exists"""
  try:
    os.mkdir( os.path.join(os.getcwd(),folder) )
  except OSError as e:
    print "Exception",repr(e)

def mkRest( filename, modulename, fullmodulename, subpackages=None, modules=None ):
  """make a rst file for filename"""
  if modulename == "scripts":
    modulefinal = fullmodulename.split(".")[-2]+" Scripts"
  else:
    modulefinal = modulename

  lines = []
  lines.append("%s" % modulefinal)
  lines.append("="*len(modulefinal))
  lines.append(".. module:: %s " % fullmodulename )
  lines.append("" )

  if subpackages or modules:
    lines.append(".. toctree::")
    lines.append("   :maxdepth: 1")
    lines.append("")

  if subpackages:
    for package in subpackages:
      lines.append("   %s/%s.rst" % (package,package.split("/")[-1] ) )
      #lines.append("   %s " % (package, ) )

  if modules:
    for module in sorted(modules):
      lines.append("   %s.rst" % (module.split("/")[-1],) )
      #lines.append("   %s " % (package, ) )
        
  with open(filename, 'w') as rst:
    rst.write("\n".join(lines))

    
def mkModuleRest( classname, fullclassname ):
  """ create rst file for class"""
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
    if classname.startswith("_"):
      lines.append( "   :private-members:" )
    rst.write("\n".join(lines))

  
def getsubpackages( abspath, direc):
  """return list of subpackages with full path"""
  packages = []
  for dire in direc:
    if "test" in dire.lower():
      continue
    #print os.path.join( DIRACPATH,abspath,dire, "__init__.py" )
    if os.path.exists( os.path.join( DIRACPATH,abspath,dire, "__init__.py" ) ):
      #packages.append( os.path.join( "DOC", abspath, dire) )
      packages.append( os.path.join( dire) )
  #print "packages",packages
  return packages

def getmodules( _abspath, _direc, files ):
  """return list of subpackages with full path"""
  packages = []
  for filename in files:
    if "test" in filename.lower():
      continue
    if filename != "__init__.py":
      packages.append( filename.split(".py")[0] )

  return packages


def createDoc():
  """create the rst files for all the things we want them for"""
  mkdir(BASEPATH)
  os.chdir(BASEPATH)

  for root,direc,files in os.walk(ILCDIRACPATH):
    files = [ _ for _ in files if _.endswith(".py") ]
    if "__init__.py" not in files:
      continue
    if "test" in root.lower():
      continue
    #print root, direc, files
    modulename = root.split("/")[-1]
    abspath = root.split(DIRACPATH)[1].strip("/")
    fullmodulename = ".".join(abspath.split("/"))
    packages = getsubpackages(abspath,direc)
    if abspath:
      mkdir( abspath )
      os.chdir( abspath )
    #print "Making rst",modulename
    mkRest( modulename+".rst", modulename, fullmodulename, subpackages=packages, modules=getmodules(abspath, direc, files) )

    for filename in files:
      if "test" in filename.lower():
        continue
      if "__init__.py" == filename:
        continue
      if not filename.endswith(".py"):
        continue
      fullclassname = ".".join(abspath.split("/")+[filename])
      mkModuleRest( filename.split(".py")[0], fullclassname.split(".py")[0] )

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
