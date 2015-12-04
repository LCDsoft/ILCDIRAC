#!/usr/bin/env python
"""
This program will create a tar ball suitable for running the program on the grid with ILCDIRAC
Needs the chrpath and readelf utilities
"""

import sys
import os
import commands
import re
from pprint import pprint

LCGEO_ENV="lcgeo_DIR"
DDHEP_ENV="DD4hep_DIR"

RSYNCBASE="rsync --exclude '.svn'"

def killRPath( folder ):
  """remove rpath from all libraries in folder and below"""
  for root,_dirs,files in os.walk(folder, followlinks=True):
    for fil in files:
      if fil.count(".so"):
        commands.getstatusoutput( "chrpath -d %s " % os.path.join(root, fil) )

def getFiles( folder, ext ):
  """ return a list of files with ext in the folder """
  libraries = set()
  for root,_dirs,files in os.walk(folder, followlinks=True):
    for fil in files:
      if fil.count(ext):
        fullPath = os.path.join(root, fil )
        libraries.add(fullPath)
  return libraries

def copyLibraries( files, targetFolder, ):
  """rsync all the files to the targetFolder """
  print "Copying files to",targetFolder
  listOfFiles = " ".join(files)
  status, copyOut = commands.getstatusoutput( RSYNCBASE+" -avzL  %s %s " % ( listOfFiles, targetFolder) )
  if not status == 0:
    print copyOut
    raise RuntimeError( "Error during rsync" )


def resolveLinks( targetFolder ):
  """ if library is there twice make a link from one to the other """
  cwd = os.getcwd()
  os.chdir(targetFolder)
  files = getFiles( targetFolder, ".so" )
  print files
  files = [ os.path.basename(fil) for fil in files ]
  print "files",files
  for lib in files:
    matchingLib = next(( x for x in files if lib+"." in x), None)
    if matchingLib:
      print "going to link",lib,"with", matchingLib
      os.remove(lib)
      os.symlink( os.path.basename(matchingLib), os.path.basename(lib) )
  os.chdir(cwd)

def getLibraryPath( basePath ):
  """ return the path to the libraryfolder """
  return os.path.join( basePath, "lib")

def copyDetectorModels( basePath, folder, targetFolder ):
  """copy the compact folders to the targetFolder """
  for root,dirs,_files in os.walk( os.path.join(basePath, folder) ):
    for direct in dirs:
      if root.endswith("compact"):
        copyFolder( os.path.join(root, direct),  targetFolder )
  

def copyFolder( basePath, targetFolder ):
  """copy folder basePath to targetFolder """
  commandString = RSYNCBASE+" -avzL  %s %s " % ( basePath, targetFolder)
  print commandString
  status, copyOut = commands.getstatusoutput( commandString )
  if not status == 0:
    print copyOut
    raise RuntimeError( "Error during rsync" )
  
def getPythonStuff( basePath, targetFolder ):
  """ copy the python stuff from basePath to targetFolder """
  copyFolder( basePath, targetFolder )

  
def removeSystemLibraries( folder ):
  """remove the system libraries from the folder
  #FIXME: get this from ILCDIRAC
    for file in libc.so* libc-2.5.so* libm.so* libpthread.so* libdl.so* libstdc++.so* libgcc_s.so.1*; do
	rm $LIBFOLDER/$file 2> /dev/null
    done
  """
  systemLibraires = ['libc.so', 'libc-2.5.so', 'libm.so', 'libpthread.so', 'libdl.so',
                     'libstdc++.so', 'libgcc_s.so.1' ]

  for root,_dirs,files in os.walk(folder):
    for fil in files:
      if any( lib in fil for lib in systemLibraires):
        fullPath = os.path.join( root, fil )
        print "Removing:",fullPath
        try:
          os.remove( fullPath )
        except OSError:
          print "Error to remove",fullPath


  
def getDependentLibraries( library ):
  """ get all shared objects the library depends on
    string1=$(ldd $programpath | grep "=>" | sed 's/.*=>//g' | sed "s/(.*)//g")
  """
  libraries=set()
  _status, lddOutput = commands.getstatusoutput( " ".join(["ldd", library]) )
  for line in lddOutput.splitlines():
    match = re.match(r'\t.*=> (.*) \(0x', line)
    if match:
      libraries.add(match.group(1))
  return libraries


def cleanRPath( folder ):
  """ remove rpath from all libraries and executables in the folder
    # for file in $( ls --color=never $LIBFOLDER/* ); do
    # 	chrpath -d $file
    # 	readelf -d $file | grep RPATH
    # 	if [ $? == 0 ]; then
    # 	    echo "FOUND RPATH Aborting!!"
    # 	    exit 1
    # 	fi
    # done
  """
  pass
  
def createTarBall( name, version, folder ):
  """create a tar ball from the folder
    tar zcf $TARBALLNAME $LIBFOLDER/*
  """
  pass

def getRootStuff( rootsys, targetFolder ):
  """copy the root stuff we need """
  print "Copying Root"
  status, copyOut = commands.getstatusoutput( RSYNCBASE+" -av %(rootsys)s/lib %(rootsys)s/etc %(rootsys)s/bin %(rootsys)s/cint  %(targetFolder)s" % dict(rootsys=rootsys,
                                                                                                                                                                     targetFolder=targetFolder) )
  if not status == 0:
    print copyOut
    raise RuntimeError( "Error during rsync" )

  libraries = getFiles( targetFolder+"/lib", ".so" )
  allLibs = set()
  for lib in libraries:
    allLibs.update( getDependentLibraries(lib) )
  copyLibraries( allLibs, targetFolder+"/lib" )


def parseArgs():
  """ parse the command line arguments"""
  if len(sys.argv) != 3:
    raise RuntimeError( "Wrong number of arguments in call: '%s'" % " ".join(sys.argv) )
  return (sys.argv[1], sys.argv[2])

def checkEnvironment():
  """ check if dd4hep and lcgeo are in the environment """
  if not ( DDHEP_ENV in os.environ and LCGEO_ENV in os.environ and 'ROOTSYS' in os.environ):
    raise RuntimeError( "ROOTSYS, or %s or %s not set" % (DDHEP_ENV, LCGEO_ENV) )
  return os.environ[DDHEP_ENV], os.environ[LCGEO_ENV], os.environ['ROOTSYS']

def getGeant4DataFolders( variable, targetFolder ):
  path = os.environ[variable]
  copyFolder(path, targetFolder)

  
def createDDSimTarBall():
  """ do everything to create the DDSim tarball"""
  name, version = parseArgs()
  ddBase, lcgeoBase, rootsys = checkEnvironment()

  realTargetFolder = os.path.join( os.getcwd(), name+version )
  targetFolder = os.path.join( os.getcwd(), "temp", name+version )
  for folder in (targetFolder, targetFolder+"/lib"):
    try:
      os.makedirs( folder )
    except OSError:
      pass

  libraries = set()
  rootmaps = set()

  dd4hepLibPath = getLibraryPath( ddBase )
  lcgeoPath = getLibraryPath( lcgeoBase )


  copyDetectorModels( lcgeoBase, "CLIC" , targetFolder+"/detectors" )
  copyDetectorModels( lcgeoBase, "ILD"  , targetFolder+"/detectors" )

  
  libraries.update( getFiles( dd4hepLibPath, ".so") )
  libraries.update( getFiles( lcgeoPath, ".so" ) )

  rootmaps.update( getFiles( dd4hepLibPath, ".rootmap") )
  rootmaps.update( getFiles( lcgeoPath, ".rootmap" ) )
  
  pprint( libraries )
  pprint( rootmaps )

  
  allLibs = set()
  for lib in libraries:
    allLibs.update( getDependentLibraries(lib) )
  ### remote root and geant4 libraries, we pick them up from
  allLibs = set( [ lib for lib in allLibs if not ( "/geant4/" in lib.lower() or "/root/" in lib.lower()) ] )

  print allLibs
  
  copyLibraries( libraries, targetFolder+"/lib" )
  copyLibraries( allLibs, targetFolder+"/lib" )
  copyLibraries( rootmaps, targetFolder+"/lib" )

  getPythonStuff( ddBase+"/python"       , targetFolder+"/lib/")
  getPythonStuff( lcgeoBase+"/lib/python", targetFolder+"/lib/" )
  getPythonStuff( lcgeoBase+"/bin/ddsim", targetFolder+"/bin/" )


  ##Should get this from CVMFS
  #getRootStuff( rootsys, targetFolder+"/ROOT" )

  copyFolder( targetFolder+"/", realTargetFolder.rstrip("/") )

  killRPath( realTargetFolder )
  resolveLinks( realTargetFolder+"/lib" )
  removeSystemLibraries( realTargetFolder+"/lib" )
  #removeSystemLibraries( realTargetFolder+"/ROOT/lib" )
  
if __name__=="__main__":
  print "Creating Tarball for DDSim"
  try:
    createDDSimTarBall()
  except RuntimeError as e:
    print "ERROR during tarball creation: %s " % e
    exit(1)
  exit(0)
