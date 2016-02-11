"""
Helper methods to create proper tarballs
"""

import os
import commands
import re

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
  if status != 0:
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

def copyFolder( basePath, targetFolder ):
  """copy folder basePath to targetFolder """
  commandString = RSYNCBASE+" -avzL  %s %s " % ( basePath, targetFolder)
  print commandString
  status, copyOut = commands.getstatusoutput( commandString )
  if status != 0:
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

def getGeant4DataFolders( variable, targetFolder ):
  path = os.environ[variable]
  copyFolder(path, targetFolder)


def getRootStuff( rootsys, targetFolder ):
  """copy the root stuff we need """
  print "Copying Root"
  status, copyOut = commands.getstatusoutput( RSYNCBASE+" -av %(rootsys)s/lib %(rootsys)s/etc %(rootsys)s/bin %(rootsys)s/cint  %(targetFolder)s" % dict( rootsys=rootsys,
                                                                                                                                                          targetFolder=targetFolder) )
  if status != 0:
    print copyOut
    raise RuntimeError( "Error during rsync" )

  libraries = getFiles( targetFolder+"/lib", ".so" )
  allLibs = set()
  for lib in libraries:
    allLibs.update( getDependentLibraries(lib) )
  copyLibraries( allLibs, targetFolder+"/lib" )
