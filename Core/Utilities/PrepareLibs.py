'''
Remove any system library provided in the application tar ball

:author: sposs
:since: Jan 26, 2011
'''
from DIRAC import gLogger
import os

files_to_remove = ["libc.so","libc-2.5","libm.so","libpthread.so","libdl.so", "libstdc++.so", "libgcc_s.so.1"]

def removeLibc(libraryPath):
  """ Remove libraries that can be problematic, like libc.so

  :param string libraryPath: libraryPath to look for libraries to remove
  """

  gLogger.debug("RemoveLibC: Trying to remove these libraries:")
  gLogger.debug("RemoveLibC - "+ "\nRemoveLibC - ".join(files_to_remove) )

  curdir = os.getcwd()
  try:
    os.chdir(libraryPath)
  except:
    return True  
  listlibs = os.listdir(os.getcwd())
  for lib in listlibs:
    for lib_to_remove in files_to_remove:
      if lib.count(lib_to_remove):
        try:
          libraryPath = os.getcwd() + os.sep + lib
          gLogger.info("RemoveLibC: Trying to remove: %s" % libraryPath)
          os.remove(libraryPath)
        except:
          gLogger.error("RemoveLibC: Could not remove", lib)
          os.chdir(curdir)
          return False
  os.chdir(curdir)
  return True

def getLibsToIgnore():
  return files_to_remove

if __name__ == "__main__":
  import sys
  if not len(sys.argv)>1:
    print "You need to pass the path"
    exit(1)
  path = sys.argv[1]
  print "Will remove from %s the files that look like %s" % (path, getLibsToIgnore())  
  
  if not removeLibc(path):
    print "Could not clean libs"
    exit(1)
  exit(0)