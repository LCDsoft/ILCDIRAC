'''
Remove any system library provided in the application tar ball

:author: sposs
:since: Jan 26, 2011
'''
__RCSID__ = "$Id$"

from DIRAC import gLogger
import os

FILES_TO_REMOVE = ["libc.so","libc-2.5","libm.so","libpthread.so","libdl.so", "libstdc++.so", "libgcc_s.so.1"]

def removeLibc(libraryPath):
  """ Remove libraries that can be problematic, like libc.so

  :param string libraryPath: libraryPath to look for libraries to remove
  :returns: True on Success, False in case of error
  """

  gLogger.debug("RemoveLibC: Trying to remove these libraries:")
  gLogger.debug("RemoveLibC - "+ "\nRemoveLibC - ".join(FILES_TO_REMOVE) )

  curdir = os.getcwd()
  try:
    os.chdir(libraryPath)
  except OSError:
    return True
  listlibs = os.listdir(os.getcwd())
  for lib in listlibs:
    for lib_to_remove in FILES_TO_REMOVE:
      if lib.count(lib_to_remove):
        try:
          libraryPath = os.getcwd() + os.sep + lib
          gLogger.info("RemoveLibC: Trying to remove: %s" % libraryPath)
          os.remove(libraryPath)
        except OSError:
          gLogger.error("RemoveLibC: Could not remove", lib)
          os.chdir(curdir)
          return False
  os.chdir(curdir)
  return True

def getLibsToIgnore():
  """ :returns: static list of system libraries """
  return FILES_TO_REMOVE

def main():
  """ Main method, executed when this file is executed as a python script """
  import sys
  if not len(sys.argv)>1:
    print "You need to pass the path"
    return 1
  PATH = sys.argv[1]
  print "Will remove from %s the files that look like %s" % (PATH, getLibsToIgnore())
  
  if not removeLibc(PATH):
    print "Could not clean libs"
    return 1
  return 0

if __name__ == "__main__":
  exit( main() )
