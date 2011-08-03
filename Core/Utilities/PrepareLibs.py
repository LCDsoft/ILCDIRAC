'''
Created on Jan 26, 2011

@author: sposs
'''
import os

def removeLibc(path):
  #return True

  curdir = os.getcwd()
  try:
    os.chdir(path)
  except:
    return True  
  listlibs = os.listdir(os.getcwd())
  for lib in listlibs:
    if (lib.count("libc.so") or lib.count("libc-2.5") or lib.count("libm.so") 
        or lib.count("libpthread.so") or lib.count("libdl.so")):
      try:
        os.remove(os.getcwd()+os.sep+lib)
      except:
        print "Could not remove %s"%lib
        os.chdir(curdir)
        return False
  os.chdir(curdir)
  return True

  