'''
Created on Jan 26, 2011

@author: sposs
'''
import os

def removeLibc(path):
  curdir = os.getcwd()
  os.chdir(path)
  listlibs = os.listdir(os.getcwd())
  print listlibs
  for lib in listlibs:
    if lib.count("libc.so"):
      try:
        os.remove(os.getcwd()+os.sep+lib)
      except:
        print "Could not remove libc"
        os.chdir(curdir)
        return False
  os.chdir(curdir)
  return True

  