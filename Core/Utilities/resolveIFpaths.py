'''
Created on Jun 28, 2010

@author: sposs
'''
import os
def resolveIFpaths(inputfiles):
  listoffiles = []
  for file in inputfiles:
    listoffiles.append(os.path.basename(file))
  listofpaths = []
  listofdirs = []
  for dir in os.listdir(os.getcwd()):
    if os.path.isdir(dir):
      listofdirs.append(dir)
  for f in listoffiles:
    if os.path.exists(f):
      listofpaths.append(f)
    else:
      for dir in listofdirs:
        if os.path.exists(os.getcwd()+os.sep+dir+os.sep+f):
          listofpaths.append(dir+os.sep+f)
          listofdirs.remove(dir)
          break
  return listofpaths