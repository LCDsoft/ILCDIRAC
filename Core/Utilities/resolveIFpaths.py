'''
Created on Jun 28, 2010

@author: sposs
'''
import os
from DIRAC import S_OK,S_ERROR
def resolveIFpaths(inputfiles):
  listoffiles = []
  for file in inputfiles:
    listoffiles.append(os.path.basename(file))
  listofpaths = []
  listofdirs = []
  for dir in os.listdir(os.getcwd()):
    if os.path.isdir(dir):
      listofdirs.append(dir)
  filefound = False
  for f in listoffiles:
    filefound = False
    if os.path.exists(f):
      listofpaths.append(f)
      filefound=True
    else:
      for dir in listofdirs:
        if os.path.exists(os.getcwd()+os.sep+dir+os.sep+f):
          listofpaths.append(dir+os.sep+f)
          listofdirs.remove(dir)
          filefound = True
          break
  if not filefound:
    return S_ERROR("File not found")
  return S_OK(listofpaths)