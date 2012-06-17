'''
Created on Jun 28, 2010

@author: sposs
'''
import os
from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger

def resolveIFpaths(inputfiles):
  """ Try to find out in which sub-directory are each file. In the future, should be useless if PoolXMLCatalog can be used. 
  """
  listoffiles = []
  string = "Will look for:"
  for myfile in inputfiles:
    if not len(myfile):
      continue
    listoffiles.append(os.path.basename(myfile))
    string += "%s\n" % os.path.basename(myfile)
  gLogger.info(string)

  listofpaths = []
  listofdirs = []
  for mydir in os.listdir(os.getcwd()):
    if os.path.isdir(mydir):
      listofdirs.append(mydir)
  filesnotfound = []
  for f in listoffiles:
    filefound = False
    if os.path.exists(f):
      listofpaths.append(os.getcwd() + os.sep + f)
      filefound = True
    else:
      for mydir in listofdirs:
        if os.path.exists(os.getcwd() + os.sep + mydir + os.sep + f):
          listofpaths.append(os.getcwd() + os.sep + mydir + os.sep + f)
          listofdirs.remove(mydir)
          filefound = True
          break
    if not filefound:
      filesnotfound.append(f)
  if len(filesnotfound):
    return S_ERROR("resolveIFPath: Input file(s) '%s' not found locally" % (", ".join(filesnotfound)))
  return S_OK(listofpaths)
