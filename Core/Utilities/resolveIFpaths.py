'''
Find out in which sub-directory are each InputData files

@author: sposs
@since: Jun 28, 2010
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
  for infile in listoffiles:
    filefound = False
    if os.path.exists(infile):
      listofpaths.append(os.getcwd() + os.sep + infile)
      filefound = True
    else:
      for mydir in listofdirs:
        if os.path.exists(os.getcwd() + os.sep + mydir + os.sep + infile):
          listofpaths.append(os.getcwd() + os.sep + mydir + os.sep + infile)
          listofdirs.remove(mydir)
          filefound = True
          break
    if not filefound:
      filesnotfound.append(infile)
  if len(filesnotfound):
    return S_ERROR("resolveIFPath: Input file(s) '%s' not found locally" % (", ".join(filesnotfound)))
  return S_OK(listofpaths)
