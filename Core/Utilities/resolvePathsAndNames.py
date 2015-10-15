'''
Several utilities to "guess" the files names and the paths
:author: sposs
:since: March 13th, 2013
'''
__RCSID__ = "$Id$"

import os
from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger

def getProdFilename(filename, prodID, jobID):
  """ Build the output file names based of local job property.

  :param string filename: File name before change
  :param int prodID: Production ID
  :param int jobID: Job ID
  :return: the modified file name
  """
  outfile = ""
  if filename.count(".slcio"):
    name = filename.split(".slcio")
    outfile = name[0] + "_" + str(prodID) + "_" + str(jobID) + ".slcio"
  elif filename.count(".stdhep"):
    name = filename.split(".stdhep")
    outfile = name[0] + "_" + str(prodID) + "_" + str(jobID) + ".stdhep"
  elif filename.count(".root"):
    name = filename.split(".root")
    outfile = name[0] + "_" + str(prodID) + "_" + str(jobID) + ".root"
  return outfile

def resolveIFpaths(inputfiles):
  """ Try to find out in which sub-directory are each file. In the future, should be useless if
  PoolXMLCatalog can be used.
  """
  log = gLogger.getSubLogger("ResolveInputFiles")
  listoffiles = []
  string = "Will look for:"
  for myfile in inputfiles:
    if not len(myfile):
      continue
    listoffiles.append(os.path.basename(myfile))
    string += "%s\n" % os.path.basename(myfile)
  string = string.rstrip()
  log.info(string)

  listofpaths = []
  listofdirs = []
  for mydir in os.listdir(os.getcwd()):
    if os.path.isdir(mydir):
      listofdirs.append(mydir)
  filesnotfound = []
  for infile in listoffiles:
    filefound = False
    if os.path.exists(os.path.join(os.getcwd(), infile)):
      listofpaths.append(os.path.join(os.getcwd(), infile))
      filefound = True
    else:
      for mydir in listofdirs:
        if os.path.exists( os.path.join(os.getcwd(), mydir, infile)):
          listofpaths.append( os.path.join( os.getcwd(), mydir, infile))
          listofdirs.remove(mydir)
          filefound = True
          break
    if not filefound:
      filesnotfound.append(infile)
  if len(filesnotfound):
    return S_ERROR("resolveIFPath: Input file(s) '%s' not found locally" % (", ".join(filesnotfound)))
  log.verbose("Found all input files")
  return S_OK(listofpaths)
