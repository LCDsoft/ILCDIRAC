'''
Several utilities to "guess" the files names and the paths

:author: sposs
:since: March 13th, 2013
'''
__RCSID__ = "$Id$"

import os
from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger
from ILCDIRAC.Core.Utilities.FilenameEncoder import FilenameEncoder, decodeFilename
###############################################################################
def getProdFilenameFromInput( inputfile, outfileOriginal, prodID, jobID ) :
  '''  Build the output file names based on inputfile name and job property

  If outfileOriginal starts with 's' we assume a simulation job, if it starts
  with 'r' we assume a reconstruction job, if starts with "E" we assume
  a stdhepsplit job.

  :param str inputfile: Input file LFN, either \\*.stdhep or \\*.slcio
  :param str outfileOriginal: Output file LFN before change
  :param prodID: Production ID
  :type prodID: `str`, `int`
  :param jobID: jobID
  :type jobID: `str`, `int`
  :returns: Full LFN to changed output file

  '''
  finp = FilenameEncoder()
  inpitem = decodeFilename(inputfile)

  origitem = decodeFilename(outfileOriginal)
  originalOutputBaseName = os.path.basename( outfileOriginal )

  outfile = ""
  if originalOutputBaseName.startswith("s"):
    inpitem["s"] = origitem["s"]
    inpitem["m"] = origitem["m"]
    inpitem["d"] = "sim"
    inpitem["t"] = str(prodID).zfill(8)
    inpitem["j"] = str(jobID)
    outfile = finp.convert( "sim", "file", inpitem )
  elif originalOutputBaseName.startswith("r"):
    inpitem["r"] = origitem["r"]
    inpitem["d"] = origitem["d"]
    inpitem["t"] = str(prodID).zfill(8)
    inpitem["j"] = str(jobID)
    outfile  = finp.convert( origitem["d"], "file", inpitem )
  elif originalOutputBaseName.startswith("E"):
    inpitem["d"] = "gen"
    seqstr = origitem["n"].split("_")
    if len(seqstr) > 1:  # Add sub-sequence number if found in the outfileOriginal
      inpitem["n"]="_".join([inpitem["n"]]+seqstr[1:])
    else:
      inpitem["n"] = origitem["n"]
    inpitem["t"] = str(prodID).zfill(8)
    inpitem["j"] = str(jobID)
    outfile  = finp.convert( "gen", "file", inpitem )
  else:  # Output as it is if not match above
    outfile = originalOutputBaseName

  basepath = os.path.dirname( outfileOriginal )
  return os.path.join( basepath, outfile )

###############################################################################
def getProdFilename(filename, prodID, jobID, workflow_commons=None):
  """ Build the output file names based of local job property.

  If workflow_commons is given and contains a ProductionOutputData entry of
  basestring that file is returned

  :param str filename: File name before change
  :param int prodID: Production ID
  :param int jobID: Job ID
  :param dict workflow_commons: workflow_commons dictionary
  :return: the modified file name

  """
  if workflow_commons is not None and \
     workflow_commons.get('ProductionOutputData') and \
     isinstance( workflow_commons.get('ProductionOutputData'), basestring ):
    outfile = workflow_commons.get('ProductionOutputData')
    return os.path.basename(outfile)

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

###############################################################################
def resolveIFpaths(inputfiles):
  """ Try to find out in which sub-directory are each file. In the future, should be useless if
  PoolXMLCatalog can be used.

  :param list inputfiles: list of inputfiles
  :returns: S_OK(listOfFilePaths), S_ERROR
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
