'''
Determine the final output files names

@author: S. Poss
@since: Jul 30, 2010
'''

def getProdFilename(filename, prodID, jobID):
  """ Build the output file names based of local job property.
  @param filename: File name before change
  @type filename: string
  @param prodID: Production ID
  @type prodID: int
  @param jobID: Job ID
  @type jobID: int
  @return: the modified file name
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