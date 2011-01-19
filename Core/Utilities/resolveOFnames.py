'''
Created on Jul 30, 2010

@author: sposs
'''

def getProdFilename(filename,prodID,jobID):
  outfile = ""
  if filename.count(".slcio"):
    name = filename.split(".slcio")
    outfile = name[0]+"_"+str(prodID)+"_"+str(jobID)+".slcio"
  elif filename.count(".stdhep"):
    name = filename.split(".stdhep")
    outfile = name[0]+"_"+str(prodID)+"_"+str(jobID)+".stdhep"
  elif filename.count(".root"):
    name = filename.split(".root")
    outfile = name[0]+"_"+str(prodID)+"_"+str(jobID)+".root"
  return outfile