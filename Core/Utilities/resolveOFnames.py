'''
Created on Jul 30, 2010

@author: sposs
'''

def getProdFilename(filename,prodID,jobID):
  name = filename.split(".slcio")
  outfile = name[0]+"_"+str(prodID)+"_"+str(jobID)+".slcio"
  return outfile