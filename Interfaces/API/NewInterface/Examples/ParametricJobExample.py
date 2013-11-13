'''
Example on how to submit jobs using parametric input data.

Created on Nov 6, 2013

@author: sposs
'''
from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC import gLogger, exit as dexit
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
from ILCDIRAC.Interfaces.API.NewInterface.Applications import Marlin
from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob

__RCSID__ = "$Id$" 

def getFiles():
  """ Get the lfns: This is not the point of this example, so keep it out of the main
  """
  fc = FileCatalogClient()
  
  meta = {}
  meta['ProdID'] = 1543
  meta["Datatype"] = "DST"
  
  result = fc.findFilesByMetadata(meta, "/ilc/prod/clic")
  if not result["OK"]:
    gLogger.error(result["Message"])
    dexit(1)
  return result['Value']

def getJob():
  """ produce a job: it's always the same, so we don't need to put it in the main
  """
  j = UserJob()
  ma = Marlin()
  ma.setVersion("v0111Prod")
  ma.setSteeringFile("clic_ild_cdr_steering.xml")
  ma.setGearFile("clic_ild_cdr.gear")
  result = j.append(ma)
  if not result['OK']:
    gLogger.error(result["Message"])
    dexit(1)
  j.setCPUTime(10000)
  j.setOutputSandbox("*.log")
  return j

if __name__ == '__main__':
  
  lfns = getFiles()#get a list of files
  
  d = DiracILC(True, "paramjobtest.rep")#get your dirac instance
  
  job = getJob()#get a job, any can do
  
  #here is where the interesting stuff happen
  from DIRAC.Core.Utilities.List import breakListIntoChunks
  for flist in breakListIntoChunks(lfns, 200):
    #200 is the number of files per chunk, and the max number of jobs produced in one go
    
    #This is the magical line
    job.setParametricInputData(flist)
    
    #The rest of the sumission is the same
    res = job.submit(d)
    if not res["OK"]:
      gLogger.error("Failed to submit the job: ", res["Message"])
      
  