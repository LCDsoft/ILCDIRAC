#/bin/env python

if __name__ == "__main__":
  from DIRAC.Core.Base import Script
  Script.parseCommandLine()
  from DIRAC import gLogger, exit as dexit
  
  from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
  fc = FileCatalogClient()
  
  #Set the meta data here.
  meta = {}
  meta['ProdID']={">=":2354}
  meta['Datatype']='SIM'
  
  res = fc.findFilesByMetadata(meta)
  if not res['OK']:
    gLogger.error(res['Message'])
    dexit(1)
  
  lfns = res['Value']
  
  from DIRAC.Core.Utilities.List import breakListIntoChunks
  anc_dict = {}
  desc_dict = {}
  for sublist in breakListIntoChunks(lfns, 100):
    res = fc.getFileAncestors(sublist, 1)
    if not res['OK']:
      gLogger.error(res['Message'])
      dexit(1)
  
    anc_dict.update(res['Value']['Successful'])
  
    res = fc.getFileDescendents(sublist, 1)
    if not res['OK']:
      gLogger.error(res['Message'])
      dexit(1)  
    desc_dict.update(res['Value']['Successful'])
  files_to_remove = []
  for lfn in lfns:
    if lfn in anc_dict.keys():
      if not anc_dict[lfn]:
        #gLogger.notice("Bad lfn:",lfn)
        if lfn in desc_dict:
          #gLogger.notice(desc_dict['Successful'][lfn].keys())
          files_to_remove.extend(desc_dict[lfn].keys())
        files_to_remove.append(lfn)
  
  gLogger.notice("Initial files:",len(lfns))
  gLogger.notice("Will remove files:",len(files_to_remove) )
  for f in files_to_remove:
    if f in lfns:
      lfns.remove(f)
  gLogger.notice("Remaining files:",len(lfns))
  
  from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
  rm = ReplicaManager()
  res = rm.removeFile(files_to_remove, force=True)
  if not res['OK']:
    gLogger.error(res['Message'])
    dexit(1)
  gLogger.notice("Cleaned files:",len(files_to_remove))
  dexit(0)
