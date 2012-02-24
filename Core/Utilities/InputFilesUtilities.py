'''
Created on Nov 2, 2010

@author: sposs
'''
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
import string,os

def getNumberOfevents(inputfile):
  """ Find from the FileCatalog the number of events in a file
  """
  files = inputfile.split(";")
  flist = {}
  for file in files:
    if not file:
      continue
    bpath = os.path.dirname(file)
    if not bpath in flist.keys():
      flist[bpath] = [file]
    else:
      flist[bpath].append(file)
      
  fc = FileCatalogClient()
  nbevts = {} 
  luminosity = 0
  numberofevents = 0
  evttype = ''
  for path,files in flist.items():
    found_nbevts = False
    found_lumi = False

    if len(files)==1:
      res = fc.getFileUserMetadata(files[0])
      if not res['OK']:
        continue
      tags= res['Value']
      if tags.has_key("NumberOfEvents") and not found_nbevts:
        numberofevents+=tags["NumberOfEvents"]
        found_nbevts = True
      if tags.has_key("Luminosity") and not found_lumi:
        luminosity+=tags["Luminosity"]  
        found_lumi = True
        
    res = fc.getDirectoryMetadata(path)
    if res['OK']:   
      tags= res['Value']
      if tags.has_key("NumberOfEvents") and not found_nbevts:
        numberofevents += len(files)*tags["NumberOfEvents"]
        found_nbevts = True
      if tags.has_key("Luminosity") and not found_lumi:
        luminosity += len(files)*tags["Luminosity"]
        found_lumi = True
      if tags.has_key("EvtType"):
        evttype=tags["EvtType"]
      if found_nbevts: 
        continue
      
    for file in files:
      res = fc.getFileUserMetadata(file)
      if not res['OK']:
        continue
      tags= res['Value']
      if tags.has_key("NumberOfEvents"):
        numberofevents+=tags["NumberOfEvents"]
      if tags.has_key("Luminosity") and not found_lumi:
        luminosity+=tags["Luminosity"]  
        
  nbevts['nbevts'] = numberofevents
  nbevts['lumi'] = luminosity
  nbevts['EvtType'] = evttype
  return nbevts
