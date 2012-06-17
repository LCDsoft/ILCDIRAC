'''
Created on Nov 2, 2010

@author: sposs
'''
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
import os

from DIRAC import gLogger

def getNumberOfevents(inputfile):
  """ Find from the FileCatalog the number of events in a file
  """

  files = inputfile
  flist = {}
  for myfile in files:
    if not myfile:
      continue
    bpath = os.path.dirname(myfile)
    if not bpath in flist.keys():
      flist[bpath] = [myfile]
    else:
      flist[bpath].append(myfile)
      
  fc = FileCatalogClient()
  nbevts = {} 
  luminosity = 0
  numberofevents = 0
  evttype = ''
  for path, files in flist.items():
    found_nbevts = False
    found_lumi = False

    if len(files) == 1:
      res = fc.getFileUserMetadata(files[0])
      if not res['OK']:
        gLogger.verbose("Failed to get meta data")
        continue
      tags = res['Value']
      if tags.has_key("NumberOfEvents") and not found_nbevts:
        numberofevents += int(tags["NumberOfEvents"])
        found_nbevts = True
      if tags.has_key("Luminosity") and not found_lumi:
        luminosity += float(tags["Luminosity"])  
        found_lumi = True
      if found_nbevts: 
        continue
        
    res = fc.getDirectoryMetadata(path)
    if res['OK']:   
      tags = res['Value']
      if tags.has_key("NumberOfEvents") and not found_nbevts:
        numberofevents += len(files)*int(tags["NumberOfEvents"])
        found_nbevts = True
      if tags.has_key("Luminosity") and not found_lumi:
        luminosity += len(files) * float(tags["Luminosity"])
        found_lumi = True
      if tags.has_key("EvtType"):
        evttype = tags["EvtType"]
      if found_nbevts: 
        continue
      
    for myfile in files:
      res = fc.getFileUserMetadata(myfile)
      if not res['OK']:
        continue
      tags = res['Value']
      if tags.has_key("NumberOfEvents"):
        numberofevents += int(tags["NumberOfEvents"])
      if tags.has_key("Luminosity") and not found_lumi:
        luminosity += float(tags["Luminosity"])
        
  nbevts['nbevts'] = numberofevents
  nbevts['lumi'] = luminosity
  nbevts['EvtType'] = evttype
  return nbevts
