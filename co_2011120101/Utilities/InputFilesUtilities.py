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
  fc = FileCatalogClient()
  nbevts = {} 
  nbevts['nbevts'] = 0
  nbevts['lumi'] = 0
  for file in files:
    if not file:
      continue
    res = fc.getDirectoryMetadata(file)
    if not res['OK']:
        continue
    tags= res['Value']
    if tags.has_key("NumberOfEvents"):
      nbevts['nbevts']+=tags["NumberOfEvents"]
    if tags.has_key("Luminosity"):
      nbevts['lumi']+=tags["Luminosity"]
    if tags.has_key("EvtType"):
      nbevts['EvtType']=tags["EvtType"]
  return nbevts
