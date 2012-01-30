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
  luminosity = 0
  numberofevents = 0
  evttype = ''
  for file in files:
    if not file:
      continue
    res = fc.getFileUserMetadata(file)
    if not res['OK']:
        continue  
    tags= res['Value']
    if tags.has_key("NumberOfEvents"):
      numberofevents+=tags["NumberOfEvents"]
    if tags.has_key("Luminosity"):
      luminosity+=tags["Luminosity"]
    #if tags.has_key("EvtType"):
    #  evttype=tags["EvtType"]
    
    res = fc.getDirectoryMetadata(file)
    if not res['OK']:
        continue  
    #tags= res['Value']
    #if tags.has_key("NumberOfEvents"):
    #  numberofevents+=tags["NumberOfEvents"]
    #if tags.has_key("Luminosity"):
    #  luminosity+=tags["Luminosity"]
    if tags.has_key("EvtType"):
      evttype=tags["EvtType"]
  nbevts['nbevts'] = numberofevents
  nbevts['lumi'] = luminosity
  nbevts['EvtType'] = evttype
  return nbevts
