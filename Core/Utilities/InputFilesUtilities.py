'''
Created on Nov 2, 2010

@author: sposs
'''
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
import string

def getNumberOfevents(inputfile):
  files = inputfile.split(";")
  fc = FileCatalogClient()
  nbevts = {}
  for file in files:
    print file
    elements = file.split("/")
    prodiddir = string.join(elements[0:8],"/")
    print prodiddir
    res = fc.getDirectoryMetadata(prodiddir)
    if not res['OK']:
        continue
    tags= res['Value']
    if tags.has_key("NumberOfEvents"):
      nbevts['nbevts']=tags["NumberOfEvents"]
    if tags.has_key("Luminosity"):
      nbevts['lumi']=tags["Luminosity"]
    if tags.has_key("EvtType"):
      nbevts['EvtType']=tags["EvtType"]
  return nbevts
