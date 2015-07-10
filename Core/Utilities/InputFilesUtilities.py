'''
For any input file, try to determine from the FC the number of events / luminosity / event type. 

@author: S. Poss
@since: Nov 2, 2010
'''

__RCSID__ = "$Id$"

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
import os

from DIRAC import gLogger, S_OK, S_ERROR

def getNumberOfEvents(inputfile):
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
  others = {}
  completeFailure = True

  for path, files in flist.items():
    found_nbevts = False
    found_lumi = False

    if len(files) == 1:
      res = fc.getFileUserMetadata(files[0])
      if not res['OK']:
        gLogger.warn("Failed to get Metadata from file: %s, because: %s" % (files[0], res['Message']))
      else:
        tags = res['Value']
        if "NumberOfEvents" in tags and not found_nbevts:
          numberofevents += int(tags["NumberOfEvents"])
          found_nbevts = True
          completeFailure = False
        if "Luminosity" in tags and not found_lumi:
          luminosity += float(tags["Luminosity"])
          found_lumi = True
        others.update(tags)
        if found_nbevts:
          continue

    res = fc.getDirectoryUserMetadata(path)
    if res['OK']:   
      tags = res['Value']
      if tags.has_key("NumberOfEvents") and not found_nbevts:
        numberofevents += len(files)*int(tags["NumberOfEvents"])
        found_nbevts = True
        completeFailure = False
      if tags.has_key("Luminosity") and not found_lumi:
        luminosity += len(files) * float(tags["Luminosity"])
        found_lumi = True
      if tags.has_key("EvtType"):
        evttype = tags["EvtType"]
      others.update(tags)    
      if found_nbevts: 
        continue
    else:
      gLogger.warn("Failed to get Metadata from path: %s, because: %s" % (path, res['Message']))

    for myfile in files:
      res = fc.getFileUserMetadata(myfile)
      if not res['OK']:
        gLogger.warn("Failed to get Metadata from file: %s, because: %s" % (myfile, res['Message']))
        continue
      tags = res['Value']
      if tags.has_key("NumberOfEvents"):
        numberofevents += int(tags["NumberOfEvents"])
        completeFailure = False
      if tags.has_key("Luminosity") and not found_lumi:
        luminosity += float(tags["Luminosity"])
      others.update(tags)  
        
  nbevts['nbevts'] = numberofevents
  nbevts['lumi'] = luminosity
  nbevts['EvtType'] = evttype
  if 'NumberOfEvents' in others:
    del others['NumberOfEvents']
  if 'Luminosity' in others:
    del others['Luminosity']
  nbevts['AdditionalMeta'] = others

  if completeFailure:
    gLogger.warn("Did not obtain NumberOfEvents from FileCatalog")
    return S_ERROR("Failed to get Number of Events")

  return S_OK(nbevts)
