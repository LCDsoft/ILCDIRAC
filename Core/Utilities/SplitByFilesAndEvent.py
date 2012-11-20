# $HeadURL$
# $Id$
'''
Try some fancy splitting, DO NOT USE

Based on Dirac.SplitByFiles idea, but doing the splitting by number of events
Gives a list of dictionaries

Created on Feb 10, 2010

@author: sposs
'''

from ILCDIRAC.Core.Utilities.InputFilesUtilities import getNumberOfevents
from DIRAC import S_OK, S_ERROR

def SplitByFilesAndEvents(listoffiles, evtsperjob):
  """ Group the input files 
  """
  mylist = [] 
  total_evts = 0
  for files in listoffiles:
    myfdict = {}
    info =  getNumberOfevents([files])
    if not "nbevts" in info:
      return S_ERROR("The file %s does not have attached number of events, cannot split" % files)
    myfdict['file'] = files
    myfdict['nbevts'] = info['nbevts']
    mylist.append(myfdict) 
    total_evts += info['nbevts']

  #nb_jobs = total_evts/evtsperjob
  joblist = []
  jdict = {}
  startfromevt = 0
  cur_events = 0
  for event in range(total_evts):
    cur_events +=1
    if not len(mylist):
      break
    item = mylist[0]
    nb_evts_in_file = item['nbevts']

    if cur_events == evtsperjob :
      jdict['startFrom'] = startfromevt
      if not 'files' in jdict:
        jdict['files'] = []
      jdict['files'].append(item['file'])
      startfromevt = int(cur_events)
      cur_events = 0
      mylist.remove(item)
      joblist.append(jdict)
      continue
    if cur_events > nb_evts_in_file:
      if not 'files' in jdict:
        jdict['files'] = []
      jdict['files'].append(item['file'])
      mylist.remove(item)
      continue
    #joblist.append(jdict)
    #print "final jdict", jdict
  return S_OK(joblist)

if __name__=="__main__":
  from DIRAC.Core.Base import Script
  Script.parseCommandLine()
  from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
  fc = FileCatalogClient()

  res = fc.findFilesByMetadata({"ProdID":1978})
  if not res['OK']:
    print res['Message']
    exit(1)
  lfns = res['Value']
  lfns.sort()
  res = SplitByFilesAndEvents(lfns,70)
  print res['Value'][1]
