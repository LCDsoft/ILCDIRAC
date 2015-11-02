'''
Try some fancy splitting, DO NOT USE

Based on :func:`DIRAC.SplitByFiles` idea, but doing the splitting by number of events

:since: Feb 10, 2010
:author: sposs
'''

__RCSID__ = "$Id$"

from ILCDIRAC.Core.Utilities.InputFilesUtilities import getNumberOfEvents
from DIRAC import S_OK, S_ERROR

def SplitByFilesAndEvents(listoffiles, evtsperjob):
  """ Group the input files to have equal number of events per job
  files must have metadata number of events

  :param listoffiles: list of inputfiles
  :param int eventsperjob: desired number of events per job
  :returns: S_OK with a list of dictionaries
  """
  mylist = [] 
  total_evts = 0
  for files in listoffiles:
    myfdict = {}
    resInfo =  getNumberOfEvents([files])
    if not resInfo['OK'] or not "nbevts" in resInfo['Value']:
      return S_ERROR("The file %s does not have attached number of events, cannot split" % files)
    myfdict['file'] = files
    myfdict['nbevts'] = resInfo['Value']['nbevts']
    mylist.append(myfdict) 
    total_evts += resInfo['Value']['nbevts']

  #nb_jobs = total_evts/evtsperjob
  joblist = []
  jdict = {}
  startfromevt = 0
  cur_events = 0
  for dummy_eventNumber in range(total_evts):
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

  RES = FileCatalogClient().findFilesByMetadata({"ProdID":1978})
  if not RES['OK']:
    print RES['Message']
    exit(1)
  LFNS = RES['Value']
  LFNS.sort()
  RES = SplitByFilesAndEvents(LFNS,70)
  print RES['Value'][1]
