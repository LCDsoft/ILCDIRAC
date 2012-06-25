'''
Created on Mar 7, 2011

@author: sposs
'''
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

import os
from DIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient

from ILCDIRAC.Core.Utilities.ProcessList                      import ProcessList
from DIRAC                                                    import gConfig
from DIRAC.Resources.Catalog.FileCatalogClient                import FileCatalogClient

fc = FileCatalogClient()

processlist = gConfig.getValue('/LocalSite/ProcessListPath')
prl = ProcessList(processlist)
processesdict = prl.getProcessesDict()

trc = TransformationClient()
prodids = []
if not len(args):
  prodtype = 'MCGeneration'
  res = trc.getTransformations( {'Type' : prodtype} )
  if res['OK']:
    for transfs in res['Value']:
      prodids.append(transfs['TransformationID'])

else:
  prodids = args

def translate(detail):
  detail = detail.replace('e1','e-')
  detail = detail.replace('E1','e+')
  detail = detail.replace('e2','mu-')
  detail = detail.replace('E2','mu+')
  detail = detail.replace('e3','tau-')
  detail = detail.replace('E3','tau+')
  detail = detail.replace('n1','nue')
  detail = detail.replace('N1','nueb')
  detail = detail.replace('n2','numu')
  detail = detail.replace('N2','numub')
  detail = detail.replace('n3','nutau')
  detail = detail.replace('N3','nutaub')
  detail = detail.replace('U','ubar')
  detail = detail.replace('C','cbar')
  detail = detail.replace('T','tbar')
  detail = detail.replace('tbareV','TeV')
  detail = detail.replace('D','dbar')
  detail = detail.replace('S','sbar')
  detail = detail.replace('B','bbar')
  detail = detail.replace('Z0','Z')
  detail = detail.replace('Z','Z0')
  detail = detail.replace(',','')
  detail = detail.replace('n N','nu nub')
  detail = detail.replace('se--','seL-')
  detail = detail.replace('se-+','seL+')
  detail = detail.replace(' -> ','->')
  detail = detail.replace('->',' -> ')
  detail = detail.replace(' H ->',', H ->')
  detail = detail.replace(' Z0 ->',', Z0 ->')
  detail = detail.replace(' W ->',', W ->')
    
  return detail

metadata = []

for prodID in prodids:
  meta = {}
  meta['ProdID'] = int(prodID)

  res = fc.findFilesByMetadata(meta)  
  if not res['OK']:
    print res['Message']
    continue
  lfns = res['Value']
  nb_files = len(lfns)
  path = ""
  if not len(lfns):
    print "no files found"
    continue
  path = os.path.dirname(lfns[0])
  res = fc.getDirectoryMetadata(path)
  dirmeta = {}
  dirmeta['nb_files'] = nb_files
  dirmeta.update(res['Value'])
  if not dirmeta.has_key('Luminosity'):
    dirmeta['Luminosity'] = 0
  if not dirmeta.has_key('NumberOfEvents'):
    dirmeta['NumberOfEvents'] = 0
  #print processesdict[dirmeta['EvtType']]
  detail = processesdict[dirmeta['EvtType']]['Detail']

  dirmeta['detail'] = translate(detail)
  metadata.append(dirmeta)

for channel in metadata:
  print "<tr> <td>%s</td> <td>%s</td> <td>%s</td> <td>%s</td> <td>%s</td> <td>%s</td> <td>%s</td> </tr>" % (channel['detail'],
                                                                                                channel['Energy'],
                                                                                                channel['ProdID'],
                                                                                                channel['nb_files'],
                                                                                                channel['NumberOfEvents'],
                                                                                                channel['nb_files'] * channel['NumberOfEvents'],
                                                                                                channel['nb_files'] * channel['Luminosity'])
