"""Akiyas script for replication."""
from __future__ import print_function
from DIRAC.Core.Base import Script
Script.parseCommandLine()

name_of_replication = 'Replicate_higgs_desy'
description = "Replicate higgs at 250GeV from KEK to DESY"
#below are meta data tags
evttype = 'higgs'#or any other event type
detectormodel = None #"ILD_o1_V2"#or any other
energy = "250"
machineparams= 'TDR_ws'
datatype='gen' #this is either gen or REC

detectortype=None # or 'ILD'

softwaretag = None # or 'v01-16' or any other value
#fmask = ".+(/v01-16)+.+(DST)+."#this need to be adapted to select only the software and the file type wanted
#it matches anything that contains /v01-15-03 and DST

machine='ilc' #This is alway ilc

source = 'KEK-SRM' #source of the files
destination = 'DESY-SRM' # destination of the files

##below should not change

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

from DIRAC.Core.Security.ProxyInfo                          import getProxyInfo

proxyinfo = getProxyInfo()
if not proxyinfo['OK']:
  print("Not allowed to create production, you need a ilc_prod proxy: use dirac-proxy-init -g ilc_prod")
  exit(1)
if 'group' in proxyinfo['Value']:
  group = proxyinfo['Value']['group']
  if not group == "ilc_prod":
    print("Not allowed to create production, you need a ilc_prod proxy: use dirac-proxy-init -g ilc_prod")
    exit(1)
else:
  print("Could not determine group, you do not have the right proxy: use dirac-proxy-init -g ilc_prod")
  exit(1)

fc = FileCatalogClient()

meta = {}
if evttype:
  meta['EvtType']=evttype
if energy:  
  meta['Energy']=energy

meta['Machine']=machine

if detectortype:
  meta['DetectorType']=detectortype

meta['Datatype']=datatype
if softwaretag:
  meta['SoftwareTag'] = softwaretag
if detectormodel:
  meta['DetectorModel']=detectormodel
if machineparams:
  meta['MachineParams']=machineparams

res = fc.findFilesByMetadata(meta)
if not res['OK']:
  print("Something went wrong when finding the files: %s" % res['Message'])
  exit(1)

lfns = res['Value']
if not len(lfns):
  print("No files found with this meta data query:%s" % meta)
  exit(1)

print("Example file: %s" % lfns[0])

answer = raw_input('Proceed and submit replication? (Y/N): ')
if not answer.lower() in ('y', 'yes'):
  print("Canceled")
  exit(1)

trc = TransformationClient()
res = trc.getTransformationStats(name_of_replication)
if res['OK']:
  print("Replication with name %s already exists! Cannot proceed." % name_of_replication)
  exit(1)
  
Trans = Transformation()
Trans.setTransformationName(name_of_replication)
Trans.setDescription(description)
Trans.setLongDescription(description)
Trans.setType('Replication')
Trans.setPlugin('Broadcast')
#Trans.setFileMask(fmask)
Trans.setSourceSE(source)
Trans.setTargetSE(destination)
res = Trans.addTransformation()
if not res['OK']:
  print("Failed to add Replication: %s" % res['Message'])
  exit(1)
Trans.setStatus("Active")
Trans.setAgentType("Automatic")
currtrans = Trans.getTransformationID()['Value']
res = trc.createTransformationInputDataQuery(currtrans,meta)
