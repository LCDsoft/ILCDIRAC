from DIRAC.Core.Base import Script
Script.parseCommandLine()

name_of_replication = 'some_name'
description = "describe what this replication is going to do"
evttype = 'tth_XXX'#or any other event type
detectormodel = "ILD_o1_V2"#or 
energy = 1000
machineparams= 'b1s_ws'
detectortype='ILD'
machine='ilc'
fmask = ""#this need to be adated to select only the sofrware and the file type wanted

source = 'DESY-SRM'
destination = 'KEK-SRM'

##below should not change

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

from DIRAC.Core.Security.ProxyInfo                          import getProxyInfo

proxyinfo = getProxyInfo()
if not proxyinfo['OK']:
  print "Not allowed to create production, you need a ilc_prod proxy: use dirac-proxy-init -g ilc_prod"
  exit(1)
if proxyinfo['Value'].has_key('group'):
  group = proxyinfo['Value']['group']
  if not group == "ilc_prod":
    print "Not allowed to create production, you need a ilc_prod proxy: use dirac-proxy-init -g ilc_prod"
    exit(1)
else:
  print "Could not determine group, you do not have the right proxy: use dirac-proxy-init -g ilc_prod"
  exit(1)

fc = FileCatalogClient()

meta = {}
meta['EvtType']=evttype
meta['Energy']=energy
meta['Machine']=machine
meta['DetectorType']=detectortype
meta['DetectorModel']=detectormodel
meta['MachineParams']=machineparams

res = fc.findFilesByMetadata(meta)
if not res['OK']:
  print "something went wrong when finding the files: %s" % res['Message']
  exit(1)

lfns = res['Value']
if not len(lfns):
  print "No files found with this meta data query:%s" % meta
  exit(1)

trc = TransformationClient()
res = trc.getTransformationStats(name_of_replication)
if res['OK']:
  print "Replication with name %s already exists! Cannot proceed." % name_of_replication
  
Trans = Transformation()
Trans.setTransformationName(name_of_replication)
Trans.setDescription(description)
Trans.setLongDescription(description)
Trans.setType('Replication')
Trans.setPlugin('Broadcast')
Trans.setFileMask(fmask)
Trans.setSourceSE(source)
Trans.setTargetSE(destination)
res = Trans.addTransformation()
if not res['OK']:
  print "failed to add Replication: %s" % res['Message']
  exit(1)
Trans.setStatus("Active")
Trans.setAgentType("Automatic")
currtrans = Trans.getTransformationID()['Value']
res = trc.createTransformationInputDataQuery(currtrans,meta)
