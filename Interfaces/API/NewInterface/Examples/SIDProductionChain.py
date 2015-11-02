'''
Created on Feb 10, 2012

:author: Stephane Poss
'''

from DIRAC.Core.Base import Script

import sys

Script.registerSwitch( 'D:', 'description=', 'Short description of the workflow (default set by metadata)' )
Script.registerSwitch( 'e:', 'evttype=', 'Name of the production event type (optional in addition to production ID)' )
Script.registerSwitch( 'E:', 'energy=', 'Energy of the production events (default 3tev)' )
Script.registerSwitch( 'm:', 'model=', 'Name of detector model to use' )
Script.registerSwitch( 'n', 'nocheck', 'Switches off additional check before submission' )
Script.registerSwitch( 'g:', 'group=', 'Name of the production group (default set by metadata)' )
Script.registerSwitch( 'p:', 'prodid=', 'Production ID of input' )
Script.registerSwitch( 't:', 'time=', 'CPU time limit per job in seconds (default 300000)' )
Script.registerSwitch( 'v:', 'version=', 'SLIC version to use (default v2r8p4)' )
Script.registerSwitch( 'l:', 'lcsim=', 'LCSIM version to use (default 1.15-SNAPHOT)' )
Script.registerSwitch( 'P:', 'pandora=', 'SlicPandora version to use (default CDR0)' )

Script.registerSwitch( 'w:', 'workflow=', 'Name of the workflow (default set by metadata)' )

Script.setUsageMessage( sys.argv[0]+' -m <detectorModel> -p <prodID> (<additional options>)' )

Script.parseCommandLine()
switches = Script.getUnprocessedSwitches()

detectorName = None
prodID = None
eventType = None
dataType = ''
energy = ''
prodGroup = None
cpuLimit = 300000
workflowName = None
workflowDescription = None
checkMeta = True

slicVersion = 'v2r8p4'
lcsimVers = '1.15-SNAPSHOT'
pandoraVers = 'CDR0'

slicMacro = 'defaultClicCrossingAngle.mac'
lcsimSteering1 = "clic_cdr_prePandora.lcsim"
lcsimSteering1_ov = "clic_cdr_prePandoraOverlay.lcsim"
lcsimSteering2 = "clic_cdr_postPandoraOverlay.lcsim"
pandoraSettings = "PandoraSettingsSlic.xml"
strategies = "defaultStrategies_%s.xml"


for switch in switches:
  opt = switch[0]
  arg = switch[1]
  if opt in ('D', 'description'):
    workflowDescription = arg
  elif opt in ('e', 'evttype'):
    eventType = arg
  elif opt in ('m', 'model'):
    detectorName = arg
    strategies = strategies%detectorName
  elif opt in ('g', 'group'):
    prodGroup = arg
  elif opt in ('p', 'prodid'):
    prodID = arg
  elif opt in ('t', 'time'):
    cpuLimit = arg
  elif opt in ('v', 'version'):
    slicVersion = arg
  elif opt in ('w', 'workflow'):
    workflowName = arg
  elif opt in ('n', 'nocheck'):
    checkMeta = False
  
if (detectorName == None) or (prodID == None):
  Script.showHelp()
  sys.exit(2)  


from ILCDIRAC.Interfaces.API.NewInterface.ProductionJob import ProductionJob
from ILCDIRAC.Interfaces.API.NewInterface.Applications import SLIC,LCSIM,SLICPandora,OverlayInput
from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
dirac = DiracILC()


meta = {}
meta[ 'ProdID' ] = prodID
if eventType:
  meta[ 'EvtType' ] = eventType
if energy:
  meta[ 'Energy'] = energy
if dataType:
  meta[ 'Datatype' ] = dataType


fc = FileCatalogClient()
res = fc.getCompatibleMetadata( meta )
if not res['OK']:
  print "Error looking up the catalog for metadata"
  exit(2)

metaValues = res['Value']
#print metaValues

print 'Meta data for production', prodID, ':'
for key, value in metaValues.iteritems():
  if not len(value) == 0:
    print '\t', key, ':', value[0]

if dataType:
  print '\t', 'Datatype :', dataType

if not eventType:
  eventType = metaValues['EvtType'][0]
else:
  print '\t', 'EvtType :', eventType
  
if not energy:
  energy = metaValues['Energy'][0]
else:
  print '\t', 'Energy :', energy
  
print 'SLIC version:', slicVersion
print 'LCSim version:', lcsimVers
print 'SLICPandora version:', pandoraVers
print 'CPU limit:', cpuLimit, 'sec'


if not prodGroup:
  prodGroup = eventType+'_'+energy+'_cdr'  
print 'Production group:', prodGroup

if not workflowName:
  workflowName = eventType+'_'+energy  
print 'Workflow name:', workflowName

if not workflowDescription:
  workflowDescription = eventType+' at '+energy
print 'Workflow description:', workflowDescription


if checkMeta:
  answer = raw_input('Submit production? (Y/N): ')
  if not answer.lower() in ('y', 'yes'):
    sys.exit(2)

####Define the applications

## Simulation
slic = SLIC()
slic.setVersion(slicVersion)
slic.setSteeringFile(slicMacro)
slic.setDetectorModel(detectorName)

## Reco without overlay: Do not take the input from SLIC as those are ran in different prods
lcsim_prepandora = LCSIM()
lcsim_prepandora.setVersion(lcsimVers)
lcsim_prepandora.setSteeringFile(lcsimSteering1)
lcsim_prepandora.setTrackingStrategy(strategies)
lcsim_prepandora.setDetectorModel(detectorName)
lcsim_prepandora.setOutputFile("prePandora.slcio")

slicpandora = SLICPandora()
slicpandora.setVersion(pandoraVers)
slicpandora.setDetectorModel(detectorName)
slicpandora.setPandoraSettings(pandoraSettings)
slicpandora.getInputFromApp(lcsim_prepandora)
slicpandora.setOutputFile('pandora.slcio')

lcsim_postpandora = LCSIM()
lcsim_postpandora.setVersion(lcsimVers)
lcsim_postpandora.getInputFromApp(slicpandora)
lcsim_postpandora.setSteeringFile(lcsimSteering2)
lcsim_postpandora.setTrackingStrategy(strategies)
lcsim_postpandora.setDetectorModel(detectorName)

### Now with overlay
overlay = OverlayInput()
overlay.setBXOverlay(60)
overlay.setGGToHadInt(1.3)##When running at 1.4TeV
overlay.setDetectorModel("CLIC_SID_CDR")
overlay.setBkgEvtType("gghad")

lcsim_prepandora_ov = LCSIM()
lcsim_prepandora_ov.setVersion(lcsimVers)
lcsim_prepandora_ov.setSteeringFile(lcsimSteering1_ov)
lcsim_prepandora_ov.setTrackingStrategy(strategies)
lcsim_prepandora_ov.setDetectorModel(detectorName)
lcsim_prepandora_ov.setOutputFile("prePandora.slcio")

slicpandora_ov = SLICPandora()
slicpandora_ov.setVersion(pandoraVers)
slicpandora_ov.setDetectorModel(detectorName)
slicpandora_ov.setPandoraSettings(pandoraSettings)
slicpandora_ov.getInputFromApp(lcsim_prepandora_ov)
slicpandora_ov.setOutputFile('pandora.slcio')

lcsim_postpandora_ov = LCSIM()
lcsim_postpandora_ov.setVersion(lcsimVers)
lcsim_postpandora_ov.getInputFromApp(slicpandora_ov)
lcsim_postpandora_ov.setSteeringFile(lcsimSteering2)
lcsim_postpandora_ov.setTrackingStrategy(strategies)
lcsim_postpandora_ov.setDetectorModel(detectorName)

####################################################33
##Now define the productions

pslic = ProductionJob()
pslic.setInputDataQuery(meta)
res = pslic.append(slic)
if not res['OK']:
  print res['Message']
  exit(1)
pslic.addFinalization(True,True,True,True)

pslic.setProdType("MCSimulation")
pslic.setProdGroup(prodGroup)
pslic.setWorkflowName(workflowName+'_sim_sid_cdr')
pslic.setCPUTime(cpuLimit)
pslic.setOutputSE("CERN-SRM")
pslic.setDescription('Simulating '+workflowDescription)
res = pslic.createProduction()
if not res['OK']:
  print res['Message']
  exit(1)
res = pslic.finalizeProd()
if not res['OK']:
  print res['Message']
  exit(1)

meta = pslic.getMetadata() ##This is needed to link Sim and Rec


###Reconstruction w/o overlay 
plcsim = ProductionJob()
plcsim.setInputDataQuery(meta)
res = plcsim.append(lcsim_prepandora)
if not res['OK']:
  print res['Message']
  exit(1)
res = plcsim.append(slicpandora)
if not res['OK']:
  print res['Message']
  exit(1)
res = plcsim.append(lcsim_postpandora)
if not res['OK']:
  print res['Message']
  exit(1)
plcsim.addFinalization(True,True,True,True)

plcsim.setProdType("MCReconstruction")
plcsim.setProdGroup(prodGroup)
plcsim.setWorkflowName(workflowName+'_rec_sid_cdr')
plcsim.setCPUTime(cpuLimit)
plcsim.setOutputSE("CERN-SRM")
plcsim.setDescription('Reconstructing '+workflowDescription)
res = plcsim.createProduction()
if not res['OK']:
  print res['Message']
  exit(1)
res = plcsim.finalizeProd()
if not res['OK']:
  print res['Message']
  exit(1)


###Now do the reconstruction with overlay
plcsim_ov = ProductionJob()
plcsim_ov.setInputDataQuery(meta)

res = plcsim_ov.append(overlay)
if not res['OK']:
  print res['Message']
  exit(1)
res = plcsim_ov.append(lcsim_prepandora_ov)
if not res['OK']:
  print res['Message']
  exit(1)
res = plcsim_ov.append(slicpandora_ov)
if not res['OK']:
  print res['Message']
  exit(1)
res = plcsim_ov.append(lcsim_postpandora_ov)
if not res['OK']:
  print res['Message']
  exit(1)
plcsim_ov.addFinalization(True,True,True,True)

plcsim_ov.setProdType("MCReconstruction_Overlay")
plcsim_ov.setProdGroup(prodGroup)
plcsim_ov.setWorkflowName(workflowName+'_rec_sid_cdr_overlay')
plcsim_ov.setCPUTime(cpuLimit)
plcsim_ov.setOutputSE("CERN-SRM")
plcsim_ov.setDescription('Reconstructing with overlay '+workflowDescription)
res = plcsim_ov.createProduction()
if not res['OK']:
  print res['Message']
  exit(1)
res = plcsim_ov.finalizeProd()
if not res['OK']:
  print res['Message']
  exit(1)

