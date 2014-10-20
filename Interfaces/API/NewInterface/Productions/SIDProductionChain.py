'''
Created on Feb 8, 2012

@author: Stephane Poss
'''

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from ILCDIRAC.Interfaces.API.NewInterface.SIDProductionJob import SIDProductionJob
from ILCDIRAC.Interfaces.API.NewInterface.Applications import OverlayInput
from ILCDIRAC.Interfaces.API.NewInterface.Applications import SLIC, LCSIM, SLICPandora, SLCIOSplit, StdHepSplit, Marlin
from ILCDIRAC.Interfaces.API.DiracILC import DiracILC

dirac = DiracILC()

###As it's a full chain, we start at generation
##so we need to define the process and the energy
## The rest will be set later. We could also set the process 
##and the energy directly in the whizard def, but for clarity
## it's better to do it before, that way we know the very 
##essential


analysis = 'tth_siddbd'
process = 'tth-6q-hbb'
additional_name = ''
energy = 1000. #Needed for overlay
meta_energy = '1000'

#For meta def
meta = {}
#meta['ProdID']=838
meta['EvtType']=process
#meta['Energy'] = meta_energy
meta['Datatype'] = 'GEN'
meta['Polarisation'] = 'm80p20'
meta['Machine'] = 'ilc'
meta['DetectorType'] = 'sid'

detector_model = "clic_sid_cdr" #Once set, no need to chnage it, all prods should use the same (except bugs)
tracking_strategy = "defaultStrategies_clic_sid_cdr.xml" #Once set, never change (EVER)
#DoSplit
activesplitstdhep = False
nbevtsperfilestdhep = 100

#Do Sim
sid_sim = True

#DoSplit
activesplit = False
nbevtsperfile = 200

#Do Reco
sid_rec = True
#Do Reco with Overlay
sid_rec_ov = False

n_events = 100 #How many events per job. (Is also obtained from the FC in principle)


##Split
stdhepsplit = StdHepSplit()
stdhepsplit.setVersion("V2")
stdhepsplit.setNumberOfEventsPerFile(nbevtsperfilestdhep)


##Simulation SID
slic = SLIC()
slic.setVersion('v2r9p8')#This will change only once
slic.setSteeringFile('defaultClicCrossingAngle.mac')#This will change only once
slic.setDetectorModel(detector_model)
slic.setNumberOfEvents(n_events) 

##Split
split = SLCIOSplit()
split.setNumberOfEventsPerFile(nbevtsperfile)

## SID Reco w/o overlay
lcsim_prepandora = LCSIM()
lcsim_prepandora.setVersion('CLIC_CDR')#This will change only once
lcsim_prepandora.setSteeringFile("clic_cdr_prePandora.lcsim")#The steering files should NOT CHANGE
lcsim_prepandora.setTrackingStrategy(tracking_strategy)
#lcsim_prepandora.setDetectorModel(detector_model)
lcsim_prepandora.setOutputFile("prePandora.slcio")#NEVER CHANGE THIS, this file is not stored in any case
lcsim_prepandora.setNumberOfEvents(n_events)
lcsim_prepandora.willRunSLICPandora()

slicpandora = SLICPandora()
slicpandora.setVersion('CLIC_CDR')#This will change only once
slicpandora.setDetectorModel(detector_model)
slicpandora.setPandoraSettings("PandoraSettingsSlic.xml")
slicpandora.getInputFromApp(lcsim_prepandora)
slicpandora.setOutputFile('pandora.slcio')#NEVER CHANGE THIS, this file is not stored in any case

lcfivertex = Marlin()
lcfivertex.setVersion()
lcfivertex.getInputFromApp(slicpandora)
lcfivertex.setSteeringFile()
lcfivertex.setOutputFile("lcfivertex.slcio")#NEVER CHANGE THIS, this file is not stored in any case

#Final step. Outpufile is resolved automatically 
lcsim_postpandora = LCSIM()
lcsim_postpandora.setVersion('CLIC_CDR')#This will change only once
lcsim_postpandora.getInputFromApp(lcfivertex)
lcsim_postpandora.setSteeringFile("clic_cdr_postPandoraOverlay.lcsim")#This will change only once
lcsim_postpandora.setTrackingStrategy(tracking_strategy)
#lcsim_postpandora.setDetectorModel(detector_model)

## SID Reco w/o overlay
overlay_sid_gg = OverlayInput()
overlay_sid_gg.setBkgEvtType("gghad")#DO NOT TOUCH THIS
overlay_sid_gg.setEnergy(energy)
if energy == 1000.:
  overlay_sid_gg.setBXOverlay(1)
  overlay_sid_gg.setGGToHadInt(4.1)##When running at 3TeV
else:
  print "Overlay SID: No overlay parameters defined for this energy"  
overlay_sid_gg.setDetectorModel("sidloi3")#DO NOT TOUCH THIS

overlay_sid_pairs = OverlayInput()
overlay_sid_pairs.setBkgEvtType("pairs")#DO NOT TOUCH THIS
overlay_sid_pairs.setEnergy(energy)
if energy == 1000.:
  overlay_sid_pairs.setBXOverlay(1)
  overlay_sid_pairs.setGGToHadInt(1)##When running at 3TeV
else:
  print "Overlay SID: No overlay parameters defined for this energy"  
overlay_sid_pairs.setDetectorModel("sidloi3")#DO NOT TOUCH THIS


lcsim_prepandora_ov = LCSIM()
lcsim_prepandora_ov.setVersion('CLIC_CDR')#This will change only once
if energy==1000.0:
  lcsim_prepandora_ov.setSteeringFile("clic_cdr_prePandoraOverlay_3000.0.lcsim")#This will change only once
else:
  print "LCSIM: No steering files defined for this energy"
  
lcsim_prepandora_ov.setTrackingStrategy(tracking_strategy)
#lcsim_prepandora_ov.setDetectorModel(detector_model)
lcsim_prepandora_ov.setOutputFile("prePandora.slcio")#NEVER CHANGE THIS, this file is not stored in any case
lcsim_prepandora_ov.willRunSLICPandora()

slicpandora_ov = SLICPandora()
slicpandora_ov.getInputFromApp(lcsim_prepandora_ov)
slicpandora_ov.setVersion('CLIC_CDR')#This will change only once
slicpandora_ov.setDetectorModel(detector_model)
slicpandora_ov.setPandoraSettings("PandoraSettingsSlic.xml")#This will change only once
slicpandora_ov.setOutputFile('pandora.slcio')#NEVER CHANGE THIS, this file is not stored in any case

lcfivertex_ov = Marlin()
lcfivertex_ov.setVersion()
lcfivertex_ov.getInputFromApp(slicpandora_ov)
lcfivertex_ov.setSteeringFile()
lcfivertex_ov.setOutputFile("lcfivertex.slcio")#NEVER CHANGE THIS, this file is not stored in any case

#Final step. Outpufile is resolved automatically 
lcsim_postpandora_ov = LCSIM()
lcsim_postpandora_ov.getInputFromApp(lcfivertex_ov)
lcsim_postpandora_ov.setVersion('CLIC_CDR')#This will change only once
lcsim_postpandora_ov.setSteeringFile("clic_cdr_postPandoraOverlay.lcsim")#This will change only once
lcsim_postpandora_ov.setTrackingStrategy(tracking_strategy)
#lcsim_postpandora_ov.setDetectorModel(detector_model)

############################################
#What is below WILL NEVER NEED TO BE TOUCHED 
#(I'm not kidding, if you touch and break, not my problem)
#
if activesplitstdhep and meta:
  pstdhepsplit =  SIDProductionJob()
  pstdhepsplit.setLogLevel("verbose")
  pstdhepsplit.setProdType('Split')
  res = pstdhepsplit.setInputDataQuery(meta)
  if not res['OK']:
    print res['Message']
    exit(1)
  pstdhepsplit.setOutputSE("CERN-SRM")
  wname = process+"_"+str(energy)+"_split"
  wname += additional_name  
  pstdhepsplit.setWorkflowName(wname)
  pstdhepsplit.setProdGroup(analysis+"_"+str(energy))
  
  #Add the application
  res = pstdhepsplit.append(stdhepsplit)
  if not res['OK']:
    print res['Message']
    exit(1)
  pstdhepsplit.addFinalization(True,True,True,True)
  descrp = "Splitting stdhep files"
  if additional_name:  
    descrp += ", %s"%additional_name
  pstdhepsplit.setDescription(descrp)  
  
  res = pstdhepsplit.createProduction()
  if not res['OK']:
    print res['Message']
  res = pstdhepsplit.finalizeProd()
  if not res['OK']:
    print res['Message']
    exit(1)
  #As before: get the metadata for this production to input into the next
  meta = pstdhepsplit.getMetadata()
  

if sid_sim and meta:
  ####################
  ##Define the second production (simulation). Notice the setInputDataQuery call
  psl = SIDProductionJob()
  psl.setLogLevel("verbose")
  psl.setProdType('MCSimulation')
  res = psl.setInputDataQuery(meta)
  if not res['OK']:
    print res['Message']
    exit(1)
  psl.setOutputSE("CERN-SRM")
  wname = process+"_"+str(energy)+"_sid_sim"
  wname += additional_name  
  psl.setWorkflowName(wname)
  psl.setProdGroup(analysis+"_"+str(energy))
  #Add the application
  res = psl.append(slic)
  if not res['OK']:
    print res['Message']
    exit(1)
  psl.addFinalization(True,True,True,True)
  descrp = "CLIC_SID_CDR model"
  if additional_name:  
    descrp += ", %s"%additional_name
  psl.setDescription(descrp)

  res = psl.createProduction()
  if not res['OK']:
    print res['Message']
  res = psl.finalizeProd()
  if not res['OK']:
    print res['Message']
    exit(1)
  #As before: get the metadata for this production to input into the next
  meta = psl.getMetadata()

if activesplit and meta:
  #######################
  ## Split the input files.  
  psplit =  SIDProductionJob()
  psplit.setCPUTime(30000)
  psplit.setLogLevel("verbose")
  psplit.setProdType('Split')
  psplit.setDestination("LCG.CERN.ch")
  res = psplit.setInputDataQuery(meta)
  if not res['OK']:
    print res['Message']
    exit(1)
  psplit.setOutputSE("CERN-SRM")
  wname = process+"_"+str(energy)+"_split"
  wname += additional_name  
  psplit.setWorkflowName(wname)
  psplit.setProdGroup(analysis+"_"+str(energy))
  
  #Add the application
  res = psplit.append(split)
  if not res['OK']:
    print res['Message']
    exit(1)
  psplit.addFinalization(True,True,True,True)
  descrp = "Splitting slcio files"
  if additional_name:  
    descrp += ", %s"%additional_name
  psplit.setDescription(descrp)  
  
  res = psplit.createProduction()
  if not res['OK']:
    print res['Message']
  res = psplit.finalizeProd()
  if not res['OK']:
    print res['Message']
    exit(1)
  #As before: get the metadata for this production to input into the next
  meta = psplit.getMetadata()
  


if sid_rec and meta:
  #######################
  #Define the reconstruction prod      
  psidrec = SIDProductionJob()
  psidrec.setLogLevel("verbose")
  psidrec.setProdType('MCReconstruction')
  psidrec.setBannedSites(['LCG.Bristol.uk','LCG.RAL-LCG2.uk'])
  res = psidrec.setInputDataQuery(meta)
  if not res['OK']:
    print res['Message']
    exit(1)
  psidrec.setOutputSE("CERN-SRM")
  wname = process+"_"+str(energy)+"_sid_rec"
  wname += additional_name  
  psidrec.setWorkflowName(wname)
  psidrec.setProdGroup(analysis+"_"+str(energy))
  res = psidrec.append(lcsim_prepandora)
  if not res['OK']:
    print res['Message']
    exit(1)
  res = psidrec.append(slicpandora)
  if not res['OK']:
    print res['Message']
    exit(1)
  res = psidrec.append(lcsim_postpandora)
  if not res['OK']:
    print res['Message']
    exit(1)
  psidrec.addFinalization(True,True,True,True)
  descrp = "CLIC_SID_CDR, No overlay"
  if additional_name:  
    descrp += ", %s"%additional_name  
  psidrec.setDescription(descrp)
  
  res = psidrec.createProduction()
  if not res['OK']:
    print res['Message']
  res = psidrec.finalizeProd()
  if not res['OK']:
    print res['Message']
    exit(1)

if sid_rec_ov and meta:
  #######################
  #Define the reconstruction prod      
  psidreco = SIDProductionJob()
  psidreco.setLogLevel("verbose")
  psidreco.setProdType('MCReconstruction_Overlay')
  psidreco.setBannedSites(['LCG.Bristol.uk','LCG.RAL-LCG2.uk'])
  res = psidreco.setInputDataQuery(meta)
  if not res['OK']:
    print res['Message']
    exit(1)
  psidreco.setOutputSE("CERN-SRM")
  wname = process+"_"+str(energy)+"_sid_rec_overlay"
  wname += additional_name  
  psidreco.setWorkflowName(wname)
  psidreco.setProdGroup(analysis+"_"+str(energy))
  res = psidreco.append(overlay_sid_gg)
  if not res['OK']:
    print res['Message']
    exit(1)
  res = psidreco.append(overlay_sid_pairs)
  if not res['OK']:
    print res['Message']
    exit(1)
  res = psidreco.append(lcsim_prepandora_ov)
  if not res['OK']:
    print res['Message']
    exit(1)
  res = psidreco.append(slicpandora_ov)
  if not res['OK']:
    print res['Message']
    exit(1)
  res = psidreco.append(lcsim_postpandora_ov)
  if not res['OK']:
    print res['Message']
    exit(1)
  psidreco.addFinalization(True,True,True,True)
  descrp = "CLIC_SID_CDR, overlay"
  if additional_name:  
    descrp += ", %s"%additional_name
  psidreco.setDescription(descrp)
  
  res = psidreco.createProduction()
  if not res['OK']:
    print res['Message']
  res = psidreco.finalizeProd()
  if not res['OK']:
    print res['Message']
    exit(1)
    
##In principle nothing else is needed.
