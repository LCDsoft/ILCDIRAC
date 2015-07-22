'''
Created on Mar 26, 2012

@author: Stephane Poss
'''

__RCSID__ = "$Id$"
#pylint: skip-file
#pylint: disable=C0103
from DIRAC.Core.Base import Script
Script.parseCommandLine()

from ILCDIRAC.Interfaces.API.NewInterface.ILDProductionJob import ILDProductionJob
from ILCDIRAC.Interfaces.API.NewInterface.Applications import Mokka, Marlin, OverlayInput
from ILCDIRAC.Interfaces.API.NewInterface.Applications import SLCIOSplit, StdHepSplit

analysis = 'ILD-DBD-ttH' ##Some analysis: the prods will belong to the ProdGroup
process = '106452' ##Only used for the meta data query, here tth-6q-hbb
additional_name = '' ## This is to allow defining unique name productions
energy = 1000. ##This is mostly needed to define easily the steering files and the overlay parameters
meta_energy = '1000' ##This is needed for the meta data search below

dryrun = True #do not register anything nor create anything. 
# Should be used once the splitting-at-stdhep-level prods are submitted.

detectorModel = 'ILD_o1_v05' ##OR anything valid, but be careful with the overlay, the files need to exist
ILDConfig = 'SOMETHING' #whatever you defined

#For meta def
##This is where magic happens
meta = {}
#meta['ProdID']=1 
meta['GenProcessID']=process
meta['Energy'] = meta_energy
meta['Machine'] = 'ilc'

#DoSplit at stdhep level
activesplitstdhep = False
nbevtsperfilestdhep = 100

#Do Sim
ild_sim = False
nbtasks = 10 #Take 10 files from input meta data query result
#It's possible to get this number automatically by getting the number of events per file (if known)
#nbtasks = math.ceil(number_of_events_to_process/nb_events_per_signal_file) #needs import math
#can be extended with dirac-ilc-add-tasks-to-prod

#DoSplit
activesplit = False
nbevtsperfile = 200

#Do Reco
ild_rec = False
#Do Reco with Overlay
ild_rec_ov = False




###### Whatever is below is not to be touched... Or at least only when something changes

##Split
stdhepsplit = StdHepSplit()
stdhepsplit.setVersion("V2")
stdhepsplit.setNumberOfEventsPerFile(nbevtsperfilestdhep)

##Simulation ILD
mo = Mokka()
mo.setVersion('0706P08') ###SET HERE YOUR MOKKA VERSION
mo.setDetectorModel(detectorModel)
if energy in [500.]: ##YOU COULD HAVE THE SAME STEERING FILE FOR DIFFERENT ENERGIES
  mo.setSteeringFile("clic_ild_cdr500.steer") ## define the prod steering file
else:
  mo.setSteeringFile("clic_ild_cdr.steer")## define the prod steering file


##Split
split = SLCIOSplit()
split.setNumberOfEventsPerFile(nbevtsperfile)

##Define the overlay
overlay = OverlayInput()
overlay.setMachine("ilc_dbd") #Don't touch, this is how the system knows what files to get
overlay.setEnergy(energy)#Don't touch, this is how the system knows what files to get
overlay.setDetectorModel(detectorModel)#Don't touch, this is how the system knows what files to get
overlay.setBkgEvtType("aa_lowpt")
if energy==500.: #here you chose the overlay parameters as this determines how many files you need
  #it does NOT affect the content of the marlin steering file whatsoever, you need to make sure the values 
  #there are correct. Only the file names are handled properly so that you don't need to care
  overlay.setBXOverlay(300)
  overlay.setGGToHadInt(0.3)##When running at 500geV
elif energy == 1000.:
  overlay.setBXOverlay(60)
  overlay.setGGToHadInt(1.3)##When running at 1tev
else:
  print "Overlay ILD: No overlay parameters defined for this energy"  

##Reconstruction ILD with overlay
mao = Marlin()
mao.setDebug()
mao.setVersion('v0111Prod') ##PUT HERE YOUR MARLIN VERSION
if ild_rec_ov:
  if energy==500.:
    mao.setSteeringFile("clic_ild_cdr500_steering_overlay.xml") #STEERINGFILE for 500gev
    mao.setGearFile('clic_ild_cdr500.gear') #GEAR FILE for 500gev
  elif energy==1000.0:
    mao.setSteeringFile("clic_ild_cdr_steering_overlay_1400.0.xml") #STEERINGFILE for 1tev
    mao.setGearFile('clic_ild_cdr.gear') #GEAR FILE for 1tev
  else:
    print "Marlin: No reconstruction suitable for this energy"


##Reconstruction ILD w/o overlay
ma = Marlin()
ma.setDebug()
ma.setVersion('v0111Prod') ##PUT HERE YOUR MARLIN VERSION
if ild_rec:
  if energy in [500.]:
    ma.setSteeringFile("clic_ild_cdr500_steering.xml")##PUT HERE YOUR MARLIN steering files
    ma.setGearFile('clic_ild_cdr500.gear')
  elif energy in [1000.]:
    ma.setSteeringFile("clic_ild_cdr_steering.xml")
    ma.setGearFile('clic_ild_cdr.gear')
  else:
    print "Marlin: No reconstruction suitable for this energy"

###################################################################################
### HERE WE DEFINE THE PRODUCTIONS  
if activesplitstdhep and meta:
  pstdhepsplit =  ILDProductionJob()
  pstdhepsplit.basepath = "/ilc/prod/ilc/mc-dbd.generated/ild/"
  pstdhepsplit.setDryRun(dryrun)
  pstdhepsplit.setLogLevel("verbose")
  pstdhepsplit.setProdType('Split')
  res = pstdhepsplit.setInputDataQuery(meta)
  if not res['OK']:
    print res['Message']
    exit(1)
  pstdhepsplit.setOutputSE("DESY-SRM")
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
  
if ild_sim and meta:
  ####################
  ##Define the second production (simulation). Notice the setInputDataQuery call
  pmo = ILDProductionJob()
  pmo.setDryRun(dryrun)
  pmo.setProdPlugin('Limited')
  pmo.setILDConfig(ILDConfig)
  pmo.setLogLevel("verbose")
  pmo.setProdType('MCSimulation')
  res = pmo.setInputDataQuery(meta)
  if not res['OK']:
    print res['Message']
    exit(1)
  pmo.setOutputSE("DESY-SRM")
  wname = process+"_"+str(energy)+"_ild_sim"
  wname += additional_name  
  pmo.setWorkflowName(wname)
  pmo.setProdGroup(analysis+"_"+str(energy))
  #Add the application
  res = pmo.append(mo)
  if not res['OK']:
    print res['Message']
    exit(1)
  pmo.addFinalization(True,True,True,True)
  descrp = "%s model" % detectorModel
  
  if additional_name:  
    descrp += ", %s"%additional_name   
  pmo.setDescription(descrp)
  res = pmo.createProduction()
  if not res['OK']:
    print res['Message']
  res = pmo.finalizeProd()
  if not res['OK']:
    print res['Message']
    exit(1)
  pmo.setNbOfTasks(nbtasks)    
  #As before: get the metadata for this production to input into the next
  meta = pmo.getMetadata()

##Split at slcio level (after sim)
if activesplit and meta:
  #######################
  ## Split the input files.  
  psplit =  ILDProductionJob()
  psplit.setDryRun(dryrun)
  psplit.setCPUTime(30000)
  psplit.setLogLevel("verbose")
  psplit.setProdType('Split')
  psplit.setDestination("LCG.CERN.ch") #this is because we can do it there.
  res = psplit.setInputDataQuery(meta)
  if not res['OK']:
    print res['Message']
    exit(1)
  psplit.setOutputSE("CERN-SRM")#this is because we can do it there.
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
  
if ild_rec and meta:
  #######################
  #Define the reconstruction prod    
  pma = ILDProductionJob()
  pma.setDryRun(dryrun)
  pma.setILDConfig(ILDConfig)
  pma.setLogLevel("verbose")
  pma.setProdType('MCReconstruction')
  res = pma.setInputDataQuery(meta)
  if not res['OK']:
    print res['Message']
    exit(1)
  pma.setOutputSE("DESY-SRM")
  wname = process+"_"+str(energy)+"_ild_rec"
  wname += additional_name  
  pma.setWorkflowName(wname)
  pma.setProdGroup(analysis+"_"+str(energy))
  
  #Add the application
  res = pma.append(ma)
  if not res['OK']:
    print res['Message']
    exit(1)
  pma.addFinalization(True,True,True,True)
  descrp = "%s, No overlay" % detectorModel
  if additional_name:  
    descrp += ", %s"%additional_name  
  pma.setDescription(descrp)
  
  res = pma.createProduction()
  if not res['OK']:
    print res['Message']
  res = pma.finalizeProd()
  if not res['OK']:
    print res['Message']
    exit(1)

if ild_rec_ov and meta:
  #######################
  #Define the reconstruction prod    
  pmao = ILDProductionJob()
  pmao.setILDConfig(ILDConfig)
  pmao.setLogLevel("verbose")
  pmao.setProdType('MCReconstruction_Overlay')
  res = pmao.setInputDataQuery(meta)
  if not res['OK']:
    print res['Message']
    exit(1)
  pmao.setOutputSE("DESY-SRM")
  wname = process+"_"+str(energy)+"_ild_rec_overlay"
  wname += additional_name  
  pmao.setWorkflowName(wname)
  pmao.setProdGroup(analysis+"_"+str(energy))
  
  #Add the application
  res = pmao.append(overlay)
  if not res['OK']:
    print res['Message']
    exit(1)
  #Add the application
  res = pmao.append(mao)
  if not res['OK']:
    print res['Message']
    exit(1)
  pmao.addFinalization(True,True,True,True)
  descrp = "%s, Overlay" % detectorModel
  
  if additional_name:  
    descrp += ", %s"%additional_name
  pmao.setDescription( descrp ) 
  res = pmao.createProduction()
  if not res['OK']:
    print res['Message']
  res = pmao.finalizeProd()
  if not res['OK']:
    print res['Message']
    exit(1)

    
##In principle nothing else is needed.
