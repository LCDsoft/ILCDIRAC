'''
Created on Feb 8, 2012

:author: Stephane Poss
'''

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from ILCDIRAC.Interfaces.API.NewInterface.ProductionJob import ProductionJob
from ILCDIRAC.Interfaces.API.NewInterface.Applications import Whizard, Mokka, Marlin, OverlayInput
from ILCDIRAC.Interfaces.API.DiracILC import DiracILC

dirac = DiracILC()

###As it's a full chain, we start at generation
##so we need to define the process and the energy
## The rest will be set later. We could also set the process 
##and the energy directly in the whizard def, but for clarity
## it's better to do it before, that way we know the very 
##essential
process = 'stau1stau1_r'
energy = 1400


##Start by defining the whizard application

wh = Whizard(processlist=dirac.getProcessList())
wh.setModel("sm")
#wh.setModel("susyStagedApproach")
pdict = {}
pdict['process_input'] = {}
pdict['process_input']['process_id']='e2e2n1n1'
pdict['process_input']['sqrts'] = energy
#pdict['process_input']['input_file'] = "LesHouches.msugra_1"
#pdict['process_input']['input_slha_format'] = 'T'

pdict['integration_input'] = {}
pdict['integration_input']['calls'] = '1  500000 10  500000  1  1500000'

pdict['simulation_input'] = {}
pdict['simulation_input']['normalize_weight']='F'
pdict['simulation_input']['n_events']= 1000
pdict['simulation_input']['keep_initials'] = 'T'
pdict['simulation_input']['events_per_file'] = 500000000
#pdict['simulation_input']['pythia_parameters'] = "PMAS(25,1)=120.; PMAS(25,2)=0.3605E-02; MSTU(22)=20 ; MSTJ(28)=2 ;PARJ(21)=0.40000;PARJ(41)=0.11000; PARJ(42)=0.52000; PARJ(81)=0.25000; PARJ(82)=1.90000; MSTJ(11)=3; PARJ(54)=-0.03100; PARJ(55)=-0.00200;PARJ(1)=0.08500; PARJ(3)=0.45000; PARJ(4)=0.02500; PARJ(2)=0.31000; PARJ(11)=0.60000; PARJ(12)=0.40000; PARJ(13)=0.72000;PARJ(14)=0.43000; PARJ(15)=0.08000; PARJ(16)=0.08000; PARJ(17)=0.17000; MSTP(3)=1;IMSS(1)=11; IMSS(21)=71; IMSS(22)=71"
pdict['simulation_input']['pythia_parameters'] = "PMAS(25,1)=12000.; PMAS(25,2)=0.3605E-02; MSTU(22)=20 ; MSTJ(28)=2 ;PARJ(21)=0.40000;PARJ(41)=0.11000; PARJ(42)=0.52000; PARJ(81)=0.25000; PARJ(82)=1.90000; MSTJ(11)=3; PARJ(54)=-0.03100; PARJ(55)=-0.00200;PARJ(1)=0.08500; PARJ(3)=0.45000; PARJ(4)=0.02500; PARJ(2)=0.31000; PARJ(11)=0.60000; PARJ(12)=0.40000; PARJ(13)=0.72000;PARJ(14)=0.43000; PARJ(15)=0.08000; PARJ(16)=0.08000; PARJ(17)=0.17000; MSTP(3)=1;"
pdict['parameter_input'] = {}
pdict['parameter_input']['mH']=12000.
pdict['beam_input_1'] = {}
pdict['beam_input_1']['particle_name']='e1'
pdict['beam_input_1']['polarization'] = "0.0 0.0"
pdict['beam_input_1']['USER_spectrum_on'] = 'T'
pdict['beam_input_1']['USER_spectrum_mode'] = 19
pdict['beam_input_1']['ISR_on'] = 'T'
pdict['beam_input_2'] = {}
pdict['beam_input_2']['particle_name']='E1'
pdict['beam_input_2']['polarization'] = "0.0 0.0"
pdict['beam_input_2']['USER_spectrum_on'] = 'T'
pdict['beam_input_2']['ISR_on'] = 'T'
pdict['beam_input_2']['USER_spectrum_mode'] = -19

wh.setFullParameterDict(pdict)

##Simulation
mo = Mokka()
mo.setVersion('0706P08')
mo.setSteeringFile("clic_ild_cdr.steer")


overlay = OverlayInput()
overlay.setBXOverlay(60)
overlay.setGGToHadInt(1.3)##When running at 1.4TeV
overlay.setDetectorModel("CLIC_ILD_CDR")
overlay.setBkgEvtType("gghad")

##Reconstruction
ma = Marlin()
ma.setVersion('v0111Prod')
ma.setSteeringFile("clic_ild_cdr_steering.xml")
ma.setGearFile("clic_ild_cdr.gear")

##########################################
##Define the generation production.
pwh = ProductionJob()
pwh.setOutputSE("CERN-SRM")
pwh.setProdType("MCGeneration")
pwh.setWorkflowName(process+"_"+str(energy))
pwh.setProdGroup(process+"_"+str(energy))
res = pwh.append(wh)
if not res['OK']:
    print res['Message']
    exit(1)

pwh.addFinalization(True,True,True,True)
pwh.setDescription("CLIC 1.4Tev BeamSpread, ISR ON, whizard")

res = pwh.createProduction()
if not res['OK']:
    print res['Message']
res = pwh.finalizeProd()
if not res['OK']:
    print res['Message']
    exit(1)
pwh.setNbOfTasks(1)
##The production is created, one can now take care of the second step:
#For that we will use the metadata of the previous production as input
meta = pwh.getMetadata()

####################
##Define the second production (simulation). Notice the setInputDataQuery call
pmo = ProductionJob()
pmo.setProdType('MCSimulation')
res = pmo.setInputDataQuery(meta)
if not res['OK']:
  print res['Message']
  exit(1)
pmo.setOutputSE("CERN-SRM")
pmo.setWorkflowName(process+"_"+str(energy)+"_ild_sim")
pmo.setProdGroup(process+"_"+str(energy))
#Add the application
res = pmo.append(mo)
if not res['OK']:
    print res['Message']
    exit(1)
pmo.addFinalization(True,True,True,True)
pmo.setDescription("CLIC_ILD_CDR")

res = pmo.createProduction()
if not res['OK']:
    print res['Message']
res = pmo.finalizeProd()
if not res['OK']:
    print res['Message']
    exit(1)
#As before: get the metadata for this production to input into the next
meta = pmo.getMetadata()
    
#######################
#Define the reconstruction prod    
pma = ProductionJob()
pma.setProdType('MCReconstruction')
res = pma.setInputDataQuery(meta)
if not res['OK']:
  print res['Message']
  exit(1)
pma.setOutputSE("CERN-SRM")
pma.setWorkflowName(process+"_"+str(energy)+"_ild_rec")
pma.setProdGroup(process+"_"+str(energy))

#Add the overlay
res = pma.append(overlay)
if not res['OK']:
    print res['Message']
    exit(1)

#Add the application
res = pma.append(ma)
if not res['OK']:
    print res['Message']
    exit(1)
pma.addFinalization(True,True,True,True)
pma.setDescription("CLIC_ILD_CDR, No Overlay")

res = pma.createProduction()
if not res['OK']:
    print res['Message']
res = pma.finalizeProd()
if not res['OK']:
    print res['Message']
    exit(1)

##In principle nothing else is needed.
