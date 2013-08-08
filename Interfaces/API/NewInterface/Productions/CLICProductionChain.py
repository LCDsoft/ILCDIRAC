'''
Created on Feb 8, 2012

@author: Stephane Poss
'''

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from ILCDIRAC.Interfaces.API.NewInterface.ProductionJob import ProductionJob
from ILCDIRAC.Interfaces.API.NewInterface.Applications import Whizard, Mokka, Marlin, OverlayInput, StdhepCut, StdhepCutJava
from ILCDIRAC.Interfaces.API.NewInterface.Applications import SLIC, LCSIM, SLICPandora, SLCIOSplit, StdHepSplit
from ILCDIRAC.Interfaces.API.DiracILC import DiracILC

dirac = DiracILC()

###As it's a full chain, we start at generation
##so we need to define the process and the energy
## The rest will be set later. We could also set the process 
##and the energy directly in the whizard def, but for clarity
## it's better to do it before, that way we know the very 
##essential


def getdicts(process):
  """ Create the proper structures to build all the prodcutions for the samples with ee_, ea_ aa_.
  """
  plist = []
  if process.count("ee_"):
    plist.append({'process':process,'pname1':'e1', 'pname2':'E1', "epa_b1":'F', "epa_b2":'F'})
  elif process.count("ea_"):
    plist.append({'process':process,'pname1':'e1', 'pname2':'E1', "epa_b1":'F', "epa_b2":'T'})
    plist.append({'process':process,'pname1':'e1', 'pname2':'A', "epa_b1":'F', "epa_b2":'F'})
    plist.append({'process':process.replace("ea_","ae_"),'pname1':'e1', 'pname2':'E1', "epa_b1":'T', "epa_b2":'F'})
    plist.append({'process':process.replace("ea_","ae_"),'pname1':'A', 'pname2':'E1', "epa_b1":'F', "epa_b2":'F'})
  elif process.count("aa_"):
    plist.append({'process':process,'pname1':'e1', 'pname2':'E1', "epa_b1":'T', "epa_b2":'T'})
    plist.append({'process':process,'pname1':'e1', 'pname2':'A', "epa_b1":'T', "epa_b2":'F'})
    plist.append({'process':process,'pname1':'A', 'pname2':'E1', "epa_b1":'F', "epa_b2":'T'})
    plist.append({'process':process,'pname1':'A', 'pname2':'A', "epa_b1":'F', "epa_b2":'F'})
  else:
    plist.append({'process':process,'pname1':'e1', 'pname2':'E1', "epa_b1":'F', "epa_b2":'F'})
  return plist

## tripleH, Hrecoil, stau, gauginos, Hmass, tt, Htautau, Hmumu, Hee, Hbbccmumu, squarks, LCFITraining, Hgammagamma
## HZgamma Hinclusive ZZfusion, Any, ttH, bb_cc_gg
analysis = 'tripleH'
process = 'tt'
#additional_name = '_neu1_356'
globname = ""
additional_name = ''
energy = 3000.
meta_energy = str(int(energy))

#For meta def
meta = {}
meta['ProdID']=1
meta['EvtType']=process
meta['Energy'] = meta_energy


detectormodel='' #Can be ILD_00 (and noting else)

#Here get the prod list: initial particles combinasions
prodlist = getdicts(process)

beamrecoil = True

#Do generation
gen = False

#For cuts
cut = False
if cut:
  additional_name += "_cut"

javacut = True
cutfile = 'cuts_qq_nunu_1400.txt'
seleff = 0.52
n_keep = 500

#DoSplit
activesplitstdhep = False
if activesplitstdhep:
  additional_name += "_gensplit"
nbevtsperfilestdhep = 100

#Do Sim
ild_sim = False
sid_sim = True

#DoSplit
activesplit = False
if activesplit:
  additional_name += "_simsplit"

nbevtsperfile = 200

#Do Reco
ild_rec = False
sid_rec = False
#Do Reco with Overlay
ild_rec_ov = False
sid_rec_ov = True

n_events = 100

model = 'sm'
#model = 'susyStagedApproach'
#model = 'susyStagedApproach'+additional_name
#additional_name = '_'
##NO TAUOLA ADD MSTJ(28)=2 in pythia parameters
whcutdict = {}
#whcutdict = {'e2e2_o':["cut M of 3 within 100 150"]}

additionnalreqs = ''
#additionnalreqs = 'MDCY(25,2)=214;MDCY(25,3)=1;' #for decay of H to bb
#additionnalreqs = 'MDCY(25,2)=213;MDCY(25,3)=1;' #for decay of H to cc
#additionnalreqs = 'MDCY(25,2)=219;MDCY(25,3)=1;' #for decay of H to mumu
#additionnalreqs = 'MWID(25)=2;MDCY(25,2)=223;MDCY(25,3)=1;BRAT(223)=1' #for decay of H to gammagamma
#additionnalreqs = 'MWID(25)=2;MDCY(25,2)=224;MDCY(25,3)=1;BRAT(224)=1;MDCY(23,2)=174;MDCY(23,3)=5' #for decay of H to Zgamma, Z to qq
#additionnalreqs = 'MWID(25)=2;MDCY(25,2)=224;MDCY(25,3)=1;BRAT(224)=1;MWID(23)=2;MDCY(23,2)=182;MDCY(23,3)=6; BRAT(182)=0.3333;BRAT(183)=0.;BRAT(184)=0.3333;BRAT(185)=0.;BRAT(186)=0.333$
#additionnalreqs = 'MDCY(25,3)=5;' #for decay of H to quarks
#additionnalreqs = 'MDCY(25,2)=220; MDCY(25,3)=1;'#for decay of H to tau tau
#additionnalreqs = 'MDCY(23,2)=174; MDCY(23,3)=3;'#for decay of Z to bb
mh = 12000.
mb = 2.9
mc = 0
mmu = 0.10566
mtau = 1.77
##Use gridfiles ?
gridfiles = True
##Set exchange lines to 4 (needed for complex final states)
exchange_lines = False







###### Whatever is below is not to be touched...
for proddict in prodlist:
  prod_name = additional_name
  process = proddict['process']
  
  if energy==1400.:
      spectrum = 19
  elif energy == 3000.:
      spectrum = 11
  elif energy == 500.:
      spectrum = 13
  elif energy == 350.:
      spectrum = 20
  else:
      print "No spectrum defined, cannot proceed"
      exit(1)
  
  
  ##Start by defining the whizard application
  
  wh = Whizard(processlist=dirac.getProcessList())
  wh.setModel(model)
  pdict = {}
  pdict['process_input'] = {}
  pdict['process_input']['process_id']=proddict['process']
  pdict['process_input']['sqrts'] = energy
  if not model=='sm':
    pdict['process_input']['input_file'] = "LesHouches.msugra_1"
    pdict['process_input']['input_slha_format'] = 'T'
  if beamrecoil and gen:
    prod_name += "_beamrecoil"
    pdict['process_input']['beam_recoil']='T'

  pdict['integration_input'] = {}
  pdict['integration_input']['calls'] = '1  500000 10  500000  1  1500000'
  if gridfiles:
    pdict['integration_input']['read_grids'] = 'T'
  if exchange_lines:
    pdict['integration_input']['exchange_lines'] = 4  
  pdict['simulation_input'] = {}
  pdict['simulation_input']['normalize_weight']='F'
  pdict['simulation_input']['n_events']= n_events
  pdict['simulation_input']['keep_initials'] = 'T'
  pdict['simulation_input']['events_per_file'] = 500000
  if not model=='sm':
    pdict['simulation_input']['pythia_parameters'] = "PMAS(25,1)=%s; PMAS(25,2)=0.3605E-02; MSTU(22)=20 ;PARJ(21)=0.40000;PARJ(41)=0.11000; PARJ(42)=0.52000; PARJ(81)=0.25000; PARJ(82)=1.90000; MSTJ(11)=3; PARJ(54)=-0.03100; PARJ(55)=-0.00200;PARJ(1)=0.08500; PARJ(3)=0.45000; PARJ(4)=0.02500; PARJ(2)=0.31000; PARJ(11)=0.60000; PARJ(12)=0.40000; PARJ(13)=0.72000;PARJ(14)=0.43000; PARJ(15)=0.08000; PARJ(16)=0.08000; PARJ(17)=0.17000; MSTP(3)=1;IMSS(1)=11; IMSS(21)=71; IMSS(22)=71;%s"%(mh,additionnalreqs)
  else:  
    pdict['simulation_input']['pythia_parameters'] = "PMAS(25,1)=%s; PMAS(25,2)=0.3605E-02; MSTU(22)=20 ; MSTJ(28)=2 ;PARJ(21)=0.40000;PARJ(41)=0.11000; PARJ(42)=0.52000; PARJ(81)=0.25000; PARJ(82)=1.90000; MSTJ(11)=3; PARJ(54)=-0.03100; PARJ(55)=-0.00200;PARJ(1)=0.08500; PARJ(3)=0.45000; PARJ(4)=0.02500; PARJ(2)=0.31000; PARJ(11)=0.60000; PARJ(12)=0.40000; PARJ(13)=0.72000;PARJ(14)=0.43000; PARJ(15)=0.08000; PARJ(16)=0.08000; PARJ(17)=0.17000; MSTP(3)=1;%s"%(mh,additionnalreqs)
  if model=='sm':
    pdict['parameter_input'] = {}
  #  pdict['parameter_input']['mmu']=mmu
  #  pdict['parameter_input']['mtau']=mtau
  #  pdict['parameter_input']['mb']=mb
  #  pdict['parameter_input']['mc']=mc
    pdict['parameter_input']['mH']=mh
  pdict['beam_input_1'] = {}
  pdict['beam_input_1']['particle_name']=proddict['pname1']
  pdict['beam_input_1']['polarization'] = "0.0 0.0"
  pdict['beam_input_1']['USER_spectrum_on'] = 'T'
  pdict['beam_input_1']['USER_spectrum_mode'] = spectrum
  pdict['beam_input_1']['ISR_on'] = 'T'
  pdict['beam_input_1']['EPA_on'] = proddict['epa_b1']

  pdict['beam_input_2'] = {}
  pdict['beam_input_2']['particle_name']=proddict['pname2']
  pdict['beam_input_2']['polarization'] = "0.0 0.0"
  pdict['beam_input_2']['USER_spectrum_on'] = 'T'
  pdict['beam_input_2']['ISR_on'] = 'T'
  pdict['beam_input_2']['USER_spectrum_mode'] = -spectrum
  pdict['beam_input_2']['EPA_on'] = proddict['epa_b2']
  
  prod_name+= "_"+proddict['pname1']+proddict['epa_b1']+"_"+proddict['pname2']+proddict['epa_b2']
  
  wh.setFullParameterDict(pdict)
  #wh.setGlobalEvtType("aa_e3e3nn")
  #process = "aa_e3e3nn"
  if globname:
    wh.setGlobalEvtType(globname)
    process = globname

  if whcutdict:
    wh.setGeneratorLevelCuts(whcutdict)

  if cut:
    wh.willCut()
  
  if gridfiles:
    wh.usingGridFiles()  
  
  if javacut:
    stdhepc = StdhepCutJava()
    stdhepc.setVersion('1.0')
  else:
    stdhepc = StdhepCut()
    stdhepc.setVersion("V7")
  if cut and not cutfile:
      print "No cut file defined, cannot proceed"
      exit(1)
  stdhepc.setSteeringFile(cutfile)
  stdhepc.setMaxNbEvts(n_keep)
  stdhepc.setSelectionEfficiency(seleff)
  
  ##Split
  stdhepsplit = StdHepSplit()
  stdhepsplit.setVersion("V2")
  stdhepsplit.setNumberOfEventsPerFile(nbevtsperfilestdhep)
  
  ##Simulation ILD
  mo = Mokka()
  mo.setVersion('0706P08')
  #mo.setNbEvts(1000)
  if energy in [500., 375., 350.]:
    mo.setSteeringFile("clic_ild_cdr500.steer")
  elif energy in [3000., 1400.]:
    mo.setSteeringFile("clic_ild_cdr.steer")
  else:
    print 'Detector Model for Mokka undefined for this energy'  
  if detectormodel=='ild_00':
    mo.setSteeringFile("ild_00.steer")

  
  ##Simulation SID
  slic = SLIC()
  slic.setVersion('v2r9p8')
  slic.setSteeringFile('defaultClicCrossingAngle.mac')
  slic.setDetectorModel('clic_sid_cdr')
  
  
  ##Split
  split = SLCIOSplit()
  split.setNumberOfEventsPerFile(nbevtsperfile)
  
  
  overlay = OverlayInput()
  overlay.setMachine("clic_cdr")
  overlay.setEnergy(energy)
  overlay.setBkgEvtType("gghad")
  if energy==500.:
    overlay.setBXOverlay(300)
    overlay.setGGToHadInt(0.3)##When running at 500geV
    overlay.setDetectorModel("CLIC_ILD_CDR500")
  elif energy == 350.:
    overlay.setBXOverlay(300)
    overlay.setGGToHadInt(0.0464)##When running at 350geV
    overlay.setDetectorModel("CLIC_ILD_CDR500")
  elif energy == 3000.:
    overlay.setBXOverlay(60)
    overlay.setGGToHadInt(3.2)##When running at 3TeV
    overlay.setDetectorModel("CLIC_ILD_CDR")
  elif energy == 1400.:
    overlay.setBXOverlay(60)
    overlay.setGGToHadInt(1.3)##When running at 1.4TeV
    overlay.setDetectorModel("CLIC_ILD_CDR")
  else:
    print "Overlay ILD: No overlay parameters defined for this energy"  
  
  ##Reconstruction ILD with overlay
  mao = Marlin()
  mao.setDebug()
  mao.setVersion('v0111Prod')
  if ild_rec_ov:
    if energy==500.:
      mao.setSteeringFile("clic_ild_cdr500_steering_overlay.xml")
      mao.setGearFile('clic_ild_cdr500.gear')
    elif energy==350.:
      mao.setSteeringFile("clic_ild_cdr500_steering_overlay_350.0.xml")
      mao.setGearFile('clic_ild_cdr500.gear')
    elif energy==3000.0:
      mao.setSteeringFile("clic_ild_cdr_steering_overlay_3000.0.xml")
      mao.setGearFile('clic_ild_cdr.gear')
    elif energy==1400.0:
      mao.setSteeringFile("clic_ild_cdr_steering_overlay_1400.0.xml")
      mao.setGearFile('clic_ild_cdr.gear')
    else:
      print "Marlin: No reconstruction suitable for this energy"
  
  
  ##Reconstruction w/o overlay
  ma = Marlin()
  ma.setDebug()
  ma.setVersion('v0111Prod')
  if ild_rec:
    if energy in [500.,350.]:
      ma.setSteeringFile("clic_ild_cdr500_steering.xml")
      ma.setGearFile('clic_ild_cdr500.gear')
    elif energy in [3000., 1400.]:
      ma.setSteeringFile("clic_ild_cdr_steering.xml")
      ma.setGearFile('clic_ild_cdr.gear')
    else:
      print "Marlin: No reconstruction suitable for this energy"
  
  ## SID Reco w/o overlay
  lcsim_prepandora = LCSIM()
  lcsim_prepandora.setVersion('CLIC_CDR')
  lcsim_prepandora.setSteeringFile("clic_cdr_prePandora.lcsim")
  lcsim_prepandora.setTrackingStrategy("defaultStrategies_clic_sid_cdr.xml")
  #lcsim_prepandora.setDetectorModel('clic_sid_cdr')
  lcsim_prepandora.setOutputFile("prePandora.slcio")
  lcsim_prepandora.willRunSLICPandora()
  
  slicpandora = SLICPandora()
  slicpandora.setVersion('CLIC_CDR')
  slicpandora.setDetectorModel('clic_sid_cdr')
  slicpandora.setPandoraSettings("PandoraSettingsSlic.xml")
  slicpandora.getInputFromApp(lcsim_prepandora)
  slicpandora.setOutputFile('pandora.slcio')
  
  lcsim_postpandora = LCSIM()
  lcsim_postpandora.setVersion('CLIC_CDR')
  lcsim_postpandora.getInputFromApp(slicpandora)
  lcsim_postpandora.setSteeringFile("clic_cdr_postPandoraOverlay.lcsim")
  lcsim_postpandora.setTrackingStrategy("defaultStrategies_clic_sid_cdr.xml")
  #lcsim_postpandora.setDetectorModel('clic_sid_cdr')
  
  ## SID Reco w/o overlay
  overlay_sid = OverlayInput()
  overlay_sid.setMachine("clic_cdr")
  overlay_sid.setEnergy(energy)
  overlay_sid.setBkgEvtType("gghad")
  if energy == 3000.:
    overlay_sid.setBXOverlay(60)
    overlay_sid.setGGToHadInt(3.2)##When running at 3TeV
  elif energy == 350.:
    overlay.setBXOverlay(300)
    overlay.setGGToHadInt(0.0464)##When running at 350geV
  elif energy == 1400.:
    overlay_sid.setBXOverlay(60)
    overlay_sid.setGGToHadInt(1.3)##When running at 1.4TeV
  else:
    print "Overlay SID: No overlay parameters defined for this energy"  
  overlay_sid.setDetectorModel("CLIC_SID_CDR")
  
  
  lcsim_prepandora_ov = LCSIM()
  lcsim_prepandora_ov.setVersion('CLIC_CDR')
  if energy==3000.0:
    lcsim_prepandora_ov.setSteeringFile("clic_cdr_prePandoraOverlay_3000.0.lcsim")
  elif energy == 1400.0:   
    lcsim_prepandora_ov.setSteeringFile("clic_cdr_prePandoraOverlay_1400.0.lcsim")
  else:
    print "LCSIM: No steering files defined for this energy"
    
  lcsim_prepandora_ov.setTrackingStrategy("defaultStrategies_clic_sid_cdr.xml")
  #lcsim_prepandora_ov.setDetectorModel('clic_sid_cdr')
  lcsim_prepandora_ov.setOutputFile("prePandora.slcio")
  lcsim_prepandora_ov.willRunSLICPandora()
  
  slicpandora_ov = SLICPandora()
  slicpandora_ov.getInputFromApp(lcsim_prepandora_ov)
  slicpandora_ov.setVersion('CLIC_CDR')
  slicpandora_ov.setDetectorModel('clic_sid_cdr')
  slicpandora_ov.setPandoraSettings("PandoraSettingsSlic.xml")
  slicpandora_ov.setOutputFile('pandora.slcio')
  
  lcsim_postpandora_ov = LCSIM()
  lcsim_postpandora_ov.getInputFromApp(slicpandora_ov)
  lcsim_postpandora_ov.setVersion('CLIC_CDR')
  lcsim_postpandora_ov.setSteeringFile("clic_cdr_postPandoraOverlay.lcsim")
  lcsim_postpandora_ov.setTrackingStrategy("defaultStrategies_clic_sid_cdr.xml")
  #lcsim_postpandora_ov.setDetectorModel('clic_sid_cdr')
  
  
  if gen:  
    ##########################################
    ##Define the generation production.
    pwh = ProductionJob()
    pwh.setLogLevel("verbose")
    pwh.setOutputSE("CERN-SRM")
    pwh.setProdType("MCGeneration")
    wname = process+"_"+str(energy)
    if additionnalreqs:
      wname += "_forced_decay"  
    if cut:
      wname += "_cut"
    wname += prod_name
    pwh.setWorkflowName(wname)  
    pwh.setProdGroup(analysis+"_"+str(energy))
    res = pwh.append(wh)
    if not res['OK']:
        print res['Message']
        exit(1)
    
    if cut:
        res = pwh.append(stdhepc)
        if not res['OK']:
            print res['Message']
            exit(1)
    
    pwh.addFinalization(True,True,True,True)
    descrp = "CLIC %s BeamSpread, ISR ON, whizard"%energy
    if additionnalreqs:
      descrp += ", %s"%additionnalreqs
    if cut:
      descrp += ", cut at stdhep level"
    if prod_name:
      descrp += ", %s"%prod_name
  
    pwh.setDescription(descrp)
    
    res = pwh.createProduction()
    if not res['OK']:
        print res['Message']
        
    pwh.addMetadataToFinalFiles({"BeamParticle1":proddict['pname1'], "BeamParticle2":proddict['pname2'],
                                 "EPA_B1":proddict['epa_b1'], "EPA_B2":proddict['epa_b2']})
    
    res = pwh.finalizeProd()
    if not res['OK']:
        print res['Message']
        exit(1)
    pwh.setNbOfTasks(1)
    ##The production is created, one can now take care of the second step:
    #For that we will use the metadata of the previous production as input
    meta = pwh.getMetadata()
    
  if activesplitstdhep and meta:
    pstdhepsplit =  ProductionJob()
    pstdhepsplit.setLogLevel("verbose")
    pstdhepsplit.setProdType('Split')
    res = pstdhepsplit.setInputDataQuery(meta)
    if not res['OK']:
        print res['Message']
        exit(1)
    pstdhepsplit.setOutputSE("CERN-SRM")
    wname = process+"_"+str(energy)+"_split"
    wname += prod_name
    pstdhepsplit.setWorkflowName(wname)
    pstdhepsplit.setProdGroup(analysis+"_"+str(energy))
    
    #Add the application
    res = pstdhepsplit.append(stdhepsplit)
    if not res['OK']:
        print res['Message']
        exit(1)
    pstdhepsplit.addFinalization(True,True,True,True)
    descrp = "Splitting stdhep files"

    if prod_name:
      descrp += ", %s"%prod_name

    pstdhepsplit.setDescription(descrp)  
    
    res = pstdhepsplit.createProduction()
    if not res['OK']:
        print res['Message']
        
    pstdhepsplit.addMetadataToFinalFiles({"BeamParticle1":proddict['pname1'], "BeamParticle2":proddict['pname2'],
                                 "EPA_B1":proddict['epa_b1'], "EPA_B2":proddict['epa_b2']})

    res = pstdhepsplit.finalizeProd()
    if not res['OK']:
        print res['Message']
        exit(1)
    #As before: get the metadata for this production to input into the next
    meta = pstdhepsplit.getMetadata()
    
  if ild_sim and meta:
    ####################
    ##Define the second production (simulation). Notice the setInputDataQuery call
    pmo = ProductionJob()
    pmo.setLogLevel("verbose")
    pmo.setProdType('MCSimulation')
    res = pmo.setInputDataQuery(meta)
    if not res['OK']:
        print res['Message']
        exit(1)
    pmo.setOutputSE("CERN-SRM")
    wname = process+"_"+str(energy)+"_ild_sim"
    wname += prod_name
    pmo.setWorkflowName(wname)
    pmo.setProdGroup(analysis+"_"+str(energy))
    #Add the application
    res = pmo.append(mo)
    if not res['OK']:
        print res['Message']
        exit(1)
    pmo.addFinalization(True,True,True,True)
    if energy >550.:
      descrp = "CLIC_ILD_CDR model"
    else:
      descrp = "CLIC_ILD_CDR_500 model"
    if prod_name:  
      descrp += ", %s"%prod_name   
    pmo.setDescription(descrp)
    res = pmo.createProduction()
    if not res['OK']:
        print res['Message']
        
    pmo.addMetadataToFinalFiles({"BeamParticle1":proddict['pname1'], "BeamParticle2":proddict['pname2'],
                                 "EPA_B1":proddict['epa_b1'], "EPA_B2":proddict['epa_b2']})
    
        
    res = pmo.finalizeProd()
    if not res['OK']:
        print res['Message']
        exit(1)
    #As before: get the metadata for this production to input into the next
    meta = pmo.getMetadata()
  
  if sid_sim and meta:
    ####################
    ##Define the second production (simulation). Notice the setInputDataQuery call
    psl = ProductionJob()
    psl.setLogLevel("verbose")
    psl.setProdType('MCSimulation')
    res = psl.setInputDataQuery(meta)
    if not res['OK']:
        print res['Message']
        exit(1)
    psl.setOutputSE("CERN-SRM")
    wname = process+"_"+str(energy)+"_sid_sim"
    wname += prod_name
    psl.setWorkflowName(wname)
    psl.setProdGroup(analysis+"_"+str(energy))
    #Add the application
    res = psl.append(slic)
    if not res['OK']:
        print res['Message']
        exit(1)
    psl.addFinalization(True,True,True,True)
    descrp = "CLIC_SID_CDR model"
    if prod_name:  
      descrp += ", %s"%prod_name
    psl.setDescription(descrp)
  
    res = psl.createProduction()
    if not res['OK']:
        print res['Message']
        
    psl.addMetadataToFinalFiles({"BeamParticle1":proddict['pname1'], "BeamParticle2":proddict['pname2'],
                                 "EPA_B1":proddict['epa_b1'], "EPA_B2":proddict['epa_b2']})    
            
    res = psl.finalizeProd()
    if not res['OK']:
        print res['Message']
        exit(1)
    #As before: get the metadata for this production to input into the next
    meta = psl.getMetadata()
  
  if activesplit and meta:
    #######################
    ## Split the input files.  
    psplit =  ProductionJob()
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
    wname += prod_name  
    psplit.setWorkflowName(wname)
    psplit.setProdGroup(analysis+"_"+str(energy))
    
    #Add the application
    res = psplit.append(split)
    if not res['OK']:
        print res['Message']
        exit(1)
    psplit.addFinalization(True,True,True,True)
    descrp = "Splitting slcio files"
    if prod_name:  
      descrp += ", %s"%prod_name
    psplit.setDescription(descrp)  
    
    res = psplit.createProduction()
    if not res['OK']:
        print res['Message']
    psplit.addMetadataToFinalFiles({"BeamParticle1":proddict['pname1'], "BeamParticle2":proddict['pname2'],
                                    "EPA_B1":proddict['epa_b1'], "EPA_B2":proddict['epa_b2']})
    
    res = psplit.finalizeProd()
    if not res['OK']:
        print res['Message']
        exit(1)
    #As before: get the metadata for this production to input into the next
    meta = psplit.getMetadata()
    
  if ild_rec and meta:
    #######################
    #Define the reconstruction prod    
    pma = ProductionJob()
    pma.setLogLevel("verbose")
    pma.setProdType('MCReconstruction')
    res = pma.setInputDataQuery(meta)
    if not res['OK']:
        print res['Message']
        exit(1)
    pma.setOutputSE("CERN-SRM")
    wname = process+"_"+str(energy)+"_ild_rec"
    wname += prod_name  
    pma.setWorkflowName(wname)
    pma.setProdGroup(analysis+"_"+str(energy))
    
    #Add the application
    res = pma.append(ma)
    if not res['OK']:
        print res['Message']
        exit(1)
    pma.addFinalization(True,True,True,True)
    if energy >550.:
      descrp = "CLIC_ILD_CDR, No overlay"
    else:
      descrp = "CLIC_ILD_CDR 500 gev, No overlay"
    if prod_name:  
      descrp += ", %s"%prod_name  
    pma.setDescription(descrp)
    
    res = pma.createProduction()
    if not res['OK']:
        print res['Message']
    pma.addMetadataToFinalFiles({"BeamParticle1":proddict['pname1'], "BeamParticle2":proddict['pname2'],
                                 "EPA_B1":proddict['epa_b1'], "EPA_B2":proddict['epa_b2']})
    
    res = pma.finalizeProd()
    if not res['OK']:
        print res['Message']
        exit(1)
  
  if sid_rec and meta:
    #######################
    #Define the reconstruction prod      
    psidrec = ProductionJob()
    psidrec.setLogLevel("verbose")
    psidrec.setProdType('MCReconstruction')
    psidrec.setBannedSites(['LCG.Bristol.uk','LCG.RAL-LCG2.uk'])
    res = psidrec.setInputDataQuery(meta)
    if not res['OK']:
        print res['Message']
        exit(1)
    psidrec.setOutputSE("CERN-SRM")
    wname = process+"_"+str(energy)+"_sid_rec"
    wname += prod_name  
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
    if prod_name:  
      descrp += ", %s"%prod_name  
    psidrec.setDescription(descrp)
    
    res = psidrec.createProduction()
    if not res['OK']:
        print res['Message']
        
    psidrec.addMetadataToFinalFiles({"BeamParticle1":proddict['pname1'], "BeamParticle2":proddict['pname2'],
                                 "EPA_B1":proddict['epa_b1'], "EPA_B2":proddict['epa_b2']})
    
    res = psidrec.finalizeProd()
    if not res['OK']:
        print res['Message']
        exit(1)
  
  if ild_rec_ov and meta:
    #######################
    #Define the reconstruction prod    
    pmao = ProductionJob()
    pmao.setLogLevel("verbose")
    pmao.setProdType('MCReconstruction_Overlay')
    res = pmao.setInputDataQuery(meta)
    if not res['OK']:
        print res['Message']
        exit(1)
    pmao.setOutputSE("CERN-SRM")
    wname = process+"_"+str(energy)+"_ild_rec_overlay"
    wname += prod_name  
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
    if energy >550.:
      descrp = "CLIC_ILD_CDR, Overlay"
    else:
      descrp = "CLIC_ILD_CDR_500, Overlay"
    if prod_name:  
      descrp += ", %s"%prod_name
    pmao.setDescription( descrp ) 
    res = pmao.createProduction()
    if not res['OK']:
        print res['Message']
        
    pmao.addMetadataToFinalFiles({"BeamParticle1":proddict['pname1'], "BeamParticle2":proddict['pname2'],
                                  "EPA_B1":proddict['epa_b1'], "EPA_B2":proddict['epa_b2']})
    
    res = pmao.finalizeProd()
    if not res['OK']:
        print res['Message']
        exit(1)
  
  if sid_rec_ov and meta:
    #######################
    #Define the reconstruction prod      
    psidreco = ProductionJob()
    psidreco.setLogLevel("verbose")
    psidreco.setProdType('MCReconstruction_Overlay')
    psidreco.setBannedSites(['LCG.Bristol.uk','LCG.RAL-LCG2.uk'])
    res = psidreco.setInputDataQuery(meta)
    if not res['OK']:
        print res['Message']
        exit(1)
    psidreco.setOutputSE("CERN-SRM")
    wname = process+"_"+str(energy)+"_sid_rec_overlay"
    wname += prod_name  
    psidreco.setWorkflowName(wname)
    psidreco.setProdGroup(analysis+"_"+str(energy))
    res = psidreco.append(overlay_sid)
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
    if prod_name:  
      descrp += ", %s"%prod_name
    psidreco.setDescription(descrp)
    
    res = psidreco.createProduction()
    if not res['OK']:
        print res['Message']
    psidreco.addMetadataToFinalFiles({"BeamParticle1":proddict['pname1'], "BeamParticle2":proddict['pname2'],
                                 "EPA_Beam1":proddict['epa_b1'], "EPA_Beam2":proddict['epa_b2']}) 
        
    res = psidreco.finalizeProd()
    if not res['OK']:
        print res['Message']
        exit(1)
      
  ##In principle nothing else is needed.
