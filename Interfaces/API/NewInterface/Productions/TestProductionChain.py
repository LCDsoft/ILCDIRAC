'''
Created on Feb 8, 2012

@author: Stephane Poss
'''
__RCSID__ = "$Id$"
#pylint: disable=C0103
#pylint: skip-file
from DIRAC.Core.Base import Script

from ILCDIRAC.Interfaces.API.NewInterface.ProductionJob import ProductionJob
from ILCDIRAC.Interfaces.API.NewInterface.Applications import Whizard, Mokka, Marlin, OverlayInput, StdhepCut, StdhepCutJava
from ILCDIRAC.Interfaces.API.NewInterface.Applications import SLIC, LCSIM, SLICPandora, SLCIOSplit, StdHepSplit
from ILCDIRAC.Interfaces.API.DiracILC import DiracILC


from DIRAC import S_OK, S_ERROR

dirac = DiracILC()

###As it's a full chain, we start at generation
##so we need to define the process and the energy
## The rest will be set later. We could also set the process
##and the energy directly in the whizard def, but for clarity
## it's better to do it before, that way we know the very
##essential

class Params(object):
  """command line parameters to create test productions"""

  def __init__(self):
    self.knownModels = ['CLIC_ILD_CDR', 'ILD_o1_v05']

    self.energy = 250
    self.detectorModel = "CLIC_ILD_CDR"
    self.outputSE = "CERN-DIP-4"
    self.ildConfig = "CLICSteeringFilesV22"
    self.onlyDestination = []
    self.logLevel = "INFO"
    self.numberOfEvents = 10
    self.dryrun = False

  def setDetectorModel(self, model):
    if model not in self.knownModels:
      return S_ERROR("Unknown detectormodel, use one of these %s " % (",".join(self.knownModels) ) )
    self.detectorModel = model
    return S_OK()

  def setOutputSE(self, se):
    self.outputSE = se
    return S_OK()

  def setILDConfig(self, config):
    self.ildConfig = config
    return S_OK()

  def setOnlyDestination(self, dest):
    if isinstance(dest, list):
      self.onlyDestination = dest
    elif ',' in dest:
      self.onlyDestination = dest.split(',')
    elif ' ' in dest:
      self.onlyDestination = dest.split(' ')
    return S_OK()

  def setEnergy(self, energy):
    self.energy = energy
    return S_OK()

  def setDryRun(self, _dummy):
    self.dryrun = True
    return S_OK()

  def setNumberOfEvents(self, numberOfEvents):
    try:
      numberOfEvents = int(numberOfEvents)
    except ValueError as e:
      return S_ERROR("ERROR: Number of events needs to be an integer: %s" % str(e) )
    self.numberOfEvents = numberOfEvents
    return S_OK()

  def registerSwitches(self):
    Script.registerSwitch( "O:", "OutputSE=",       "Output SE",                        self.setOutputSE )
    Script.registerSwitch( "D:", "Destination=",    "Only send jobs to these CEs",      self.setOnlyDestination )
    Script.registerSwitch( "M:", "DetectorModel=",  "DetectorModel: [%s]" % ", ".join(self.knownModels), self.setDetectorModel )
    Script.registerSwitch( "C:", "Configuration=",  "Configuration with Steeringfiles", self.setILDConfig )
    Script.registerSwitch( "E:", "Energy=",         "Energy",                           self.setEnergy )
    Script.registerSwitch( "N:", "NumberOfEvents=", "Number of Events",                 self.setNumberOfEvents )
    Script.registerSwitch( "",    "dry-run",        "Just do a dry run",                self.setDryRun )
    Script.setUsageMessage("%s [opts] <extraName>" % Script.scriptName)


def getdicts(process):
  """ Create the proper structures to build all the prodcutions for the samples with ee_, ea_ aa_.
  """
  plist = []
  if process.count("ee_"):
    plist.append({'process':process,'pname1':'e1', 'pname2':'E1', "epa_b1":'F', "epa_b2":'F', "isr_b1":'T', "isr_b2":'T'})
  elif process.count("ea_"):
    plist.append({'process':process,'pname1':'e1', 'pname2':'E1', "epa_b1":'F', "epa_b2":'T', "isr_b1":'T', "isr_b2":'F'})
    plist.append({'process':process,'pname1':'e1', 'pname2':'A', "epa_b1":'F', "epa_b2":'F', "isr_b1":'T', "isr_b2":'F'})
    plist.append({'process':process.replace("ea_","ae_"),'pname1':'e1', 'pname2':'E1', "epa_b1":'T', "epa_b2":'F', "isr_b1":'F', "isr_b2":'T'})
    plist.append({'process':process.replace("ea_","ae_"),'pname1':'A', 'pname2':'E1', "epa_b1":'F', "epa_b2":'F', "isr_b1":'F', "isr_b2":'T'})
  elif process.count("aa_"):
    plist.append({'process':process,'pname1':'e1', 'pname2':'E1', "epa_b1":'T', "epa_b2":'T', "isr_b1":'F', "isr_b2":'F'})
    plist.append({'process':process,'pname1':'e1', 'pname2':'A', "epa_b1":'T', "epa_b2":'F', "isr_b1":'F', "isr_b2":'F'})
    plist.append({'process':process,'pname1':'A', 'pname2':'E1', "epa_b1":'F', "epa_b2":'T', "isr_b1":'F', "isr_b2":'F'})
    plist.append({'process':process,'pname1':'A', 'pname2':'A', "epa_b1":'F', "epa_b2":'F', "isr_b1":'F', "isr_b2":'F'})
  else:
    plist.append({'process':process,'pname1':'e1', 'pname2':'E1', "epa_b1":'F', "epa_b2":'F', "isr_b1":'T', "isr_b2":'T'})
  return plist




PARAMS = Params()
PARAMS.registerSwitches()
Script.parseCommandLine()
extraargs= Script.getPositionalArgs()
if len(extraargs) == 0:
  print "ERROR: ExtraName not defined"
  Script.showHelp()
  raise RuntimeError("1")
additional_name = extraargs[0]


energy = float(PARAMS.energy)


## tripleH, Hrecoil, stau, gauginos, Hmass, tt, Htautau, Hmumu, Hee, Hbbccmumu, squarks, LCFITraining, Hgammagamma
## HZgamma Hinclusive ZZfusion, Any, ttH, bb_cc_gg
analysis = 'several'
process = 'hzqq'
#additional_name = '_neu1_356'
globname = ""
meta_energy = str(int(energy))

#For meta def
meta = {}
meta['ProdID']=1
meta['EvtType']=process
meta['Energy'] = meta_energy

#ILDCONFIG = "CLICSteeringFilesV22"
ILDCONFIG = "v01-16-p10_250"
SOFTWAREVERSION = "ILCSoft-01-17-06"

ILDDetectorModels = ['ILD_o1_v05']
CLICDetectorModels = ['CLIC_ILD_CDR']

detectormodel=PARAMS.detectorModel

#Here get the prod list: initial particles combinasions
prodlist = getdicts(process)

beamrecoil = True

#Do generation
gen = True

#For cuts
cut = False
if cut:
  additional_name += "_cut"

javacut = False
cutfile = 'cuts_qq_nunu_1400.txt'
seleff = 0.52
n_keep = 500

#DoSplit
activesplitstdhep = False
if activesplitstdhep:
  additional_name += "_gensplit"
nbevtsperfilestdhep = 10

#Do Sim
ild_sim = True
sid_sim = False

#DoSplit
activesplit = False
if activesplit:
  additional_name += "_simsplit"

nbevtsperfile = 10

#Do Reco
ild_rec = False
sid_rec = False
#Do Reco with Overlay
ild_rec_ov = True
sid_rec_ov = False

n_events = PARAMS.numberOfEvents
#rodOutputSE = "PNNL3-SRM"
prodOutputSE = PARAMS.outputSE
onlyDestination = PARAMS.onlyDestination
logLevel = PARAMS.logLevel

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
mh = 126.
mb = 2.9
mc = 0
mmu = 0.10566
mtau = 1.77
#alphas = 0.000001
##Use gridfiles ?
gridfiles = False
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
  elif energy == 420.:
    spectrum = 13
  elif energy == 250.:
    spectrum = 20
  elif energy == 350.:
    spectrum = 20
  else:
    print "No spectrum defined, cannot proceed"
    raise RuntimeError("1")


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
  if model != 'sm':
    pdict['simulation_input']['pythia_parameters'] = "PMAS(25,1)=%s; PMAS(25,2)=0.3605E-02; MSTU(22)=20 ;PARJ(21)=0.40000;PARJ(41)=0.11000; PARJ(42)=0.52000; PARJ(81)=0.25000; PARJ(82)=1.90000; MSTJ(11)=3; PARJ(54)=-0.03100; PARJ(55)=-0.00200;PARJ(1)=0.08500; PARJ(3)=0.45000; PARJ(4)=0.02500; PARJ(2)=0.31000; PARJ(11)=0.60000; PARJ(12)=0.40000; PARJ(13)=0.72000;PARJ(14)=0.43000; PARJ(15)=0.08000; PARJ(16)=0.08000; PARJ(17)=0.17000; MSTP(3)=1;IMSS(1)=11; IMSS(21)=71; IMSS(22)=71;%s"%(mh,additionnalreqs)
  else:
    pdict['simulation_input']['pythia_parameters'] = "PMAS(25,1)=%s; PMAS(25,2)=0.3605E-02; MSTU(22)=20 ; MSTJ(28)=2 ;PARJ(21)=0.40000;PARJ(41)=0.11000; PARJ(42)=0.52000; PARJ(81)=0.25000; PARJ(82)=1.90000; MSTJ(11)=3; PARJ(54)=-0.03100; PARJ(55)=-0.00200;PARJ(1)=0.08500; PARJ(3)=0.45000; PARJ(4)=0.02500; PARJ(2)=0.31000; PARJ(11)=0.60000; PARJ(12)=0.40000; PARJ(13)=0.72000;PARJ(14)=0.43000; PARJ(15)=0.08000; PARJ(16)=0.08000; PARJ(17)=0.17000; MSTP(3)=1;%s"%(mh,additionnalreqs)
    pdict['parameter_input'] = {}
  #  pdict['parameter_input']['mmu']=mmu
  #  pdict['parameter_input']['mtau']=mtau
  #  pdict['parameter_input']['mb']=mb
  #  pdict['parameter_input']['mc']=mc
    pdict['parameter_input']['mH']=mh
  #  pdict['parameter_input']['alphas']=alphas
  pdict['beam_input_1'] = {}
  pdict['beam_input_1']['particle_name']=proddict['pname1']
  pdict['beam_input_1']['polarization'] = "0.0 0.0"
  pdict['beam_input_1']['USER_spectrum_on'] = 'T'
  pdict['beam_input_1']['USER_spectrum_mode'] = spectrum
  pdict['beam_input_1']['ISR_on'] = proddict['isr_b1']
  pdict['beam_input_1']['EPA_on'] = proddict['epa_b1']

  pdict['beam_input_2'] = {}
  pdict['beam_input_2']['particle_name']=proddict['pname2']
  pdict['beam_input_2']['polarization'] = "0.0 0.0"
  pdict['beam_input_2']['USER_spectrum_on'] = 'T'
  pdict['beam_input_2']['ISR_on'] = proddict['isr_b2']
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
    raise RuntimeError("1")
  stdhepc.setSteeringFile(cutfile)
  stdhepc.setMaxNbEvts(n_keep)
  stdhepc.setSelectionEfficiency(seleff)

  ##Split
  stdhepsplit = StdHepSplit()
  stdhepsplit.setVersion("V2")
  stdhepsplit.setNumberOfEventsPerFile(nbevtsperfilestdhep)

  ##Simulation ILD
  mo = Mokka()
  mo.setVersion(SOFTWAREVERSION)
  #mo.setNbEvts(10)
  if energy in [500., 420., 375., 350., 250.]:
    mo.setSteeringFile("clic_ild_cdr500.steer")
  elif energy in [3000., 1400.]:
    mo.setSteeringFile("clic_ild_cdr.steer")
  else:
    print 'Detector Model for Mokka undefined for this energy'
  if detectormodel=='ild_00':
    mo.setSteeringFile("ild_00.steer")
  if detectormodel=='ILD_o1_v05':
    mo.setSteeringFile("bbudsc_3evt.steer")


  ##Simulation SID
  slic = SLIC()
  slic.setVersion('v2r9p8')
  slic.setSteeringFile('defaultClicCrossingAngle.mac')
  slic.setDetectorModel('clic_sid_cdr')


  ##Split
  split = SLCIOSplit()
  split.setNumberOfEventsPerFile(nbevtsperfile)


  overlay = OverlayInput()
  if detectormodel in CLICDetectorModels:
    overlay.setMachine("clic_cdr")
    overlay.setEnergy(energy)
    overlay.setBkgEvtType("gghad")

    if energy == 500.:
      overlay.setBXOverlay(300)
      overlay.setGGToHadInt(0.3)##When running at 500geV
      overlay.setDetectorModel("CLIC_ILD_CDR500")
    elif energy == 420.:
      overlay.setBXOverlay(300)
      overlay.setGGToHadInt(0.17)##When running at 420geV
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
      print "Overlay CLIC_ILD: No overlay parameters defined for this energy"

  if detectormodel in (ILDDetectorModels):
    overlay.setMachine("ilc_dbd")
    overlay.setBackgroundType("aa_lowpt")
    overlay.setBXOverlay(1)
    overlay.setDetectorModel(detectormodel)
    if energy == 250:
      overlay.setGGToHadInt(0.3)
    
  ##Reconstruction ILD with overlay
  mao = Marlin()
  mao.setDebug()
  mao.setVersion(SOFTWAREVERSION)
  if ild_rec_ov:
    if energy==500.:
      mao.setSteeringFile("clic_ild_cdr500_steering_overlay.xml")
      mao.setGearFile('clic_ild_cdr500.gear')
    elif energy==420.:
      mao.setSteeringFile("clic_ild_cdr500_steering_overlay_420.0.xml")
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

  if detectormodel in ILDDetectorModels:
    mao.setSteeringFile("bbudsc_3evt_stdreco.xml")
    mao.setGearFile("GearOutput.xml")

  ##Reconstruction w/o overlay
  ma = Marlin()
  ma.setDebug()
  ma.setVersion(SOFTWAREVERSION)
  if ild_rec:
    if energy in [500.,420.,350.,250.]:
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
    pwh.setDryRun(PARAMS.dryrun)
    pwh.setLogLevel(logLevel)
    pwh.setOutputSE(prodOutputSE)
    if onlyDestination:
      pwh.setDestination(onlyDestination)
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
      raise RuntimeError("1")

    if cut:
      res = pwh.append(stdhepc)
      if not res['OK']:
        print res['Message']
        raise RuntimeError("1")

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
      raise RuntimeError("1")
    pwh.setNbOfTasks(1)
    ##The production is created, one can now take care of the second step:
    #For that we will use the metadata of the previous production as input
    meta = pwh.getMetadata()

  if activesplitstdhep and meta:
    pstdhepsplit = ProductionJob()
    pstdhepsplit.setDryRun(PARAMS.dryrun)
    pstdhepsplit.setLogLevel(logLevel)
    pstdhepsplit.setProdType('Split')
    if onlyDestination:
      pstdhepsplit.setDestination(onlyDestination)
    res = pstdhepsplit.setInputDataQuery(meta)
    if not res['OK']:
      print res['Message']
      raise RuntimeError("1")
    pstdhepsplit.setOutputSE(prodOutputSE)
    wname = process+"_"+str(energy)+"_split"
    wname += prod_name
    pstdhepsplit.setWorkflowName(wname)
    pstdhepsplit.setProdGroup(analysis+"_"+str(energy))

    #Add the application
    res = pstdhepsplit.append(stdhepsplit)
    if not res['OK']:
      print res['Message']
      raise RuntimeError("1")
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
      raise RuntimeError("1")
    #As before: get the metadata for this production to input into the next
    meta = pstdhepsplit.getMetadata()

  if ild_sim and meta:
    ####################
    ##Define the second production (simulation). Notice the setInputDataQuery call
    pmo = ProductionJob()
    pmo.setDryRun(PARAMS.dryrun)
    pmo.setLogLevel(logLevel)
    if onlyDestination:
      pmo.setDestination(onlyDestination)
    pmo.setProdType('MCSimulation')
    pmo.setConfig(ILDCONFIG)
    res = pmo.setInputDataQuery(meta)
    if not res['OK']:
      print res['Message']
      raise RuntimeError("1")
    pmo.setOutputSE(prodOutputSE)
    wname = process+"_"+str(energy)+"_ild_sim"
    wname += prod_name
    pmo.setWorkflowName(wname)
    pmo.setProdGroup(analysis+"_"+str(energy))
    #Add the application
    res = pmo.append(mo)
    if not res['OK']:
      print res['Message']
      raise RuntimeError("1")
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
      raise RuntimeError("1")
    #As before: get the metadata for this production to input into the next
    meta = pmo.getMetadata()

  if sid_sim and meta:
    ####################
    ##Define the second production (simulation). Notice the setInputDataQuery call
    psl = ProductionJob()
    psl.setDryRun(PARAMS.dryrun)
    psl.setLogLevel(logLevel)
    psl.setProdType('MCSimulation')
    if onlyDestination:
      psl.setDestination(onlyDestination)
    res = psl.setInputDataQuery(meta)
    if not res['OK']:
      print res['Message']
      raise RuntimeError("1")
    psl.setOutputSE(prodOutputSE)
    wname = process+"_"+str(energy)+"_sid_sim"
    wname += prod_name
    psl.setWorkflowName(wname)
    psl.setProdGroup(analysis+"_"+str(energy))
    #Add the application
    res = psl.append(slic)
    if not res['OK']:
      print res['Message']
      raise RuntimeError("1")
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
      raise RuntimeError("1")
    #As before: get the metadata for this production to input into the next
    meta = psl.getMetadata()

  if activesplit and meta:
    #######################
    ## Split the input files.
    psplit = ProductionJob()
    psplit.setDryRun(PARAMS.dryrun)
    psplit.setCPUTime(30000)
    if onlyDestination:
      psplit.setDestination(onlyDestination)
    psplit.setLogLevel(logLevel)
    psplit.setProdType('Split')
    psplit.setDestination("LCG.CERN.ch")
    res = psplit.setInputDataQuery(meta)
    if not res['OK']:
      print res['Message']
      raise RuntimeError("1")
    psplit.setOutputSE(prodOutputSE)
    wname = process+"_"+str(energy)+"_split"
    wname += prod_name
    psplit.setWorkflowName(wname)
    psplit.setProdGroup(analysis+"_"+str(energy))

    #Add the application
    res = psplit.append(split)
    if not res['OK']:
      print res['Message']
      raise RuntimeError("1")
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
      raise RuntimeError("1")
    #As before: get the metadata for this production to input into the next
    meta = psplit.getMetadata()

  if ild_rec and meta:
    #######################
    #Define the reconstruction prod
    pma = ProductionJob()
    pma.setDryRun(PARAMS.dryrun)
    pma.setLogLevel(logLevel)
    pma.setProdType('MCReconstruction')
    pma.setConfig(ILDCONFIG)
    if onlyDestination:
      pma.setDestination(onlyDestination)
    res = pma.setInputDataQuery(meta)
    if not res['OK']:
      print res['Message']
      raise RuntimeError("1")
    pma.setOutputSE(prodOutputSE)
    wname = process+"_"+str(energy)+"_ild_rec"
    wname += prod_name
    pma.setWorkflowName(wname)
    pma.setProdGroup(analysis+"_"+str(energy))

    #Add the application
    res = pma.append(ma)
    if not res['OK']:
      print res['Message']
      raise RuntimeError("1")
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
      raise RuntimeError("1")

  if sid_rec and meta:
    #######################
    #Define the reconstruction prod
    psidrec = ProductionJob()
    psidrec.setDryRun(PARAMS.dryrun)
    psidrec.setLogLevel(logLevel)
    psidrec.setProdType('MCReconstruction')
    psidrec.setBannedSites(['LCG.Bristol.uk','LCG.RAL-LCG2.uk'])
    if onlyDestination:
      psidrec.setDestination(onlyDestination)
    res = psidrec.setInputDataQuery(meta)
    if not res['OK']:
      print res['Message']
      raise RuntimeError("1")
    psidrec.setOutputSE(prodOutputSE)
    wname = process+"_"+str(energy)+"_sid_rec"
    wname += prod_name
    psidrec.setWorkflowName(wname)
    psidrec.setProdGroup(analysis+"_"+str(energy))
    res = psidrec.append(lcsim_prepandora)
    if not res['OK']:
      print res['Message']
      raise RuntimeError("1")
    res = psidrec.append(slicpandora)
    if not res['OK']:
      print res['Message']
      raise RuntimeError("1")
    res = psidrec.append(lcsim_postpandora)
    if not res['OK']:
      print res['Message']
      raise RuntimeError("1")
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
      raise RuntimeError("1")

  if ild_rec_ov and meta:
    #######################
    #Define the reconstruction prod
    pmao = ProductionJob()
    pmao.setDryRun(PARAMS.dryrun)
    pmao.setLogLevel(logLevel)
    pmao.setProdType('MCReconstruction_Overlay')
    if onlyDestination:
      pmao.setDestination(onlyDestination)
    res = pmao.setInputDataQuery(meta)
    if not res['OK']:
      print res['Message']
      raise RuntimeError("1")
    pmao.setOutputSE(prodOutputSE)
    wname = process+"_"+str(energy)+"_ild_rec_overlay"
    wname += prod_name
    pmao.setWorkflowName(wname)
    pmao.setProdGroup(analysis+"_"+str(energy))

    #Add the application
    res = pmao.append(overlay)
    if not res['OK']:
      print res['Message']
      raise RuntimeError("1")
    #Add the application
    res = pmao.append(mao)
    if not res['OK']:
      print res['Message']
      raise RuntimeError("1")
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
      raise RuntimeError("1")

  if sid_rec_ov and meta:
    #######################
    #Define the reconstruction prod
    psidreco = ProductionJob()
    psidreco.setDryRun(PARAMS.dryrun)
    psidreco.setLogLevel(logLevel)
    psidreco.setProdType('MCReconstruction_Overlay')
    psidreco.setBannedSites(['LCG.Bristol.uk','LCG.RAL-LCG2.uk'])
    if onlyDestination:
      psidreco.setDestination(onlyDestination)
    res = psidreco.setInputDataQuery(meta)
    if not res['OK']:
      print res['Message']
      raise RuntimeError("1")
    psidreco.setOutputSE(prodOutputSE)
    wname = process+"_"+str(energy)+"_sid_rec_overlay"
    wname += prod_name
    psidreco.setWorkflowName(wname)
    psidreco.setProdGroup(analysis+"_"+str(energy))
    res = psidreco.append(overlay_sid)
    if not res['OK']:
      print res['Message']
      raise RuntimeError("1")
    res = psidreco.append(lcsim_prepandora_ov)
    if not res['OK']:
      print res['Message']
      raise RuntimeError("1")
    res = psidreco.append(slicpandora_ov)
    if not res['OK']:
      print res['Message']
      raise RuntimeError("1")
    res = psidreco.append(lcsim_postpandora_ov)
    if not res['OK']:
      print res['Message']
      raise RuntimeError("1")
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
      raise RuntimeError("1")

  ##In principle nothing else is needed.
