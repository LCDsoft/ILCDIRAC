#!/bin/env python
'''
Run many different applications as a test. Creates a temp directory and runs in there. 
Stops at any error.

@since: Nov 8, 2013

@author: sposs
'''
__RCSID__ = "$Id$" 

from DIRAC.Core.Base import Script
from DIRAC import S_OK, S_ERROR, gLogger, exit as dexit
import os, glob, shutil, tempfile

def cleaunp():
  """
  Remove files after run
  """
  all_files = glob.glob("./*")
  for of in all_files:
    if os.path.isdir(of):
      shutil.rmtree(of)
    else:
      os.unlink(of)

def getJob():
  """ Define a generic job, it should be always the same
  """
  from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob
  myjob = UserJob()
  myjob.setName("Testing")
  myjob.setJobGroup("Tests")
  myjob.setCPUTime(30000)
  myjob.dontPromptMe()
  myjob.setLogLevel("VERBOSE")
  myjob.setPlatform("x86_64-slc5-gcc43-opt")
  myjob.setOutputSandbox(["*.log","*.xml"])
  return myjob

def getWhizardModel(dirac, nbevts, energy, model):
  """ Create a default whizard
  """
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import Whizard
  proddict = "e2e2_o"
  if model != "sm":
    proddict = "se2se2_r"
  whiz = Whizard(processlist = dirac.getProcessList())
  whiz.setModel(model)
  pdict = {}
  pdict['process_input'] = {}
  pdict['process_input']['process_id'] = proddict
  pdict['process_input']['sqrts'] = energy
  if model != 'sm':
    pdict['process_input']['input_file'] = "LesHouches.msugra_1"
    pdict['process_input']['input_slha_format'] = 'T'

  pdict['process_input']['beam_recoil'] = 'T'

  pdict['integration_input'] = {}
  pdict['integration_input']['calls'] = '1  50000 10  5000  1  15000'
  pdict['simulation_input'] = {}
  pdict['simulation_input']['normalize_weight'] = 'F'
  pdict['simulation_input']['n_events'] = nbevts
  pdict['simulation_input']['keep_initials'] = 'T'
  pdict['simulation_input']['events_per_file'] = 500000
  if model != 'sm':
    pdict['simulation_input']['pythia_parameters'] = "PMAS(25,1)=125; PMAS(25,2)=0.3605E-02; MSTU(22)=20 ;PARJ(21)=0.40000;PARJ(41)=0.11000; PARJ(42)=0.52000; PARJ(81)=0.25000; PARJ(82)=1.90000; MSTJ(11)=3; PARJ(54)=-0.03100; PARJ(55)=-0.00200;PARJ(1)=0.08500; PARJ(3)=0.45000; PARJ(4)=0.02500; PARJ(2)=0.31000; PARJ(11)=0.60000; PARJ(12)=0.40000; PARJ(13)=0.72000;PARJ(14)=0.43000; PARJ(15)=0.08000; PARJ(16)=0.08000; PARJ(17)=0.17000; MSTP(3)=1;IMSS(1)=11; IMSS(21)=71; IMSS(22)=71"
  else:  
    pdict['simulation_input']['pythia_parameters'] = "PMAS(25,1)=125; PMAS(25,2)=0.3605E-02; MSTU(22)=20 ; MSTJ(28)=2 ;PARJ(21)=0.40000;PARJ(41)=0.11000; PARJ(42)=0.52000; PARJ(81)=0.25000; PARJ(82)=1.90000; MSTJ(11)=3; PARJ(54)=-0.03100; PARJ(55)=-0.00200;PARJ(1)=0.08500; PARJ(3)=0.45000; PARJ(4)=0.02500; PARJ(2)=0.31000; PARJ(11)=0.60000; PARJ(12)=0.40000; PARJ(13)=0.72000;PARJ(14)=0.43000; PARJ(15)=0.08000; PARJ(16)=0.08000; PARJ(17)=0.17000; MSTP(3)=1"
    pdict['parameter_input'] = {}
    #  pdict['parameter_input']['mmu']=mmu
    #  pdict['parameter_input']['mtau']=mtau
    #  pdict['parameter_input']['mb']=mb
    #  pdict['parameter_input']['mc']=mc
    pdict['parameter_input']['mH'] = 125
  pdict['beam_input_1'] = {}
  pdict['beam_input_1']['particle_name'] = "e1"
  pdict['beam_input_1']['polarization'] = "0.0 0.0"
  pdict['beam_input_1']['USER_spectrum_on'] = 'T'
  if energy == 1400:
    pdict['beam_input_1']['USER_spectrum_mode'] = 19
  else:
    pdict['beam_input_1']['USER_spectrum_mode'] = 11
  pdict['beam_input_1']['ISR_on'] = 'T'
  pdict['beam_input_1']['EPA_on'] = "F"

  pdict['beam_input_2'] = {}
  pdict['beam_input_2']['particle_name'] = "E1"
  pdict['beam_input_2']['polarization'] = "0.0 0.0"
  pdict['beam_input_2']['USER_spectrum_on'] = 'T'
  pdict['beam_input_2']['ISR_on'] = 'T'
  if energy == 1400:
    pdict['beam_input_2']['USER_spectrum_mode'] = -19
  else:
    pdict['beam_input_2']['USER_spectrum_mode'] = -11  
  pdict['beam_input_2']['EPA_on'] = 'F'
  
  
  whiz.setFullParameterDict(pdict)
  whiz.setOutputFile("testgen.stdhep")
  return whiz

def getWhizard(dirac, nbevts):
  """ Get some defualt SM whizard
  """
  return getWhizardModel(dirac, nbevts, 1400, "sm")

def getWhizardSUSY(dirac, nbevts):
  """ Get a susy whizard
  """
  return getWhizardModel(dirac, nbevts, 3000, "slsqhh")

def getStdhepcut(generated):
  """ Get some cuts in
  """
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import StdhepCutJava
  stdhepcut = StdhepCutJava()
  stdhepcut.setVersion('1.0')
  stdhepcut.setSelectionEfficiency(1.)
  #stdhepcut.setSteeringFile("cuts_testing_1400.txt")
  stdhepcut.setInlineCuts("leptonInvMass_R 13 100 200")
  stdhepcut.setSteeringFileVersion("V18")
  stdhepcut.setMaxNbEvts(1)
  stdhepcut.setNumberOfEvents(generated)
  return stdhepcut

def getMokka():
  """ Define a mokka app
  """
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import Mokka
  mokka = Mokka()
  mokka.setVersion("Test")
  mokka.setSteeringFile("clic_ild_cdr.steer")
  mokka.setOutputFile("testsim.slcio")
  mokka.setSteeringFileVersion("V19")
  return mokka

def getSLIC():
  """ Get a SLIC instance
  """
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import SLIC
  slic = SLIC()
  slic.setVersion('v2r9p8')
  slic.setSteeringFile('defaultClicCrossingAngle.mac')
  slic.setDetectorModel('clic_sid_cdr')
  slic.setOutputFile('testsim.slcio')
  return slic

def getOverlay(nbevts):
  """ Create an overlay step
  """
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import OverlayInput
  overlay = OverlayInput()
  overlay.setMachine("clic_cdr")
  overlay.setEnergy(1400)
  overlay.setBkgEvtType("gghad")
  overlay.setBXOverlay(60)
  overlay.setGGToHadInt(1.3)
  overlay.setDetectorModel("CLIC_ILD_CDR")
  overlay.setNbSigEvtsPerJob(nbevts)
  return overlay

def getMarlin(withoverlay = False):
  """ Define a marlin step
  """
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import Marlin
  marlin = Marlin()
  marlin.setVersion("v0111Prod")
  if not withoverlay:
    marlin.setSteeringFile("clic_ild_cdr_steering.xml")
  else:
    marlin.setSteeringFile("clic_ild_cdr_steering_overlay_1400.0.xml")
  marlin.setGearFile('clic_ild_cdr.gear')
  marlin.setOutputDstFile("testmarlinDST.slcio")
  marlin.setOutputRecFile("testmarlinREC.slcio")
  return marlin

def getLCSIM(prepandora = True, withoverlay = False):
  """ Get some LCSIM
  """
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import LCSIM
  lcsim = LCSIM()
  lcsim.setVersion('CLIC_CDR')
  if prepandora:
    if not withoverlay:
      lcsim.setSteeringFile("clic_cdr_prePandora.lcsim")
    else:
      lcsim.setSteeringFile("clic_cdr_prePandoraOverlay_1400.0.lcsim")
    lcsim.setOutputFile("testlcsim.slcio")
  else:
    lcsim.setSteeringFile("clic_cdr_postPandoraOverlay.lcsim")
    #lcsim.setOutputFile("testlcsimfinal.slcio")
    lcsim.setOutputDstFile("testlcsimDST.slcio")
    lcsim.setOutputRecFile("testlcsimREC.slcio")
  lcsim.setTrackingStrategy("defaultStrategies_clic_sid_cdr.xml")  
    
  return lcsim

def getSLICPandora():
  """ Get some SLICPAndora app
  """
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import SLICPandora
  slicp = SLICPandora()
  slicp.setVersion('CLIC_CDR')
  slicp.setDetectorModel('clic_sid_cdr')
  slicp.setPandoraSettings("PandoraSettingsSlic.xml")
  slicp.setOutputFile('testpandora.slcio')
  return slicp

def getStdhepSplit():
  """ Get some stdhep split
  """
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import StdHepSplit
  stdhepsplit = StdHepSplit()
  stdhepsplit.setVersion("V2")
  stdhepsplit.setNumberOfEventsPerFile(5)
  stdhepsplit.setOutputFile("teststdhepsplit.stdhep")
  return stdhepsplit

def getLCIOSplit(events_per_file):
  """ Get a LCIO split
  """
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import SLCIOSplit
  lciosplit = SLCIOSplit()
  lciosplit.setNumberOfEventsPerFile(events_per_file)
  lciosplit.setOutputFile("testlciosplit.slcio")
  return lciosplit

def getLCIOConcat():
  """ Get a LCIO Concat
  """
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import SLCIOConcatenate
  lcioconcat = SLCIOConcatenate()
  lcioconcat.setOutputFile("testlcioconcat.slcio")
  return lcioconcat

class CLIParams( object ):
  """ CLI parameters
  """
  def __init__(self):
    """ c'tor
    """
    self.submitMode = "local"
    self.testWhizard = False
    self.testInputData = False
    self.testUtilities = False
    self.testMokka = False
    self.testMarlin = False
    self.testOverlay = False
    self.testSlic = False
    self.testLCSIM = False
    self.testSlicPandora = False
    self.testChain = False
    self.testall  = False
    
  def setSubmitMode(self, opt):
    """ Define the submit mode
    """
    if not opt in ["local",'WMS']:
      return S_ERROR("SubmitMode must be either 'local' or 'WMS'.")
    self.submitMode = opt
    return S_OK()
  
  def setTestWhizard(self, opt):
    """ Test whizard
    """
    self.testWhizard = True
    return S_OK()
  
  def setTestInputData(self, opt):
    """ Test the InputData resolution
    """
    self.testInputData = True
    return S_OK()
  
  def setTestUtilities(self, opt):
    """ Test the utilities
    """
    self.testUtilities = True
    return S_OK()
  
  def setTestMokka(self, opt):
    """ Test Mokka
    """
    self.testMokka = True
    return S_OK()
  
  def setTestMarlin(self, opt):
    """ Test Marlin
    """
    self.testMarlin = True
    return S_OK()
  
  def setTestSLIC(self, opt):
    """ Test SLIC
    """
    self.testSlic = True
    return S_OK()
  
  def setTestLCSIM(self, opt):
    """ Test LCSIM
    """
    self.testLCSIM = True
    return S_OK() 
  
  def setTestSlicPandora(self, opt):
    """ Test of SLICPanodra
    """
    self.testSlicPandora = True
    return S_OK()
  
  def setTestOverlay(self, opt):
    """ Test Overlay
    """
    self.testOverlay = True
    return S_OK()
  
  def setTestChain(self, opt):
    """ Test the chaining of apps
    """
    self.testChain = True
    return S_OK()
  
  def setTestAll(self, opt):
    """ As name suggests, test everything
    """
    self.testInputData = True
    self.testWhizard = True
    self.testUtilities = True
    self.testMokka = True
    self.testMarlin = True
    self.testOverlay = True
    self.testSlic = True
    self.testLCSIM = True
    self.testSlicPandora = True
    self.testChain = True
    self.testall = True
    return S_OK()
  
  def registerCLIParams(self):
    """ Register the switches
    """
    Script.registerSwitch("", "submitmode=", "Submission mode: local or WMS", self.setSubmitMode)
    Script.registerSwitch('', 'whizard', "Test Whizard", self.setTestWhizard)
    Script.registerSwitch("", "mokka", 'Test Mokka', self.setTestMokka)
    Script.registerSwitch("", "marlin", 'Test Marlin', self.setTestMarlin)
    Script.registerSwitch("", "slic", 'Test SLIC', self.setTestSLIC)
    Script.registerSwitch("", "lcsim", "Test LCSIM", self.setTestLCSIM)
    Script.registerSwitch("", "slicpandora", 'Test SLICPandora', self.setTestSlicPandora)
    Script.registerSwitch("", 'overlay', "Test the overlay", self.setTestOverlay)
    Script.registerSwitch("", 'inputdata', "Test the InputData resolution", self.setTestInputData)
    Script.registerSwitch("", "utilities", "Test the utilities: cut, split, concatenate", self.setTestUtilities)
    Script.registerSwitch("", 'chain', 'Test the chaining of applications', self.setTestChain)
    Script.registerSwitch("a", "all", "Test them ALL!", self.setTestAll)
    Script.setUsageMessage("%s --all --submitmode=local" % Script.scriptName)
    
  
if __name__ == '__main__':
  clip = CLIParams()
  clip.registerCLIParams()
  Script.parseCommandLine()
  
  if clip.testall:
    gLogger.notice("Running all the jobs possible")
  
  from DIRAC import gConfig
    
  if clip.submitMode == "local":
    gLogger.notice("I will run the tests locally.")
    localarea = gConfig.getValue("/LocalSite/LocalArea", "")
    if not localarea:
      gLogger.error("You need to have /LocalSite/LocalArea defined in your dirac.cfg")
      dexit(1)
  
    if not localarea.find("/tmp") == 0:
      gLogger.error("You have to have your /LocalSite/LocalArea set to /tmp/something as you'll get to install there")
      dexit(1)
  
  from ILCDIRAC.Interfaces.API.DiracILC import DiracILC, __RCSID__ as drcsid
  from ILCDIRAC.Interfaces.API.NewInterface.UserJob import __RCSID__ as jrcsid
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import __RCSID__ as apprcsid
  
  
  curdir = os.getcwd()
  if clip.submitMode == "local":
    gLogger.notice("To run locally, I will create a temp directory here.")
    tmpdir = tempfile.mkdtemp("", dir = "./")
    os.chdir(tmpdir)
  
  ilcd = DiracILC(True, 'tests.rep')
  if clip.submitMode == "local":
    gLogger.notice("")
    gLogger.notice("       DIRAC RCSID:", drcsid )
    gLogger.notice("         Job RCSID:", jrcsid )
    gLogger.notice("Applications RCSID:", apprcsid )
    gLogger.notice("")

  joblist = []

  if clip.testWhizard:
    ##### WhizardJob
    jobw = getJob()
    wh = getWhizard(ilcd, 2)
    res = jobw.append(wh)
    if not res['OK']:
      gLogger.error("Failed adding Whizard:", res['Message'])
      dexit(1)
    joblist.append(jobw)
    
    ##### WhizardJob
    jobwsusy = getJob()
    whsusy = getWhizardSUSY(ilcd, 2)
    res = jobwsusy.append(whsusy)
    if not res['OK']:
      gLogger.error("Failed adding Whizard:", res['Message'])
      dexit(1)
    joblist.append(jobwsusy)

  if clip.testMokka:
    #(Whizard + )Mokka
    jobmo = getJob()
    if clip.testChain:
      whmo = getWhizard(ilcd, 2)
      res = jobmo.append(whmo)
      if not res['OK']:
        gLogger.error("Failed adding Whizard:", res['Message'])
        dexit(1)
    elif clip.testInputData:
      jobmo.setInputData("/ilc/prod/clic/1.4tev/e2e2_o/gen/00002213/000/e2e2_o_gen_2213_25.stdhep")
    else:
      gLogger.error("Mokka does not know where to get its input from")
      dexit(1)
      
    mo = getMokka()
    if clip.testChain:
      mo.getInputFromApp(whmo)
    else:
      mo.setNumberOfEvents(2)
    res = jobmo.append(mo)
    if not res['OK']:
      gLogger.error("Failed adding Mokka:", res['Message'])
      dexit(1)
    joblist.append(jobmo)

  if clip.testSlic:
    #run (Whizard +)SLIC
    jobslic = getJob()
    if clip.testChain:
      whslic = getWhizard(ilcd, 2)
      res = jobslic.append(whslic)
      if not res["OK"]:
        gLogger.error("Failed adding Whizard:", res['Value'])
        dexit(1)
    elif clip.testInputData:
      jobslic.setInputData("/ilc/prod/clic/1.4tev/e2e2_o/gen/00002213/000/e2e2_o_gen_2213_25.stdhep")
    else:
      gLogger.error("SLIC does not know where to get its input from")
      dexit(1)
    myslic = getSLIC()
    if clip.testChain:
      myslic.getInputFromApp(whslic)
    else:
      myslic.setNumberOfEvents(2)
    res = jobslic.append(myslic)
    if not res['OK']:
      gLogger.error("Failed adding slic: ", res["Message"])
      dexit(1)
    joblist.append(jobslic)
    
  if clip.testMarlin:  
    #((Whizard + Mokka +)Overlay+) Marlin  
    jobma = getJob()
    if clip.testChain:
      if not clip.testInputData:
        whma = getWhizard(ilcd, 2)
        res = jobma.append(whma)
        if not res['OK']:
          gLogger.error("Failed adding Whizard:", res['Message'])
          dexit(1)
      else:
        jobma.setInputData("/ilc/prod/clic/1.4tev/e2e2_o/gen/00002213/000/e2e2_o_gen_2213_25.stdhep")    
      moma = getMokka()
      if clip.testChain and not clip.testInputData:
        moma.getInputFromApp(whma)
      else:
        moma.setNumberOfEvents(2)
      res = jobma.append(moma)
      if not res['OK']:
        gLogger.error("Failed adding Mokka:", res['Message'])
        dexit(1)
    elif clip.testInputData:
      jobma.setInputData("/ilc/prod/clic/1.4tev/e2e2_o/ILD/SIM/00002214/000/e2e2_o_sim_2214_26.slcio")
    else:
      gLogger.error("Marlin does not know where to get its input from")
      dexit(1)
    if clip.testOverlay:
      ov = getOverlay(2)
      res = jobma.append(ov)
      if not res["OK"]:
        gLogger.error("Failed adding Overlay:", res['Message'])
        dexit(1)
      overlayrun = True
    else:
      overlayrun = False
    ma = getMarlin(overlayrun)
    if clip.testChain:
      ma.getInputFromApp(moma)
    else:
      ma.setNumberOfEvents(2)
      
    res = jobma.append(ma)
    if not res['OK']:
      gLogger.error("Failed adding Marlin:", res['Message'])
      dexit(1)  
    joblist.append(jobma)
    
  if clip.testLCSIM:  
    #run ((whiz+SLIC+)+Overlay+)LCSIM
    joblcsim = getJob()
    if clip.testChain:
      if not clip.testInputData:
        whlcsim = getWhizard(ilcd, 2)
        res = joblcsim.append(whlcsim)
        if not res["OK"]:
          gLogger.error("Failed adding Whizard:", res['Value'])
          dexit(1)
      else:
        joblcsim.setInputData("/ilc/prod/clic/1.4tev/e2e2_o/gen/00002213/000/e2e2_o_gen_2213_25.stdhep")
      mysliclcsim = getSLIC()
      if not clip.testInputData:
        mysliclcsim.getInputFromApp(whlcsim)
      else:
        mysliclcsim.setNumberOfEvents(2)
      res = joblcsim.append(mysliclcsim)
      if not res['OK']:
        gLogger.error("Failed adding slic: ", res["Message"])
        dexit(1)
    elif clip.testInputData:
      joblcsim.setInputData("/ilc/prod/clic/1.4tev/ee_qqaa/SID/SIM/00002308/000/ee_qqaa_sim_2308_222.slcio")
    else:
      gLogger.error("LCSIM does not know where to get its input from")
      dexit(1)
    if clip.testOverlay:
      ovlcsim = getOverlay(2)
      res = joblcsim.append(ovlcsim)
      if not res["OK"]:
        gLogger.error("Failed adding Overlay:", res['Message'])
        dexit(1)
      overlayrun = True
    else:
      overlayrun = False
    mylcsim = getLCSIM(True, overlayrun)
    if clip.testChain:
      mylcsim.getInputFromApp(mysliclcsim)
    else:
      mylcsim.setNumberOfEvents(2)
    res = joblcsim.append(mylcsim)
    if not res['OK']:
      gLogger.error("Failed adding LCSIM: ", res["Message"])
      dexit(1)

    joblist.append(joblcsim)
  
  if clip.testSlicPandora:
    #run ((whiz+SLIC) + (Overlay +) LCSIM +) SLICPandora + LCSIM
    joblcsimov = getJob()
    if clip.testChain:
      if not clip.testInputData:
        whlcsimov = getWhizard(ilcd, 2)
        res = joblcsimov.append(whlcsimov)
        if not res["OK"]:
          gLogger.error("Failed adding Whizard:", res['Value'])
          dexit(1)
        mysliclcsimov = getSLIC()
        mysliclcsimov.getInputFromApp(whlcsimov)
        res = joblcsimov.append(mysliclcsimov)
        if not res['OK']:
          gLogger.error("Failed adding slic: ", res["Message"])
          dexit(1)
      else:
        joblcsimov.setInputData("/ilc/prod/clic/1.4tev/ee_qqaa/SID/SIM/00002308/000/ee_qqaa_sim_2308_222.slcio")
        
      if clip.testOverlay:
        ovslicp = getOverlay(2)
        res = joblcsimov.append(ovslicp)
        if not res["OK"]:
          gLogger.error("Failed adding Overlay:", res['Message'])
          dexit(1)
        overlayrun = True
      else:
        overlayrun = False

      mylcsimov = getLCSIM(True, overlayrun)
      if not clip.testInputData:
        mylcsimov.getInputFromApp(mysliclcsimov)
      else:
        mylcsimov.setNumberOfEvents(2)
      res = joblcsimov.append(mylcsimov)
      if not res['OK']:
        gLogger.error("Failed adding LCSIM: ", res["Message"])
        dexit(1)
    else:
      gLogger.error("SLICPandora does not know where to get its input from")
      dexit(1)
      
    myslicpov = getSLICPandora()
    if clip.testChain:
      myslicpov.getInputFromApp(mylcsimov)
    res = joblcsimov.append(myslicpov)
    if not res['OK']:
      gLogger.error("Failed adding SLICPandora: ", res["Message"])
      dexit(1)
    mylcsimovp = getLCSIM(True, False)
    mylcsimovp.getInputFromApp(myslicpov)
    res = joblcsimov.append(mylcsimovp)
    if not res['OK']:
      gLogger.error("Failed adding LCSIM: ", res["Message"])
      dexit(1)
    joblist.append(joblcsimov)

  if clip.testUtilities:
    ##### WhizardJob + split
    jobwsplit = getJob()
    whsplit = getWhizard(ilcd, 10)
    res = jobwsplit.append(whsplit)
    if not res['OK']:
      gLogger.error("Failed adding Whizard:", res['Message'])
      dexit(1)
    mystdsplit = getStdhepSplit()
    mystdsplit.getInputFromApp(whsplit)
    res = jobwsplit.append(mystdsplit)
    if not res['OK']:
      gLogger.error("Failed adding StdHepSplit:", res['Message'])
      dexit(1)
    joblist.append(jobwsplit)

    ##### WhizardJob + split
    jobwcut = getJob()
    whcut = getWhizard(ilcd, 100)
    res = jobwcut.append(whcut)
    if not res['OK']:
      gLogger.error("Failed adding Whizard:", res['Message'])
      dexit(1)
    mystdcut = getStdhepcut( 100 )
    mystdcut.getInputFromApp(whcut)
    res = jobwcut.append(mystdcut)
    if not res['OK']:
      gLogger.error("Failed adding StdHepCut:", res['Message'])
      dexit(1)
    joblist.append(jobwcut)

    #LCIO split
    joblciosplit = getJob()
    joblciosplit.setInputData("/ilc/prod/clic/1.4tev/e2e2_o/ILD/DST/00002215/000/e2e2_o_dst_2215_46.slcio")
    mylciosplit = getLCIOSplit(100)
    res = joblciosplit.append(mylciosplit)
    if not res['OK']:
      gLogger.error("Failed adding SLCIOSplit:", res['Message'])
      dexit(1)
    joblist.append(joblciosplit)
  
    #LCIO concat
    jobconcat = getJob()
    jobconcat.setInputData(["/ilc/prod/clic/1.4tev/e2e2_o/ILD/DST/00002215/000/e2e2_o_dst_2215_27.slcio",
                            "/ilc/prod/clic/1.4tev/e2e2_o/ILD/DST/00002215/000/e2e2_o_dst_2215_46.slcio"])
    myconcat = getLCIOConcat()
    res = jobconcat.append(myconcat)
    if not res['OK']:
      gLogger.error("Failed adding SLCIOConcatenate:", res['Message'])
      dexit(1)
    joblist.append(jobconcat)
    
  ##Now submit/run all  
  for finjob in joblist:
    res = finjob.submit(ilcd, mode = clip.submitMode)
    if not res["OK"]:
      gLogger.error("Failed job:", res['Message'])
      dexit(1)
    if clip.submitMode == "local":
      cleaunp()
  if clip.submitMode == 'local':
    gLogger.notice("All good")
  os.chdir(curdir)
  dexit(0)

