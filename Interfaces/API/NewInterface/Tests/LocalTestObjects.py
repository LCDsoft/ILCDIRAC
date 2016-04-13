"""module collecting functions needed to run tests on local machine"""

__RCSID__ ="$Id$"


from DIRAC.Core.Base import Script
import os, shutil, tempfile
from DIRAC import S_OK, S_ERROR, gLogger

def cleanup(tempdir):
  """
  Remove files after run
  """
  shutil.rmtree(tempdir)

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
    self.testRoot = False
    self.testall  = False
    self.nocleanup = False

  def setSubmitMode(self, opt):
    """ Define the submit mode
    """
    if not opt in ["local",'WMS']:
      return S_ERROR("SubmitMode must be either 'local' or 'WMS'.")
    self.submitMode = opt
    return S_OK()

  def setTestWhizard(self, dummy_opt):
    """ Test whizard
    """
    self.testWhizard = True
    return S_OK()

  def setTestInputData(self, dummy_opt):
    """ Test the InputData resolution
    """
    self.testInputData = True
    return S_OK()

  def setTestUtilities(self, dummy_opt):
    """ Test the utilities
    """
    self.testUtilities = True
    return S_OK()

  def setTestMokka(self, dummy_opt):
    """ Test Mokka
    """
    self.testMokka = True
    return S_OK()

  def setTestMarlin(self, dummy_opt):
    """ Test Marlin
    """
    self.testMarlin = True
    return S_OK()

  def setTestSLIC(self, dummy_opt):
    """ Test SLIC
    """
    self.testSlic = True
    return S_OK()

  def setTestLCSIM(self, dummy_opt):
    """ Test LCSIM
    """
    self.testLCSIM = True
    return S_OK()

  def setTestSlicPandora(self, dummy_opt):
    """ Test of SLICPanodra
    """
    self.testSlicPandora = True
    return S_OK()

  def setTestOverlay(self, dummy_opt):
    """ Test Overlay
    """
    self.testOverlay = True
    return S_OK()

  def setTestChain(self, dummy_opt):
    """ Test the chaining of apps
    """
    self.testChain = True
    return S_OK()

  def setTestRoot(self, dummy_opt):
    """ Test the chaining of apps
    """
    self.testRoot = True
    return S_OK()

  def setNoCleanup(self, dummy_opt):
    """ Test the chaining of apps
    """
    self.nocleanup = True
    return S_OK()

  def setTestAll(self, dummy_opt):
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

  def registerSwitches(self):
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
    Script.registerSwitch("", 'root', 'Test the root application', self.setTestRoot)
    Script.registerSwitch("", 'chain', 'Test the chaining of applications', self.setTestChain)
    Script.registerSwitch("", 'nocleanup', 'Do not clean the tmp directories', self.setNoCleanup)
    Script.registerSwitch("a", "all", "Test them ALL!", self.setTestAll)
    Script.setUsageMessage("%s --all --submitmode=local" % Script.scriptName)


class TestCreater(object):
  """contains all the versions and parameters to create all theses tests"""

  def __init__( self,
                clip,
                params
              ):
    self.clip = clip
    self.ildConfig = params.get( "ildConfig", None )
    self.alwaysOverlay = params.get( "alwaysOverlay", False )
    self.runOverlay = self.clip.testOverlay or self.alwaysOverlay
    self.mokkaVersion = params["mokkaVersion"]
    self.mokkaSteeringFile = params.get( "mokkaSteeringFile" )
    self.detectorModel = params.get( "detectorModel" )
    self.marlinVersion = params.get( "marlinVersion" )
    self.marlinSteeringFile = params.get( "marlinSteeringFile" )
    self.marlinInputdata = params.get ( "marlinInputdata" )
    self.gearFile = params.get( "gearFile" )
    self.lcsimVersion = params.get( "lcsimVersion" )
    self.steeringFileVersion = params.get( "steeringFileVersion", None )
    self.rootVersion = params["rootVersion"]
    self.energy = params.get("energy")
    self.backgroundType = params.get("backgroundType")
    self.machine = params.get("machine")

    self.gearFile           = params.get( "gearFile" )
    self.marlinSteeringFile = params.get( "marlinSteeringFile" )
    self.marlinVersion      = params.get( "marlinVersion" )

    self.lcsimPreSteeringFile  = params.get( "lcsimPreSteeringFile" )
    self.lcsimPostSteeringFile = params.get( "lcsimPostSteeringFile" )
    ### other things needed to run tests
    self.log = gLogger.getSubLogger("TestCreater")

    from ILCDIRAC.Interfaces.API.DiracILC                  import DiracILC, __RCSID__ as drcsid
    from ILCDIRAC.Interfaces.API.NewInterface.UserJob      import __RCSID__ as jrcsid
    from ILCDIRAC.Interfaces.API.NewInterface.Applications import __RCSID__ as apprcsid

    if self.clip.submitMode == "local":
      self.log.notice("")
      self.log.notice("       DIRAC RCSID:", drcsid )
      self.log.notice("         Job RCSID:", jrcsid )
      self.log.notice("Applications RCSID:", apprcsid )
      self.log.notice("")

    self.diracInstance = DiracILC(False, 'tests.rep')
    self.jobList = {}

  def createMokkaTest(self):
    """create a job running mokka, and maybe whizard before"""
    self.log.notice("Creating jobs for Mokka")
    #(Whizard + )Mokka
    jobmo = self.getJob()
    if self.clip.testChain:
      whmo = self.getWhizard(2)
      res = jobmo.append(whmo)
      if not res['OK']:
        self.log.error("Failed adding Whizard:", res['Message'])
        return S_ERROR("Failed adding Whizard")
    elif self.clip.testInputData:
      jobmo.setInputData("/ilc/user/s/sailer/testFiles/prod_clic_e2e2_o_gen_2213_25.stdhep")
    else:
      self.log.error("Mokka does not know where to get its input from")
      return S_ERROR("Mokka does not know where to gets its input from")

    mo = self.getMokka()

    if self.clip.testChain:
      mo.getInputFromApp(whmo)
    else:
      mo.setNumberOfEvents(2)
    res = jobmo.append(mo)
    if not res['OK']:
      self.log.error("Failed adding Mokka:", res['Message'])
      return S_ERROR("Failed adding Mokka to Job")
    jobmo.setOutputData("testsim.slcio", OutputSE="CERN-DIP-4")
    self.jobList['Mokka1'] = jobmo
    return S_OK(jobmo)

  def createRootScriptTest(self):
    """create a job running root"""
    self.log.notice("Creating jobs for Root")
    jobRoot = self.getJob()
    jobRoot.setInputSandbox(["root.sh", "input.root","input2.root"])
    root = self.getRoot()
    res = jobRoot.append(root)
    if not res['OK']:
      self.log.error("Failed adding Root:", res['Message'])
      return S_ERROR("Failed adding Root to Job")
    self.jobList['Root'] = jobRoot
    return S_OK(jobRoot)

  def createRootHaddTest(self):
    """create a job running root"""
    self.log.notice("Creating jobs for Root")
    jobRoot = self.getJob()
    jobRoot.setInputSandbox(["input.root","input2.root"])
    root = self.getRoot()
    root.setScript("hadd")
    res = jobRoot.append(root)
    if not res['OK']:
      self.log.error("Failed adding Root:", res['Message'])
      return S_ERROR("Failed adding Root to Job")
    self.jobList['Root'] = jobRoot
    return S_OK(jobRoot)

  def createRootMacroTest(self):
    """create a job running root"""
    self.log.notice("Creating jobs for Root")
    jobRoot = self.getJob()
    jobRoot.setInputSandbox(["func.C", "input.root","input2.root"])
    root = self.getRootMacro()
    root.setScript("func.C")
    res = jobRoot.append(root)
    if not res['OK']:
      self.log.error("Failed adding Root:", res['Message'])
      return S_ERROR("Failed adding Root to Job")
    self.jobList['Root'] = jobRoot
    return S_OK(jobRoot)

  def getOverlay(self, nbevts):
    """ Create an overlay step
    """
    pathToFiles = None
    from ILCDIRAC.Interfaces.API.NewInterface.Applications import OverlayInput
    overlay = OverlayInput()
    if self.energy==350:
      if self.detectorModel=="ILD_o1_v05":
        pathToFiles="/ilc/user/s/sailer/testFiles/overlay/ild_350/"
    if pathToFiles:
      overlay.setPathToFiles(pathToFiles)
    else:
      self.log.warn("better define pathToFiles for this overlay: %s, %s, %s" %
                    (self.energy, self.machine, self.backgroundType) )
      overlay.setMachine(self.machine)
      overlay.setEnergy(self.energy)
      overlay.setDetectorModel(self.detectorModel)

    overlay.setBkgEvtType(self.backgroundType)
    overlay.setBXOverlay(60)
    overlay.setGGToHadInt(0.3)
    overlay.setNumberOfSignalEventsPerJob(nbevts)

    return overlay

  def getMokka(self):
    """ Define a mokka app
    """
    from ILCDIRAC.Interfaces.API.NewInterface.Applications import Mokka
    mokka = Mokka()
    mokka.setVersion(self.mokkaVersion)
    mokka.setSteeringFile(self.mokkaSteeringFile)
    mokka.setOutputFile("testsim.slcio")
    mokka.setDetectorModel(self.detectorModel)
    if self.steeringFileVersion:
      mokka.setSteeringFileVersion(self.steeringFileVersion)
    return mokka

  def getRoot(self):
    """ Define a root app
    """
    from ILCDIRAC.Interfaces.API.NewInterface.Applications import RootScript
    root = RootScript()
    root.setScript("root.sh")
    root.setArguments("output.root input.root input2.root")
    root.setVersion(self.rootVersion)
    root.setOutputFile("output.root")
    return root

  def getRootMacro(self):
    """ Define a root app
    """
    from ILCDIRAC.Interfaces.API.NewInterface.Applications import RootMacro
    root = RootMacro()
    root.setMacro("func.C")
    root.setArguments(r"\"input.root\"")
    root.setVersion(self.rootVersion)
    return root

  @staticmethod
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

  @staticmethod
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


  def getMarlin( self ):
    """ Define a marlin step
    """
    from ILCDIRAC.Interfaces.API.NewInterface.Applications import Marlin
    marlin = Marlin()
  #  marlin.setVersion("v0111Prod")
    marlin.setVersion(self.marlinVersion)
    marlin.setSteeringFile(self.marlinSteeringFile)
    marlin.setGearFile(self.gearFile)
    marlin.setOutputDstFile("testmarlinDST.slcio")
    marlin.setOutputRecFile("testmarlinREC.slcio")
    marlin.setNumberOfEvents(2)
    return marlin

  def getLCSIM(self, prepandora = True):
    """ Get some LCSIM
    """
    from ILCDIRAC.Interfaces.API.NewInterface.Applications import LCSIM
    lcsim = LCSIM()
    lcsim.setVersion('CLIC_CDR')
    lcsim.setDetectorModel('clic_sid_cdr.zip')
    if prepandora:
      lcsim.setSteeringFile(self.lcsimPreSteeringFile)
      lcsim.setOutputFile("testlcsim.slcio")
    else:
      lcsim.setSteeringFile(self.lcsimPostSteeringFile)
      #lcsim.setOutputFile("testlcsimfinal.slcio")
      lcsim.setOutputDstFile("testlcsimDST.slcio")
      lcsim.setOutputRecFile("testlcsimREC.slcio")
    lcsim.setTrackingStrategy("defaultStrategies_clic_sid_cdr.xml")
    return lcsim



  @staticmethod
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

  @staticmethod
  def getStdhepSplit():
    """ Get some stdhep split
    """
    from ILCDIRAC.Interfaces.API.NewInterface.Applications import StdHepSplit
    stdhepsplit = StdHepSplit()
    stdhepsplit.setVersion("V2")
    stdhepsplit.setNumberOfEventsPerFile(5)
    stdhepsplit.setOutputFile("teststdhepsplit.stdhep")
    return stdhepsplit

  @staticmethod
  def getLCIOSplit(events_per_file):
    """ Get a LCIO split
    """
    from ILCDIRAC.Interfaces.API.NewInterface.Applications import SLCIOSplit
    lciosplit = SLCIOSplit()
    lciosplit.setNumberOfEventsPerFile(events_per_file)
    lciosplit.setOutputFile("testlciosplit.slcio")
    return lciosplit

  @staticmethod
  def getLCIOConcat():
    """ Get a LCIO Concat
    """
    from ILCDIRAC.Interfaces.API.NewInterface.Applications import SLCIOConcatenate
    lcioconcat = SLCIOConcatenate()
    lcioconcat.setOutputFile("testlcioconcat.slcio")
    return lcioconcat

  def getJob(self):
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
    myjob.setOutputSandbox(["*.log","*.xml", "*.sh"])
    myjob._addParameter( myjob.workflow, 'TestFailover', 'String', True, 'Test failoverRequest')
    myjob._addParameter( myjob.workflow, 'Platform', 'String', "x86_64-slc5-gcc43-opt", 'Test failoverRequest')
    if self.ildConfig:
      myjob.setILDConfig(self.ildConfig)
    return myjob



  def getWhizardModel(self, nbevts, energy, model):
    """ Create a default whizard
    """
    from ILCDIRAC.Interfaces.API.NewInterface.Applications import Whizard
    proddict = "e2e2_o"
    if model != "sm":
      proddict = "se2se2_r"
    whiz = Whizard(processlist = self.diracInstance.getProcessList())
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
    pdict['beam_input_1']['USER_spectrum_mode'] = 19 if energy == 1400 else 11
    pdict['beam_input_1']['ISR_on'] = 'T'
    pdict['beam_input_1']['EPA_on'] = "F"

    pdict['beam_input_2'] = {}
    pdict['beam_input_2']['particle_name'] = "E1"
    pdict['beam_input_2']['polarization'] = "0.0 0.0"
    pdict['beam_input_2']['USER_spectrum_on'] = 'T'
    pdict['beam_input_2']['ISR_on'] = 'T'
    pdict['beam_input_2']['USER_spectrum_mode'] = 19 if energy == 1400 else 11
    pdict['beam_input_2']['EPA_on'] = 'F'


    whiz.setFullParameterDict(pdict)
    whiz.setOutputFile("testgen.stdhep")
    return whiz


  def getWhizard(self, nbevts):
    """ Get some defualt SM whizard
    """
    return self.getWhizardModel(nbevts, 1400, "sm")

  def getWhizardSUSY(self, nbevts):
    """ Get a susy whizard
    """
    return self.getWhizardModel(nbevts, 3000, "slsqhh")



  def createWhizardTest(self):
    """create a test for whizard"""
    self.log.notice("Creating jobs for Whizard")
    ##### WhizardJob
    jobw = self.getJob()
    wh = self.getWhizard(2)
    res = jobw.append(wh)
    if not res['OK']:
      self.log.error("Failed adding Whizard:", res['Message'])
      return S_ERROR()
    self.jobList['Whizard1'] = jobw

    ##### WhizardJob
    jobwsusy = self.getJob()
    whsusy = self.getWhizardSUSY(2)
    res = jobwsusy.append(whsusy)
    if not res['OK']:
      self.log.error("Failed adding Whizard:", res['Message'])
      return S_ERROR()
    self.jobList['WhizSusy'] = jobwsusy
    return S_OK((jobw, jobwsusy))

  def createSlicTest(self):
    """create tests for slic"""
    self.log.notice("Creating jobs for SLIC")
    #run (Whizard +)SLIC
    jobslic = self.getJob()
    if self.clip.testChain:
      whslic = self.getWhizard(2)
      res = jobslic.append(whslic)
      if not res["OK"]:
        self.log.error("Failed adding Whizard:", res['Value'])
        return S_ERROR()
    elif self.clip.testInputData:
      jobslic.setInputData("/ilc/user/s/sailer/testFiles/prod_clic_e2e2_o_gen_2213_25.stdhep")
    else:
      self.log.error("SLIC does not know where to get its input from")
      return S_ERROR()
    myslic = self.getSLIC()
    if self.clip.testChain:
      myslic.getInputFromApp(whslic)
    else:
      myslic.setNumberOfEvents(2)
    res = jobslic.append(myslic)
    if not res['OK']:
      self.log.error("Failed adding slic: ", res["Message"])
      return S_ERROR()
    self.jobList['Slic1'] = jobslic
    return S_OK(jobslic)


  def createMarlinTest(self):
    """create tests for marlin"""
    self.log.notice( "Creating test for Marlin" )
        #((Whizard + Mokka +)Overlay+) Marlin
    jobma = self.getJob()
    if self.clip.testChain:
      moma = self.getMokka()
      if not self.clip.testInputData:
        whma = self.getWhizard(2)
        res = jobma.append(whma)
        if not res['OK']:
          self.log.error("Failed adding Whizard:", res['Message'])
          return S_ERROR()
        moma.getInputFromApp(whma)
      else:
        jobma.setInputData("/ilc/user/s/sailer/testFiles/prod_clic_e2e2_o_gen_2213_25.stdhep")
        moma.setNumberOfEvents(2)
      res = jobma.append(moma)
      if not res['OK']:
        self.log.error("Failed adding Mokka:", res['Message'])
        return S_ERROR()
    elif self.clip.testInputData:
      jobma.setInputData(self.marlinInputdata)
    else:
      self.log.error("Marlin does not know where to get its input from")
      return S_ERROR()
    if self.runOverlay:
      ov = self.getOverlay(2)
      res = jobma.append(ov)
      if not res["OK"]:
        self.log.error("Failed adding Overlay:", res['Message'])
        return S_ERROR
    ma = self.getMarlin()
    if self.clip.testChain:
      ma.getInputFromApp(moma)
    else:
      ma.setNumberOfEvents(2)

    res = jobma.append(ma)
    if not res['OK']:
      self.log.error("Failed adding Marlin:", res['Message'])
      return S_ERROR()
    self.jobList['Marlin1'] =jobma
    return S_OK(jobma)

  def createLCSimTest(self):
    """create tests for LCSIM"""
    self.log.notice( "Creating test for LCSIM" )
    #run ((whiz+SLIC+)+Overlay+)LCSIM
    joblcsim = self.getJob()
    if self.clip.testChain:
      mysliclcsim = self.getSLIC()

      if not self.clip.testInputData:
        whlcsim = self.getWhizard(2)
        res = joblcsim.append(whlcsim)
        if not res["OK"]:
          self.log.error("Failed adding Whizard:", res['Value'])
          return S_ERROR()
        mysliclcsim.getInputFromApp(whlcsim)
      else:
        joblcsim.setInputData("/ilc/user/s/sailer/testFiles/prod_clic_e2e2_o_gen_2213_25.stdhep")
        mysliclcsim.setNumberOfEvents(2)

      res = joblcsim.append(mysliclcsim)
      if not res['OK']:
        self.log.error("Failed adding slic: ", res["Message"])
        return S_ERROR()
    elif self.clip.testInputData:
      #joblcsim.setInputData("/ilc/prod/clic/1.4tev/ee_qqaa/SID/SIM/00002308/000/ee_qqaa_sim_2308_222.slcio")
      joblcsim.setInputData("/ilc/user/s/sailer/testFiles/clic_prod_sid_h_nunu_sim.slcio")
    else:
      self.log.error("LCSIM does not know where to get its input from")
      return S_ERROR()
    if self.runOverlay:
      ovlcsim = self.getOverlay(2)
      res = joblcsim.append(ovlcsim)
      if not res["OK"]:
        self.log.error("Failed adding Overlay:", res['Message'])
        return S_ERROR()
    mylcsim = self.getLCSIM(True)
    if self.clip.testChain:
      mylcsim.getInputFromApp(mysliclcsim)
    else:
      mylcsim.setNumberOfEvents(2)
    res = joblcsim.append(mylcsim)
    if not res['OK']:
      self.log.error("Failed adding LCSIM: ", res["Message"])
      return S_ERROR()
    self.jobList['lcsim1'] = joblcsim

    return S_OK(joblcsim)

  def createSlicPandoraTest(self):
    """create tests for slicPandora"""
    self.log.notice("Creating tests for SLICPandora")
    #run ((whiz+SLIC) + (Overlay +) LCSIM +) SLICPandora + LCSIM
    joblcsimov = self.getJob()
    if not self.clip.testChain:
      self.log.error("SLICPandora does not know where to get its input from")
      return S_ERROR()
    mylcsimov = self.getLCSIM(True)
    if not self.clip.testInputData:
      whlcsimov = self.getWhizard(2)
      res = joblcsimov.append(whlcsimov)
      if not res["OK"]:
        self.log.error("Failed adding Whizard:", res['Value'])
        return S_ERROR()
      mysliclcsimov = self.getSLIC()
      mysliclcsimov.getInputFromApp(whlcsimov)
      res = joblcsimov.append(mysliclcsimov)
      if not res['OK']:
        self.log.error("Failed adding slic: ", res["Message"])
        return S_ERROR()
      mylcsimov.getInputFromApp(mysliclcsimov)
    else:
      #joblcsimov.setInputData("/ilc/prod/clic/1.4tev/ee_qqaa/SID/SIM/00002308/000/ee_qqaa_sim_2308_222.slcio")
      joblcsimov.setInputData("/ilc/user/s/sailer/testFiles/clic_prod_sid_h_nunu_sim.slcio")
      mylcsimov.setNumberOfEvents(2)

    if self.runOverlay:
      ovslicp = self.getOverlay(2)
      res = joblcsimov.append(ovslicp)
      if not res["OK"]:
        self.log.error("Failed adding Overlay:", res['Message'])
        return S_ERROR()

    res = joblcsimov.append(mylcsimov)
    if not res['OK']:
      self.log.error("Failed adding LCSIM: ", res["Message"])
      return S_ERROR()

    myslicpov = self.getSLICPandora()
    myslicpov.getInputFromApp(mylcsimov)
    res = joblcsimov.append(myslicpov)
    if not res['OK']:
      self.log.error("Failed adding SLICPandora: ", res["Message"])
      return S_ERROR()
    mylcsimovp = self.getLCSIM(False)
    mylcsimovp.getInputFromApp(myslicpov)
    res = joblcsimov.append(mylcsimovp)
    if not res['OK']:
      self.log.error("Failed adding LCSIM: ", res["Message"])
      return S_ERROR()
    self.jobList['lcsimov1'] = joblcsimov
    return S_OK(joblcsimov)

  def createUtilityTests(self):
    """Create tests for utility applications"""
    self.log.notice("Creating tests for utility applications")
    jobwsplit = self.getJob()
    whsplit = self.getWhizard(10)
    res = jobwsplit.append(whsplit)
    if not res['OK']:
      self.log.error("Failed adding Whizard:", res['Message'])
      return S_ERROR()
    mystdsplit = TestCreater.getStdhepSplit()
    mystdsplit.getInputFromApp(whsplit)
    res = jobwsplit.append(mystdsplit)
    if not res['OK']:
      self.log.error("Failed adding StdHepSplit:", res['Message'])
      return S_ERROR()
    self.jobList['whizSplit'] = jobwsplit

    ##### WhizardJob + split
    jobwcut = self.getJob()
    whcut = self.getWhizard(100)
    res = jobwcut.append(whcut)
    if not res['OK']:
      self.log.error("Failed adding Whizard:", res['Message'])
      return S_ERROR()
    mystdcut = self.getStdhepcut( 100 )
    mystdcut.getInputFromApp(whcut)
    res = jobwcut.append(mystdcut)
    if not res['OK']:
      self.log.error("Failed adding StdHepCut:", res['Message'])
      return S_ERROR()
    self.jobList['whizCut'] = jobwcut

    #LCIO split
    joblciosplit = self.getJob()
    # joblciosplit.setInputData("/ilc/prod/clic/1.4tev/e2e2_o/ILD/DST/00002215/000/e2e2_o_dst_2215_46.slcio")
    joblciosplit.setInputData("/ilc/user/s/sailer/testFiles/prod_clic_ild_e2e2_o_sim_2214_26.slcio")
    mylciosplit = self.getLCIOSplit(100)
    res = joblciosplit.append(mylciosplit)
    if not res['OK']:
      self.log.error("Failed adding SLCIOSplit:", res['Message'])
      return S_ERROR()
    self.jobList['lcioSplit'] = joblciosplit

    #LCIO concat
    jobconcat = self.getJob()
    # jobconcat.setInputData(["/ilc/prod/clic/1.4tev/e2e2_o/ILD/DST/00002215/000/e2e2_o_dst_2215_27.slcio",
    #                         "/ilc/prod/clic/1.4tev/e2e2_o/ILD/DST/00002215/000/e2e2_o_dst_2215_46.slcio"])

    jobconcat.setInputData(["/ilc/prod/clic/1.4tev/aa_qqll_all/ILD/DST/00004275/002/aa_qqll_all_dst_4275_2104.slcio",
                            "/ilc/prod/clic/1.4tev/aa_qqll_all/ILD/DST/00004275/002/aa_qqll_all_dst_4275_2105.slcio"])

    myconcat = self.getLCIOConcat()
    res = jobconcat.append(myconcat)
    if not res['OK']:
      self.log.error("Failed adding SLCIOConcatenate:", res['Message'])
      return S_ERROR()
    self.jobList['concat'] = jobconcat
    return S_OK((jobconcat, joblciosplit,jobwcut,jobwsplit))


  def runJobLocally(self, job, jobName="unknown"):
    """run a job locally"""
    self.log.notice("I will run the tests locally.")
    from DIRAC import gConfig
    localarea = gConfig.getValue("/LocalSite/LocalArea", "")
    if not localarea:
      self.log.error("You need to have /LocalSite/LocalArea defined in your dirac.cfg")
      return S_ERROR()
    if localarea.find("/afs") == 0:
      self.log.error("Don't set /LocalSite/LocalArea set to /afs/... as you'll get to install there")
      self.log.error("check ${HOME}/.dirac.cfg")
      return S_ERROR()
    self.log.notice("To run locally, I will create a temp directory here.")
    curdir = os.getcwd()
    tmpdir = tempfile.mkdtemp("", dir = "./")
    os.chdir(tmpdir)

    if 'root' in jobName.lower():
      with open("root.sh", "w") as rScript:
        rScript.write( "echo $ROOTSYS" )
      with open("func.C", "w") as rMacro:
        rMacro.write( '''
                      void func( TString string ) {
                        std::cout << string << std::endl;
                        TFile* file = TFile::Open(string);
                        file->ls();
                      }
                      ''' )
      testfiledir = 'Testfiles'
      for fileName in ['input.root', 'input2.root']:
        shutil.copy( os.path.join( curdir, testfiledir, fileName ), os.getcwd() )
        print os.path.join( curdir, "input2.root"), os.getcwd()

    resJob = self.runJob(job, jobName)

    os.chdir(curdir)
    if not resJob['OK']:
      return resJob
    os.chdir(curdir)
    if not self.clip.nocleanup:
      cleanup(tmpdir)
    return S_OK()

  def run(self):
    """submit and run all the tests in jobList"""
    res = S_ERROR()
    for name, finjob in self.jobList.iteritems():
      if self.clip.submitMode == 'local':
        res = self.runJobLocally(finjob, name)
      else:
        res = self.runJob(finjob, name)
    return res

  def runJob(self, finjob, name):
    """runs or submits the job"""
    self.log.notice("############################################################")
    self.log.notice(" Running or submitting job: %s " % name)
    self.log.notice("\n\n")
    res = finjob.submit(self.diracInstance, mode = self.clip.submitMode)
    if not res["OK"]:
      self.log.error("Failed job:", res['Message'])
      return S_ERROR()
    return S_OK()


  def checkForTests(self):
    """check which tests to run"""

    if self.clip.testMokka:
      resMokka = self.createMokkaTest()
      if not resMokka['OK']:
        return S_ERROR()

    if self.clip.testWhizard:
      resWhiz = self.createWhizardTest()
      if not resWhiz['OK']:
        return S_ERROR()

    if self.clip.testSlic:
      resSlic = self.createSlicTest()
      if not resSlic['OK']:
        return S_ERROR()

    if self.clip.testMarlin:
      resMarlin = self.createMarlinTest()
      if not resMarlin['OK']:
        return S_ERROR()

    if self.clip.testLCSIM:
      resLCSim = self.createLCSimTest()
      if not resLCSim['OK']:
        return S_ERROR()

    if self.clip.testSlicPandora:
      resSP = self.createSlicPandoraTest()
      if not resSP['OK']:
        return S_ERROR()

    if self.clip.testUtilities:
      resUtil = self.createUtilityTests()
      if not resUtil['OK']:
        return S_ERROR()

    if self.clip.testRoot:
      resRoot = self.createRootScriptTest()
      if not resRoot['OK']:
        return S_ERROR()

      resRoot = self.createRootHaddTest()
      if not resRoot['OK']:
        return S_ERROR()

      resRoot = self.createRootMacroTest()
      if not resRoot['OK']:
        return S_ERROR()

    return S_OK()
