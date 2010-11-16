'''
Created on Jun 24, 2010

@author: sposs
'''
__RCSID__ = "$Id: Production.py 24/06/2010 sposs $"

from DIRAC.Core.Workflow.Workflow                     import *
from ILCDIRAC.Interfaces.API.DiracILC                       import DiracILC
from DIRAC.Core.Utilities.List                        import removeEmptyElements
from DIRAC.Core.DISET.RPCClient                       import RPCClient
from DIRAC.TransformationSystem.Client.TransformationDBClient import TransformationDBClient

from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

from ILCDIRAC.Interfaces.API.ILCJob                           import ILCJob
from DIRAC                                          import gConfig, gLogger, S_OK, S_ERROR
import string, shutil,os,types

 
class Production(ILCJob): 
  ############################################################################# 
  def __init__(self,script=None):
    """Instantiates the Workflow object and some default parameters.
    """
    ILCJob.__init__(self,script) 
    self.prodVersion=__RCSID__
    self.csSection = '/Production/Defaults'
    self.StepCount = 0
    self.currentStepPrefix = ''
    self.inputDataType = 'STDHEP' #Default
    #self.tier1s=gConfig.getValue('%s/Tier1s' %(self.csSection),['LCG.CERN.ch','LCG.CNAF.it','LCG.NIKHEF.nl','LCG.PIC.es','LCG.RAL.uk','LCG.GRIDKA.de','LCG.IN2P3.fr','LCG.SARA.nl'])
    #self.histogramName =gConfig.getValue('%s/HistogramName' %(self.csSection),'@{applicationName}_@{STEP_ID}_Hist.root')
    #self.histogramSE =gConfig.getValue('%s/HistogramSE' %(self.csSection),'CERN-HIST')
    self.systemConfig = gConfig.getValue('%s/SystemConfig' %(self.csSection),'x86_64-slc5-gcc43-opt')
    #self.systemConfig = gConfig.getValue('%s/SystemConfig' %(self.csSection),'slc4_ia32_gcc34')
    self.inputDataDefault = gConfig.getValue('%s/InputDataDefault' %(self.csSection),'/ilc/prod/clic/3tev/gen/bb/0/BS_01.stdhep')
    self.defaultProdID = '12345'
    self.defaultProdJobID = '12345'
    self.ioDict = {}
    #self.gaussList = []
    self.prodTypes = ['MCSimulation','Test','MCReconstruction']
    self.pluginsTriggeringStreamTypes = ['ByFileTypeSize','ByRunFileTypeSize','ByRun','AtomicRun']
    self.name='unspecifiedWorkflow'
    self.firstEventType = ''
    #self.bkSteps = {}
    self.prodGroup = ''
    self.plugin = ''
    self.inputFileMask = ''
    self.inputBKSelection = {}
    self.nbtasks = 0
    self.jobFileGroupSize = 0
    self.ancestorProduction = ''
    self.currtransID = None
    self.importLine = """
from ILCDIRAC.Workflow.Modules.<MODULE> import <MODULE>
"""
    is_prod = "IS_PROD"
    self._addParameter(self.workflow,is_prod,'JDL',True,"This job is a production job")
    if not script:
      self.__setDefaults()

  #############################################################################
  def __setDefaults(self):
    """Sets some default parameters.
    """
    self.setType('MCReconstruction')
    self.setSystemConfig(self.systemConfig)
    self.setCPUTime('300000')
    self.setLogLevel('verbose')
    self.setJobGroup('@{PRODUCTION_ID}')
    self.setFileMask('')

    #version control
    self._setParameter('productionVersion','string',self.prodVersion,'ProdAPIVersion')

    #General workflow parameters
    self._setParameter('PRODUCTION_ID','string',self.defaultProdID.zfill(8),'ProductionID')
    self._setParameter('JOB_ID','string',self.defaultProdJobID.zfill(8),'ProductionJobID')
    #self._setParameter('poolXMLCatName','string','pool_xml_catalog.xml','POOLXMLCatalogName')
    self._setParameter('Priority','JDL','1','Priority')
    self._setParameter('emailAddress','string','stephane.poss@cern.ch','CrashEmailAddress')
    self._setParameter('DataType','string','MC','Priority') #MC or DATA
    self._setParameter('outputMode','string','Local','SEResolutionPolicy')

    #Options related parameters
    self._setParameter('EventMaxDefault','string','-1','DefaultNumberOfEvents')
    #BK related parameters
    self._setParameter('lfnprefix','string','prod','LFNprefix')
    self._setParameter('lfnpostfix','string','2009','LFNpostfix')
    #self._setParameter('conditions','string','','SimOrDataTakingCondsString')
  #############################################################################
  def _setParameter(self,name,parameterType,parameterValue,description):
    """Set parameters checking in CS in case some defaults need to be changed.
    """
    if gConfig.getValue('%s/%s' %(self.csSection,name),''):
      self.log.debug('Setting %s from CS defaults = %s' %(name,gConfig.getValue('%s/%s' %(self.csSection,name))))
      self._addParameter(self.workflow,name,parameterType,gConfig.getValue('%s/%s' %(self.csSection,name),'default'),description)
    else:
      self.log.debug('Setting parameter %s = %s' %(name,parameterValue))
      self._addParameter(self.workflow,name,parameterType,parameterValue,description)

  def addWhizardStep(self,processlist,process,nbevts=0,lumi=0,outputpath="",outputSE=""):
    """ Define Whizard step
    
    Must get the process list from dirac.
    
    The output file name is created automatically by whizard using the process name (process_gen.stdhep).
    
    Number of events and luminosity should not be specified together, they are determined one with the other using the cross section in the process list. Luminosity prevails.
    
    @param process: process to generate, must be available in the processlist
    @type process: string
    @param nbevts: number of events to generate
    @type nbevts: int
    @param lumi: luminosity to generate
    @type lumi: double
    @param outputpath: path to store the output file
    @type outputpath: string
    @param outputSE: Storage element to use
    @type putputSE: string
    """
    appvers = ""

    if process:
      if not processlist.existsProcess(process)['Value']:
        self.log.error('Process %s does not exist in any whizard version, please contact responsible.'%process)
        self.log.info("Available processes are:")
        processlist.printProcesses()
        return S_ERROR('Process %s does not exist in any whizard version.'%process)
      else:
        cspath = processlist.getCSPath(process)
        whiz_file = os.path.basename(cspath)
        appvers= whiz_file.replace(".tar.gz","").replace(".tgz","").replace("whizard","")
        self.log.info("Found process %s corresponding to whizard%s"%(process,appvers))
        processes = processlist.getProcessesDict()
        cross_section = float(processes[process]["CrossSection"])
        if cross_section:
          if not lumi and nbevts:
            lumi = nbevts/cross_section
          if lumi and not nbevts:
            nbevts = lumi*cross_section
        print "Will generate %s evts, or lumi=%s fb"%(nbevts,lumi)    
    else:
      print "Process to generate was not specified"
      return S_ERROR("Process to generate was not specified")
    
    
    outputfile = process+"_gen.stdhep"
    
    self.StepCount +=1
    stepName = 'Whizard'
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' %('Whizard',stepNumber)
    self._addParameter(self.workflow,'TotalSteps','String',self.StepCount,'Total number of steps')

    whizardStep =     ModuleDefinition('WhizardAnalysis')
    whizardStep.setDescription('Whizard step: generate the physics events')
    body = string.replace(self.importLine,'<MODULE>','WhizardAnalysis')
    whizardStep.setBody(body)
    
    createoutputlist = ModuleDefinition('ComputeOutputDataList')
    createoutputlist.setDescription('Compute the outputList parameter, needed by outputdataPolicy')
    body = string.replace(self.importLine,'<MODULE>','ComputeOutputDataList')
    createoutputlist.setBody(body)
    
    WhizardAppDefn = StepDefinition(stepDefn)
    WhizardAppDefn.addModule(whizardStep)
    WhizardAppDefn.createModuleInstance('WhizardAnalysis',stepDefn)
    WhizardAppDefn.addModule(createoutputlist)
    WhizardAppDefn.createModuleInstance('ComputeOutputDataList',stepDefn)
    self._addParameter(WhizardAppDefn,'applicationVersion','string','','ApplicationVersion')
    self._addParameter(WhizardAppDefn,"applicationLog","string","","Application log file")
    self._addParameter(WhizardAppDefn,"EvtType","string","","Process to generate")
    self._addParameter(WhizardAppDefn,"NbOfEvts","int",0,"Number of events to generate")
    self._addParameter(WhizardAppDefn,'listoutput',"list",[],"list of output file name")
    self._addParameter(WhizardAppDefn,"outputPath","string","","Output data path")
    self._addParameter(WhizardAppDefn,"outputFile","string","","output file name")

    self._addParameter(WhizardAppDefn,"Lumi","float",0,"Number of events to generate")
    self.workflow.addStep(WhizardAppDefn)
    mstep = self.workflow.createStepInstance(stepDefn,stepName)
    mstep.setValue('applicationVersion',appvers)
    mstep.setValue('applicationLog', 'Whizard_@{STEP_ID}.log')
    mstep.setValue("EvtType",process)
    mstep.setValue("NbOfEvts",nbevts)
    mstep.setValue("Lumi",lumi)
    mstep.setValue("outputFile",outputfile)
    mstep.setValue("outputPath",outputpath)
    
    outputList=[]
    outputList.append({"outputFile":"@{outputFile}","outputPath":"@{outputPath}","outputDataSE":outputSE})
    mstep.setValue('listoutput',(outputList))

    self.__addSoftwarePackages('whizard.%s' %(appvers))   
    self._addParameter(self.workflow,"WhizardOutput","string",outputfile,"whizard expected output file name")
    if nbevts:
      self._addParameter(self.workflow,"NbOfEvents","int",nbevts,"Number of events")
    if lumi:
      self._addParameter(self.workflow,"Luminosity","float",lumi,"Luminosity")
    self.ioDict["WhizardStep"]=mstep.getName()
    return S_OK()

  def addMokkaStep(self,appvers,steeringfile,detectormodel=None,numberofevents=0,outputfile="",outputpath="",outputSE=""):
    """ Define Mokka step in production system
    
    @param appvers: version of MOKKA to use
    @type appvers: string
    @param steeringfile: file name of the steering. Should not be lfn. Should be passed in with setInputSandbox.
    @type steeringfile: string
    @param detectormodel: detector model to use. Must be available with the Mokka version specified
    @type detectormodel: string
    @param numberofevents: number of events to process. If whizard was run before, number of events is resolved from there
    @type numberofevents: int
    @param outputfile: File name to be produced with mokka.
    @type outputfile: string
    @param outputpath: path where to store the data. Should be /ilc/prod/<machine>/<energy>/<evt type>/ILD/SIM/
    @type outputpath: string
    @param outputSE: Storage element to use
    @type putputSE: string    
    """
    self.StepCount +=1
    
    stepName = 'RunMokka'
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' %('Mokka',stepNumber)
    
    self._addParameter(self.workflow,'TotalSteps','String',self.StepCount,'Total number of steps')    
    mokkaStep = ModuleDefinition('MokkaAnalysis')
    mokkaStep.setDescription('Mokka step: simulation in ILD-like geometries context')
    body = string.replace(self.importLine,'<MODULE>','MokkaAnalysis')
    mokkaStep.setBody(body)
    
    createoutputlist = ModuleDefinition('ComputeOutputDataList')
    createoutputlist.setDescription('Compute the outputList parameter, needed by outputdataPolicy')
    body = string.replace(self.importLine,'<MODULE>','ComputeOutputDataList')
    createoutputlist.setBody(body)
     
    MokkaAppDefn = StepDefinition(stepDefn)
    MokkaAppDefn.addModule(mokkaStep)
    MokkaAppDefn.createModuleInstance('MokkaAnalysis',stepDefn)
    MokkaAppDefn.addModule(createoutputlist)
    MokkaAppDefn.createModuleInstance('ComputeOutputDataList',stepDefn)
    self._addParameter(MokkaAppDefn,'applicationVersion','string','','ApplicationVersion')
    self._addParameter(MokkaAppDefn,"steeringFile","string","","Name of the steering file")
    self._addParameter(MokkaAppDefn,"detectorModel","string","",'Detector Model')
    self._addParameter(MokkaAppDefn,"numberOfEvents","int",0,"Number of events to generate")
    self._addParameter(MokkaAppDefn,"applicationLog","string","","Application log file")
    self._addParameter(MokkaAppDefn,"outputPath","string","","Output data path")
    self._addParameter(MokkaAppDefn,"outputFile","string","","output file name")
    self._addParameter(MokkaAppDefn,'listoutput',"list",[],"list of output file name")
    self._addParameter(MokkaAppDefn,"stdhepFile","string","","Name of the stdhep file")

    self.workflow.addStep(MokkaAppDefn)
    mstep = self.workflow.createStepInstance(stepDefn,stepName)
    mstep.setValue('applicationVersion',appvers)
    mstep.setValue('steeringFile',steeringfile)
    mstep.setValue("detectorModel",detectormodel)
    if self.ioDict.has_key("WhizardStep"):
      mstep.setLink("numberOfEvents",self.ioDict["WhizardStep"],"NbOfEvts")
    else:
      mstep.setValue("numberOfEvents",numberofevents)
    mstep.setValue('applicationLog', 'Mokka_@{STEP_ID}.log')
    
    if self.ioDict.has_key("WhizardStep"):
      mstep.setLink('stdhepFile',self.ioDict["WhizardStep"],'outputFile')
    mstep.setValue("outputFile",outputfile)
    mstep.setValue("outputPath",outputpath)
    outputList=[]
    outputList.append({"outputFile":"@{outputFile}","outputPath":"@{outputPath}","outputDataSE":outputSE})
    mstep.setValue('listoutput',(outputList))

    self.__addSoftwarePackages('mokka.%s' %(appvers))   
    self._addParameter(self.workflow,"MokkaOutput","string",outputfile,"Mokka expected output file name")
    self.ioDict["MokkaStep"]=mstep.getName()
    return S_OK()
  
  def addMarlinStep(self,appVers,inputXML="",inputGEAR=None,inputslcio=None,outputRECfile="",outputRECpath="",outputDSTfile="",outputDSTpath="",outputSE=""):
    """ Define Marlin step in production system
    
    @param appVers: Version of Marlin to use
    @type appVers: string
    @param inputXML: Input XML file name to perform reconstruction. Should not be lfn. Should be passed with setInputSandbox.
    @type inputXML: string
    @param inputGEAR: GEAR File name to use. Resolved automatically if MOKKA ran before.
    @type inputGEAR: string
    @param inputslcio: input slcio to use, should be used for tests only. Input slcio come from the production definition, not the workflow def.
    @param outputRECfile: File name of the REC file
    @param outputDSTfile: File name of the DST file
    @param outputRECpath: path to the REC file in the catalog. Should be like /ilc/prod/<machine>/<energy>/<process>/ILD/REC
    @param outputDSTpath: path to the DST file in the catalog. Should be like /ilc/prod/<machine>/<energy>/<process>/ILD/DST
    @param outputSE: Storage element to use
    """
    
    inputslcioStr =''
    if(inputslcio):
      if type(inputslcio) in types.StringTypes:
        inputslcio = [inputslcio]
      #for i in xrange(len(inputslcio)):
      #  inputslcio[i] = inputslcio[i].replace('LFN:','')
      #inputslcio = map( lambda x: 'LFN:'+x, inputslcio)
      inputslcioStr = string.join(inputslcio,';')    
    if not inputGEAR:
      if self.ioDict.has_key("MokkaStep"):
        inputGEAR="GearOutput.xml"
      else:
        return self._reportError('As Mokka do not run before, you need to specify gearfile')
    
    self.StepCount +=1
    stepName = 'RunMarlin'
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' %('Marlin',stepNumber)
    self._addParameter(self.workflow,'TotalSteps','String',self.StepCount,'Total number of steps')
    
    marlinStep = ModuleDefinition('MarlinAnalysis')
    marlinStep.setDescription('Marlin step: reconstruction in ILD like detectors')
    body = string.replace(self.importLine,'<MODULE>','MarlinAnalysis')
    marlinStep.setBody(body)
    createoutputlist = ModuleDefinition('ComputeOutputDataList')
    createoutputlist.setDescription('Compute the outputList parameter, needed by outputdataPolicy')
    body = string.replace(self.importLine,'<MODULE>','ComputeOutputDataList')
    createoutputlist.setBody(body)
     
    MarlinAppDefn = StepDefinition(stepDefn)
    MarlinAppDefn.addModule(marlinStep)
    MarlinAppDefn.createModuleInstance('MarlinAnalysis', stepDefn)
    MarlinAppDefn.addModule(createoutputlist)
    MarlinAppDefn.createModuleInstance('ComputeOutputDataList',stepDefn)
    self._addParameter(MarlinAppDefn,'applicationVersion','string','','ApplicationVersion')
    self._addParameter(MarlinAppDefn,"applicationLog","string","","Application log file")
    self._addParameter(MarlinAppDefn,"inputXML","string","","Name of the input XML file")
    self._addParameter(MarlinAppDefn,"inputGEAR","string","","Name of the input GEAR file")
    self._addParameter(MarlinAppDefn,"inputSlcio","string","","List of input SLCIO files")
    self._addParameter(MarlinAppDefn,"outputPathREC","string","","Output data path of REC")
    self._addParameter(MarlinAppDefn,"outputREC","string","","output file name of REC")
    self._addParameter(MarlinAppDefn,"outputPathDST","string","","Output data path of DST")
    self._addParameter(MarlinAppDefn,"outputDST","string","","output file name of DST")
    self._addParameter(MarlinAppDefn,'listoutput',"list",[],"list of output file name")    
    self.workflow.addStep(MarlinAppDefn)
    mstep = self.workflow.createStepInstance(stepDefn,stepName)
    mstep.setValue('applicationVersion',appVers)    
    mstep.setValue('applicationLog', 'Marlin_@{STEP_ID}.log')
    if(inputslcioStr):
      mstep.setValue("inputSlcio",inputslcioStr)
    else:
      if not self.ioDict.has_key("MokkaStep"):
        raise TypeError,'Expected previously defined Mokka step for input data'
      mstep.setLink('inputSlcio',self.ioDict["MokkaStep"],'outputFile')
    mstep.setValue("inputXML",inputXML)
    mstep.setValue("inputGEAR",inputGEAR)
    mstep.setValue("outputREC",outputRECfile)
    mstep.setValue("outputPathREC",outputRECpath)
    mstep.setValue("outputDST",outputDSTfile)
    mstep.setValue("outputPathDST",outputDSTpath)
    outputList=[]
    outputList.append({"outputFile":"@{outputREC}","outputPath":"@{outputPathREC}","outputDataSE":outputSE})
    outputList.append({"outputFile":"@{outputDST}","outputPath":"@{outputPathDST}","outputDataSE":outputSE})
    mstep.setValue('listoutput',(outputList))

    self.__addSoftwarePackages('marlin.%s' %(appVers))
    self.ioDict["MarlinStep"]=mstep.getName()
    return S_OK()

  def addSLICStep(self,appVers,inputmac="",detectormodel=None,numberofevents=0,outputfile="",outputpath="",outputSE=""):
    """ Define SLIC step for production
    
    @param appVers: SLIC version to use
    @param inputmac: File name of the mac file to use. Should not be lfn. Should be passed with setInputSandbox.
    @param detectormodel: Detector model to use. Is downloaded automatically from the web
    @param numberofevents: number of events to process
    @param outputfile: File name of the outputfile
    @param outputpath: path to file in File Catalog. Should be like /ilc/prod/<machine>/<energy>/<evt type>/SID/SIM/
    @param outputSE: Storage element to use
    """
    self.StepCount +=1
    stepName = 'RunSLIC'
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' %('SLIC',stepNumber)
    self._addParameter(self.workflow,'TotalSteps','String',self.StepCount,'Total number of steps')
    
    slicStep = ModuleDefinition('SLICAnalysis')
    slicStep.setDescription('SLIC step: simulation in SiD like detectors')
    body = string.replace(self.importLine,'<MODULE>','SLICAnalysis')
    slicStep.setBody(body)
    createoutputlist = ModuleDefinition('ComputeOutputDataList')
    createoutputlist.setDescription('Compute the outputList parameter, needed by outputdataPolicy')
    body = string.replace(self.importLine,'<MODULE>','ComputeOutputDataList')
    createoutputlist.setBody(body)
     
    slicAppDefn = StepDefinition(stepDefn)
    slicAppDefn.addModule(slicStep)
    slicAppDefn.createModuleInstance('SLICAnalysis', stepDefn)
    slicAppDefn.addModule(createoutputlist)
    slicAppDefn.createModuleInstance('ComputeOutputDataList',stepDefn)
    self._addParameter(slicAppDefn,'applicationVersion','string','','ApplicationVersion')
    self._addParameter(slicAppDefn,"applicationLog","string","","Application log file")
    self._addParameter(slicAppDefn,"detectorModel","string","","Name of the detector model")
    self._addParameter(slicAppDefn,"inputmacFile","string","","Name of the mac file")
    self._addParameter(slicAppDefn,"numberOfEvents","int",0,"Number of events to process")
    self._addParameter(slicAppDefn,"outputPath","string","","Output data path")
    self._addParameter(slicAppDefn,"outputFile","string","","output file name")
    self._addParameter(slicAppDefn,'listoutput',"list",[],"list of output file name")    
    self.workflow.addStep(slicAppDefn)
    mstep = self.workflow.createStepInstance(stepDefn,stepName)
    mstep.setValue('applicationVersion',appVers)    
    mstep.setValue('applicationLog', 'Slic_@{STEP_ID}.log')
    mstep.setValue("detectorModel",detectormodel)
    mstep.setValue("numberOfEvents",numberofevents)
    mstep.setValue("inputmacFile",inputmac)
    mstep.setValue("outputFile",outputfile)
    mstep.setValue("outputPath",outputpath)
    outputList=[]
    outputList.append({"outputFile":"@{outputFile}","outputPath":"@{outputPath}","outputDataSE":outputSE})
    mstep.setValue('listoutput',(outputList))

    self.__addSoftwarePackages('slic.%s' %(appVers))
    self.ioDict["SLICStep"]=mstep.getName()
    return S_OK()

  def addLCSIMStep(self,appVers,outputfile="",outputpath="",outputSE=""):
    """ Define LCSIM step for production
    
    @param appVers: LCSIM version to use
    @param outputfile: Outputfile name 
    @todo: define properly REC and DST files
    @param outputSE: Storage element to use
    
    """
    self.StepCount +=1
    stepName = 'RunLCSIM'
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' %('LCSIM',stepNumber)
    self._addParameter(self.workflow,'TotalSteps','String',self.StepCount,'Total number of steps')
    
    LCSIMStep = ModuleDefinition('LCSIMAnalysis')
    LCSIMStep.setDescription('LCSIM step: reconstruction in SiD like detectors')
    body = string.replace(self.importLine,'<MODULE>','LCSIMAnalysis')
    LCSIMStep.setBody(body)
    createoutputlist = ModuleDefinition('ComputeOutputDataList')
    createoutputlist.setDescription('Compute the outputList parameter, needed by outputdataPolicy')
    body = string.replace(self.importLine,'<MODULE>','ComputeOutputDataList')
    createoutputlist.setBody(body)
     
    LCSIMAppDefn = StepDefinition(stepDefn)
    LCSIMAppDefn.addModule(LCSIMStep)
    LCSIMAppDefn.createModuleInstance('LCSIMAnalysis', stepDefn)
    LCSIMAppDefn.addModule(createoutputlist)
    LCSIMAppDefn.createModuleInstance('ComputeOutputDataList',stepDefn)
    self._addParameter(LCSIMAppDefn,'applicationVersion','string','','ApplicationVersion')
    self._addParameter(LCSIMAppDefn,"applicationLog","string","","Application log file")
    self._addParameter(LCSIMAppDefn,"outputPath","string","","Output data path")
    self._addParameter(LCSIMAppDefn,"outputFile","string","","output file name")
    self._addParameter(LCSIMAppDefn,'listoutput',"list",[],"list of output file name")    
    self.workflow.addStep(LCSIMAppDefn)
    mstep = self.workflow.createStepInstance(stepDefn,stepName)
    mstep.setValue('applicationVersion',appVers)    
    mstep.setValue('applicationLog', 'LCSIM_@{STEP_ID}.log')
    mstep.setValue("outputFile",outputfile)
    mstep.setValue("outputPath",outputpath)
    outputList=[]
    outputList.append({"outputFile":"@{outputFile}","outputPath":"@{outputPath}","outputDataSE":outputSE})
    mstep.setValue('listoutput',(outputList))

    self.__addSoftwarePackages('lcsim.%s' %(appVers))
    self.ioDict["LCSIMStep"]=mstep.getName()
    return S_OK()
  
  def addFinalizationStep(self,uploadData=False,uploadLog = False,registerData=False):
    """ Add finalization step
    
    @param uploadData: Upload or not the data to the storage
    @param uploadLog: Upload log file to storage (currently only available for admins, thus add them to OutputSandbox)
    @param registerData: Register data in the file catalog
    @todo: Do the registration only once, instead of once for each job
    
    """
    dataUpload = ModuleDefinition('UploadOutputData')
    dataUpload.setDescription('Uploads the output data')
    self._addParameter(dataUpload,'Enable','bool','False','EnableFlag')
    body = string.replace(self.importLine,'<MODULE>','UploadOutputData')
    dataUpload.setBody(body)
    
    logUpload = ModuleDefinition('UploadLogFile')
    logUpload.setDescription('Uploads the output log files')
    self._addParameter(logUpload,'Enable','bool','False','EnableFlag')
    body = string.replace(self.importLine,'<MODULE>','UploadLogFile')
    logUpload.setBody(body)

    registerdata = ModuleDefinition('RegisterOutputData')
    registerdata.setDescription('Module to add in the metadata catalog the relevant info about the files')
    self._addParameter(registerdata,'Enable','bool','False','EnableFlag')
    body = string.replace(self.importLine,'<MODULE>','RegisterOutputData')
    registerdata.setBody(body)
 
    finalization = StepDefinition('Job_Finalization')
    
    dataUpload.setLink('Enable','self','UploadEnable')
    finalization.addModule(dataUpload)
    finalization.createModuleInstance('UploadOutputData','dataUpload')
    self._addParameter(finalization,'UploadEnable','bool',str(uploadData),'EnableFlag')

    logUpload.setLink('Enable','self','LogEnable')
    finalization.addModule(logUpload)
    finalization.createModuleInstance('UploadLogFile','logUpload')
    self._addParameter(finalization,'LogEnable','bool',str(uploadLog),'EnableFlag')

    registerdata.setLink('Enable','self','RegisterEnable')
    finalization.addModule(registerdata)
    finalization.createModuleInstance('RegisterOutputData','RegisterOutputData')
    self._addParameter(finalization,'RegisterEnable','bool',str(uploadLog),'EnableFlag')

    self.workflow.addStep(finalization)
    finalizeStep = self.workflow.createStepInstance('Job_Finalization', 'finalization')
    finalizeStep.setValue('UploadEnable',uploadData)
    finalizeStep.setValue('LogEnable',uploadLog)
    finalizeStep.setValue('RegisterEnable',registerData)

    ##Hack until log server is available
    self.addToOutputSandbox.append("*.log")
    
    return S_OK()
  #############################################################################  
  def create(self,name=None):
    """ Create the transformation based on the production definition
    
    @param name: name of the production
    """
    workflowName = self.workflow.getName()
    fileName = '%s.xml' %workflowName
    self.log.verbose('Workflow XML file name is: %s' %fileName)
    try:
      self.createWorkflow()
    except Exception,x:
      self.log.error(x)
      return S_ERROR('Could not create workflow')
    oFile = open(fileName,'r')
    workflowXML = oFile.read()
    oFile.close()
    if not name:
      name = workflowName
    ###Create Tranformation
    Trans = Transformation()
    Trans.setTransformationName(name)
    Trans.setDescription(self.workflow.getDescrShort())
    Trans.setLongDescription(self.workflow.getDescription())
    Trans.setType(self.type)
    Trans.setPlugin('Standard')
    Trans.setTransformationGroup(self.prodGroup)
    Trans.setBody(workflowXML)
    res = Trans.addTransformation()
    if not res['OK']:
      print res['Message']
      return res
    self.currtrans = Trans
    self.currtrans.setStatus("Active")
    self.currtrans.setAgentType("Automatic")
    
    return S_OK()

  def setNbOfTasks(self,nbtasks):
    """ Define the number of tasks you want. Useful for generation jobs.
    """
    if not self.currtrans:
      print "Not transformation defined earlier"
      return S_ERROR("No transformation defined")
    if self.inputBKSelection:
      print "Meta data selection activated, should not specify the nu,ber of jobs"
      return S_ERROR()
    self.nbtasks = nbtasks
    self.currtrans.setMaxNumberOfTasks(self.nbtasks)
    return S_OK()
  
  def setInputDataQuery(self,metadata,prodid=None):
    """ Tell the production to update itself using the metadata query specified, i.e. submit new jobs if new files are added corresponding to same query.
    """
    currtrans = 0
    if self.currtrans:
      currtrans = self.currtrans.getTransformationID()['Value']
    if prodid:
      currtrans = prodid
    if not currtrans:
      print "Not transformation defined earlier"
      return S_ERROR("No transformation defined")
    if self.nbtasks:
      print "Nb of tasks defined already, should not use InputDataQuery"
      return S_ERROR()
    if not type(metadata) == type({}):
      print "metadata should be a dictionnary"
      return S_ERROR()
    metakeys = metadata.keys()
    client = FileCatalogClient()
    res = client.getMetadataFields()
    if not res['OK']:
      print "Could not contact File Catalog"
      self.explainInputDataQuery()
      return S_ERROR()
    metaFCkeys = res['Value'].keys()
    for key in metakeys:
      for meta in metaFCkeys:
        if meta != key:
          if meta.lower()==key.lower():
            print "Key syntax error %s, should be %s"%(key,meta)
            self.explainInputDataQuery()
            return S_ERROR()
      if not metaFCkeys.count(key):
        print "Key %s not found in metadata keys, allowed are %s"%(key,metaFCkeys)
        self.explainInputDataQuery()
        return S_ERROR()
      
    self.inputBKSelection = metadata
    client = TransformationDBClient()
    res = client.createTransformationInputDataQuery(currtrans,self.inputBKSelection)
    if not res['OK']:
      return res

  def explainInputDataQuery(self):
    print """To create production using input data query, do the following:
    1) get the production number (prodid) you want to modify (from the web interface)
    2) code the following:
    p = Production()
    meta = {}
    meta['Some metadata key'] = some value
    p.setInputDataQuery(meta,prodid)
    3) check from web monitor that files are found. If not, let ilc-dirac@cern.ch know.
    """
    return

  def setInputDataDirectoryMask(self,dir):
    """ More or less same feature as above, but useful for user's directory that don't have metadata info specified
    """
    self.currtrans.setFileMask(dir)
    return S_OK()    

  def setInputDataLFNs(self,lfns):
    """ Define by hand the input LFN list instead of relying on the input data query. 
    
    Useful when list is obtained from a user's job reopository.
    """
    self.currtrans.addFilesToTransformation(lfns)
    return S_OK()

  #############################################################################
  def getParameters(self,prodID,pname='',printOutput=False):
    """Get a production parameter or all of them if no parameter name specified.
    """
    prodClient = RPCClient('Transformation/TransformationManager',timeout=120)
    result = prodClient.getTransformation(int(prodID),True)
    if not result['OK']:
      self.log.error(result)
      return S_ERROR('Could not retrieve parameters for production %s' %prodID)

    if not result['Value']:
      self.log.info(result)
      return S_ERROR('No additional parameters available for production %s' %prodID)

    if pname:
      if result['Value'].has_key(pname):
        return S_OK(result['Value'][pname])
      else:
        self.log.verbose(result)
        return S_ERROR('Production %s does not have parameter %s' %(prodID,pname))

    if printOutput:
      for n,v in result['Value'].items():
        if not n.lower()=='body':
          print '='*len(n),'\n',n,'\n','='*len(n)
          print v
        else:
          print '*Omitted Body from printout*'

    return result
  #############################################################################
  def __addSoftwarePackages(self,nameVersion):
    """ Internal method to accumulate software packages.
    """
    swPackages = 'SoftwarePackages'
    description='ILCSoftwarePackages'
    if not self.workflow.findParameter(swPackages):
      self._addParameter(self.workflow,swPackages,'JDL',nameVersion,description)
    else:
      apps = self.workflow.findParameter(swPackages).getValue()
      apps = apps.split(';')
      apps.append(nameVersion)
      apps = removeEmptyElements(apps)
      apps = string.join(apps,';')
      self._addParameter(self.workflow,swPackages,'JDL',apps,description)
  #############################################################################
  def createWorkflow(self):
    """ Create XML for local testing.
    """
    name = '%s.xml' % self.name
    if os.path.exists(name):
      shutil.move(name,'%s.backup' %name)
    self.workflow.toXMLFile(name)
  #############################################################################
  def runLocal(self):
    """ Create XML workflow for local testing then reformulate as a job and run locally.
    """
    name = '%s.xml' % self.name
    if os.path.exists(name):
      shutil.move(name,'%s.backup' %name)
    self.workflow.toXMLFile(name)
    j = ILCJob(name)
    d = DiracILC()
    return d.submit(j,mode='local')

  #############################################################################
  def setFileMask(self,fileMask):
    """Output data related parameters.
    """
    if type(fileMask)==type([]):
      fileMask = string.join(fileMask,';')
    self._setParameter('outputDataFileMask','string',fileMask,'outputDataFileMask')

  #############################################################################
  def setWorkflowName(self,name):
    """Set workflow name.
    """
    self.workflow.setName(name)
    self.name = name

  #############################################################################
  def setWorkflowDescription(self,desc):
    """Set workflow name.
    """
    self.workflow.setDescription(desc)

  #############################################################################
  def setProdType(self,prodType):
    """Set prod type.
    """
    if not prodType in self.prodTypes:
      raise TypeError,'Prod must be one of %s' %(string.join(self.prodTypes,', '))
    self.setType(prodType)

  #############################################################################
  def banTier1s(self):
    """ Sets Tier1s as banned.
    """
    self.setBannedSites(self.tier1s)

  #############################################################################
  def setTargetSite(self,site):
    """ Sets destination for all jobs.
    """
    self.setDestination(site)

  #############################################################################
  def setOutputMode(self,outputMode):
    """ Sets output mode for all jobs, this can be 'Local' or 'Any'.
    """
    if not outputMode.lower().capitalize() in ('Local','Any'):
      raise TypeError,'Output mode must be Local or Any'
    self._setParameter('outputMode','string',outputMode.lower().capitalize(),'SEResolutionPolicy')
  #############################################################################
  def setProdPriority(self,priority):
    """ Sets destination for all jobs.
    """
    self._setParameter('Priority','JDL',str(priority),'UserPriority')

  #############################################################################
  def setProdGroup(self,group):
    """ Sets a user defined tag for the production as appears on the monitoring page
    """
    self.prodGroup = group

  #############################################################################
  def setProdPlugin(self,plugin):
    """ Sets the plugin to be used to creating the production jobs
    """
    self.plugin = plugin

  #############################################################################
  def setInputFileMask(self,fileMask):
    """ Sets the input data selection when using file mask.
    """
    self.inputFileMask = fileMask

  #############################################################################
  #############################################################################
  def setJobFileGroupSize(self,files):
    """ Sets the number of files to be input to each job created.
    """
    self.jobFileGroupSize = files

  #############################################################################
  def setWorkflowString(self, wfString):
    """ Uses the supplied string to create the workflow
    """
    self.workflow = fromXMLString(wfString)
    self.name = self.workflow.getName()

  #############################################################################
  def disableCPUCheck(self):
    """ Uses the supplied string to create the workflow
    """
    self._setParameter('DisableCPUCheck','JDL','True','DisableWatchdogCPUCheck')
  