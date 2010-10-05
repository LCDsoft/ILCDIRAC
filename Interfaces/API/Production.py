'''
Created on Jun 24, 2010

@author: sposs
'''
__RCSID__ = "$Id: Production.py 24/06/2010 sposs $"

from DIRAC.Core.Workflow.Workflow                     import *
from ILCDIRAC.Interfaces.API.DiracILC                       import DiracILC
from DIRAC.Core.Utilities.List                        import removeEmptyElements

from ILCDIRAC.Interfaces.API.ILCJob                           import ILCJob
from DIRAC                                          import gConfig, gLogger, S_OK, S_ERROR
import string, shutil

 
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
    self.prodTypes = ['MCSimulation','Test']
    self.pluginsTriggeringStreamTypes = ['ByFileTypeSize','ByRunFileTypeSize','ByRun','AtomicRun']
    self.name='unspecifiedWorkflow'
    self.firstEventType = ''
    #self.bkSteps = {}
    self.prodGroup = ''
    self.plugin = ''
    self.inputFileMask = ''
    self.inputBKSelection = {}
    self.jobFileGroupSize = 0
    self.ancestorProduction = ''
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
    else:
      print "Process to generate was not specified"
      return S_ERROR("Process to generate was not specified")
    
    
    outputfile = process+"_gen.stdhep"
    
    self.StepCount +=1
    whizardStep =     ModuleDefinition('WhizardAnalysis')
    whizardStep.setDescription('Whizard step: generate the physics events')
    body = string.replace(self.importLine,'<MODULE>','WhizardAnalysis')
    whizardStep.setBody(body)
    
    createoutputlist = ModuleDefinition('ComputeOutputDataList')
    createoutputlist.setDescription('Compute the outputList parameter, needed by outputdataPolicy')
    body = string.replace(self.importLine,'<MODULE>','ComputeOutputDataList')
    createoutputlist.setBody(body)
    
    WhizardAppDefn = StepDefinition('Whizard_App_Step')
    WhizardAppDefn.addModule(whizardStep)
    WhizardAppDefn.createModuleInstance('Whizardanalysis',"WhizardApp")
    WhizardAppDefn.addModule(createoutputlist)
    WhizardAppDefn.createModuleInstance('ComputeOutputDataList','compOutputDataList')
    self._addParameter(WhizardAppDefn,'applicationVersion','string','','ApplicationVersion')
    self._addParameter(WhizardAppDefn,"applicationLog","string","","Application log file")
    self._addParameter(WhizardAppDefn,"EvtType","string","","Process to generate")
    self._addParameter(WhizardAppDefn,"NbOfEvts","int",0,"Number of events to generate")
    self._addParameter(WhizardAppDefn,"Lumi","int",0,"Number of events to generate")
    self.workflow.addStep(WhizardAppDefn)
    mstep = self.workflow.createStepInstance('Whizard_App_Step','Whizard')
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
    self._addParameter(self.workflow,"MokkaOutput","string",outputfile,"Mokka expected output file name")
    self.ioDict["WhizardStep"]=mstep.getName()
    return S_OK()

  def addMokkaStep(self,appvers,steeringfile,detectormodel=None,numberofevents=0,outputfile="",outputpath="",outputSE=""):
    self.StepCount +=1
    mokkaStep = ModuleDefinition('MokkaAnalysis')
    mokkaStep.setDescription('Mokka step: simulation in ILD-like geometries context')
    body = string.replace(self.importLine,'<MODULE>','MokkaAnalysis')
    mokkaStep.setBody(body)
    
    createoutputlist = ModuleDefinition('ComputeOutputDataList')
    createoutputlist.setDescription('Compute the outputList parameter, needed by outputdataPolicy')
    body = string.replace(self.importLine,'<MODULE>','ComputeOutputDataList')
    createoutputlist.setBody(body)
     
    MokkaAppDefn = StepDefinition('Mokka_App_Step')
    MokkaAppDefn.addModule(mokkaStep)
    MokkaAppDefn.createModuleInstance('MokkaAnalysis', 'MokkaApp')
    MokkaAppDefn.addModule(createoutputlist)
    MokkaAppDefn.createModuleInstance('ComputeOutputDataList','compOutputDataList')
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
    mstep = self.workflow.createStepInstance('Mokka_App_Step','Mokka')
    mstep.setValue('applicationVersion',appvers)
    mstep.setValue('steeringFile',steeringfile)
    mstep.setValue("detectorModel",detectormodel)
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
    marlinStep = ModuleDefinition('MarlinAnalysis')
    marlinStep.setDescription('Marlin step: reconstruction in ILD like detectors')
    body = string.replace(self.importLine,'<MODULE>','MarlinAnalysis')
    marlinStep.setBody(body)
    createoutputlist = ModuleDefinition('ComputeOutputDataList')
    createoutputlist.setDescription('Compute the outputList parameter, needed by outputdataPolicy')
    body = string.replace(self.importLine,'<MODULE>','ComputeOutputDataList')
    createoutputlist.setBody(body)
     
    MarlinAppDefn = StepDefinition('Marlin_App_Step')
    MarlinAppDefn.addModule(marlinStep)
    MarlinAppDefn.createModuleInstance('MarlinAnalysis', 'MarlinApp')
    MarlinAppDefn.addModule(createoutputlist)
    MarlinAppDefn.createModuleInstance('ComputeOutputDataList','compOutputDataList')
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
    mstep = self.workflow.createStepInstance('Marlin_App_Step','Marlin')
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
    self.StepCount +=1
    slicStep = ModuleDefinition('SLICAnalysis')
    slicStep.setDescription('SLIC step: simulation in SiD like detectors')
    body = string.replace(self.importLine,'<MODULE>','SLICAnalysis')
    slicStep.setBody(body)
    createoutputlist = ModuleDefinition('ComputeOutputDataList')
    createoutputlist.setDescription('Compute the outputList parameter, needed by outputdataPolicy')
    body = string.replace(self.importLine,'<MODULE>','ComputeOutputDataList')
    createoutputlist.setBody(body)
     
    slicAppDefn = StepDefinition('SLIC_App_Step')
    slicAppDefn.addModule(slicStep)
    slicAppDefn.createModuleInstance('SLICAnalysis', 'SLICApp')
    slicAppDefn.addModule(createoutputlist)
    slicAppDefn.createModuleInstance('ComputeOutputDataList','compOutputDataList')
    self._addParameter(slicAppDefn,'applicationVersion','string','','ApplicationVersion')
    self._addParameter(slicAppDefn,"applicationLog","string","","Application log file")
    self._addParameter(slicAppDefn,"detectorModel","string","","Name of the detector model")
    self._addParameter(slicAppDefn,"inputmacFile","string","","Name of the mac file")
    self._addParameter(slicAppDefn,"numberOfEvents","int",0,"Number of events to process")
    self._addParameter(slicAppDefn,"outputPath","string","","Output data path")
    self._addParameter(slicAppDefn,"outputFile","string","","output file name")
    self._addParameter(slicAppDefn,'listoutput',"list",[],"list of output file name")    
    self.workflow.addStep(slicAppDefn)
    mstep = self.workflow.createStepInstance('SLIC_App_Step','SLIC')
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
    self.StepCount +=1
    LCSIMStep = ModuleDefinition('LCSIMAnalysis')
    LCSIMStep.setDescription('LCSIM step: reconstruction in SiD like detectors')
    body = string.replace(self.importLine,'<MODULE>','LCSIMAnalysis')
    LCSIMStep.setBody(body)
    createoutputlist = ModuleDefinition('ComputeOutputDataList')
    createoutputlist.setDescription('Compute the outputList parameter, needed by outputdataPolicy')
    body = string.replace(self.importLine,'<MODULE>','ComputeOutputDataList')
    createoutputlist.setBody(body)
     
    LCSIMAppDefn = StepDefinition('LCSIM_App_Step')
    LCSIMAppDefn.addModule(LCSIMStep)
    LCSIMAppDefn.createModuleInstance('LCSIMAnalysis', 'LCSIMApp')
    LCSIMAppDefn.addModule(createoutputlist)
    LCSIMAppDefn.createModuleInstance('ComputeOutputDataList','compOutputDataList')
    self._addParameter(LCSIMAppDefn,'applicationVersion','string','','ApplicationVersion')
    self._addParameter(LCSIMAppDefn,"applicationLog","string","","Application log file")
    self._addParameter(LCSIMAppDefn,"outputPath","string","","Output data path")
    self._addParameter(LCSIMAppDefn,"outputFile","string","","output file name")
    self._addParameter(LCSIMAppDefn,'listoutput',"list",[],"list of output file name")    
    self.workflow.addStep(LCSIMAppDefn)
    mstep = self.workflow.createStepInstance('LCSIM_App_Step','LCSIM')
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
    
    return
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
  