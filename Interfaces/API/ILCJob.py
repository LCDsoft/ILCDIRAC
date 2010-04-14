# $HeadURL$
# $Id$
'''
ILCDIRAC.Interfaces.API.ILCJob : equivalent of LHCbJob for ILC group and applications.

Created on Feb 8, 2010

@author: sposs
'''
import string
from DIRAC.Core.Workflow.Parameter                  import *
from DIRAC.Core.Workflow.Module                     import *
from DIRAC.Core.Workflow.Step                       import *
from DIRAC.Core.Workflow.Workflow                   import *
from DIRAC.Core.Workflow.WorkflowReader             import *
from DIRAC.Interfaces.API.Job                       import Job
#from DIRAC.Core.Utilities.File                      import makeGuid
#from DIRAC.Core.Utilities.List                      import uniqueElements
from DIRAC                                          import gConfig
 
COMPONENT_NAME='/WorkflowLib/API/ILCJob' 

class ILCJob(Job):
  def __init__(self,script=None):
    """Instantiates the Workflow object and some default parameters.
    """
    Job.__init__(self,script)
    self.importLocation = 'ILCDIRAC.Workflow.Modules'
    self.StepCount = 0
    self.ioDict = {}
  
  def setMokka(self,appVersion,steeringFile,inputStdhep,detectorModel='',nbOfEvents=10000,startFrom=1,dbslice='',outputFile=None,logFile='',debug=False):
    """Helper function.
       Define Mokka step
       
       steeringFile should be the path to the steering file
       All options files are automatically appended to the job input sandbox
       
       inputStdhep is the path to the stdhep file to read. Can be LFN:

       Example usage:

       >>> job = ILCJob()
       >>> job.setMokka('v00-01',steeringFile='clic01_ILD.steer',inputStdhep=['/lcd/event/data/somedata.stdhep'],nbOfEvents=100,logFile='mokka.log')

       Modified drivers (.so files) should be put in a 'lib' directory and input as inputdata:
       >>> job.setInputData('lib')
       This 'lib' directory will be prepended to LD_LIBRARY_PATH

       @param appVersion: Mokka version
       @type appVersion: string
       @param steeringFile: Path to steering file
       @type steeringFile: string or list
       @param inputStdhep: Input stdhep (if a subset of the overall input data for a given job is required)
       @type inputStdhep: single file
       @param detectorModel: Mokka detector model to use (if different from steering file)
       @type detectorModel: string
       @param nbOfEvents: Number of events to process in Mokka
       @type nbOfEvents: int
       @param startFrom: Event number in the file to start reading from
       @typr startFrom: int
       @param dbslice: MySQL database slice to use different geometry, needed if not standard
       @type dbslice: string
       @param logFile: Optional log file name
       @type logFile: string
       @param debug: By default, change printout level to least verbosity
       @type debug: bool
       
    """
    
    kwargs = {'appVersion':appVersion,'steeringFile':steeringFile,'inputStdhep':inputStdhep,'DetectorModel':detectorModel,'NbOfEvents':nbOfEvents,'StartFrom':startFrom,'outputFile':outputFile,'DBSlice':dbslice,'logFile':logFile,'debug':debug}
    if not type(appVersion) in types.StringTypes:
      return self._reportError('Expected string for version',__name__,**kwargs)
    if not type(steeringFile) in types.StringTypes:
      return self._reportError('Expected string for steering file',__name__,**kwargs)
    if not type(inputStdhep) in types.StringTypes:
      return self._reportError('Expected string for stdhep file',__name__,**kwargs)
    if not type(detectorModel) in types.StringTypes:
      return self._reportError('Expected string for detector model',__name__,**kwargs)
    if not type(nbOfEvents) == types.IntType:
      return self._reportError('Expected int for NbOfEvents',__name__,**kwargs)
    if not type(startFrom) == types.IntType:
      return self._reportError('Expected int for StartFrom',__name__,**kwargs)
    if not type(dbslice) in types.StringTypes:
      return self._reportError('Expected string for DB slice name',__name__,**kwargs)
    if not type(debug) == types.BooleanType:
      return self._reportError('Expected bool for debug',__name__,**kwargs)
 
    self.StepCount +=1
    
    if logFile:
      if type(logFile) in types.StringTypes:
        logName = logFile
      else:
        return self._reportError('Expected string for log file name',__name__,**kwargs)
    else:
      logName = 'Mokka_%s.log' %(appVersion)
    self.addToOutputSandbox.append(logName)
      
    if os.path.exists(steeringFile):
      self.log.verbose('Found specified steering file %s'%steeringFile)
      self.addToInputSandbox.append(steeringFile)
    else:
      return self._reportError('Specified steering file %s does not exist' %(steeringFile),__name__,**kwargs)

    if(inputStdhep):
      #inputStdhep = inputStdhep.replace("LFN:","")
      self.addToInputSandbox.append(inputStdhep)
      
    if(dbslice):
      if(os.path.exists(dbslice)):
        self.addToInputSandbox.append(dbslice)
      else:
        return self._reportError('Specified DB slice %s does not exist'%dbslice,__name__,**kwargs)

    stepName = 'RunMokka'

    
    ##now define MokkaAnalysis
    moduleName = "MokkaAnalysis"
    module = ModuleDefinition(moduleName)
    module.setDescription('Mokka module definition')
    body = 'from %s.%s import %s\n' %(self.importLocation,moduleName,moduleName)
    module.setBody(body)
    step = StepDefinition('Mokka')
    step.addModule(module)
    moduleInstance = step.createModuleInstance('MokkaAnalysis','Mokka')
    step.addParameter(Parameter("applicationVersion","","string","","",False, False, "Application Name"))
    step.addParameter(Parameter("steeringFile","","string","","",False,False,"Name of the steering file"))
    step.addParameter(Parameter("stdhepFile","","string","","",False,False,"Name of the stdhep file"))
    step.addParameter(Parameter("detectorModel","","string","","",False,False,"Name of the detector model"))
    step.addParameter(Parameter("numberOfEvents",10000,"int","","",False,False,"Number of events to process"))
    step.addParameter(Parameter("startFrom",0,"int","","",False,False,"Event in Stdhep file to start from"))
    step.addParameter(Parameter("dbSlice","","string","","",False,False,"Name of the DB slice to use"))
    step.addParameter(Parameter("applicationLog","","string","","",False,False,"Name of the log file of the application"))
    step.addParameter(Parameter("outputFile","","string","","",False,False,"Name of the output file of the application"))
    step.addParameter(Parameter("debug",False,"bool","","",False,False,"Keep debug level as set in input file"))
    
    self.workflow.addStep(step)
    stepInstance = self.workflow.createStepInstance('Mokka',stepName)
    stepInstance.setValue("applicationVersion",appVersion)
    stepInstance.setValue("steeringFile",steeringFile)
    stepInstance.setValue("stdhepFile",inputStdhep)
    if(detectorModel):
      stepInstance.setValue("detectorModel",detectorModel)
    stepInstance.setValue("numberOfEvents",nbOfEvents)
    stepInstance.setValue("startFrom",startFrom)
    if(dbslice):
      stepInstance.setValue("dbSlice",dbslice)
    stepInstance.setValue("applicationLog",logName)
    if(outputFile):
      stepInstance.setValue('outputFile',outputFile)
    stepInstance.setValue('debug',debug)
    currentApp = "Mokka.%s"%appVersion
    swPackages = 'SoftwarePackages'
    description='ILC Software Packages to be installed'
    if not self.workflow.findParameter(swPackages):
      self._addParameter(self.workflow,swPackages,'JDL',currentApp,description)
    else:
      apps = self.workflow.findParameter(swPackages).getValue()
      if not currentApp in string.split(apps,';'):
        apps += ';'+currentApp
      self._addParameter(self.workflow,swPackages,'JDL',apps,description)
    self.ioDict["MokkaStep"]=stepInstance.getName()
    return S_OK()
    
  def setMarlin(self,appVersion,xmlfile,gearfile=None,inputslcio=None,evtstoprocess=None,logFile='',debug=False):
    """ Define Marlin step
     Example usage:

      >>> job = ILCJob()
      >>> job.setMarlin("v00-17",xmlfile='myMarlin.xml',gearfile='GearFile.xml',inputslcio='input.slcio')
      
      If personal processors are needed, put them in a 'lib' directory, and do 
      >>> job.setInputData('lib')
      so that they get shipped to the grid site. All contents are prepended in MARLIN_DLL
      
      @param xmlfile: the marlin xml definition
      @type xmlfile: string
      @param gearfile: as the name suggests, not needed if Mokka is ran before
      @type gearfile: string
      @param inputslcio: path to input slcio, list of strings or string
      @type inputslcio: string or list
      @param evtstoprocess: number of events to process
      @type evtstoprocess: int or string
      @param debug: By default, change printout level to least verbosity
      @type debug: bool
    """
    kwargs = {'appVersion':appVersion,'XMLFile':xmlfile,'GEARFile':gearfile,'inputslcio':inputslcio,'evtstoprocess':evtstoprocess,'logFile':logFile,'debug':debug}
    if not type(appVersion) in types.StringTypes:
      return self._reportError('Expected string for version',__name__,**kwargs)
    if not type(xmlfile) in types.StringTypes:
      return self._reportError('Expected string for xml file',__name__,**kwargs)
    #if not type(gearfile) in types.StringTypes:
    #  return self._reportError('Expected string for gear file',__name__,**kwargs)
    if not type(debug) == types.BooleanType:
      return self._reportError('Expected bool for debug',__name__,**kwargs)
    
    self.StepCount +=1
     
    if logFile:
      if type(logFile) in types.StringTypes:
        logName = logFile
      else:
        return self._reportError('Expected string for log file name',__name__,**kwargs)
    else:
      logName = 'Marlin_%s.log' %(appVersion)
    self.addToOutputSandbox.append(logName)

    if os.path.exists(xmlfile):
      self.log.verbose('Found specified XML file %s'%xmlfile)
      self.addToInputSandbox.append(xmlfile)
    else:
      return self._reportError('Specified XML file %s does not exist' %(xmlfile),__name__,**kwargs)
    if gearfile:
      if os.path.exists(gearfile):
        self.log.verbose('Found specified GEAR file %s'%gearfile)
        self.addToInputSandbox.append(gearfile)
      else:
        return self._reportError('Specified GEAR file %s does not exist' %(gearfile),__name__,**kwargs)
    else:
      if self.ioDict.has_key("MokkaStep"):
        gearfile="GearOutput.xml"
      else:
        return self._reportError('As Mokka do not run before, you need to specify GearFile and have it in the current directory')

    inputslcioStr =''
    if(inputslcio):
      if type(inputslcio) in types.StringTypes:
        inputslcio = [inputslcio]
      if not type(inputslcio)==type([]):
        return self._reportError('Expected string or list of strings for input slcio file',__name__,**kwargs)
      #for i in xrange(len(inputslcio)):
      #  inputslcio[i] = inputslcio[i].replace('LFN:','')
      #inputslcio = map( lambda x: 'LFN:'+x, inputslcio)
      inputslcioStr = string.join(inputslcio,';')
      self.addToInputSandbox.append(inputslcioStr)


    stepName = 'RunMarlin'

    
    ##now define MokkaAnalysis
    moduleName = "MarlinAnalysis"
    module = ModuleDefinition(moduleName)
    module.setDescription('Marlin module definition')
    body = 'from %s.%s import %s\n' %(self.importLocation,moduleName,moduleName)
    module.setBody(body)
    step = StepDefinition('Marlin')
    step.addModule(module)
    moduleInstance = step.createModuleInstance('MarlinAnalysis','Marlin')
    step.addParameter(Parameter("applicationVersion","","string","","",False, False, "Application Name"))
    step.addParameter(Parameter("applicationLog","","string","","",False,False,"Name of the log file of the application"))
    step.addParameter(Parameter("inputXML","","string","","",False,False,"Name of the input XML file"))
    step.addParameter(Parameter("inputGEAR","","string","","",False,False,"Name of the input GEAR file"))
    step.addParameter(Parameter("inputSlcio","","string","","",False,False,"Name of the input slcio file"))
    step.addParameter(Parameter("EvtsToProcess",-1,"int","","",False,False,"Number of events to process"))
    step.addParameter(Parameter("debug",False,"bool","","",False,False,"Number of events to process"))

    self.workflow.addStep(step)
    stepInstance = self.workflow.createStepInstance('Marlin',stepName)
    stepInstance.setValue("applicationVersion",appVersion)
    stepInstance.setValue("applicationLog",logName)
    if(inputslcioStr):
      stepInstance.setValue("inputSlcio",inputslcioStr)
    else:
      if not self.ioDict.has_key("MokkaStep"):
        raise TypeError,'Expected previously defined Mokka step for input data'
      stepInstance.setLink('inputSlcio',self.ioDict["MokkaStep"],'outputFile')
    stepInstance.setValue("inputXML",xmlfile)
    stepInstance.setValue("inputGEAR",gearfile)
    if(evtstoprocess):
      stepInstance.setValue("EvtsToProcess",evtstoprocess)
    else:
      if self.ioDict.has_key(self.StepCount-1):
        stepInstance.setLink('EvtsToProcess',self.ioDict[self.StepCount-1],'numberOfEvents')
      else :
        stepInstance.setValue("EvtsToProcess",-1)
    stepInstance.setValue("debug",debug)
        
    currentApp = "Marlin.%s"%appVersion

    swPackages = 'SoftwarePackages'
    description='LCD Software Packages to be installed'
    if not self.workflow.findParameter(swPackages):
      self._addParameter(self.workflow,swPackages,'JDL',currentApp,description)
    else:
      apps = self.workflow.findParameter(swPackages).getValue()
      if not currentApp in string.split(apps,';'):
        apps += ';'+currentApp
      self._addParameter(self.workflow,swPackages,'JDL',apps,description)
    self.ioDict["MarlinStep"]=stepInstance.getName()
    return S_OK()
    
  def setSLIC(self,appVersion,macFile,inputStdhep,detectorModel,nbOfEvents=10000,startFrom=1,outputFile=None,logFile=''):
    """Helper function.
       Define SLIC step
       
       macFile should be the path to the mac file
       All options files are automatically appended to the job input sandbox
       
       inputStdhep is the path to the stdhep file to read. Can be LFN:

       Example usage:

       >>> job = ILCJob()
       >>> job.setSLIC('v2r8p0',macFile='clic01_SiD.mac',inputStdhep=['/lcd/event/data/somedata.stdhep'],nbOfEvents=100,logFile='slic.log')

       @param appVersion: SLIC version
       @type appVersion: string
       @param macFile: Path to mac file
       @type macFile: string or list
       @param inputStdhep: Input stdhep (if a subset of the overall input data for a given job is required)
       @type inputStdhep: single file
       @param detectorModel: SLIC detector model to use (if different from mac file), must be base name of zip file found on http://lcsim.org/detectors
       @type detectorModel: string
       @param nbOfEvents: Number of events to process in SLIC
       @type nbOfEvents: int
       @param startFrom: Event number in the file to start reading from
       @type startFrom: int
       @param outputFile: Name of the expected output file produced, to be passed to LCSIM
       @type outputFile: string 
       @param logFile: Optional log file name
       @type logFile: string
       
    """
    
    kwargs = {'appVersion':appVersion,'steeringFile':macFile,'inputStdhep':inputStdhep,'DetectorModel':detectorModel,'NbOfEvents':nbOfEvents,'StartFrom':startFrom,'outputFile':outputFile,'logFile':logFile}
    if not type(appVersion) in types.StringTypes:
      return self._reportError('Expected string for version',__name__,**kwargs)
    if not type(macFile) in types.StringTypes:
      return self._reportError('Expected string for mac file',__name__,**kwargs)
    if not type(inputStdhep) in types.StringTypes:
      return self._reportError('Expected string for stdhep file',__name__,**kwargs)
    if not type(detectorModel) in types.StringTypes:
      return self._reportError('Expected string for detector model',__name__,**kwargs)
    if not type(nbOfEvents) == types.IntType:
      return self._reportError('Expected int for NbOfEvents',__name__,**kwargs)
    if not type(startFrom) == types.IntType:
      return self._reportError('Expected int for StartFrom',__name__,**kwargs)
     
    self.StepCount +=1
    
    if logFile:
      if type(logFile) in types.StringTypes:
        logName = logFile
      else:
        return self._reportError('Expected string for log file name',__name__,**kwargs)
    else:
      logName = 'SLIC_%s.log' %(appVersion)
    self.addToOutputSandbox.append(logName)
      
    if os.path.exists(macFile):
      self.log.verbose('Found specified mac file %s'%macFile)
      self.addToInputSandbox.append(macFile)
    else:
      return self._reportError('Specified mac file %s does not exist' %(macFile),__name__,**kwargs)

    if(inputStdhep):
      self.addToInputSandbox.append(inputStdhep)    
    
    detectormodeltouse = os.path.basename(detectorModel).rstrip(".zip")
    if os.path.exists(detectorModel):
      self.addToInputSandbox(detectorModel)

    stepName = 'RunSLIC'

    
    ##now define MokkaAnalysis
    moduleName = "SLICAnalysis"
    module = ModuleDefinition(moduleName)
    module.setDescription('SLIC module definition')
    body = 'from %s.%s import %s\n' %(self.importLocation,moduleName,moduleName)
    module.setBody(body)
    step = StepDefinition('SLIC')
    step.addModule(module)
    moduleInstance = step.createModuleInstance('SLICAnalysis','SLIC')
    step.addParameter(Parameter("applicationVersion","","string","","",False, False, "Application Name"))
    step.addParameter(Parameter("inputmacFile","","string","","",False,False,"Name of the mac file"))
    step.addParameter(Parameter("stdhepFile","","string","","",False,False,"Name of the stdhep file"))
    step.addParameter(Parameter("detectorModel","","string","","",False,False,"Name of the detector model"))
    step.addParameter(Parameter("numberOfEvents",10000,"int","","",False,False,"Number of events to process"))
    step.addParameter(Parameter("startFrom",0,"int","","",False,False,"Event in Stdhep file to start from"))
    step.addParameter(Parameter("applicationLog","","string","","",False,False,"Name of the log file of the application"))
    step.addParameter(Parameter("outputFile","","string","","",False,False,"Name of the output file of the application"))
    
    self.workflow.addStep(step)
    stepInstance = self.workflow.createStepInstance('SLIC',stepName)
    stepInstance.setValue("applicationVersion",appVersion)
    stepInstance.setValue("inputmacFile",macFile)
    stepInstance.setValue("stdhepFile",inputStdhep)
    if(detectorModel):
      stepInstance.setValue("detectorModel",detectormodeltouse)
    stepInstance.setValue("numberOfEvents",nbOfEvents)
    stepInstance.setValue("startFrom",startFrom)
    stepInstance.setValue("applicationLog",logName)
    if(outputFile):
      stepInstance.setValue('outputFile',outputFile)
    currentApp = "SLIC.%s"%appVersion
    swPackages = 'SoftwarePackages'
    description='ILC Software Packages to be installed'
    if not self.workflow.findParameter(swPackages):
      self._addParameter(self.workflow,swPackages,'JDL',currentApp,description)
    else:
      apps = self.workflow.findParameter(swPackages).getValue()
      if not currentApp in string.split(apps,';'):
        apps += ';'+currentApp
      self._addParameter(self.workflow,swPackages,'JDL',apps,description)
    self.ioDict["SLICStep"]=stepInstance.getName()
    return S_OK()
  
  def setLCSIM(self,appVersion,inputXML,inputslcio=None,evtstoprocess=None,logFile=''):
    """Helper function.
       Define LCSIM step
       
       sourceDir should be the path to the source directory used, can be tar ball
       All options files are automatically appended to the job input sandbox
       
       Example usage:

       >>> job = ILCJob()
       >>> job.setLCSIM('',inputXML='mylcsim.lcsim',inputslcio=['LFN:/lcd/event/data/somedata.slcio'],logFile='lcsim.log')

       @param appVersion: LCSIM version
       @type appVersion: string
       @param inputXML: Path to xml file
       @type inputXML: string
       @param inputslcio: path to input slcio, list of strings or string
       @type inputslcio: string or list
       @param logFile: Optional log file name
       @type logFile: string
    """
    kwargs = {'appVersion':appVersion,'inputXML':inputXML,'inputslcio':inputslcio,'evtstoprocess':evtstoprocess,'logFile':logFile}
    if not type(appVersion) in types.StringTypes:
      return self._reportError('Expected string for version',__name__,**kwargs)
    if not type(inputXML) in types.StringTypes:
      return self._reportError('Expected string for source dir',__name__,**kwargs)
    inputslcioStr =''
    if(inputslcio):
      if type(inputslcio) in types.StringTypes:
        inputslcio = [inputslcio]
      if not type(inputslcio)==type([]):
        return self._reportError('Expected string or list of strings for input slcio file',__name__,**kwargs)
      #for i in xrange(len(inputslcio)):
      #  inputslcio[i] = inputslcio[i].replace('LFN:','')
      #inputslcio = map( lambda x: 'LFN:'+x, inputslcio)
      inputslcioStr = string.join(inputslcio,';')
      self.addToInputSandbox.append(inputslcioStr)         

    if logFile:
      if type(logFile) in types.StringTypes:
        logName = logFile
      else:
        return self._reportError('Expected string for log file name',__name__,**kwargs)
    else:
      logName = 'Marlin_%s.log' %(appVersion)
    self.addToOutputSandbox.append(logName)

    self.addToInputSandbox(inputXML)

    self.StepCount +=1
    stepName = 'RunLCSIM'

    
    ##now define LCSIMAnalysis
    moduleName = "LCSIMAnalysis"
    module = ModuleDefinition(moduleName)
    module.setDescription('LCSIM module definition')
    body = 'from %s.%s import %s\n' %(self.importLocation,moduleName,moduleName)
    module.setBody(body)
    step = StepDefinition('LCSIM')
    step.addModule(module)
    moduleInstance = step.createModuleInstance('LCSIMAnalysis','LCSIM')
    step.addParameter(Parameter("applicationVersion","","string","","",False, False, "Application Name"))
    step.addParameter(Parameter("applicationLog","","string","","",False,False,"Name of the log file of the application"))
    step.addParameter(Parameter("inputXML","","string","","",False,False,"Name of the source directory to use"))
    step.addParameter(Parameter("inputSlcio","","string","","",False,False,"Name of the input slcio file"))
    step.addParameter(Parameter("EvtsToProcess",-1,"int","","",False,False,"Number of events to process"))

    self.workflow.addStep(step)
    stepInstance = self.workflow.createStepInstance('LCSIM',stepName)
    stepInstance.setValue("applicationVersion",appVersion)
    stepInstance.setValue("applicationLog",logName)
    stepInstance.setValue("inputXML",inputXML)

    if(inputslcioStr):
      stepInstance.setValue("inputSlcio",inputslcioStr)
    else:
      if not self.ioDict.has_key("SLICStep"):
        raise TypeError,'Expected previously defined SLIC step for input data'
      stepInstance.setLink('inputSlcio',self.ioDict["SLICStep"],'outputFile')
    if(evtstoprocess):
      stepInstance.setValue("EvtsToProcess",evtstoprocess)
    else:
      if self.ioDict.has_key("SLICStep"):
        stepInstance.setLink('EvtsToProcess',self.ioDict["SLICStep"],'numberOfEvents')
      else :
        stepInstance.setValue("EvtsToProcess",-1)
      
    currentApp = "LCSIM.%s"%appVersion

    swPackages = 'SoftwarePackages'
    description='LCD Software Packages to be installed'
    if not self.workflow.findParameter(swPackages):
      self._addParameter(self.workflow,swPackages,'JDL',currentApp,description)
    else:
      apps = self.workflow.findParameter(swPackages).getValue()
      if not currentApp in string.split(apps,';'):
        apps += ';'+currentApp
      self._addParameter(self.workflow,swPackages,'JDL',apps,description)
    self.ioDict["LCSIMStep"]=stepInstance.getName()    
    return S_OK()
 