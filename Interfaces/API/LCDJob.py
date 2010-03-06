# $HeadURL$
# $Id$
'''
LCDDIRAC.Interfaces.API.LCDJob : equivalent of LHCbJob for LCD group and applications.

Created on Feb 8, 2010

@author: sposs
'''
import os, types, string
from DIRAC.Core.Workflow.Parameter                  import *
from DIRAC.Core.Workflow.Module                     import *
from DIRAC.Core.Workflow.Step                       import *
from DIRAC.Core.Workflow.Workflow                   import *
from DIRAC.Core.Workflow.WorkflowReader             import *
from DIRAC.Interfaces.API.Job                       import Job
#from DIRAC.Core.Utilities.File                      import makeGuid
#from DIRAC.Core.Utilities.List                      import uniqueElements
from DIRAC                                          import gConfig
 
COMPONENT_NAME='/WorkflowLib/API/LCDJob' 

class LCDJob(Job):
  def __init__(self,script=None):
    """Instantiates the Workflow object and some default parameters.
    """
    Job.__init__(self,script)
    self.importLocation = 'LCDDIRAC.Workflow.Modules'
    self.StepCount = 0
  
  def setMokka(self,appVersion,steeringFile,inputStdhep,detectorModel='',nbOfEvents=10000,startFrom=1,dbslice='',logFile=''):
    """Helper function.
       Define Mokka step
       
       steeringFile should be the path to the steering file
       All options files are automatically appended to the job input sandbox
       
       inputStdhep is the path to the stdhep file to read. Can be LFN:

       Example usage:

       >>> job = LCDJob()
       >>> job.setMokka('v00-01',steeringFile='clic01_ILD.steer',inputStdhep=['/lcd/event/data/somedata.stdhep'],nbOfEvents=100,logFile='mokka.log')

       Modified drivers (.so files) should be put in a 'lib' directory and input as inputdata:
       >>> job.setInputData('lib')
       This 'lib' directory will be prepended to LD_LIBRARY_PATH

       @param appVersion: Mokka version
       @type appVersion: string
       @param optionsFiles: Path to steering file
       @type optionsFiles: string or list
       @param inputStdhep: Input stdhep (if a subset of the overall input data for a given job is required)
       @type inputStdhep: single LFN
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
       
    """
    
    kwargs = {'appVersion':appVersion,'steeringFile':steeringFile,'inputStdhep':inputStdhep,'DetectorModel':detectorModel,'NbOfEvents':nbOfEvents,'StartFrom':startFrom,'DBSlice':dbslice,'logFile':logFile}
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
      inputStdhep = inputStdhep.replace("LFN:","")
      self.addToInputData.append(inputStdhep)
      
    if(dbslice):
      if(os.path.exists(dbslice)):
        self.addToInputData.append(dbslice)
      else:
        return self._reportError('Specified DB slice %s does not exist'%dbslice,__name__,**kwargs)

    self.StepCount +=1
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
    currentApp = "Mokka.%s"%appVersion
    swPackages = 'SoftwarePackages'
    description='LCD Software Packages to be installed'
    if not self.workflow.findParameter(swPackages):
      self._addParameter(self.workflow,swPackages,'JDL',currentApp,description)
    else:
      apps = self.workflow.findParameter(swPackages).getValue()
      if not currentApp in string.split(apps,';'):
        apps += ';'+currentApp
      self._addParameter(self.workflow,swPackages,'JDL',apps,description)
    return S_OK()
    
  def setMarlin(self,appVersion,xmlfile,gearfile,inputslcio=None,logFile=''):
    """ Define Marlin step
     Example usage:

      >>> job = LCDJob()
      >>> job.setMarlin("v00-17",xmlfile='myMarlin.xml',gearfile='GearFile.xml',inputslcio='input.slcio')
      
      If personal processors are needed, put them in a 'lib' directory, and do 
      >>> job.setInputData('lib')
      so that they get shipped to the grid site. All contents are prepended in MARLIN_DLL
      
      @param xmlfile: the marlin xml definition
      @type xmlfile: string
      @param gearfile: as the name suggests
      @type gearfile: string
      @param inputslcio: path to input slcio, list of strings or string
      @type inputslcio: string or list      
    """
    kwargs = {'appVersion':appVersion,'XMLFile':xmlfile,'GEARFile':gearfile,'inputslcio':inputslcio,'logFile':logFile}
    if not type(appVersion) in types.StringTypes:
      return self._reportError('Expected string for version',__name__,**kwargs)
    if not type(xmlfile) in types.StringTypes:
      return self._reportError('Expected string for xml file',__name__,**kwargs)
    if not type(gearfile) in types.StringTypes:
      return self._reportError('Expected string for gear file',__name__,**kwargs)
 
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
    
    if os.path.exists(gearfile):
      self.log.verbose('Found specified GEAR file %s'%gearfile)
      self.addToInputSandbox.append(gearfile)
    else:
      return self._reportError('Specified GEAR file %s does not exist' %(gearfile),__name__,**kwargs)

    if(inputslcio):
      if type(inputslcio) in types.StringTypes:
        inputslcio = [inputslcio]
      print "input slcio",inputslcio
      if not type(inputslcio)==type([]):
        return self._reportError('Expected string or list of strings for input slcio file',__name__,**kwargs)
      for i in xrange(len(inputslcio)):
        inputslcio[i] = inputslcio[i].replace('LFN:','')
      #inputslcio = map( lambda x: 'LFN:'+x, inputslcio)
      inputslcioStr = string.join(inputslcio,';') 
      self.addToInputSandbox.append(inputslcioStr)
      
    self.StepCount +=1
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
    self.workflow.addStep(step)
    stepInstance = self.workflow.createStepInstance('Marlin',stepName)
    stepInstance.setValue("applicationVersion",appVersion)
    stepInstance.setValue("applicationLog",logName)
    stepInstance.setValue("inputSlcio",inputslcio)
    stepInstance.setValue("inputXML",xmlfile)
    stepInstance.setValue("inputGEAR",gearfile)
   
    
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
    return S_OK()
    
    
    