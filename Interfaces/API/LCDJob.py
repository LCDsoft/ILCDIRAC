'''
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
from DIRAC.Core.Utilities.File                      import makeGuid
from DIRAC.Core.Utilities.List                      import uniqueElements
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
       Specify application for DIRAC workflows.
       Applications can be Mokka or Marlin
       
       optionFiles should be the path to the steering or xml file
       All options files are automatically appended to the job input sandbox
       
       Input data for application script must be specified here, please note that if this is set at the job level,
       e.g. via setInputData() but not above, input data is not in the scope of the specified application.
       Any input data specified at the step level that is not already specified at the job level is added automatically
       as a requirement for the job.

       Example usage:

       >>> job = LCDJob()
       >>> job.setMokka('v00-01',steeringFile='clic01_ILD.steer',inputStdhep=['/lcd/event/data/somedata.stdhep'],nbOfEvents=100,logFile='mokka.log')

       @param appVersion: Mokka version
       @type appVersion: string
       @param optionsFiles: Path to steering file
       @type optionsFiles: string or list
       @param inputStdhep: Input stdhep (if a subset of the overall input data for a given job is required)
       @type inputStdhep: single LFN or list of LFNs
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
    
    kwargs = {'appVersion':appVersion,'steeringFile':steeringFile,'inputStdhep':inputStdhep,'DetectorModel':detectorModel,'NbOfEvents':nbOfEvents,'StartFrom',startFrom,'DBSlice':dbslice,'logFile':logFile}
    if not type(appVersion) in types.StringTypes:
      return self._reportError('Expected string for version',__name__,**kwargs)
    if not type(steeringFile) in types.StringTypes:
      return self._reportError('Expected string for steering file',__name__,**kwargs)
    if not type(inputStdhep) in types.StringTypes:
      return self._reportError('Expected string for stdhep file',__name__,**kwargs)
    if not type(detectorModel) in types.StringTypes:
      return self._reportError('Expected string for detector model',__name__,**kwargs)
    if not type(nbOfEvents) in types.IntType:
      return self._reportError('Expected int for NbOfEvents',__name__,**kwargs)
    if not type(startFrom) in types.IntType:
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
    step.addParameter(Parameter("stdhepFile","","string","","",False,False,"Name of the stdhep file"))
    step.addParameter(Parameter("detectorModel","","string","","",False,False,"Name of the detector model"))
    step.addParameter(Parameter("numberOfEvents","","int","","",False,False,"Number of events to process"))
    step.addParameter(Parameter("startFrom","","int","","",False,False,"Event in Stdhep file to start from"))
    step.addParameter(Parameter("dbSlice","","string","","",False,False,"Name of the DB slice to use"))
    step.addParameter(Parameter("applicationLog","","string","","",False,False,"Name of the log file of the application"))
    
    self.workflow.addStep(step)
    stepInstance = self.workflow.createStepInstance('Mokka',stepName)
    stepInstance.setValue("applicationVersion",appVersion)
    stepInstance.setValue("stdhepFile",inputStdhep)
    if(detectorModel):
      stepInstance.setValue("detectorModel",detectorModel)
    stepInstance.setValue("numberOfEvents",nbOfEvents)
    stepInstance.setValue("startFrom",startFrom)
    if(dbslice):
      stepInstance.setValue("dbSlice",dbslice)
    stepInstance.setValue("applicationLog",logName)
    currentApp = "Mokka.%s"appVersion
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
    
    
    
    
    
    