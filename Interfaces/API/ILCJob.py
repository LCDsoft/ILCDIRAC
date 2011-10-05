# $HeadURL$
# $Id$
"""
  ILCJob : Job definition API for the ILC community

  Inherits from Job class in DIRAC.Interfaces.API.Job.py

  Add ILC specific application support

  See tutorial slides for usage, and this doc for full review of possibilities.

  @since: Feb 8, 2010

  @author: Stephane Poss and Przemyslaw Majewski
"""
from DIRAC.Core.Workflow.Parameter                  import *
from DIRAC.Core.Workflow.Module                     import *
from DIRAC.Core.Workflow.Step                       import *
from DIRAC.Core.Workflow.Workflow                   import *
from DIRAC.Core.Workflow.WorkflowReader             import *
from DIRAC.Interfaces.API.Job                       import Job
#from DIRAC.Core.Utilities.File                      import makeGuid
#from DIRAC.Core.Utilities.List                      import uniqueElements
from DIRAC                                          import gConfig, S_OK

from ILCDIRAC.Core.Utilities.CheckXMLValidity import CheckXMLValidity

import os, types, string, inspect 
from random import randrange

COMPONENT_NAME = '/WorkflowLib/API/ILCJob'

class ILCJob(Job):
  """Main ILC job definition utility

  Each application is configured using specific interface

  The needed files are passed to the L{setInputSandbox} method

  Each application corresponds to a module that is called from the JobAgent, on the worker node. This module is defined below by modulename.
  All available modules can be found in ILCDIRAC.Worflow.Modules.
  """
  def __init__(self, script=None, processlist=None):
    """Instantiates the Workflow object and some default parameters.
    """
    Job.__init__(self, script)
    self.importLocation = 'ILCDIRAC.Workflow.Modules'
    listbansite = gConfig.getValue("/LocalSite/BannedSites", [])
    if len(listbansite):
      self.setBannedSites(listbansite)
    self.StepCount = 0
    self.ioDict = {}
    self.srms = ""
    self.processlist = None
    self.systemConfig = "x86_64-slc5-gcc43-opt"
    self.energy = ''
    self.detector = ''
    if processlist:
      self.processlist = processlist
    else:
      self.log.warn('Process list was not given, limited WHIZARD functionality') 
    self.prodparameters = {}
    
  def _askUser(self):
    """ Method needed to be forward compatible
    """
    return S_OK()    

  def setApplicationScript(self, appName, appVersion, script, arguments=None, log=None, logInOutputData=False):
    """ method needed by Ganga, and also for pyroot
    """
    if log:
      if not logInOutputData:
        self.addToOutputSandbox.append(log)
    if os.path.exists(script):
      self.addToInputSandbox.append(script)

    self.StepCount += 1
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' % (appName, stepNumber)
    self._addParameter(self.workflow, 'TotalSteps', 'String', self.StepCount, 'Total number of steps')

    # Create the GaudiApplication script module first
    moduleName = 'ApplicationScript'
    module = ModuleDefinition(moduleName)
    module.setDescription('An Application script module that can execute any provided script in the given project name and version environment')
    body = 'from %s.%s import %s\n' % (self.importLocation, moduleName, moduleName)
    module.setBody(body)
    #Add user job finalization module
    moduleName = 'UserJobFinalization'
    userData = ModuleDefinition(moduleName)
    userData.setDescription('Uploads user output data files with LHCb specific policies.')
    body = 'from %s.%s import %s\n' % (self.importLocation, moduleName, moduleName)
    userData.setBody(body)
    name = stepDefn
    # Create Step definition
    step = StepDefinition(name)
    step.addModule(module)
    step.addModule(userData)
    step.createModuleInstance('ApplicationScript', name)
    step.createModuleInstance('UserJobFinalization', name)

    # Define step parameters
    step.addParameter(Parameter("applicationName", "", "string", "", "", False, False, "Application Name"))
    step.addParameter(Parameter("applicationVersion", "", "string", "", "", False, False, "Application Name"))
    step.addParameter(Parameter("applicationLog", "", "string", "", "", False, False, "Name of the output file of the application"))
    step.addParameter(Parameter("arguments", "", "string", "", "", False, False, "arguments to pass to script"))
    step.addParameter(Parameter("script", "", "string", "", "", False, False, "Script name"))

    stepName = 'Run%sStep%s' % (appName, stepNumber)

    self.workflow.addStep(step)
    stepPrefix = '%s_' % stepName
    self.currentStepPrefix = stepPrefix

    # Define Step and its variables
    stepInstance = self.workflow.createStepInstance(stepDefn, stepName)

    stepInstance.setValue("applicationName", appName)
    stepInstance.setValue("applicationVersion", appVersion)
    stepInstance.setValue("script", script)
    if arguments:
      stepInstance.setValue("arguments", arguments)
    if log:
      stepInstance.setValue("applicationLog", log)


    currentApp = "%s.%s" % (appName.lower(), appVersion)
    swPackages = 'SoftwarePackages'
    description = 'ILC Software Packages to be installed'
    if not self.workflow.findParameter(swPackages):
      self._addParameter(self.workflow, swPackages, 'JDL', currentApp, description)
    else:
      apps = self.workflow.findParameter(swPackages).getValue()
      if not currentApp in string.split(apps, ';'):
        apps += ';'+currentApp
      self._addParameter(self.workflow, swPackages, 'JDL', apps, description)
    return S_OK()

  def getSRMFile(self, filedict=None):
    """ Helper function
        Retrieve file or list of files based on its SRM path. Must be first step in workflow.

        Example usage:

        >>> job = ILCJob()
        >>> fdict = {"file":"srm://srm-public.cern.ch/castor/cern.ch/grid/ilc/prod/clic/1tev/Z_uds/gen/0/nobeam_nobrem_0-200.stdhep","site":"CERN-SRM"}
        >>> fdict = str(fdict)
        >>> job.getSRMFile(fdict)

        If specified, possible to omit the input files in the next steps

        @param filedict: stringed Dictionary or list of stringed dictionaries
        @type filedict: "{}" or ["{}"]
        @return: S_OK() or S_ERROR()
    """
    kwargs = {"filedict":filedict}
    if not type(filedict) == type("") and not type(filedict) == type([]):
      return self._reportError('Expected string or list of strings for filedict', __name__, **kwargs)

    self.StepCount += 1

    stepName = 'GetSRM'
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' % ('GetSRM', stepNumber)
    self._addParameter(self.workflow, 'TotalSteps', 'String', self.StepCount, 'Total number of steps')
    ##now define MokkaAnalysis
    moduleName = "GetSRMFile"
    module = ModuleDefinition(moduleName)
    module.setDescription('GetSRM module definition')
    body = 'from %s.%s import %s\n' % (self.importLocation, moduleName, moduleName)
    module.setBody(body)

    step = StepDefinition(stepDefn)
    step.addModule(module)
    step.createModuleInstance('GetSRMFile', stepDefn)
    step.addParameter(Parameter("srmfiles", "", "string", "", "", False, False, "list of files to retrieve"))
    self.workflow.addStep(step)
    stepInstance = self.workflow.createStepInstance(stepDefn, stepName)

    files = ""
    if type(filedict) == type(""):
      filedict = [str(filedict)]
    if type(filedict) == type([]):
      files = string.join(filedict,";")

    stepInstance.setValue('srmfiles', files)
    self.srms = files
    self.ioDict["GetSRMStep"] = stepInstance.getName()

    return S_OK()

  def setWhizard(self, process=None, version=None, in_file=None, susymodel=None,
                 nbevts=0, lumi = 0, energy=3000, randomseed=0, extraparameters={},
                 jobindex=None, outputFile=None, logFile=None, logInOutputData=False, 
                 debug=False):
    """Helper function

       Define Whizard step

       Two possibilities:

       1) process is specified

       2) version and in_file are specified

       In the latter case, version is the whizard version to use and the in_file parameter is the path to the whizard.in to use.

       Example usage:

       >>> job = ILCJob()
       >>> job.setWhizard(process="ee_h_bb",nbevts=100)

       @param process: process id, if doers not exist, dirac prints out the known ones
       @type process: string
       @param version: version of whizard to use, e.g. SM or SUSY
       @type version: string
       @param in_file: path to whizard.in to use
       @type in_file: string
       @param susymodel: model of susy to use, can be only chne (charginos neutralino) or slsqhh (slepton, squarks and heavy higgs)
       @param energy: CM energy to use
       @type energy: int
       @param nbevts: number of event to generate
       @type nbevts: int
       @param lumi: luminosity to generate
       @type lumi: int
       @param randomseed: random seed to use. By default using current job ID
       @type randomseed: int
       @param extraparameters: dictionary of parameters for whizard template. e.g. RECOIL for beam_recoil
       @type extraparameters: dict
       @param jobindex: index to add to output file names so that several job can be stored in the same ouput directory
       @type jobindex: int
       @param logFile: log file name. Default is provided
       @type logFile: string
       @param logInOutputData: put the log file in the OutputData, default is False
       @type logInOutputData: bool
       @param debug: print all in stdout, default is False
       @type debug: bool
    """

    kwargs = {"process":process, "version":version, "in_file":in_file, "susymodel":susymodel,
              "randomseed":randomseed, "energy":energy, "lumi":lumi, "nbevts":nbevts,
              "jobindex":jobindex, 'logFile':logFile, "logInOutputData":logInOutputData, "debug":debug}
    if not self.processlist:
      return self._reportError('Process list was not passed, please define job = ILCJob(processlist=dirac.getProcessList()).', 
                               __name__, **kwargs)
    if process:
      if not self.processlist.existsProcess(process)['Value']:
        self.log.error('Process %s does not exist in any whizard version, please contact responsible.' % process)
        self.log.info("Available processes are:")
        self.processlist.printProcesses()
        return self._reportError('Process %s does not exist in any whizard version.'% process, __name__, **kwargs)
      else:
        cspath = self.processlist.getCSPath(process)
        whiz_file = os.path.basename(cspath)
        version = whiz_file.replace(".tar.gz","").replace(".tgz","").replace("whizard","")
        self.log.info("Found process %s corresponding to whizard%s" % (process, version))
    if not version:
      return self._reportError("Version has to be defined somewhere", __name__, **kwargs)

    if not nbevts and not lumi:
      return self._reportError("Nb of evts has to be defined via nbevts or luminosity via lumi", __name__, **kwargs)
    if nbevts and lumi:
      self.log.info('Nb of evts and lumi have been specified, only lumi will be taken into account')

    if susymodel:
      if not susymodel == "slsqhh" and not susymodel == 'chne':
        self._reportError("susymodel must be either slsqhh or chne")

    self.StepCount += 1

    if logFile:
      if type(logFile) in types.StringTypes:
        logName = logFile
      else:
        return self._reportError('Expected string for log file name', __name__, **kwargs)
    else:
      logName = 'Whizard_%s_%s.log' % (version,self.StepCount)
    if not logInOutputData:
      self.addToOutputSandbox.append(logName)
    if in_file:
      if in_file.lower().find("lfn:") > -1:
        self.addToInputSandbox.append(in_file)
      elif os.path.exists(in_file):
        self.addToInputSandbox.append(in_file)
      else:
        return self._reportError('Specified input generator file %s does not exist' % (in_file), __name__, **kwargs)
    parameters = []
    if extraparameters:
      if not type(extraparameters) == type({}):
        return self._reportError('Extraparameter argument must be dictionnary', __name__, **kwargs)
    else:
      extraparameters['PNAME1'] = 'e1'
      print "Assuming incoming beam 1 to be electrons"

    for n, v in extraparameters.items():
      parameters.append("%s=%s" % (n, v))
    if not extraparameters.has_key('PNAME1'):
      print "Assuming incoming beam 1 to be electrons"
      parameters.append('PNAME1=e1')
    if not extraparameters.has_key('PNAME2'):
      print "Assuming incoming beam 2 to be positrons"
      parameters.append('PNAME2=E1')
    if not extraparameters.has_key('POLAB1'):
      print "Assuming no polarization for beam 1"
      parameters.append('POLAB1=0.0 0.0')
    if not extraparameters.has_key('POLAB2'):
      print "Assuming no polarization for beam 2"
      parameters.append('POLAB2=0.0 0.0')
    if not extraparameters.has_key('USERB1'):
      print "Will put beam spectrum to True for beam 1"
      parameters.append('USERB1=T')
    if not extraparameters.has_key('USERB2'):
      print "Will put beam spectrum to True for beam 2"
      parameters.append('USERB2=T')
    if not extraparameters.has_key('ISRB1'):
      print "Will put ISR to True for beam 1"
      parameters.append('ISRB1=T')
    if not extraparameters.has_key('ISRB2'):
      print "Will put ISR to True for beam 2"
      parameters.append('ISRB2=T')
    if not extraparameters.has_key('EPAB1'):
      print "Will put EPA to False for beam 1"
      parameters.append('EPAB1=F')
    if not extraparameters.has_key('EPAB2'):
      print "Will put EPA to False for beam 2"
      parameters.append('EPAB2=F')
    if not extraparameters.has_key('RECOIL'):
      print "Will set Beam_recoil to False"
      parameters.append('RECOIL=F')
    if not extraparameters.has_key('INITIALS'):
      print "Will set keep_initials to False"
      parameters.append('INITIALS=F')
    if not extraparameters.has_key('USERSPECTRUM'):
      print "Will set USER_spectrum_on to +-11"
      parameters.append('USERSPECTRUM=11')

    if self.ioDict.has_key("WhizardStep"):
      randomseed = randrange(1000000)
      jobindex = str(self.StepCount)
      if outputFile:
        base = outputFile.split(".stdhep")[0]
        outputFile = base+"_%s.stdhep"
    
    #Add to input sandbox the processlist: if it fails getting it, the job get rescheduled
    res = gConfig.getValue('/Operations/ProcessList/Location','')
    if not res:
      return self._reportError('Could not resolve location of processlist.cfg')
    res = 'LFN:' + res
    self.addToInputSandbox.append(res)

    stepName = 'RunWhizard'
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' % ('Whizard', stepNumber)
    self._addParameter(self.workflow, 'TotalSteps', 'String', self.StepCount, 'Total number of steps')

    moduleName = "WhizardAnalysis"
    module = ModuleDefinition(moduleName)
    module.setDescription('Whizard module definition')
    body = 'from %s.%s import %s\n' % (self.importLocation, moduleName, moduleName)
    module.setBody(body)
    #Add user job finalization module
    moduleName = 'UserJobFinalization'
    userData = ModuleDefinition(moduleName)
    userData.setDescription('Uploads user output data files with ILC specific policies.')
    body = 'from %s.%s import %s\n' % (self.importLocation, moduleName, moduleName)
    userData.setBody(body)
    step = StepDefinition(stepDefn)
    step.addModule(module)
    step.addModule(userData)
    step.createModuleInstance('WhizardAnalysis', stepDefn)
    step.createModuleInstance('UserJobFinalization', stepDefn)
    step.addParameter(Parameter("applicationVersion", "", "string", "", "", False, False, "Application Name"))
    step.addParameter(Parameter("applicationLog", "", "string", "", "", False, False, "Name of the log file of the application"))
    step.addParameter(Parameter("InputFile", "", "string", "", "", False, False, "Name of the whizard.in file"))
    step.addParameter(Parameter("EvtType", "", "string", "", "", False, False, "Name of the whizard.in file"))
    if energy:
      step.addParameter(Parameter("Energy", 0, "int", "", "", False, False, "Energy to use"))
    if randomseed:
      step.addParameter(Parameter("RandomSeed", 0, "int", "", "", False, False, "Random seed to use"))
    if jobindex:
      step.addParameter(Parameter("JobIndex", "", "string", "", "", False, False, "job index to add in final file name"))
    step.addParameter(Parameter("NbOfEvts", 0, "int", "", "", False, False, "Nb of evts to generated per job"))
    step.addParameter(Parameter("Lumi", 0, "float", "", "", False, False, "Luminosity to  generate per job"))
    step.addParameter(Parameter("debug", False, "bool", "", "", False, False, "Keep debug level as set in input file"))
    step.addParameter(Parameter("outputFile", "", "string", "", "", False, False, "Name of the output file of the application"))
    step.addParameter(Parameter("parameters", "", "string", "", "", False, False, "Parameters for whizard.in"))
    if susymodel:
      step.addParameter(Parameter("SusyModel", 0, "int", "", "", False, False, "SUSY model to use"))

    self.workflow.addStep(step)
    stepInstance = self.workflow.createStepInstance(stepDefn, stepName)
    stepInstance.setValue("applicationVersion", version)
    stepInstance.setValue("applicationLog", logName)
    if in_file:
      stepInstance.setValue("InputFile", in_file)
    if process:
      stepInstance.setValue("EvtType", process)
    if energy:
      stepInstance.setValue("Energy", energy)
    if randomseed:
      stepInstance.setValue("RandomSeed", randomseed)
    if jobindex:
      stepInstance.setValue("JobIndex", jobindex)
    if susymodel:
      if susymodel == 'slsqhh':
        stepInstance.setValue('SusyModel', 1)
      if susymodel == 'chne':
        stepInstance.setValue('SusyModel', 2)
    stepInstance.setValue("NbOfEvts", nbevts)
    stepInstance.setValue("Lumi", lumi)
    stepInstance.setValue("debug", debug)
    if(outputFile):
      stepInstance.setValue('outputFile', outputFile)
    stepInstance.setValue("parameters", string.join(parameters, ";"))

    currentApp = "whizard.%s" % version
    swPackages = 'SoftwarePackages'
    description = 'ILC Software Packages to be installed'
    if not self.workflow.findParameter(swPackages):
      self._addParameter(self.workflow, swPackages, 'JDL', currentApp, description)
    else:
      apps = self.workflow.findParameter(swPackages).getValue()
      if not currentApp in string.split(apps, ';'):
        apps += ';' + currentApp
      self._addParameter(self.workflow, swPackages, 'JDL', apps, description)
    self.ioDict["WhizardStep"] = stepInstance.getName()

    return S_OK()

  def setPythiaStep(self,name,appvers,nbevts,outputFile,logFile=None, logInOutputData=False):
    """ Helper function 
    """
    kwargs = {'name':name,"appvers":appvers,"nbevts":nbevts,"outputFile":outputFile,'logFile':logFile}
    if not nbevts:
      return self._reportError("Number of events has to be specified",__name__,**kwargs)
    if not outputFile:
      return self._reportError("outputFile must be specified",__name__,**kwargs)

    self.StepCount += 1

    if logFile:
      if type(logFile) in types.StringTypes:
        logName = logFile
      else:
        return self._reportError('Expected string for log file name', __name__, **kwargs)
    else:
      logName = 'Pythia_%s_%s.log' % (appvers,self.StepCount)
    if not logInOutputData:
      self.addToOutputSandbox.append(logName)

    
    stepName = 'RunPythia'
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' % ('Pythia', stepNumber)
    self._addParameter(self.workflow, 'TotalSteps', 'String', self.StepCount, 'Total number of steps')

    moduleName = "PythiaAnalysis"
    module = ModuleDefinition(moduleName)
    module.setDescription('Whizard module definition')
    body = 'from %s.%s import %s\n' % (self.importLocation, moduleName, moduleName)
    module.setBody(body)
    #Add user job finalization module
    moduleName = 'UserJobFinalization'
    userData = ModuleDefinition(moduleName)
    userData.setDescription('Uploads user output data files with ILC specific policies.')
    body = 'from %s.%s import %s\n' % (self.importLocation, moduleName, moduleName)
    userData.setBody(body)
    step = StepDefinition(stepDefn)
    step.addModule(module)
    step.addModule(userData)
    step.createModuleInstance('PythiaAnalysis', stepDefn)
    step.createModuleInstance('UserJobFinalization', stepDefn)
    step.addParameter(Parameter("applicationName", "", "string", "", "", False, False, "Application Name"))
    step.addParameter(Parameter("applicationVersion", "", "string", "", "", False, False, "Application version"))
    step.addParameter(Parameter("applicationLog", "", "string", "", "", False, False, "Name of the log file of the application"))
    step.addParameter(Parameter("NbOfEvts", 0, "int", "", "", False, False, "Nb of evts to generated per job"))
    step.addParameter(Parameter("outputFile", "", "string", "", "", False, False, "Name of the output file of the application"))
    self.workflow.addStep(step)
    stepInstance = self.workflow.createStepInstance(stepDefn, stepName)
    stepInstance.setValue("applicationName", name)
    stepInstance.setValue("applicationVersion", appvers)
    stepInstance.setValue("applicationLog", logName)
    stepInstance.setValue("NbOfEvts", nbevts)
    stepInstance.setValue('outputFile', outputFile)
    
    currentApp = "%s.%s" % (name,appvers)
    swPackages = 'SoftwarePackages'
    description = 'ILC Software Packages to be installed'
    if not self.workflow.findParameter(swPackages):
      self._addParameter(self.workflow, swPackages, 'JDL', currentApp, description)
    else:
      apps = self.workflow.findParameter(swPackages).getValue()
      if not currentApp in string.split(apps, ';'):
        apps += ';' + currentApp
      self._addParameter(self.workflow, swPackages, 'JDL', apps, description)
    self.ioDict["WhizardStep"] = stepInstance.getName()

    return S_OK()

  def setPostGenStep(self, appVersion, inputStdhep = "", NbEvts = 0, outputFile = None,
                     logFile = '', logInOutputData = False):
    """ Helper application:
       apply selection at generator level

       @param appVersion: Version of the Post Generation Selection software to use
       @type appVersion: string
       @param inputStdhep: Input stdhep to filter. If whizard is run before, use its output
       @type inputStdhep: string
       @param NbEvts: Number of events to keep
       @type NbEvts: int
       @param outputFile: Name of the output file. By default = inputStdhep
       @type outputFile: string
       @return: S_OK() or S_ERROR()
    """
    kwargs = {"appVersion":appVersion, "inputStdhep":inputStdhep, "NbEvts":NbEvts, "outputFile":outputFile}
    if not type(inputStdhep) in types.StringTypes:
      self._reportError("inputStdhep should be string", __name__, **kwargs)
    if not outputFile:
      outputFile = inputStdhep
    if not NbEvts:
      return self._reportError("Number of events to keep must be specified", __name__, **kwargs)

    self.StepCount += 1

    if logFile:
      if type(logFile) in types.StringTypes:
        logName = logFile
      else:
        return self._reportError('Expected string for log file name', __name__, **kwargs)
    else:
      logName = 'PostGenSel_%s_%s.log' % (appVersion,self.StepCount)
    if not logInOutputData:
      self.addToOutputSandbox.append(logName)

    if inputStdhep:
      if inputStdhep.lower().count("lfn:") or os.path.exists(inputStdhep):
        self.addToInputSandbox.append(inputStdhep)

    stepName = 'RunPostGenSel'
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' % ('PostGenSel', stepNumber)
    self._addParameter(self.workflow, 'TotalSteps', 'String', self.StepCount, 'Total number of steps')

    moduleName = "PostGenSelection"
    module = ModuleDefinition(moduleName)
    module.setDescription('PostGenSelection module definition')
    body = 'from %s.%s import %s\n' % (self.importLocation, moduleName, moduleName)
    module.setBody(body)
    #Add user job finalization module
    moduleName = 'UserJobFinalization'
    userData = ModuleDefinition(moduleName)
    userData.setDescription('Uploads user output data files with ILC specific policies.')
    body = 'from %s.%s import %s\n' % (self.importLocation, moduleName, moduleName)
    userData.setBody(body)
    step = StepDefinition(stepDefn)
    step.addModule(module)
    step.addModule(userData)
    step.createModuleInstance('PostGenSelection', stepDefn)
    step.createModuleInstance('UserJobFinalization', stepDefn)
    step.addParameter(Parameter("applicationVersion", "", "string", "", "", False, False, "Application Name"))
    step.addParameter(Parameter("applicationLog", "", "string", "", "", False, False, "Name of the log file of the application"))
    step.addParameter(Parameter("InputFile", "", "string", "", "", False, False, "Name of the input file"))
    step.addParameter(Parameter("outputFile", "", "string", "", "", False, False, "Name of the output file"))
    step.addParameter(Parameter("NbEvts", 0, "int", "", "", False, False, "Number of events to keep"))

    self.workflow.addStep(step)
    stepInstance = self.workflow.createStepInstance(stepDefn, stepName)
    stepInstance.setValue("applicationVersion", appVersion)
    stepInstance.setValue("applicationLog", logName)
    if inputStdhep:
      stepInstance.setValue('InputFile', inputStdhep)
    else:
      if self.ioDict.has_key("WhizardStep"):
        stepInstance.setLink('InputFile', self.ioDict["WhizardStep"], 'outputFile')
      else:
        return self._reportError("Input STDHEP to filter canot be found")
    stepInstance.setValue('outputFile', outputFile)
    stepInstance.setValue("NbEvts", NbEvts)

    currentApp = "postgensel.%s" % appVersion
    swPackages = 'SoftwarePackages'
    description = 'ILC Software Packages to be installed'
    if not self.workflow.findParameter(swPackages):
      self._addParameter(self.workflow, swPackages, 'JDL', currentApp, description)
    else:
      apps = self.workflow.findParameter(swPackages).getValue()
      if not currentApp in string.split(apps, ';'):
        apps += ';' + currentApp
      self._addParameter(self.workflow, swPackages, 'JDL', apps, description)
    self.ioDict["PostGenSelStep"] = stepInstance.getName()

    return S_OK()

  def setStdhepCut(self, appVersion, cutfile, inputFile = '',outputFile = None, MaxNbEvts = None, logFile = '', debug = False,
                   logInOutputData = False):
    """Helper function
    Call Stdhep cut. 
    
    Should be used after whizard or any other program producing stdhep files.
    
    Example usage:
  
    >>> job = setStdhepCut('v1',"mycutfile.txt")
    
    @param appVersion: Version to use
    @type appVersion: string
    @param cutfile: Cut file to use. Can be LFN.
    @type cutfile: string
    @param inputFile: File to cut on, default is to run on all files found locally.
    @type inputFile: string
    @param outputFile: Output file name, default is same as input if set
    @type outputFile: string
    @param MaxNbEvts: Maximum number of events to retain
    @type MaxNbEvts: int
    
    """

    kwargs = {"appVersion":appVersion,'cutfile':cutfile,'inputFile':inputFile,'outputFile':outputFile,
              "MaxNbEvts":MaxNbEvts, 'logFile':logFile, 'debug':debug,"logInOutputData":logInOutputData}
    
    if not type(appVersion) in types.StringTypes:
      return self._reportError('Expected string for version', __name__, **kwargs)
    if not type(cutfile) in types.StringTypes:
      return self._reportError('Expected string for cutfile', __name__, **kwargs)

    self.StepCount += 1

    if logFile:
      if type(logFile) in types.StringTypes:
        logName = logFile
      else:
        return self._reportError('Expected string for log file name', __name__, **kwargs)
    else:
      logName = 'stdhepCut_%s_%s.log' % (appVersion,self.StepCount)
    if not logInOutputData:
      self.addToOutputSandbox.append(logName)

    if inputFile:
      if os.path.exists(inputFile) or inputFile.lower().count("lfn:"):
        self.addToInputSandbox.append(inputFile)
      else:
        return self._reportError("Input file %s not found"%inputFile, __name__, **kwargs)  

    if os.path.exists(cutfile) or cutfile.lower().count("lfn:"):
      self.addToInputSandbox.append(cutfile)
    else:
      return self._reportError("Cut file %s not found"%cutfile,__name__, **kwargs)

    stepName = 'RunStdHepCut'
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' % ('stdhepCut', stepNumber)
    self._addParameter(self.workflow, 'TotalSteps', 'String', self.StepCount, 'Total number of steps')


    ##now define MokkaAnalysis
    moduleName = "StdHepCut"
    module = ModuleDefinition(moduleName)
    module.setDescription('stdhepCut module definition')
    body = 'from %s.%s import %s\n' % (self.importLocation, moduleName, moduleName)
    module.setBody(body)
    #Add user job finalization module
    moduleName = 'UserJobFinalization'
    userData = ModuleDefinition(moduleName)
    userData.setDescription('Uploads user output data files with ILC specific policies.')
    body = 'from %s.%s import %s\n' % (self.importLocation, moduleName, moduleName)
    userData.setBody(body)
    step = StepDefinition(stepDefn)
    step.addModule(module)
    step.addModule(userData)
    step.createModuleInstance('StdHepCut', stepDefn)
    step.createModuleInstance('UserJobFinalization', stepDefn)
    step.addParameter(Parameter("applicationVersion", "", "string", "", "", False, False, "Application Name"))
    step.addParameter(Parameter("applicationLog",     "", "string", "", "", False, False, "Name of the log file of the application"))
    step.addParameter(Parameter("CutFile",            "", "string", "", "", False, False, "Cut file to use"))
    #step.addParameter(Parameter("inputFile",          "", "string", "", "", False, False, "Name of the input file of the application"))
    step.addParameter(Parameter("outputFile",         "", "string", "", "", False, False, "Name of the output file of the application"))
    step.addParameter(Parameter("MaxNbEvts",           0,    "int", "", "", False, False, "Max nb of events to keep"))
    step.addParameter(Parameter("debug",           False,   "bool", "", "", False, False, "Keep debug level as set in input file"))

    self.workflow.addStep(step)
    stepInstance = self.workflow.createStepInstance(stepDefn, stepName)
    stepInstance.setValue("applicationVersion", appVersion)
    stepInstance.setValue("applicationLog",     logName)
    stepInstance.setValue("CutFile",            cutfile)

    #if inputFile:
    #  stepInstance.setValue("inputFile",inputFile)
    #else:
    #  stepInstance.setLink( 'inputFile', self.ioDict[ "MokkaStep" ], 'outputFile' )
  
    
    if MaxNbEvts:
      stepInstance.setValue('MaxNbEvts',        MaxNbEvts)
    if(outputFile):
      stepInstance.setValue('outputFile',       outputFile)

    stepInstance.setValue('debug', debug)
    currentApp = "stdhepcut.%s" % appVersion
    swPackages = 'SoftwarePackages'
    description = 'ILC Software Packages to be installed'
    if not self.workflow.findParameter(swPackages):
      self._addParameter(self.workflow, swPackages, 'JDL', currentApp, description)
    else:
      apps = self.workflow.findParameter(swPackages).getValue()
      if not currentApp in string.split(apps, ';'):
        apps += ';' + currentApp
      self._addParameter(self.workflow, swPackages, 'JDL', apps, description)
    self.ioDict["StdHepCutStep"] = stepInstance.getName()
        
    return S_OK()

  def setMokka(self, appVersion, steeringFile, inputGenfile=None, macFile = None, detectorModel='',
               nbOfEvents=None, startFrom=0, RandomSeed=None, dbslice='', outputFile=None, processID=None,
               logFile='', debug=False, logInOutputData=False):
    """Helper function.
       Define Mokka step

       steeringFile should be the path to the steering file.

       All options files are automatically appended to the job input sandbox.

       inputGenfile is the path to the generator file to read. Can be LFN:

       Example usage:

       >>> job = ILCJob()
       >>> job.setMokka('v00-01',steeringFile='clic01_ILD.steer',inputGenfile=['LFN:/ilc/some/data/somedata.stdhep'],nbOfEvents=100,logFile='mokka.log')

       If macFile is not specified, nbOfEvents must be.

       Modified drivers (.so files) should be put in a 'lib' directory and input as inputdata:

       >>> job.setInputData('lib')

       This 'lib' directory will be prepended to LD_LIBRARY_PATH

       @param appVersion: Mokka version
       @type appVersion: string
       @param steeringFile: Path to steering file
       @type steeringFile: string or list
       @param inputGenfile: Input generator file
       @type inputGenfile: string
       @param macFile: Input mac file
       @type macFile: string
       @param detectorModel: Mokka detector model to use (if different from steering file)
       @type detectorModel: string
       @param nbOfEvents: Number of events to process in Mokka
       @type nbOfEvents: int
       @param startFrom: Event number in the file to start reading from
       @type startFrom: int
       @param dbslice: MySQL database slice to use different geometry, needed if not standard
       @type dbslice: string
       @param RandomSeed: Seed to use. Not so random if set by user. By default it's the JobID.
       @type RandomSeed: int
       @param outputFile: Name of the output file
       @type outputFile: string
       @param processID: process ID string to set for every event
       @type processID: string
       @param logFile: Optional log file name
       @type logFile: string
       @param debug: By default, change printout level to least verbosity
       @type debug: bool
       @param logInOutputData: If set to False (default) then automatically appended to output sandbox, if True, manually add it to OutputData
       @type logInOutputData: bool
       @return: S_OK() or S_ERROR()
    """

    kwargs = {'appVersion':appVersion, 'steeringFile':steeringFile, 'inputGenfile':inputGenfile,
              'macFile':macFile, 'DetectorModel':detectorModel, 'NbOfEvents':nbOfEvents,
              'StartFrom':startFrom, 'outputFile':outputFile, 'DBSlice':dbslice, "RandomSeed":RandomSeed,
              'processID':processID, 'logFile':logFile, 'debug':debug, 'logInOutputData':logInOutputData}
    if not type(appVersion) in types.StringTypes:
      return self._reportError('Expected string for version', __name__, **kwargs)
    if not type(steeringFile) in types.StringTypes:
      return self._reportError('Expected string for steering file', __name__, **kwargs)
    if inputGenfile:
      if not type(inputGenfile) in types.StringTypes:
        return self._reportError('Expected string for generator file', __name__, **kwargs)
    if macFile:
      if not type(macFile) in types.StringTypes:
        return self._reportError('Expected string for mac file', __name__, **kwargs)
    if not type(detectorModel) in types.StringTypes:
      return self._reportError('Expected string for detector model', __name__, **kwargs)
    if nbOfEvents:
      if not type(nbOfEvents) == types.IntType:
        return self._reportError('Expected int for NbOfEvents', __name__, **kwargs)
    if not type(startFrom) == types.IntType:
      return self._reportError('Expected int for StartFrom', __name__, **kwargs)
    if not type(dbslice) in types.StringTypes:
      return self._reportError('Expected string for DB slice name', __name__, **kwargs)
    if not type(debug) == types.BooleanType:
      return self._reportError('Expected bool for debug', __name__, **kwargs)
    if RandomSeed:
      if not type(RandomSeed) == types.IntType:
        return self._reportError("Expected Int for RandomSeed", __name__, **kwargs)
    if processID:
      if not type(processID) in types.StringTypes:
        return self._reportError('Expected string for processID', __name__, **kwargs)

    self.StepCount += 1

    if logFile:
      if type(logFile) in types.StringTypes:
        logName = logFile
      else:
        return self._reportError('Expected string for log file name', __name__, **kwargs)
    else:
      logName = 'Mokka_%s_%s.log' % (appVersion,self.StepCount)
    if not logInOutputData:
      self.addToOutputSandbox.append(logName)

    if steeringFile:
      if os.path.exists(steeringFile):
        self.log.verbose('Found specified steering file %s' % steeringFile)
        self.addToInputSandbox.append(steeringFile)
      elif steeringFile.lower().find("lfn:") > -1:
        self.log.verbose('Found specified lfn to steering file %s' % steeringFile)
        self.addToInputSandbox.append(steeringFile)
      else:
        return self._reportError('Specified steering file %s does not exist' % (steeringFile), __name__, **kwargs)
    else:
      return self._reportError('Specified steering file %s does not exist' % (steeringFile), __name__, **kwargs)

    srmflag = False
    if(inputGenfile):
      if inputGenfile.lower().find("lfn:") > -1:
        self.addToInputSandbox.append(inputGenfile)
      elif inputGenfile.lower() == "srm":
        self.log.info("Found SRM flag, so will assume getSRMFile to have been called before")
        srmflag = True
      elif os.path.exists(inputGenfile):
        self.addToInputSandbox.append(inputGenfile)
      else:
        return self._reportError('Specified input generator file %s does not exist' % (inputGenfile), __name__, **kwargs)

    if(macFile):
      if os.path.exists(macFile):
        self.addToInputSandbox.append(macFile)
      else:
        return self._reportError('Specified input mac file %s does not exist' % (macFile), __name__, **kwargs)

    if(dbslice):
      if dbslice.lower().find("lfn:") > -1:
        self.addToInputSandbox.append(dbslice)
      elif(os.path.exists(dbslice)):
        self.addToInputSandbox.append(dbslice)
      else:
        return self._reportError('Specified DB slice %s does not exist' % dbslice, __name__, **kwargs)

    #if not inputGenfile and not macFile:
    #  return self._reportError('No generator file nor mac file specified, please check what you want to run',__name__,**kwargs)

    if not macFile:
      if not nbOfEvents:
        return self._reportError("No nbOfEvents specified and no mac file given, please specify either one", __name__, **kwargs )

    stepName = 'RunMokka'
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' % ('Mokka', stepNumber)
    self._addParameter(self.workflow, 'TotalSteps', 'String', self.StepCount, 'Total number of steps')


    ##now define MokkaAnalysis
    moduleName = "MokkaAnalysis"
    module = ModuleDefinition(moduleName)
    module.setDescription('Mokka module definition')
    body = 'from %s.%s import %s\n' % (self.importLocation, moduleName, moduleName)
    module.setBody(body)
    #Add user job finalization module
    moduleName = 'UserJobFinalization'
    userData = ModuleDefinition(moduleName)
    userData.setDescription('Uploads user output data files with ILC specific policies.')
    body = 'from %s.%s import %s\n' % (self.importLocation, moduleName, moduleName)
    userData.setBody(body)
    step = StepDefinition(stepDefn)
    step.addModule(module)
    step.addModule(userData)
    step.createModuleInstance('MokkaAnalysis', stepDefn)
    step.createModuleInstance('UserJobFinalization', stepDefn)
    step.addParameter(Parameter("applicationVersion", "", "string", "", "", False, False, "Application Name"))
    step.addParameter(Parameter("steeringFile", "", "string", "", "", False, False, "Name of the steering file"))
    step.addParameter(Parameter("stdhepFile", "", "string", "", "", False, False, "Name of the stdhep file"))
    step.addParameter(Parameter("macFile", "", "string", "", "", False, False, "Name of the mac file"))
    step.addParameter(Parameter("detectorModel", "", "string", "", "", False, False, "Name of the detector model"))
    step.addParameter(Parameter("numberOfEvents", 10000, "int", "", "", False, False, "Number of events to process"))
    step.addParameter(Parameter("startFrom", 0, "int", "", "", False, False, "Event in Stdhep file to start from"))
    step.addParameter(Parameter("dbSlice", "", "string", "", "", False, False, "Name of the DB slice to use"))
    step.addParameter(Parameter("applicationLog", "", "string", "", "", False, False, "Name of the log file of the application"))
    step.addParameter(Parameter("outputFile", "", "string", "", "", False, False, "Name of the output file of the application"))
    step.addParameter(Parameter("debug", False, "bool", "", "", False, False, "Keep debug level as set in input file"))
    if RandomSeed:
      step.addParameter(Parameter("RandomSeed", 0, "int", "", "", False, False, "RandomSeed to use"))
    if processID: 
      step.addParameter(Parameter("ProcessID", "", "string", "", "", False, False, "Name of the procesSID to set per event"))

    self.workflow.addStep(step)
    stepInstance = self.workflow.createStepInstance(stepDefn, stepName)
    stepInstance.setValue("applicationVersion", appVersion)
    stepInstance.setValue("steeringFile", steeringFile)
    if inputGenfile:
      if not srmflag:
        stepInstance.setValue("stdhepFile", inputGenfile)
      else:
        if not self.ioDict.has_key("GetSRMStep"):
          return self._reportError("Could not find SRM step. Please check that getSRMFile is called before.", __name__, **kwargs)
        else:
          srms = self._sortSRM(self.srms)
          stepInstance.setValue("stdhepFile", srms[0])
    if macFile:
      stepInstance.setValue("macFile", macFile)
    if(detectorModel):
      stepInstance.setValue("detectorModel", detectorModel)
    if nbOfEvents:
      stepInstance.setValue("numberOfEvents", nbOfEvents)
    stepInstance.setValue("startFrom", startFrom)
    if(dbslice):
      stepInstance.setValue("dbSlice", dbslice)
    stepInstance.setValue("applicationLog", logName)
    if(outputFile):
      stepInstance.setValue('outputFile', outputFile)
    if (RandomSeed):
      stepInstance.setValue("RandomSeed", RandomSeed)
    if processID:
      stepInstance.setValue('ProcessID', processID)

    stepInstance.setValue('debug', debug)
    currentApp = "mokka.%s" % appVersion
    swPackages = 'SoftwarePackages'
    description = 'ILC Software Packages to be installed'
    if not self.workflow.findParameter(swPackages):
      self._addParameter(self.workflow, swPackages, 'JDL', currentApp, description)
    else:
      apps = self.workflow.findParameter(swPackages).getValue()
      if not currentApp in string.split(apps, ';'):
        apps += ';' + currentApp
      self._addParameter(self.workflow, swPackages, 'JDL', apps, description)
    self.ioDict["MokkaStep"] = stepInstance.getName()
    return S_OK()

  def setMarlin(self, appVersion, xmlfile, gearfile=None, inputslcio=None,
                evtstoprocess=None, logFile='', debug=False, logInOutputData=False):

    """Helper function: define Marlin step
    
      Example usage:

      >>> job = ILCJob()
      >>> job.setMarlin("v00-17",xmlfile='myMarlin.xml',gearfile='GearFile.xml',inputslcio='input.slcio')

      If personal processors are needed, put them in a 'lib/marlin_dll/' directory, and do

      >>> job.setInputSandbox('lib')

      so that they get shipped to the grid site. All contents are prepended in MARLIN_DLL. 
      Or you can put that lib in a lib.tar.gz archive, and pass that. It will get untarred on site automatically. 

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
      @param logInOutputData: If set to False (default) then automatically appended to output sandbox, if True, manually add it to OutputData
      @type logInOutputData: bool
      @return: S_OK() or S_ERROR()
    """

    # Define appName, used below for setting up step instance
    #---------------------------------------------------------------------------

    appName = 'Marlin'

    # Check the required arguments
    #---------------------------------------------------------------------------

    self._checkArgs( {
      'appVersion'     : types.StringTypes,
      'xmlfile'        : types.StringTypes,
      'debug'          : types.BooleanType
    } )

    # XML Steering file
    #---------------------------------------------------------------------------
    if not xmlfile.lower().count('lfn:'): 
      res = CheckXMLValidity(xmlfile)
      if not res['OK']:
        return self._reportError('XML file %s has this problem: %s' % (xmlfile, res['Message']), __name__)
    else:
      self.log.info('Cannot validate the XML file %s as it is an LFN, check yourself!' % xmlfile)  
    
    self._addFileToInputSandbox( xmlfile, 'Marlin steering file')

    # GEAR file
    #---------------------------------------------------------------------------

    if gearfile:

      self._checkArgs( { 'gearfile' : types.StringTypes } )
      self._addFileToInputSandbox( gearfile, 'GEAR file')

    else:

      if self.ioDict.has_key( "MokkaStep" ):
        gearfile = "GearOutput.xml"

    # SLCIO files
    #---------------------------------------------------------------------------

    inputslcioStr = ''
    srmflag = False

    if inputslcio:

      if type(inputslcio) in types.StringTypes:

        if inputslcio.lower() == "srm":

          self.log.verbose("Will assume SRM file was set in getSRMFile before.")
          srmflag = True

        else:
          inputslcio = [inputslcio]

      if not srmflag:

        self._checkArgs( { 'inputslcio' : types.ListType } )

        inputslcioStr = string.join(inputslcio,';')

        for file in inputslcio:
          self._addFileToInputSandbox( file, 'SLCIO file' )

    # Add count to number of steps

    self.StepCount += 1


    # Log file
    #---------------------------------------------------------------------------

    if logFile:
      self._checkArgs( { 'logFile' : types.StringTypes } )
    else:
      logFile = 'Marlin_%s_%s.log' % ( appVersion, self.StepCount )

    if not logInOutputData:
      self._addFileToOutputSandbox( logFile, 'Log file for application stdout' )

    # Setting up a step instance
    #---------------------------------------------------------------------------
    # 1. Create Step definition
    # 2. Add step parameters
    # 3. Add modules to step
    # 4. Add step to workflow
    # 5. Create Step instance
    # 6. Set variables that are used by the modules
    # 7. Install software
    # 8. Set ioDict to pass parameters to future steps


    #--------------------------
    # 1. Create Step definition

    stepNumber = self.StepCount
    stepDefn   = '%sStep%s'    % ( appName, stepNumber )
    stepName   = 'Run%sStep%s' % ( appName, stepNumber )

    self._addParameter(
      self.workflow,
      'TotalSteps',
      'String',
      self.StepCount,
      'Total number of steps'
    )

    step = StepDefinition( stepDefn )

    #-----------------------
    # 2. Add step parameters

    step.addParameter( Parameter( "applicationVersion", "",    "string", "", "", False, False, "Application Name"))
    step.addParameter( Parameter( "applicationLog",     "",    "string", "", "", False, False, "Name of the log file of the application"))
    step.addParameter( Parameter( "inputXML",           "",    "string", "", "", False, False, "Name of the input XML file"))

    if gearfile:
      step.addParameter( Parameter( "inputGEAR",        "",    "string", "", "", False, False, "Name of the input GEAR file"))

    step.addParameter( Parameter( "inputSlcio",         "",    "string", "", "", False, False, "Name of the input slcio file"))
    step.addParameter( Parameter( "EvtsToProcess",      -1,    "int",    "", "", False, False, "Number of events to process"))
    step.addParameter( Parameter( "debug",              False, "bool",   "", "", False, False, "Number of events to process"))

    #-----------------------
    # 3. Add modules to step

    modules = [
      ['MarlinAnalysis',      'Marlin module definition'],
      ['UserJobFinalization', 'Uploads user output data files with ILC specific policies.']
    ]

    self._addModuleToStep( modules, step, stepDefn )

    #------------------------
    # 4. Add step to workflow

    self.workflow.addStep( step )

    #------------------------
    # 5. Create Step instance

    stepInstance = self.workflow.createStepInstance( stepDefn, stepName )

    #----------------------------------------------
    # 6. Set variables that are used by the modules

    # Application version

    stepInstance.setValue( "applicationVersion", appVersion )

    # Logfile

    if logFile:
      stepInstance.setValue( "applicationLog", logFile )

    # Input SLCIO

    if inputslcioStr :
      stepInstance.setValue( "inputSlcio", inputslcioStr )

    else:

      if not srmflag:

        if self.ioDict.has_key( "MokkaStep" ):
          stepInstance.setLink( 'inputSlcio', self.ioDict[ "MokkaStep" ], 'outputFile' )

      else:

        if not self.ioDict.has_key( "GetSRMStep" ):
          return self._reportError( "Could not find SRM step. Please check that getSRMFile is called before.")
        else:
          srms = self._sortSRM( self.srms )
          stepInstance.setValue( "inputSlcio", string.join( srms, ";" ) )

    # Marlin steering file

    stepInstance.setValue( "inputXML", xmlfile )

    # GEAR file

    if gearfile:
      stepInstance.setValue( "inputGEAR", gearfile )

    # Events to process

    if( evtstoprocess ):
      stepInstance.setValue( "EvtsToProcess", evtstoprocess )
    else:
      if self.ioDict.has_key( "MokkaStep" ):
        stepInstance.setLink( 'EvtsToProcess', self.ioDict[ "MokkaStep" ], 'numberOfEvents' )
      else :
        stepInstance.setValue( "EvtsToProcess", -1 )

    # Debug flag

    stepInstance.setValue("debug", debug)

    #--------------------
    # 7. Install software

    self._addSoftware( 'marlin', appVersion )

    # 8. Set ioDict to pass parameters to future steps

    self.ioDict["MarlinStep"] = stepInstance.getName()

    # Return
    #---------------------------------------------------------------------------

    return S_OK()

  def setSLIC(self, appVersion, macFile, inputGenfile=None, detectorModel='',
              nbOfEvents=10000, startFrom=1, RandomSeed=0, outputFile=None,
              logFile='', debug = False, logInOutputData=False):
    """Helper function.
       Define SLIC step

       macFile should be the path to the mac file

       All options files are automatically appended to the job input sandbox

       inputGenfile is the path to the generator file to read. Can be LFN:

       Example usage:

       >>> job = ILCJob()
       >>> job.setSLIC('v2r8p0',macFile='clic01_SiD.mac',inputGenfile=['LFN:/ilc/some/event/data/somedata.stdhep'],nbOfEvents=100,logFile='slic.log')

       @param appVersion: SLIC version
       @type appVersion: string
       @param macFile: Path to mac file
       @type macFile: string or list
       @param inputGenfile: Input generator file
       @type inputGenfile: string
       @param detectorModel: SLIC detector model to use (if different from mac file), must be base name of zip file found on http://lcsim.org/detectors
       @type detectorModel: string
       @param nbOfEvents: Number of events to process in SLIC
       @type nbOfEvents: int
       @param startFrom: Event number in the file to start reading from
       @type startFrom: int
       @param RandomSeed: Seed to use. Default is JobID
       @type RandomSeed: int
       @param outputFile: Name of the expected output file produced, to be passed to LCSIM
       @type outputFile: string
       @param logFile: Optional log file name
       @type logFile: string
       @param debug: not used yet
       @param logInOutputData: If set to False (default) then automatically appended to output sandbox, if True, manually add it to OutputData
       @type logInOutputData: bool
       @return: S_OK() or S_ERROR()
    """

    kwargs = {'appVersion':appVersion, 'steeringFile':macFile, 'inputGenfile':inputGenfile, 'DetectorModel':detectorModel,
              'NbOfEvents':nbOfEvents, 'StartFrom':startFrom, "RandomSeed":RandomSeed,
              'outputFile':outputFile, 'logFile':logFile,
              'debug':debug, "logInOutputData":logInOutputData}
    if not type(appVersion) in types.StringTypes:
      return self._reportError('Expected string for version', __name__, **kwargs)
    if macFile:
      if not type(macFile) in types.StringTypes:
        return self._reportError('Expected string for mac file', __name__, **kwargs)
    if inputGenfile:
      if not type(inputGenfile) in types.StringTypes:
        return self._reportError('Expected string for generator file', __name__, **kwargs)
    if not type(detectorModel) in types.StringTypes:
      return self._reportError('Expected string for detector model', __name__, **kwargs)
    if not type(nbOfEvents) == types.IntType:
      return self._reportError('Expected int for NbOfEvents', __name__, **kwargs)
    if not type(startFrom) == types.IntType:
      return self._reportError('Expected int for StartFrom', __name__, **kwargs)
    if not type(debug) == types.BooleanType:
      return self._reportError('Expected bool for debug', __name__, **kwargs)

    self.StepCount += 1

    if logFile:
      if type(logFile) in types.StringTypes:
        logName = logFile
      else:
        return self._reportError('Expected string for log file name', __name__, **kwargs)
    else:
      logName = 'SLIC_%s_%s.log' % ( appVersion, self.StepCount )
    if not logInOutputData:
      self.addToOutputSandbox.append(logName)

    if macFile:
      if os.path.exists(macFile):
        self.log.verbose('Found specified mac file %s' % macFile)
        self.addToInputSandbox.append(macFile)
      elif macFile.lower().count("lfn:"):
        self.addToInputSandbox.append(macFile)
      else:
        return self._reportError('Specified mac file %s does not exist' % (macFile), __name__, **kwargs)

    if(inputGenfile):
      if inputGenfile.lower().find("lfn:") > -1:
        self.addToInputSandbox.append(inputGenfile)
      elif os.path.exists(inputGenfile):
        self.addToInputSandbox.append(inputGenfile)
      else:
        return self._reportError("Input generator file %s cannot be found" % (inputGenfile), __name__, **kwargs )

    if not macFile and not inputGenfile:
      return self._reportError("No mac file nor generator file specified, cannot do anything", __name__, **kwargs )

    detectormodeltouse = os.path.basename(detectorModel).rstrip(".zip")
    if os.path.exists(detectorModel):
      self.addToInputSandbox.append(detectorModel)

    stepName = 'RunSLIC'
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' % ('SLIC', stepNumber)
    self._addParameter(self.workflow, 'TotalSteps', 'String', self.StepCount, 'Total number of steps')


    ##now define MokkaAnalysis
    moduleName = "SLICAnalysis"
    module = ModuleDefinition(moduleName)
    module.setDescription('SLIC module definition')
    body = 'from %s.%s import %s\n' % (self.importLocation, moduleName, moduleName)
    module.setBody(body)
    #Add user job finalization module
    moduleName = 'UserJobFinalization'
    userData = ModuleDefinition(moduleName)
    userData.setDescription('Uploads user output data files with ILC specific policies.')
    body = 'from %s.%s import %s\n' % (self.importLocation, moduleName, moduleName)
    userData.setBody(body)
    step = StepDefinition(stepDefn)
    step.addModule(module)
    step.addModule(userData)
    step.createModuleInstance('SLICAnalysis', stepDefn)
    step.createModuleInstance('UserJobFinalization', stepDefn)
    step.addParameter(Parameter("applicationVersion", "", "string", "", "", False, False, "Application Name"))
    step.addParameter(Parameter("inputmacFile",       "", "string", "", "", False, False, "Name of the mac file"))
    step.addParameter(Parameter("stdhepFile",         "", "string", "", "", False, False, "Name of the stdhep file"))
    step.addParameter(Parameter("detectorModel",      "", "string", "", "", False, False, "Name of the detector model"))
    step.addParameter(Parameter("numberOfEvents",  10000,    "int", "", "", False, False, "Number of events to process"))
    step.addParameter(Parameter("startFrom",           0,    "int", "", "", False, False, "Event in Stdhep file to start from"))
    step.addParameter(Parameter("applicationLog",     "", "string", "", "", False, False, "Name of the log file of the application"))
    step.addParameter(Parameter("outputFile",         "", "string", "", "", False, False, "Name of the output file of the application"))
    step.addParameter(Parameter("debug",           False,   "bool", "", "", False, False, "Number of events to process"))
    if RandomSeed:
      step.addParameter(Parameter("RandomSeed",        0,    "int", "", "", False, False, "RandomSeed to use"))

    self.workflow.addStep(step)
    stepInstance = self.workflow.createStepInstance(stepDefn, stepName)
    stepInstance.setValue("applicationVersion", appVersion)
    if macFile:
      stepInstance.setValue("inputmacFile", macFile)
    if inputGenfile:
      stepInstance.setValue("stdhepFile", inputGenfile)
    if(detectorModel):
      stepInstance.setValue("detectorModel", detectormodeltouse)
    stepInstance.setValue("numberOfEvents", nbOfEvents)
    stepInstance.setValue("startFrom", startFrom)
    if (RandomSeed):
      stepInstance.setValue("RandomSeed", RandomSeed)

    stepInstance.setValue("applicationLog", logName)
    stepInstance.setValue("debug", debug)
    if(outputFile):
      stepInstance.setValue('outputFile', outputFile)
    currentApp = "slic.%s" % appVersion
    swPackages = 'SoftwarePackages'
    description = 'ILC Software Packages to be installed'
    if not self.workflow.findParameter(swPackages):
      self._addParameter(self.workflow, swPackages, 'JDL', currentApp, description)
    else:
      apps = self.workflow.findParameter(swPackages).getValue()
      if not currentApp in string.split(apps, ';'):
        apps += ';' + currentApp
      self._addParameter(self.workflow, swPackages, 'JDL', apps, description)
    self.ioDict["SLICStep"] = stepInstance.getName()
    return S_OK()

  def setLCSIM(self, appVersion, xmlfile, inputslcio=None, evtstoprocess=None, aliasproperties = None,
               outputFile = "", extraparams='', outputRECFile="", outputDSTFile= "", logFile='',
               debug = False, logInOutputData=False):
    """Helper function.
       Define LCSIM step

       sourceDir should be the path to the source directory used, can be tar ball

       All options files are automatically appended to the job input sandbox

       Example usage:

       >>> job = ILCJob()
       >>> job.setLCSIM('',inputXML='mylcsim.lcsim',inputslcio=['LFN:/lcd/event/data/somedata.slcio'],logFile='lcsim.log')

       @param appVersion: LCSIM version
       @type appVersion: string
       @param xmlfile: Path to xml file
       @type xmlfile: string
       @param inputslcio: path to input slcio, list of strings or string
       @type inputslcio: string or list
       @param aliasproperties: Path to the alias.properties file name that will be used
       @type aliasproperties: string
       @param logFile: Optional log file name
       @type logFile: string
       @param extraparams: Command line parameter to pass to java
       @type extraparams: string
       @param debug: set to True to have verbosity set to 1
       @type debug: bool
       @param logInOutputData: If set to False (default) then automatically appended to output sandbox, if True, manually add it to OutputData
       @type logInOutputData: bool
       @return: S_OK() or S_ERROR()
    """
    kwargs = {'appVersion':appVersion, 'xmlfile':xmlfile, 'inputslcio':inputslcio, 
              'evtstoprocess':evtstoprocess, "aliasproperties":aliasproperties,
              'logFile':logFile, "outputFile":outputFile, 'extraparams':extraparams,"outputRECFile":outputRECFile,
              "outputDSTFile":outputDSTFile, 'debug':debug, 'logInOutputData':logInOutputData}
    if not type(appVersion) in types.StringTypes:
      return self._reportError('Expected string for version', __name__, **kwargs)
    if not type(xmlfile) in types.StringTypes:
      return self._reportError('Expected string for XML file dir', __name__, **kwargs)
    inputslcioStr = ''
    if(inputslcio):
      if type(inputslcio) in types.StringTypes:
        inputslcio = [inputslcio]
      if not type(inputslcio) == type([]):
        return self._reportError('Expected string or list of strings for input slcio file', __name__, **kwargs)
      #for i in xrange(len(inputslcio)):
      #  inputslcio[i] = inputslcio[i].replace('LFN:','')
      #inputslcio = map( lambda x: 'LFN:'+x, inputslcio)
      inputslcioStr = string.join(inputslcio, ';')
      self.addToInputSandbox.append(inputslcioStr)
    if not type(debug) == types.BooleanType:
      return self._reportError('Expected bool for debug', __name__, **kwargs)

    if aliasproperties:
      if not type(aliasproperties) in types.StringTypes:
        return self._reportError('Expected string for alias properties file', __name__, **kwargs)
      elif aliasproperties.lower().find("lfn:"):
        self.addToInputSandbox.append(aliasproperties)
      elif os.path.exists(aliasproperties):
        self.addToInputSandbox.append(aliasproperties)
      else:
        return self._reportError("Could not find alias properties files specified %s" % (aliasproperties), __name__, **kwargs)

      
    if not xmlfile.lower().count('lfn:'):  
      res = CheckXMLValidity(xmlfile)
      if not res['OK']:
        return self._reportError('XML file %s has this problem: %s' % (xmlfile, res['Message']), __name__)
    else:
      self.log.info('Cannot check validity of file %s as it is an LFN' % xmlfile)

    if os.path.exists(xmlfile) or xmlfile.lower().count('lfn:'):
      self.addToInputSandbox.append(xmlfile)
    else:
      return self._reportError("Cannot find specified input xml file %s, please fix !" % (xmlfile), __name__, **kwargs)

    self.StepCount += 1
    stepName = 'RunLCSIM'
    stepNumber = self.StepCount

    if logFile:
      if type(logFile) in types.StringTypes:
        logName = logFile
      else:
        return self._reportError('Expected string for log file name', __name__, **kwargs)
    else:
      logName = 'LCSIM_%s_%s.log' % (appVersion,stepNumber)
    if not logInOutputData:
      self.addToOutputSandbox.append(logName)
    
    
    stepDefn = '%sStep%s' % ('LCSIM', stepNumber)
    self._addParameter(self.workflow, 'TotalSteps', 'String', self.StepCount, 'Total number of steps')


    ##now define LCSIMAnalysis
    moduleName = "LCSIMAnalysis"
    module = ModuleDefinition(moduleName)
    module.setDescription('LCSIM module definition')
    body = 'from %s.%s import %s\n' % (self.importLocation, moduleName, moduleName)
    module.setBody(body)
    #Add user job finalization module
    moduleName = 'UserJobFinalization'
    userData = ModuleDefinition(moduleName)
    userData.setDescription('Uploads user output data files with ILC specific policies.')
    body = 'from %s.%s import %s\n' % (self.importLocation, moduleName, moduleName)
    userData.setBody(body)
    step = StepDefinition(stepDefn)
    step.addModule(module)
    step.addModule(userData)
    step.createModuleInstance('LCSIMAnalysis', stepDefn)
    step.createModuleInstance('UserJobFinalization', stepDefn)
    step.addParameter(Parameter("applicationVersion", "", "string", "", "", False, False, "Application Name"))
    step.addParameter(Parameter("applicationLog",     "", "string", "", "", False, False, "Name of the log file of the application"))
    step.addParameter(Parameter("inputXML",           "", "string", "", "", False, False, "Name of the source directory to use"))
    step.addParameter(Parameter("inputSlcio",         "", "string", "", "", False, False, "Name of the input slcio file"))
    step.addParameter(Parameter("aliasproperties",    "", "string", "", "", False, False, "Name of the alias properties file"))
    if extraparams:
      step.addParameter(Parameter("ExtraParams",        "", "string", "", "", False, False, "Name of the alias properties file"))
    step.addParameter(Parameter("EvtsToProcess",      -1,    "int", "", "", False, False, "Number of events to process"))
    if outputFile:
      step.addParameter(Parameter("outputFile",       "", "string", "", "", False, False, "Name of the output file of the application"))
    if outputRECFile:
      step.addParameter(Parameter("outputREC",    "", "string", "", "", False, False, "Name of the output REC file of the application"))
    if outputDSTFile:
      step.addParameter(Parameter("outputDST",    "", "string", "", "", False, False, "Name of the output DST file of the application"))

    step.addParameter(Parameter("debug",           False,   "bool", "", "", False, False, "Number of events to process"))

    self.workflow.addStep(step)
    stepInstance = self.workflow.createStepInstance(stepDefn, stepName)
    stepInstance.setValue("applicationVersion", appVersion)
    stepInstance.setValue("applicationLog", logName)
    stepInstance.setValue("inputXML", xmlfile)
    stepInstance.setValue("debug", debug)
    if(outputFile):
      stepInstance.setValue('outputFile', outputFile)
    if(outputRECFile):
      stepInstance.setValue('outputREC', outputRECFile)
    if(outputDSTFile):
      stepInstance.setValue('outputDST', outputDSTFile)
    if aliasproperties:
      stepInstance.setValue("aliasproperties", aliasproperties)
    
    if extraparams:
      stepInstance.setValue('ExtraParams',extraparams)  
        
    if(inputslcioStr):
      stepInstance.setValue("inputSlcio", inputslcioStr)
    else:
      if self.ioDict.has_key("SLICPandoraStep"):
        stepInstance.setLink('inputSlcio', self.ioDict["SLICPandoraStep"], 'outputFile')
      elif self.ioDict.has_key("SLICStep"):
        stepInstance.setLink('inputSlcio', self.ioDict["SLICStep"], 'outputFile')
      else:
        raise TypeError, 'Expected previously defined SLIC step or SLICPandora step for input data'

    if(evtstoprocess):
      stepInstance.setValue("EvtsToProcess", evtstoprocess)
    else:
      if self.ioDict.has_key("SLICStep"):
        stepInstance.setLink('EvtsToProcess', self.ioDict["SLICStep"], 'numberOfEvents')
      else :
        stepInstance.setValue("EvtsToProcess", -1)

    currentApp = "lcsim.%s" % appVersion

    swPackages = 'SoftwarePackages'
    description = 'ILC Software Packages to be installed'
    if not self.workflow.findParameter(swPackages):
      self._addParameter(self.workflow, swPackages, 'JDL', currentApp, description)
    else:
      apps = self.workflow.findParameter(swPackages).getValue()
      if not currentApp in string.split(apps, ';'):
        apps += ';' + currentApp
      self._addParameter(self.workflow, swPackages, 'JDL', apps, description)
    self.ioDict["LCSIMStep"] = stepInstance.getName()
    return S_OK()

  def setSLICPandora(self, appVersion, detectorgeo = None, pandorasettings = None, 
                     inputslcio = None, nbevts= 0, outputFile = "", logFile='',
                     debug = False, logInOutputData=False):
    """Helper function.
       Define SLICPandora step

       All options files are automatically appended to the job input sandbox

       Example usage:

       >>> job = ILCJob()
       >>> job.setSLICPandora('',detectorxml='mydetector.xml',inputslcio=['LFN:/lcd/event/data/somedata.slcio'])

       @param appVersion: SLICPandora version
       @type appVersion: string
       @param detectorgeo: Path to detector xml file. Like SLIC step: will download the detector model if needed. Can be path to xml or detector model name
       @type detectorgeo: string
       @param inputslcio: path to input slcio, list of strings or string
       @type inputslcio: string or list
       @param pandorasettings: Path to the pandora settings file name that will be used
       @type pandorasettings: string
       @param logFile: Optional log file name
       @type logFile: string
       @param debug: set to True to have verbosity set to 1
       @type debug: bool
       @param logInOutputData: If set to False (default) then automatically appended to output sandbox, if True, manually add it to OutputData
       @type logInOutputData: bool
       @return: S_OK() or S_ERROR()
    """
    kwargs = {'appVersion':appVersion, "detectorgeo":detectorgeo, "inputslcio":inputslcio,
              "nbevts":nbevts, "outputFile":outputFile, "logFile":logFile,
              "debug":debug, "logInOutputData":logInOutputData}
    if pandorasettings:
      if not type(pandorasettings) in types.StringTypes:
        return self._reportError('Expected string for PandoraSettings xml file', __name__, **kwargs)
      if pandorasettings.lower().count("lfn:"):
        self.addToInputSandbox.append(pandorasettings)
      elif os.path.exists(pandorasettings):
        self.addToInputSandbox.append(pandorasettings)
      else:
        return self._reportError("Could not find PandoraSettings xml files specified %s" % (pandorasettings), __name__, **kwargs)

    if detectorgeo:
      if not type(detectorgeo) in types.StringTypes:
        return self._reportError('Expected string for detector xml file', __name__, **kwargs)
      if detectorgeo.lower().count("lfn:"):
        self.addToInputSandbox.append(detectorgeo)
      elif detectorgeo.lower().count(".xml") and  os.path.exists(detectorgeo):
        self.addToInputSandbox.append(detectorgeo)
      #else:
      #  return self._reportError("Could not find detector xml files specified %s"%(detectorgeo), __name__,**kwargs)

    if inputslcio:
      if not type(inputslcio) in types.StringTypes:
        return self._reportError('Expected string for inputslcio file', __name__, **kwargs)
      elif inputslcio.lower().count("lfn:") or os.path.exists(inputslcio):
        self.addToInputSandbox.append(inputslcio)
      else:
        return self._reportError("Could not find inputslcio file specified %s" % (pandorasettings), __name__, **kwargs)


    if not type(debug) == types.BooleanType:
      return self._reportError('Expected bool for debug', __name__, **kwargs)

    self.StepCount += 1

    if logFile:
      if type(logFile) in types.StringTypes:
        logName = logFile
      else:
        return self._reportError('Expected string for log file name', __name__, **kwargs)
    else:
      logName = 'SLICPandora_%s_%s.log' % (appVersion,self.StepCount)
    if not logInOutputData:
      self.addToOutputSandbox.append(logName)

    stepName = 'RunSLICPandora'
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' % ('SLICPandora', stepNumber)
    self._addParameter(self.workflow, 'TotalSteps', 'String', self.StepCount, 'Total number of steps')


    moduleName = "SLICPandoraAnalysis"
    module = ModuleDefinition(moduleName)
    module.setDescription('SLICPandora module definition')
    body = 'from %s.%s import %s\n' % (self.importLocation, moduleName, moduleName)
    module.setBody(body)
    #Add user job finalization module
    moduleName = 'UserJobFinalization'
    userData = ModuleDefinition(moduleName)
    userData.setDescription('Uploads user output data files with ILC specific policies.')
    body = 'from %s.%s import %s\n' % (self.importLocation, moduleName, moduleName)
    userData.setBody(body)
    step = StepDefinition(stepDefn)
    step.addModule(module)
    step.addModule(userData)
    step.createModuleInstance('SLICPandoraAnalysis', stepDefn)
    step.createModuleInstance('UserJobFinalization', stepDefn)
    step.addParameter(Parameter("applicationVersion", "", "string", "", "", False, False, "Application Name"))
    step.addParameter(Parameter("applicationLog",     "", "string", "", "", False, False, "Name of the log file of the application"))
    step.addParameter(Parameter("DetectorXML",        "", "string", "", "", False, False, "Name of the detector xml to use"))
    step.addParameter(Parameter("inputSlcio",         "", "string", "", "", False, False, "Name of the input slcio file"))
    step.addParameter(Parameter("PandoraSettings",    "", "string", "", "", False, False, "Name of the PandoraSettings file"))
    step.addParameter(Parameter("EvtsToProcess",      -1,    "int", "", "", False, False, "Number of events to process"))
    step.addParameter(Parameter("debug",           False,   "bool", "", "", False, False, "Number of events to process"))
    step.addParameter(Parameter("outputFile",         "", "string", "", "", False, False, "Name of the output file of the application"))

    self.workflow.addStep(step)
    stepInstance = self.workflow.createStepInstance(stepDefn, stepName)
    stepInstance.setValue("applicationVersion", appVersion)
    stepInstance.setValue("applicationLog", logName)
    if detectorgeo:
      stepInstance.setValue("DetectorXML", detectorgeo)
    elif self.ioDict.has_key("SLICStep"):
      stepInstance.setLink('DetectorXML', self.ioDict["SLICStep"], 'detectorModel')
    stepInstance.setValue("debug", debug)

    if(pandorasettings):
      stepInstance.setValue("PandoraSettings", pandorasettings)

    if(inputslcio):
      stepInstance.setValue("inputSlcio", inputslcio)
    else:
      if not self.ioDict.has_key("LCSIMStep"):
        raise TypeError, 'Expected previously defined LCSIM step for input data'
      stepInstance.setLink('inputSlcio', self.ioDict["LCSIMStep"], 'outputFile')

    if(nbevts):
      stepInstance.setValue("EvtsToProcess", nbevts)
    elif self.ioDict.has_key("LCSIMStep"):
        stepInstance.setLink('EvtsToProcess', self.ioDict["LCSIMStep"], 'EvtsToProcess')
    else :
      stepInstance.setValue("EvtsToProcess", -1)
    if(outputFile):
      stepInstance.setValue('outputFile', outputFile)

    currentApp = "slicpandora.%s" % appVersion

    swPackages = 'SoftwarePackages'
    description = 'ILC Software Packages to be installed'
    if not self.workflow.findParameter(swPackages):
      self._addParameter(self.workflow, swPackages, 'JDL', currentApp, description)
    else:
      apps = self.workflow.findParameter(swPackages).getValue()
      if not currentApp in string.split(apps, ';'):
        apps += ';' + currentApp
      self._addParameter(self.workflow, swPackages, 'JDL', apps, description)
    self.ioDict["SLICPandoraStep"] = stepInstance.getName()
    return S_OK()

  def addOverlay(self, detector='', energy='', BXOverlay=0, NbGGtoHadInts=0, ProdID=0, NSigEventsPerJob = 0):
    """ Helper call to define Overlay processor/driver inputs

    @param detector: Detector type to use (ILD or SID). Obtained in prod context via the metadata lookup
    @type detector: string
    @param energy: Energy to use (usually 3tev). In production context, obtained from metadata input
    @type energy: string
    @param BXOverlay: Bunch crossings to overlay
    @type BXOverlay: int
    @param NbGGtoHadInts: optional number of gamma gamma -> hadrons interactions per bunch crossing, default is 3.2
    @type NbGGtoHadInts: float
    @param ProdID: Optional parameter to force using one specific prodID for the input files. By default it's the latest one
    @type ProdID: int
    @param NSigEventsPerJob: Number of signal events per job
    @type NSigEventsPerJob: int
    """
    kwargs = {"detector":detector, "energy":energy, 'BXOverlay':BXOverlay, "NbGGtoHadInts":NbGGtoHadInts,
              'ProdID':ProdID, 'NSigEventsPerJob':NSigEventsPerJob}
    if detector:
      self.detector = detector
    if not self.detector:
      return self._reportError('Detector type (ILD or SID) must be specified somewhere')
    elif self.detector not in ('ILD', 'SID'):
      return self._reportError('Detector Model must be either ILD or SID')
    if energy:
      if not type(energy) in types.StringTypes:
        return self._reportError('Energy type must be string', __name__, **kwargs)
      self.energy = energy
    if not self.energy:
      return self._reportError('Energy must be set somewhere')
    if not BXOverlay:
      return self._reportError('BXOverlay parameter must be set')

    self.StepCount += 1
    stepName = 'RunOverlay'
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' % ('OverlayInput', stepNumber)
    self._addParameter(self.workflow, 'TotalSteps', 'String', self.StepCount, 'Total number of steps')

    moduleName = "OverlayInput"
    module = ModuleDefinition(moduleName)
    module.setDescription('OverlayInput module definition')
    body = 'from %s.%s import %s\n' % (self.importLocation, moduleName, moduleName)
    module.setBody(body)
    step = StepDefinition(stepDefn)
    step.addModule(module)
    step.createModuleInstance('OverlayInput', stepDefn)
    step.addParameter(Parameter("Detector",         "", "string", "", "", False, False, "Name of the detector to use"))
    step.addParameter(Parameter("Energy",           "", "string", "", "", False, False, "Energy to use"))
    step.addParameter(Parameter("BXOverlay",        -1,    "int", "", "", False, False, "Number of BX to overlay"))
    if NbGGtoHadInts:
      step.addParameter(Parameter("ggtohadint",      0,  "float", "", "", False, False, "gamma gamma ints per BX"))
    if ProdID:
      step.addParameter(Parameter("ProdID",          0,    "int", "", "", False, False, "ProdID for input"))

    if NSigEventsPerJob:
      step.addParameter(Parameter("NbSigEvtsPerJob", 0,    "int", "", "", False, False, "Number of signal events per job"))
    self.workflow.addStep(step)
    stepInstance = self.workflow.createStepInstance(stepDefn, stepName)
    stepInstance.setValue("Detector",     self.detector)
    stepInstance.setValue("Energy",       self.energy)
    stepInstance.setValue("BXOverlay",    BXOverlay)
    if NbGGtoHadInts:
      stepInstance.setValue("ggtohadint", NbGGtoHadInts)
    if ProdID:
      stepInstance.setValue("ProdID",     ProdID)
    if NSigEventsPerJob:
      stepInstance.setValue("NbSigEvtsPerJob", NSigEventsPerJob)

    ##Define prod parameters
    self.prodparameters['BXOverlay'] = BXOverlay
    if NbGGtoHadInts:
      self.prodparameters['GGInt'] = NbGGtoHadInts

    return S_OK()

  def setRootAppli(self, appVersion, scriptpath, args=None, logFile='', logInOutputData=False):
    """Define root macro or executable execution

    Example usage:

    >>> job = ILCJob()
    >>> job.setRootAppli('v5.26',scriptpath="myscript.C",args="34,56,\\\\\\\"a_string\\\\\\\"}")

    Mind the escape characters \ so that the quotes are properly used. If not that many \ the
    call

    root -b -q myscript.C\\(34,56,\\"a_string\\"\\)

    will fail

    @param appVersion: ROOT version to use
    @type appVersion: string
    @param scriptpath: path to macro file or executable
    @type scriptpath: string
    @param args: arguments to pass to the macro or executable
    @type args: string
    @return: S_OK,S_ERROR

    """
    kwargs = {'appVersion':appVersion, "macropath":scriptpath, "args":args, "logFile":logFile, "logInOutputData":logInOutputData}

    if not type(appVersion) in types.StringTypes:
      return self._reportError('Expected string for version', __name__, **kwargs)
    if not type(scriptpath) in types.StringTypes:
      return self._reportError('Expected string for macro path', __name__, **kwargs)
    if args:
      if not type(args) in types.StringTypes:
        return self._reportError('Expected string for arguments', __name__, **kwargs)

    if scriptpath.find("lfn:") > -1:
      self.addToInputSandbox.append(scriptpath)
    elif os.path.exists(scriptpath):
      self.addToInputSandbox.append(scriptpath)
    else:
      return self._reportError("Could not find specified macro %s" % scriptpath, __name__, **kwargs)

    self.StepCount += 1

    if logFile:
      if type(logFile) in types.StringTypes:
        logName = logFile
      else:
        return self._reportError('Expected string for log file name', __name__, **kwargs)
    else:
      logName = 'ROOT_%s_%s.log' % (appVersion,self.StepCount)
    if not logInOutputData:
      self.addToOutputSandbox.append(logName)

    stepName = 'RunRootMacro'
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' %('RootMacro', stepNumber)
    self._addParameter(self.workflow, 'TotalSteps', 'String', self.StepCount, 'Total number of steps')


    rootmoduleName = self._rootType(scriptpath)#"RootMacroAnalysis"
    module = ModuleDefinition(rootmoduleName)
    module.setDescription('Root Macro module definition')
    body = 'from %s.%s import %s\n' % (self.importLocation, rootmoduleName, rootmoduleName)
    module.setBody(body)
    #Add user job finalization module
    moduleName = 'UserJobFinalization'
    userData = ModuleDefinition(moduleName)
    userData.setDescription('Uploads user output data files with ILC specific policies.')
    body = 'from %s.%s import %s\n' % (self.importLocation, moduleName, moduleName)
    userData.setBody(body)
    step = StepDefinition(stepDefn)
    step.addModule(module)
    step.addModule(userData)
    step.createModuleInstance(rootmoduleName, stepDefn)
    step.createModuleInstance('UserJobFinalization', stepDefn)
    step.addParameter(Parameter("applicationVersion",    "", "string", "", "", False, False, "Application Name"))
    step.addParameter(Parameter("applicationLog",        "", "string", "", "", False, False, "Name of the log file of the application"))
    step.addParameter(Parameter("script",                "", "string", "", "", False, False, "Name of the source directory to use"))
    step.addParameter(Parameter("args",                  "", "string", "", "", False, False, "Name of the input slcio file"))

    self.workflow.addStep(step)
    stepInstance = self.workflow.createStepInstance(stepDefn, stepName)
    stepInstance.setValue("applicationVersion", appVersion)
    stepInstance.setValue("applicationLog",     logName)
    stepInstance.setValue("script",             scriptpath)
    if args:
      stepInstance.setValue("args",             args)



    currentApp = "root.%s" % appVersion

    swPackages = 'SoftwarePackages'
    description = 'ILC Software Packages to be installed'
    if not self.workflow.findParameter(swPackages):
      self._addParameter(self.workflow, swPackages, 'JDL', currentApp, description)
    else:
      apps = self.workflow.findParameter(swPackages).getValue()
      if not currentApp in string.split(apps, ';'):
        apps += ';' + currentApp
      self._addParameter(self.workflow, swPackages, 'JDL', apps, description)
    self.ioDict["RootStep"] = stepInstance.getName()

    return S_OK()

  #-----------------------------------------------------------------------------
  # Experimental apps

  def setStdhepToSLCIO( self, inputStdhepFiles = None, logFile = None ):
    """ Helper function
    """

    # Define appName, used below for setting up step instance
    #---------------------------------------------------------------------------

    appName    = 'StdhepToSLCIO'
    appVersion = "HEAD"

    # Check the required arguments
    #---------------------------------------------------------------------------

    pass

    # Check for input files
    #---------------------------------------------------------------------------
    # Two possibilities:
    # 1. User defined in a user job.
    # 2. Files from previous step in a user job.
    #    The actual linking of the output files from previous step occurs
    #    below in another section of code where the step is set up

    if inputStdhepFiles:

      self._checkArgs( {
        'inputStdhepFiles' : types.ListType
      } )

      for inputStdhepFile in inputStdhepFiles:
        self._addFileToInputSandbox( inputStdhepFile, 'Input Stdhep file' )

        filename = os.path.splitext( inputStdhepFile )[0] + ".slcio"
        self._addFileToOutputSandbox( filename, 'Output slcio file' )

    elif self.ioDict.has_key( "WhizardStep" ):
      pass

    else:
      self.log.notice( "No input files found for stdhep->slcio conversion." )

    # Log file
    #---------------------------------------------------------------------------

    if logFile:

      self._checkArgs( { 'logFile' : types.StringTypes } )
      self._addFileToOutputSandbox( logFile, 'Log file for application stdout' )

    # Setting up a step instance
    #---------------------------------------------------------------------------
    # 1. Create Step definition
    # 2. Add step parameters
    # 3. Add modules to step
    # 4. Add step to workflow
    # 5. Create Step instance
    # 6. Set variables that are used by the modules
    # 7. Install software
    # 8. Set ioDict to pass parameters to future steps

    # Add count to number of steps

    self.StepCount += 1

    # 1. Create Step definition

    stepNumber = self.StepCount
    stepDefn   = '%sStep%s'    % ( appName, stepNumber )
    stepName   = 'Run%sStep%s' % ( appName, stepNumber )

    self._addParameter(
      self.workflow,
      'TotalSteps',
      'String',
      self.StepCount,
      'Total number of steps'
    )

    step = StepDefinition( stepDefn )

     # 2. Add step parameters

    step.addParameter( Parameter( "applicationVersion", "", "string", "", "", False, False, "Application version" ) )
    step.addParameter( Parameter( "applicationLog",     "", "string", "", "", False, False, "Name of the output file of the application" ) )

    # 3. Add modules to step

    modules = [
      ['StdHepConverter',     'An Application script module that can execute any provided script in the given project name and version environment'],
      ['UserJobFinalization', 'Uploads user output data files with ILC specific policies.']
    ]

    self._addModuleToStep( modules, step, stepDefn )

    # 4. Add step to workflow

    self.workflow.addStep( step )

    # 5. Create Step instance

    stepInstance = self.workflow.createStepInstance( stepDefn, stepName )

    # 6. Set variables that are used by the modules

    # Application version

    stepInstance.setValue( "applicationVersion", appVersion )

    # Logfile

    if logFile:
      stepInstance.setValue( "applicationLog", logFile )

    # 7. Install software

    self._addSoftware( 'lcio', appVersion )

    # 8. Set ioDict to pass parameters to future steps

    self.ioDict["StdhepToSLCIOStep"] = stepInstance.getName()

    # Return
    #---------------------------------------------------------------------------

    return S_OK()

  def setLCIOConcatenate( self, inputSLCIOFiles = None, outputSLCIOFile = None, logFile = None ):
    """ Helper function
    """

    # Define appName, used below for setting up step instance
    #---------------------------------------------------------------------------

    appName    = 'LCIOConcatenate'
    appVersion = 'HEAD'

    # Check the required arguments
    #---------------------------------------------------------------------------

    self._checkArgs( {
      'outputSLCIOFile' : types.StringTypes,
    } )

    # Check for input files
    #---------------------------------------------------------------------------
    # Three possibilities:
    # 1. User defined in a user job.
    # 2. Files from previous step in a user job.
    #    The actual linking of the output files from previous step occurs
    #    below in another section of code where the step is set up.
    # 3. There are no input files. Raise error.

    # 1. User defined in a user job.

    if inputSLCIOFiles:

      self._checkArgs( { 'inputSLCIOFiles' : types.ListType  } )

      for inputSLCIOFile in inputSLCIOFiles:
        self._addFileToInputSandbox( inputSLCIOFile, 'Input slcio file' )

    # 3. There are no input files. Raise error.

    else:
      self.log.notice( "No user defined input files found to concatenate." )

    # Check for output files
    #---------------------------------------------------------------------------

    if outputSLCIOFile:
      self._addFileToOutputSandbox( outputSLCIOFile, 'Output slcio file' )
    else:
      self._reportError( "No output file defined. Please define outputSLCIOFile parameter." )

    # Log file
    #---------------------------------------------------------------------------

    if logFile:

      self._checkArgs( { 'logFile' : types.StringTypes } )
      self._addFileToOutputSandbox( logFile, 'Log file for application stdout' )

    # Setting up a step instance
    #---------------------------------------------------------------------------
    # 1. Create Step definition
    # 2. Add step parameters
    # 3. Add modules to step
    # 4. Add step to workflow
    # 5. Create Step instance
    # 6. Set variables that are used by the modules
    # 7. Install software
    # 8. Set ioDict to pass parameters to future steps

    # Add count to number of steps

    self.StepCount +=1

    # 1. Create Step definition

    stepNumber = self.StepCount
    stepDefn   = '%sStep%s'    % ( appName, stepNumber )
    stepName   = 'Run%sStep%s' % ( appName, stepNumber )

    self._addParameter(
      self.workflow,
      'TotalSteps',
      'String',
      self.StepCount,
      'Total number of steps'
    )

    step = StepDefinition( stepDefn )

     # 2. Add step parameters

    step.addParameter( Parameter( "applicationVersion", "", "string", "", "", False, False, "Application version" ) )
    step.addParameter( Parameter( "applicationLog",     "", "string", "", "", False, False, "Name of the output file of the application" ) )
    step.addParameter( Parameter( "inputSLCIOFiles",    "", "string", "", "", False, False, "Input slcio files" ) )
    step.addParameter( Parameter( "outputSLCIOFile",    "", "string", "", "", False, False, "Output slcio files" ) )

    # 3. Add modules to step

    modules = [
      ['LCIOConcatenate',     'LCIO file concatenator'],
      ['UserJobFinalization', 'Uploads user output data files with ILC specific policies.']
    ]

    self._addModuleToStep( modules, step, stepDefn )

    # 4. Add step to workflow

    self.workflow.addStep( step )

    # 5. Create Step instance

    stepInstance = self.workflow.createStepInstance( stepDefn, stepName )

    # 6. Set variables that are used by the modules

    #

    stepInstance.setValue( "applicationVersion", appVersion )
    stepInstance.setValue( "outputSLCIOFile",    outputSLCIOFile )

    #

    if logFile:
      stepInstance.setValue( "applicationLog", logFile )

    # 7. Install software

    self._addSoftware( 'lcio', appVersion )

    # 8. Set ioDict to pass parameters to future steps

    self.ioDict["LCIOConcatenateStep"] = stepInstance.getName()

    # Return
    #---------------------------------------------------------------------------

    return S_OK()

  def setCheckCollections( self, inputSLCIOFiles = None, collections = None, logFile = None ):
    """ Helper function
    """
    # Define appName, used below for setting up step instance
    #---------------------------------------------------------------------------

    appName    = 'CheckCollections'
    appVersion = 'HEAD'

    # Check the arguments
    #---------------------------------------------------------------------------
    # Input files are checked below separately because inputSLCIOFiles can be
    # left undefined. The input will then be taken from any previous steps, if
    # any.

    self._checkArgs( {
      'collections'     : types.ListType
    } )

    # Check for input files
    #---------------------------------------------------------------------------
    # Two possibilities:
    # 1. User defined in a user job.
    # 2. Files from previous step in a user job.
    #    The actual linking of the output files from previous step occurs
    #    below in another section of code where the step is set up
    # 3. There are no input files. Raise error.

    # 1. User defined in a user job.

    if inputSLCIOFiles:

      self._checkArgs( {
        'inputSLCIOFiles' : types.ListType
      } )

      for inputSLCIOFile in inputSLCIOFiles:
        self._addFileToInputSandbox( inputSLCIOFile, 'Input slcio files' )

    # 3. There are no input files.

    else:
      self.log.notice( "No input files found to check." )

    # Log file
    #---------------------------------------------------------------------------

    if logFile:

      self._checkArgs( { 'logFile' : types.StringTypes } )
      self._addFileToOutputSandbox( logFile, 'Log file for application stdout' )

    # Setting up a step instance
    #---------------------------------------------------------------------------
    # 1. Create Step definition
    # 2. Add step parameters
    # 3. Add modules to step
    # 4. Add step to workflow
    # 5. Create Step instance
    # 6. Set variables that are used by the modules
    # 7. Install software

    # Add count to number of steps

    self.StepCount += 1

    # 1. Create Step definition

    stepNumber = self.StepCount
    stepDefn   = '%sStep%s'    % ( appName, stepNumber )
    stepName   = 'Run%sStep%s' % ( appName, stepNumber )

    self._addParameter(
      self.workflow,
      'TotalSteps',
      'String',
      self.StepCount,
      'Total number of steps'
    )

    step = StepDefinition( stepDefn )

     # 2. Add step parameters

    step.addParameter( Parameter( "applicationVersion", "", "string", "", "", False, False, "Application version" ) )
    step.addParameter( Parameter( "applicationLog",     "", "string", "", "", False, False, "Name of the output file of the application" ) )
    step.addParameter( Parameter( "inputSLCIOFiles",    "", "string", "", "", False, False, "Input slcio files" ) )
    step.addParameter( Parameter( "collections",        "", "string", "", "", False, False, "Collections to check for" ) )

    # 3. Add modules to step

    modules = [
      ['CheckCollections',    'Check whether a certain collection is available'],
      ['UserJobFinalization', 'Uploads user output data files with ILC specific policies.']
    ]

    self._addModuleToStep( modules, step, stepDefn )

    # 4. Add step to workflow

    self.workflow.addStep( step )

    # 5. Create Step instance

    stepInstance = self.workflow.createStepInstance( stepDefn, stepName )

    # 6. Set variables that are used by the modules

    stepInstance.setValue( "applicationVersion", appVersion )
    stepInstance.setValue( "collections",        ";".join( collections ) )

    #

    if inputSLCIOFiles:
      stepInstance.setValue( "inputSLCIOFiles",    ";".join( inputSLCIOFiles ) )

    elif self.ioDict.has_key( "StdhepToSLCIOStep" ):
      stepInstance.setLink( 'inputSLCIOFiles', self.ioDict["StdhepToSLCIOStep"], 'outputSLCIOFiles' )

    elif self.ioDict.has_key( "LCIOConcatenateStep" ):
      stepInstance.setLink( 'inputSLCIOFiles', self.ioDict["LCIOConcatenateStep"], 'outputSLCIOFile' )

    elif self.ioDict.has_key( "MokkaStep" ):
      stepInstance.setLink( 'inputSLCIOFiles', self.ioDict["MokkaStep"], 'outputFile' )

    #elif self.ioDict.has_key( "MarlinStep" ):
    #  stepInstance.setLink( 'inputSLCIOFiles', self.ioDict["MarlinStep"], 'outputFile' )
    #
    #elif self.ioDict.has_key( "SLICStep" ):
    #  stepInstance.setLink( 'inputSLCIOFiles', self.ioDict["SLICStep"], 'outputFile' )
    #
    #elif self.ioDict.has_key( "LCSIMStep" ):
    #  stepInstance.setLink( 'inputSLCIOFiles', self.ioDict["LCSIMStep"], 'outputFile' )
    #
    #elif self.ioDict.has_key( "SLICPandoraStep" ):
    #  stepInstance.setLink( 'inputSLCIOFiles', self.ioDict["SLICPandoraStep"], 'outputFile' )

    #

    if logFile:
      stepInstance.setValue( "applicationLog", logFile )

    # 7. Install software

    self._addSoftware( 'lcio', appVersion )

    # Return

    return S_OK()

  def setTomato( self, appVersion, xmlFile, inputSLCIOFiles = None, libTomato = None, logFile = ''):
    """ Helper function
    """
    # Wrapper for Marlin

    # Check the arguments
    #---------------------------------------------------------------------------
    # Input files are checked below separately because inputSLCIOFiles can be
    # left undefined. The input will then be taken from any previous steps, if
    # any.

    self._checkArgs( {
      'xmlFile'     : types.StringTypes
    } )

    # Check for input files
    #---------------------------------------------------------------------------
    # Two possibilities:
    # 1. User defined in a user job.
    # 2. Files from previous step in a user job.
    #    The actual linking of the output files from previous step occurs
    #    below in another section of code where the step is set up
    # 3. There are no input files. Raise error.

    # 1. User defined in a user job.

    if inputSLCIOFiles:

      self._checkArgs( {
        'inputSLCIOFiles' : types.ListType
      } )

      for inputSLCIOFile in inputSLCIOFiles:
        self._addFileToInputSandbox( inputSLCIOFile, 'Input slcio files' )

    # 3. There are no input files.

    else:
      self.log.notice( "No input files found to run Tomato on." )

    # User specified libTomato
    #---------------------------------------------------------------------------

    if libTomato:

      self._checkArgs( {
        'libTomato' : types.StringTypes
      } )

      self._addFileToInputSandbox( libTomato, 'User provided libTomato' )

    # Log file
    #---------------------------------------------------------------------------

    if logFile:
      self._addFileToOutputSandbox( logFile, 'Log file for application stdout' )

    # Marlin stuffs
    #---------------------------------------------------------------------------

    # 7. Install software

    self._addSoftware( 'tomato', appVersion )

  #############################################################################
  def setOutputData(self, lfns, OutputSE=[], OutputPath=''):
    """Helper function, used in preference to Job.setOutputData() for ILC.

       For specifying output data to be registered in Grid storage.  If a list
       of OutputSEs are specified the job wrapper will try each in turn until
       successful.

       Example usage:

       >>> job = Job()
       >>> job.setOutputData(['Ntuple.root'])

       @param lfns: Output data file or files
       @type lfns: Single string or list of strings ['','']
       @param OutputSE: Optional parameter to specify the Storage
       @param OutputPath: Optional parameter to specify the Path in the Storage, postpented to /ilc/user/u/username/
       Element to store data or files, e.g. CERN-tape
       @type OutputSE: string or list
       @type OutputPath: string
    """
    kwargs = {'lfns':lfns, 'OutputSE':OutputSE, 'OutputPath':OutputPath}
    if type(lfns) == list and len(lfns):
      outputDataStr = string.join(lfns, ';')
      description = 'List of output data files'
      self._addParameter(self.workflow, 'UserOutputData', 'JDL', outputDataStr, description)
    elif type(lfns) == type(" "):
      description = 'Output data file'
      self._addParameter(self.workflow, 'UserOutputData', 'JDL', lfns, description)
    else:
      return self._reportError('Expected file name string or list of file names for output data', **kwargs)

    if OutputSE:
      description = 'User specified Output SE'
      if type(OutputSE) in types.StringTypes:
        OutputSE = [OutputSE]
      elif type(OutputSE) != types.ListType:
        return self._reportError('Expected string or list for OutputSE', **kwargs)
      OutputSE = ';'.join(OutputSE)
      self._addParameter(self.workflow, 'UserOutputSE', 'JDL', OutputSE, description)

    if OutputPath:
      description = 'User specified Output Path'
      if not type(OutputPath) in types.StringTypes:
        return self._reportError('Expected string for OutputPath', **kwargs)
      # Remove leading "/" that might cause problems with os.path.join
      while OutputPath[0] == '/': 
        OutputPath = OutputPath[1:]
      if OutputPath.count("ilc/user"):
        return self._reportError('Output path contains /ilc/user/ which is not what you want', **kwargs)
      self._addParameter(self.workflow, 'UserOutputPath', 'JDL', OutputPath, description)

    return S_OK()
  
  def setBannedSites(self, sites):
    """Helper function.

       Can specify banned sites for job.  This can be useful
       for debugging purposes but often limits the possible candidate sites
       and overall system response time.

       Example usage:

       >>> job = ILCJob()
       >>> job.setBannedSites(['LCG.DESY.de','LCG.KEK.jp'])

       @param sites: single site string or list
       @type sites: string or list
    """
    bannedsites = []
    bannedsiteswp = self.workflow.findParameter("BannedSites")
    if bannedsiteswp:
      bannedsites = bannedsiteswp.getValue().split(";")
    if type(sites) == list and len(sites):
      bannedsites.extend(sites)
    elif type(sites) == type(" "):
      bannedsites.append(sites)
    else:
      kwargs = {'sites':sites}
      return self._reportError('Expected site string or list of sites', **kwargs)
    description = 'Sites excluded by user'
    self._addParameter(self.workflow, 'BannedSites', 'JDLReqt', string.join(bannedsites,';'), description)
    return S_OK()

  def setIgnoreApplicationErrors(self):
    """Helper function
    
    When called, all applications will always return OK, even if something went wrong: allows upload of output data
    """
    self._addParameter(self.workflow, 'IgnoreAppError', 'JDL', True, 'To ignore application errors')
    return S_OK()

  def testing(self,dict):
    self.StepCount += 1
    self._addParameter(self.workflow, 'TotalSteps', 'String', self.StepCount, 'Total number of steps')
    self._addParameter(self.workflow, "test","list",dict,"testing dict")
    return S_OK()

  def checkWorkflowParams(self):
    self.StepCount += 1
    stepName = 'DummyModule'
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' % ('DummyModule', stepNumber)
    self._addParameter(self.workflow, 'TotalSteps', 'String', self.StepCount, 'Total number of steps')
    moduleName = "DummyModule"
    module = ModuleDefinition(moduleName)
    module.setDescription('DummyModule module definition')
    body = 'from %s.%s import %s\n' % (self.importLocation, moduleName, moduleName)
    module.setBody(body)
    step = StepDefinition(stepDefn)
    step.addModule(module)
    step.createModuleInstance('DummyModule', stepDefn)
    self.workflow.addStep(step)
    stepInstance = self.workflow.createStepInstance(stepDefn, stepName)
        
    return S_OK()

  def _AddSoftwarePackages(self,softdict):
    """
    Expert tool Add software packages, mostly for tests and new software
    """
    for appn,appv in softdict.items():
      self._addSoftware(appn, appv)

    return S_OK()
  
  def _RemoveSoftwarePackages(self,softdict):
    """ Expert tool to remove software
    """
    apps = ''
    for appname,appversion in softdict.items():
      apps += appname+"."+appversion+";"
    apps.rstrip(";")  
    step = StepDefinition( 'RemoveSoft' )
    step.addParameter( Parameter( "Apps", "", "string", "", "", False, False, "Applications to remove" ) )
    
    module = ModuleDefinition( 'RemoveSoft' )
    module.setDescription( 'Remove software module' )
    module.setBody( 'from ILCDIRAC.Core.Utilities.RemoveSoft import RemoveSoft\n' )

    step.addModule( module )
    step.createModuleInstance( 'RemoveSoft', 'RemoveSoft' )

    self.workflow.addStep(step)

    stepInstance = self.workflow.createStepInstance( 'RemoveSoft', 'RemoveSoft' )

    # 6. Set variables that are used by the modules

    stepInstance.setValue( "Apps", apps )
    
    return S_OK()

  def _rootType(self, name):
    """ Private method
    """
    modname = ''
    if name.endswith((".C", ".cc", ".cxx", ".c")):
      modname = "RootMacroAnalysis"
    else:
      modname = "RootExecutableAnalysis"
    return modname

  def _sortSRM(self, srmfiles = ""):
    """ Private method
    """
    list = []
    srmlist = srmfiles.split(";")
    for srmdict in srmlist:
      srm = eval(srmdict)['file']
      list.append(srm)
    return list

  #-----------------------------------------------------------------------------
  # Helper methods

  def _addModuleToStep( self, modules, step, stepDefn ):
    """ Private method
    """

    for aModule in modules:

      moduleName        = aModule[0]
      moduleDescription = aModule[1]

      module = ModuleDefinition( moduleName )
      module.setDescription( moduleDescription )
      module.setBody( 'from %s.%s import %s\n' % ( self.importLocation, moduleName, moduleName ) )

      step.addModule( module )
      step.createModuleInstance( moduleName, stepDefn )

  def _addSoftware( self, appName, appVersion ):
    """ Private method
    """

    currentApp  = "%s.%s" % ( appName.lower(), appVersion )
    swPackages  = 'SoftwarePackages'
    description = 'ILC Software Packages to be installed'

    if not self.workflow.findParameter( swPackages ):
      self._addParameter( self.workflow, swPackages, 'JDL', currentApp, description )
    else:
      apps = self.workflow.findParameter( swPackages ).getValue()

      if not currentApp in string.split( apps, ';' ):
        apps += ';' + currentApp

      self._addParameter( self.workflow, swPackages, 'JDL', apps, description )

  def _addFileToInputSandbox( self, filePath, fileDescription ):
    """ Private method
    """

    if not filePath:
      return self._reportError(
        '%s does not exist: %s' % (fileDescription, filePath),
        __name__,
        **self._getArgsDict( 1 )
      )

    if os.path.exists( filePath ):

      self.log.verbose( 'Found %s: %s' % ( fileDescription, filePath ) )
      self.addToInputSandbox.append( filePath )
      return

    elif filePath.lower().find( "lfn:" ) > -1:

      self.log.verbose('Found specified lfn to %s: %s' % ( fileDescription, filePath ) )
      self.addToInputSandbox.append(filePath)
      return

    return self._reportError(
      '%s does not exist: %s' % (fileDescription, filePath),
      __name__,
      **self._getArgsDict( 1 )
    )

  def _addFileToOutputSandbox( self, filePath, fileDescription ):
    """ Private method
    """

    if not filePath:
      return self._reportError(
        'Specified path for file (%s) is empty' % (fileDescription, filePath),
        __name__,
        **self._getArgsDict( 1 )
      )

    self.addToOutputSandbox.append( filePath )

  def _checkArgs( self, argNamesAndTypes ):
    """ Private method
    """

    # inspect.stack()[1][0] returns the frame object ([0]) of the caller
    # function (stack()[1]).
    # The frame object is required for getargvalues. Getargvalues returns
    # a typle with four items. The fourth item ([3]) contains the local
    # variables in a dict.

    args = inspect.getargvalues( inspect.stack()[ 1 ][ 0 ] )[ 3 ]

    #

    for argName, argType in argNamesAndTypes.iteritems():

      if not args.has_key(argName):
        self._reportError(
          'Method does not contain argument \'%s\'' % argName,
          __name__,
          **self._getArgsDict( 1 )
        )

      if not isinstance( args[argName], argType):
        self._reportError(
          'Argument \'%s\' is not of type %s' % ( argName, argType ),
          __name__,
          **self._getArgsDict( 1 )
        )

  def _getArgsDict( self, level = 0 ):
    """ Private method
    """

    # Add one to stack level such that we take the caller function as the
    # reference point for 'level'

    level += 1

    #

    args = inspect.getargvalues( inspect.stack()[ level ][ 0 ] )
    dict = {}

    for arg in args[0]:

      if arg == "self":
        continue

      # args[3] contains the 'local' variables

      dict[arg] = args[3][arg]

    return dict
