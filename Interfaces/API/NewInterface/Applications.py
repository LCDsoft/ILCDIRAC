'''
Created on Jul 28, 2011

@author: Stephane Poss
'''
from ILCDIRAC.Interfaces.API.NewInterface.Application import Application
from ILCDIRAC.Core.Utilities.GeneratorModels import GeneratorModels
from DIRAC.Core.Workflow.Parameter                  import Parameter
from DIRAC import S_OK,S_ERROR


import os,types


#################################################################
#            Generic Application: use a script in an 
#                 application framework
#################################################################  
class GenericApplication(Application):
  def __init__(self,paramdict = None):
    Application.__init__(self,paramdict)
    self.script = None
    self.arguments = ''
    self.dependencies = {}
    self._modulename = "ApplicationScript"
    self.appname = self._modulename
    self._moduledescription = 'An Application script module that can execute any provided script in the given project name and version environment'
      
  def setScript(self,script):
    """ Define script to use
    """
    self._checkArgs( {
        'script' : types.StringTypes
      } )
    if os.path.exists(script) or script.lower().count("lfn:"):
      self.inputSB.append(script)
    self.script = script
    
  def setArguments(self,arguments):
    """ Define the arguments of the script (if any)
    """
    self._checkArgs( {
        'script' : types.StringTypes
      } )  
    self.arguments = arguments
      
  def addDependency(self,appdict):
    """ Define list of application you need
    
    >>> app.addDependency({"mokka":"v0706P08","marlin":"v0111Prod"})
    """  
    #check that dict has proper structure
    self._checkArgs( {
        'appdict' : types.DictType
      } )
    
    self.dependencies.update(appdict)

  def _applicationModule(self):
    m1 = self._createModule()
    m1.addParameter(Parameter("script", "", "string", "", "", False, False, "Script to execute"))
    m1.addParameter(Parameter("arguments", "", "string", "", "", False, False, "Arguments to pass to the script"))
    self._modules.append(m1)      
    return m1
  
  def _applicationModuleValues(self,moduleinstance):
    moduleinstance.setValue("script",self.script)
    moduleinstance.setValue('arguments',self.arguments)
  
  def _userjobmodules(self,stepdefinition):
    m1 = self._applicationModule()
    stepdefinition.addModule(m1)
    m1i = stepdefinition.createModuleInstance(m1.getType(),stepdefinition.getType())
    self._applicationModuleValues(m1i)
    
    m2 = self._getUserOutputDataModule()
    self._modules.append(m2)
    stepdefinition.addModule(m2)
    stepdefinition.createModuleInstance(m2.getType(),stepdefinition.getType())
    return S_OK()

  def _prodjobmodules(self,stepdefinition):
    m1 = self._applicationModule()
    stepdefinition.addModule(m1)
    m1i = stepdefinition.createModuleInstance(m1.getType(),stepdefinition.getType())
    self._applicationModuleValues(m1i)
    
    m2 = self._getComputeOutputDataListModule()
    self._modules.append(m2)
    stepdefinition.addModule(m2)
    stepdefinition.createModuleInstance(m2.getType(),stepdefinition.getType())
    return S_OK()    

  def _addParametersToStep(self,stepdefinition):
    res = self._addBaseParameters(stepdefinition)
    if not res["OK"]:
      return S_ERROR("Failed to set base parameters")
    return S_OK()
  
  def _setStepParametersValues(self, instance):
    self._setBaseStepParametersValues(instance)
    for depn,depv in self.dependencies.items():
      self.job._addSoftware(depn,depv)
    return S_OK()
      
  def _checkConsistency(self):
    """ Called from Job
    """
    if not self.script:
      return S_ERROR("Script not defined")
    elif not self.script.lower().count("lfn:") and not os.path.exists(self.script):
      return S_ERROR("Specified script is not an LFN and was not found on disk")
      
    if not len(self.dependencies):
      return S_ERROR("Dependencies not set")
    return S_OK()  
  
#################################################################
#            GetSRMFile: as its name suggests...
#################################################################  
class GetSRMFile(Application):
  """ Gets a given file from storage directly using srm path.
  """
  def __init__(self):
    Application.__init__(self)
    self._modulename = "GetSRMFile"
    self.appname = self._modulename
    self._moduledescription = "Module to get files directly from Storage"

  def setFiles(self,fdict):
    """ Specify the files you need
    
    @param fdict: file dictionary: {file:site}, can be also ["{}","{}"] etc.
    @type fdict: dict or list
    """
    kwargs = {"fdict":fdict}
    if not type(fdict) == type("") and not type(fdict) == type([]):
      return self._reportError('Expected string or list of strings for fdict', __name__, **kwargs)
    
    self.filedict = fdict

  def _applicationModule(self):
    m1 = self._createModule()
    #m1.addParameter(Parameter...)
    self._modules.append(m1)
    return m1

  def _applicationModuleValues(self,moduleinstance):
    moduleinstance.setValue()  
  
  def _userjobmodules(self,step):
    m1 = self._applicationModule()
    step.addModule(m1)
    m1i = step.createModuleInstance(m1.getType(),step.getType())
    self._applicationModuleValues(m1i)
    return S_OK()

  def _prodjobmodules(self,step):
    self.log.error("This application is not meant to be used in Production context")
    return S_ERROR('Should not use in Production')

  
  def _checkConsistency(self):
    if not self.filedict:
      return S_ERROR("The file list was not defined")
    return S_OK()

  def _addParametersToStep(self,step):
    res = self._addBaseParameters(step)
    if not res["OK"]:
      return S_ERROR("Failed to set base parameters")
    step.addParameter(Parameter("srmfiles", "", "string", "", "", False, False, "list of files to retrieve"))
    return S_OK()

#################################################################
#            Whizard: First Generator application
#################################################################    
class Whizard(Application):
  """ Calls a whizard application step
  """
  def __init__(self,processlist=None):    
    Application.__init__(self)
    self._modulename = 'WhizardAnalysis'
    self._moduledescription = 'Module to run WHIZARD'
    
    self.appname = 'whizard'
    self.process = ''
    if processlist:
      self.processlist = processlist
    self.model = 'sm'  
    self.leshouchesfiles = None
    self.generatormodels = GeneratorModels()
    self.datatype = 'gen'
    
    
  def setProcess(self,process):
    """ Define process
    """
    self._checkArgs( {
        'process' : types.StringTypes
      } )
    self.process = process

  def setLuminosity(self,lumi):
    """ Define luminosity to generate 
    """
    self._checkArgs( {
        'lumi' : types.FloatType
      } )    
    self.lumi = lumi

  def setRandomSeed(self,seed):
    """ Define random seed to use 
    """
    self._checkArgs( {
        'seed' : types.IntType
      } )

    self.seed = seed
  
  def setParameterDict(self,paramdict):
    """ Parameters for Whizard steering files
    """
    self._checkArgs( {
        'paramdict' : types.DictType
      } )

    self.parameterdict = paramdict
  
  def setModel(self,model):
    """ Define Model
    """  
    self._checkArgs( {
        'model' : types.StringTypes
      } )

    self. model = model
    
  def _checkConsistency(self):
    if not self.process:
      return S_ERROR("Process not defined")
    if not self.processlist:
      return S_ERROR("Process list was not given")
    ### TODO: In the future here lies the call the ProcessProduction about validity of process
    if not self.version:
      self.version = 'anything'###  Must be found in processlist, or 
      if not self.version:
        return S_ERROR("Version not set")
    ## Check that all keys are in the paramdict, set them to default if not set
    if not self.parameterdict.has_key("PNAME1"):
      self.parameterdict["PNAME1"] = 'e1'
    ##And so on...
      
    if self.model:
      if not self.generatormodels.has_key(self.model):
        return S_ERROR("Unknown model %s"%self.model)

    return S_OK()  

  def _applicationModule(self):
    m1 = self._createModule()
    m1.addParameter(Parameter("evttype", "", "string", "", "", False, False, "Process to generate"))
    self._modules.append(m1)      
    return m1
  
  def _applicationModuleValues(self,moduleinstance):
    moduleinstance.setValue("evttype",self.process)
    
  def _userjobmodules(self,step):
    m1 = self._applicationModule()
    step.addModule(m1)
    m1i = step.createModuleInstance(m1.getType(),step.getType())
    self._applicationModuleValues(m1i)
    
    m2 = self._getUserOutputDataModule()
    self._modules.append(m2)
    step.addModule(m2)
    step.createModuleInstance(m2.getType(),step.getType())
    return S_OK()

  def _prodjobmodules(self,step):
    m1 = self._applicationModule()
    step.addModule(m1)
    m1i = step.createModuleInstance(m1.getType(),step.getType())
    self._applicationModuleValues(m1i)
    
    m2 = self._getComputeOutputDataListModule()
    self._modules.append(m2)
    step.addModule(m2)
    step.createModuleInstance(m2.getType(),step.getType())
    return S_OK()

#################################################################
#            PYTHIA: Second Generator application
#################################################################    
class Pythia(Application):
  """ Call pythia
  """
  def __init__(self):
    Application.__init__(self)
    self.appname = 'pythia'
    self._modulename = 'PythiaAnalysis'
    self._moduledescription = 'Module to run PYTHIA'

  def _applicationModule(self):
    m1 = self._createModule()
    return m1  

  def _userjobmodules(self,step):
    m1 = self._applicationModule()
    step.addModule(m1)
    m1i = step.createModuleInstance(m1.getType(),step.getType())
    self._applicationModuleValues(m1i)
    
    m2 = self._getUserOutputDataModule()
    self._modules.append(m2)
    step.addModule(m2)
    step.createModuleInstance(m2.getType(),step.getType())
    return S_OK()

  def _prodjobmodules(self,step):
    m1 = self._applicationModule()
    step.addModule(m1)
    m1i = step.createModuleInstance(m1.getType(),step.getType())
    self._applicationModuleValues(m1i)
    
    m2 = self._getComputeOutputDataListModule()
    self._modules.append(m2)
    step.addModule(m2)
    step.createModuleInstance(m2.getType(),step.getType())
    return S_OK()
      
  def _checkConsistency(self):
    if not self.version:
      return S_ERROR("Version not specified")
    
    if not self.systemconfig:
      return S_ERROR("System Config not set")
    ##Check that pythia.self.version exists in CS by uing self.systemconfig
    
    if not self.nbevts:
      return S_ERROR("Number of events to generate not defined")

    return S_OK()
  
##########################################################################
#            StdhepCut: apply generator level cuts after pythia or whizard
##########################################################################
class StdhepCut(Application): 
  """ Call stdhep cut after whizard of pythia
  """
  def __init__(self, paramdict = None):
    Application.__init__(self,paramdict)
    self.appname = 'stdhepcut'
    self._modulename = 'StdHepCut'
    self._moduledescription = 'Module to cut on Generator (Whizard of PYTHIA)'
    
    self.cutfile = None
    self.maxevts = 0
    self.nbevtsperfile = 0
    
  def setCutFile(self,cutfile):
    """ Define cut file
    """
    self._checkArgs( {
        'cutfile' : types.StringTypes
      } )
    self.cutfile = cutfile  

  def setMaxNbEvts(self,nbevts):
    """ Max number of events to keep in each file
    """
    self._checkArgs( {
        'nbevts' : types.IntType
      } )
    self.maxevts = nbevts
    
  def setNbEvtsPerFile(self,nbevts):
    """ Number of events per file
    """
    self._checkArgs( {
        'nbevts' : types.IntType
      } )
    self.nbevtsperfile = nbevts  

  def _applicationModule(self):
    m1 = self._createModule()
    m1.addParameter(Parameter("CutFile", "", "string", "", "", False, False, "Process to generate"))
    self._modules.append(m1)  
    return m1

  def _applicationModuleValues(self,moduleinstance):
    moduleinstance.setValue("CutFile",self.cutfile)

  def _userjobmodules(self,step):
    m1 = self._applicationModule()
    step.addModule(m1)
    m1i = step.createModuleInstance(m1.getType(),step.getType())
    self._applicationModuleValues(m1i)
    
    m2 = self._getUserOutputDataModule()
    self._modules.append(m2)
    step.addModule(m2)
    step.createModuleInstance(m2.getType(),step.getType())
    return self._modules

  def _prodjobmodules(self,step):
    m1 = self._applicationModule()
    step.addModule(m1)
    m1i = step.createModuleInstance(m1.getType(),step.getType())
    self._applicationModuleValues(m1i)
    
    m2 = self._getComputeOutputDataListModule()
    self._modules.append(m2)
    step.addModule(m2)
    step.createModuleInstance(m2.getType(),step.getType())
    return self._modules


  def _checkConsistency(self):
    if not self.cutfile:
      return S_ERROR("Cut file not specified")
    elif not self.cutfile.lower().count("lfn:") and not os.path.exists(self.cutfile):
      return S_ERROR("Cut file not found and is not an LFN")
    
    if not self.maxevts:
      return S_ERROR("You did not specify how many events you need to keep per file (MaxNbEvts)")
    
    res = self._checkRequiredApp() ##Check that job order is correct
    if not res['OK']:
      return res
    
    return S_OK()
  
  def _resolveLinkedParameters(self,stepinstance):
    if self.inputappstep:
        
      res = stepinstance.setLink("InputFile",self.inputappstep.getType(),"OutputFile")
      if not res:
        return S_ERROR("Failed to resolve InputFile from %s's OutputFile, possibly not defined."%self.inputappstep.getName())
    return S_OK()  
    