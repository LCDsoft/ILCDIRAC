'''
Created on Jul 28, 2011

@author: Stephane Poss
'''
from ILCDIRAC.Interfaces.API.NewInterface.Application import Application
from ILCDIRAC.Core.Utilities.GeneratorModels import GeneratorModels

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
    self.dependencies = []
    self._modulename = "ApplicationScript"
    self._moduledescription = 'An Application script module that can execute any provided script in the given project name and version environment'
    self._modules.append(self._createModule())
    self._modules.append(self._getUserOutputDataModule())
    
    
  def setScript(self,script):
    """ Define script to use
    """
    self._checkArgs( {
        'script' : types.StringTypes
      } )    
    self.script = script
    
  def addDependency(self,appdict):
    """ Define list of application you need
    """  
    #check that dict has proper structure
    self._checkArgs( {
        'appdict' : types.DictType
      } )
    
    self.dependencies.append(appdict)
      
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
  def __init__(self):
    Application.__init__(self)
    self._modulename = "GetSRMFile"
    self._moduledescription = "Module to get files directly from Storage"
    self._modules.append(self._createModule())

  def setFiles(self,fdict):
    """ Specify the files you need
    
    @param fdict: file dictionary: {file:site}
    @type fdict: dict or list
    """
    self.filedict = fdict
  
  def _checkConsistency(self):
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
    self._modules.append(self._createModule())
    self._modules.append(self._getUserOutputDataModule)
    
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
    self._modules.append(self._createModule())
    self._modules.append(self._getUserOutputDataModule)
    
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
  def __init__(self):
    Application.__init__(self)
    self.appname = 'stdhepcut'
    self._modulename = 'StdHepCut'
    self._moduledescription = 'Module to cut on Generator (Whizard of PYTHIA)'
    self._modules.append(self._createModule())
    self._modules.append(self._getUserOutputDataModule)
    
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

  def _checkConsistency(self):
    if not self.cutfile:
      return S_ERROR("Cut file not spcified")
    elif not self.cutfile.lower().count("lfn:") and not os.path.exists(self.cutfile):
      return S_ERROR("Cut file not found and is not an LFN")
    
    if not self.maxevts:
      return S_ERROR("You did not specify how many events you need to keep per file (MaxNbEvts)")
    
    res = self._checkRequiredApp() ##Check that job order is correct
    if not res['OK']:
      return res
    
    return S_OK()
    
    