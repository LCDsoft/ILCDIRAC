'''
Created on Jul 28, 2011

This module contains the definition of the different applications that can
be used to create jobs.

Example usage:

>>> from ILCDIRAC.Interfaces.API.Applications import *
>>> from ILCDIRAC.Interfaces.API.Job import Job 
>>> from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
>>> dirac = DiracILC()
>>> job = Job()
>>> ga = GenericApplication()
>>> ga.setScript("myscript.py")
>>> ga.setArguments("some arguments")
>>> ga.setDependency({"mokka":"v0706P08","marlin":"v0111Prod"})
>>> job.append(ga)
>>> dirac.submit(job)

It's also possible to set all the application's properties in the constructor

>>> ga = GenericApplication({'Script':'myscript.py',"Arguments":"some arguments","Dependency":{"mokka":"v0706P08","marlin":"v0111Prod"}})

but this is more an expert's functionality. 

Running:

>>> help(GenericApplication)

prints out all the available methods.

@author: Stephane Poss, Remi Ete, Ching Bon Lam
'''

from ILCDIRAC.Interfaces.API.NewInterface.Application import Application
from ILCDIRAC.Core.Utilities.ProcessList              import *
from ILCDIRAC.Core.Utilities.GeneratorModels          import GeneratorModels
from DIRAC.Core.Workflow.Parameter                    import Parameter
from DIRAC.Core.Workflow.Step                         import *
from DIRAC.Core.Workflow.Module                       import *
from DIRAC                                            import S_OK,S_ERROR


import os, types, string


#################################################################
#            Generic Application: use a script in an 
#                 application framework
#################################################################  
class GenericApplication(Application):
  """ Run a script (python or shell) in an application environment. 
  
  Example:
  
  >>> ga = GenericApplication()
  >>> ga.setScript("myscript.py")
  >>> ga.setArguments("some command line arguments")
  >>> ga.setDependency({"root":"5.26"})
  
  """
  def __init__(self, paramdict = None):
    self.script = None
    self.arguments = ''
    self.dependencies = {}
    ### The Application init has to come last as if not the passed parameters are overwritten by the defaults.
    Application.__init__(self, paramdict)
    #Those have to come last as the defaults from Application are not right
    self._modulename = "ApplicationScript"
    self.appname = self._modulename
    self._moduledescription = 'An Application script module that can execute any provided script in the given project name and version environment'
      
  def setScript(self,script):
    """ Define script to use
    
    @param script: Script to run on. Can be shell or python. Can be local file or LFN.
    @type script: string
    """
    self._checkArgs( {
        'script' : types.StringTypes
      } )
    if os.path.exists(script) or script.lower().count("lfn:"):
      self.inputSB.append(script)
    self.script = script
    return S_OK()
    
  def setArguments(self,args):
    """ Define the arguments of the script (if any)
    
    @param arguments: Arguments to pass to the command line call
    @type arguments: string
    
    """
    self._checkArgs( {
        'args' : types.StringTypes
      } )  
    self.arguments = args
    return S_OK()
      
  def setDependency(self,appdict):
    """ Define list of application you need
    
    >>> app.setDependency({"mokka":"v0706P08","marlin":"v0111Prod"})
    
    @param appdict: Dictionary of applciation to use: {"App":"version"}
    @type appdict: dict
    
    """  
    #check that dict has proper structure
    self._checkArgs( {
        'appdict' : types.DictType
      } )
    
    self.dependencies.update(appdict)
    return S_OK()

  def _applicationModule(self):
    m1 = self._createModuleDefinition()
    m1.addParameter(Parameter("script", "", "string", "", "", False, False, "Script to execute"))
    m1.addParameter(Parameter("arguments", "", "string", "", "", False, False, "Arguments to pass to the script"))
    m1.addParameter(Parameter("debug", False, "bool", "", "", False, False, "debug mode"))
    return m1
  
  def _applicationModuleValues(self,moduleinstance):
    moduleinstance.setValue("script",self.script)
    moduleinstance.setValue('arguments',self.arguments)
    moduleinstance.setValue('debug',self.debug)
  
  def _userjobmodules(self,stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setUserJobFinalization(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('userjobmodules failed')
    return S_OK() 

  def _prodjobmodules(self,stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setOutputComputeDataList(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('prodjobmodules failed')
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
    """ Checks that script and dependencies are set.
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
  
  Usage:
  
  >>> gf = GetSRMFile()
  >>> fdict = {"file":"srm://srm-public.cern.ch/castor/cern.ch/grid/ilc/prod/clic/1tev/Z_uds/gen/0/nobeam_nobrem_0-200.stdhep","site":"CERN-SRM"}
  >>> fdict = str(fdict)
  >>> gf.setFiles(fdict)
  
  """
  def __init__(self, paramdict = None):
    Application.__init__(self, paramdict)
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
    m1 = self._createModuleDefinition()
    m1.addParameter(Parameter("srmfiles", "", "string", "", "", False, False, "list of files to retrieve"))
    m1.addParameter(Parameter("debug", False, "bool", "", "", False, False, "debug mode"))
    return m1

  def _applicationModuleValues(self,moduleinstance):
    moduleinstance.setValue("srmfiles",self.filedict)  
    moduleinstance.setValue("debug",self.debug)  
  
  def _userjobmodules(self,stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    if not res1["OK"]  :
      return S_ERROR("userjobmodules method failed")
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
    return S_OK()


#################################################################
#                    ROOT master class
#################################################################  
class Root(Application):
  """ Root principal class. Will inherit in RootExe and RootMacro classes, so don't use this (you can't anyway)!
  """
  
  def __init__(self, paramdict = None):
    self.arguments = ''
    Application.__init__(self, paramdict)
    
    
  def setScript(self,script):
    self.log.error("Don't use this!")
    return S_ERROR("Not allowed here")
  
  
  def setMacro(self,macro):
    self.log.error("Don't use this!")
    return S_ERROR("Not allowed here")

     
  def setArguments(self,args):
    """ Define the arguments of the script (if any)
    
    @param arguments: Arguments to pass to the command line call
    @type arguments: string
    
    """
    self._checkArgs( {
        'args' : types.StringTypes
      } )  
    self.arguments = args
    return S_OK()
      

  def _applicationModule(self):
    m1 = self._createModuleDefinition()
    m1.addParameter(Parameter("arguments",    "", "string", "", "", False, False, "Arguments to pass to the script"))
    m1.addParameter(Parameter("script",       "", "string", "", "", False, False, "Script to execute"))
    m1.addParameter(Parameter("debug",     False,   "bool", "", "", False, False, "debug mode"))
    return m1
  
  
  def _applicationModuleValues(self,moduleinstance):
    moduleinstance.setValue('arguments',   self.arguments)
    moduleinstance.setValue("script",      self.script)
    moduleinstance.setValue('debug',       self.debug)
    
  
  def _userjobmodules(self,stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setUserJobFinalization(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('userjobmodules failed')
    return S_OK() 


  def _prodjobmodules(self,stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setOutputComputeDataList(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('prodjobmodules failed')
    return S_OK()    


#################################################################
#            Root Script Application: use a script in the 
#                    Root application framework
#################################################################  
class RootScript(Root):
  """ Run a script (root executable or shell) in the root application environment. 
  
  Example:
  
  >>> rootsc = RootScript()
  >>> rootsc.setScript("myscript.exe")
  >>> rootsc.setArguments("some command line arguments")
  
  """
  def __init__(self, paramdict = None):
    self.script = None
    Root.__init__(self, paramdict)
    self._modulename = "RootExecutableAnalysis"
    self.appname = self._modulename
    self._moduledescription = 'Root application script'
      
      
  def setScript(self,executable):
    """ Define executable to use
    
    @param executable: Script to run on. Can be shell or root executable. Must be a local file.
    @type executable: string
    """
    self._checkArgs( {
        'script' : types.StringTypes
      } )
    if os.path.exists(executable) :
      self.inputSB.append(executable)
    self.script = executable
    return S_OK()
    
    
  def setArguments(self,args):
    """ Define the arguments of the script (if any)
    
    @param arguments: Arguments to pass to the command line call
    @type arguments: string
    
    """
    self._checkArgs( {
        'args' : types.StringTypes
      } )  
    self.arguments = args
    return S_OK()
      

  def _checkConsistency(self):
    """ Checks that script is set.
    """
    if not self.script:
      return S_ERROR("Script not defined")
    elif not os.path.exists(self.script):
      return S_ERROR("Specified script was not found on disk")
      
    return S_OK()


#################################################################
#            Root Macro Application: use a macro in the 
#                   Root application framework
#################################################################  
class RootMacro(Root):
  """ Run a root macro in the root application environment. 
  
  Example:
  
  >>> rootmac = RootScript()
  >>> rootmac.setMacro("mymacro.C")
  >>> rootmac.setArguments("some command line arguments")
  
  """
  def __init__(self, paramdict = None):
    self.script = None
    Root.__init__(self, paramdict)
    self._modulename = "RootExecutableAnalysis"
    self.appname = self._modulename
    self._moduledescription = 'Root application script'
      
      
  def setMacro(self,macro):
    """ Define macro to use
    
    @param macro: Macro to run on. Must be a local C file.
    @type macro: string
    """
    self._checkArgs( {
        'macro' : types.StringTypes
      } )
    if os.path.exists(macro) :
      self.inputSB.append(macro)
    self.script = macro
    return S_OK()
    
    
  def setArguments(self,args):
    """ Define the arguments of the script (if any)
    
    @param arguments: Arguments to pass to the command line call
    @type arguments: string
    
    """
    self._checkArgs( {
        'args' : types.StringTypes
      } )  
    self.arguments = args
    return S_OK()
      
  
  def _checkConsistency(self):
    """ Checks that macro is set.
    """
    if not self.script:
      return S_ERROR("Macro not defined")
    elif not os.path.exists(self.script):
      return S_ERROR("Specified macro was not found on disk")
      
    return S_OK()


#################################################################
#            Whizard: First Generator application
#################################################################    
class Whizard(Application):
  """ Runs whizard to generate a given event type
  
  Usage:
  
  >>> wh = Whizard(dirac.getProcessList())
  >>> wh.setProcess("ee_h_mumu")
  >>> wh.setEnergy(500)
  >>> wh.setNbEvts(1000)
  >>> wh.setModel("SM")

  """
  def __init__(self, processlist = None, paramdict = None):    
    
    self.parameterdict = {}
    self.model = 'sm'  
    self.seed = 0
    self.lumi = 0
    self.jobindex = ''
    self.leshouchesfiles = None
    self.generatormodels = GeneratorModels()
    self.evttype = ''
    self.allowedparams = ['PNAME1','PNAME2','POLAB1','POLAB2','USERB1','USERB2','ISRB1','ISRB2','EPAB1','EPAB2','RECOIL','INITIALS','USERSPECTRUM']
    self.parameters = []
    if processlist:
      self.processlist = processlist
    Application.__init__(self, paramdict)
    ##Those 4 need to come after default constructor
    self._modulename = 'WhizardAnalysis'
    self._moduledescription = 'Module to run WHIZARD'
    self.appname = 'whizard'
    self.datatype = 'gen'
    
    
  def setEvtType(self,evttype):
    """ Define process
    
    @param process: Process to generate
    """
    self._checkArgs( {
        'evttype' : types.StringTypes
      } )
    self.evttype = evttype

  def setLuminosity(self,lumi):
    """ Define luminosity to generate 
    
    @param lumi: Luminosity to generate. Not available if cross section is not known a priori. Use with care.
    @type lumi: float
    """
    self._checkArgs( {
        'lumi' : types.FloatType
      } )    
    self.lumi = lumi

  def setRandomSeed(self,seed):
    """ Define random seed to use 
    
    @param seed: Seed to use during integration and generation. Default is Job ID.
    @type seed: int
    """
    self._checkArgs( {
        'seed' : types.IntType
      } )

    self.seed = seed
  
  def setParameterDict(self,paramdict):
    """ Parameters for Whizard steering files
    
    @param paramdict: Dictionary of parameters for the whizard templates. Most parameters are set on the fly.
    @type paramdict: dict
    """
    self._checkArgs( {
        'paramdict' : types.DictType
      } )

    self.parameterdict = paramdict
  
  def setModel(self,model):
    """ Define Model
    
    @param model: Model to use for generation. Predefined list available in GeneratorModels class.
    @type model: string
    """  
    self._checkArgs( {
        'model' : types.StringTypes
      } )

    self. model = model
    
  def setJobIndex(self,index):
    """ Define Job Index
    
    @param index: Index to use for generation
    @type index: string
    """  
    self._checkArgs( {
        'index' : types.StringTypes
      } )

    self.JobIndex = index
    
  def _checkConsistency(self):

    if not self.energy :
      return S_ERROR('Energy not set')
      
    if not self.nbevts :
      return S_ERROR('Number of events not set!')
    
    if not self.evttype:
      return S_ERROR("Process not defined")
    
    if not self.processlist:
      return S_ERROR("Process list was not given")
    
    if self.evttype:
      if not self.processlist.existsProcess(self.evttype)['Value']:
        self.log.info("Available processes are:")
        self.processlist.printProcesses()
        return S_ERROR('Process does no exists')
      else:
        cspath = self.processlist.getCSPath(self.evttype)
        whiz_file = os.path.basename(cspath)
        self.version = whiz_file.replace(".tar.gz","").replace(".tgz","").replace("whizard","")
        self.log.info("Found the process %s in whizard %s"%(self.evttype,self.version))
        
    if not self.version:
      return S_ERROR('No version found')
      
    if self.model:
      if not self.generatormodels.has_key(self.model):
        return S_ERROR("Unknown model %s"%self.model)

    if not self._jobtype == 'User':
      if not self.outputFile:
        return S_ERROR("Output File not defined")
      if not self.outputPath:
        return S_ERROR("Output Path not defined")
   
   
    for key in self.parameterdict.keys():
      if not key in self.allowedparams:
        return S_ERROR("Unknown parameter %s"%key)

    if not self.parameterdict.has_key('PNAME1'):
      print "Assuming incoming beam 1 to be electrons"
      self.parameters.append('PNAME1=e1')
    else:
      self.parameters.append("PNAME1=%s" %self.parameterdict["PNAME1"] )
      
    if not self.parameterdict.has_key('PNAME2'):
      print "Assuming incoming beam 2 to be positrons"
      self.parameters.append('PNAME2=E1')
    else:
      self.parameters.append("PNAME2=%s" %self.parameterdict["PNAME2"] )
       
    if not self.parameterdict.has_key('POLAB1'):
      print "Assuming no polarization for beam 1"
      self.parameters.append('POLAB1=0.0 0.0')
    else:
      self.parameters.append("POLAR1=%s" %self.parameterdict["POLAR1"] )
        
    if not self.parameterdict.has_key('POLAB2'):
      print "Assuming no polarization for beam 2"
      self.parameters.append('POLAB2=0.0 0.0')
    else:
      self.parameters.append("POLAR2=%s" %self.parameterdict["POLAR2"] )
        
    if not self.parameterdict.has_key('USERB1'):
      print "Will put beam spectrum to True for beam 1"
      self.parameters.append('USERB1=T')
    else:
      self.parameters.append("USERB1=%s" %self.parameterdict["USERB1"] )
        
    if not self.parameterdict.has_key('USERB2'):
      print "Will put beam spectrum to True for beam 2"
      self.parameters.append('USERB2=T')
    else:
      self.parameters.append("USERB2=%s" %self.parameterdict["USERB2"] )
        
    if not self.parameterdict.has_key('ISRB1'):
      print "Will put ISR to True for beam 1"
      self.parameters.append('ISRB1=T')
    else:
      self.parameters.append("ISRB1=%s" %self.parameterdict["ISRB1"] )
        
    if not self.parameterdict.has_key('ISRB2'):
      print "Will put ISR to True for beam 2"
      self.parameters.append('ISRB2=T')
    else:
      self.parameters.append("ISRB2=%s" %self.parameterdict["ISRB2"] )
        
    if not self.parameterdict.has_key('EPAB1'):
      print "Will put EPA to False for beam 1"
      self.parameters.append('EPAB1=F')
    else:
      self.parameters.append("EPAB1=%s" %self.parameterdict["EPAB1"] )
        
    if not self.parameterdict.has_key('EPAB2'):
      print "Will put EPA to False for beam 2"
      self.parameters.append('EPAB2=F')
    else:
      self.parameters.append("EPAB2=%s" %self.parameterdict["EPAB2"] )
       
    if not self.parameterdict.has_key('RECOIL'):
      print "Will set Beam_recoil to False"
      self.parameters.append('RECOIL=F')
    else:
      self.parameters.append("RECOIL=%s" %self.parameterdict["RECOIL"] )
        
    if not self.parameterdict.has_key('INITIALS'):
      print "Will set keep_initials to False"
      self.parameters.append('INITIALS=F')
    else:
      self.parameters.append("INITIALS=%s" %self.parameterdict["INITIALS"] )
        
    if not self.parameterdict.has_key('USERSPECTRUM'):
      print "Will set USER_spectrum_on to +-11"
      self.parameters.append('USERSPECTRUM=11')
    else:
      self.parameters.append("USERSPECTRUM=%s" %self.parameterdict["USERSPECTRUM"] )
    
    self.parameters = string.join(self.parameters,";")
      
    return S_OK()  

  def _applicationModule(self):
    md1 = self._createModuleDefinition()
    md1.addParameter(Parameter("evttype",     "", "string", "", "", False, False, "Process to generate"))
    md1.addParameter(Parameter("RandomSeed",   0,  "float", "", "", False, False, "Random seed for the generator"))
    md1.addParameter(Parameter("Lumi",         0,  "float", "", "", False, False, "Luminosity of beam"))
    md1.addParameter(Parameter("Model",       "", "string", "", "", False, False, "Model for generation"))
    md1.addParameter(Parameter("SteeringFile","", "string", "", "", False, False, "Steering file"))
    md1.addParameter(Parameter("JobIndex",    "", "string", "", "", False, False, "Job Index"))
    md1.addParameter(Parameter("parameters",  "", "string", "", "", False, False, "Specific steering parameters"))
    md1.addParameter(Parameter("debug",    False,   "bool", "", "", False, False, "debug mode"))
    return md1

  
  def _applicationModuleValues(self,moduleinstance):

    moduleinstance.setValue("evttype",      self.evttype)
    moduleinstance.setValue("RandomSeed",   self.seed)
    moduleinstance.setValue("Lumi",         self.lumi)
    moduleinstance.setValue("Model",        self.model)
    moduleinstance.setValue("SteeringFile", self.steeringfile)
    moduleinstance.setValue("JobIndex",     self.jobindex)
    moduleinstance.setValue("parameters",   self.parameters)
    moduleinstance.setValue("debug",        self.debug)
    
  def _userjobmodules(self,stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setUserJobFinalization(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('userjobmodules failed')
    return S_OK() 

  def _prodjobmodules(self,stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setOutputComputeDataList(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('prodjobmodules failed')
    return S_OK()

    
  
#################################################################
#            PYTHIA: Second Generator application
#################################################################    
class Pythia(Application):
  """ Call pythia.
  
  Usage:
  
  >>> py = Pythia()
  >>> py.setVersion("tt_500gev_V2")
  >>> py.setNbEvts(50)

  """
  def __init__(self,paramdict = None):
    Application.__init__(self,paramdict)
    self.appname = 'pythia'
    self._modulename = 'PythiaAnalysis'
    self._moduledescription = 'Module to run PYTHIA'

  def _applicationModule(self):
    m1 = self._createModuleDefinition()
    return m1  

  def _userjobmodules(self,stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setUserJobFinalization(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('userjobmodules failed')
    return S_OK() 

  def _prodjobmodules(self,stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setOutputComputeDataList(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('prodjobmodules failed')
    return S_OK()
      
  def _checkConsistency(self):
    if not self.version:
      return S_ERROR("Version not specified")
    
    if not self.nbevts:
      return S_ERROR("Number of events to generate not defined")
    
    if not self._jobtype == 'User':
      if not self.outputFile:
        return S_ERROR("Output File not defined")
      if not self.outputPath:
        return S_ERROR("Output Path not defined")

    return S_OK()
  
##########################################################################
#            StdhepCut: apply generator level cuts after pythia or whizard
##########################################################################
class StdhepCut(Application): 
  """ Call stdhep cut after whizard of pythia
  
  Usage:
  
  >>> py = Pythia()
  ...
  >>> cut = StdhepCut()
  >>> cut.getInputFromApp(py)
  >>> cut.setSteeringFile("mycut.cfg")
  >>> cut setMaxNbEvts(10)
  >>> cut.setNbEvtsPerFile(10)
  
  """
  def __init__(self, paramdict = None):
    self.maxevts = 0
    self.nbevtsperfile = 0
    Application.__init__(self,paramdict)

    self.appname = 'stdhepcut'
    self._modulename = 'StdHepCut'
    self._moduledescription = 'Module to cut on Generator (Whizard of PYTHIA)'

  def setMaxNbEvts(self,nbevts):
    """ Max number of events to keep in each file
    
    @param nbevts: Maximum number of events to read from input file
    @type nbevts: int
    """
    self._checkArgs( {
        'nbevts' : types.IntType
      } )
    self.maxevts = nbevts
    
  def setNbEvtsPerFile(self,nbevts):
    """ Number of events per file
    
    @param nbevts: Number of evetns to keep in each file.
    @type nbevts: int
    """
    self._checkArgs( {
        'nbevts' : types.IntType
      } )
    self.nbevtsperfile = nbevts  

  def _applicationModule(self):
    m1 = self._createModuleDefinition()
    m1.addParameter(Parameter("MaxNbEvts", 0, "int", "", "", False, False, "Number of evetns to read"))
    m1.addParameter(Parameter("debug", False, "bool", "", "", False, False, "debug mode"))

    return m1

  def _applicationModuleValues(self,moduleinstance):
    moduleinstance.setValue("MaxNbEvts",self.maxevts)
    moduleinstance.setValue("debug",    self.debug)

  def _userjobmodules(self,step):
    m1 = self._applicationModule()
    step.addModule(m1)
    m1i = step.createModuleInstance(m1.getType(),step.getType())
    self._applicationModuleValues(m1i)
    
    m2 = self._getUserOutputDataModule()
    step.addModule(m2)
    step.createModuleInstance(m2.getType(),step.getType())
    return S_OK()

  def _prodjobmodules(self,step):
    m1 = self._applicationModule()
    step.addModule(m1)
    m1i = step.createModuleInstance(m1.getType(),step.getType())
    self._applicationModuleValues(m1i)
    
    m2 = self._getComputeOutputDataListModule()
    step.addModule(m2)
    step.createModuleInstance(m2.getType(),step.getType())
    return S_OK()


  def _checkConsistency(self):
    if not self.steeringfile:
      return S_ERROR("Cut file not specified")
    elif not self.steeringfile.lower().count("lfn:") and not os.path.exists(self.steeringfile):
      return S_ERROR("Cut file not found locally and is not an LFN")
    
    if not self.maxevts:
      return S_ERROR("You did not specify how many events you need to keep per file (MaxNbEvts)")
    
    res = self._checkRequiredApp() ##Check that job order is correct
    if not res['OK']:
      return res
    
    return S_OK()
  
  def _resolveLinkedStepParameters(self,stepinstance):
    if self.inputappstep:
      stepinstance.setLink("InputFile",self.inputappstep.getType(),"OutputFile")
    return S_OK()  
    
    
##########################################################################
#            Mokka: Simulation after Whizard or StdHepCut
##########################################################################
class Mokka(Application): 
  """ Call Mokka simulator (after Whizard, Pythia or StdHepCut)
  
  Usage:
  
  >>> wh = Whizard()
  ...
  >>> mo = Mokka()
  >>> mo.getInputFromApp(wh)
  >>> mo.setSteeringFile("mycut.cfg")
  >>> mo.setMacFile('MyMacFile.mac')
  >>> mo.setStartFrom(10)
  
  """
  def __init__(self, paramdict = None):

    self.startFrom = 0
    self.macFile = ''
    self.seed = 0
    self.dbSlice = ''
    self.detectorModel = ''
    self.processID = ''
    Application.__init__(self,paramdict)
    ##Those 5 need to come after default constructor
    self._modulename = 'MokkaAnalysis'
    self._moduledescription = 'Module to run MOKKA'
    self.appname = 'mokka'    
    self.datatype = 'SIM'
    self.detectortype = 'ILD'
     
  def setRandomSeed(self,seed):
    """ Define random seed to use 
    
    @param seed: Seed to use during integration and generation. Default is Job ID.
    @type seed: int
    """
    self._checkArgs( {
        'seed' : types.IntType
      } )

    self.seed = seed    
    
  def setDetectorModel(self,detectorModel):
    """ Define detector to use for Mokka simulation 
    
    @param detectorModel: Detector Model to use for Mokka simulation. Default is ??????
    @type detectorModel: string
    """
    self._checkArgs( {
        'detectorModel' : types.StringTypes
      } )

    self.detectorModel = detectorModel    
    
  def setMacFile(self,macfile):
    """ Define Mac File
    
    @param macfile: Mac file for Mokka
    @type macfile: string
    """
    self._checkArgs( {
        'macfile' : types.StringTypes
      } )
    self.macFile = macfile
    if os.path.exists(macfile) or macfile.lower().count("lfn:"):
      self.inputSB.append(macfile) 
    
    
  def setStartFrom(self,startfrom):
    """ Define from how mokka start to read in the input file
    
    @param startfrom: from how mokka start to read the input file
    @type startfrom: int
    """
    self._checkArgs( {
        'startfrom' : types.IntType
      } )
    self.startfrom = startfrom  
    
    
  def setProcessID(self,processID):
    """ Define the ID's process
    
    @param processID: ID's process
    @type processID: string
    """
    self._checkArgs( {
        'processID' : types.StringTypes
      } )
    self.processID = processID
    
    
  def setDbSlice(self,dbSlice):
    """ Define the data base that will use mokka
    
    @param dbSlice: data base used by mokka
    @type dbSlice: string
    """
    self._checkArgs( {
        'dbSlice' : types.StringTypes
      } )
    self.dbSlice = dbSlice
    if os.path.exists(dbSlice) or dbSlice.lower().count("lfn:"):
      self.inputSB.append(dbSlice) 
    
    
  def _userjobmodules(self,stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setUserJobFinalization(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('userjobmodules failed')
    return S_OK() 

  def _prodjobmodules(self,stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setOutputComputeDataList(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('prodjobmodules failed')
    return S_OK()
  
  def _checkConsistency(self):

    if not self.version:
      return S_ERROR('No version found')   
    
    if not self.steeringfile :
      return S_ERROR('No Steering File') 
    
    res = self._checkRequiredApp()
    if not res['OK']:
      return res

    if not self._jobtype == 'User':
      if not self.outputFile:
        return S_ERROR("Output File not defined")
      if not self.outputPath:
        return S_ERROR("Output Path not defined")
   
    return S_OK()  
  
  def _applicationModule(self):
    
    md1 = self._createModuleDefinition()
    md1.addParameter(Parameter("RandomSeed",           0,    "int", "", "", False, False, "Random seed for the generator"))
    md1.addParameter(Parameter("detectorModel",       "", "string", "", "", False, False, "Detecor model for simulation"))
    md1.addParameter(Parameter("macFile",             "", "string", "", "", False, False, "Mac file"))
    md1.addParameter(Parameter("startFrom",            0, "string", "", "", False, False, "From how Mokka start to read the input file"))
    md1.addParameter(Parameter("dbSlice",             "", "string", "", "", False, False, "Data base used"))
    md1.addParameter(Parameter("ProcessID",           "", "string", "", "", False, False, "Process ID"))
    md1.addParameter(Parameter("debug",            False,   "bool", "", "", False, False, "debug mode"))
    return md1
  
  def _applicationModuleValues(self,moduleinstance):

    moduleinstance.setValue("RandomSeed",      self.seed)
    moduleinstance.setValue("detectorModel",   self.detectorModel)
    moduleinstance.setValue("macFile",         self.macFile)
    moduleinstance.setValue("startFrom",       self.startFrom)
    moduleinstance.setValue("dbSlice",         self.dbSlice)
    moduleinstance.setValue("ProcessID",       self.processID)
    moduleinstance.setValue("debug",           self.debug)

    
  def _resolveLinkedStepParameters(self,stepinstance):
    if self.inputappstep:
      stepinstance.setLink("InputFile",self.inputappstep.getType(),"OutputFile")
    return S_OK() 
  


##########################################################################
#            SLIC : Simulation after Whizard or StdHepCut
##########################################################################
class SLIC(Application): 
  """ Call SLIC simulator (after Whizard, Pythia or StdHepCut)
  
  Usage:
  
  >>> wh = Whizard()
  >>> slic = SLIC()
  >>> slic.getInputFromApp(wh)
  >>> slic.setSteeringFile("mycut.cfg")
  >>> slic.setMacFile('MyMacFile.mac')
  >>> slic.setStartFrom(10)
  
  """
  def __init__(self, paramdict = None):

    self.startFrom = 0
    self.seed = 0
    self.detectorModel = ''
    Application.__init__(self,paramdict)
    ##Those 5 need to come after default constructor
    self._modulename = 'SLICAnalysis'
    self._moduledescription = 'Module to run SLIC'
    self.appname = 'slic'    
    self.datatype = 'SIM'
    self.detectortype = 'SID'
     
  def setRandomSeed(self,seed):
    """ Define random seed to use 
    
    @param seed: Seed to use during integration and generation. Default is Job ID.
    @type seed: int
    """
    self._checkArgs( {
        'seed' : types.IntType
      } )

    self.seed = seed    
    
  def setDetectorModel(self,detectorModel):
    """ Define detector to use for Slic simulation 
    
    @param detectorModel: Detector Model to use for Slic simulation. Default is ??????
    @type detectorModel: string
    """
    self._checkArgs( {
        'detectorModel' : types.StringTypes
      } )

    self.detectorModel = detectorModel    
    
    
  def setStartFrom(self,startfrom):
    """ Define from how slic start to read in the input file
    
    @param startfrom: from how slic start to read the input file
    @type startfrom: int
    """
    self._checkArgs( {
        'startfrom' : types.IntType
      } )
    self.startfrom = startfrom  
    
    
  def _userjobmodules(self,stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setUserJobFinalization(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('userjobmodules failed')
    return S_OK() 

  def _prodjobmodules(self,stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setOutputComputeDataList(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('prodjobmodules failed')
    return S_OK()
  
  def _checkConsistency(self):

    if not self.version:
      return S_ERROR('No version found')   
    
    res = self._checkRequiredApp()
    if not res['OK']:
      return res

    if not self._jobtype == 'User':
      if not self.outputFile:
        return S_ERROR("Output File not defined")
      if not self.outputPath:
        return S_ERROR("Output Path not defined")
   
    if not self.startFrom :
      self.log.info('No startFrom define for Slic : start from the begining')
    
    return S_OK()  
  
  def _applicationModule(self):
    
    md1 = self._createModuleDefinition()
    md1.addParameter(Parameter("RandomSeed",           0,    "int", "", "", False, False, "Random seed for the generator"))
    md1.addParameter(Parameter("detectorModel",       "", "string", "", "", False, False, "Detecor model for simulation"))
    md1.addParameter(Parameter("startFrom",            0,    "int", "", "", False, False, "From how Slic start to read the input file"))
    md1.addParameter(Parameter("debug",            False,   "bool", "", "", False, False, "debug mode"))
    return md1
  
  def _applicationModuleValues(self,moduleinstance):

    moduleinstance.setValue("RandomSeed",      self.seed)
    moduleinstance.setValue("detectorModel",   self.detectorModel)
    moduleinstance.setValue("startFrom",       self.startFrom)
    moduleinstance.setValue("debug",           self.debug)

    
  def _resolveLinkedStepParameters(self,stepinstance):
    if self.inputappstep:
      stepinstance.setLink("InputFile",self.inputappstep.getType(),"OutputFile")
    return S_OK()   
  
  
##########################################################################
#            Marlin: Reconstructor after Mokka
##########################################################################
class Marlin(Application): 
  """ Call Marlin reconstructor (after Mokka simulator)
  
  Usage:
  
  >>> mo = Mokka()
  >>> marlin = Marlin()
  >>> marlin.getInputFromApp(mo)
  >>> marlin.setSteeringfile('SteeringFile.xml')
  >>> marlin.setOutputRecFile('MyOutputRecFile.rec')
  >>> marlin.setOutputDstFile('MyOutputDstFile.dst')
  
  """
  def __init__(self, paramdict = None):

    self.outputDstFile = ''
    self.outputRecFile = ''
    self.inputGearFile = ''
    Application.__init__(self,paramdict)
    ##Those 5 need to come after default constructor
    self._modulename = 'MarlinAnalysis'
    self._moduledescription = 'Module to run MARLIN'
    self.appname = 'marlin'    
    self.datatype = 'REC'
    self.detectortype = 'ILD'
     
    
  def setInputGearFile(self,inputGearFile):
    """ Define input gear file for Marlin reconstructor
    
    @param inputGearFile: input gear file for Marlin reconstrcutor
    @type inputGearFile: string
    """
    self._checkArgs( {
        'inputGearFile' : types.StringTypes
      } )

    self.inputGearFile = inputGearFile
    if os.path.exists(inputGearFile) or inputGearFile.lower().count("lfn:"):
      self.inputSB.append(inputGearFile) 
    
  def setOutputRecFile(self,outputRecFile):
    """ Define output rec file for Marlin reconstructor
    
    @param outputRecFile: output rec file for Marlin reconstructor
    @type outputRecFile: string
    """
    self._checkArgs( {
        'outputRecFile' : types.StringTypes
      } )
    self.outputRecFile = outputRecFile
    self.prodparameters[self.outputRecFile]['datatype']= 'REC'
      
    
  def setOutputDstFile(self,outputDstFile):
    """ Define output dst file for Marlin reconstructor
    
    @param outputDstFile: output dst file for Marlin reconstructor
    @type outputDstFile: string
    """
    self._checkArgs( {
        'outputDstFile' : types.StringTypes
      } )
    self.outputDstFile = outputDstFile
    self.prodparameters[self.outputDstFile]['datatype']= 'DST'
    
  def _userjobmodules(self,stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setUserJobFinalization(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('userjobmodules failed')
    return S_OK() 

  def _prodjobmodules(self,stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setOutputComputeDataList(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('prodjobmodules failed')
    return S_OK()
  
  def _checkConsistency(self):

    if not self.energy :
      self.log.error('Energy set to 0 !')
      
    if not self.nbevts :
      self.log.error('Number of events set to 0 !')
        
    if not self.version:
      return S_ERROR('Version not set!')   
    
    if not self.inputGearFile :
      self.log.info('Input GEAR file not given')

    res = self._checkRequiredApp()
    if not res['OK']:
      return res


    if not self._jobtype == 'User' :
      if not self.outputDstFile :
        return S_ERROR('Dst output file not given')  
      if not self.outputRecFile :
        return S_ERROR('Rec output file not given')
      if not self.outputPath:
        return S_ERROR("Output Path not defined")
     
    return S_OK()  
  
  def _applicationModule(self):
    
    md1 = self._createModuleDefinition()
    md1.addParameter(Parameter("inputGEAR",     '', "string", "", "", False, False, "Input GEAR file"))
    md1.addParameter(Parameter("outputDST",     '', "string", "", "", False, False, "Output DST file"))
    md1.addParameter(Parameter("outputREC",     '', "string", "", "", False, False, "Output REC file"))
    md1.addParameter(Parameter("debug",      False,   "bool", "", "", False, False, "debug mode"))
    return md1
  
  def _applicationModuleValues(self,moduleinstance):

    moduleinstance.setValue("inputGEAR",         self.inputGearFile)
    moduleinstance.setValue("outputREC",         self.outputRecFile)
    moduleinstance.setValue("outputDST",         self.outputDstFile)
    moduleinstance.setValue("debug",             self.debug)

    
  def _resolveLinkedStepParameters(self,stepinstance):
    if self.inputappstep:
      stepinstance.setLink("InputFile",self.inputappstep.getType(),"OutputFile")
    return S_OK() 
  
##########################################################################
#            LCSIM: Reconstruction after SLIC Simulation
##########################################################################
class LCSIM(Application): 
  """ Call LCSIM Reconstructor (after SLIC Simulation)
  
  Usage:
  
  >>> slic = SLIC()
  >>> lcsim = LCSIM()
  >>> lcsim.getInputFromApp(slic)
  >>> lcsim.setSteeringFile("MySteeringFile.xml")
  >>> lcsim.setStartFrom(10)
  
  """
  def __init__(self, paramdict = None):

    self.extraParams = ''
    self.aliasProperties = ''
    self.outputDstFile = ''
    self.outputRecFile = ''
    Application.__init__(self,paramdict)
    ##Those 5 need to come after default constructor
    self._modulename = 'LCSIMAnalysis'
    self._moduledescription = 'Module to run LCSIM'
    self.appname = 'lcsim'
    self.datatype = 'REC'
    self.detectortype = 'SID'
     
  def setOutputRecFile(self,outputRecFile):
    """ Define output rec file for LCSIM reconstructor
    
    @param outputRecFile: output rec file for LCSIM reconstructor
    @type outputRecFile: string
    """
    self._checkArgs( {
        'outputRecFile' : types.StringTypes
                       } )
    self.outputRecFile = outputRecFile
    self.prodparameters[self.outputRecFile]['datatype']= 'REC'
      
    
  def setOutputDstFile(self,outputDstFile):
    """ Define output dst file for LCSIM reconstructor
    
    @param outputDstFile: output dst file for LCSIM reconstructor
    @type outputDstFile: string
    """
    self._checkArgs( {
        'outputDstFile' : types.StringTypes
      } )
    self.outputDstFile = outputDstFile 
    self.prodparameters[self.outputDstFile]['datatype']= 'DST'
            
    
  def setAliasProperties(self,alias):
    """ Define the path to the alias.properties file name that will be used 
    
    @param alias: Path to the alias.properties file name that will be used
    @type alias: string
    """
    self._checkArgs( {
        'alias' : types.StringTypes
      } )

    self.aliasProperties = alias     
    if os.path.exists(alias) or alias.lower().count("lfn:"):
      self.inputSB.append(alias) 
    
    
  def setExtraParams(self,extraparams):
    """ Define command line parameters to pass to java
    
    @param extraparams: Command line parameters to pass to java
    @type extraparams: string
    """
    self._checkArgs( {
        'extraparams' : types.StringTypes
      } )

    self.extraParams = extraparams     
    
    
  def _userjobmodules(self,stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setUserJobFinalization(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('userjobmodules failed')
    return S_OK() 

  def _prodjobmodules(self,stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setOutputComputeDataList(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('prodjobmodules failed')
    return S_OK()
  
  def _checkConsistency(self):

    if not self.energy :
      self.log.error('Energy set to 0 !')
      
    if not self.nbevts :
      self.log.error('Number of events set to 0 !')
        
    if not self.version:
      return S_ERROR('No version found')   
        
    res = self._checkRequiredApp()
    if not res['OK']:
      return res
      
    return S_OK()  
  
  def _applicationModule(self):
    
    md1 = self._createModuleDefinition()
    md1.addParameter(Parameter("ExtraParams",           "", "string", "", "", False, False, "Command line parameters to pass to java"))
    md1.addParameter(Parameter("aliasproperties",       "", "string", "", "", False, False, "Path to the alias.properties file name that will be used"))
    md1.addParameter(Parameter("outputREC",             "", "string", "", "", False, False, "REC output file"))
    md1.addParameter(Parameter("outputDST",             "", "string", "", "", False, False, "DST output file"))
    md1.addParameter(Parameter("debug",               None, "string", "", "", False, False, "debug mode"))
    return md1
  
  def _applicationModuleValues(self,moduleinstance):

    moduleinstance.setValue("ExtraParams",        self.extraParams)
    moduleinstance.setValue("aliasproperties",    self.aliasProperties)
    moduleinstance.setValue("outputREC",          self.outputRecFile)
    moduleinstance.setValue("outputDST",          self.outputDSTFile)
    moduleinstance.setValue("debug",              self.debug)
    
  def _resolveLinkedStepParameters(self,stepinstance):
    if self.inputappstep:
      stepinstance.setLink("InputFile",self.inputappstep.getType(),"OutputFile")
    return S_OK()
  
##########################################################################
#            SLICPandora : Run Pandora in the SID context
##########################################################################
class SLICPandora(Application): 
  """ Call SLICPandora 
  
  Usage:
  
  >>> lcsim = LCSIM()
  ...
  >>> slicpandora = SLICPandora()
  >>> slicpandora.getInputFromApp(lcsim)
  >>> slicpandora.setPandoraSettings("~/GreatPathToHeaven/MyPandoraSettings.xml")
  >>> slicpandora.setStartFrom(10)
  
  """
  def __init__(self, paramdict = None):

    self.startFrom = 0
    self.pandoraSettings = ''
    self.detectorModel = ''
    Application.__init__(self,paramdict)
    ##Those 5 need to come after default constructor
    self._modulename = 'SLICPandoraAnalysis'
    self._moduledescription = 'Module to run SLICPANDORA'
    self.appname = 'slicpandora'    
    self.datatype = 'SIM'
    self.detectortype = 'SID'
        
    
  def setDetectorModel(self,detectorModel):
    """ Define detector to use for SlicPandora simulation 
    
    @param detectorModel: Detector Model to use for SlicPandora simulation. Default is ??????
    @type detectorModel: string
    """
    self._checkArgs( {
        'detectorModel' : types.StringTypes
      } )

    self.detectorModel = detectorModel    
    if os.path.exists(detectorModel) or detectorModel.lower().count("lfn:"):
      self.inputSB.append(detectorModel)   
    
  def setStartFrom(self,startfrom):
    """ Define from how slicpandora start to read in the input file
    
    @param startfrom: from how slicpandora start to read the input file
    @type startfrom: int
    """
    self._checkArgs( {
        'startfrom' : types.IntType
      } )
    self.startfrom = startfrom     
    
  def setPandoraSettings(self,pandoraSettings):
    """ Define the path where pandora settings are
    
    @param pandoraSettings: path where pandora settings are
    @type pandoraSettings: string
    """
    self._checkArgs( {
        'pandoraSettings' : types.StringTypes
      } )
    self.pandoraSettings = pandoraSettings  
    if os.path.exists(pandoraSettings) or pandoraSettings.lower().count("lfn:"):
      self.inputSB.append(pandoraSettings)    
    
  def _userjobmodules(self,stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setUserJobFinalization(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('userjobmodules failed')
    return S_OK() 

  def _prodjobmodules(self,stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setOutputComputeDataList(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('prodjobmodules failed')
    return S_OK()
  
  def _checkConsistency(self):

    if not self.version:
      return S_ERROR('No version found')   
    
    if not self.pandoraSettings :
      return S_ERROR('No Pandora Settings')
    
    res = self._checkRequiredApp()
    if not res['OK']:
      return res

    if not self._jobtype == 'User':
      if not self.outputFile:
        return S_ERROR("Output File not defined")
      if not self.outputPath:
        return S_ERROR("Output Path not defined")
      
    if not self.startFrom :
      self.log.info('No startFrom define for SlicPandora : start from the begining')
      
    return S_OK()  
  
  def _applicationModule(self):
    
    md1 = self._createModuleDefinition()
    md1.addParameter(Parameter("PandoraSettings",   "", "string", "", "", False, False, "Random seed for the generator"))
    md1.addParameter(Parameter("DetectorXML",       "", "string", "", "", False, False, "Detecor model for simulation"))
    md1.addParameter(Parameter("startFrom",          0,    "int", "", "", False, False, "From how SlicPandora start to read the input file"))
    md1.addParameter(Parameter("debug",          False,   "bool", "", "", False, False, "debug mode"))
    return md1
  
  def _applicationModuleValues(self,moduleinstance):

    moduleinstance.setValue("PandoraSettings",    self.pandoraSettings)
    moduleinstance.setValue("DetectorXML",        self.detectorModel)
    moduleinstance.setValue("startFrom",          self.startFrom)
    moduleinstance.setValue("debug",              self.debug)

    
  def _resolveLinkedStepParameters(self,stepinstance):
    if self.inputappstep:
      stepinstance.setLink("InputFile",self.inputappstep.getType(),"OutputFile")
    return S_OK()

