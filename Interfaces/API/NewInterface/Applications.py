"""
This module contains the definition of the different applications that can
be used to create jobs.

Example usage:

>>> from ILCDIRAC.Interfaces.API.NewInterface.Applications import *
>>> from ILCDIRAC.Interfaces.API.NewInterface.UserJob import * 
>>> from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
>>> dirac = DiracILC()
>>> job = UserJob()
>>> ga = GenericApplication()
>>> ga.setScript("myscript.py")
>>> ga.setArguments("some arguments")
>>> ga.setDependency({"mokka":"v0706P08","marlin":"v0111Prod"})
>>> job.append(ga)
>>> job.submit(dirac)

It's also possible to set all the application's properties in the constructor

>>> ga = GenericApplication({"Script":"myscript.py", "Arguments":"some arguments", \
         "Dependency":{"mokka":"v0706P08","marlin":"v0111Prod"}})

but this is more an expert's functionality. 

Running:

>>> help(GenericApplication)

prints out all the available methods.

@author: Stephane Poss
@author: Remi Ete
@author: Ching Bon Lam
"""

from ILCDIRAC.Interfaces.API.NewInterface.LCApplication import LCApplication as Application
from ILCDIRAC.Interfaces.API.NewInterface.LCUtilityApplication import LCUtilityApplication
from ILCDIRAC.Core.Utilities.GeneratorModels          import GeneratorModels
from ILCDIRAC.Core.Utilities.InstalledFiles           import Exists
from ILCDIRAC.Core.Utilities.WhizardOptions           import WhizardOptions, getDict

from DIRAC.Resources.Catalog.FileCatalogClient        import FileCatalogClient
from DIRAC.Core.Workflow.Parameter                    import Parameter
from DIRAC                                            import S_OK, S_ERROR
from ILCDIRAC.Core.Utilities.CheckXMLValidity         import CheckXMLValidity

from math import modf
from decimal import Decimal
import types, os

__RCSID__ = "$Id$"


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
  
  Use setExtraCLIArguments if you want to add arguments to the PandoraFrontend call
  
  """
  def __init__(self, paramdict = None):

    self.StartFrom = 0
    self.PandoraSettings = ''
    self.DetectorModel = ''
    super(SLICPandora, self).__init__( paramdict)
    ##Those 5 need to come after default constructor
    self._modulename = 'SLICPandoraAnalysis'
    self._moduledescription = 'Module to run SLICPANDORA'
    self.appname = 'slicpandora'    
    self.datatype = 'REC'
    self.detectortype = 'SID'
    self._paramsToExclude.extend( [ "outputDstPath", "outputRecPath", "OutputDstFile", "OutputRecFile" ] )
    
  def setDetectorModel(self, detectorModel):
    """ Define detector to use for SlicPandora simulation 
    
    @param detectorModel: Detector Model to use for SlicPandora simulation. 
    @type detectorModel: string
    """
    self._checkArgs( { 'detectorModel' : types.StringTypes } )

    self.DetectorModel = detectorModel    
    if os.path.exists(detectorModel) or detectorModel.lower().count("lfn:"):
      self.inputSB.append(detectorModel)   
    
  def setStartFrom(self, startfrom):
    """ Optional: Define from where slicpandora start to read in the input file
    
    @param startfrom: from how slicpandora start to read the input file
    @type startfrom: int
    """
    self._checkArgs( { 'startfrom' : types.IntType } )
    self.StartFrom = startfrom     
    
  def setPandoraSettings(self, pandoraSettings):
    """ Optional: Define the path where pandora settings are
    
    @param pandoraSettings: path where pandora settings are
    @type pandoraSettings: string
    """
    self._checkArgs( { 'pandoraSettings' : types.StringTypes } )
    self.PandoraSettings = pandoraSettings  
    if os.path.exists(pandoraSettings) or pandoraSettings.lower().count("lfn:"):
      self.inputSB.append(pandoraSettings)    
    
  def _userjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setUserJobFinalization(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('userjobmodules failed')
    return S_OK() 

  def _prodjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setOutputComputeDataList(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('prodjobmodules failed')
    return S_OK()
  
  def _checkConsistency(self):

    if not self.Version:
      return S_ERROR('No version found')   

    if self.SteeringFile:
      if not os.path.exists(self.SteeringFile) and not self.SteeringFile.lower().count("lfn:"):
        res = Exists(self.SteeringFile)
        if not res['OK']:
          return res  
    
    if not self.PandoraSettings:
      return S_ERROR("PandoraSettings not set, you need it")
    
    #res = self._checkRequiredApp()
    #if not res['OK']:
    #  return res
      
    if not self.StartFrom :
      self._log.info('No startFrom defined for SlicPandora : start from the begining')
      
    if not self._jobtype == 'User':
      self.prodparameters['slicpandora_steeringfile'] = self.SteeringFile
      self.prodparameters['slicpandora_detectorModel'] = self.DetectorModel
      
        
    return S_OK()  
  
  def _applicationModule(self):
    
    md1 = self._createModuleDefinition()
    md1.addParameter(Parameter("pandorasettings",   "", "string", "", "", False, False, 
                               "Pandora Settings"))
    md1.addParameter(Parameter("detectorxml",       "", "string", "", "", False, False, 
                               "Detector model for simulation"))
    md1.addParameter(Parameter("startFrom",          0,    "int", "", "", False, False, 
                               "From how SlicPandora start to read the input file"))
    md1.addParameter(Parameter("debug",          False,   "bool", "", "", False, False, 
                               "debug mode"))
    return md1
  
  def _applicationModuleValues(self, moduleinstance):

    moduleinstance.setValue("pandorasettings",    self.PandoraSettings)
    moduleinstance.setValue("detectorxml",        self.DetectorModel)
    moduleinstance.setValue("startFrom",          self.StartFrom)
    moduleinstance.setValue("debug",              self.Debug)

  def _checkWorkflowConsistency(self):
    return self._checkRequiredApp()
    
  def _resolveLinkedStepParameters(self, stepinstance):
    if type(self._linkedidx) == types.IntType:
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
    return S_OK()


#################################################################
#            CheckCollection : Helper to check collection 
#################################################################  
class CheckCollections(LCUtilityApplication):
  """ Helper to check collection 
  
  Example:
  
  >>> check = OverlayInput()
  >>> check.setInputFile( [slcioFile_1.slcio , slcioFile_2.slcio , slcioFile_3.slcio] )
  >>> check.setCollections( ["some_collection_name"] )
  
  """
  def __init__(self, paramdict = None):
    self.Collections = []
    super(CheckCollections, self).__init__( paramdict )
    if not self.Version:
      self.Version = 'HEAD'
    self._modulename = "CheckCollections"
    self.appname = 'lcio'
    self._moduledescription = 'Helper call to define Overlay processor/driver inputs'

  def setCollections(self, CollectionList):
    """ Set collections. Must be a list
    
    @param CollectionList: Collections. Must be a list
    @type CollectionList: list
    
    """  
    self._checkArgs( { 'CollectionList' : types.ListType } )
    
    self.Collections = CollectionList
    return S_OK()


  def _applicationModule(self):
    m1 = self._createModuleDefinition()
    m1.addParameter( Parameter( "collections",         [], "list", "", "", False, False, "Collections to check for" ) )
    m1.addParameter( Parameter( "debug",            False, "bool", "", "", False, False, "debug mode"))
    return m1
  

  def _applicationModuleValues(self, moduleinstance):
    moduleinstance.setValue('collections',             self.Collections)
    moduleinstance.setValue('debug',                   self.Debug)
  
  def _userjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setUserJobFinalization(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('userjobmodules failed')
    return S_OK() 

  def _prodjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setOutputComputeDataList(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('prodjobmodules failed')
    return S_OK()    

  def _checkConsistency(self):
    """ Checks that all needed parameters are set
    """

    if not self.Collections :
      return S_ERROR('No collections to check')

    res = self._checkRequiredApp()
    if not res['OK']:
      return res
      
    return S_OK()
  
  def _resolveLinkedStepParameters(self, stepinstance):
    if type(self._linkedidx) == types.IntType:
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
    return S_OK()
  
#################################################################
#     SLCIOConcatenate : Helper to concatenate SLCIO files 
#################################################################  
class SLCIOConcatenate(LCUtilityApplication):
  """ Helper to concatenate slcio files
  
  Example:
  
  >>> slcioconcat = SLCIOConcatenate()
  >>> slcioconcat.setInputFile( ["slcioFile_1.slcio" , "slcioFile_2.slcio" , "slcioFile_3.slcio"] )
  >>> slcioconcat.setOutputFile("myNewSLCIOFile.slcio")
  
  """
  def __init__(self, paramdict = None):

    super(SLCIOConcatenate, self).__init__( paramdict)
    if not self.Version:
      self.Version = 'HEAD'
    self._modulename = "LCIOConcatenate"
    self.appname = 'lcio'
    self._moduledescription = 'Helper call to concatenate SLCIO files'

  def _applicationModule(self):
    m1 = self._createModuleDefinition()
    m1.addParameter( Parameter( "debug",            False,  "bool", "", "", False, False, "debug mode"))
    return m1

  def _applicationModuleValues(self, moduleinstance):
    moduleinstance.setValue('debug',                       self.Debug)
   
  def _userjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setUserJobFinalization(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('userjobmodules failed')
    return S_OK() 

  def _prodjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setOutputComputeDataList(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('prodjobmodules failed')
    return S_OK()    

  def _checkConsistency(self):
    """ Checks that all needed parameters are set
    """
      
    if not self.OutputFile and self._jobtype =='User' :
      self.setOutputFile('LCIOFileConcatenated.slcio')
      self._log.notice('No output file name specified. Output file : LCIOFileConcatenated.slcio')

    if not self._jobtype == 'User':
      self._listofoutput.append({"outputFile":"@{OutputFile}", "outputPath":"@{OutputPath}", 
                                 "outputDataSE":'@{OutputSE}'})

    #res = self._checkRequiredApp()
    #if not res['OK']:
    #  return res
      
    return S_OK()
  
  def _checkWorkflowConsistency(self):
    return self._checkRequiredApp()
  
  def _resolveLinkedStepParameters(self, stepinstance):
    if type(self._linkedidx) == types.IntType:
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
    return S_OK()
  
#################################################################
#     SLCIOSplit : Helper to split SLCIO files 
#################################################################  
class SLCIOSplit(LCUtilityApplication):
  """ Helper to split slcio files
  
  Example:
  
  >>> slciosplit = SLCIOSplit()
  >>> slciosplit.setInputFile( "slcioFile_1.slcio" )
  >>> slciosplit.setNumberOfEventsPerFile(100)
  
  """
  def __init__(self, paramdict = None):
    self.NumberOfEventsPerFile = 0
    super(SLCIOSplit, self).__init__( paramdict)
    if not self.Version:
      self.Version = 'HEAD'
    self._modulename = "LCIOSplit"
    self.appname = 'lcio'
    self._moduledescription = 'Helper call to split SLCIO files'

  def setNumberOfEventsPerFile(self, numberofevents):
    """ Number of events to have in each file
    """
    self._checkArgs( { 'numberofevents' : types.IntType } )
    self.NumberOfEventsPerFile = numberofevents

  

  def _applicationModule(self):
    m1 = self._createModuleDefinition()
    m1.addParameter( Parameter( "debug",            False,  "bool", "", "", False, False, "debug mode"))
    m1.addParameter( Parameter( "nbEventsPerSlice",     0,   "int", "", "", False, False, 
                                "Number of events per output file"))
    return m1

  def _applicationModuleValues(self, moduleinstance):
    moduleinstance.setValue('debug',            self.Debug)
    moduleinstance.setValue('nbEventsPerSlice', self.NumberOfEventsPerFile)

  def _userjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setUserJobFinalization(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('userjobmodules failed')
    return S_OK() 

  def _prodjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setOutputComputeDataList(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('prodjobmodules failed')
    return S_OK()    

  def _checkConsistency(self):
    """ Checks that all needed parameters are set
    """
    
    #steal the datatype and detector type from the job (for production):
    if hasattr(self._job, "datatype"):
      self.datatype = self._job.datatype
    if hasattr(self._job, "detector"):
      self.detectortype = self._job.detector
    
    #This is needed for metadata registration
    self.NbEvts = self.NumberOfEventsPerFile
      
    if not self.OutputFile and self._jobtype =='User' :
      self._log.error('No output file name specified.')

    if not self._jobtype == 'User':
      self._listofoutput.append({"outputFile":"@{OutputFile}", "outputPath":"@{OutputPath}", 
                                 "outputDataSE":'@{OutputSE}'})
      self.prodparameters['nb_events_per_file'] = self.NumberOfEventsPerFile

      
    return S_OK()
  
  def _checkWorkflowConsistency(self):
    return self._checkRequiredApp()
  
  def _resolveLinkedStepParameters(self, stepinstance):
    if type(self._linkedidx) == types.IntType:
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
    return S_OK()

#################################################################
#     StdHepSplit : Helper to split Stdhep files 
#################################################################  
class StdHepSplit(LCUtilityApplication):
  """ Helper to split stdhep files
  
  Example:
  
  >>> stdhepsplit = StdHepSplit()
  >>> stdhepsplit.setInputFile( "File_1.stdhep" )
  >>> stdhepsplit.setNumberOfEventsPerFile(100)
  >>> stdhepsplit.setOutputFile("somefile.stdhep")
  
  The outpufiles will then be somefile_X.stdhep, where X corresponds to the slice index
  
  """
  def __init__(self, paramdict = None):
    self.NumberOfEventsPerFile = 0
    super(StdHepSplit, self).__init__( paramdict )
    if not self.Version:
      self.Version = 'V2'
    self._modulename = "StdHepSplit"
    self.appname = 'stdhepsplit'
    self._moduledescription = 'Helper call to split Stdhep files'

  def setNumberOfEventsPerFile(self, numberofevents):
    """ Number of events to have in each file
    """
    self._checkArgs( { 'numberofevents' : types.IntType } )
    self.NumberOfEventsPerFile = numberofevents

  

  def _applicationModule(self):
    m1 = self._createModuleDefinition()
    m1.addParameter( Parameter( "debug",            False,  "bool", "", "", False, False, "debug mode"))
    m1.addParameter( Parameter( "nbEventsPerSlice",     0,   "int", "", "", False, False, 
                                "Number of events per output file"))
    return m1

  def _applicationModuleValues(self, moduleinstance):
    moduleinstance.setValue('debug',            self.Debug)
    moduleinstance.setValue('nbEventsPerSlice', self.NumberOfEventsPerFile)

  def _userjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setUserJobFinalization(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('userjobmodules failed')
    return S_OK() 

  def _prodjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setOutputComputeDataList(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('prodjobmodules failed')
    return S_OK()    

  def _checkConsistency(self):
    """ Checks that all needed parameters are set
    """
    
    #steal the datatype and detector type from the job (for production):
    if hasattr(self._job, "datatype"):
      self.datatype = self._job.datatype
    
    #This is needed for metadata registration
    self.NbEvts = self.NumberOfEventsPerFile
      
    if not self.OutputFile and self._jobtype =='User' :
      self._log.notice('No output file name specified.')

    if not self._jobtype == 'User':
      self._listofoutput.append({"outputFile":"@{OutputFile}", "outputPath":"@{OutputPath}", 
                                 "outputDataSE":'@{OutputSE}'})
      self.prodparameters['nb_events_per_file'] = self.NumberOfEventsPerFile

      
    return S_OK()
  
  def _checkWorkflowConsistency(self):
    return self._checkRequiredApp()
  
  def _resolveLinkedStepParameters(self, stepinstance):
    if type(self._linkedidx) == types.IntType:
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
    return S_OK()
    
    
#################################################################
#     Tomato : Helper to filter generator selection 
#################################################################  
class Tomato(Application):
  """ Helper application over Tomato analysis
  
  Example:
  
  >>> cannedTomato = Tomato()
  >>> cannedTomato.setInputFile ( [pouette_1.slcio , pouette_2.slcio] )
  >>> cannedTomato.setSteeringFile ( MySteeringFile.xml )
  >>> cannedTomato.setLibTomato ( MyUserVersionOfTomato )

  
  """
  def __init__(self, paramdict = None):

    self.LibTomato = ''
    super(Tomato, self).__init__( paramdict )
    self.Version = self.Version if self.Version else 'HEAD'
    self._modulename = "TomatoAnalysis"
    self.appname = 'tomato'
    self._moduledescription = 'Helper Application over Marlin reconstruction'
      
  def setLibTomato(self, libTomato):
    """ Optional: Set the the optional Tomato library with the user version
    
    @param libTomato: Tomato library
    @type libTomato: string
    
    """  
    self._checkArgs( { 'libTomato' : types.StringTypes } )
    
    self.LibTomato = libTomato
    return S_OK()


  def _applicationModule(self):
    m1 = self._createModuleDefinition()
    m1.addParameter( Parameter( "libTomato",           '',   "string", "", "", False, False, "Tomato library" ))
    m1.addParameter( Parameter( "debug",            False,     "bool", "", "", False, False, "debug mode"))
    return m1
  

  def _applicationModuleValues(self, moduleinstance):
    moduleinstance.setValue('libTomato',     self.LibTomato)
    moduleinstance.setValue('debug',         self.Debug)
   
  def _userjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setUserJobFinalization(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('userjobmodules failed')
    return S_OK() 

  def _prodjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setOutputComputeDataList(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('prodjobmodules failed')
    return S_OK()    

  def _checkConsistency(self):
    """ Checks that all needed parameters are set
    """ 

    if not self.Version:
      return S_ERROR("You need to specify which version of Marlin to use.")
    
    if not self.LibTomato :
      self._log.info('Tomato library not given. It will run without it')

    #res = self._checkRequiredApp()
    #if not res['OK']:
    #  return res
      
    return S_OK()  

  def _checkWorkflowConsistency(self):
    return self._checkRequiredApp()

  def _resolveLinkedStepParameters(self, stepinstance):
    
    if type(self._linkedidx) == types.IntType:
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
    return S_OK()

#######################################################################################
# This application is used to obtain the host information. It has no input/output, only the log file matters
#######################################################################################
class CheckWNs(Application):
  """ Small utility to probe a worker node: list the machine's properties, the sharedare, 
  and check if CVMFS is present
  """
  def __init__(self, paramdict = None):
    super(CheckWNs, self).__init__( paramdict )
    self._modulename = "AnalyseWN"
    self.appname = 'analysewns'
    self._moduledescription = 'Analyse the WN on which this app runs'
    self.Version = "1"
    self.accountInProduction = False
    
  def _applicationModule(self):
    m1 = self._createModuleDefinition()
    return m1
  
  def _userjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    if not res1["OK"]:
      return S_ERROR('userjobmodules failed')
    return S_OK() 
  
  def _prodjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    if not res1["OK"]:
      return S_ERROR('prodjobmodules failed')
    return S_OK()    

  def _checkConsistency(self):
    """ Checks that all needed parameters are set
    """ 
    return S_OK()  
