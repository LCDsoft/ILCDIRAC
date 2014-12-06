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
