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
##########################################################################
#            Mokka: Simulation after Whizard or StdHepCut
##########################################################################
class Mokka(Application): 
  """ Call Mokka simulator (after Whizard, Pythia or StdHepCut)
  
  To ensure reproductibility, the RandomSeed is used as mcRunNumber. By default it's the jobID.
  
  Usage:
  
  >>> wh = Whizard()
  ...
  >>> mo = Mokka()
  >>> mo.getInputFromApp(wh)
  >>> mo.setSteeringFile("mysteer.steer")
  >>> mo.setMacFile('MyMacFile.mac')
  >>> mo.setStartFrom(10)
  
  Use setExtraCLIArguments if you want to pass CLI arguments to Mokka
  
  """
  def __init__(self, paramdict = None):

    self.StartFrom = 0
    self.MacFile = ''
    self.RandomSeed = 0
    self.mcRunNumber = 0
    self.DbSlice = ''
    self.DetectorModel = ''
    self.ProcessID = ''
    super(Mokka, self).__init__( paramdict )
    ##Those 5 need to come after default constructor
    self._modulename = 'MokkaAnalysis'
    self._moduledescription = 'Module to run MOKKA'
    self.appname = 'mokka'    
    self.datatype = 'SIM'
    self.detectortype = 'ILD'
    self._paramsToExclude.extend( [ "outputDstPath", "outputRecPath", "OutputDstFile", "OutputRecFile" ] )
     
  def setRandomSeed(self, RandomSeed):
    """ Optional: Define random seed to use. Default is JobID. 
    
    Also used as mcRunNumber.
    
    @param RandomSeed: Seed to use during integration and generation. Default is Job ID.
    @type RandomSeed: int
    """
    self._checkArgs( { 'RandomSeed' : types.IntType } )

    self.RandomSeed = RandomSeed    
    
  def setmcRunNumber(self, runnumber):
    """ Optional: Define mcRunNumber to use. Default is 0. In Production jobs, is equal to RandomSeed
        
    @param runnumber: mcRunNumber parameter of Mokka
    @type runnumber: int
    """
    self._checkArgs( { 'runnumber' : types.IntType } )

    self.mcRunNumber = runnumber    
    
  def setDetectorModel(self, detectorModel):
    """ Define detector to use for Mokka simulation 
    
    @param detectorModel: Detector Model to use for Mokka simulation. Default is ??????
    @type detectorModel: string
    """
    self._checkArgs( { 'detectorModel' : types.StringTypes } )

    self.DetectorModel = detectorModel    
    
  def setMacFile(self, macfile):
    """ Optional: Define Mac File. Useful if using particle gun.
    
    @param macfile: Mac file for Mokka
    @type macfile: string
    """
    self._checkArgs( { 'macfile' : types.StringTypes } )
    self.MacFile = macfile  
    if os.path.exists(macfile) or macfile.lower().count("lfn:"):
      self.inputSB.append(macfile)
    elif self.MacFile:
      self._log.notice("Mac file not found locally and is not an lfn, I hope you know what you are doing...") 
      self._log.notice("MacFile:", self.MacFile) 
    else:
      pass
    
  def setStartFrom(self, startfrom):
    """ Optional: Define from where mokka starts to read in the generator file
    
    @param startfrom: from how mokka start to read the input file
    @type startfrom: int
    """
    self._checkArgs( { 'startfrom' : types.IntType } )
    self.StartFrom = startfrom  
    
    
  def setProcessID(self, processID):
    """ Optional: Define the processID. This is added to the event header.
    
    @param processID: ID's process
    @type processID: string
    """
    self._checkArgs( { 'processID' : types.StringTypes } )
    self.ProcessID = processID
    
    
  def setDbSlice(self, dbSlice):
    """ Optional: Define the data base that will use mokka
    
    @param dbSlice: data base used by mokka
    @type dbSlice: string
    """
    self._checkArgs( { 'dbSlice' : types.StringTypes } )
    self.DbSlice = dbSlice
    if os.path.exists(dbSlice) or dbSlice.lower().count("lfn:"):
      self.inputSB.append(dbSlice)
    elif dbSlice:
      self._log.notice("Slice not found locally and is not an lfn, I hope you know what you are doing...")
      self._log.notice("DB slice:", self.DbSlice)
    else:
      pass
      
    
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
    
    if not self.SteeringFile :
      return S_ERROR('No Steering File') 
    
    if not os.path.exists(self.SteeringFile) and not self.SteeringFile.lower().count("lfn:"):
      #res = Exists(self.SteeringFile)
      res = S_OK()
      if not res['OK']:
        return res  
    
    #res = self._checkRequiredApp()
    #if not res['OK']:
    #  return res

    if not self._jobtype == 'User':
      self._listofoutput.append({"outputFile":"@{OutputFile}", "outputPath":"@{OutputPath}", "outputDataSE":'@{OutputSE}'})
      self.prodparameters['mokka_steeringfile'] = self.SteeringFile
      if self.DetectorModel:
        self.prodparameters['mokka_detectormodel'] = self.DetectorModel
      self.prodparameters['detectorType'] = self.detectortype
   
    return S_OK()  
  
  def _applicationModule(self):
    
    md1 = self._createModuleDefinition()
    md1.addParameter(Parameter("RandomSeed",           0,    "int", "", "", False, False, 
                               "Random seed for the generator"))
    md1.addParameter(Parameter("mcRunNumber",          0,    "int", "", "", False, False, 
                               "mcRunNumber parameter for Mokka"))
    md1.addParameter(Parameter("detectorModel",       "", "string", "", "", False, False, 
                               "Detector model for simulation"))
    md1.addParameter(Parameter("macFile",             "", "string", "", "", False, False, "Mac file"))
    md1.addParameter(Parameter("startFrom",            0,    "int", "", "", False, False, 
                               "From where Mokka start to read the input file"))
    md1.addParameter(Parameter("dbSlice",             "", "string", "", "", False, False, "Data base used"))
    md1.addParameter(Parameter("ProcessID",           "", "string", "", "", False, False, "Process ID"))
    md1.addParameter(Parameter("debug",            False,   "bool", "", "", False, False, "debug mode"))
    return md1
  
  def _applicationModuleValues(self, moduleinstance):

    moduleinstance.setValue("RandomSeed",      self.RandomSeed)
    moduleinstance.setValue("detectorModel",   self.DetectorModel)
    moduleinstance.setValue("mcRunNumber",     self.mcRunNumber)
    moduleinstance.setValue("macFile",         self.MacFile)
    moduleinstance.setValue("startFrom",       self.StartFrom)
    moduleinstance.setValue("dbSlice",         self.DbSlice)
    moduleinstance.setValue("ProcessID",       self.ProcessID)
    moduleinstance.setValue("debug",           self.Debug)

  def _checkWorkflowConsistency(self):
    return self._checkRequiredApp()
    
  def _resolveLinkedStepParameters(self, stepinstance):
    if type(self._linkedidx) == types.IntType:
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
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
  >>> slic.setSteeringFile("mymacrofile.mac")
  >>> slic.setStartFrom(10)
  
  Use setExtraCLIArguments in case you want to use CLI parameters
  
  """
  def __init__(self, paramdict = None):

    self.StartFrom = 0
    self.RandomSeed = 0
    self.DetectorModel = ''
    super(SLIC, self).__init__( paramdict )
    ##Those 5 need to come after default constructor
    self._modulename = 'SLICAnalysis'
    self._moduledescription = 'Module to run SLIC'
    self.appname = 'slic'    
    self.datatype = 'SIM'
    self.detectortype = 'SID'
    self._paramsToExclude.extend( [ "outputDstPath", "outputRecPath", "OutputDstFile", "OutputRecFile" ] )

     
  def setRandomSeed(self, RandomSeed):
    """ Optional: Define random seed to use. Default is Job ID.
    
    @param RandomSeed: Seed to use during simulation. 
    @type RandomSeed: int
    """
    self._checkArgs( { 'RandomSeed' : types.IntType } )

    self.RandomSeed = RandomSeed    
    
  def setDetectorModel(self, detectorModel):
    """ Define detector to use for Slic simulation 
    
    @param detectorModel: Detector Model to use for Slic simulation. Default is ??????
    @type detectorModel: string
    """
    self._checkArgs( { 'detectorModel' : types.StringTypes } )
    if detectorModel.lower().count("lfn:"):
      self.inputSB.append(detectorModel)
    elif detectorModel.lower().count(".zip"):
      if os.path.exists(detectorModel):
        self.inputSB.append(detectorModel)
      else:
        self._log.notice("Specified detector model does not exist locally, I hope you know what you're doing")
    
    
    self.DetectorModel = os.path.basename(detectorModel).replace(".zip","")
    
    
  def setStartFrom(self, startfrom):
    """ Optional: Define from how slic start to read in the input file
    
    @param startfrom: from how slic start to read the input file
    @type startfrom: int
    """
    self._checkArgs( { 'startfrom' : types.IntType } )
    self.StartFrom = startfrom  
    
    
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
    
    #res = self._checkRequiredApp()
    #if not res['OK']:
    #  return res

    if not self._jobtype == 'User':
      self._listofoutput.append({"outputFile":"@{OutputFile}", "outputPath":"@{OutputPath}", 
                                 "outputDataSE":'@{OutputSE}'})
      self.prodparameters['slic_steeringfile'] = self.SteeringFile
      self.prodparameters['detectorType'] = self.detectortype
      if self.DetectorModel:
        self.prodparameters['slic_detectormodel'] = self.DetectorModel
   
    if not self.StartFrom :
      self._log.info('No startFrom defined for Slic : start from the begining')
    
    return S_OK()  
  
  def _applicationModule(self):
    
    md1 = self._createModuleDefinition()
    md1.addParameter(Parameter("RandomSeed",           0,    "int", "", "", False, False, 
                               "Random seed for the generator"))
    md1.addParameter(Parameter("detectorModel",       "", "string", "", "", False, False, 
                               "Detecor model for simulation"))
    md1.addParameter(Parameter("startFrom",            0,    "int", "", "", False, False, 
                               "From how Slic start to read the input file"))
    md1.addParameter(Parameter("debug",            False,   "bool", "", "", False, False, "debug mode"))
    return md1
  
  def _applicationModuleValues(self, moduleinstance):

    moduleinstance.setValue("RandomSeed",      self.RandomSeed)
    moduleinstance.setValue("detectorModel",   self.DetectorModel)
    moduleinstance.setValue("startFrom",       self.StartFrom)
    moduleinstance.setValue("debug",           self.Debug)

  def _checkWorkflowConsistency(self):
    return self._checkRequiredApp()
    
  def _resolveLinkedStepParameters(self, stepinstance):
    if type(self._linkedidx) == types.IntType:
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
    return S_OK()   
  
  
#################################################################
#            OverlayInput : Helper call to define 
#              Overlay processor/driver inputs
#################################################################  
from ILCDIRAC.Workflow.Modules.OverlayInput import allowedBkg
from DIRAC.ConfigurationSystem.Client.Helpers.Operations            import Operations

class OverlayInput(LCUtilityApplication):
  """ Helper call to define Overlay processor/driver inputs. 
  
  Example:
  
  >>> over = OverlayInput()
  >>> over.setBXOverlay(300)
  >>> over.setGGToHadInt(3.2)
  >>> over.setNbSigEvtsPerJob(10)
  >>> over.setBkgEvtType("gghad")
  
  """
  def __init__(self, paramdict = None):
    self._ops = Operations()
    self.BXOverlay = None
    self.GGToHadInt = 0
    self.NbSigEvtsPerJob = 0
    self.BkgEvtType = ''
    self.ProdID = 0
    self.Machine = 'clic_cdr'
    self.DetectorModel = ''
    self.useEnergyForFileLookup = True
    super(OverlayInput, self).__init__( paramdict )
    self.Version = '1'
    self._modulename = "OverlayInput"
    self.appname = self._modulename
    self._moduledescription = 'Helper call to define Overlay processor/driver inputs'
    self.accountInProduction = False
    self._paramsToExclude.append('_ops')
    self.pathToOverlayFiles = ''
    
  def setMachine(self, machine):
    """ Define the machine to use, clic_cdr or ilc_dbd
    """
    self._checkArgs( { 'machine' : types.StringTypes } )
    self.Machine = machine

  def setProdID(self, pid):
    """ Define the prodID to use as input, experts only
    """
    self._checkArgs( {'pid': types.IntType})
    self.ProdID = pid
    return S_OK()

  def setUseEnergyForFileLookup(self, useEnergyForFileLookup):
    """
    Sets the flag to use the energy meta data in the search of the background files.
    Disable the energy when you want to use files created for a different energy than the signal events

    @param useEnergyForFileLookup: Use the Energy in the metadata search or not
    @type useEnergyForFileLookup: bool
    """
    self._checkArgs( {'useEnergyForFileLookup': types.BooleanType } )
    self.useEnergyForFileLookup = useEnergyForFileLookup
    return S_OK()

  def setOverlayBXPerSigEvt( self, bxoverlay):
    """ Define number bunch crossings to overlay for each signal event. 
    This is used to determine the number of required overlay events.
    It does not modify any of the actual application parameters using the overly input.
    
    @param bxoverlay: Bunch crossings to overlay.
    @type bxoverlay: float
    """
    self._checkArgs( { 'bxoverlay' : types.IntType } )
    self.BXOverlay = bxoverlay
    return S_OK()
    
  def setBXOverlay(self, bxoverlay):
    """ Define number bunch crossings to overlay for each signal event.
    This is used to determine the number of required overlay events.
    It does not modify any of the actual application parameters using the overly input.
    
    @param bxoverlay: Bunch crossings to overlay.
    @type bxoverlay: float
    """
    return self.setOverlayBXPerSigEvt( bxoverlay )
  
  def setOverlayEvtsPerBX( self, ggtohadint ):
    """ Define the number of overlay events per bunch crossing.
    This is used to determine the number of required overlay events.
    It does not modify any of the actual application parameters using the overly input.
    
    @param ggtohadint: optional number of overlay events interactions per bunch crossing
    @type ggtohadint: float
    
    """
    self._checkArgs( { 'ggtohadint' : types.FloatType } )
    self.GGToHadInt = ggtohadint
    return S_OK()
  
  def setGGToHadInt(self, ggtohadint):
    """ Define the number of overlay events per bunch crossing.
    This is used to determine the number of required overlay events.
    It does not modify any of the actual application parameters using the overly input.
    
    @param ggtohadint: optional number of overlay events interactions per bunch crossing
    @type ggtohadint: float
    
    """
    return self.setOverlayEvtsPerBX( ggtohadint )
      
  def setNbSigEvtsPerJob(self, nbsigevtsperjob):
    """ Set the number of signal events per job.
    This is used to determine the number of required overlay events.
    It does not modify any of the actual application parameters using the overly input.
    
    @param nbsigevtsperjob: Number of signal events per job
    @type nbsigevtsperjob: int
    
    """  
    self._checkArgs( { 'nbsigevtsperjob' : types.IntType } )
    
    self.NbSigEvtsPerJob = nbsigevtsperjob
    return S_OK()


  def setDetectorModel(self, detectormodel):
    """ Set the detector type. Must be 'CLIC_ILD_CDR' or 'CLIC_SID_CDR' or 'sidloi3'
    
    @param detectormodel: Detector type. Must be 'CLIC_ILD_CDR' or 'CLIC_SID_CDR' or 'sidloi3'
    @type detectormodel: string
    
    """  
    self._checkArgs( { 'detectormodel' : types.StringTypes } )
    
    self.DetectorModel = detectormodel
    return S_OK()

  def setPathToFiles(self, path):
    """ Sets the path to where the overlay files are located.
    Setting this option will ignore all other settings!

    @param path: LFN path to the folder containing the overlay files
    @type path: string

    """
    self._checkArgs( { 'path' : types.StringTypes } )
    self.pathToOverlayFiles = path
    return S_OK()

  def setBkgEvtType(self, BkgEvtType):
    """ Define the background type.
    
    @param BkgEvtType: Background type.
    @type BkgEvtType: string
    
    """  
    self._checkArgs( { 'BkgEvtType' : types.StringTypes } )
    
    self.BkgEvtType = BkgEvtType
    return S_OK()



  def setBackgroundType(self, backgroundType):
    """Alternative to L{setBkgEvtType}

    @param backgroundType: Background type.
    @type backgroundType: string

    """
    return self.setBkgEvtType(backgroundType)

  def setNumberOfSignalEventsPerJob(self, numberSignalEvents):
    """Alternative to L{setNbSigEvtsPerJob}

    @param numberSignalEvents: Number of signal events per job
    @type numberSignalEvents: int

    """
    return self.setNbSigEvtsPerJob(numberSignalEvents)

#  def setProdIDToUse(self,prodid):
#    """ Optional parameter: Define the production ID to use as input
#    
#    @param prodid: Production ID
#    @type prodid: int
#    """
#    self._checkArgs({"prodid" : types.IntType})
#    self.prodid = prodid
#    return S_OK()

  def _applicationModule(self):
    m1 = self._createModuleDefinition()
    m1.addParameter(Parameter("BXOverlay",       0,  "float", "", "", False, False, 
                              "Bunch crossings to overlay"))
    m1.addParameter(Parameter("ggtohadint",      0,  "float", "", "", False, False, 
                              "Optional number of gamma gamma -> hadrons interactions per bunch crossing, default is 3.2"))
    m1.addParameter(Parameter("NbSigEvtsPerJob", 0,    "int", "", "", False, False, 
                              "Number of signal events per job"))
    m1.addParameter(Parameter("prodid",          0,    "int", "", "", False, False, 
                              "ProdID to use"))
    m1.addParameter(Parameter("BkgEvtType",     "", "string", "", "", False, False, 
                              "Background type."))
    m1.addParameter(Parameter("detectormodel",       "", "string", "", "", False, False, 
                              "Detector type."))
    m1.addParameter(Parameter("machine",       "", "string", "", "", False, False, 
                              "machine: clic_cdr or ilc_dbd"))
    m1.addParameter(Parameter("useEnergyForFileLookup", True, "bool", "", "", False, False,
                              "useEnergy to look for background files: True or False"))
    m1.addParameter(Parameter("pathToOverlayFiles", "", "string", "", "", False, False,
                              "use overlay files from this path"))

    m1.addParameter(Parameter("debug",          False,   "bool", "", "", False, False, "debug mode"))
    return m1
  

  def _applicationModuleValues(self, moduleinstance):
    moduleinstance.setValue("BXOverlay",         self.BXOverlay)
    moduleinstance.setValue('ggtohadint',        self.GGToHadInt)
    moduleinstance.setValue('NbSigEvtsPerJob',   self.NbSigEvtsPerJob)
    moduleinstance.setValue('prodid',            self.ProdID)
    moduleinstance.setValue('BkgEvtType',        self.BkgEvtType)
    moduleinstance.setValue('detectormodel',     self.DetectorModel)
    moduleinstance.setValue('debug',             self.Debug)
    moduleinstance.setValue('machine',           self.Machine  )
    moduleinstance.setValue('useEnergyForFileLookup', self.useEnergyForFileLookup  )
    moduleinstance.setValue('pathToOverlayFiles', self.pathToOverlayFiles )
  
  def _userjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    if not res1["OK"] :
      return S_ERROR('userjobmodules failed')
    return S_OK() 

  def _prodjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    if not res1["OK"] :
      return S_ERROR('prodjobmodules failed')
    return S_OK()    

  def _addParametersToStep(self, stepdefinition):
    res = self._addBaseParameters(stepdefinition)
    if not res["OK"]:
      return S_ERROR("Failed to set base parameters")
    return S_OK()
      
  def _checkConsistency(self):
    """ Checks that all needed parameters are set
    """
    if self.pathToOverlayFiles:
      res = FileCatalogClient().findFilesByMetadata({}, self.pathToOverlayFiles)
      self._log.notice("Found %i files in path %s" %( len(res['Value']), self.pathToOverlayFiles))
      if len(res['Value']) == 0 :
        return S_ERROR("OverlayInput: PathToFiles is specified, but there are no files in that path")

    if not self.BXOverlay :
      return S_ERROR("Number of overlay bunch crossings not defined")
          
    if not self.GGToHadInt :
      return S_ERROR("Number of background events per bunch crossing is not defined")
      
    if not self.BkgEvtType :
      return S_ERROR("Background event type is not defined: Chose one gghad, aa_lowpt, ...")
        
    if self._jobtype == 'User' :
      if not self.NbSigEvtsPerJob :
        return S_ERROR("Number of signal event per job is not defined")
    else:
      self.prodparameters['detectorModel'] = self.DetectorModel
      self.prodparameters['BXOverlay']  = self.BXOverlay
      self.prodparameters['GGtoHadInt'] = self.GGToHadInt

    return S_OK() 
  
  def _checkFinalConsistency(self):
    """ Final check of consistency: the overlay files for the specifed energy must exist
    """
    if self.pathToOverlayFiles:
      return S_OK() # can ignore other parameter

    if not self.Energy:
      return S_ERROR("Energy MUST be specified for the overlay")

    res = self._ops.getSections('/Overlay')
    if not res['OK']:
      return S_ERROR("Could not resolve the CS path to the overlay specifications")
    sections = res['Value']
    if not self.Machine in sections:
      return S_ERROR("Machine %s does not have overlay data, use any of %s" % (self.Machine, sections))  
    
    fracappen = modf(float(self.Energy)/1000.)
    if fracappen[1] > 0: 
      energytouse = "%stev" % (Decimal(str(self.Energy))/Decimal("1000."))
    else:
      energytouse =  "%sgev" % (Decimal(str(self.Energy)))
    if energytouse.count(".0"):
      energytouse = energytouse.replace(".0", "")
    res = self._ops.getSections("/Overlay/%s" % self.Machine)
    if not energytouse in res['Value']:
      return S_ERROR("No overlay files corresponding to %s" % energytouse)
    
    res = self._ops.getSections("/Overlay/%s/%s" % (self.Machine, energytouse))
    if not res['OK']:
      return S_ERROR("Could not find the detector models")
    
    if not self.DetectorModel in res['Value']:
      return S_ERROR("Detector model specified has no overlay data with that energy and machine")

    res = allowedBkg(self.BkgEvtType, energytouse, detectormodel = self.DetectorModel, machine = self.Machine)  
    if not res['OK']:
      return res
    if res['Value'] < 0:
      return S_ERROR("No proper production ID found") 
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
  
  Use setExtraCLIArguments if you want to get CLI parameters
  Needed for easy parameter scan, and passing non-standard strings (like cuts)
  
  """
  def __init__(self, paramdict = None):

    self.outputDstPath = ''
    self.OutputDstFile = ''
    self.outputRecPath = ''
    self.OutputRecFile = ''
    self.GearFile = ''
    self.ProcessorsToUse = []
    self.ProcessorsToExclude = []
    super(Marlin, self).__init__( paramdict )
    ##Those 5 need to come after default constructor
    self._modulename = 'MarlinAnalysis'
    self._moduledescription = 'Module to run MARLIN'
    self.appname = 'marlin'    
    self.datatype = 'REC'
    self.detectortype = 'ILD'
    
  def setGearFile(self, GearFile):
    """ Define input gear file for Marlin
    
    @param GearFile: input gear file for Marlin reconstrcutor
    @type GearFile: string
    """
    self._checkArgs( { 'GearFile' : types.StringTypes } )

    self.GearFile = GearFile
    if os.path.exists(GearFile) or GearFile.lower().count("lfn:"):
      self.inputSB.append(GearFile) 
    
  def setOutputRecFile(self, outputRecFile, path = None):
    """ Optional: Define output rec file for Marlin
    
    @param outputRecFile: output rec file for Marlin
    @type outputRecFile: string
    @param path: Path where to store the file. Used only in prouction context. Use setOutputData if 
    you want to keep the file on the grid.
    @type path: string
    """
    self._checkArgs( { 'outputRecFile' : types.StringTypes } )
    self.OutputRecFile = outputRecFile
    self.prodparameters[self.OutputRecFile] = {}
    self.prodparameters[self.OutputRecFile]['datatype'] = 'REC'
    if path:
      self.outputRecPath = path      
    
  def setOutputDstFile(self, outputDstFile, path = None):
    """ Optional: Define output dst file for Marlin
    
    @param outputDstFile: output dst file for Marlin
    @type outputDstFile: string
    @param path: Path where to store the file. Used only in prouction context. Use setOutputData if 
    you want to keep the file on the grid.
    @type path: string
    """
    self._checkArgs( { 'outputDstFile' : types.StringTypes } )
    self.OutputDstFile = outputDstFile
    self.prodparameters[self.OutputDstFile] = {}
    self.prodparameters[self.OutputDstFile]['datatype'] = 'DST'
    if path:
      self.outputDstPath = path    
  
  def setProcessorsToUse(self, processorlist):
    """ Define processor list to use
    
    Overwrite the default list (full reco). Useful for users willing to do dedicated analysis (TPC, Vertex digi, etc.)
    
    >>> ma.setProcessorsToUse(['libMarlinTPC.so','libMarlinReco.so','libOverlay.so','libMarlinTrkProcessors.so'])
    
    @param processorlist: list of processors to use
    @type processorlist: list
    """
    self._checkArgs( { 'processorlist' : types.ListType } )
    self.ProcessorsToUse = processorlist
    
  def setProcessorsToExclude(self, processorlist):
    """ Define processor list to exclude
    
    Overwrite the default list (full reco). Useful for users willing to do dedicated analysis (TPC, Vertex digi, etc.)
    
    >>> ma.setProcessorsToExclude(['libLCFIVertex.so'])
    
    @param processorlist: list of processors to exclude
    @type processorlist: list
    """
    self._checkArgs( { 'processorlist' : types.ListType } )
    self.ProcessorsToExclude = processorlist
      
  def _userjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setUserJobFinalization(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('userjobmodules failed')
    return S_OK() 

  def _prodjobmodules(self, stepdefinition):
    
    ## Here one needs to take care of listoutput
    if self.OutputPath:
      self._listofoutput.append({'OutputFile' : '@{OutputFile}', "outputPath" : "@{OutputPath}", 
                                 "outputDataSE" : '@{OutputSE}'})
    
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setOutputComputeDataList(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('prodjobmodules failed')
    return S_OK()
  
  def _checkConsistency(self):
        
    if not self.Version:
      return S_ERROR('Version not set!')   

    if self.SteeringFile:
      if not os.path.exists(self.SteeringFile) and not self.SteeringFile.lower().count("lfn:"):
        #res = Exists(self.SteeringFile)
        res = S_OK()
        if not res['OK']:
          return res  
      if os.path.exists(self.SteeringFile):
        res = CheckXMLValidity(self.SteeringFile)
        if not res['OK']:
          return S_ERROR("Supplied steering file cannot be read with xml parser: %s" % (res['Message']) )
    
    if not self.GearFile :
      self._log.info('GEAR file not given, will use GearOutput.xml (default from Mokka, CLIC_ILD_CDR model)')
    if self.GearFile:
      if not os.path.exists(self.GearFile) and not self.GearFile.lower().count("lfn:"):
        #res = Exists(self.GearFile)
        res = S_OK()
        if not res['OK']:
          return res  

    #res = self._checkRequiredApp()
    #if not res['OK']:
    #  return res

    if not self.GearFile:
      self.GearFile = 'GearOutput.xml'

    if not self._jobtype == 'User' :
      if not self.OutputFile:
        self._listofoutput.append({"outputFile":"@{outputREC}", "outputPath":"@{outputPathREC}", 
                                   "outputDataSE":'@{OutputSE}'})
        self._listofoutput.append({"outputFile":"@{outputDST}", "outputPath":"@{outputPathDST}", 
                                   "outputDataSE":'@{OutputSE}'})
      self.prodparameters['detectorType'] = self.detectortype
      self.prodparameters['marlin_gearfile'] = self.GearFile
      self.prodparameters['marlin_steeringfile'] = self.SteeringFile
      

    return S_OK()  
  
  def _applicationModule(self):
    
    md1 = self._createModuleDefinition()
    md1.addParameter(Parameter("inputGEAR",              '', "string", "", "", False, False, 
                               "Input GEAR file"))
    md1.addParameter(Parameter("ProcessorListToUse",     [],   "list", "", "", False, False, 
                               "List of processors to use"))
    md1.addParameter(Parameter("ProcessorListToExclude", [],   "list", "", "", False, False, 
                               "List of processors to exclude"))
    md1.addParameter(Parameter("debug",               False,   "bool", "", "", False, False, 
                               "debug mode"))
    return md1
  
  def _applicationModuleValues(self, moduleinstance):

    moduleinstance.setValue("inputGEAR",              self.GearFile)
    moduleinstance.setValue('ProcessorListToUse',     self.ProcessorsToUse)
    moduleinstance.setValue('ProcessorListToExclude', self.ProcessorsToExclude)
    moduleinstance.setValue("debug",                  self.Debug)

  def _checkWorkflowConsistency(self):
    return self._checkRequiredApp()
    
  def _resolveLinkedStepParameters(self, stepinstance):
    if type(self._linkedidx) == types.IntType:
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
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
  
  Use setExtraCLIArguments to add CLI arguments to the lcsim call
  
  """
  def __init__(self, paramdict = None):

    self.ExtraParams = ''
    self.AliasProperties = ''
    self.TrackingStrategy = ''
    self.DetectorModel = ''
    super(LCSIM, self).__init__( paramdict)
    ##Those 5 need to come after default constructor
    self._modulename = 'LCSIMAnalysis'
    self._moduledescription = 'Module to run LCSIM'
    self.appname = 'lcsim'
    self.datatype = 'REC'
    self.detectortype = 'SID'
     
  def setOutputRecFile(self, outputRecFile, path = None):
    """ Optional: Define output rec file for LCSIM
    
    @param outputRecFile: output rec file for LCSIM
    @type outputRecFile: string
    @param path: Path where to store the file. Used only in prouction context. Use setOutputData if 
    you want to keep the file on the grid.
    @type path: string
    """
    self._checkArgs( { 'outputRecFile' : types.StringTypes } )
    self.OutputRecFile = outputRecFile
    self.prodparameters[self.OutputRecFile] = {}
    self.prodparameters[self.OutputRecFile]['datatype'] = 'REC'
    if path:
      self.outputRecPath = path
    
  def setOutputDstFile(self, outputDstFile, path = None):
    """ Optional: Define output dst file for LCSIM
    
    @param outputDstFile: output dst file for LCSIM
    @type outputDstFile: string
    @param path: Path where to store the file. Used only in prouction context. Use setOutputData if you want 
    to keep the file on the grid.
    @type path: string
    """
    self._checkArgs( { 'outputDstFile' : types.StringTypes } )
    self.OutputDstFile = outputDstFile 
    self.prodparameters[self.OutputDstFile] = {}
    self.prodparameters[self.OutputDstFile]['datatype'] = 'DST'
    if path:
      self.outputDstPath = path            
    
  def setAliasProperties(self, alias):
    """ Optional: Define the path to the alias.properties file name that will be used 
    
    @param alias: Path to the alias.properties file name that will be used
    @type alias: string
    """
    self._checkArgs( { 'alias' : types.StringTypes } )

    self.AliasProperties = alias     
    if os.path.exists(alias) or alias.lower().count("lfn:"):
      self.inputSB.append(alias) 
  
  def setDetectorModel(self, model):
    """ Detector Model to use
    
    @param model: name, zip file, or lfn that points to the detector model
    @type model: string
    """
    self._checkArgs( { 'model' : types.StringTypes } )
    self.DetectorModel = model
    if os.path.exists(model) or model.lower().count("lfn:"):
      self.inputSB.append(model)
  
  def setTrackingStrategy(self, trackingstrategy):
    """ Optional: Define the tracking strategy to use.  
    
    @param trackingstrategy: path to the trackingstrategy file to use. If not called, will use whatever is 
    in the steering file
    @type trackingstrategy: string
    """
    self._checkArgs( { 'trackingstrategy' : types.StringTypes } )
    self.TrackingStrategy = trackingstrategy
    if os.path.exists(self.TrackingStrategy) or self.TrackingStrategy.lower().count('lfn:'):
      self.inputSB.append(self.TrackingStrategy)
      
  def setExtraParams(self, extraparams):
    """ Optional: Define command line parameters to pass to java
    
    @param extraparams: Command line parameters to pass to java
    @type extraparams: string
    """
    self._checkArgs( { 'extraparams' : types.StringTypes } )

    self.ExtraParams = extraparams     
    
  def willRunSLICPandora(self):
    """ You need this if you plan on running L{SLICPandora}
    """
    self.willBeCut = True  
    
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

    if not self.Energy :
      self._log.info('Energy set to 0 !')
      
    if not self.NbEvts :
      self._log.info('Number of events set to 0 !')
        
    if not self.Version:
      return S_ERROR('No version found')   

    if self.SteeringFile:
      if not os.path.exists(self.SteeringFile) and not self.SteeringFile.lower().count("lfn:"):
        res = Exists(self.SteeringFile)
        if not res['OK']:
          return res  
      if os.path.exists(self.SteeringFile):
        res = CheckXMLValidity(self.SteeringFile)
        if not res['OK']:
          return S_ERROR("Supplied steering file cannot be read by XML parser: %s" % ( res['Message'] ) )
    if self.TrackingStrategy:
      if not os.path.exists(self.TrackingStrategy) and not self.TrackingStrategy.lower().count("lfn:"):
        res = Exists(self.TrackingStrategy)
        if not res['OK']:
          return res  
        
    if self.DetectorModel:
      if not self.DetectorModel.lower().count(".zip"):
        return S_ERROR("setDetectorModel: You HAVE to pass an existing .zip file, either as local file or as LFN. \
        Or use the alias.properties.")
        
    #res = self._checkRequiredApp()
    #if not res['OK']:
    #  return res
    
    if not self._jobtype == 'User':
      #slicp = False
      if self._inputapp and not self.OutputFile and not self.willBeCut:
        for app in self._inputapp:
          if app.appname in ['slicpandora', 'marlin']:
            self._listofoutput.append({"outputFile":"@{outputREC}", "outputPath":"@{outputPathREC}", 
                                       "outputDataSE":'@{OutputSE}'})
            self._listofoutput.append({"outputFile":"@{outputDST}", "outputPath":"@{outputPathDST}", 
                                       "outputDataSE":'@{OutputSE}'})
            #slicp = True
            break
      self.prodparameters['detectorType'] = self.detectortype
      self.prodparameters['lcsim_steeringfile'] = self.SteeringFile
      self.prodparameters['lcsim_trackingstrategy'] = self.TrackingStrategy

      #if not slicp:
      #  self._listofoutput.append({"outputFile":"@{OutputFile}","outputPath":"@{OutputPath}","outputDataSE":'@{OutputSE}'})    
      
      
    return S_OK()  
  
  def _applicationModule(self):
    
    md1 = self._createModuleDefinition()
    md1.addParameter(Parameter("extraparams",           "", "string", "", "", False, False, 
                               "Command line parameters to pass to java"))
    md1.addParameter(Parameter("aliasproperties",       "", "string", "", "", False, False, 
                               "Path to the alias.properties file name that will be used"))
    md1.addParameter(Parameter("debug",              False,   "bool", "", "", False, False, 
                               "debug mode"))
    md1.addParameter(Parameter("detectorModel",         "", "string", "", "", False, False, 
                               "detector model zip file"))
    md1.addParameter(Parameter("trackingstrategy",      "", "string", "", "", False, False, 
                               "trackingstrategy"))
    return md1
  
  def _applicationModuleValues(self, moduleinstance):

    moduleinstance.setValue("extraparams",        self.ExtraParams)
    moduleinstance.setValue("aliasproperties",    self.AliasProperties)
    moduleinstance.setValue("debug",              self.Debug)
    moduleinstance.setValue("detectorModel",      self.DetectorModel)
    moduleinstance.setValue("trackingstrategy",   self.TrackingStrategy)

  def _checkWorkflowConsistency(self):
    return self._checkRequiredApp()

  def _resolveLinkedStepParameters(self, stepinstance):
    if type(self._linkedidx) == types.IntType:
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
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
