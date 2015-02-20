"""
Whizard: First Generator application
"""
__RCSID__ = "$Id$"

from ILCDIRAC.Interfaces.API.NewInterface.LCApplication import LCApplication
from ILCDIRAC.Core.Utilities.WhizardOptions import WhizardOptions, getDict
from ILCDIRAC.Core.Utilities.GeneratorModels import GeneratorModels

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Workflow.Parameter import Parameter
import types, os

class Whizard(LCApplication):
  """ Runs whizard to generate a given event type

  Usage:

  >>> wh = Whizard(dirac.getProcessList())
  >>> wh.setProcess("ee_h_mumu")
  >>> wh.setEnergy(500)
  >>> wh.setNbEvts(1000)
  >>> wh.setModel("sm")

  use setExtraArguments to overwrite the content of the whizard.in
  in case you use something not standard (parameter scan for exmple)
  """
  def __init__(self, processlist = None, paramdict = None):

    self.ParameterDict = {}
    self.Model = 'sm'
    self.RandomSeed = 0
    self.Luminosity = 0
    self.JobIndex = ''
    self._optionsdictstr = ''
    self.FullParameterDict = {}
    self.GeneratorLevelCuts = {}
    self._genlevelcutsstr = ''
    self._leshouchesfiles = None
    self._generatormodels = GeneratorModels()
    self.EvtType = ''
    self.GlobalEvtType = ''
    self.useGridFiles = False
    self._allowedparams = ['PNAME1', 'PNAME2', 'POLAB1', 'POLAB2', 'USERB1', 'USERB2',
                           'ISRB1', 'ISRB2', 'EPAB1', 'EPAB2', 'RECOIL', 'INITIALS', 'USERSPECTRUM']
    self._wo = None
    self.parameters = []
    self._processlist = None
    if processlist:
      self._processlist = processlist
    super(Whizard, self).__init__( paramdict )
    ##Those 4 need to come after default constructor
    self._modulename = 'WhizardAnalysis'
    self._moduledescription = 'Module to run WHIZARD'
    self.appname = 'whizard'
    self.datatype = 'gen'
    self._paramsToExclude.extend( [ '_optionsdictstr', '_genlevelcutsstr', '_leshouchesfiles', '_generatormodels',
                                    '_allowedparams', '_wo','_processlist' ] )

  def getPDict(self):
    """ Provide predefined parameter dictionary
    """
    return getDict()

  def setEvtType(self, evttype):
    """ Define process. If the process given is not found, when calling job.append a full list is printed.

    @param evttype: Process to generate
    @type evttype: string
    """
    self._checkArgs( { 'evttype' : types.StringTypes } )
    if self.addedtojob:
      self._log.error("Cannot modify this attribute once application has been added to Job")
      return S_ERROR("Cannot modify")
    self.EvtType = evttype

  def setGlobalEvtType(self, globalname):
    """ When producing multiple process in one job, it is needed to define this for the output file name.
    It's mandatory to use the L{setFullParameterDict} method when using this.
    """
    self._checkArgs( { 'globalname' : types.StringTypes } )
    self.GlobalEvtType = globalname

  def setLuminosity(self, lumi):
    """ Optional: Define luminosity to generate

    @param lumi: Luminosity to generate. Not available if cross section is not known a priori. Use with care.
    @type lumi: float
    """
    self._checkArgs( { 'lumi' : types.FloatType } )
    self.Luminosity = lumi

  def setRandomSeed(self, RandomSeed):
    """ Optional: Define random seed to use. Default is Job ID.

    @param RandomSeed: Seed to use during integration and generation.
    @type RandomSeed: int
    """
    self._checkArgs( { 'RandomSeed' : types.IntType } )

    self.RandomSeed = RandomSeed

  def setParameterDict(self, paramdict):
    """ Parameters for Whizard steering files

    @param paramdict: Dictionary of parameters for the whizard templates. Most parameters are set on the fly.
    @type paramdict: dict
    """
    self._checkArgs( { 'paramdict' : types.DictType } )
    self.ParameterDict = paramdict

  def setGeneratorLevelCuts(self, cutsdict):
    """ Define generator level cuts (to be put in whizard.cut1)

    Refer to U{http://projects.hepforge.org/whizard/manual_w1/manual005.html#toc12} for details about how to set cuts.

    >>> wh.setGeneratorLevelCuts({'e1e1_o':["cut M of  3 within 10 99999","cut E of  3 within  5 99999"]})

    @param cutsdict: Dictionary of cuts
    @type cutsdict: dict
    """
    self._checkArgs( { 'cutsdict' : types.DictType } )
    self.GeneratorLevelCuts = cutsdict

  def setFullParameterDict(self, pdict):
    """ Parameters for Whizard steering files, better than above as much more complete (cannot be more complete)

    >>> pdict = {}
    >>> pdict['process_input'] = {}
    >>> #processes below are not those of the templates, but those of the whizard.prc
    >>> pdict['process_input']['process_id']='h_n1n1'
    >>> pdict['process_input']['sqrts'] = 3000.
    >>> pdict['simulation_input'] = {}
    >>> pdict['simulation_input']['n_events'] = 100
    >>> pdict['beam_input_1'] = {}
    >>> pdict['beam_input_1']['polarization']='1.0 0.0'
    >>> pdict['beam_input_1']['USER_spectrum_mode'] = 11
    >>> pdict['beam_input_2'] = {}
    >>> pdict['beam_input_2']['polarization']='0.0 1.0'
    >>> pdict['beam_input_2']['USER_spectrum_mode'] = -11
    >>> wh.setFullParameterDict(pdict)

    The first key corresponds to the sections of the whizard.in, while the second corresponds to the possible parameters.
    All keys/values can be found in the WHIZARD documentation:
    U{http://projects.hepforge.org/whizard/manual_w1/manual005.html#toc11}

    @param pdict: Dictionnary of parameters
    @type pdict: dict
    """
    self._checkArgs( { 'pdict' : types.DictType } )

    self.FullParameterDict = pdict
    #self._wo.changeAndReturn(dict)

  def setModel(self, model):
    """ Optional: Define Model

    @param model: Model to use for generation. Predefined list available in GeneratorModels class.
    @type model: string
    """
    self._checkArgs( { 'model' : types.StringTypes } )

    self.Model = model

  def willCut(self):
    """ You need this if you plan on cutting using L{StdhepCut}
    """
    self.willBeCut = True

  def usingGridFiles(self):
    """ Call this if you want to use the grid files that come with the Whizard version used.

    Beware: Depends on the energy and generator cuts, use it if you know what you are doing.
    """
    self.useGridFiles = True

  def setJobIndex(self, index):
    """ Optional: Define Job Index. Added in the file name between the event type and the extension.

    @param index: Index to use for generation
    @type index: string
    """
    self._checkArgs( { 'index' : types.StringTypes } )

    self.JobIndex = index

  def dumpWhizardDotIn(self, fname = 'whizard.in'):
    """ Dump the content of the whizard.in file requested for this application
    """
    if self.addedtojob:
      self._wo.toWhizardDotIn(fname)
    else:
      self._reportError("Can't dump the whizard.in as there can be further changes")

  def _checkConsistency(self):
    """ Check the consistency, called from Application
    """
    self._wo = WhizardOptions(self.Model)

    if not self.FullParameterDict:
      if not self.Energy :
        return S_ERROR('Energy not set')

      if not self.NbEvts :
        return S_ERROR('Number of events not set!')

      if not self.EvtType:
        return S_ERROR("Process not defined")
    else:
      res = self._wo.checkFields(self.FullParameterDict)
      if not res['OK']:
        return res
      self._wo.changeAndReturn(self.FullParameterDict)
      res = self._wo.getValue("process_input/process_id")
      if not len(res['Value']):
        if self.EvtType:
          if not self.FullParameterDict.has_key('process_input'):
            self.FullParameterDict['process_input'] = {}
          self.FullParameterDict['process_input']['process_id'] = self.EvtType
        else:
          return S_ERROR("Event type not specified")
      self.EvtType = res['Value']

      res = self._wo.getValue("process_input/sqrts")
      if type(res['Value']) == type(3) or type(res['Value']) == type(3.):
        energy = res['Value']
      else:
        energy = eval(res['Value'])
      if not energy:
        if self.Energy:
          if not self.FullParameterDict.has_key('process_input'):
            self.FullParameterDict['process_input'] = {}
          self.FullParameterDict['process_input']['sqrts'] = self.Energy
          energy = self.Energy
        else:
          return S_ERROR("Energy set to 0")
      self.Energy = energy

      res = self._wo.getValue("simulation_input/n_events")
      if type(res['Value']) == type(3) or type(res['Value']) == type(3.):
        nbevts = res['Value']
      else:
        nbevts = eval(res['Value'])
      if not nbevts:
        if self.NbEvts:
          if not self.FullParameterDict.has_key('simulation_input'):
            self.FullParameterDict['simulation_input'] = {}
          self.FullParameterDict['simulation_input']['n_events'] = self.NbEvts
          nbevts = self.NbEvts
        else:
          return S_ERROR("Number of events set to 0")
      self.NbEvts = nbevts

    if not self._processlist:
      return S_ERROR("Process list was not given")

    if self.GeneratorLevelCuts:
      for process in self.GeneratorLevelCuts.keys():
        if not process in self.EvtType.split():
          self._log.info("You want to cut on %s but that process is not to be generated" % process)
      for values in self.GeneratorLevelCuts.values():
        if not type(values) == types.ListType:
          return S_ERROR('Type of %s is not a list, cannot proceed' % values)
      self._genlevelcutsstr = str(self.GeneratorLevelCuts)

    if self.EvtType:
      processes = self.EvtType.split()
      if len(processes) > 1 and not self.GlobalEvtType:
        return S_ERROR("Global name MUST be defined when producing multiple processes in one job")
      elif self.GlobalEvtType:
        self.EvtType = self.GlobalEvtType
      for process in processes:
        if not self._processlist.existsProcess(process)['Value']:
          self._log.notice("Available processes are:")
          self._processlist.printProcesses()
          return S_ERROR('Process %s does not exists'%process)
        else:
          cspath = self._processlist.getCSPath(process)
          whiz_file = os.path.basename(cspath)
          version = whiz_file.replace(".tar.gz","").replace(".tgz","").replace("whizard","")
          if self.Version:
            if self.Version != version:
              return S_ERROR("All processes to consider are not available in the same WHIZARD version")
          else:
            self.Version = version
          self._log.info("Found the process %s in whizard %s"%(process, self.Version))

    if not self.Version:
      return S_ERROR('No version found')

    if self.Model:
      if not self._generatormodels.hasModel(self.Model)['OK']:
        return S_ERROR("Unknown model %s" % self.Model)

    if self.OutputFile:
      if self.OutputFile.count("/"):
        return S_ERROR("The OutputFile name is a file name, not a path. Remove any / in there")

    if not self.OutputFile and self._jobtype == 'User':
      self.OutputFile = self.EvtType
      if self.JobIndex :
        self.OutputFile += "_" + self.JobIndex
      self.OutputFile += "_gen.stdhep"

    if not self._jobtype == 'User':
      if not self.willBeCut:
        self._listofoutput.append({"outputFile":"@{OutputFile}", "outputPath":"@{OutputPath}",
                                   "outputDataSE":'@{OutputSE}'})
      self.prodparameters['nbevts'] = self.NbEvts
      self.prodparameters['Process'] = self.EvtType
      self.prodparameters['model'] = self.Model
      self.prodparameters['Energy'] = self.Energy
      self.prodparameters['whizardparams'] = self.FullParameterDict
      self.prodparameters['gencuts'] = self.GeneratorLevelCuts
      self.prodparameters['gridfiles'] = self.useGridFiles

    if not self.FullParameterDict and  self.ParameterDict:
      for key in self.ParameterDict.keys():
        if not key in self._allowedparams:
          return S_ERROR("Unknown parameter %s"%key)

      if not self.ParameterDict.has_key('PNAME1'):
        self._log.info("Assuming incoming beam 1 to be electrons")
        self.parameters.append('PNAME1=e1')
      else:
        self.parameters.append("PNAME1=%s" % self.ParameterDict["PNAME1"] )

      if not self.ParameterDict.has_key('PNAME2'):
        self._log.info("Assuming incoming beam 2 to be positrons")
        self.parameters.append('PNAME2=E1')
      else:
        self.parameters.append("PNAME2=%s" %self.ParameterDict["PNAME2"] )

      if not self.ParameterDict.has_key('POLAB1'):
        self._log.info("Assuming no polarization for beam 1")
        self.parameters.append('POLAB1=0.0 0.0')
      else:
        self.parameters.append("POLAB1=%s" % self.ParameterDict["POLAB1"])

      if not self.ParameterDict.has_key('POLAB2'):
        self._log.info("Assuming no polarization for beam 2")
        self.parameters.append('POLAB2=0.0 0.0')
      else:
        self.parameters.append("POLAB2=%s" % self.ParameterDict["POLAB2"])

      if not self.ParameterDict.has_key('USERB1'):
        self._log.info("Will put beam spectrum to True for beam 1")
        self.parameters.append('USERB1=T')
      else:
        self.parameters.append("USERB1=%s" % self.ParameterDict["USERB1"])

      if not self.ParameterDict.has_key('USERB2'):
        self._log.info("Will put beam spectrum to True for beam 2")
        self.parameters.append('USERB2=T')
      else:
        self.parameters.append("USERB2=%s" % self.ParameterDict["USERB2"])

      if not self.ParameterDict.has_key('ISRB1'):
        self._log.info("Will put ISR to True for beam 1")
        self.parameters.append('ISRB1=T')
      else:
        self.parameters.append("ISRB1=%s" % self.ParameterDict["ISRB1"])

      if not self.ParameterDict.has_key('ISRB2'):
        self._log.info("Will put ISR to True for beam 2")
        self.parameters.append('ISRB2=T')
      else:
        self.parameters.append("ISRB2=%s" % self.ParameterDict["ISRB2"])

      if not self.ParameterDict.has_key('EPAB1'):
        self._log.info("Will put EPA to False for beam 1")
        self.parameters.append('EPAB1=F')
      else:
        self.parameters.append("EPAB1=%s" % self.ParameterDict["EPAB1"])

      if not self.ParameterDict.has_key('EPAB2'):
        self._log.info("Will put EPA to False for beam 2")
        self.parameters.append('EPAB2=F')
      else:
        self.parameters.append("EPAB2=%s" % self.ParameterDict["EPAB2"])

      if not self.ParameterDict.has_key('RECOIL'):
        self._log.info("Will set Beam_recoil to False")
        self.parameters.append('RECOIL=F')
      else:
        self.parameters.append("RECOIL=%s" % self.ParameterDict["RECOIL"])

      if not self.ParameterDict.has_key('INITIALS'):
        self._log.info("Will set keep_initials to False")
        self.parameters.append('INITIALS=F')
      else:
        self.parameters.append("INITIALS=%s" % self.ParameterDict["INITIALS"])

      if not self.ParameterDict.has_key('USERSPECTRUM'):
        self._log.info("Will set USER_spectrum_on to +-11")
        self.parameters.append('USERSPECTRUM=11')
      else:
        self.parameters.append("USERSPECTRUM=%s" % self.ParameterDict["USERSPECTRUM"])

      self.parameters = ";".join( self.parameters )
    elif self.FullParameterDict:
      self._optionsdictstr = str(self.FullParameterDict)


    return S_OK()

  def _applicationModule(self):
    md1 = self._createModuleDefinition()
    md1.addParameter(Parameter("evttype",      "", "string", "", "", False, False, "Process to generate"))
    md1.addParameter(Parameter("RandomSeed",    0,    "int", "", "", False, False, "Random seed for the generator"))
    md1.addParameter(Parameter("Lumi",          0,  "float", "", "", False, False, "Luminosity of beam"))
    md1.addParameter(Parameter("Model",        "", "string", "", "", False, False, "Model for generation"))
    md1.addParameter(Parameter("SteeringFile", "", "string", "", "", False, False, "Steering file"))
    md1.addParameter(Parameter("steeringparameters",  "", "string", "", "", False, False,
                               "Specific steering parameters"))
    md1.addParameter(Parameter("OptionsDictStr",      "", "string", "", "", False, False,
                               "Options dict to create full whizard.in on the fly"))
    md1.addParameter(Parameter("GenLevelCutDictStr",  "", "string", "", "", False, False,
                               "Generator level cuts to put in whizard.cut1"))
    md1.addParameter(Parameter("willCut",  False,   "bool", "", "", False, False, "Will cut after"))
    md1.addParameter(Parameter("useGridFiles",  True,   "bool", "", "", False, False, "Will use grid files"))
    md1.addParameter(Parameter("debug",    False,   "bool", "", "", False, False, "debug mode"))
    return md1


  def _applicationModuleValues(self, moduleinstance):
    moduleinstance.setValue("evttype",            self.EvtType)
    moduleinstance.setValue("RandomSeed",         self.RandomSeed)
    moduleinstance.setValue("Lumi",               self.Luminosity)
    moduleinstance.setValue("Model",              self.Model)
    moduleinstance.setValue("SteeringFile",       self.SteeringFile)
    moduleinstance.setValue("steeringparameters", self.parameters)
    moduleinstance.setValue("OptionsDictStr",     self._optionsdictstr)
    moduleinstance.setValue("GenLevelCutDictStr", self._genlevelcutsstr)
    moduleinstance.setValue("willCut",            self.willBeCut)
    moduleinstance.setValue("useGridFiles",       self.useGridFiles)
    moduleinstance.setValue("debug",              self.Debug)

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
