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
  >>> wh.setEvtType("ee_h_mumu")
  >>> wh.setEnergy(500)
  >>> wh.setNumberOfEvents(1000)
  >>> wh.setModel("sm")

  use setExtraArguments to overwrite the content of the whizard.in
  in case you use something not standard (parameter scan for exmple)
  """
  def __init__(self, processlist = None, paramdict = None):

    self.parameterDict = {}
    self.model = 'sm'
    self.randomSeed = 0
    self.luminosity = 0
    self.jobIndex = ''
    self._optionsdictstr = ''
    self.fullParameterDict = {}
    self.generatorLevelCuts = {}
    self._genlevelcutsstr = ''
    self._leshouchesfiles = None
    self._generatormodels = GeneratorModels()
    self.eventType = ''
    self.globalEventType = ''
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
    """ Define process. If the process given is not found, when calling :func:`UserJob.append() <ILCDIRAC.Interfaces.API.NewInterface.UserJob.UserJob.append>` a full list is printed.

    :param string evttype: Process to generate
    """
    self._checkArgs( { 'evttype' : types.StringTypes } )
    if self.addedtojob:
      self._log.error("Cannot modify this attribute once application has been added to Job")
      return S_ERROR("Cannot modify")
    self.eventType = evttype

  def setGlobalEvtType(self, globalname):
    """ When producing multiple process in one job, it is needed to define this for the output file name.
    It's mandatory to use the L{setFullParameterDict} method when using this.
    """
    self._checkArgs( { 'globalname' : types.StringTypes } )
    self.globalEventType = globalname

  def setLuminosity(self, lumi):
    """ Optional: Define luminosity to generate

    :param float lumi: Luminosity to generate. Not available if cross section is not known a priori. Use with care.
    """
    self._checkArgs( { 'lumi' : types.FloatType } )
    self.luminosity = lumi

  def setRandomSeed(self, randomSeed):
    """ Optional: Define random seed to use. Default is Job ID.

    :param int randomSeed: Seed to use during integration and generation.
    """
    self._checkArgs( { 'randomSeed' : types.IntType } )

    self.randomSeed = randomSeed

  def setParameterDict(self, paramdict):
    """ Parameters for Whizard steering files

    :param dict paramdict: Dictionary of parameters for the whizard templates. Most parameters are set on the fly.
    """
    self._checkArgs( { 'paramdict' : types.DictType } )
    self.parameterDict = paramdict

  def setGeneratorLevelCuts(self, cutsdict):
    """ Define generator level cuts (to be put in whizard.cut1)

    Refer to http://whizard.hepforge.org/manual_w1/manual005.html#toc12 for details about how to set cuts.

    >>> wh.setGeneratorLevelCuts({'e1e1_o':["cut M of  3 within 10 99999","cut E of  3 within  5 99999"]})

    :param dict cutsdict: Dictionary of cuts
    """
    self._checkArgs( { 'cutsdict' : types.DictType } )
    self.generatorLevelCuts = cutsdict

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
    All keys/values can be found in the WHIZARD documentation: http://whizard.hepforge.org/manual_w1/manual005.html#toc11

    :param dict pdict: Dictionnary of parameters
    """
    self._checkArgs( { 'pdict' : types.DictType } )

    self.fullParameterDict = pdict
    #self._wo.changeAndReturn(dict)

  def setModel(self, model):
    """ Optional: Define Model

    :param string model: Model to use for generation. Predefined list available in the :mod:`GeneratorModels<ILCDIRAC.Core.Utilities.GeneratorModels.GeneratorModels>` class.
    """
    self._checkArgs( { 'model' : types.StringTypes } )

    self.model = model

  def willCut(self):
    """ You need this if you plan on cutting using :mod:`StdhepCut <ILCDIRAC.Interfaces.API.NewInterface.Applications.StdhepCut.StdhepCut>`
    """
    self.willBeCut = True

  def usingGridFiles(self):
    """ Call this if you want to use the grid files that come with the Whizard version used.

    Beware: Depends on the energy and generator cuts, use it if you know what you are doing.
    """
    self.useGridFiles = True

  def setJobIndex(self, index):
    """ Optional: Define Job Index. Added in the file name between the event type and the extension.

    :param string index: Index to use for generation
    """
    self._checkArgs( { 'index' : types.StringTypes } )

    self.jobIndex = index

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
    self._wo = WhizardOptions(self.model)

    if not self.fullParameterDict:
      if not self.energy :
        return S_ERROR('Energy not set')

      if not self.numberOfEvents :
        return S_ERROR('Number of events not set!')

      if not self.eventType:
        return S_ERROR("Process not defined")
    else:
      res = self._wo.checkFields(self.fullParameterDict)
      if not res['OK']:
        return res
      self._wo.changeAndReturn(self.fullParameterDict)
      res = self._wo.getValue("process_input/process_id")
      if not len(res['Value']):
        if self.eventType:
          if not 'process_input' in self.fullParameterDict:
            self.fullParameterDict['process_input'] = {}
          self.fullParameterDict['process_input']['process_id'] = self.eventType
        else:
          return S_ERROR("Event type not specified")
      self.eventType = res['Value']

      res = self._wo.getValue("process_input/sqrts")
      if type(res['Value']) == type(3) or type(res['Value']) == type(3.):
        energy = res['Value']
      else:
        energy = eval(res['Value'])
      if not energy:
        if self.energy:
          if not 'process_input' in self.fullParameterDict:
            self.fullParameterDict['process_input'] = {}
          self.fullParameterDict['process_input']['sqrts'] = self.energy
          energy = self.energy
        else:
          return S_ERROR("Energy set to 0")
      self.energy = energy

      res = self._wo.getValue("simulation_input/n_events")
      if type(res['Value']) == type(3) or type(res['Value']) == type(3.):
        numberOfEvents = res['Value']
      else:
        numberOfEvents = eval(res['Value'])
      if not numberOfEvents:
        if self.numberOfEvents:
          if not 'simulation_input' in self.fullParameterDict:
            self.fullParameterDict['simulation_input'] = {}
          self.fullParameterDict['simulation_input']['n_events'] = self.numberOfEvents
          numberOfEvents = self.numberOfEvents
        else:
          return S_ERROR("Number of events set to 0")
      self.numberOfEvents = numberOfEvents

    if not self._processlist:
      return S_ERROR("Process list was not given")

    if self.generatorLevelCuts:
      for process in self.generatorLevelCuts.keys():
        if not process in self.eventType.split():
          self._log.info("You want to cut on %s but that process is not to be generated" % process)
      for values in self.generatorLevelCuts.values():
        if not type(values) == types.ListType:
          return S_ERROR('Type of %s is not a list, cannot proceed' % values)
      self._genlevelcutsstr = str(self.generatorLevelCuts)

    if self.eventType:
      processes = self.eventType.split()
      if len(processes) > 1 and not self.globalEventType:
        return S_ERROR("Global name MUST be defined when producing multiple processes in one job")
      elif self.globalEventType:
        self.eventType = self.globalEventType
      for process in processes:
        if not self._processlist.existsProcess(process)['Value']:
          self._log.notice("Available processes are:")
          self._processlist.printProcesses()
          return S_ERROR('Process %s does not exists'%process)
        else:
          cspath = self._processlist.getCSPath(process)
          whiz_file = os.path.basename(cspath)
          version = whiz_file.replace(".tar.gz","").replace(".tgz","").replace("whizard","")
          if self.version:
            if self.version != version:
              return S_ERROR("All processes to consider are not available in the same WHIZARD version")
          else:
            self.version = version
          self._log.info("Found the process %s in whizard %s"%(process, self.version))

    if not self.version:
      return S_ERROR('No version found')

    if self.model:
      if not self._generatormodels.hasModel(self.model)['OK']:
        return S_ERROR("Unknown model %s" % self.model)

    if self.outputFile:
      if self.outputFile.count("/"):
        return S_ERROR("The OutputFile name is a file name, not a path. Remove any / in there")

    if not self.outputFile and self._jobtype == 'User':
      self.outputFile = self.eventType
      if self.jobIndex :
        self.outputFile += "_" + self.jobIndex
      self.outputFile += "_gen.stdhep"

    if not self._jobtype == 'User':
      if not self.willBeCut:
        self._listofoutput.append({"outputFile":"@{OutputFile}", "outputPath":"@{OutputPath}",
                                   "outputDataSE":'@{OutputSE}'})
      self.prodparameters['nbevts'] = self.numberOfEvents
      self.prodparameters['Process'] = self.eventType
      self.prodparameters['model'] = self.model
      self.prodparameters['Energy'] = self.energy
      self.prodparameters['whizardparams'] = self.fullParameterDict
      self.prodparameters['gencuts'] = self.generatorLevelCuts
      self.prodparameters['gridfiles'] = self.useGridFiles

    if not self.fullParameterDict and self.parameterDict:
      for key in self.parameterDict.keys():
        if not key in self._allowedparams:
          return S_ERROR("Unknown parameter %s"%key)

      self.setParameter( 'PNAME1', 'e1', "Assuming incoming beam 1 to be electrons" )
      self.setParameter( 'PNAME2', 'E1', "Assuming incoming beam 2 to be positrons" )
      self.setParameter( 'POLAB1', '0.0 0.0', "Assuming no polarization for beam 1" )
      self.setParameter( 'POLAB2', '0.0 0.0', "Assuming no polarization for beam 2" )
      self.setParameter( 'USERB1', 'T', "Will put beam spectrum to True for beam 1" )
      self.setParameter( 'USERB2', 'T', "Will put beam spectrum to True for beam 2" )
      self.setParameter( 'ISRB1', 'T', "Will put ISR to True for beam 1" )
      self.setParameter( 'ISRB2', 'T', "Will put ISR to True for beam 2" )

      self.setParameter( 'EPAB1', 'F', "Will put EPA to False for beam 1" )
      self.setParameter( 'EPAB2', 'F', "Will put EPA to False for beam 2" )

      self.setParameter( 'RECOIL', 'F', "Will set Beam_recoil to False" )
      self.setParameter( 'INITIALS', 'F', "Will set keep_initials to False" )
      self.setParameter( 'USERSPECTRUM', '11', "Will set USER_spectrum_on to +-11" )

      self.parameters = ";".join( self.parameters )
    elif self.fullParameterDict:
      self._optionsdictstr = str(self.fullParameterDict)


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
    moduleinstance.setValue("evttype",            self.eventType)
    moduleinstance.setValue("RandomSeed",         self.randomSeed)
    moduleinstance.setValue("Lumi",               self.luminosity)
    moduleinstance.setValue("Model",              self.model)
    moduleinstance.setValue("SteeringFile",       self.steeringFile)
    moduleinstance.setValue("steeringparameters", self.parameters)
    moduleinstance.setValue("OptionsDictStr",     self._optionsdictstr)
    moduleinstance.setValue("GenLevelCutDictStr", self._genlevelcutsstr)
    moduleinstance.setValue("willCut",            self.willBeCut)
    moduleinstance.setValue("useGridFiles",       self.useGridFiles)
    moduleinstance.setValue("debug",              self.debug)

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

  def setParameter(self, parameter, defaultValue, docString):
    if not parameter in self.parameterDict:
      self._log.info(docString)
      self.parameters.append( "%s=%s" % (parameter, defaultValue) )
    else:
      self.parameters.append( "%s=%s" % (parameter, self.parameterDict[parameter]) )
