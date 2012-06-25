'''
Created on Jun 24, 2010

@author: sposs
'''
__RCSID__ = "$Id: Production.py 24/06/2010 sposs $"

from DIRAC.Core.Workflow.Workflow                     import *
from ILCDIRAC.Interfaces.API.DiracILC                 import DiracILC
from DIRAC.Core.Utilities.List                        import removeEmptyElements
from DIRAC.Core.DISET.RPCClient                       import RPCClient
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.Resources.Catalog.FileCatalogClient        import FileCatalogClient

from ILCDIRAC.Interfaces.API.ILCJob                   import ILCJob
from DIRAC                                            import gConfig
import string, shutil

from random import randrange


class Production(ILCJob):
  """ Production API class
  """
  #############################################################################
  def __init__(self, script=None):
    """Instantiates the Workflow object and some default parameters.
    """
    ILCJob.__init__(self, script)
    self.prodVersion = __RCSID__
    self.csSection = '/Operations/Production/Defaults'
    self.StepCount = 0
    self.currentStepPrefix = ''
    self.systemConfig = gConfig.getValue('%s/SystemConfig' %(self.csSection), 'x86_64-slc5-gcc43-opt')
    self.defaultProdID = '12345'
    self.defaultProdJobID = '12345'
    self.ioDict = {}
    self.prodTypes = ['MCGeneration', 'MCSimulation', 'Test', 'MCReconstruction', 'MCReconstruction_Overlay']
    self.name = 'unspecifiedWorkflow'
    self.firstEventType = ''
    self.prodGroup = ''
    self.plugin = ''
    self.inputFileMask = ''
    self.inputBKSelection = {}
    self.nbtasks = 0
    self.nbofevents = 0
    self.process = ""
    self.basepath = ""
    self.basename = ""
    self.prodparameters = {}    
    self.prodparameters['NbInputFiles'] = 1
    self.prodparameters['UsingWhizardOutput'] = False
    self.prodparameters['UsingMokkaOutput'] = False
    self.prodparameters['UsingSLICOutput'] = False
    self.prodparameters['PostGenSelApplied'] = False
    self.prodparameters['BXOverlay'] = 0
    self.prodparameters['GGInt'] = 0
    
    self.jobFileGroupSize = 1
    self.ancestorProduction = ''
    self.currtrans = None
    self.currtransID = None
    self.importLine = """
from ILCDIRAC.Workflow.Modules.<MODULE> import <MODULE>
"""
    is_prod = "IS_PROD"
    self._addParameter(self.workflow, is_prod, 'JDL', True, "This job is a production job")
    if not script:
      self.__setDefaults()

  #############################################################################
  def __setDefaults(self):
    """Sets some default parameters.
    """
    self.setType('MCReconstruction')
    self.setSystemConfig(self.systemConfig)
    self.setCPUTime('300000')
    self.setLogLevel('verbose')
    self.setJobGroup('@{PRODUCTION_ID}')
    self.setFileMask('')

    #version control
    self._setParameter('productionVersion', 'string', self.prodVersion, 'ProdAPIVersion')

    #General workflow parameters
    self._setParameter('PRODUCTION_ID',     'string', self.defaultProdID.zfill(8), 'ProductionID')
    self._setParameter('JOB_ID',            'string', self.defaultProdJobID.zfill(8), 'ProductionJobID')
    #self._setParameter('poolXMLCatName','string','pool_xml_catalog.xml','POOLXMLCatalogName')
    self._setParameter('Priority',             'JDL',                     '1', 'Priority')
    self._setParameter('emailAddress',      'string', 'stephane.poss@cern.ch', 'CrashEmailAddress')
    self._setParameter('DataType',          'string',                    'MC', 'Priority') #MC or DATA
    self._setParameter('outputMode',        'string',                 'Local', 'SEResolutionPolicy')

    #Options related parameters
    self._setParameter('EventMaxDefault',   'string',                    '-1', 'DefaultNumberOfEvents')
    #BK related parameters
    self._setParameter('lfnprefix',         'string',                  'prod', 'LFNprefix')
    self._setParameter('lfnpostfix',        'string',                  '2009', 'LFNpostfix')
    #self._setParameter('conditions','string','','SimOrDataTakingCondsString')
  #############################################################################
  
  def _setParameter(self, name, parameterType, parameterValue, description):
    """Set parameters checking in CS in case some defaults need to be changed.
    """
    if gConfig.getValue('%s/%s' % (self.csSection, name), ''):
      self.log.debug('Setting %s from CS defaults = %s' % (name, gConfig.getValue('%s/%s' % (self.csSection, name))))
      self._addParameter(self.workflow, name, parameterType, gConfig.getValue('%s/%s' % (self.csSection, name), 'default'), description)
    else:
      self.log.debug('Setting parameter %s = %s' % (name, parameterValue))
      self._addParameter(self.workflow, name, parameterType, parameterValue, description)

  def defineInputData(self, metadata):
    """ Define input data for the production

    Pass the metadata dictionary meta, that is looked up in the catalog to extract the process and number of events per file.

    @param metadata: metadata dictionary used as InputDataQuery
    @type metadata: dict
    """
    if not type(metadata) == type({}):
      print "metadata should be a dictionnary"
      return S_ERROR()
    metakeys = metadata.keys()
    client = FileCatalogClient()
    res = client.getMetadataFields()
    if not res['OK']:
      print "Could not contact File Catalog"
      self.explainInputDataQuery()
      return S_ERROR()
    metaFCkeys = res['Value'].keys()
    for key in metakeys:
      for meta in metaFCkeys:
        if meta != key:
          if meta.lower() == key.lower():
            return self._reportError("Key syntax error %s, should be %s" % (key, meta))
      if not metaFCkeys.count(key):
        return self._reportError("Key %s not found in metadata keys, allowed are %s" % (key, metaFCkeys))

    if not   metadata.has_key("ProdID"):
      return self._reportError("Input metadata dictionary must contain at least a key 'ProdID' as reference")
    
    res = client.findFilesByMetadata(metadata)
    if not res['OK']:
      return self._reportError("Error looking up the catalog for available files")
    elif len(res['Value']) < 1:
      return self._reportError('Could not find any files corresponding to the query issued')
    directory = os.path.dirname(res['Value'][0])
    res = client.getDirectoryMetadata(directory)
    if not res['OK']:
      return self._reportError("Error looking up the catalog for directory metadata")
    #res =   client.getCompatibleMetadata(metadata)
    #if not res['OK']:
    #  return self._reportError("Error looking up the catalog for compatible metadata")
    compatmeta = res['Value']
    compatmeta.update(metadata)
    if compatmeta.has_key('EvtType'):
      if type(compatmeta['EvtType']) in types.StringTypes:
        self.process  = compatmeta['EvtType']
      if type(compatmeta['EvtType']) == type([]):
        self.process = compatmeta['EvtType'][0]
    else:
      return self._reportError("EvtType is not in the metadata, it has to be!")
    if compatmeta.has_key('NumberOfEvents'):
      if type(compatmeta['NumberOfEvents']) == type([]):
        self.nbofevents = compatmeta['NumberOfEvents'][0]
      else:
        #type(compatmeta['NumberOfEvents']) in types.StringTypes:
        self.nbofevents = compatmeta['NumberOfEvents']
      #else:
      #  return self._reportError('Nb of events does not have any type recognised')

    self.basename = self.process
    self.basepath = "/ilc/prod/"
    if compatmeta.has_key("Machine"):
      if type(compatmeta["Machine"]) in types.StringTypes:
        self.basepath += compatmeta["Machine"]+"/"
      if type(compatmeta["Machine"]) == type([]):
        self.basepath += compatmeta["Machine"][0]+"/"
    if compatmeta.has_key("Energy"):
      if type(compatmeta["Energy"]) in types.StringTypes:
        self.basepath += compatmeta["Energy"]+"/"
        self.energy=compatmeta["Energy"]
      if type(compatmeta["Energy"]) == type([]):
        self.basepath += compatmeta["Energy"][0]+"/"
        self.energy=compatmeta["Energy"][0]        
    if compatmeta.has_key("EvtType"):
      if type(compatmeta["EvtType"]) in types.StringTypes:
        self.basepath += compatmeta["EvtType"]+"/"
      if type(compatmeta["EvtType"]) == type([]):
        self.basepath += compatmeta["EvtType"][0]+"/"
    gendata = False
    if compatmeta.has_key('Datatype'):
      if type(compatmeta['Datatype']) in types.StringTypes:
        if compatmeta['Datatype'] == 'gen':
          gendata = True
      if type(compatmeta['Datatype']) == type([]):
        if compatmeta['Datatype'][0] == 'gen':
          gendata = True
    if compatmeta.has_key("DetectorType") and not gendata:
      if type(compatmeta["DetectorType"]) in types.StringTypes:
        self.detector = compatmeta["DetectorType"]
      if type(compatmeta["DetectorType"]) == type([]):
        self.detector = compatmeta["DetectorType"][0]
    self.inputBKSelection = metadata

    self.prodparameters["FCInputQuery"] = self.inputBKSelection
    self.prodparameters['nbevts'] = self.nbofevents
    return S_OK()

  def addWhizardStep(self, processlist, process, version = None, susymodel=None, energy = 3000, nbevts=0, lumi=0,
                     extraparameters=None, randomseed = None, outputpath="", outputSE=""):
    """ Define Whizard step

    Must get the process list from dirac.

    The output file name is created automatically by whizard using the process name (process_gen.stdhep).

    Number of events and luminosity should not be specified together, they are determined one with the other using the cross section in the process list. Luminosity prevails.

    @param process: process to generate, must be available in the processlist
    @type process: string
    @param nbevts: number of events to generate
    @type nbevts: int
    @param lumi: luminosity to generate
    @type lumi: double
    @param extraparameters: dictionary of whizard parameters to replace in template (e.g. RECOIL for beam_recoil)
    @type extraparameters: dict
    @param outputpath: path to store the output file
    @type outputpath: string
    @param outputSE: Storage element to use
    @type outputSE: string
    """
    kwargs = {"process":process, 'susymodel':susymodel, "energy":energy, "nbevts":nbevts,
              "lumi":lumi, "outputpath":outputpath, "outputSE":outputSE}
    appvers = ""

    if process:
      if not processlist.existsProcess(process)['Value']:
        self.log.error('Process %s does not exist in any whizard version, please contact responsible.' % process)
        self.log.info("Available processes are:")
        processlist.printProcesses()
        return self._reportError('Process %s does not exist in any whizard version.' % process, __name__, **kwargs)
      else:
        cspath = processlist.getCSPath(process)
        whiz_file = os.path.basename(cspath)
        appvers = whiz_file.replace(".tar.gz","").replace(".tgz","").replace("whizard","")
        self.log.info("Found process %s corresponding to whizard%s" % (process, appvers))
        processes = processlist.getProcessesDict()
        cross_section = float(processes[process]["CrossSection"])
        if cross_section:
          if not lumi and nbevts:
            lumi = nbevts/cross_section
          if lumi and not nbevts:
            nbevts = lumi*cross_section
        print "Will generate %s evts, or lumi=%s fb" % (nbevts, lumi)
    else:
      return self._reportError("Process to generate was not specified", __name__, **kwargs)
    if version:
      self.log.info("Overwritting version to %s"%version)
      appvers = version

    if not outputpath:
      return self._reportError("Output path not defined" , __name__, **kwargs)
    if not outputSE:
      return self._reportError("Output Storage element not defined" , __name__, **kwargs)

    if susymodel:
      if not susymodel == "slsqhh" and not susymodel == 'chne':
        self._reportError("susymodel must be either slsqhh or chne")

    outputfile = process + "_gen.stdhep"

    self.StepCount += 1


    parameters = []
    if extraparameters:
      if not type(extraparameters) == type({}):
        return self._reportError('extraparameter argument must be dictionary', __name__, **kwargs)
    else:
      extraparameters['PNAME1'] = 'e1'
      print "Assuming incoming beam 1 to be electrons"

    for n,v in extraparameters.items():
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
      print "Will set USER_spectrum_on to 11"
      parameters.append('USERSPECTRUM=11')


    jobindex = ""
    if self.ioDict.has_key("WhizardStep"):
      randomseed = randrange(1000000)
      jobindex = str(self.StepCount)
      outputfile = process + "_%s_gen.stdhep"%jobindex
      

    #TODO look how to allow changing pythia parameters, which are separated with ;
    #if not extraparameters.has_key('PYTHIAPARAMS'):
    #  print "Using default pythia parameters"
    #  parameters.append("PYTHIAPARAMS=\"PMAS(25,1)=120.; PMAS(25,2)=0.3605E-02; MSTU(22)=20 ; MSTJ(28)=2 ;\"")

    #Add to input sandbox the processlist: if it fails getting it, the job get rescheduled
    res = gConfig.getValue('/Operations/ProcessList/Location', '')
    if not res:
      return self._reportError('Could not resolve location of processlist.cfg')
    res = 'LFN:' + res
    self.addToInputSandbox.append(res)

    stepName = 'Whizard'
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' % ('Whizard', stepNumber)
    self._addParameter(self.workflow, 'TotalSteps', 'String', self.StepCount, 'Total number of steps')

    whizardStep =     ModuleDefinition('WhizardAnalysis')
    whizardStep.setDescription('Whizard step: generate the physics events')
    body = string.replace(self.importLine, '<MODULE>', 'WhizardAnalysis')
    whizardStep.setBody(body)

    createoutputlist = ModuleDefinition('ComputeOutputDataList')
    createoutputlist.setDescription('Compute the outputList parameter, needed by outputdataPolicy')
    body = string.replace(self.importLine, '<MODULE>', 'ComputeOutputDataList')
    createoutputlist.setBody(body)

    WhizardAppDefn = StepDefinition(stepDefn)
    WhizardAppDefn.addModule(whizardStep)
    WhizardAppDefn.createModuleInstance('WhizardAnalysis', stepDefn)
    WhizardAppDefn.addModule(createoutputlist)
    WhizardAppDefn.createModuleInstance('ComputeOutputDataList', stepDefn)
    self._addParameter(WhizardAppDefn, 'applicationVersion', 'string', '', 'ApplicationVersion')
    self._addParameter(WhizardAppDefn, "applicationLog",     "string", "", "Application log file")
    self._addParameter(WhizardAppDefn, "EvtType",            "string", "", "Process to generate")
    self._addParameter(WhizardAppDefn, "parameters",         "string", "", "Parameters for template")
    self._addParameter(WhizardAppDefn, "Energy",                "int",  0, "Energy to generate")
    if randomseed:
      self._addParameter(WhizardAppDefn, "RandomSeed",                "int",  0, "RandomSeed")
    self._addParameter(WhizardAppDefn, "NbOfEvts",              "int",  0, "Number of events to generate")
    self._addParameter(WhizardAppDefn, 'listoutput',           "list", [], "list of output file name")
    self._addParameter(WhizardAppDefn, "JobIndex",           "string", "", "JobIndex")
    self._addParameter(WhizardAppDefn, "outputPath",         "string", "", "Output data path")
    self._addParameter(WhizardAppDefn, "outputFile",         "string", "", "output file name")
    if susymodel:
      self._addParameter(WhizardAppDefn, "SusyModel"           ,"int",  0, "SUSY model to use")

    self._addParameter(WhizardAppDefn, "Lumi",                "float",  0, "Number of events to generate")
    self.workflow.addStep(WhizardAppDefn)
    mstep = self.workflow.createStepInstance(stepDefn, stepName)
    mstep.setValue('applicationVersion', appvers)
    mstep.setValue('applicationLog', 'Whizard_@{STEP_ID}.log')
    mstep.setValue("Energy", energy)
    self.prodparameters["Energy"] = energy
    mstep.setValue("EvtType", process)
    self.prodparameters['Process'] = process
    mstep.setValue("NbOfEvts", nbevts)
    self.prodparameters['nbevts'] = nbevts
    mstep.setValue("Lumi", lumi)
    self.prodparameters['lumi'] = lumi
    mstep.setValue('parameters',string.join(parameters, ";"))
    self.prodparameters['WhizardParameters'] = string.join(parameters,";")
    mstep.setValue("outputFile", outputfile)
    mstep.setValue("outputPath", outputpath)
    if susymodel:
      if susymodel == 'slsqhh':
        mstep.setValue('SusyModel', 1)
      if susymodel == 'chne':
        mstep.setValue('SusyModel', 2)

    if randomseed:
      mstep.setValue("RandomSeed",randomseed)

    outputList = []
    outputList.append({"outputFile":"@{outputFile}", "outputPath":"@{outputPath}", "outputDataSE":outputSE})
    mstep.setValue('listoutput', (outputList))
    mstep.setValue('JobIndex',jobindex)
    self.__addSoftwarePackages('whizard.%s' % (appvers))
    self._addParameter(self.workflow, "WhizardOutput", "string", outputfile, "whizard expected output file name")
    if nbevts:
      self._addParameter(self.workflow, "NbOfEvents", "int", nbevts, "Number of events")
    if lumi:
      self._addParameter(self.workflow, "Luminosity", "float", lumi, "Luminosity")
    self.ioDict["WhizardStep"] = mstep.getName()
    return S_OK()

  def addPYTHIAStep(self, name, appvers, nbevts=0, process = None, outputFile = '',outputpath="", outputSE=""):
    """ Define PYTHIA step
    """
    kwargs = {'name':name,"appvers":appvers,"nbevts":nbevts,"process":process,"outputFile":outputFile,"outputpath":outputpath, "outputSE":outputSE}
    if not nbevts:
      return self._reportError("Number of events has to be specified",__name__,**kwargs)
    if not outputFile:
      return self._reportError("outputFile must be specified",__name__,**kwargs)
    elif not outputFile.count("_gen"):
      outputfile = outputFile.split(".stdhep")[0]
      outputFile = outputfile+"_gen.stdhep"
    self.StepCount += 1
    
    stepName = 'Pythia'
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' % ('Pythia', stepNumber)
    self._addParameter(self.workflow, 'TotalSteps', 'String', self.StepCount, 'Total number of steps')

    pythiaStep =     ModuleDefinition('PythiaAnalysis')
    pythiaStep.setDescription('Pythia step: generate the physics events')
    body = string.replace(self.importLine, '<MODULE>', 'PythiaAnalysis')
    pythiaStep.setBody(body)

    createoutputlist = ModuleDefinition('ComputeOutputDataList')
    createoutputlist.setDescription('Compute the outputList parameter, needed by outputdataPolicy')
    body = string.replace(self.importLine, '<MODULE>', 'ComputeOutputDataList')
    createoutputlist.setBody(body)

    PythiaAppDefn = StepDefinition(stepDefn)
    PythiaAppDefn.addModule(pythiaStep)
    PythiaAppDefn.createModuleInstance('PythiaAnalysis', stepDefn)
    PythiaAppDefn.addModule(createoutputlist)
    PythiaAppDefn.createModuleInstance('ComputeOutputDataList', stepDefn)
    self._addParameter(PythiaAppDefn, 'applicationName', 'string', '', 'Application name')
    self._addParameter(PythiaAppDefn, 'applicationVersion', 'string', '', 'ApplicationVersion')
    self._addParameter(PythiaAppDefn, "applicationLog",     "string", "", "Application log file")
    self._addParameter(PythiaAppDefn, "NbOfEvts",              "int",  0, "Number of events to generate")
    self._addParameter(PythiaAppDefn, 'listoutput',           "list", [], "list of output file name")
    self._addParameter(PythiaAppDefn, "outputPath",         "string", "", "Output data path")
    self._addParameter(PythiaAppDefn, "outputFile",         "string", "", "output file name")
    self.workflow.addStep(PythiaAppDefn)
    mstep = self.workflow.createStepInstance(stepDefn, stepName)
    mstep.setValue('applicationName', name)
    mstep.setValue('applicationVersion', appvers)
    mstep.setValue('applicationLog', 'Pythia_@{STEP_ID}.log')
    mstep.setValue("NbOfEvts", nbevts)
    self.prodparameters['nbevts'] = nbevts
    self.prodparameters['Process'] = process
    mstep.setValue("outputFile", outputFile)
    mstep.setValue("outputPath", outputpath)
    outputList = []
    outputList.append({"outputFile":"@{outputFile}", "outputPath":"@{outputPath}", "outputDataSE":outputSE})
    mstep.setValue('listoutput', (outputList))

    self.__addSoftwarePackages('%s.%s' % (name,appvers))

    if nbevts:
      self._addParameter(self.workflow, "NbOfEvents", "int", nbevts, "Number of events")

    self.ioDict["WhizardStep"] = mstep.getName()
    
    return S_OK()

  def addPostGenSelStep(self, appvers, NbEvts):
    """ Define Post Generation Selection
    @param appVers: Version to use
    @type appVers: string
    """
    kwargs = {"appvers":appvers, "NbEvts":NbEvts}
    if not appvers:
      return self._reportError('PostGenSel version not specified', __name__, **kwargs)
    if not NbEvts:
      return self._reportError('Number of events must be specified', __name__, **kwargs)

    self.StepCount += 1
    stepName = 'PostGenSel'
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' %('PostGenSel', stepNumber)
    self._addParameter(self.workflow, 'TotalSteps', 'String', self.StepCount, 'Total number of steps')
    
    PostGenSelStep = ModuleDefinition('PostGenSelection')
    PostGenSelStep.setDescription('Post Generation Selection step: apply cuts to the physics events')
    body = string.replace(self.importLine, '<MODULE>', 'PostGenSelection')
    PostGenSelStep.setBody(body)
    
    postgenselDefn = StepDefinition(stepDefn)
    postgenselDefn.addModule(PostGenSelStep)
    postgenselDefn.createModuleInstance('PostGenSel', stepDefn)
    self._addParameter(postgenselDefn, 'applicationVersion', 'string', '', 'ApplicationVersion')
    self._addParameter(postgenselDefn,             'NbEvts',    'int',  0, 'Number of events to keep')
    self.workflow.addStep(postgenselDefn)
    
    mstep = self.workflow.createStepInstance(stepDefn, stepName)
    mstep.setValue('applicationVersion', appvers)
    mstep.setValue('applicationLog',     'PostGenSel_@{STEP_ID}.log')
    mstep.setValue('NbEvts',             NbEvts)
    self.prodparameters['nbevts'] = NbEvts
    self.prodparameters['PostGenSelApplied'] = True
    self.__addSoftwarePackages('postgensel.%s' % (appvers))
    if NbEvts:
      self._addParameter(self.workflow, "NbOfEvents", "int", NbEvts, "Number of events")

    self.ioDict["PostGenSelStep"] = mstep.getName()

    return S_OK()

  def addMokkaStep(self,appvers,steeringfile,detectormodel=None,numberofevents=0,outputfile="",outputpath="",outputSE=""):
    """ Define Mokka step in production system

    @param appvers: version of MOKKA to use
    @type appvers: string
    @param steeringfile: file name of the steering. Should not be lfn. Should be passed in with setInputSandbox.
    @type steeringfile: string
    @param detectormodel: detector model to use. Must be available with the Mokka version specified
    @type detectormodel: string
    @param numberofevents: number of events to process. If whizard was run before, number of events is resolved from there
    @type numberofevents: int
    @param outputfile: File name to be produced with mokka.
    @type outputfile: string
    @param outputpath: path where to store the data. Should be /ilc/prod/<machine>/<energy>/<evt type>/ILD/SIM/
    @type outputpath: string
    @param outputSE: Storage element to use
    @type outputSE: string
    """
    kwargs = {"appvers":appvers,"steeringfile":steeringfile,"detectormodel":detectormodel,"numberofevents":numberofevents,
              "outputfile":outputfile,"outputpath":outputpath,"outputSE":outputSE}
    if not appvers:
      return self._reportError('Mokka version not specified',__name__,**kwargs)
    if not steeringfile:
      return self._reportError("Steering file name not specified",__name__,**kwargs)
    if not outputpath:
      if self.basepath:
        outputpath = self.basepath+"ILD/SIM"
      else:
        return self._reportError('Output path not defined, please set it',__name__,**kwargs)
    if not outputfile:
      if self.basename:
        outputfile = self.basename+"_sim.slcio"
      else:
        return self._reportError('Output file name was not specified',__name__,**kwargs)
    if not outputSE:
      return self._reportError('Output Storage element not defined',__name__,**kwargs)

    if not numberofevents:
      if self.nbofevents:
        numberofevents = self.nbofevents

    self.StepCount +=1

    stepName = 'RunMokka'
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' %('Mokka',stepNumber)

    self._addParameter(self.workflow,'TotalSteps','String',self.StepCount,'Total number of steps')
    mokkaStep = ModuleDefinition('MokkaAnalysis')
    mokkaStep.setDescription('Mokka step: simulation in ILD-like geometries context')
    body = string.replace(self.importLine,'<MODULE>','MokkaAnalysis')
    mokkaStep.setBody(body)

    createoutputlist = ModuleDefinition('ComputeOutputDataList')
    createoutputlist.setDescription('Compute the outputList parameter, needed by outputdataPolicy')
    body = string.replace(self.importLine,'<MODULE>','ComputeOutputDataList')
    createoutputlist.setBody(body)

    MokkaAppDefn = StepDefinition(stepDefn)
    MokkaAppDefn.addModule(mokkaStep)
    MokkaAppDefn.createModuleInstance('MokkaAnalysis',stepDefn)
    MokkaAppDefn.addModule(createoutputlist)
    MokkaAppDefn.createModuleInstance('ComputeOutputDataList',stepDefn)
    self._addParameter(MokkaAppDefn,'applicationVersion','string','','ApplicationVersion')
    self._addParameter(MokkaAppDefn,"steeringFile","string","","Name of the steering file")
    self._addParameter(MokkaAppDefn,"detectorModel","string","",'Detector Model')
    self._addParameter(MokkaAppDefn,"numberOfEvents","int",0,"Number of events to generate")
    self._addParameter(MokkaAppDefn,"applicationLog","string","","Application log file")
    self._addParameter(MokkaAppDefn,"outputPath","string","","Output data path")
    self._addParameter(MokkaAppDefn,"outputFile","string","","output file name")
    self._addParameter(MokkaAppDefn,'listoutput',"list",[],"list of output file name")
    self._addParameter(MokkaAppDefn,"stdhepFile","string","","Name of the stdhep file")

    self.workflow.addStep(MokkaAppDefn)
    mstep = self.workflow.createStepInstance(stepDefn,stepName)
    mstep.setValue('applicationVersion',appvers)
    mstep.setValue('steeringFile',steeringfile)
    self.prodparameters['MokkaSteer']=steeringfile
    if detectormodel:
      mstep.setValue("detectorModel",detectormodel)
      self.prodparameters['MokkaDetectorModel']=detectormodel
    if self.ioDict.has_key("PostGenSelStep"):
      mstep.setLink("numberOfEvents",self.ioDict["PostGenSelStep"],"NbEvts")
    elif self.ioDict.has_key("WhizardStep"):
      mstep.setLink("numberOfEvents",self.ioDict["WhizardStep"],"NbOfEvts")
    else:
      mstep.setValue("numberOfEvents",numberofevents)

    mstep.setValue('applicationLog', 'Mokka_@{STEP_ID}.log')

    if self.ioDict.has_key("WhizardStep"):
      mstep.setLink('stdhepFile',self.ioDict["WhizardStep"],'outputFile')
      self.prodparameters['UsingWhizardOutput']=True
    mstep.setValue("outputFile",outputfile)
    mstep.setValue("outputPath",outputpath)
    outputList=[]
    outputList.append({"outputFile":"@{outputFile}","outputPath":"@{outputPath}","outputDataSE":outputSE})
    mstep.setValue('listoutput',(outputList))

    self.__addSoftwarePackages('mokka.%s' %(appvers))
    self._addParameter(self.workflow,"MokkaOutput","string",outputfile,"Mokka expected output file name")
    self.ioDict["MokkaStep"]=mstep.getName()
    return S_OK()

  def addMarlinStep(self,appVers,inputXML="",inputGEAR=None,inputslcio=None,outputRECfile="",outputRECpath="",outputDSTfile="",outputDSTpath="",outputSE=""):
    """ Define Marlin step in production system

    @param appVers: Version of Marlin to use
    @type appVers: string
    @param inputXML: Input XML file name to perform reconstruction. Should not be lfn. Should be passed with setInputSandbox.
    @type inputXML: string
    @param inputGEAR: GEAR File name to use. Resolved automatically if MOKKA ran before.
    @type inputGEAR: string
    @param inputslcio: input slcio to use, should be used for tests only. Input slcio come from the production definition, not the workflow def.
    @param outputRECfile: File name of the REC file
    @param outputDSTfile: File name of the DST file
    @param outputRECpath: path to the REC file in the catalog. Should be like /ilc/prod/<machine>/<energy>/<process>/ILD/REC
    @param outputDSTpath: path to the DST file in the catalog. Should be like /ilc/prod/<machine>/<energy>/<process>/ILD/DST
    @param outputSE: Storage element to use
    """

    kwargs = {"appVers":appVers,"inputXML":inputXML,"inputGEAR":inputGEAR,"outputRECfile":outputRECfile,
              "outputRECpath":outputRECpath,"outputDSTfile":outputDSTfile,"outputDSTpath":outputDSTpath,
              "outputSE":outputSE}

    if not appVers:
      return self._reportError("Marlin version not specified",__name__,**kwargs)
    if not inputXML:
      return self._reportError("XML reconstruction file not specified",__name__,**kwargs)
    if not outputRECfile:
      if self.basename:
        outputRECfile = self.basename+"_rec.slcio"
      else:
        return self._reportError("Rec File not defined",__name__,**kwargs)
    if not outputDSTfile:
      if self.basename:
        outputDSTfile = self.basename+"_dst.slcio"
      else:
        return self._reportError("DST File not defined",__name__,**kwargs)
    if not   outputRECpath:
      if self.basepath:
        outputRECpath = self.basepath+"ILD/REC"
      else:
        return self._reportError("Output rec file path not specified",__name__,**kwargs)
    if not   outputDSTpath:
      if self.basepath:
        outputDSTpath = self.basepath+"ILD/DST"
      else:
        return self._reportError("Output dst file path not specified",__name__,**kwargs)

    if not outputSE:
      return self._reportError("Output Storage Element not specified",__name__,**kwargs)


    inputslcioStr =''
    if(inputslcio):
      if type(inputslcio) in types.StringTypes:
        inputslcio = [inputslcio]
      #for i in xrange(len(inputslcio)):
      #  inputslcio[i] = inputslcio[i].replace('LFN:','')
      #inputslcio = map( lambda x: 'LFN:'+x, inputslcio)
      inputslcioStr = string.join(inputslcio,';')
    if not inputGEAR:
      if self.ioDict.has_key("MokkaStep"):
        inputGEAR="GearOutput.xml"
      else:
        return self._reportError('As Mokka do not run before, you need to specify gearfile')

    self.StepCount +=1
    stepName = 'RunMarlin'
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' %('Marlin',stepNumber)
    self._addParameter(self.workflow,'TotalSteps','String',self.StepCount,'Total number of steps')

    marlinStep = ModuleDefinition('MarlinAnalysis')
    marlinStep.setDescription('Marlin step: reconstruction in ILD like detectors')
    body = string.replace(self.importLine,'<MODULE>','MarlinAnalysis')
    marlinStep.setBody(body)
    createoutputlist = ModuleDefinition('ComputeOutputDataList')
    createoutputlist.setDescription('Compute the outputList parameter, needed by outputdataPolicy')
    body = string.replace(self.importLine,'<MODULE>','ComputeOutputDataList')
    createoutputlist.setBody(body)

    MarlinAppDefn = StepDefinition(stepDefn)
    MarlinAppDefn.addModule(marlinStep)
    MarlinAppDefn.createModuleInstance('MarlinAnalysis', stepDefn)
    MarlinAppDefn.addModule(createoutputlist)
    MarlinAppDefn.createModuleInstance('ComputeOutputDataList',stepDefn)
    self._addParameter(MarlinAppDefn,'applicationVersion','string','','ApplicationVersion')
    self._addParameter(MarlinAppDefn,"applicationLog","string","","Application log file")
    self._addParameter(MarlinAppDefn,"inputXML","string","","Name of the input XML file")
    self._addParameter(MarlinAppDefn,"inputGEAR","string","","Name of the input GEAR file")
    self._addParameter(MarlinAppDefn,"inputSlcio","string","","List of input SLCIO files")
    self._addParameter(MarlinAppDefn,"outputPathREC","string","","Output data path of REC")
    self._addParameter(MarlinAppDefn,"outputREC","string","","output file name of REC")
    self._addParameter(MarlinAppDefn,"outputPathDST","string","","Output data path of DST")
    self._addParameter(MarlinAppDefn,"outputDST","string","","output file name of DST")
    self._addParameter(MarlinAppDefn,'listoutput',"list",[],"list of output file name")
    self.workflow.addStep(MarlinAppDefn)
    mstep = self.workflow.createStepInstance(stepDefn,stepName)
    mstep.setValue('applicationVersion',appVers)
    mstep.setValue('applicationLog', 'Marlin_@{STEP_ID}.log')
    if(inputslcioStr):
      mstep.setValue("inputSlcio",inputslcioStr)
    else:
      if self.ioDict.has_key("MokkaStep"):
        #raise TypeError,'Expected previously defined Mokka step for input data'
        mstep.setLink('inputSlcio',self.ioDict["MokkaStep"],'outputFile')
        
    mstep.setValue("inputXML",inputXML)
    self.prodparameters['MarlinXML']=inputXML
    mstep.setValue("inputGEAR",inputGEAR)
    self.prodparameters['MarlinGEAR']=inputGEAR
    mstep.setValue("outputREC",outputRECfile)
    mstep.setValue("outputPathREC",outputRECpath)
    mstep.setValue("outputDST",outputDSTfile)
    mstep.setValue("outputPathDST",outputDSTpath)
    outputList=[]
    outputList.append({"outputFile":"@{outputREC}","outputPath":"@{outputPathREC}","outputDataSE":outputSE})
    outputList.append({"outputFile":"@{outputDST}","outputPath":"@{outputPathDST}","outputDataSE":outputSE})
    mstep.setValue('listoutput',(outputList))

    self.__addSoftwarePackages('marlin.%s' %(appVers))
    self.ioDict["MarlinStep"]=mstep.getName()
    return S_OK()

  def addSLICStep(self,appVers,inputmac="",detectormodel=None,numberofevents=0,outputfile="",outputpath="",outputSE=""):
    """ Define SLIC step for production

    @param appVers: SLIC version to use
    @param inputmac: File name of the mac file to use. Should not be lfn. Should be passed with setInputSandbox.
    @param detectormodel: Detector model to use. Is downloaded automatically from the web
    @param numberofevents: number of events to process
    @param outputfile: File name of the outputfile
    @param outputpath: path to file in File Catalog. Should be like /ilc/prod/<machine>/<energy>/<evt type>/SID/SIM/
    @param outputSE: Storage element to use
    """
    kwargs = {"appVers":appVers,"inputmac":inputmac,"detectormodel":detectormodel,"numberofevents":numberofevents,
              "outputfile":outputfile,"outputpath":outputpath,"outputSE":outputSE}

    if not appVers:
      return self._reportError("SLIC version not specified",__name__,**kwargs)
    if not inputmac:
      return self._reportError("Mac file not defined",__name__,**kwargs)
    if not detectormodel:
      return self._reportError("Detector model not specified",__name__,**kwargs)
    if not numberofevents:
      if self.nbofevents:
        numberofevents = self.nbofevents
      else:
        return self._reportError("Number of events to process not defined",__name__,**kwargs)
    if not outputfile:
      if self.basename:
        outputfile = self.basename+"_sim.slcio"
      else:
        return self._reporError("Output file name not specified",__name__,**kwargs)
    if not outputpath:
      if self.basepath:
        outputpath = self.basepath+"SID/SIM"
      else:
        return self._reportError("Output path in Storage not defined",__name__,**kwargs)
    if not outputSE:
      return self._reportError('Storage Element to use not defined',__name__,**kwargs)

    self.StepCount +=1
    stepName = 'RunSLIC'
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' %('SLIC',stepNumber)
    self._addParameter(self.workflow,'TotalSteps','String',self.StepCount,'Total number of steps')

    slicStep = ModuleDefinition('SLICAnalysis')
    slicStep.setDescription('SLIC step: simulation in SiD like detectors')
    body = string.replace(self.importLine,'<MODULE>','SLICAnalysis')
    slicStep.setBody(body)
    createoutputlist = ModuleDefinition('ComputeOutputDataList')
    createoutputlist.setDescription('Compute the outputList parameter, needed by outputdataPolicy')
    body = string.replace(self.importLine,'<MODULE>','ComputeOutputDataList')
    createoutputlist.setBody(body)

    slicAppDefn = StepDefinition(stepDefn)
    slicAppDefn.addModule(slicStep)
    slicAppDefn.createModuleInstance('SLICAnalysis', stepDefn)
    slicAppDefn.addModule(createoutputlist)
    slicAppDefn.createModuleInstance('ComputeOutputDataList',stepDefn)
    self._addParameter(slicAppDefn,'applicationVersion','string','','ApplicationVersion')
    self._addParameter(slicAppDefn,"applicationLog","string","","Application log file")
    self._addParameter(slicAppDefn,"detectorModel","string","","Name of the detector model")
    self._addParameter(slicAppDefn,"inputmacFile","string","","Name of the mac file")
    self._addParameter(slicAppDefn,"numberOfEvents","int",0,"Number of events to process")
    self._addParameter(slicAppDefn,"outputPath","string","","Output data path")
    self._addParameter(slicAppDefn,"outputFile","string","","output file name")
    self._addParameter(slicAppDefn,'listoutput',"list",[],"list of output file name")
    self.workflow.addStep(slicAppDefn)
    mstep = self.workflow.createStepInstance(stepDefn,stepName)
    mstep.setValue('applicationVersion',appVers)
    mstep.setValue('applicationLog', 'Slic_@{STEP_ID}.log')
    mstep.setValue("detectorModel",detectormodel)
    self.prodparameters['SlicDetectorModel']=detectormodel
    mstep.setValue("numberOfEvents",numberofevents)
    self.prodparameters['nbevts']=numberofevents
    mstep.setValue("inputmacFile",inputmac)
    self.prodparameters['SlicInputMAC']=inputmac
    mstep.setValue("outputFile",outputfile)
    mstep.setValue("outputPath",outputpath)
    outputList=[]
    outputList.append({"outputFile":"@{outputFile}","outputPath":"@{outputPath}","outputDataSE":outputSE})
    mstep.setValue('listoutput',(outputList))

    self.__addSoftwarePackages('slic.%s' %(appVers))
    self._addParameter(self.workflow,"SLICOutput","string",outputfile,"SLIC expected output file name")

    self.ioDict["SLICStep"]=mstep.getName()
    return S_OK()

  def addLCSIMStep(self,appVers,steeringXML="",outputfile="",outputRECfile=None,outputDSTfile=None,outputpathREC="",outputpathDST="",outputSE=""):
    """ Define LCSIM step for production

    @param appVers: LCSIM version to use
    @param outputfile: Outputfile name
    @todo: define properly REC and DST files
    @param outputSE: Storage element to use

    """
    kwargs = {"appVers":appVers,"steeringXML":steeringXML,"outputfile":outputfile,"outputRECfile":outputRECfile,"outputDSTfile":outputDSTfile,
              "outputpath":outputpathREC,"outputpath":outputpathDST,"outputSE":outputSE}
    if outputRECfile=="default":
      if self.basename:
        outputRECfile = self.basename+"_rec.slcio"
      else:
        return self._reportError("Rec File not defined",__name__,**kwargs)
    if outputDSTfile=="default":
      if self.basename:
        outputDSTfile = self.basename+"_dst.slcio"
      else:
        return self._reportError("DST File not defined",__name__,**kwargs)
    if not outputpathDST:
      if self.basepath:
        outputpathDST = self.basepath+"SID/DST"
      else:
        return self._reportError("Output path in Storage not defined",__name__,**kwargs)
    if not outputpathREC:
      if self.basepath:
        outputpathREC = self.basepath+"SID/REC"
      else:
        return self._reportError("Output path in Storage not defined",__name__,**kwargs)
    if not outputSE:
      return self._reportError('Storage Element to use not defined',__name__,**kwargs)
    if not steeringXML:
      return self._reportError('Steering XML file was not specified, please provide',__name__,**kwargs)
    self.StepCount +=1
    stepName = 'RunLCSIM'
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' %('LCSIM',stepNumber)
    self._addParameter(self.workflow,'TotalSteps','String',self.StepCount,'Total number of steps')

    LCSIMStep = ModuleDefinition('LCSIMAnalysis')
    LCSIMStep.setDescription('LCSIM step: reconstruction in SiD like detectors')
    body = string.replace(self.importLine,'<MODULE>','LCSIMAnalysis')
    LCSIMStep.setBody(body)
    createoutputlist = ModuleDefinition('ComputeOutputDataList')
    createoutputlist.setDescription('Compute the outputList parameter, needed by outputdataPolicy')
    body = string.replace(self.importLine,'<MODULE>','ComputeOutputDataList')
    createoutputlist.setBody(body)

    LCSIMAppDefn = StepDefinition(stepDefn)
    LCSIMAppDefn.addModule(LCSIMStep)
    LCSIMAppDefn.createModuleInstance('LCSIMAnalysis', stepDefn)
    LCSIMAppDefn.addModule(createoutputlist)
    LCSIMAppDefn.createModuleInstance('ComputeOutputDataList',stepDefn)
    self._addParameter(LCSIMAppDefn,'applicationVersion','string','','ApplicationVersion')
    self._addParameter(LCSIMAppDefn,"applicationLog","string","","Application log file")
    self._addParameter(LCSIMAppDefn,"inputSlcio","string","","List of input SLCIO files")
    self._addParameter(LCSIMAppDefn,"inputXML","string","","XML steering")
    self._addParameter(LCSIMAppDefn,"outputFile","string","","output file name")
    if outputRECfile:
      self._addParameter(LCSIMAppDefn,"outputPathREC","string","","Output REC data path")
      self._addParameter(LCSIMAppDefn,"outputREC","string","","output REC file name")
    if outputDSTfile:
      self._addParameter(LCSIMAppDefn,"outputDST","string","","output DST file name")
      self._addParameter(LCSIMAppDefn,"outputPathDST","string","","Output DST data path")
    if outputDSTfile or outputRECfile:
      self._addParameter(LCSIMAppDefn,'listoutput',"list",[],"list of output file name")
    self.workflow.addStep(LCSIMAppDefn)
    mstep = self.workflow.createStepInstance(stepDefn,stepName)
    mstep.setValue('applicationVersion',appVers)
    mstep.setValue('applicationLog', 'LCSIM_@{STEP_ID}.log')
    mstep.setValue('inputXML',steeringXML)

    if self.ioDict.has_key("SLICPanStep"):
      mstep.setLink('inputSlcio',self.ioDict["SLICPanStep"],'outputFile')
    elif self.ioDict.has_key("SLICStep"):
      mstep.setLink('inputSlcio',self.ioDict["SLICStep"],'outputFile')
    mstep.setValue("outputFile",outputfile)
    outputList=[]
    if outputRECfile:
      mstep.setValue("outputREC",outputRECfile)
      mstep.setValue("outputPathREC",outputpathREC)
      outputList.append({"outputFile":"@{outputREC}","outputPath":"@{outputPathREC}","outputDataSE":outputSE})
    if outputDSTfile:
      mstep.setValue("outputDST",outputDSTfile)
      mstep.setValue("outputPathDST",outputpathDST)
      outputList.append({"outputFile":"@{outputDST}","outputPath":"@{outputPathDST}","outputDataSE":outputSE})
    if len(outputList):
      mstep.setValue('listoutput',(outputList))

    self.__addSoftwarePackages('lcsim.%s' %(appVers))
    self.ioDict["LCSIMStep"]=mstep.getName()
    return S_OK()

  def addSLICPandoraStep(self,appVers,detector,pandorasettings = None,outputfile = ""):
    self.StepCount +=1
    stepName = 'RunSLICPan'
    stepNumber = self.StepCount
    stepDefn = '%sStep%s' %('SLICPandora',stepNumber)
    self._addParameter(self.workflow,'TotalSteps','String',self.StepCount,'Total number of steps')

    SLICPanStep = ModuleDefinition('SLICPandoraAnalysis')
    SLICPanStep.setDescription('SLIC Pandora step: Pandora for SID')
    body = string.replace(self.importLine,'<MODULE>','SLICPandoraAnalysis')
    SLICPanStep.setBody(body)
    createoutputlist = ModuleDefinition('ComputeOutputDataList')
    createoutputlist.setDescription('Compute the outputList parameter, needed by outputdataPolicy')
    body = string.replace(self.importLine,'<MODULE>','ComputeOutputDataList')
    createoutputlist.setBody(body)

    SLICPanAppDefn = StepDefinition(stepDefn)
    SLICPanAppDefn.addModule(SLICPanStep)
    SLICPanAppDefn.createModuleInstance('SLICPandoraAnalysis', stepDefn)
    SLICPanAppDefn.addModule(createoutputlist)
    SLICPanAppDefn.createModuleInstance('ComputeOutputDataList',stepDefn)
    self._addParameter(SLICPanAppDefn,'applicationVersion','string','','ApplicationVersion')
    self._addParameter(SLICPanAppDefn,"applicationLog","string","","Application log file")
    self._addParameter(SLICPanAppDefn,"inputSlcio","string","","List of input SLCIO files")
    self._addParameter(SLICPanAppDefn,"DetectorXML","string","","Detector Model")

    if pandorasettings:
      self._addParameter(SLICPanAppDefn,"PandoraSettings","string","","Pandora settings file for SID")

    self._addParameter(SLICPanAppDefn,"outputFile","string","","output file name")
    self.workflow.addStep(SLICPanAppDefn)
    mstep = self.workflow.createStepInstance(stepDefn,stepName)
    mstep.setValue('applicationVersion',appVers)
    mstep.setValue('applicationLog', 'SLICPan_@{STEP_ID}.log')
    mstep.setValue('DetectorXML',detector)

    if pandorasettings:
      mstep.setValue('PandoraSettings',pandorasettings)

    mstep.setValue("outputFile",outputfile)
    if self.ioDict.has_key("LCSIMStep"):
      mstep.setLink('inputSlcio',self.ioDict["LCSIMStep"],'outputFile')

    self.__addSoftwarePackages('slicpandora.%s' %(appVers))
    self.ioDict["SLICPanStep"]=mstep.getName()
    return S_OK()

  def addFinalizationStep(self,uploadData=False,uploadLog = False,sendFailover=False,registerData=False):
    """ Add finalization step

    @param uploadData: Upload or not the data to the storage
    @param uploadLog: Upload log file to storage (currently only available for admins, thus add them to OutputSandbox)
    @param sendFailover: Send Failover requests, and declare files as processed or unused in transfDB
    @param registerData: Register data in the file catalog
    @todo: Do the registration only once, instead of once for each job

    """
    dataUpload = ModuleDefinition('UploadOutputData')
    dataUpload.setDescription('Uploads the output data')
    self._addParameter(dataUpload,'Enable','bool','False','EnableFlag')
    body = string.replace(self.importLine,'<MODULE>','UploadOutputData')
    dataUpload.setBody(body)

    failoverRequest = ModuleDefinition('FailoverRequest')
    failoverRequest.setDescription('Sends any failover requests')
    self._addParameter(failoverRequest,'Enable','bool','True','EnableFlag')
    body = string.replace(self.importLine,'<MODULE>','FailoverRequest')
    failoverRequest.setBody(body)

    registerdata = ModuleDefinition('RegisterOutputData')
    registerdata.setDescription('Module to add in the metadata catalog the relevant info about the files')
    self._addParameter(registerdata,'Enable','bool','False','EnableFlag')
    body = string.replace(self.importLine,'<MODULE>','RegisterOutputData')
    registerdata.setBody(body)

    logUpload = ModuleDefinition('UploadLogFile')
    logUpload.setDescription('Uploads the output log files')
    self._addParameter(logUpload,'Enable','bool','False','EnableFlag')
    body = string.replace(self.importLine,'<MODULE>','UploadLogFile')
    logUpload.setBody(body)

    finalization = StepDefinition('Job_Finalization')

    dataUpload.setLink('Enable','self','UploadEnable')
    finalization.addModule(dataUpload)
    finalization.createModuleInstance('UploadOutputData','dataUpload')
    self._addParameter(finalization,'UploadEnable','bool',str(uploadData),'EnableFlag')

    registerdata.setLink('Enable','self','RegisterEnable')
    finalization.addModule(registerdata)
    finalization.createModuleInstance('RegisterOutputData','RegisterOutputData')
    self._addParameter(finalization,'RegisterEnable','bool',str(uploadLog),'EnableFlag')

    logUpload.setLink('Enable','self','LogEnable')
    finalization.addModule(logUpload)
    finalization.createModuleInstance('UploadLogFile','logUpload')
    self._addParameter(finalization,'LogEnable','bool',str(uploadLog),'EnableFlag')

    failoverRequest.setLink('Enable','self','FailoverEnable')
    finalization.addModule(failoverRequest)
    finalization.createModuleInstance('FailoverRequest','failoverRequest')
    self._addParameter(finalization,'FailoverEnable','bool',str(sendFailover),'EnableFlag')

    self.workflow.addStep(finalization)
    finalizeStep = self.workflow.createStepInstance('Job_Finalization', 'finalization')
    finalizeStep.setValue('UploadEnable',uploadData)
    finalizeStep.setValue('LogEnable',uploadLog)
    finalizeStep.setValue('FailoverEnable',sendFailover)
    finalizeStep.setValue('RegisterEnable',registerData)

    ##Hack until log server is available
    self.addToOutputSandbox.append("*.log")

    return S_OK()
  #############################################################################
  def create(self,name=None):
    """ Create the transformation based on the production definition

    @param name: name of the production
    """
    workflowName = self.workflow.getName()
    fileName = '%s.xml' %workflowName
    self.log.verbose('Workflow XML file name is: %s' %fileName)
    try:
      self.createWorkflow()
    except Exception,x:
      self.log.error(x)
      return S_ERROR('Could not create workflow')
    oFile = open(fileName,'r')
    workflowXML = oFile.read()
    oFile.close()
    if not name:
      name = workflowName
    ###Create Tranformation
    Trans = Transformation()
    Trans.setTransformationName(name)
    Trans.setDescription(self.workflow.getDescrShort())
    Trans.setLongDescription(self.workflow.getDescription())
    Trans.setType(self.type)
    self.prodparameters['JobType']=self.type
    Trans.setPlugin('Standard')
    Trans.setGroupSize(self.jobFileGroupSize)
    Trans.setTransformationGroup(self.prodGroup)
    Trans.setBody(workflowXML)
    Trans.setEventsPerTask(self.prodparameters['nbevts']*self.prodparameters['NbInputFiles'])
    res = Trans.addTransformation()
    if not res['OK']:
      print res['Message']
      return res
    self.currtrans = Trans
    self.currtrans.setStatus("Active")
    self.currtrans.setAgentType("Automatic")

    return S_OK()

  def setNbOfTasks(self,nbtasks):
    """ Define the number of tasks you want. Useful for generation jobs.
    """
    if not self.currtrans:
      print "Not transformation defined earlier"
      return S_ERROR("No transformation defined")
    if self.inputBKSelection:
      print "Meta data selection activated, should not specify the number of jobs"
      return S_ERROR()
    self.nbtasks = nbtasks
    self.currtrans.setMaxNumberOfTasks(self.nbtasks)
    return S_OK()

  def setInputDataQuery(self,metadata=None,prodid=None):
    """ Tell the production to update itself using the metadata query specified, i.e. submit new jobs if new files are added corresponding to same query.
    """
    currtrans = 0
    if self.currtrans:
      currtrans = self.currtrans.getTransformationID()['Value']
    if prodid:
      currtrans = prodid
    if not currtrans:
      print "Not transformation defined earlier"
      return S_ERROR("No transformation defined")
    if self.nbtasks:
      print "Nb of tasks defined already, should not use InputDataQuery"
      return S_ERROR()
    if metadata:
      self.inputBKSelection=metadata

    client = TransformationClient()
    res = client.createTransformationInputDataQuery(currtrans,self.inputBKSelection)
    if not res['OK']:
      return res
    return S_OK()

  def explainInputDataQuery(self):
    print """To create production using input data query, do the following:
    1) get the production number (prodid) you want to modify (from the web interface)
    2) code the following:
    p = Production()
    meta = {}
    meta['Some metadata key'] = some value
    p.setInputDataQuery(meta,prodid)
    3) check from web monitor that files are found. If not, let ilc-dirac@cern.ch know.
    """
    return

  def setInputDataDirectoryMask(self,mydir):
    """ More or less same feature as above, but useful for user's directory that don't have metadata info specified
    """
    self.currtrans.setFileMask(mydir)
    return S_OK()

  def setInputDataLFNs(self,lfns):
    """ Define by hand the input LFN list instead of relying on the input data query.

    Useful when list is obtained from a user's job reopository.
    """
    self.currtrans.addFilesToTransformation(lfns)
    return S_OK()

  def finalizeProdSubmission(self,prodid=None,prodinfo=None):
    currtrans = 0
    if self.currtrans:
      currtrans = self.currtrans.getTransformationID()['Value']
    if prodid:
      currtrans = prodid
    if not currtrans:
      print "Not transformation defined earlier"
      return S_ERROR("No transformation defined")
    if prodinfo:
      self.prodparameters = prodinfo

    info = []
    info.append('%s Production %s has following parameters:\n' %(self.prodparameters['JobType'],currtrans))
    if self.prodparameters.has_key("Process"):
      info.append('- Process %s'%self.prodparameters['Process'])
    if self.prodparameters.has_key("Energy"):
      info.append('- Energy %s GeV'%self.prodparameters["Energy"])
    info.append("- %s events per job"%(self.prodparameters['nbevts']*self.prodparameters['NbInputFiles']))
    if self.prodparameters.has_key('lumi'):
      if self.prodparameters['lumi']:
        info.append('    corresponding to a luminosity %s fb'%(self.prodparameters['lumi']*self.prodparameters['NbInputFiles']))

    if self.prodparameters.has_key("WhizardParameters"):
      info.append('- Whizard parameters: \n %s'%(string.join(self.prodparameters['WhizardParameters'].split(";"),'\n')))
    if  self.prodparameters['PostGenSelApplied']:
      info.append(' --> Events are selected after whizard generation !')
    if self.prodparameters.has_key('MokkaSteer'):
      info.append("- Mokka steering file %s"%(self.prodparameters['MokkaSteer']))
      if self.prodparameters.has_key('MokkaDetectorModel'):
        info.append("- Mokka detector model %s"%self.prodparameters['MokkaDetectorModel'])

    if self.prodparameters.has_key('MarlinXML'):
      info.append('- Marlin xml file %s'%self.prodparameters['MarlinXML'])
    if self.prodparameters.has_key('MarlinGEAR'):
      info.append("- Marlin GEAR file %s"%self.prodparameters['MarlinGEAR'])

    if self.prodparameters.has_key('SlicDetectorModel'):
      info.append("- SLIC detector model %s"%self.prodparameters['SlicDetectorModel'])
    if self.prodparameters.has_key('SlicInputMAC'):
      info.append('- SLIC MAC file %s'%self.prodparameters['SlicInputMAC'])

    if self.prodparameters.has_key('BXOverlay'):
      if self.prodparameters['BXOverlay']:
        info.append('- Overlaying %s bunch crossings of gamma gamma -> hadrons'%self.prodparameters['BXOverlay'])
        if self.prodparameters['GGInt']:
          info.append('  Using %s gamma gamma -> hadrons interactions per bunch crossing'%self.prodparameters['GGInt'])
        else:
          info.append('  Using default 3.2 gamma gamma -> hadrons interactions per bunch crossing')
    
    if self.prodparameters['UsingWhizardOutput']:
      info.append('Mokka or SLIC use whizard output from previous step')
    if self.prodparameters['UsingMokkaOutput']:
      info.append('Marlin uses mokka output from previous step')
    if self.prodparameters['UsingSLICOutput']:
      info.append('LCSIM uses slic output from previous step')
    if not self.prodparameters['UsingWhizardOutput'] and not self.prodparameters['UsingMokkaOutput'] and not self.prodparameters['UsingSLICOutput'] \
      and self.prodparameters.has_key('FCInputQuery'):
      info.append('Using InputDataQuery :')
      for n,v in self.prodparameters['FCInputQuery'].items():
        info.append('    %s = %s' %(n,v))

    info.append('- SW packages %s'%self.prodparameters["SWPackages"])

    infoString = string.join(info,'\n')
    self.prodparameters['DetailedInfo']=infoString
    for n,v in self.prodparameters.items():
      result = self.setProdParameter(currtrans,n,v)
      if not result['OK']:
        self.log.error(result['Message'])

    return S_OK()

  #############################################################################
  def setProdParameter(self,prodID,pname,pvalue):
    """Set a production parameter.
    """
    if type(pvalue)==type([]):
      pvalue=string.join(pvalue,'\n')

    prodClient = RPCClient('Transformation/TransformationManager',timeout=120)
    if type(pvalue)==type(2):
      pvalue = str(pvalue)
    result = prodClient.setTransformationParameter(int(prodID),str(pname),str(pvalue))
    if not result['OK']:
      self.log.error('Problem setting parameter %s for production %s and value:\n%s' %(prodID,pname,pvalue))
    return result

  #############################################################################
  def getParameters(self,prodID,pname='',printOutput=False):
    """Get a production parameter or all of them if no parameter name specified.
    """
    prodClient = RPCClient('Transformation/TransformationManager',timeout=120)
    result = prodClient.getTransformation(int(prodID),True)
    if not result['OK']:
      self.log.error(result)
      return S_ERROR('Could not retrieve parameters for production %s' %prodID)

    if not result['Value']:
      self.log.info(result)
      return S_ERROR('No additional parameters available for production %s' %prodID)

    if pname:
      if result['Value'].has_key(pname):
        return S_OK(result['Value'][pname])
      else:
        self.log.verbose(result)
        return S_ERROR('Production %s does not have parameter %s' %(prodID,pname))

    if printOutput:
      for n,v in result['Value'].items():
        if not n.lower()=='body':
          print '='*len(n),'\n',n,'\n','='*len(n)
          print v
        else:
          print '*Omitted Body from printout*'

    return result
  #############################################################################
  def __addSoftwarePackages(self,nameVersion):
    """ Internal method to accumulate software packages.
    """
    swPackages = 'SoftwarePackages'
    description='ILCSoftwarePackages'
    if not self.workflow.findParameter(swPackages):
      self._addParameter(self.workflow,swPackages,'JDL',nameVersion,description)
      self.prodparameters["SWPackages"]=nameVersion
    else:
      apps = self.workflow.findParameter(swPackages).getValue()
      apps = apps.split(';')
      if not apps.count(nameVersion):
        apps.append(nameVersion)
        apps = removeEmptyElements(apps)
      apps = string.join(apps,';')
      self._addParameter(self.workflow,swPackages,'JDL',apps,description)
      self.prodparameters["SWPackages"]=apps
  #############################################################################
  def createWorkflow(self):
    """ Create XML for local testing.
    """
    name = '%s.xml' % self.name
    if os.path.exists(name):
      shutil.move(name,'%s.backup' %name)
    self.workflow.toXMLFile(name)
  #############################################################################
  def runLocal(self):
    """ Create XML workflow for local testing then reformulate as a job and run locally.
    """
    name = '%s.xml' % self.name
    if os.path.exists(name):
      shutil.move(name,'%s.backup' %name)
    self.workflow.toXMLFile(name)
    j = ILCJob(name)
    d = DiracILC()
    return d.submit(j,mode='local')

  #############################################################################
  def setFileMask(self,fileMask):
    """Output data related parameters.
    """
    if type(fileMask)==type([]):
      fileMask = string.join(fileMask,';')
    self._setParameter('outputDataFileMask','string',fileMask,'outputDataFileMask')

  #############################################################################
  def setWorkflowName(self,name):
    """Set workflow name.
    """
    self.workflow.setName(name)
    self.name = name

  #############################################################################
  def setWorkflowDescription(self,desc):
    """Set workflow name.
    """
    self.workflow.setDescription(desc)

  #############################################################################
  def setProdType(self,prodType):
    """Set prod type.
    """
    if not prodType in self.prodTypes:
      raise TypeError,'Prod must be one of %s' %(string.join(self.prodTypes,', '))
    self.setType(prodType)

  #############################################################################
  def banTier1s(self):
    """ Sets Tier1s as banned.
    """
    self.setBannedSites(self.tier1s)

  #############################################################################
  def setTargetSite(self,site):
    """ Sets destination for all jobs.
    """
    self.setDestination(site)

  #############################################################################
  def setOutputMode(self,outputMode):
    """ Sets output mode for all jobs, this can be 'Local' or 'Any'.
    """
    if not outputMode.lower().capitalize() in ('Local','Any'):
      raise TypeError,'Output mode must be Local or Any'
    self._setParameter('outputMode','string',outputMode.lower().capitalize(),'SEResolutionPolicy')
  #############################################################################
  def setProdPriority(self,priority):
    """ Sets destination for all jobs.
    """
    self._setParameter('Priority','JDL',str(priority),'UserPriority')

  #############################################################################
  def setProdGroup(self,group):
    """ Sets a user defined tag for the production as appears on the monitoring page
    """
    self.prodGroup = group

  #############################################################################
  def setProdPlugin(self,plugin):
    """ Sets the plugin to be used to creating the production jobs
    """
    self.plugin = plugin

  #############################################################################
  def setInputFileMask(self,fileMask):
    """ Sets the input data selection when using file mask.
    """
    self.inputFileMask = fileMask

  #############################################################################
  #############################################################################
  def setJobFileGroupSize(self,files):
    """ Sets the number of files to be input to each job created.
    """
    self.jobFileGroupSize = files
    self.prodparameters['NbInputFiles'] = files

  #############################################################################
  def setWorkflowString(self, wfString):
    """ Uses the supplied string to create the workflow
    """
    self.workflow = fromXMLString(wfString)
    self.name = self.workflow.getName()

  #############################################################################
  def disableCPUCheck(self):
    """ Uses the supplied string to create the workflow
    """
    self._setParameter('DisableCPUCheck','JDL','True','DisableWatchdogCPUCheck')
