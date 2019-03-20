"""Create productions for the Whizard2/DDSim/Marlin software chain.

First create a config file template::

  dirac-clic-make-productions -p

Then modify the template to describe the productions::

    [Production Parameters]
    detectorModel = CLIC_o3_v14
    version = 2018-08-10
    softwareVersion = ILCSoft-%(version)s_gcc62
    configVersion = ILCSoft-%(version)s
    whizard2Version = 2.6.3
    productionLogLevel = VERBOSE
    outputSE = CERN-DST-EOS
    finalOutputSE = CERN-SRM
    DRC=200
    prodGroup = %(detectorModel)s_DiJets
    ProdTypes = Gen, Sim, Rec, RecOver
    energies =                100, 200
    processes =          DY_uds
    eventsPerJobs =           100
    MoveTypes = REC, GEN, SIM
    MoveStatus = Active
    MoveGroupSize = 10
    move = True
    overlayEvents = 380GeV
    cliReco = --Config.Tracking=Conformal --MyDDMarlinPandora.D0TrackCut=%(DRC)s
        --MyDDMarlinPandora.Z0TrackCut=%(DRC)s --MyDDMarlinPandora.MaxBarrelTrackerInnerRDistance=%(DRC)s
    additionalName = 3
    whizard2SinFile = dy_uds.sin

Further options can be found in the created template file. Many options can contain comma separated values to submit
multiple chains of productions in one command. The example above will create two chains of Generation, Simulation,
Reconstruction and Reconstruction with overlay transformations, one for 100 GeV and one for 200 GeV.

Then test if everything works::

  dirac-clic-make-productions -f myProd

And finally submit to the system::

  dirac-clic-make-productions -f myProd -x

Options:

   -p, --printConfigFile      Create the template to create productions
   -f, --configFile <file>    Defines the file with the parameters to create a production
   -x, --enable               Disable dry-run mode and actually create the production
   --additionalName       Define a string to add to the production name if the original name already exists


Parameters in the steering file

  :configPackage: Steering file package to use for simulation and reconstruction
  :configVersion: Steering file version to use for simulation and reconstruction
  :detectorModel: Detector Model to use in simulation and reconstruction
  :outputSE: output SE for transformation jobs
  :finalOutputSE: final destination for files when moving transformations are enabled
  :prodGroup: basename of the production group the productions are part of
  :productionLogLevel: log level to use in production jobs
  :softwareVersion: softwareVersion to use for generation/simulation/reconstruction
  :additionalName: additionalName to add to the transformation name in case a transformation
     with that name already exists

  :ProdTypes: Which transformations to create: Gen, Split, Sim, Rec, RecOver
  :MoveTypes: Which output file types to move: Gen, Sim, Rec, Dst
  :MoveGroupSize: The number of files to put in one replicationRequest
  :MoveStatus: The status of the Replication transformations: Active or Stopped
  :move: Whether or not to create the transformations to the output files to the finalOutputSE

  :energies: energy to use for generation or meta data search for each transformation chain
  :eventsPerJobs: number of events per job
  :processes: name of the processes to generate or use in meta data search
  :prodids: transformation IDs to use in meta data search for the first transformation of each chain

  :whizard2SinFile: path to sindarin file to be used with
     :mod:`~ILCDIRAC.Interfaces.API.NewInterface.Applications.Whizard2`
  :whizard2Version: specify which version of :mod:`~ILCDIRAC.Interfaces.API.NewInterface.Applications.Whizard2` to use
  :numberOfTasks: number of production jobs/task to create for Gen transformations (default is 1)

  :eventsInSplitFiles: For split transformations, number of events in the input files

  :cliReco: additional CLI options for reconstruction

  :overlayEvents: For ``RecOver`` transformations use these events for Overlay. By default the gghad
     events with the process energy are used for overlay. E.g.: ``380GeV``


The individual applications can be further modified in their respective section::

  [Marlin]
  #ApplicationAttributeName=Value

  [DDSim]
  #ApplicationAttributeName=Value

  [Overlay]
  #ApplicationAttributeName=Value

  [Whizard2]
  #ApplicationAttributeName=Value

All attributes with a ``set`` method can be changed. See
:mod:`~ILCDIRAC.Interfaces.API.NewInterface.Applications.Whizard2`,
:mod:`~ILCDIRAC.Interfaces.API.NewInterface.Applications.Marlin`,
:mod:`~ILCDIRAC.Interfaces.API.NewInterface.Applications.OverlayInput`,
:mod:`~ILCDIRAC.Interfaces.API.NewInterface.Applications.DDSim`.



:since: July 14, 2017
:author: A Sailer

"""

#pylint disable=wrong-import-position

from __future__ import print_function
from pprint import pformat
from collections import defaultdict
from copy import deepcopy
from itertools import izip_longest
import ConfigParser
import os

from DIRAC.Core.Base import Script
from DIRAC import S_OK, S_ERROR, gLogger

from ILCDIRAC.Core.Utilities.OverlayFiles import energyWithUnit, energyToInt
from ILCDIRAC.Core.Utilities.Utilities import listify
from ILCDIRAC.ILCTransformationSystem.Utilities.Utilities import Task

PRODUCTION_PARAMETERS = 'Production Parameters'
PP = PRODUCTION_PARAMETERS
APPLICATION_LIST = ['Marlin', 'DDSim', 'Overlay', 'Whizard2']
LIST_ATTRIBUTES = ['ignoreMetadata',
                   'whizard2SinFile',
                   'energies',
                   'eventsPerJobs',
                   'numberOfTasks',
                   'processes',
                   'prodIDs',
                   'eventsInSplitFiles',
                   ]

STRING_ATTRIBUTES = ['configPackage',
                     'configVersion',
                     'additionalName',
                     'productionloglevel',
                     'outputSE',
                     'finalOutputSE',
                     'whizard2Version',
                     'MoveStatus',
                     'MoveGroupSize',
                     'prodGroup',
                     'detectorModel',
                     'softwareVersion',
                     'overlayEvents',
                     'overlayEventType',
                     ]


class Params(object):
  """Parameter Object"""
  def __init__(self):
    self.prodConfigFilename = None
    self.dumpConfigFile = False
    self.dryRun = True
    self.additionalName = ''

  def setProdConf(self,fileName):
    if not os.path.exists( fileName ):
      return S_ERROR("ERROR: File %r not found" % fileName )
    self.prodConfigFilename = fileName
    return S_OK()
  def setDumpConf(self, _):
    self.dumpConfigFile = True
    return S_OK()
  def setEnable(self, _):
    self.dryRun = False
    return S_OK()
  def setAddName(self, addName):
    self.additionalName = addName
    return S_OK()

  def registerSwitches(self):
    Script.registerSwitch("f:", "configFile=", "Set config file for production", self.setProdConf)
    Script.registerSwitch("x", "enable", "create productions, if off dry-run", self.setEnable)
    Script.registerSwitch("p", "printConfigFile", "Print a config file to stdout", self.setDumpConf)
    Script.registerSwitch("", "additionalName=", "Name to add to the production", self.setAddName)
    Script.setUsageMessage("""%s --configFile=myProduction""" % ("dirac-clic-make-productions", ) )


class CLICDetProdChain( object ):
  """Create applications and productions for CLIC physics studies."""

  class Flags( object ):
    """ flags to enable or disable productions

    :param bool dryRun: if False no productions are created
    :param bool gen: if True create generation production
    :param bool spl: if True create split production
    :param bool sim: if True create simulation production
    :param bool rec: if True create reconstruction production
    :param bool over: if True create reconstruction production with overlay, if `rec` is False this flag is also False
    :param bool move: if True create moving transformations, the other move flags only take effect if this one is True
    :param bool moveGen: if True move GEN files after they have been used in the production, also for split files
    :param bool moveSim: if True move SIM files after they have been used in the production
    :param bool moveRev: if True move REC files when they were created
    :param bool moveDst: if True move DST files when they were created
    """

    def __init__( self ):
      # general flag to create anything at all
      self._dryRun = True

      #create transformations
      self._gen = False
      self._spl = False
      self._sim = False
      self._rec = False
      self._over = False

      # create moving transformations
      self._moves = False
      self._moveGen = False
      self._moveSim = False
      self._moveRec = False
      self._moveDst = False

      ## list of tuple to preserve order
      self._prodTypes = [ ('gen', 'Gen'), ('spl', 'Split'), ('sim', 'Sim'), ('rec', 'Rec'), ('over', 'RecOver') ]
      self._moveTypes = [ ('moveGen', 'Gen'), ('moveSim', 'Sim'), ('moveRec', 'Rec'), ('moveDst', 'Dst') ]

    @property
    def dryRun( self ): #pylint: disable=missing-docstring
      return self._dryRun
    @property
    def gen( self ): #pylint: disable=missing-docstring
      return self._gen
    @property
    def spl( self ): #pylint: disable=missing-docstring
      return self._spl
    @property
    def sim( self ): #pylint: disable=missing-docstring
      return self._sim
    @property
    def rec( self ): #pylint: disable=missing-docstring
      return self._rec
    @property
    def over( self ): #pylint: disable=missing-docstring
      return self._over
    @property
    def move( self ): #pylint: disable=missing-docstring
      return self._moves
    @property
    def moveGen( self ): #pylint: disable=missing-docstring
      return (self._gen or self._spl) and self._moves and self._moveGen
    @property
    def moveSim( self ): #pylint: disable=missing-docstring
      return self._sim and self._moves and self._moveSim
    @property
    def moveRec( self ): #pylint: disable=missing-docstring
      return (self._rec or self._over) and self._moves and self._moveRec
    @property
    def moveDst( self ): #pylint: disable=missing-docstring
      return (self._rec or self._over) and self._moves and self._moveDst


    def __str__( self ):
      pDict = vars(self)
      self.updateDictWithFlags( pDict )
      return """

#Productions to create: %(prodOpts)s
ProdTypes = %(prodTypes)s

move = %(_moves)s

#Datatypes to move: %(moveOpts)s
MoveTypes = %(moveTypes)s
""" %( vars(self) )

    def updateDictWithFlags( self, pDict ):
      """ add flags and values to pDict """
      for attr in dir(self):
        if isinstance( getattr(type(self), attr, None), property):
          pDict.update( { attr: str(getattr(self, attr)) } )

      pDict.update( prodOpts = ", ".join([ pTuple[1] \
                                           for pTuple in self._prodTypes ] ) )
      pDict.update( prodTypes = ", ".join([ pTuple[1] \
                                            for pTuple in self._prodTypes \
                                            if getattr( self, pTuple[0]) ] ) )
      pDict.update( moveOpts = ", ".join([ pTuple[1] \
                                            for pTuple in self._moveTypes ] ) )
      pDict.update( moveTypes = ", ".join([ pTuple[1] \
                                            for pTuple in self._moveTypes \
                                            if getattr( self, '_'+pTuple[0] ) ] ) )


    def __splitStringToOptions( self, config, tuples, optString, prefix='_'):
      """ split the option string into separate values and set the corresponding flag """
      prodsToCreate = config.get( PRODUCTION_PARAMETERS, optString )
      for prodType in listify(prodsToCreate):
        if not prodType:
          continue
        found = False
        for attribute, name in tuples:
          if name.capitalize() == prodType.strip().capitalize():
            setattr( self, prefix+attribute, True )
            found = True
            break
        if not found:
          raise AttributeError( "Unknown parameter: %r " % prodType )

    def loadFlags( self, config ):
      """ load flags values from configfile """
      self.__splitStringToOptions( config, self._prodTypes, 'ProdTypes', prefix='_' )
      self.__splitStringToOptions( config, self._moveTypes, 'MoveTypes', prefix='_' )
      self._moves = config.getboolean( PP, 'move' )

  def __init__(self, params=None, group='ilc_prod'):

    from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
    self._ops = Operations( vo='ilc' )

    self._machine = {'ilc_prod': 'clic',
                     'fcc_prod': 'fccee',
                     }[group]

    self.overlayEventType = {'ilc_prod': 'gghad',
                             'fcc_prod': 'pairs',
                             }[group]

    self.prodGroup = 'several'
    prodPath = os.path.join('/Production', self._machine.upper())
    self.basepath = self._ops.getValue(os.path.join(prodPath, 'BasePath'))

    self.detectorModel = self._ops.getValue(os.path.join(prodPath, 'DefaultDetectorModel'))
    self.softwareVersion = self._ops.getValue(os.path.join(prodPath, 'DefaultSoftwareVersion'))
    self.configVersion = self._ops.getValue(os.path.join(prodPath, 'DefaultConfigVersion'))
    self.configPackage = self._ops.getValue(os.path.join(prodPath, 'DefaultConfigPackage'))
    self.productionLogLevel = 'VERBOSE'
    self.outputSE = 'CERN-DST-EOS'
    self.moveStatus = 'Stopped'
    self.moveGroupSize = '10'

    self.ddsimSteeringFile = 'clic_steer.py'
    self.marlinSteeringFile = 'clicReconstruction.xml'

    self.eventsPerJobs = []
    self.numberOfTasks = []
    self.energies = []
    self.processes = []
    self.prodIDs = []
    self.eventsInSplitFiles = []

    # final destination for files once they have been used
    self.finalOutputSE = self._ops.getValue( 'Production/CLIC/FailOverSE' )

    self.additionalName = params.additionalName

    self.overlayEvents = ''

    self.cliRecoOption = ''
    self.cliReco = ''

    self.whizard2Version = self._ops.getValue('Production/CLIC/DefaultWhizard2Version')
    self.whizard2SinFile = []

    self.ignoreMetadata = []

    self.applicationOptions = {appName: {} for appName in APPLICATION_LIST}

    self._flags = self.Flags()

    self.loadParameters( params )

    self._flags._dryRun = params.dryRun #pylint: disable=protected-access

  def meta( self, prodID, process, energy ):
    """ return meta data dictionary, always new"""
    metaD = {'ProdID': str(prodID),
             'EvtType': process,
             'Energy' : self.metaEnergy( energy ),
             'Machine': self._machine,
             }
    for key in self.ignoreMetadata:
      metaD.pop(key)
    return metaD

  def loadParameters(self, parameter):
    """Load parameters from config file."""
    if parameter.prodConfigFilename is not None:
      defaultValueDict = vars(self)
      self._flags.updateDictWithFlags(defaultValueDict)
      # we are passing all instance attributes as the default dict so generally we do not have to check
      # if an option exists, also options are case insensitive and stored in lowercase
      config = ConfigParser.SafeConfigParser(defaults=defaultValueDict, dict_type=dict)
      config.read(parameter.prodConfigFilename)
      self._flags.loadFlags(config)

      for attribute in LIST_ATTRIBUTES:
        setattr(self, attribute, listify(config.get(PP, attribute)))

      for attribute in STRING_ATTRIBUTES:
        setattr(self, attribute, config.get(PP, attribute))

      # this parameter is deprecated and not part of the instance attributes so we need to check for existence
      if config.has_option(PP, 'clicConfig'):
        gLogger.warn('"clicConfig" parameter is deprected, please dump a new steering file!')
        self.configVersion = config.get(PP, 'clicConfig')

      # attribute and option names differ, special treatment
      self.cliRecoOption = config.get(PP, 'cliReco')

      if self.moveStatus not in ('Active', 'Stopped'):
        raise AttributeError("MoveStatus can only be 'Active' or 'Stopped' not %r" % self.moveStatus)

      self.overlayEvents = self.checkOverlayParameter(self.overlayEvents)
      self.overlayEventType = self.overlayEventType + self.overlayEvents.lower()

      self.processes = [ process.strip() for process in self.processes if process.strip() ]
      self.energies = [ float(eng.strip()) for eng in self.energies if eng.strip() ]
      self.eventsPerJobs = [ int( epj.strip() ) for epj in self.eventsPerJobs if epj.strip() ]
      ## these do not have to exist so we fill them to the same length if they are not set
      self.prodIDs = [ int( pID.strip() ) for pID in self.prodIDs if pID.strip() ]
      self.prodIDs = self.prodIDs if self.prodIDs else [ 1 for _ in self.energies ]

      # if one of the lists only has size 1 and there is a longer list we extend
      # the list to the maximum size assuming the values are re-used
      maxLength = 0
      parameterLists = [self.processes, self.energies, self.eventsPerJobs, self.whizard2SinFile]
      for parList in parameterLists:
        maxLength = len(parList) if len(parList) > maxLength else maxLength
      for parList in parameterLists:
        if len(parList) == 1 and maxLength > 1:
          parList.extend([parList[0]] * (maxLength - 1))

      if not (self.processes and self.energies and self.eventsPerJobs) and self.prodIDs:
        eventsPerJobSave = list(self.eventsPerJobs) if self.eventsPerJobs else None
        self._getProdInfoFromIDs()
        self.eventsPerJobs = eventsPerJobSave if eventsPerJobSave else self.eventsPerJobs

      self.numberOfTasks = [int(nbtask.strip()) for nbtask in self.numberOfTasks if nbtask.strip()]
      self.numberOfTasks = self.numberOfTasks if self.numberOfTasks else [1 for _ in self.energies]

      if len(self.processes) != len(self.energies) or \
         len(self.energies) != len(self.eventsPerJobs) or \
         len( self.prodIDs) != len(self.eventsPerJobs):
        raise AttributeError( "Lengths of Processes, Energies, and EventsPerJobs do not match" )

      if self._flags.gen:
        if len(self.numberOfTasks) != len(self.energies) or \
           len(self.whizard2SinFile) != len(self.energies):
          raise AttributeError("Lengths of numberOfTasks, whizard2SinFile, and Energies do not match")

      self.eventsInSplitFiles = listify(self.eventsInSplitFiles, int)
      self.eventsInSplitFiles = self.eventsInSplitFiles if self.eventsInSplitFiles else [-1] * len(self.energies)

      if self._flags.spl and len(self.eventsInSplitFiles) != len(self.energies):
        raise AttributeError( "Length of eventsInSplitFiles does not match: %d vs %d" %(
          len(self.eventsInSplitFiles), \
          len(self.energies) ) )

      # read options from application sections
      config2 = ConfigParser.SafeConfigParser(dict_type=dict)
      config2.optionxform = str  # do not transform options to lowercase
      config2.read(parameter.prodConfigFilename)
      for appName in APPLICATION_LIST:
        try:
          self.applicationOptions[appName] = dict(config2.items(appName))
        except ConfigParser.NoSectionError:
          pass

    if parameter.dumpConfigFile:
      print(self)
      raise RuntimeError('')

  def _getProdInfoFromIDs(self):
    """get the processName, energy and eventsPerJob from the MetaData catalog

    :raises: AttributeError if some of the information cannot be found
    :returns: None
    """
    if not self.prodIDs:
      raise AttributeError("No prodIDs defined")

    self.eventsPerJobs = []
    self.processes = []
    self.energies = []
    from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
    from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
    trc = TransformationClient()
    fc = FileCatalogClient()
    for prodID in self.prodIDs:
      gLogger.notice("Getting information for %s" % prodID)
      tRes = trc.getTransformation(str(prodID))
      if not tRes['OK']:
        raise AttributeError("No prodInfo found for %s" % prodID)
      self.eventsPerJobs.append(int(tRes['Value']['EventsPerTask']))
      lfnRes = fc.findFilesByMetadata({'ProdID': prodID})
      if not lfnRes['OK'] or not lfnRes['Value']:
        raise AttributeError("Could not find files for %s: %s " % (prodID, lfnRes.get('Message', lfnRes.get('Value'))))
      path = os.path.dirname(lfnRes['Value'][0])
      fileRes = fc.getDirectoryUserMetadata(path)
      self.processes.append(fileRes['Value']['EvtType'])
      self.energies.append(fileRes['Value']['Energy'])
      gLogger.notice("Found (Evts,Type,Energy): %s %s %s " %
                     (self.eventsPerJobs[-1], self.processes[-1], self.energies[-1]))

  def __str__( self ):
    pDict = vars(self)
    appOptionString = ''
    for appName in APPLICATION_LIST:
      appOptionString += '[%s]\n#ApplicationAttributeName=Value\n\n' % appName

    pDict.update({'ProductionParameters':PRODUCTION_PARAMETERS})
    pDict.update({'ApplicationOptions': appOptionString})
    return """
%(ApplicationOptions)s
[%(ProductionParameters)s]
prodGroup = %(prodGroup)s
detectorModel = %(detectorModel)s
softwareVersion = %(softwareVersion)s
whizard2Version = %(whizard2Version)s
whizard2SinFile = %(whizard2SinFile)s
configVersion = %(configVersion)s
configPackage = %(configPackage)s
eventsPerJobs = %(eventsPerJobs)s
## Number of jobs/task to generate (default = 1)
# numberOfTasks =

energies = %(energies)s
processes = %(processes)s
## optional prodid to search for input files
# prodIDs =

## number of events for input files to split productions
eventsInSplitFiles = %(eventsInSplitFiles)s

productionLogLevel = %(productionLogLevel)s
outputSE = %(outputSE)s

finalOutputSE = %(finalOutputSE)s
MoveStatus = %(moveStatus)s
MoveGroupSize = %(moveGroupSize)s

## optional additional name
# additionalName = %(additionalName)s

## optional marlin CLI options
# cliReco = %(cliReco)s

overlayEventType = %(overlayEventType)s
## optional energy to use for overlay: e.g. 3TeV
# overlayEvents = %(overlayEvents)s

%(_flags)s

""" %( pDict )

  @staticmethod
  def metaEnergy(energy):
    """Return string of the energy with no non-zero digits."""
    if isinstance(energy, basestring):
      return energy
    energy = ("%1.2f" % energy).rstrip('0').rstrip('.')
    return energy

  @staticmethod
  def checkOverlayParameter( overlayParameter ):
    """ check that the overlay parameter has the right formatting, XTeV or YYYGeV """
    if not overlayParameter:
      return overlayParameter
    if not any( overlayParameter.endswith( unit ) for unit in ('GeV', 'TeV') ):
      raise RuntimeError( "OverlayParameter %r does not end with unit: X.YTeV, ABCGeV" % overlayParameter )

    return overlayParameter


  @staticmethod
  def getParameterDictionary( process ):
    """ Create the proper structures to build all the prodcutions for the samples with *ee_*, *ea_*, *aa_*. """
    plist = []
    if 'ee_' in process:
      plist.append({'process': process,'pname1':'e1', 'pname2':'E1', "epa_b1":'F', "epa_b2":'F', "isr_b1":'T', "isr_b2":'T'})
    elif 'ea_' in process:
      plist.append({'process': process,'pname1':'e1', 'pname2':'E1', "epa_b1":'F', "epa_b2":'T', "isr_b1":'T', "isr_b2":'F'})
      plist.append({'process': process,'pname1':'e1', 'pname2':'A', "epa_b1":'F', "epa_b2":'F', "isr_b1":'T', "isr_b2":'F'})
      plist.append({'process': process.replace("ea_","ae_"),'pname1':'e1', 'pname2':'E1', "epa_b1":'T', "epa_b2":'F', "isr_b1":'F', "isr_b2":'T'})
      plist.append({'process': process.replace("ea_","ae_"),'pname1':'A', 'pname2':'E1', "epa_b1":'F', "epa_b2":'F', "isr_b1":'F', "isr_b2":'T'})
    elif 'aa_' in process:
      plist.append({'process':process,'pname1':'e1', 'pname2':'E1', "epa_b1":'T', "epa_b2":'T', "isr_b1":'F', "isr_b2":'F'})
      plist.append({'process':process,'pname1':'e1', 'pname2':'A', "epa_b1":'T', "epa_b2":'F', "isr_b1":'F', "isr_b2":'F'})
      plist.append({'process':process,'pname1':'A', 'pname2':'E1', "epa_b1":'F', "epa_b2":'T', "isr_b1":'F', "isr_b2":'F'})
      plist.append({'process':process,'pname1':'A', 'pname2':'A', "epa_b1":'F', "epa_b2":'F', "isr_b1":'F', "isr_b2":'F'})
    else:
      plist.append({'process':process,'pname1':'e1', 'pname2':'E1', "epa_b1":'F', "epa_b2":'F', "isr_b1":'T', "isr_b2":'T'})
    return plist

  @staticmethod
  def setOverlayParameters(energy, machine, overlay):
    """Set parameters for the overlay application, depending on energy and machine.

    :param float energy: energy to get parameters for
    :param overlay: overlay application
    """
    overlay.setMachine({'clic': 'clic_opt', 'fccee': 'fccee'}[machine])
    {'clic': {350.: (lambda o: [o.setBXOverlay(30), o.setGGToHadInt(0.0464), o.setProcessorName('Overlay350GeV')]),
              380.: (lambda o: [o.setBXOverlay(30), o.setGGToHadInt(0.0464), o.setProcessorName('Overlay380GeV')]),
              420.: (lambda o: [o.setBXOverlay(30), o.setGGToHadInt(0.17), o.setProcessorName('Overlay420GeV')]),
              500.: (lambda o: [o.setBXOverlay(30), o.setGGToHadInt(0.3), o.setProcessorName('Overlay500GeV')]),
              1400.: (lambda o: [o.setBXOverlay(30), o.setGGToHadInt(1.3), o.setProcessorName('Overlay1.4TeV')]),
              3000.: (lambda o: [o.setBXOverlay(30), o.setGGToHadInt(3.2), o.setProcessorName('Overlay3TeV')]),
              },
     'fccee': {91.2: (lambda o: [o.setBXOverlay(20), o.setGGToHadInt(1.0), o.setProcessorName('Overlay91GeV')]),
               365: (lambda o: [o.setBXOverlay(3), o.setGGToHadInt(1.0), o.setProcessorName('Overlay365GeV')]),
               },
     }[machine][energy](overlay)

  @staticmethod
  def createSplitApplication( eventsPerJob, eventsPerBaseFile, splitType='stdhep' ):
    """ create Split application """
    from ILCDIRAC.Interfaces.API.NewInterface.Applications import StdHepSplit, SLCIOSplit

    if splitType.lower() == 'stdhep':
      stdhepsplit = StdHepSplit()
      stdhepsplit.setVersion("V3")
      stdhepsplit.setNumberOfEventsPerFile( eventsPerJob )
      stdhepsplit.datatype = 'gen'
      stdhepsplit.setMaxRead( eventsPerBaseFile )
      return stdhepsplit

    if  splitType.lower() == 'lcio':
      split = SLCIOSplit()
      split.setNumberOfEventsPerFile( eventsPerJob )
      return stdhepsplit

    raise NotImplementedError( 'unknown splitType: %s ' % splitType )

  def addOverlayOptionsToMarlin( self, energy ):
    """ add options to marlin that are needed for running with overlay """
    energyString = self.overlayEvents if self.overlayEvents else energyWithUnit( energy )
    self.cliReco += ' --Config.Overlay=%s ' % energyString

  def createWhizard2Application(self, task):
    """ create Whizard2 Application """
    from ILCDIRAC.Interfaces.API.NewInterface.Applications import Whizard2

    whiz = Whizard2()
    whiz.setVersion(self.whizard2Version)
    whiz.setSinFile(task.sinFile)
    whiz.setEvtType(task.meta['EvtType'])
    whiz.setNumberOfEvents(task.eventsPerJob)
    whiz.setEnergy(task.meta['Energy'])
    self._setApplicationOptions('Whizard2', whiz, task.applicationOptions)

    return whiz

  def createDDSimApplication(self, task):
    """ create DDSim Application """
    from ILCDIRAC.Interfaces.API.NewInterface.Applications import DDSim

    ddsim = DDSim()
    ddsim.setVersion( self.softwareVersion )
    ddsim.setSteeringFile(self.ddsimSteeringFile)
    ddsim.setDetectorModel( self.detectorModel )

    self._setApplicationOptions('DDSim', ddsim, task.applicationOptions)

    return ddsim

  def createOverlayApplication(self, task):
    """ create Overlay Application """
    from ILCDIRAC.Interfaces.API.NewInterface.Applications import OverlayInput
    energy = float(task.meta['Energy'])
    overlay = OverlayInput()
    overlay.setEnergy(energy)
    overlay.setBackgroundType(self.overlayEventType)
    overlay.setDetectorModel(self.detectorModel)
    try:
      overlayEnergy = energyToInt( self.overlayEvents ) if self.overlayEvents else energy
      self.setOverlayParameters(overlayEnergy, self._machine, overlay)
    except KeyError:
      raise RuntimeError("No overlay parameters defined for %r GeV and %s " % (energy, self.overlayEventType))

    if self.overlayEvents:
      overlay.setUseEnergyForFileLookup( False )

    self._setApplicationOptions('Overlay', overlay, task.applicationOptions)

    return overlay

  def createMarlinApplication(self, task, over):
    """Create Marlin application with or without overlay."""
    from ILCDIRAC.Interfaces.API.NewInterface.Applications import Marlin
    marlin = Marlin()
    marlin.setDebug()
    marlin.setVersion( self.softwareVersion )
    marlin.setDetectorModel( self.detectorModel )
    marlin.detectortype = self.detectorModel
    marlin.setKeepRecFile(False)

    if over:
      energy = float(task.meta['Energy'])
      self.addOverlayOptionsToMarlin( energy )

    self.cliReco = ' '.join([self.cliRecoOption, self.cliReco, task.cliReco]).strip()
    marlin.setExtraCLIArguments(self.cliReco)
    self.cliReco = ''

    marlin.setSteeringFile(self.marlinSteeringFile)

    self._setApplicationOptions('Marlin', marlin, task.applicationOptions)

    return marlin

  def createGenerationProduction(self, task):
    """Create generation production."""
    prodName = task.getProdName(self._machine, 'gen', self.additionalName)
    parameterDict = task.parameterDict
    nbTasks = task.nbTasks
    gLogger.notice("*" * 80 + "\nCreating generation production: %s " % prodName)
    genProd = self.getProductionJob()
    genProd.setProdType('MCGeneration')
    genProd.setWorkflowName(prodName)
    # Add the application
    print('Task', task)
    res = genProd.append(self.createWhizard2Application(task))
    if not res['OK']:
      raise RuntimeError("Error creating generation production: %s" % res['Message'])
    genProd.addFinalization(True, True, True, True)
    if not prodName:
      raise RuntimeError("Error creating generation production: prodName empty")
    genProd.setDescription(prodName)
    res = genProd.createProduction()
    if not res['OK']:
      raise RuntimeError("Error creating generation production: %s" % res['Message'])

    genProd.addMetadataToFinalFiles({'BeamParticle1': parameterDict['pname1'],
                                     'BeamParticle2': parameterDict['pname2'],
                                     'EPA_B1': parameterDict['epa_b1'],
                                     'EPA_B2': parameterDict['epa_b2'],
                                    }
                                   )

    res = genProd.finalizeProd()
    if not res['OK']:
      raise RuntimeError("Error finalizing generation production: %s" % res['Message'])

    genProd.setNbOfTasks(nbTasks)
    generationMeta = genProd.getMetadata()
    return generationMeta

  def createSimulationProduction(self, task):
    """Create simulation production."""
    meta = task.meta
    prodName = task.getProdName('sim', self.detectorModel, self.additionalName)
    parameterDict = task.parameterDict
    gLogger.notice( "*"*80 + "\nCreating simulation production: %s " % prodName )
    simProd = self.getProductionJob()
    simProd.setProdType( 'MCSimulation' )
    simProd.setConfigPackage(appName=self.configPackage, version=self.configVersion)
    res = simProd.setInputDataQuery( meta )
    if not res['OK']:
      raise RuntimeError( "Error creating Simulation Production: %s" % res['Message'] )
    simProd.setWorkflowName(prodName)
    #Add the application
    res = simProd.append(self.createDDSimApplication(task))
    if not res['OK']:
      raise RuntimeError( "Error creating simulation Production: %s" % res[ 'Message' ] )
    simProd.addFinalization(True,True,True,True)
    description = "Model: %s" % self.detectorModel
    if prodName:
      description += ", %s"%prodName
    simProd.setDescription( description )
    res = simProd.createProduction()
    if not res['OK']:
      raise RuntimeError( "Error creating simulation production: %s" % res['Message'] )

    simProd.addMetadataToFinalFiles( { 'BeamParticle1': parameterDict['pname1'],
                                       'BeamParticle2': parameterDict['pname2'],
                                       'EPA_B1': parameterDict['epa_b1'],
                                       'EPA_B2': parameterDict['epa_b2'],
                                     }
                                   )

    res = simProd.finalizeProd()
    if not res['OK']:
      raise RuntimeError( "Error finalizing simulation production: %s" % res[ 'Message' ] )

    simulationMeta = simProd.getMetadata()
    return simulationMeta

  def createReconstructionProduction(self, task, over):
    """Create reconstruction production."""
    meta = task.meta
    recType = 'rec_overlay' if over else 'rec'
    prodName = task.getProdName(recType, self.detectorModel, self.additionalName)
    if over:
      prodName = prodName.replace('overlay', 'overlay%s' % self.overlayEvents if self.overlayEvents else meta['Energy'])
    parameterDict = task.parameterDict
    gLogger.notice("*" * 80 + "\nCreating %s reconstruction production: %s " % ('overlay' if over else '', prodName))
    recProd = self.getProductionJob()
    productionType = 'MCReconstruction_Overlay' if over else 'MCReconstruction'
    recProd.setProdType( productionType )
    recProd.setConfigPackage(appName=self.configPackage, version=self.configVersion)

    res = recProd.setInputDataQuery( meta )
    if not res['OK']:
      raise RuntimeError( "Error setting inputDataQuery for Reconstruction production: %s " % res['Message'] )

    recProd.setWorkflowName(prodName)

    #Add overlay if needed
    if over:
      res = recProd.append(self.createOverlayApplication(task))
      if not res['OK']:
        raise RuntimeError( "Error appending overlay to reconstruction transformation: %s" % res['Message'] )

    #Add reconstruction
    res = recProd.append(self.createMarlinApplication(task, over))
    if not res['OK']:
      raise RuntimeError( "Error appending Marlin to reconstruction production: %s" % res['Message'] )
    recProd.addFinalization(True,True,True,True)

    description = "CLICDet2017 %s" % meta['Energy']
    description += "Overlay" if over else "No Overlay"
    if prodName:
      description += ", %s"%prodName
    recProd.setDescription( description )

    res = recProd.createProduction()
    if not res['OK']:
      raise RuntimeError( "Error creating reconstruction production: %s" % res['Message'] )

    recProd.addMetadataToFinalFiles( { 'BeamParticle1': parameterDict['pname1'],
                                       'BeamParticle2': parameterDict['pname2'],
                                       'EPA_B1': parameterDict['epa_b1'],
                                       'EPA_B2': parameterDict['epa_b2'],
                                     }
                                   )

    res = recProd.finalizeProd()
    if not res['OK']:
      raise RuntimeError( "Error finalising reconstruction production: %s " % res['Message'] )

    reconstructionMeta = recProd.getMetadata()
    return reconstructionMeta

  def createSplitProduction(self, task, limited=False):
    """Create splitting transformation for splitting files."""
    meta = task.meta
    prodName = task.getProdName('split', task.meta['ProdID'], self.additionalName)
    parameterDict = task.parameterDict
    eventsPerJob = task.eventsPerJob
    eventsPerBaseFile = task.eventsPerBaseFile

    gLogger.notice( "*"*80 + "\nCreating split production: %s " % prodName )
    splitProd = self.getProductionJob()
    splitProd.setProdPlugin( 'Limited' if limited else 'Standard' )
    splitProd.setProdType( 'Split' )

    res = splitProd.setInputDataQuery(meta)
    if not res['OK']:
      raise RuntimeError( 'Split production: failed to set inputDataQuery: %s' % res['Message'] )
    splitProd.setWorkflowName(prodName)

    #Add the application
    res = splitProd.append( self.createSplitApplication( eventsPerJob, eventsPerBaseFile, 'stdhep' ) )
    if not res['OK']:
      raise RuntimeError( 'Split production: failed to append application: %s' % res['Message'] )
    splitProd.addFinalization(True,True,True,True)
    description = 'Splitting stdhep files'

    splitProd.setDescription( description )

    res = splitProd.createProduction()
    if not res['OK']:
      raise RuntimeError( "Failed to create split production: %s " % res['Message'] )

    splitProd.addMetadataToFinalFiles( { "BeamParticle1": parameterDict['pname1'],
                                         "BeamParticle2": parameterDict['pname2'],
                                         "EPA_B1": parameterDict['epa_b1'],
                                         "EPA_B2": parameterDict['epa_b2'],
                                       }
                                     )

    res = splitProd.finalizeProd()
    if not res['OK']:
      raise RuntimeError( 'Split production: failed to finalize: %s' % res['Message'] )

    return splitProd.getMetadata()


  def createMovingTransformation( self, meta, prodType ):
    """ create moving transformations for output files """

    sourceSE = self.outputSE
    targetSE = self.finalOutputSE
    prodID = meta['ProdID']
    try:
      dataTypes = { 'MCReconstruction': ('DST', 'REC'),
                    'MCReconstruction_Overlay': ('DST', 'REC'),
                    'MCSimulation': ('SIM',),
                    'MCGeneration': ('GEN',),
                  }[prodType]
    except KeyError:
      raise RuntimeError( "ERROR creating MovingTransformation" + repr(prodType) + "unknown" )

    if not any( getattr( self._flags, "move%s" % dataType.capitalize() ) for dataType in dataTypes ):
      gLogger.notice( "*"*80 + "\nNot creating moving transformation for prodID: %s, %s " % (meta['ProdID'], prodType ) )
      return

    from DIRAC.TransformationSystem.Utilities.ReplicationTransformation import createDataTransformation
    from DIRAC.TransformationSystem.Client.Transformation import Transformation
    for dataType in dataTypes:
      if getattr(self._flags, "move%s" % dataType.capitalize()):
        gLogger.notice("*" * 80 + "\nCreating moving transformation for prodID: %s, %s, %s " %
                       (meta['ProdID'], prodType, dataType))
        parDict = dict(flavour='Moving',
                       targetSE=targetSE,
                       sourceSE=sourceSE,
                       plugin='Broadcast%s' % ('' if dataType.lower() not in ('gen', 'sim') else 'Processed'),
                       metaKey='ProdID',
                       metaValue=prodID,
                       extraData={'Datatype': dataType},
                       tGroup=self.prodGroup,
                       groupSize=int(self.moveGroupSize),
                       enable=not self._flags.dryRun,
                      )
        message = "Moving transformation with parameters"
        gLogger.notice("%s:\n%s" % (message, pformat(parDict, indent=len(message) + 2, width=120)))
        res = createDataTransformation(**parDict)
        if not res['OK']:
          gLogger.error("Failed to create moving transformation:", res['Message'])

        elif isinstance(res['Value'], Transformation):
          newTrans = res['Value']
          newTrans.setStatus(self.moveStatus)

  def getProductionJob(self):
    """ return production job instance with some parameters set """
    from ILCDIRAC.Interfaces.API.NewInterface.ProductionJob import ProductionJob
    prodJob = ProductionJob()
    prodJob.setLogLevel(self.productionLogLevel)
    prodJob.setProdGroup(self.prodGroup)
    prodJob.setOutputSE(self.outputSE)
    prodJob.basepath = self.basepath
    prodJob.dryrun = self._flags.dryRun
    prodJob.maxFCFoldersToCheck = 1
    return prodJob

  def _setApplicationOptions(self, appName, app, optionsDict=None):
    """ set options for given application

    :param str appName: name of the application, for print out
    :param app: application instance
    """
    if optionsDict is None:
      optionsDict = {}
    allOptions = dict(self.applicationOptions[appName])
    allOptions.update(optionsDict)
    for option, value in allOptions.items():
      if option.startswith(('FE.', 'C_', 'additionalName')):
        continue
      gLogger.notice("%s: setting option %s to %s" % (appName, option, value))
      setterFunc = 'set' + option
      if not hasattr(app, setterFunc):
        raise AttributeError("Cannot set %s for %s, check spelling!" % (option, appName))
      if value.lower() in ('false', 'true'):
        value = value.lower() == 'true'
      getattr(app, setterFunc)(value)

  def createTransformations(self, taskDict):
    """Create all the transformations we want to create."""
    for pType, createProduction in [('GEN', self.createGenerationProduction),
                                    ('SPLIT', self.createSplitProduction)]:
      for task in taskDict.get(pType, []):
        meta = createProduction(task)
        self.addSimTask(taskDict, meta, originalTask=task)
        taskDict['MOVE_' + pType].append(dict(meta))

    for task in taskDict.get('SIM', []):
      if not self._flags.sim:
        continue
      gLogger.notice("Creating task %s" % task)
      simMeta = self.createSimulationProduction(task)
      self.addRecTask(taskDict, simMeta, originalTask=task)
      taskDict['MOVE_SIM'].append(dict(simMeta))

    for task in taskDict.get('REC', []):
      for name, over, enabled in [('REC', False, self._flags.rec),
                                  ('OVER', True, self._flags.over)]:
        if enabled:
          recMeta = self.createReconstructionProduction(task, over=over)
          taskDict['MOVE_' + name].append(dict(recMeta))

    for name, pType in [('GEN', 'MCGeneration'),
                        ('SPLIT', 'MCGeneration'),
                        ('SIM', 'MCSimulation'),
                        ('REC', 'MCReconstruction'),
                        ('OVER', 'MCReconstruction_Overlay')]:
      for meta in taskDict.get('MOVE_' + name, []):
        self.createMovingTransformation(meta, pType)

  def createTaskDict(self, prodID, process, energy, eventsPerJob, sinFile, nbTasks,
                     eventsPerBaseFile):
    """Create a dictionary of tasks for the first level of transformations."""
    taskDict = defaultdict(list)
    metaInput = self.meta(prodID, process, energy)
    prodName = metaInput['EvtType']

    for parameterDict in self.getParameterDictionary(prodName):
      if self._flags.gen:
        self.addGenTask(taskDict, Task(metaInput, parameterDict, eventsPerJob, nbTasks=nbTasks, sinFile=sinFile))

      elif self._flags.spl and eventsPerBaseFile == eventsPerJob:
        gLogger.notice("*" * 80 + "\nSkipping split transformation for %s\n" % prodName + "*" * 80)
        if self._flags.sim:
          self.addSimTask(taskDict, metaInput, Task({}, parameterDict, eventsPerJob))
      elif self._flags.spl:
        taskDict['SPLIT'].append(Task(metaInput, parameterDict, eventsPerJob,
                                      eventsPerBaseFile=eventsPerBaseFile))
      elif self._flags.sim:
        self.addSimTask(taskDict, metaInput, Task({}, parameterDict, eventsPerJob))
      elif self._flags.rec or self._flags.over:
        self.addRecTask(taskDict, metaInput, Task({}, parameterDict, eventsPerJob))

    return taskDict

  def _addTask(self, taskDict, metaInput, originalTask, prodType, applicationName):
    """Add a task to the given prodType and applicatioName."""
    options = defaultdict(list)
    nTasks = 0
    for option, value in self.applicationOptions[applicationName].items():
      if option.startswith('FE.'):
        optionName = option.split('.', 1)[1]
        options[optionName] = listify(value)
        gLogger.notice("Found option %s with values %s" % (optionName, pformat(options[optionName])))
        nTasks = len(options[optionName])

    theTask = Task(metaInput,
                   parameterDict=originalTask.parameterDict,
                   eventsPerJob=originalTask.eventsPerJob,
                   metaPrev=originalTask.meta,
                   dryRun=self._flags.dryRun,
                   sinFile=originalTask.sinFile,
                   nbTasks=originalTask.nbTasks,
                   )
    theTask.sourceName = '_'.join([originalTask.sourceName, originalTask.taskName])
    if not nTasks:
      taskDict[prodType].append(theTask)
      return

    taskList = [deepcopy(theTask) for _ in xrange(nTasks)]
    taskDict[prodType].extend(taskList)
    self.addTaskOptions(options, taskList)
    return

  def addGenTask(self, taskDict, originalTask):
    """Add a gen task with required options."""
    return self._addTask(taskDict, metaInput=originalTask.meta,
                         originalTask=originalTask, prodType='GEN', applicationName='Whizard2')

  def addRecTask(self, taskDict, metaInput, originalTask):
    """Add a reconstruction task."""
    return self._addTask(taskDict, metaInput, originalTask, prodType='REC', applicationName='Marlin')

  def addSimTask(self, taskDict, metaInput, originalTask):
    """Add a sim task."""
    return self._addTask(taskDict, metaInput, originalTask, prodType='SIM', applicationName='DDSim')

  @staticmethod
  def addTaskOptions(options, taskList):
    """Add the options to each task in the taskList."""
    for optionName, values in options.items():
      if optionName.startswith('Query'):
        queryParameter = optionName[len('Query'):]
        for index, value in enumerate(values):
          taskList[index].meta[queryParameter] = value
      elif optionName == 'additionalName':
        for index, value in enumerate(values):
          taskList[index].taskName = value
      # cliReco only makes sense for REC application, but it is otherwise ignored
      elif optionName == 'cliReco':
        for index, value in enumerate(values):
          taskList[index].cliReco = value
      else:
        for index, value in enumerate(values):
          taskList[index].applicationOptions[optionName] = value

  def createAllTransformations(self):
    """Loop over the list of processes, energies and possibly prodIDs to create all the productions."""
    for energy, process, prodID, eventsPerJob, eventsPerBaseFile, sinFile, nbTasks in \
        izip_longest(self.energies, self.processes, self.prodIDs, self.eventsPerJobs, self.eventsInSplitFiles,
                     self.whizard2SinFile, self.numberOfTasks, fillvalue=None):
      taskDict = self.createTaskDict(prodID, process, energy, eventsPerJob, sinFile, nbTasks, eventsPerBaseFile)
      self.createTransformations(taskDict)


if __name__ == "__main__":
  CLIP = Params()
  CLIP.registerSwitches()
  Script.parseCommandLine()
  from ILCDIRAC.Core.Utilities.CheckAndGetProdProxy import checkOrGetGroupProxy
  CHECKGROUP = checkOrGetGroupProxy(['ilc_prod', 'fcc_prod'])
  if not CHECKGROUP['OK']:
    exit(1)
  try:
    CHAIN = CLICDetProdChain(params=CLIP, group=CHECKGROUP['Value'])
    CHAIN.createAllTransformations()
  except (AttributeError, RuntimeError) as excp:
    if str(excp) != '':
      gLogger.exception('Failure to create transformations', lException=excp)
