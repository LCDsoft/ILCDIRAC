'''
Create productions for the DDSim/Marlin software chain

Options:

   -p, --printConfigFile      Create the template to create productions
   -f, --configFile <file>    Defines the file with the parameters to create a production
   -x, --enable               Disable dry-run mode and actually create the production
   --additionalName       Define a string to add to the production name if the original name already exists


:since: July 14, 2017
:author: A Sailer
'''

#pylint disable=wrong-import-position

import ConfigParser

from DIRAC.Core.Base import Script
from DIRAC import S_OK, gLogger

PRODUCTION_PARAMETERS= 'Production Parameters'
PP= 'Production Parameters'

def energyWithUnit( energy ):
  """ return energy with unit, GeV below 1000, TeV above """
  energyString = ''
  if energy < 1000.:
    energyString = "%dGeV" % int( energy )
  elif float( energy/1000. ).is_integer():
    energyString = "%dTeV" % int( energy/1000.0 )
  else:
    energyString = "%1.1fTeV" % float( energy/1000.0 )

  return energyString

class Params(object):
  """Parameter Object"""
  def __init__(self):
    self.prodConfigFilename = None
    self.dumpConfigFile = False
    self.dryRun = True
    self.additionalName = None

  def setProdConf(self,fileName):
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
  """ create applications and productions for clic physics studies 2017


  :param str prodGroup: basename of the production group the productions are part of
  :param str process: name of the process to generate or use in meta data search
  :param str detectorModel: Detector Model to use in simulation/reconstruction
  :param str softwareVersion: softwareVersion to use for generation/simulation/reconstruction
  :param str clicConfig: Steering file version to use for simulation/reconstruction
  :param float energy: energy to use for generation or meta data search
  :param in eventsPerJob: number of events per job
  :param str productionLogLevel: log level to use in production jobs
  :param str outputSE: output SE for production jobs
  :param str finalOutputSE: final destination for files when moving transformations are enabled
  :param str additionalName: additionalName to add to the transformation name in case a
        transformation with that name already exists
  """

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
      self._prodTypes = [ ('gen','Gen'), ('spl','Split'), ('sim','Sim'), ('rec','Rec'), ('over','RecOver') ]
      self._moveTypes = [ ('moveGen','Gen'), ('moveSim','Sim'), ('moveRec','Rec'), ('moveDst','Dst') ]

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
      return self._rec and not self._over
    @property
    def over( self ): #pylint: disable=missing-docstring
      return self._over
    @property
    def move( self ): #pylint: disable=missing-docstring
      return not self._dryRun and self._moves
    @property
    def moveGen( self ): #pylint: disable=missing-docstring
      return not self._dryRun and (self._gen or self._spl) and self._moves and self._moveGen
    @property
    def moveSim( self ): #pylint: disable=missing-docstring
      return not self._dryRun and self._sim and self._moves and self._moveSim
    @property
    def moveRec( self ): #pylint: disable=missing-docstring
      return not self._dryRun and (self._rec or self._over) and self._moves and self._moveRec
    @property
    def moveDst( self ): #pylint: disable=missing-docstring
      return not self._dryRun and (self._rec or self._over) and self._moves and self._moveDst


    def __str__( self ):
      pDict = vars(self)
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
      return """

#Productions to create: %(prodOpts)s
ProdTypes = %(prodTypes)s

move = %(_moves)s

#Datatypes to move: %(moveOpts)s
MoveTypes = %(moveTypes)s
""" %( vars(self) )


    def __splitStringToOptions( self, config, tuples, optString, prefix='_'):
      """ split the option string into separate values and set the corresponding flag """
      prodsToCreate = config.get( PRODUCTION_PARAMETERS, optString )
      for prodType in prodsToCreate.split(','):
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


  def __init__( self, params=None):

    from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
    self._ops = Operations( vo='ilc' )

    self._machine = 'clic'
    self.prodGroup = 'several'
    self.detectorModel = self._ops.getValue( 'Production/CLIC/DefaultDetectorModel' )
    self.softwareVersion = self._ops.getValue( 'Production/CLIC/DefaultSoftwareVersion' )
    self.clicConfig = self._ops.getValue( 'Production/CLIC/DefaultConfigVersion' )
    self.productionLogLevel = 'VERBOSE'
    self.outputSE = 'CERN-DST-EOS'

    self.eventsPerJobs = ''
    self.energies = ''
    self.processes = ''
    self.prodIDs = ''
    self.eventsPerBaseFiles = ''

    # final destination for files once they have been used
    self.finalOutputSE = self._ops.getValue( 'Production/CLIC/FailOverSE' )

    self.additionalName = params.additionalName

    self._flags = self.Flags()

    self.loadParameters( params )

    self._flags._dryRun = params.dryRun #pylint: disable=protected-access




  def meta( self, prodID, process, energy ):
    """ return meta data dictionary, always new"""
    return { 'ProdID': prodID,
             'EvtType': process,
             'Energy' : self.metaEnergy( energy ),
             'Machine': self._machine,
           }


  def loadParameters( self, parameter ):
    """ load parameters from config file """

    if parameter.prodConfigFilename is not None:
      config = ConfigParser.SafeConfigParser( defaults=vars(self), dict_type=dict )
      config.read( parameter.prodConfigFilename )
      self._flags.loadFlags( config )

      self.prodGroup = config.get(PP, 'prodGroup')
      self.detectorModel = config.get(PP, 'detectorModel')
      self.softwareVersion = config.get(PP, 'softwareVersion')
      self.clicConfig = config.get(PP, 'clicConfig')

      self.processes = config.get(PP, 'processes').split(',')
      self.energies = config.get(PP, 'energies').split(',')
      self.eventsPerJobs = config.get(PP, 'eventsPerJobs').split(',')

      self.productionLogLevel = config.get(PP, 'productionloglevel')
      self.outputSE = config.get(PP, 'outputSE')

      # final destination for files once they have been used
      self.finalOutputSE = config.get(PP, 'finalOutputSE')

      if config.has_option(PP, 'additionalName'):
        self.additionalName = config.get(PP, 'additionalName')

      if config.has_option(PP, 'prodIDs'):
        self.prodIDs = config.get(PP, 'prodIDs').split(',')
      else:
        self.prodIDs = []

      ##for split only
      self.eventsPerBaseFiles = config.get(PP, 'NumberOfEventsInBaseFiles').split(',')

      self.processes = [ process.strip() for process in self.processes if process.strip() ]
      self.energies = [ float(eng.strip()) for eng in self.energies if eng.strip() ]
      self.eventsPerJobs = [ int( epj.strip() ) for epj in self.eventsPerJobs if epj.strip() ]
      ## these do not have to exist so we fill them to the same length if they are not set
      self.prodIDs = [ int( pID.strip() ) for pID in self.prodIDs if pID.strip() ]
      self.prodIDs = self.prodIDs if self.prodIDs else [ 1 for _ in self.energies ]

      if len(self.processes) != len(self.energies) or \
         len(self.energies) != len(self.eventsPerJobs) or \
         len( self.prodIDs) != len(self.eventsPerJobs):
        raise AttributeError( "Lengths of Processes, Energies, and EventsPerJobs do not match" )

      self.eventsPerBaseFiles = [ int( epb.strip() ) for epb in self.eventsPerBaseFiles if epb.strip() ]
      self.eventsPerBaseFiles = self.eventsPerBaseFiles if self.eventsPerBaseFiles else [ -1 for _ in self.energies ]

      if self._flags.spl and len(self.eventsPerBaseFiles) != len(self.energies):
        raise AttributeError( "Length of eventsPerBaseFiles does not match: %d vs %d" %(
          len(self.eventsPerBaseFiles), \
          len(self.energies) ) )

    if parameter.dumpConfigFile:
      print self
      raise RuntimeError('')

  def _productionName( self, prodName, metaDict, parameterDict, prodType ):
    """ create the production name """
    workflowName = "%s_%s_clic_%s_%s" %( parameterDict['process'],
                                         metaDict['Energy'],
                                         prodType,
                                         prodName )
    if isinstance( self.additionalName, basestring):
      workflowName += "_" + self.additionalName
    return workflowName

  def __str__( self ):
    pDict = vars(self)
    pDict.update({'ProductionParameters':PRODUCTION_PARAMETERS})
    return """
[%(ProductionParameters)s]
prodGroup = %(prodGroup)s
detectorModel = %(detectorModel)s
softwareVersion = %(softwareVersion)s
clicConfig = %(clicConfig)s
eventsPerJobs = %(eventsPerJobs)s

energies = %(energies)s
processes = %(processes)s
## optional prodid to search for input files
# prodIDs =

## number of events for input files to split productions
NumberOfEventsInBaseFiles = %(eventsPerBaseFiles)s

productionLogLevel = %(productionLogLevel)s
outputSE = %(outputSE)s

finalOutputSE = %(finalOutputSE)s

## optional additional name
# additionalName = %(additionalName)s

%(_flags)s


""" %( pDict )

  @staticmethod
  def metaEnergy( energy ):
    """ return string of the energy with no digits """
    return str( int( energy ) )


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
  def overlayParameterDict():
    """ return dictionary that sets the parameters for the overlay application

    keys are floats
    values are lambda functions acting on an overlay application object
    """
    return {
      350. : ( lambda overlay: [ overlay.setBXOverlay( 30 ), overlay.setGGToHadInt( 0.0464 ), overlay.setProcessorName( 'Overlay350GeV') ] ),
      380. : ( lambda overlay: [ overlay.setBXOverlay( 30 ), overlay.setGGToHadInt( 0.0464 ), overlay.setProcessorName( 'Overlay380GeV') ] ),
      420. : ( lambda overlay: [ overlay.setBXOverlay( 30 ), overlay.setGGToHadInt( 0.17 ),   overlay.setProcessorName( 'Overlay420GeV') ] ),
      500. : ( lambda overlay: [ overlay.setBXOverlay( 30 ), overlay.setGGToHadInt( 0.3 ),    overlay.setProcessorName( 'Overlay500GeV') ] ),
      1400.: ( lambda overlay: [ overlay.setBXOverlay( 30 ), overlay.setGGToHadInt( 1.3 ),    overlay.setProcessorName( 'Overlay1.4TeV') ] ),
      3000.: ( lambda overlay: [ overlay.setBXOverlay( 30 ), overlay.setGGToHadInt( 3.2 ),    overlay.setProcessorName( 'Overlay3TeV') ] ),
    }

  @staticmethod
  def addOverlayOptionsToMarlin( marlin, energy ):
    """ add options to marlin that are needed for running with overlay """
    energyString = energyWithUnit( energy )
    cliOptions = ' --Config.Overlay=%s ' % energyString
    marlin.setExtraCLIArguments( cliOptions )

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

  def createDDSimApplication( self ):
    """ create DDSim Application """
    from ILCDIRAC.Interfaces.API.NewInterface.Applications import DDSim

    ddsim = DDSim()
    ddsim.setVersion( self.softwareVersion )
    ddsim.setSteeringFile( 'clic_steer.py' )
    ddsim.setDetectorModel( self.detectorModel )
    return ddsim

  def createOverlayApplication( self, energy ):
    """ create Overlay Application """
    from ILCDIRAC.Interfaces.API.NewInterface.Applications import OverlayInput
    overlay = OverlayInput()
    overlay.setMachine( 'clic_opt' )
    overlay.setEnergy( energy )
    overlay.setBkgEvtType( 'gghad' )
    overlay.setDetectorModel( self.detectorModel )
    try:
      self.overlayParameterDict().get( energy ) ( overlay )
    except TypeError:
      raise RuntimeError( "No overlay parameters defined for %s GeV" % energy )

    return overlay



  def createMarlinApplication( self, energy ):
    """ create Marlin Application without overlay """
    from ILCDIRAC.Interfaces.API.NewInterface.Applications import Marlin
    marlin = Marlin()
    marlin.setDebug()
    marlin.setVersion( self.softwareVersion )
    marlin.setDetectorModel( self.detectorModel )
    marlin.detectortype = self.detectorModel

    if self._flags.over:
      self.addOverlayOptionsToMarlin( marlin, energy )

    steeringFile = {
      350. : "clicReconstruction.xml",
      380. : "clicReconstruction.xml",
      420. : "clicReconstruction.xml",
      1400.: "clicReconstruction.xml",
      3000.: "clicReconstruction.xml",
    }.get( energy, 'clicReconstruction.xml' )

    marlin.setSteeringFile( steeringFile )
    return marlin


  def createSimulationProduction( self, meta, prodName, parameterDict ):
    """ create simulation production """
    gLogger.notice( "*"*80 + "\nCreating simulation production: %s " % prodName )
    from ILCDIRAC.Interfaces.API.NewInterface.ProductionJob import ProductionJob
    simProd = ProductionJob()
    simProd.dryrun = self._flags.dryRun
    simProd.setLogLevel( self.productionLogLevel )
    simProd.setProdType( 'MCSimulation' )
    simProd.setClicConfig( self.clicConfig )
    res = simProd.setInputDataQuery( meta )
    if not res['OK']:
      raise RuntimeError( "Error creating Simulation Production: %s" % res['Message'] )
    simProd.setOutputSE( self.outputSE )
    simProd.setWorkflowName( self._productionName( prodName, meta, parameterDict, 'sim') )
    simProd.setProdGroup( self.prodGroup+"_"+self.metaEnergy( meta['Energy'] ) )
    #Add the application
    res = simProd.append( self.createDDSimApplication() )
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
    self._isFirst = False
    return simulationMeta

  def createReconstructionProduction( self, meta, prodName, parameterDict ):
    """ create reconstruction production """
    gLogger.notice( "*"*80 + "\nCreating reconstruction production: %s " % prodName )
    from ILCDIRAC.Interfaces.API.NewInterface.ProductionJob import ProductionJob
    recProd = ProductionJob()
    recProd.dryrun = self._flags.dryRun
    recProd.setLogLevel( self.productionLogLevel )
    productionType = 'MCReconstruction_Overlay' if self._flags.over else 'MCReconstruction'
    recProd.setProdType( productionType )
    recProd.setClicConfig( self.clicConfig )

    res = recProd.setInputDataQuery( meta )
    if not res['OK']:
      raise RuntimeError( "Error setting inputDataQuery for Reconstruction production: %s " % res['Message'] )

    recProd.setOutputSE( self.outputSE )
    recType = 'rec_overlay' if self._flags.over else 'rec'
    recProd.setWorkflowName( self._productionName( prodName, meta, parameterDict, recType ) )
    recProd.setProdGroup( "%s_%s" %( self.prodGroup, self.metaEnergy( meta['Energy'] ) ) )

    #Add overlay if needed
    if self._flags.over:
      res = recProd.append( self.createOverlayApplication( float( meta['Energy'] ) ) )
      if not res['OK']:
        raise RuntimeError( "Error appending overlay to reconstruction transformation: %s" % res['Message'] )

    #Add reconstruction
    res = recProd.append( self.createMarlinApplication( float( meta['Energy'] ) ) )
    if not res['OK']:
      raise RuntimeError( "Error appending Marlin to reconstruction production: %s" % res['Message'] )
    recProd.addFinalization(True,True,True,True)

    description = "CLICDet2017 %s" % meta['Energy']
    description += "Overlay" if self._flags.over else "No Overlay"
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

  def createSplitProduction( self, meta, prodName, parameterDict, eventsPerJob, eventsPerBaseFile, limited=False ):
    """ create splitting transformation for splitting files """
    gLogger.notice( "*"*80 + "\nCreating split production: %s " % prodName )
    from ILCDIRAC.Interfaces.API.NewInterface.ProductionJob import ProductionJob
    splitProd = ProductionJob()
    splitProd.setProdPlugin( 'Limited' if limited else 'Standard' )
    splitProd.setProdType( 'Split' )
    splitProd.setLogLevel( self.productionLogLevel )
    splitProd.dryrun = self._flags.dryRun

    res = splitProd.setInputDataQuery(meta)
    if not res['OK']:
      raise RuntimeError( 'Split production: failed to set inputDataQuery: %s' % res['Message'] )
    splitProd.setOutputSE( self.outputSE )
    splitProd.setWorkflowName( self._productionName( prodName, meta, parameterDict, 'stdhepSplit' ) )
    splitProd.setProdGroup( self.prodGroup+"_"+self.metaEnergy( meta['Energy'] ) )

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
      dataType = { 'MCReconstruction': 'REC',
                   'MCReconstruction_Overlay': 'REC',
                   'MCSimulation': 'SIM',
                   'MCGeneration': 'GEN',
                 }[prodType]
    except KeyError:
      raise RuntimeError( "ERROR creating MovingTransformation" + repr(prodType) + "unknown" )

    if not getattr( self._flags, "move%s" % dataType.capitalize() ):
      gLogger.notice( "*"*80 + "\nNot creating moving transformation for prodID: %s, %s " % (meta['ProdID'], prodType ) )
      return

    gLogger.notice( "*"*80 + "\nCreating moving transformation for prodID: %s, %s " % (meta['ProdID'], prodType ) )

    from ILCDIRAC.ILCTransformationSystem.Utilities.MovingTransformation import createMovingTransformation
    createMovingTransformation( targetSE, sourceSE, prodID, dataType )


  def _updateMeta( self, outputDict, inputDict, eventsPerJob ):
    """ add some values from the inputDict to the outputDict to fake the input dataquery result in dryRun mode """
    if not self._flags.dryRun:
      outputDict.clear()
      outputDict.update( inputDict )
      return

    for key, value in inputDict.iteritems():
      if key not in outputDict:
        outputDict[ key ] = value
    outputDict['NumberOfEvents'] = eventsPerJob


  def createTransformations( self, prodID, process, energy, eventsPerJob, eventsPerBaseFile):
    """ create all the transformations we want to create """

    metaInput = self.meta( prodID, process, energy )
    prodName = process

    for parameterDict in self.getParameterDictionary( prodName ):
      splitMeta, simMeta, recMeta = None, None, None

      if self._flags.spl:
        splitMeta = self.createSplitProduction( metaInput, prodName, parameterDict, eventsPerJob,
                                                eventsPerBaseFile, limited=False )
        self._updateMeta( metaInput, splitMeta, eventsPerJob )

      if self._flags.sim:
        simMeta = self.createSimulationProduction( metaInput, prodName, parameterDict )
        self._updateMeta( metaInput, simMeta, eventsPerJob )

      if self._flags.rec or self._flags.over:
        recMeta = self.createReconstructionProduction( metaInput, prodName, parameterDict )

      if splitMeta:
        self.createMovingTransformation( splitMeta, 'MCGeneration' )

      if simMeta:
        self.createMovingTransformation( simMeta, 'MCSimulation' )

      if recMeta:
        self.createMovingTransformation( recMeta, 'MCReconstruction' )


  def createAllTransformations( self ):
    """ loop over the list of processes, energies and possibly prodIDs to create all the productions """

    for index, energy in enumerate( self.energies ):

      process = self.processes[index]
      prodID = self.prodIDs[index]
      eventsPerJob = self.eventsPerJobs[index]
      eventsPerBaseFile = self.eventsPerBaseFiles[index]

      self.createTransformations( prodID, process, energy, eventsPerJob, eventsPerBaseFile )





if __name__ == "__main__":
  CLIP = Params()
  CLIP.registerSwitches()
  Script.parseCommandLine()
  from ILCDIRAC.Core.Utilities.CheckAndGetProdProxy import checkOrGetGroupProxy
  CHECKGROUP = checkOrGetGroupProxy( 'ilc_prod' )
  if not CHECKGROUP['OK']:
    exit(1)
  try:
    CHAIN = CLICDetProdChain( CLIP )
    CHAIN.createAllTransformations()
  except (AttributeError, RuntimeError) as excp:
    if str(excp) != '':
      print "Failure to create transformations", repr(excp)
