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
from DIRAC import S_OK, exit as dexit

PRODUCTION_PARAMETERS= 'Production Parameters'
PP= 'Production Parameters'
FLAGS = 'Flags'
MOVING_FLAGS = 'Moving Flags'

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
  :param str process: name of the process to generate or use in _meta data search
  :param str detectorModel: Detector Model to use in simulation/reconstruction
  :param str softwareVersion: softwareVersion to use for generation/simulation/reconstruction
  :param str clicConfig: Steering file version to use for simulation/reconstruction
  :param float energy: energy to use for generation or _meta data search
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
    :param bool sim: if True create simulation production
    :param bool rec: if True create reconstruction production
    :param bool over: if True create reconstruction production with overlay, if `rec` is False this flag is also False
    :param bool move: if True create moving transformations, the other move flags only take effect if this one is True
    :param bool moveGen: if True move GEN files after they have been used in the production
    :param bool moveSim: if True move SIM files after they have been used in the production
    :param bool moveRev: if True move REC files when they were created
    :param bool moveDst: if True move DST files when they were created
    """

    def __init__( self ):
      # general flag to create anything at all
      self._dryRun = True

      #create transformations
      self._gen = False
      self._sim = True
      self._rec = False
      self._over = False

      # create moving transformations
      self._moves = False
      self._cleanGen = True
      self._cleanSim = True
      self._cleanRec = True
      self._cleanDst = False

    @property
    def dryRun( self ): #pylint: disable=missing-docstring
      return self._dryRun
    @property
    def gen( self ): #pylint: disable=missing-docstring
      return not self._dryRun and self._gen
    @property
    def sim( self ): #pylint: disable=missing-docstring
      return not self._dryRun and self._sim
    @property
    def rec( self ): #pylint: disable=missing-docstring
      return not self._dryRun and self._rec
    @property
    def over( self ): #pylint: disable=missing-docstring
      return not self._dryRun and self._rec and self._over
    @property
    def move( self ): #pylint: disable=missing-docstring
      return not self._dryRun and self._moves
    @property
    def moveGen( self ): #pylint: disable=missing-docstring
      return not self._dryRun and self._gen and self._moves and self._cleanGen
    @property
    def moveSim( self ): #pylint: disable=missing-docstring
      return not self._dryRun and self._sim and self._moves and self._cleanSim
    @property
    def moveRec( self ): #pylint: disable=missing-docstring
      return not self._dryRun and self._rec and self._moves and self._cleanRec
    @property
    def moveDst( self ): #pylint: disable=missing-docstring
      return not self._dryRun and self._rec and self._moves and self._cleanDst

    def __str__( self ):
      return """
[Flags]

gen = %(_gen)s
sim = %(_sim)s
rec = %(_rec)s
over = %(_over)s

move = %(_moves)s

[Moving Flags]
## which files to move after they haven been used or created, only takes effect if move is True
gen=%(_cleanGen)s
sim=%(_cleanSim)s
rec=%(_cleanRec)s
dst=%(_cleanDst)s
""" %( vars(self) )

    def loadFlags( self, config ):
      """ load flags values from configfile """

      #create transformations
      self._gen = config.getboolean(FLAGS, 'gen')
      self._sim = config.getboolean(FLAGS, 'sim')
      self._rec = config.getboolean(FLAGS, 'rec')
      self._over = config.getboolean(FLAGS, 'over')

      # create moving transformations
      self._moves = config.getboolean(FLAGS, 'move')
      self._cleanGen = config.getboolean(MOVING_FLAGS, 'gen')
      self._cleanSim = config.getboolean(MOVING_FLAGS, 'sim')
      self._cleanRec = config.getboolean(MOVING_FLAGS, 'rec')
      self._cleanDst = config.getboolean(MOVING_FLAGS, 'dst')

  def __init__( self, params=None):

    self._machine = 'clic'
    self._prodID = None
    self.prodGroup = 'several'
    self.process = 'gghad'
    self.detectorModel='CLIC_o3_v11'
    self.softwareVersion = 'ILCSoft-2017-06-21_gcc62'
    self.clicConfig = 'ILCSoft-2017-06-21'
    self.energy = 3000.
    self.eventsPerJob = 100

    self.productionLogLevel = 'VERBOSE'
    self.outputSE = 'CERN-DST-EOS'

    # final destination for files once they have been used
    self.finalOutputSE = 'CERN-SRM'

    self.additionalName = None

    self._flags = self.Flags()

    self.loadParameters( params )

    self._flags._dryRun = params.dryRun

    if params.additionalName is not None:
      self.additionalName = params.additionalName

    #For meta data search
    self._meta = { 'ProdID': self.prodID,
                   'EvtType': self.process,
                   'Energy' : self.metaEnergy,
                   'Machine': self._machine,
                 }


  def loadParameters( self, parameter ):
    """ load parameters from config file """

    if parameter.prodConfigFilename is not None:
      config = ConfigParser.SafeConfigParser( defaults=vars(self), dict_type=dict)
      config.read( parameter.prodConfigFilename )
      self._flags.loadFlags( config )

      self.prodGroup = config.get(PP, 'prodGroup')
      self.process = config.get(PP, 'process')
      self.detectorModel = config.get(PP, 'detectorModel')
      self.softwareVersion = config.get(PP, 'softwareVersion')
      self.clicConfig = config.get(PP, 'clicConfig')
      self.energy = config.getfloat(PP, 'energy')
      self.eventsPerJob = config.getint(PP, 'eventsPerJob')

      self.productionLogLevel = config.get(PP, 'productionloglevel')
      self.outputSE = config.get(PP, 'outputSE')

      # final destination for files once they have been used
      self.finalOutputSE = config.get(PP, 'finalOutputSE')

      if config.has_option(PP, 'additionalName'):
        self.additionalName = config.get(PP, 'additionalName')

      if config.has_option(PP, 'prodID'):
        self._prodID = config.getint(PP, 'prodID')

    if parameter.dumpConfigFile:
      print self
      dexit(0)

  def _productionName( self, prodName, parameterDict, prodType ):
    """ create the production name """
    workflowName = "%s_%s_clic_%s_%s" %( parameterDict['process'],
                                         self.energy,
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
process = %(process)s
detectorModel = %(detectorModel)s
softwareVersion = %(softwareVersion)s
clicConfig = %(clicConfig)s
energy = %(energy)s
eventsPerJob = %(eventsPerJob)s

## optional prodid to search for input files
# prodid = ''

productionLogLevel = %(productionLogLevel)s
outputSE = %(outputSE)s

finalOutputSE = %(finalOutputSE)s

## optional additional name
# additionalName = %(additionalName)s

%(_flags)s

""" %( pDict )

  @property
  def metaEnergy( self ):
    """ return string of the energy with no digits """
    return str(int( self.energy ))
  @property
  def prodID( self ):
    """ return the prodID for meta data search, 1 by default """
    return 1 if not self._prodID else self._prodID


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

  def overlayParameterDict( self ):
    """ return dictionary that sets the parameters for the overlay application

    keys are floats
    values are lambda functions acting on an overlay application object
    """
    return {
      350. : ( lambda overlay: [ overlay.setBXOverlay( 300 ), overlay.setGGToHadInt( 0.0464 ), overlay.setDetectorModel( self.detectorModel ) ] ),
      420. : ( lambda overlay: [ overlay.setBXOverlay( 300 ), overlay.setGGToHadInt( 0.17 ),   overlay.setDetectorModel( self.detectorModel ) ] ),
      500. : ( lambda overlay: [ overlay.setBXOverlay( 300 ), overlay.setGGToHadInt( 0.3 ),    overlay.setDetectorModel( self.detectorModel ) ] ),
      1400.: ( lambda overlay: [ overlay.setBXOverlay(  60 ), overlay.setGGToHadInt( 1.3 ),    overlay.setDetectorModel( self.detectorModel ) ] ),
      3000.: ( lambda overlay: [ overlay.setBXOverlay(  60 ), overlay.setGGToHadInt( 3.2 ),    overlay.setDetectorModel( self.detectorModel ) ] ),
    }

  def createDDSimApplication( self ):
    """ create DDSim Application """
    from ILCDIRAC.Interfaces.API.NewInterface.Applications import DDSim

    ddsim = DDSim()
    ddsim.setVersion( self.softwareVersion )
    ddsim.setSteeringFile( 'clic_steer.py' )
    ddsim.setDetectorModel( self.detectorModel )
    return ddsim

  def createOverlayApplication( self ):
    """ create Overlay Application """
    from ILCDIRAC.Interfaces.API.NewInterface.Applications import OverlayInput
    overlay = OverlayInput()
    overlay.setMachine( 'clic_opt' )
    overlay.setEnergy( self.energy )
    overlay.setBkgEvtType( 'gghad' )
    try:
      self.overlayParameterDict().get( self.energy ) ( overlay )
    except KeyError:
      print "No overlay parameters defined for", self.energy
      raise RuntimeError( '1' )

    return overlay

  def createMarlinWithOverlay( self ):
    """ create Marlin Application when overlay is enabled """
    ## no difference between with and without overlay at the moment
    return self.createMarlinApplication()


  def createMarlinApplication( self ):
    """ create Marlin Application without overlay """
    from ILCDIRAC.Interfaces.API.NewInterface.Applications import Marlin
    marlin = Marlin()
    marlin.setDebug()
    marlin.setVersion( self.softwareVersion )
    marlin.setDetectorModel( self.detectorModel )

    try:
      steeringFile = {
        350. : "clicReconstruction.xml",
        380. : "clicReconstruction.xml",
        420. : "clicReconstruction.xml",
        1400.: "clicReconstruction.xml",
        3000.: "clicReconstruction.xml",
      }.get( self.energy )
    except KeyError:
      print "No marlin steeringFile defined for ", self.energy
      raise RuntimeError( '1' )

    marlin.setSteeringFile( steeringFile )
    return marlin


  def createSimulationProduction( self, meta, prodName, parameterDict ):
    """ create simulation production """
    from ILCDIRAC.Interfaces.API.NewInterface.ProductionJob import ProductionJob
    simProd = ProductionJob()
    simProd.dryrun = self._flags.dryRun
    simProd.setLogLevel( self.productionLogLevel )
    simProd.setProdType( 'MCSimulation' )
    simProd.setClicConfig( self.clicConfig )
    res = simProd.setInputDataQuery( meta )
    if not res['OK']:
      print "Error creating Simulation Production:",res['Message']
      raise RuntimeError( '1' )
    simProd.setOutputSE( self.outputSE )
    simProd.setWorkflowName( self._productionName( prodName, parameterDict, 'sim') )
    simProd.setProdGroup( self.prodGroup+"_"+self.metaEnergy )
    #Add the application
    res = simProd.append( self.createDDSimApplication() )
    if not res['OK']:
      print "Error creating simulation Production:", res[ 'Message' ]
      raise RuntimeError( '1' )
    simProd.addFinalization(True,True,True,True)
    description = "Model: %s" % self.detectorModel
    if prodName:
      description += ", %s"%prodName
    simProd.setDescription( description )
    res = simProd.createProduction()
    if not res['OK']:
      print "Error creating simulation production",res['Message']
      raise RuntimeError( '1' )

    simProd.addMetadataToFinalFiles( { 'BeamParticle1': parameterDict['pname1'],
                                       'BeamParticle2': parameterDict['pname2'],
                                       'EPA_B1': parameterDict['epa_b1'],
                                       'EPA_B2': parameterDict['epa_b2'],
                                     }
                                   )

    res = simProd.finalizeProd()
    if not res['OK']:
      print "Error finalizing simulation production", res[ 'Message' ]
      raise RuntimeError( '1' )

    simulationMeta = simProd.getMetadata()
    return simulationMeta

  def createReconstructionProduction( self, meta, prodName, parameterDict ):
    """ create reconstruction production """
    from ILCDIRAC.Interfaces.API.NewInterface.ProductionJob import ProductionJob
    recProd = ProductionJob()
    recProd.dryrun = self._flags.dryRun
    recProd.setLogLevel( self.productionLogLevel )
    productionType = 'MCReconstruction_Overlay' if self._flags.over else 'MCReconstruction'
    recProd.setProdType( productionType )
    recProd.setClicConfig( self.clicConfig )

    res = recProd.setInputDataQuery( meta )
    if not res['OK']:
      print "Error setting inputDataQuery for Reconstruction production",res['Message']
      raise RuntimeError( '1' )

    recProd.setOutputSE( self.outputSE )
    recType = 'rec_overlay' if self._flags.over else 'rec'
    recProd.setWorkflowName( self._productionName( prodName, parameterDict, recType ) )
    recProd.setProdGroup( "%s_%s" %( self.prodGroup, self.metaEnergy ) )

    #Add overlay if needed
    if self._flags.over:
      res = recProd.append( self.createOverlayApplication() )
      if not res['OK']:
        print "Error appending overlay to reconstruction transformation", res['Message']
        raise RuntimeError( '1' )

    #Add reconstruction
    res = recProd.append( self.createMarlinApplication() )
    if not res['OK']:
      print "Error appending Marlin to reconstruction production", res['Message']
      raise RuntimeError( '1' )
    recProd.addFinalization(True,True,True,True)

    description = "CLICDet2017 %s" % self.energy
    description += "Overlay" if self._flags.over else "No Overlay"
    if prodName:
      description += ", %s"%prodName
    recProd.setDescription( description )

    res = recProd.createProduction()
    if not res['OK']:
      print "Error creating reconstruction production", res['Message']
      raise RuntimeError( '1' )

    recProd.addMetadataToFinalFiles( { 'BeamParticle1': parameterDict['pname1'],
                                       'BeamParticle2': parameterDict['pname2'],
                                       'EPA_B1': parameterDict['epa_b1'],
                                       'EPA_B2': parameterDict['epa_b2'],
                                     }
                                   )

    res = recProd.finalizeProd()
    if not res['OK']:
      print "Error finalising reconstruction production", res['Message']
      raise RuntimeError( '1' )

    reconstructionMeta = recProd.getMetadata()
    return reconstructionMeta

  def createMovingTransformation( self, meta, prodType ):
    """ create moving transformations for output files """
    if not self._flags.move:
      return

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
      print "ERROR creating MovingTransformation",prodType,"unknown"

    from ILCDIRAC.ILCTransformationSystem.Utilities.MovingTransformation import createMovingTransformation
    createMovingTransformation( targetSE, sourceSE, prodID, dataType )


  def createTransformations( self ):
    """ create all the transformations we want to create """

    metaSimInput = dict( self._meta )
    prodName = self.process

    for parameterDict in self.getParameterDictionary( prodName ):
      if self._flags.sim:
        simMeta = self.createSimulationProduction( metaSimInput, prodName, parameterDict )
        self.createMovingTransformation( simMeta, 'MCSimulation' )

      if self._flags.rec:
        recMeta = self.createReconstructionProduction( simMeta, prodName, parameterDict )
        self.createMovingTransformation( recMeta, 'MCReconstruction' )




if __name__ == "__main__":
  CLIP = Params()
  CLIP.registerSwitches()
  Script.parseCommandLine()
  from ILCDIRAC.Core.Utilities.CheckAndGetProdProxy import checkOrGetGroupProxy
  CHECKGROUP = checkOrGetGroupProxy( 'ilc_prod' )
  if not CHECKGROUP['OK']:
    exit(1)
  CHAIN = CLICDetProdChain( CLIP )
  CHAIN.createTransformations()
