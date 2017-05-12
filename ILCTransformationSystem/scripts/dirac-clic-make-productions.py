'''
Created on May 12, 2017

:author: A Sailer
'''

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from ILCDIRAC.Interfaces.API.NewInterface.ProductionJob import ProductionJob
from ILCDIRAC.Interfaces.API.NewInterface.Applications import Marlin, OverlayInput, DDSim
from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
from ILCDIRAC.ILCTransformationSystem.Utilities.MovingTransformation import createMovingTransformation
from ILCDIRAC.Core.Utilities.CheckAndGetProdProxy import checkOrGetGroupProxy



class CLICDetProdChain( object ):
  """ create applications and productions for clic physics studies 2017 """

  def __init__( self ):
    self.dirac = DiracILC()

    self.analysis = 'several'
    self.process = 'gghad'
    self.additional_name = ''
    self.energy = 3000.
    self._meta_energy = str(int( self.energy ))
    self.dryRun = False
    self.doMovingTransformations = False
    self.eventsPerJob = 100
    self.machine = "clic"
    #For meta def
    self.meta = { 'ProdID': 1,
                  'EvtType': self.process,
                  'Energy' : self._meta_energy,
                  'Machine': self.machine,
                }

    self.detectorModel='CLIC_o3_v11'
    self.softwareVersion = 'ILCSoft-2017-06-21_gcc62'
    self.clicConfig = 'ILCSoft-2017-06-21'

    self.runSim = True
    self.runReco = True
    self.runOverlay = False
    self.doCleanUp = dict( gen=True, sim=True, rec=True, dst=False )
    self.moveFiles = True


    self.productionLogLevel = 'VERBOSE'
    self.outputSE = 'CERN-DST-EOS'
    
    # final destination for files once they have been used
    self.finalOutputSE = 'CERN-SRM'

    self._createSimProds=True
    self._createRecProds=False


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
    ddsim = DDSim()
    ddsim.setVersion( self.softwareVersion )
    ddsim.setSteeringFile( 'clic_steer.py' )
    ddsim.setDetectorModel( self.detectorModel )
    return ddsim

  def createOverlayApplication( self ):
    """ create Overlay Application """
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

    simProd = ProductionJob()
    simProd.dryrun = self.dryRun
    simProd.setLogLevel( self.productionLogLevel )
    simProd.setProdType( 'MCSimulation' )
    simProd.setClicConfig( self.clicConfig )
    res = simProd.setInputDataQuery( meta )
    if not res['OK']:
      print "Error creating Simulation Production:",res['Message']
      raise RuntimeError( '1' )
    simProd.setOutputSE( self.outputSE )
    workflowName = "%s_%s_clic_sim_%s" %( parameterDict['process'], self.energy, prodName )
    simProd.setWorkflowName( workflowName )
    simProd.setProdGroup( self.analysis+"_"+str( self.energy ) )
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

    recProd = ProductionJob()
    recProd.dryrun = self.dryRun
    recProd.setLogLevel( self.productionLogLevel )
    productionType = 'MCReconstruction_Overlay' if self.runOverlay else 'MCReconstruction'
    recProd.setProdType( productionType )
    recProd.setClicConfig( self.clicConfig )

    res = recProd.setInputDataQuery( meta )
    if not res['OK']:
      print "Error setting inputDataQuery for Reconstruction production",res['Message']
      raise RuntimeError( '1' )

    recProd.setOutputSE( self.outputSE )
    process = parameterDict['process']
    recType = 'rec_overlay' if self.runOverlay else 'rec'
    workflowName = '%s_%s_%s_%s' % ( process, self.energy, recType, prodName)
    recProd.setWorkflowName( workflowName )
    productionGroup = "%s_%s" %( self.analysis, self.energy )
    recProd.setProdGroup( productionGroup )

    #Add overlay if needed
    if self.runOverlay:
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
    description += "Overlay" if self.runOverlay else "No Overlay"
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
    if self.dryRun or not self.doMovingTransformations:
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

    createMovingTransformation( targetSE, sourceSE, prodID, dataType )


  def createTransformations( self ):
    """ create all the transformations we want to create """

    metaSimInput = dict( self.meta )
    prodName = self.process

    for parameterDict in self.getParameterDictionary( prodName ):
      if self._createSimProds:
        simMeta = self.createSimulationProduction( metaSimInput, prodName, parameterDict )
        self.createMovingTransformation( simMeta, 'MCSimulation' )

      if self._createRecProds:
        recMeta = self.createReconstructionProduction( simMeta, prodName, parameterDict )
        self.createMovingTransformation( recMeta, 'MCReconstruction' )



if __name__ == "__main__":
  CHECKGROUP = checkOrGetGroupProxy( 'ilc_prod' )
  if not CHECKGROUP['OK']:
    exit(1)
  CHAIN = CLICDetProdChain()
  CHAIN.createTransformations()
