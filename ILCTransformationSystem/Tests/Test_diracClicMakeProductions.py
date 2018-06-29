"""Test the dirac-ilc-make-productions script"""

import unittest
import importlib
import ConfigParser

from mock import MagicMock as Mock, patch

from DIRAC import S_OK, S_ERROR

#pylint: disable=protected-access, invalid-name
THE_SCRIPT = "ILCDIRAC.ILCTransformationSystem.scripts.dirac-clic-make-productions"
theScript = importlib.import_module(THE_SCRIPT)
SCP = "ILCDIRAC.ILCTransformationSystem.scripts.dirac-clic-make-productions.ConfigParser.SafeConfigParser"
__RCSID__ = "$Id$"

CONFIG_DICT = {}

class TestMaking( unittest.TestCase ):
  """Test the creation of transformation"""

  def setUp ( self ):
    self.tClientMock = Mock()
    self.tClientMock.createTransformationInputDataQuery.return_value = S_OK()
    self.tMock = Mock( return_value=self.tClientMock )
    self.opsMock = Mock()
    self.opsMock.getConfig = self.mockOpsConfig
    params = Mock()
    params.additionalName = None
    params.dryRun = True
    with patch( "ILCDIRAC.ILCTransformationSystem.scripts.dirac-clic-make-productions.CLICDetProdChain.loadParameters",
                new=Mock() ), \
         patch( "DIRAC.ConfigurationSystem.Client.Helpers.Operations.Operations",
                new=Mock( return_value=self.opsMock ) ):
      self.chain = theScript.CLICDetProdChain( params )


    self.configDict = {
      'prodGroup': "myProdGroup",
      'detectorModel': 'myDetectorModel',
      'softwareVersion': 'mySoftwareVersion',
      'configVersion': 'my',
      'configPackage': 'ClicConfig',
      'processes': 'process1, process2',
      'energies': '100, 200',
      'eventsPerJobs': '1000, 2000',
      'productionloglevel': 'DEBUGLEVEL3',
      'outputSE': 'CERN-CASTOR',
      'finalOutputSE': 'VAULT-101',
      'additionalName': 'waitForIt',
      'prodIDs': '123, 456',
      'eventsInSplitFiles': '5000, 6000',
      'ProdTypes': 'Gen, RecOver',
      'MoveTypes': '',
      'overlayEvents': '',
      'cliReco': '--Config.Tracking=Tracked',
      'whizard2Version': 'myWhizardVersion',
      'whizard2SinFile': 'myWhizardSinFile1, myWhizardSinFile2',
      'numberOfTasks': '1, 2',
    }

    self.pMockMod = Mock()
    self.pjMock = Mock( name ="ProductionJob" )
    self.pMockMod.return_value = self.pjMock
    self.pjMock.getMetadata.return_value = {}

    self.cpMock = self.getCPMock()

  def getCPMock(self):
    """Return a Mock for the ConfigParser."""
    cpMock = Mock()
    cpMock.read = Mock()
    cpMock.get = self.mockConfig
    cpMock.has_option = self.hasMock
    return cpMock

  def mockConfig( self, *args, **kwargs ): #pylint: disable=unused-argument
    """ mock the configparser object """

    self.assertEqual( args[0], theScript.PP )
    return self.configDict[ args[1] ]

  def hasMock(self, *args, **kwargs):  # pylint: disable=unused-argument
    """Mock the configparser.has_option function."""
    return self.configDict.get(args[1])

  def mockOpsConfig( self, *args, **kwargs ): #pylint: disable=unused-argument
    """ mock the operations getConfig calls """
    opsDict={
      'DefaultDetectorModel': 'detModel',
      'DefaultConfigVersion': 'Config',
      'DefaultSoftwareVersion': 'Software',
      'FailOverSE': 'FAIL=SRM',
    }
    self.assertIn( args[0], opsDict )
    return opsDict[ args[0] ]


  def test_meta( self ):
    ret = self.chain.meta( 123, 'process', 555.5 )
    self.assertEqual( {'ProdID': '123',
                       'EvtType': 'process',
                       'Energy': '555.5',
                       'Machine': 'clic',
                      }, ret )


  def test_overlayParameter( self ):
    self.assertEqual( self.chain.checkOverlayParameter( '300GeV' ), '300GeV' )
    self.assertEqual( self.chain.checkOverlayParameter( '3TeV' ), '3TeV' )
    self.assertEqual( self.chain.checkOverlayParameter( '' ), '' )

    with self.assertRaisesRegexp( RuntimeError, "does not end with unit" ):
      self.chain.checkOverlayParameter( '3000' )

    with self.assertRaisesRegexp( RuntimeError, "does not end with unit" ):
      self.chain.checkOverlayParameter( '3tev' )



  def test_loadParameters( self ):
    parameter = Mock()
    parameter.prodConfigFilename = None
    parameter.dumpConfigFile = None
    self.chain.loadParameters( parameter )

    c = self.chain

    parameter.prodConfigFilename = 'filename'

    with patch(SCP, new=Mock(return_value=self.cpMock)):
      c.loadParameters( parameter )
    self.assertEqual( c.prodGroup, "myProdGroup" )
    self.assertEqual( c.detectorModel, "myDetectorModel" )
    self.assertEqual( c.prodIDs, [123, 456] )
    self.assertEqual( c.energies, [100, 200] )
    self.assertEqual( c.eventsPerJobs, [1000, 2000] )
    self.assertEqual( c.eventsInSplitFiles, [5000, 6000] )

    self.assertEqual(c.whizard2Version, "myWhizardVersion")
    self.assertEqual(c.whizard2SinFile, ['myWhizardSinFile1', 'myWhizardSinFile2'])

    self.configDict['prodIDs'] = "123, 456, 789"
    with patch(SCP, new=Mock(return_value=self.cpMock)), \
      self.assertRaisesRegexp( AttributeError, "Lengths of Processes"):
      c.loadParameters( parameter )

    self.cpMock.has_option = Mock()
    self.cpMock.has_option.return_value = False
    with patch(SCP, new=Mock(return_value=self.cpMock)):
      c.loadParameters( parameter )
    self.assertEqual( c.prodIDs, [1, 1] )
    self.assertEqual(c.cliRecoOption, '--Config.Tracking=Tracked')


    self.configDict['eventsInSplitFiles'] = "1000"
    c._flags._spl = True
    with patch(SCP, new=Mock(return_value=self.cpMock)), \
      self.assertRaisesRegexp( AttributeError, "Length of eventsInSplitFiles"):
      c.loadParameters( parameter )



    parameter.prodConfigFilename = None
    parameter.dumpConfigFile = True
    with patch(SCP, new=Mock(return_value=self.cpMock)), \
      self.assertRaisesRegexp( RuntimeError, ''):
      c.loadParameters( parameter )

  def test_createMarlinApplication( self ):

    from ILCDIRAC.Interfaces.API.NewInterface.Applications import Marlin

    parameter = Mock()
    parameter.prodConfigFilename = 'filename'
    parameter.dumpConfigFile = False
    with patch(SCP, new=Mock(return_value=self.cpMock)):
      self.chain.loadParameters( parameter )

    ret = self.chain.createMarlinApplication(300.0, over=True)
    self.assertIsInstance( ret, Marlin )
    self.assertEqual( ret.detectortype, 'myDetectorModel' )
    self.assertEqual( ret.steeringFile, 'clicReconstruction.xml' )
    self.assertEqual(self.chain.cliRecoOption, '--Config.Tracking=Tracked')
    self.assertEqual(ret.extraCLIArguments, '--Config.Tracking=Tracked  --Config.Overlay=300GeV ')

    with patch(SCP, new=Mock(return_value=self.cpMock)):
      self.chain.loadParameters( parameter )
    self.chain._flags._over = False

    ret = self.chain.createMarlinApplication(300.0, over=False)
    self.assertIsInstance( ret, Marlin )
    self.assertEqual( ret.detectortype, 'myDetectorModel' )
    self.assertEqual( ret.steeringFile, 'clicReconstruction.xml' )
    self.assertEqual(self.chain.cliRecoOption, '--Config.Tracking=Tracked')
    self.assertEqual(ret.extraCLIArguments, '--Config.Tracking=Tracked ')

  def test_createWhizard2Application(self):

    from ILCDIRAC.Interfaces.API.NewInterface.Applications import Whizard2

    parameter = Mock()
    parameter.whizard2SinFile = 'filename'
    parameter.dumpConfigFile = False
    with patch(SCP, new=Mock(return_value=self.cpMock)), \
         patch("DIRAC.ConfigurationSystem.Client.Helpers.Operations.Operations",
               new=Mock(return_value=self.opsMock)):
      self.chain.loadParameters(parameter)

    ret = self.chain.createWhizard2Application({'ProdID': '123',
                                                'EvtType': 'process',
                                                'Energy': '555',
                                                'Machine': 'clic'},
                                               100,
                                               'sinFile')
    self.assertIsInstance(ret, Whizard2)
    self.assertEqual(ret.version, 'myWhizardVersion')

  def test_createDDSimApplication(self):

    from ILCDIRAC.Interfaces.API.NewInterface.Applications import DDSim

    parameter = Mock()
    parameter.prodConfigFilename = 'filename'
    parameter.dumpConfigFile = False
    with patch(SCP, new=Mock(return_value=self.cpMock)), \
         patch( "DIRAC.ConfigurationSystem.Client.Helpers.Operations.Operations",
                new=Mock(return_value=self.opsMock ) ):
      self.chain.loadParameters( parameter )

    ret = self.chain.createDDSimApplication()
    self.assertIsInstance( ret, DDSim )
    self.assertEqual( ret.steeringFile, 'clic_steer.py' )

  def test_createSplitApplication( self ):

    from ILCDIRAC.Interfaces.API.NewInterface.Applications import StdHepSplit

    parameter = Mock()
    parameter.prodConfigFilename = 'filename'
    parameter.dumpConfigFile = False
    with patch(SCP, new=Mock(return_value=self.cpMock)):
      self.chain.loadParameters( parameter )

    ret = self.chain.createSplitApplication( 100, 1000, 'stdhep')
    self.assertIsInstance( ret, StdHepSplit )
    self.assertEqual( ret.datatype, 'gen' )
    self.assertEqual( ret.maxRead, 1000 )
    self.assertEqual( ret.numberOfEventsPerFile, 100 )

  def test_createOverlayApplication( self ):

    from ILCDIRAC.Interfaces.API.NewInterface.Applications import OverlayInput

    parameter = Mock()
    parameter.prodConfigFilename = 'filename'
    parameter.dumpConfigFile = False
    with patch(SCP, new=Mock(return_value=self.cpMock)):
      self.chain.loadParameters( parameter )
    ret = self.chain.createOverlayApplication( 350 )
    self.assertIsInstance( ret, OverlayInput )
    self.assertEqual( ret.machine, 'clic_opt' )

    with self.assertRaisesRegexp( RuntimeError, 'No overlay parameters'):
      ret = self.chain.createOverlayApplication( 355 )


  def test_createSplitProduction( self ):

    with patch("ILCDIRAC.Interfaces.API.NewInterface.ProductionJob.ProductionJob", new=self.pMockMod ):
      retMeta = self.chain.createSplitProduction(
        meta = { 'ProdID':23, 'Energy':350 },
        prodName = "prodJamesProd",
        parameterDict = self.chain.getParameterDictionary( 'MI6' )[0],
        eventsPerJob = 007,
        eventsPerBaseFile = 700,
      )

    self.assertEqual( retMeta, {} )

  def test_createRecoProduction( self ):

    self.chain._flags._over = True
    self.assertTrue( self.chain._flags.over )
    self.chain.overlayEvents = '1.4TeV'
    with patch("ILCDIRAC.Interfaces.API.NewInterface.ProductionJob.ProductionJob", new=self.pMockMod ):
      retMeta = self.chain.createReconstructionProduction(
        meta = { 'ProdID':23, 'Energy':350 },
        prodName = "prodJamesProd",
        parameterDict = self.chain.getParameterDictionary( 'MI6' )[0],
        over=False,
      )
    self.assertEqual( retMeta, {} )
    self.assertEqual(self.chain.cliRecoOption, '')

  def test_createSimProduction( self ):
    with patch("ILCDIRAC.Interfaces.API.NewInterface.ProductionJob.ProductionJob", new=self.pMockMod ):
      retMeta = self.chain.createSimulationProduction(
        meta = { 'ProdID':23, 'Energy':350 },
        prodName = "prodJamesProd",
        parameterDict = self.chain.getParameterDictionary( 'MI6' )[0],
      )
    self.assertEqual( retMeta, {} )

  def test_createGenProduction(self):
    with patch("ILCDIRAC.Interfaces.API.NewInterface.ProductionJob.ProductionJob", new=self.pMockMod):
      retMeta = self.chain.createGenerationProduction(meta={'ProdID': 23, 'Energy': 350, 'EvtType': 'ttBond'},
                                                      prodName="prodJamesProd",
                                                      parameterDict=self.chain.getParameterDictionary('MI6')[0],
                                                      eventsPerJob=10,
                                                      nbTasks='10',
                                                      sinFile='myWhizardSinFile'
                                                     )
    self.assertEqual(retMeta, {})

  def test_createMovingTransformation( self ):
    self.chain.outputSE = "Source"
    self.chain.finalOutputSE = "Target"
    self.chain._flags._rec=True
    self.chain._flags._sim=True
    self.chain._flags._moveDst=True
    self.chain._flags._moveRec=False
    self.chain._flags._moveSim=True
    self.chain._flags._moves=True
    self.chain._flags._dryRun=False
    with patch("ILCDIRAC.ILCTransformationSystem.Utilities.DataTransformation.createDataTransformation") as moveMock:
      self.chain.createMovingTransformation( {'ProdID':666}, 'MCReconstruction' )
      moveMock.assert_called_once_with('Moving', "Target", "Source", 666, "DST")

    with patch("ILCDIRAC.ILCTransformationSystem.Utilities.DataTransformation.createDataTransformation") as moveMock:
      self.chain.createMovingTransformation( {'ProdID':666}, 'MCSimulation' )
      moveMock.assert_called_once_with('Moving', "Target", "Source", 666, "SIM")


    self.chain._flags._rec=True
    self.chain._flags._moves=False
    self.chain._flags._dryRun=False
    with patch("ILCDIRAC.ILCTransformationSystem.Utilities.DataTransformation.createDataTransformation") as moveMock:
      self.chain.createMovingTransformation( {'ProdID':666}, 'MCReconstruction' )
      moveMock.assert_not_called()

    with self.assertRaisesRegexp( RuntimeError, 'ERROR creating Moving'):
      self.chain.createMovingTransformation( {'ProdID':666}, "Split" )

  def test_setApplicationOptions(self):
    application = Mock()
    application.setSomeParameter = Mock()
    self.chain.applicationOptions['AppName'] = [('SomeParameter', 'SomeValue')]
    self.chain._setApplicationOptions('AppName', application)
    application.setSomeParameter.assert_called_once_with('SomeValue')

    from ILCDIRAC.Interfaces.API.NewInterface.Applications import Marlin
    application = Marlin()
    self.chain.applicationOptions['AppName'] = [('SomeOtherParameter', 'SomeValue')]
    with self.assertRaisesRegexp(AttributeError, 'Cannot set'):
      self.chain._setApplicationOptions('AppName', application)

  def test_getProdInfoFromIDs(self):
    # successful
    self.chain.prodIDs = [12345]
    trClientMock = Mock(name='trClient')
    trClientMock.getTransformation.return_value = S_OK({'EventsPerTask': 123})
    trMock = Mock(return_value=trClientMock)
    fcClientMock = Mock(name='fcClient')
    fcClientMock.findFilesByMetadata.return_value = S_OK(['/path/to/file'])
    fcClientMock.getDirectoryUserMetadata.return_value = S_OK({'EvtType': 'haha', 'Energy': 321})
    fcMock = Mock(return_value=fcClientMock)
    with patch('DIRAC.TransformationSystem.Client.TransformationClient.TransformationClient', new=trMock), \
         patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient', new=fcMock):
      self.chain._getProdInfoFromIDs()
    self.assertEqual(self.chain.eventsPerJobs, [123])
    self.assertEqual(self.chain.processes, ['haha'])
    self.assertEqual(self.chain.energies, [321])

    # first exception
    self.chain.prodIDs = []
    with self.assertRaisesRegexp(AttributeError, 'No prodIDs'):
      self.chain._getProdInfoFromIDs()

    # second exception
    self.chain.prodIDs = [12345]
    trClientMock = Mock(name='trClient')
    trClientMock.getTransformation.return_value = S_ERROR('No such prod')
    trMock = Mock(return_value=trClientMock)
    with patch('DIRAC.TransformationSystem.Client.TransformationClient.TransformationClient', new=trMock), \
         self.assertRaisesRegexp(AttributeError, 'No prodInfo found'):
      self.chain._getProdInfoFromIDs()

    # third exception
    self.chain.prodIDs = [12345]
    trClientMock = Mock(name='trClient')
    trClientMock.getTransformation.return_value = S_OK({'EventsPerTask': 123})
    trMock = Mock(return_value=trClientMock)
    fcClientMock = Mock(name='fcClient')
    fcClientMock.findFilesByMetadata.return_value = S_ERROR('No files found')
    fcMock = Mock(return_value=fcClientMock)
    with patch('DIRAC.TransformationSystem.Client.TransformationClient.TransformationClient', new=trMock), \
         patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient', new=fcMock), \
         self.assertRaisesRegexp(AttributeError, 'Could not find file'):
      self.chain._getProdInfoFromIDs()


class TestMakingFlags( unittest.TestCase ):
  """ Test the flags used in CLICDetProdChain """

  def setUp( self ):
    self.flags = theScript.CLICDetProdChain.Flags()

  def test_init( self ):
    f = self.flags
    self.assertTrue( f._dryRun )
    self.assertFalse( f._gen )
    self.assertFalse( f._spl )
    self.assertFalse( f._sim )
    self.assertFalse( f._rec )
    self.assertFalse( f._over )
    self.assertFalse( f._moves )
    self.assertFalse( f._moveGen )
    self.assertFalse( f._moveSim )
    self.assertFalse( f._moveRec )
    self.assertFalse( f._moveDst )

  def test_properties( self ):
    f = self.flags
    f._gen = True
    f._spl = True
    f._sim = True
    f._rec = False
    f._over = True
    self.assertTrue( f.dryRun )
    self.assertTrue( f.gen )
    self.assertTrue( f.spl )
    self.assertTrue( f.sim )
    self.assertFalse( f.rec )
    self.assertTrue( f.over )

    f._dryRun = True
    f._moves = True
    self.assertFalse( f.move )
    self.assertFalse( f.moveGen )
    self.assertFalse( f.moveSim )
    self.assertFalse( f.moveRec )
    self.assertFalse( f.moveDst )

    f._dryRun = False
    f._moveGen = True
    f._moveSim = True
    f._moveRec = True
    f._moveDst = False
    self.assertTrue( f.move )
    self.assertTrue( f.moveGen )
    self.assertTrue( f.moveSim )
    self.assertTrue( f.moveRec )
    self.assertFalse( f.moveDst )


  def test_str( self ):
    self.flags._gen = True
    self.flags._sim = True
    self.flags._rec = False
    self.flags._over = True

    flagStr = str( self.flags )

    self.assertEqual ( flagStr,
                       """

#Productions to create: Gen, Split, Sim, Rec, RecOver
ProdTypes = Gen, Sim, RecOver

move = False

#Datatypes to move: Gen, Sim, Rec, Dst
MoveTypes = \n""" )


  def test_loadFlags( self ):
    myConfig = ConfigParser.SafeConfigParser()
    myConfig.add_section( theScript.PRODUCTION_PARAMETERS )
    myConfig.set( theScript.PRODUCTION_PARAMETERS, 'ProdTypes', 'Gen, Sim,Rec' )
    myConfig.set( theScript.PRODUCTION_PARAMETERS, 'move', 'False' )
    myConfig.set( theScript.PRODUCTION_PARAMETERS, 'MoveTypes', 'gen, dst' )

    self.flags.loadFlags( myConfig )
    f = self.flags
    self.assertTrue( f.gen )
    self.assertTrue( f.sim )
    self.assertTrue( f.rec )
    self.assertFalse( f.over )
    self.assertTrue( f._moveGen )
    self.assertFalse( f._moveSim )
    self.assertFalse( f._moveRec )
    self.assertTrue( f._moveDst )

    myConfig.set( theScript.PRODUCTION_PARAMETERS, 'MoveTypes', 'gen, dst, badType' )
    with self.assertRaisesRegexp( AttributeError, 'badType'):
      self.flags.loadFlags( myConfig )


class TestMakingParams( unittest.TestCase ):
  """Test the parameters for the moving creation script"""

  def setUp ( self ):
    self.params = theScript.Params()

  def test_init( self ):
    self.assertIsNone( self.params.prodConfigFilename )
    self.assertFalse( self.params.dumpConfigFile )
    self.assertTrue( self.params.dryRun )
    self.assertIsNone( self.params.additionalName )

  def test_settters( self ):
    with patch( "%s.os.path.exists" % THE_SCRIPT, new=Mock(return_value=True)):
      self.assertTrue( self.params.setProdConf( 'myconf' )['OK'] )
    self.assertEqual( self.params.prodConfigFilename, 'myconf' )
    self.assertTrue( self.params.setDumpConf( '_' )['OK'] )
    self.assertTrue( self.params.dumpConfigFile )
    self.assertTrue( self.params.setEnable( '_' )['OK'] )
    self.assertFalse( self.params.dryRun )
    self.assertTrue( self.params.setAddName( 'addName')['OK'] )
    self.assertEqual( self.params.additionalName, 'addName')



if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( TestMaking )
  TESTRESULT = unittest.TextTestRunner( verbosity = 3 ).run( SUITE )
