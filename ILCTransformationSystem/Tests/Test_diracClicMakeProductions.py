"""Test the dirac-ilc-make-productions script"""

import unittest
import importlib
import ConfigParser

from mock import MagicMock as Mock, patch

from DIRAC import S_OK

#pylint: disable=protected-access, invalid-name

theScript = importlib.import_module("ILCDIRAC.ILCTransformationSystem.scripts.dirac-clic-make-productions")

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
      'clicConfig': 'myClicConfig',
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
    }

    self.pMockMod = Mock()
    self.pjMock = Mock( name ="ProductionJob" )
    self.pMockMod.return_value = self.pjMock
    self.pjMock.getMetadata.return_value = {}

  def mockConfig( self, *args, **kwargs ): #pylint: disable=unused-argument
    """ mock the configparser object """

    self.assertEqual( args[0], theScript.PP )
    return self.configDict[ args[1] ]

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
    self.assertEqual( {'ProdID': 123,
                       'EvtType': 'process',
                       'Energy': '555',
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

    cpMock = Mock()
    cpMock.read = Mock()
    cpMock.get = self.mockConfig

    parameter.prodConfigFilename = 'filename'

    with patch( "ILCDIRAC.ILCTransformationSystem.scripts.dirac-clic-make-productions.ConfigParser.SafeConfigParser",
                new=Mock(return_value=cpMock ) ):
      c.loadParameters( parameter )
    self.assertEqual( c.prodGroup, "myProdGroup" )
    self.assertEqual( c.detectorModel, "myDetectorModel" )
    self.assertEqual( c.prodIDs, [123, 456] )
    self.assertEqual( c.energies, [100, 200] )
    self.assertEqual( c.eventsPerJobs, [1000, 2000] )
    self.assertEqual( c.eventsInSplitFiles, [5000, 6000] )

    self.configDict['prodIDs'] = "123, 456, 789"
    with patch( "ILCDIRAC.ILCTransformationSystem.scripts.dirac-clic-make-productions.ConfigParser.SafeConfigParser",
                new=Mock(return_value=cpMock ) ), \
      self.assertRaisesRegexp( AttributeError, "Lengths of Processes"):
      c.loadParameters( parameter )

    cpMock.has_option.return_value = False
    with patch( "ILCDIRAC.ILCTransformationSystem.scripts.dirac-clic-make-productions.ConfigParser.SafeConfigParser",
                new=Mock(return_value=cpMock ) ):
      c.loadParameters( parameter )
    self.assertEqual( c.prodIDs, [1, 1] )


    self.configDict['eventsInSplitFiles'] = "1000"
    c._flags._spl = True
    with patch( "ILCDIRAC.ILCTransformationSystem.scripts.dirac-clic-make-productions.ConfigParser.SafeConfigParser",
                new=Mock(return_value=cpMock ) ), \
      self.assertRaisesRegexp( AttributeError, "Length of eventsInSplitFiles"):
      c.loadParameters( parameter )



    parameter.prodConfigFilename = None
    parameter.dumpConfigFile = True
    with patch( "ILCDIRAC.ILCTransformationSystem.scripts.dirac-clic-make-productions.ConfigParser.SafeConfigParser",
                new=Mock(return_value=cpMock ) ), \
      self.assertRaisesRegexp( RuntimeError, ''):
      c.loadParameters( parameter )

  def test_createMarlinApplication( self ):

    from ILCDIRAC.Interfaces.API.NewInterface.Applications import Marlin

    cpMock = Mock()
    cpMock.read = Mock()
    cpMock.get = self.mockConfig

    parameter = Mock()
    parameter.prodConfigFilename = 'filename'
    parameter.dumpConfigFile = False
    with patch( "ILCDIRAC.ILCTransformationSystem.scripts.dirac-clic-make-productions.ConfigParser.SafeConfigParser",
                new=Mock(return_value=cpMock ) ):
      self.chain.loadParameters( parameter )

    ret = self.chain.createMarlinApplication( 300.0 )
    self.assertIsInstance( ret, Marlin )
    self.assertEqual( ret.detectortype, 'myDetectorModel' )
    self.assertEqual( ret.steeringFile, 'clicReconstruction.xml' )

  def test_createDDSimApplication( self ):

    from ILCDIRAC.Interfaces.API.NewInterface.Applications import DDSim

    cpMock = Mock()
    cpMock.read = Mock()
    cpMock.get = self.mockConfig

    parameter = Mock()
    parameter.prodConfigFilename = 'filename'
    parameter.dumpConfigFile = False
    with patch( "ILCDIRAC.ILCTransformationSystem.scripts.dirac-clic-make-productions.ConfigParser.SafeConfigParser",
                new=Mock(return_value=cpMock ) ), \
         patch( "DIRAC.ConfigurationSystem.Client.Helpers.Operations.Operations",
                new=Mock(return_value=self.opsMock ) ):
      self.chain.loadParameters( parameter )

    ret = self.chain.createDDSimApplication()
    self.assertIsInstance( ret, DDSim )
    self.assertEqual( ret.steeringFile, 'clic_steer.py' )

  def test_createSplitApplication( self ):

    from ILCDIRAC.Interfaces.API.NewInterface.Applications import StdHepSplit

    cpMock = Mock()
    cpMock.read = Mock()
    cpMock.get = self.mockConfig

    parameter = Mock()
    parameter.prodConfigFilename = 'filename'
    parameter.dumpConfigFile = False
    with patch( "ILCDIRAC.ILCTransformationSystem.scripts.dirac-clic-make-productions.ConfigParser.SafeConfigParser",
                new=Mock(return_value=cpMock ) ):
      self.chain.loadParameters( parameter )

    ret = self.chain.createSplitApplication( 100, 1000, 'stdhep')
    self.assertIsInstance( ret, StdHepSplit )
    self.assertEqual( ret.datatype, 'gen' )
    self.assertEqual( ret.maxRead, 1000 )
    self.assertEqual( ret.numberOfEventsPerFile, 100 )

  def test_createOverlayApplication( self ):

    from ILCDIRAC.Interfaces.API.NewInterface.Applications import OverlayInput

    cpMock = Mock()
    cpMock.read = Mock()
    cpMock.get = self.mockConfig

    parameter = Mock()
    parameter.prodConfigFilename = 'filename'
    parameter.dumpConfigFile = False
    with patch( "ILCDIRAC.ILCTransformationSystem.scripts.dirac-clic-make-productions.ConfigParser.SafeConfigParser",
                new=Mock(return_value=cpMock ) ):
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

    with patch("ILCDIRAC.Interfaces.API.NewInterface.ProductionJob.ProductionJob", new=self.pMockMod ):
      retMeta = self.chain.createReconstructionProduction(
        meta = { 'ProdID':23, 'Energy':350 },
        prodName = "prodJamesProd",
        parameterDict = self.chain.getParameterDictionary( 'MI6' )[0],
      )

    self.assertEqual( retMeta, {} )


  def test_createSimProduction( self ):
    with patch("ILCDIRAC.Interfaces.API.NewInterface.ProductionJob.ProductionJob", new=self.pMockMod ):
      retMeta = self.chain.createSimulationProduction(
        meta = { 'ProdID':23, 'Energy':350 },
        prodName = "prodJamesProd",
        parameterDict = self.chain.getParameterDictionary( 'MI6' )[0],
      )
    self.assertEqual( retMeta, {} )



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
    f._rec = True
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
    self.flags._rec = True
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
