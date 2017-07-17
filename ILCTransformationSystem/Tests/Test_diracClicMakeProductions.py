"""Test the dirac-ilc-make-productions script"""

import unittest
import importlib
import ConfigParser

from mock import MagicMock as Mock, patch

from DIRAC import S_OK

#pylint: disable=protected-access, invalid-name

theScript = importlib.import_module("ILCDIRAC.ILCTransformationSystem.scripts.dirac-clic-make-productions")

__RCSID__ = "$Id$"

class TestMaking( unittest.TestCase ):
  """Test the creation of moving transformation"""

  def setUp ( self ):
    self.tClientMock = Mock()
    self.tClientMock.createTransformationInputDataQuery.return_value = S_OK()
    self.tMock = Mock( return_value=self.tClientMock )


  def tearDown ( self ):
    pass


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
MoveTypes = 
""" )


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

class TestMakingParams( unittest.TestCase ):
  """Test the parameters for the moving creation script"""

  def setUp ( self ):
    self.params = theScript.Params()

  @patch( "ILCDIRAC.Core.Utilities.CheckAndGetProdProxy.checkAndGetProdProxy", new = Mock( return_value=S_OK()) )
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
