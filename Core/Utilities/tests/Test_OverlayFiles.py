'''

tests for OverlayFiles module

'''

import unittest


from ILCDIRAC.Core.Utilities import OverlayFiles as module

class TestHelper( unittest.TestCase ):
  """Test helper functions in the script"""

  def setUp ( self ):
    pass

  def test_energyWithUnit( self ):
    self.assertEqual( module.energyWithUnit( 300.0 ), '300GeV' )
    self.assertEqual( module.energyWithUnit( 380.0 ), '380GeV' )
    self.assertEqual( module.energyWithUnit( 3000.0 ), '3TeV' )
    self.assertEqual( module.energyWithUnit( 1000.0 ), '1TeV' )
    self.assertEqual( module.energyWithUnit( 1400.0 ), '1.4TeV' )
    self.assertEqual( module.energyWithUnit( 2500.0 ), '2.5TeV' )


  def test_backwardcompatibility( self ):
    self.assertEqual( module.energyWithLowerCaseUnit( 300.0 ) , module.oldEnergyWithUnit( 300.0 ) )
    self.assertEqual( module.energyWithLowerCaseUnit( 380.0 ) , module.oldEnergyWithUnit( 380.0 ) )
    self.assertEqual( module.energyWithLowerCaseUnit( 3000.0 ), module.oldEnergyWithUnit( 3000.0 ))
    self.assertEqual( module.energyWithLowerCaseUnit( 1000.0 ), module.oldEnergyWithUnit( 1000.0 ))
    self.assertEqual( module.energyWithLowerCaseUnit( 1400.0 ), module.oldEnergyWithUnit( 1400.0 ))
    self.assertEqual( module.energyWithLowerCaseUnit( 2500.0 ), module.oldEnergyWithUnit( 2500.0 ))


  def test_energyToInt( self ):
    self.assertEqual( module.energyToInt( '300GeV' ), 300  )
    self.assertEqual( module.energyToInt( '380GeV' ), 380  )
    self.assertEqual( module.energyToInt( '3TeV'   ), 3000 )
    self.assertEqual( module.energyToInt( '1TeV'   ), 1000 )
    self.assertEqual( module.energyToInt( '1.4TeV' ), 1400 )
    self.assertEqual( module.energyToInt( '2.5TeV' ), 2500 )
