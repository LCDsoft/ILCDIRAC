"""Test the Core Splitting Module"""

import unittest
from ILCDIRAC.Core.Utilities.Splitting import addJobIndexToFilename

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Core.Utilities.Splitting'

class Splittingtest( unittest.TestCase ):
  """Test the Splitting"""

  def setUp ( self ):
    pass

  def tearDown ( self ):
    pass

  def test_Splitting( self ):
    fileIn = "output_%n.slcio"
    jobIndex=123
    self.assertEqual( "output_123.slcio", addJobIndexToFilename( fileIn, jobIndex ) )

    fileIn = "output_%n.slcio"
    jobIndex=0
    self.assertEqual( "output_0.slcio", addJobIndexToFilename( fileIn, jobIndex ) )

    fileIn = "output.slcio"
    jobIndex=123
    self.assertEqual( "output_123.slcio", addJobIndexToFilename( fileIn, jobIndex ) )

    fileIn = "output"
    jobIndex=123
    self.assertEqual( "output_123", addJobIndexToFilename( fileIn, jobIndex ) )

    fileIn = "/ilc/user/t/tester/some/folder/output"
    jobIndex=123
    self.assertEqual( "/ilc/user/t/tester/some/folder/output_123", addJobIndexToFilename( fileIn, jobIndex ) )

    fileIn = "/ilc/user/t/tester/some/folder/%n/output"
    jobIndex=123
    self.assertEqual( "/ilc/user/t/tester/some/folder/123/output", addJobIndexToFilename( fileIn, jobIndex ) )
