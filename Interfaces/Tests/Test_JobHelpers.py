"""
Tests for Interfaces.Utilities.JobHelpers

"""

from __future__ import print_function
import unittest

from ILCDIRAC.Interfaces.Utilities import JobHelpers

__RCSID__ = "$Id$"

class TestJobHelpers( unittest.TestCase ):
  """tests for the JobHelper utilities"""

  def setUp( self ):
    pass

  def tearDown( self ):
    pass

  def test_getValue_list_int( self ):
    value = [ "2", 3 ]
    ret = JobHelpers.getValue( value, int, int )
    self.assertIsInstance( ret, int )
    self.assertEqual( ret, int(value[0]) )

  def test_getValue_int( self ):
    value = 2
    ret = JobHelpers.getValue( value, int, int )
    self.assertIsInstance( ret, int )
    self.assertEqual( ret, int(value) )

  def test_getValue_int_none( self ):
    value = "2"
    ret = JobHelpers.getValue( value, int, None )
    self.assertIsInstance( ret, int )
    self.assertEqual( ret, int(value) )

  def test_getValue_string_none( self ):
    value = [ "someString", "someOther" ]
    ret = JobHelpers.getValue( value, str, basestring )
    self.assertIsInstance( ret, basestring )
    self.assertEqual( ret, value[0] )


def runTests():
  """Runs our tests"""
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestJobHelpers )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  print(testResult)


if __name__ == '__main__':
  runTests()


  # if isinstance( compatmeta['NumberOfEvents'], list ):
  #   self.nbevts = int(compatmeta['NumberOfEvents'][0])
  # else:
  #   #type(compatmeta['NumberOfEvents']) in types.StringTypes:
  #   self.nbevts = int(compatmeta['NumberOfEvents'])
