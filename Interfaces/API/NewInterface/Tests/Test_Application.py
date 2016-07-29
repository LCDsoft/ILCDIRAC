#!/bin/env python

"""
Test Application base class

"""

import unittest
from mock import MagicMock as Mock, patch, create_autospec

from DIRAC import gLogger

from ILCDIRAC.Interfaces.API.NewInterface.Application import Application

__RCSID__ = "$Id$"

#pylint: disable=protected-access

gLogger.setLevel("DEBUG")
gLogger.showHeaders(True)

class TestApplication( unittest.TestCase ):
  """tests for the Application base interface"""

  def setUp( self ):
    self.app = Application()


  def tearDown( self ):
    pass



  def test_setParam_1( self ):
    """ test _setParam function """
    params = { 'Version': '01-00' }
    ret = self.app._setparams( params )
    self.assertTrue( ret['OK'], ret.get('Message','') )
    self.assertEqual( self.app.version, params['Version'] )


  def test_setParam_Fail_1( self ):
    """ test _setParam function, make sure getattr throws correct exception """
    params = { 'NotVersion': '01-00' }
    ret = self.app._setparams( params )
    self.assertTrue( ret['OK'], ret.get('Message','') )


def runTests():
  """Runs our tests"""
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestApplication )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  print testResult


if __name__ == '__main__':
  runTests()
