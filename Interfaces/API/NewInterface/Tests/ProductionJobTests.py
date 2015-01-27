#!/usr/env python

"""Test user jobfinalization"""
__RCSID__ = "$Id$"


import unittest, copy, os
from decimal import Decimal

from mock import MagicMock as Mock

from DIRAC import gLogger, S_ERROR, S_OK

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from ILCDIRAC.Interfaces.API.NewInterface.ProductionJob import ProductionJob

gLogger.setLevel("DEBUG")
gLogger.showHeaders(True)


class ProductionJobTestCase( unittest.TestCase ):
  """ Base class for the ProductionJob test cases
  """
  def setUp(self):
    """set up the objects"""
    super(ProductionJobTestCase, self).setUp()
    self.pj = ProductionJob()
    self.pj.energy=250.0

  def test_Energy250( self ):
    """ test  250gev """
    self.pj.energy = Decimal('250.0')
    res = self.pj.getEnergyPath()
    self.assertEqual( "250gev/", res )

  def test_Energy350( self ):
    """ test  350gev """
    self.pj.energy = 350.0
    res = self.pj.getEnergyPath()
    self.assertEqual( "350gev/", res )

  def test_Energy3000( self ):
    """ test  3tev """
    self.pj.energy = 3000.0
    res = self.pj.getEnergyPath()
    self.assertEqual( "3tev/", res )

  def test_Energy1400( self ):
    """ test  1.4tev """
    self.pj.energy = 1400.0
    res = self.pj.getEnergyPath()
    self.assertEqual( "1.4tev/", res )


    
def runTests():
  """Runs our tests"""
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ProductionJobTestCase )
  
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  print testResult


if __name__ == '__main__':
  runTests()
