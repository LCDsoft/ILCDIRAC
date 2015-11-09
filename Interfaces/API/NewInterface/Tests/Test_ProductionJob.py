#!/usr/local/env python

"""
Test user jobfinalization

"""
__RCSID__ = "$Id$"

import unittest
from decimal import Decimal
from DIRAC import gLogger
from ILCDIRAC.Interfaces.API.NewInterface.ProductionJob import ProductionJob

gLogger.setLevel("DEBUG")
gLogger.showHeaders(True)

class ProductionJobTestCase( unittest.TestCase ):
  """ Base class for the ProductionJob test cases
  """
  def setUp(self):
    """set up the objects"""
    super(ProductionJobTestCase, self).setUp()
    self.prodJob = ProductionJob()
    self.prodJob.energy=250.0

  def test_Energy250( self ):
    """ProductionJob getEnergyPath 250gev..........................................................."""
    self.prodJob.energy = Decimal('250.0')
    res = self.prodJob.getEnergyPath()
    self.assertEqual( "250gev/", res )

  def test_Energy350( self ):
    """ProductionJob getEnergyPath 350gev..........................................................."""
    self.prodJob.energy = 350.0
    res = self.prodJob.getEnergyPath()
    self.assertEqual( "350gev/", res )

  def test_Energy3000( self ):
    """ProductionJob getEnergyPatt 3tev............................................................."""
    self.prodJob.energy = 3000.0
    res = self.prodJob.getEnergyPath()
    self.assertEqual( "3tev/", res )

  def test_Energy1400( self ):
    """ProductionJob getEnergyPath 1.4tev .........................................................."""
    self.prodJob.energy = 1400.0
    res = self.prodJob.getEnergyPath()
    self.assertEqual( "1.4tev/", res )


    
def runTests():
  """Runs our tests"""
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ProductionJobTestCase )
  
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  print testResult


if __name__ == '__main__':
  runTests()
