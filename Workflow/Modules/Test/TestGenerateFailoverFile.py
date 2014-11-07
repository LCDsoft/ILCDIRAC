#!/usr/env python

"""
Test generateFailoverFile
"""
__RCSID__ = "$Id$"

import unittest, itertools, os, copy, shutil

from DIRAC import gLogger

from DIRAC import gLogger
from DIRAC.Core.Base import Script
Script.parseCommandLine()

gLogger.setLevel("DEBUG")
gLogger.showHeaders(True)
from ILCDIRAC.Workflow.Modules.ModuleBase import ModuleBase

from DIRAC.Workflow.Modules.test.Test_Modules import ModulesTestCase as DiracModulesTestCase


class ModulesTestCase ( DiracModulesTestCase ):
  """ ILCDirac version of Workflow module tests"""
  def setUp( self ):
    """Set up the objects"""
    super(ModulesTestCase, self).setUp()
    self.mb = ModuleBase()
    self.mb.rm = self.rm_mock
    self.mb.request = self.rc_mock
    self.mb.jobReport = self.jr_mock
    self.mb.fileReport = self.fr_mock
    self.mb.workflow_commons = self.wf_commons[0]



class ModuleBaseFailure( ModulesTestCase ):
  """ Test the generateFailoverFile function"""

    
  def test_generateFailoverFile( self ):
    """run the generateFailoverFile function and see what happens"""
    res = self.mb.generateFailoverFile()
    print res



def runTests():
  """Runs our tests"""
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ModulesTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ModuleBaseFailure ) )
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( FailoverRequestSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  print testResult

if __name__ == '__main__':
  runTests()
  