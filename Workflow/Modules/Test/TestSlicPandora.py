#!/usr/env python

"""Test user jobfinalization"""
__RCSID__ = "$Id$"


import unittest, copy, os
from mock import MagicMock as Mock
from DIRAC import gLogger, S_ERROR, S_OK

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from ILCDIRAC.Workflow.Modules.SLICPandoraAnalysis import SLICPandoraAnalysis

gLogger.setLevel("ERROR")
gLogger.showHeaders(True)

class TestSlicPandoraAnalysis( unittest.TestCase ):
  """ test SlicPandoraAnalysis """
  def setUp( self ):
    super(TestSlicPandoraAnalysis, self).setUp()
    self.spa = SLICPandoraAnalysis()

  def test_Unzip_file_into_dir(self):
    """test unzip_file_into_dir............................................................"""
    myfile = "clic_sid_cdr.zip"
    mydir = "temp_dir"
    from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import unzip_file_into_dir
    if not os.path.exists(mydir):
      os.mkdir(mydir)
    unzip_file_into_dir( myfile, mydir )
    unzip_file_into_dir( myfile, mydir )
    self.assertTrue ( True )




def runTests():
  """Runs our tests"""
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestSlicPandoraAnalysis )
  
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  print testResult


if __name__ == '__main__':
  runTests()
