#!/usr/local/env python
"""
Test Pythia module

"""

import unittest
from mock import patch, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Interfaces.API.NewInterface.Applications import Pythia
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertDiracSucceeds

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.Applications.Pythia'

#pylint: disable=protected-access
class PythiaTestCase( unittest.TestCase ):
  """ Base class for the Pythia test cases
  """
  def setUp(self):
    """set up the objects"""
    self.pyt = Pythia( {} )

  def test_userjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.pyt._userjobmodules( module_mock ), self )

  def test_prodjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.pyt._prodjobmodules( module_mock ), self )

  def test_userjobmodules_fails( self ):
    with patch('%s._setUserJobFinalization' % MODULE_NAME, new=Mock(return_value=S_OK('something'))),\
         patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_test_err'))):
      assertDiracFailsWith( self.pyt._userjobmodules( None ),
                            'userjobmodules failed', self )

  def test_prodjobmodules_fails( self ):
    with patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_OK('something'))), \
         patch('%s._setOutputComputeDataList' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_other_test_err'))):
      assertDiracFailsWith( self.pyt._prodjobmodules( None ),
                            'prodjobmodules failed', self )

  def test_checkconsistency( self ):
    self.pyt.version = '134'
    self.pyt.numberOfEvents = 2145
    self.pyt.outputFile = 'myoutput.file'
    self.pyt._jobtype = 'User'
    assertDiracSucceeds( self.pyt._checkConsistency(), self )
    self.assertNotIn( { 'outputFile' : '@{OutputFile}', 'outputPath' : '@{OutputPath}',
                        'outputDataSE' : '@{OutputSE}' }, self.pyt._listofoutput )
    self.assertNotIn( 'nbevts', self.pyt.prodparameters )
    self.assertNotIn( 'Process', self.pyt.prodparameters )

  def test_checkconsistency_noversion( self ):
    self.pyt.version = None
    assertDiracFailsWith( self.pyt._checkConsistency(), 'version not specified', self )

  def test_checkconsistency_nonbevts( self ):
    self.pyt.version = '134'
    self.pyt.numberOfEvents = None
    assertDiracFailsWith( self.pyt._checkConsistency(), 'number of events to generate not defined', self )

  def test_checkconsistency_nooutput( self ):
    self.pyt.version = '134'
    self.pyt.numberOfEvents = 2145
    self.pyt.outputFile = None
    assertDiracFailsWith( self.pyt._checkConsistency(), 'output file not defined', self )

  def test_checkconsistency_no_userjob( self ):
    self.pyt.version = '134'
    self.pyt.numberOfEvents = 2145
    self.pyt.outputFile = 'myoutput.file'
    self.pyt._jobtype = 'notUser'
    assertDiracSucceeds( self.pyt._checkConsistency(), self )
    self.assertIn( { 'outputFile' : '@{OutputFile}', 'outputPath' : '@{OutputPath}',
                        'outputDataSE' : '@{OutputSE}' }, self.pyt._listofoutput )
    self.assertIn( 'nbevts', self.pyt.prodparameters )
    self.assertIn( 'Process', self.pyt.prodparameters )

  def test_checkconsistency_no_cut( self ):
    self.pyt.version = '134'
    self.pyt.numberOfEvents = 2145
    self.pyt.outputFile = 'myoutput.file'
    self.pyt._jobtype = 'notUser'
    self.pyt.willCut()
    assertDiracSucceeds( self.pyt._checkConsistency(), self )
    self.assertNotIn( { 'outputFile' : '@{OutputFile}', 'outputPath' : '@{OutputPath}',
                        'outputDataSE' : '@{OutputSE}' }, self.pyt._listofoutput )
    self.assertIn( 'nbevts', self.pyt.prodparameters )
    self.assertIn( 'Process', self.pyt.prodparameters )






