#!/usr/local/env python
"""
Test SLCIOConcatenatemodule

"""

import sys
import unittest
from mock import patch, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertDiracSucceeds, assertInImproved

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.Applications.SLCIOConcatenate'

#pylint: disable=protected-access
class SLCIOConcatenateTestCase( unittest.TestCase ):
  """ Base class for the SLCIOConcatenate test cases
  """
  def setUp(self):
    """set up the objects"""
    # Mock out modules that spawn other threads
    sys.modules['DIRAC.DataManagementSystem.Client.DataManager'] = Mock()
    from ILCDIRAC.Interfaces.API.NewInterface.Applications import SLCIOConcatenate
    self.sco = SLCIOConcatenate( {} )

  def test_userjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.sco._userjobmodules( module_mock ), self )

  def test_prodjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.sco._prodjobmodules( module_mock ), self )

  def test_userjobmodules_fails( self ):
    with patch('%s._setUserJobFinalization' % MODULE_NAME, new=Mock(return_value=S_OK('something'))),\
         patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_test_err'))):
      assertDiracFailsWith( self.sco._userjobmodules( None ),
                            'userjobmodules failed', self )

  def test_prodjobmodules_fails( self ):
    with patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_OK('something'))), \
         patch('%s._setOutputComputeDataList' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_other_test_err'))):
      assertDiracFailsWith( self.sco._prodjobmodules( None ),
                            'prodjobmodules failed', self )

  def test_checkworkflowconsistency( self ):
    self.sco._checkWorkflowConsistency()
    #pylint: disable=redundant-unittest-assert
    self.assertTrue( True )

  def test_resolvelinkedstepparams( self ):
    instance_mock = Mock()
    step_mock = Mock()
    step_mock.getType.return_value = 'abc'
    self.sco._inputappstep = None
    self.sco._jobsteps = [ '', '', step_mock ]
    self.sco._linkedidx = 2
    assertDiracSucceeds( self.sco._resolveLinkedStepParameters( instance_mock ), self )
    instance_mock.setLink.assert_called_once_with( 'InputFile', 'abc', 'OutputFile' )

  def test_resolvelinkedstepparams_nothing_happens( self ):
    instance_mock = Mock()
    self.sco._inputappstep = None
    self.sco._jobsteps = None
    self.sco._linkedidx = [ 'abc' ]
    assertDiracSucceeds( self.sco._resolveLinkedStepParameters( instance_mock ), self )
    self.assertFalse( instance_mock.setLink.called )

  def test_checkconsistency( self ):
    self.sco._jobtype = 'notUser'
    self.sco.OutputFile = None
    assertDiracSucceeds( self.sco._checkConsistency(), self )
    assertInImproved( { 'outputFile' : '@{OutputFile}', 'outputPath' : '@{OutputPath}',
                        'outputDataSE' : '@{OutputSE}' }, self.sco._listofoutput, self )

  def test_checkconsistency_userjob( self ):
    self.sco._jobtype = 'User'
    self.sco.OutputFile = None
    assertDiracSucceeds( self.sco._checkConsistency(), self )
    assertEqualsImproved( self.sco.outputFile, 'LCIOFileConcatenated.slcio', self )
