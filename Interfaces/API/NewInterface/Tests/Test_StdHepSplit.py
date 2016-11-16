#!/usr/local/env python
"""
Test StdHepSplit module

"""

import sys
import unittest
from mock import patch, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertDiracSucceeds, assertInImproved

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.Applications.StdHepSplit'

#pylint: disable=protected-access
class StdHepSplitTestCase( unittest.TestCase ):
  """ Base class for the StdHepSplit test cases
  """
  def setUp( self ):
    """set up the objects"""
    # Mock out modules that spawn other threads
    mocked_modules = { 'DIRAC.DataManagementSystem.Client.DataManager' : Mock() }
    self.module_patcher = patch.dict( sys.modules, mocked_modules )
    self.module_patcher.start()
    from ILCDIRAC.Interfaces.API.NewInterface.Applications import StdHepSplit
    self.shs = StdHepSplit( {} )

  def tearDown( self ):
    self.module_patcher.stop()

  def test_userjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.shs._userjobmodules( module_mock ), self )

  def test_prodjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.shs._prodjobmodules( module_mock ), self )

  def test_userjobmodules_fails( self ):
    with patch('%s._setUserJobFinalization' % MODULE_NAME, new=Mock(return_value=S_OK('something'))),\
         patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_test_err'))):
      assertDiracFailsWith( self.shs._userjobmodules( None ),
                            'userjobmodules failed', self )

  def test_prodjobmodules_fails( self ):
    with patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_OK('something'))), \
         patch('%s._setOutputComputeDataList' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_other_test_err'))):
      assertDiracFailsWith( self.shs._prodjobmodules( None ),
                            'prodjobmodules failed', self )

  def test_checkproductionmeta( self ):
    self.shs.numberOfEventsPerFile = 12348
    meta_dict = { 'NumberOfEvents' : True }
    assertDiracSucceeds( self.shs.checkProductionMetaData( meta_dict ), self )
    assertEqualsImproved( { 'NumberOfEvents' : 12348 }, meta_dict, self )

  def test_checkproductionmeta_changenothing( self ):
    meta_dict = { 'myentry' : True, 'other_entry' : 81943, 'other' : 'ae8fj', False : 1 }
    assertDiracSucceeds( self.shs.checkProductionMetaData( meta_dict ), self )
    assertEqualsImproved( { 'myentry' : True, 'other_entry' : 81943, 'other' : 'ae8fj', False : 1 },
                          meta_dict, self )

  def test_resolvelinkedstepparams( self ):
    instance_mock = Mock()
    step_mock = Mock()
    step_mock.getType.return_value = 'abc'
    self.shs._inputappstep = None
    self.shs._jobsteps = [ '', '', step_mock ]
    self.shs._linkedidx = 2
    assertDiracSucceeds( self.shs._resolveLinkedStepParameters( instance_mock ), self )
    instance_mock.setLink.assert_called_once_with( 'InputFile', 'abc', 'OutputFile' )

  def test_resolvelinkedstepparams_nothing_happens( self ):
    instance_mock = Mock()
    self.shs._inputappstep = None
    self.shs._jobsteps = None
    self.shs._linkedidx = [ 'abc' ]
    assertDiracSucceeds( self.shs._resolveLinkedStepParameters( instance_mock ), self )
    self.assertFalse( instance_mock.setLink.called )

  def test_checkconsistency( self ):
    self.shs._jobtype = 'notUser'
    self.shs.OutputFile = None
    assertDiracSucceeds( self.shs._checkConsistency(), self )
    assertInImproved( { 'outputFile' : '@{OutputFile}', 'outputPath' : '@{OutputPath}',
                        'outputDataSE' : '@{OutputSE}' }, self.shs._listofoutput, self )

  def test_checkconsistency_userjob( self ):
    job_mock = Mock()
    job_mock.datatype = 'testDatatype'
    self.shs._job = job_mock
    self.shs._jobtype = 'User'
    self.shs.OutputFile = None
    assertDiracSucceeds( self.shs._checkConsistency(), self )
    self.assertFalse( self.shs.outputFile )
    assertEqualsImproved( self.shs.datatype, 'testDatatype', self )
