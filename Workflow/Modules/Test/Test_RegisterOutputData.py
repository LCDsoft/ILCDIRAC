#!/usr/bin/env python
"""Test the RegisterOutputData class"""

import sys
import unittest
from mock import patch, MagicMock as Mock

from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertMockCalls, assertDiracSucceedsWith
from ILCDIRAC.Workflow.Modules.RegisterOutputData import RegisterOutputData
from DIRAC import S_OK, S_ERROR

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Workflow.Modules.RegisterOutputData'

#pylint: disable=too-many-public-methods
class TestRegisterOutputData( unittest.TestCase ):
  """ Test the different methods of the class
  """

  def setUp( self ):
    self.ops_mock = Mock()
    self.fcc_mock = Mock()
    mocked_modules = { 'DIRAC.ConfigurationSystem.Client.Helpers.Operations' : self.ops_mock,
                       'DIRAC.Resources.Catalog.FileCatalogClient' : self.fcc_mock }
    self.module_patcher = patch.dict( sys.modules, mocked_modules )
    self.module_patcher.start()
    self.rod = RegisterOutputData()

  def tearDown( self ):
    self.module_patcher.stop()

  def test_applicationspecificinputs( self ):
    self.rod.step_commons[ 'Enable' ] = True
    self.rod.workflow_commons[ 'ProductionOutputData' ] = 'MyOutputLFN1.php;other__lfn.stdio;;last_file.stdhep'
    self.rod.workflow_commons[ 'Luminosity' ] = 1391.2
    self.rod.workflow_commons[ 'Info' ] = { 'stdhepcut' : { 'Reduction' : 12.2, 'CutEfficiency': 0.92 } }
    assertDiracSucceedsWith( self.rod.applicationSpecificInputs(), 'Parameters resolved', self )
    assertEqualsImproved( ( self.rod.enable, self.rod.prodOutputLFNs, self.rod.luminosity,
                            self.rod.sel_eff, self.rod.cut_eff, self.rod.add_info ),
                          ( True, [ 'MyOutputLFN1.php', 'other__lfn.stdio', '', 'last_file.stdhep' ],
                            1391.2, 12.2, 0.92, 'de' ), self )
    assert 'stdhepcut' not in self.rod.workflow_commons[ 'Info' ]

  def test_applicationspecificinputs_nonset( self ):
    self.rod.workflow_commons[ 'Info' ] = { 'SomeKey' : 'someValue' }
    assertDiracSucceedsWith( self.rod.applicationSpecificInputs(), 'Parameters resolved', self )
    assertEqualsImproved( ( self.rod.enable, self.rod.prodOutputLFNs, self.rod.luminosity,
                            self.rod.nbofevents, self.rod.sel_eff, self.rod.cut_eff, self.rod.add_info ),
                          ( True, [], 0, 0, 0, 0, 'ds7:SomeKeys9:someValuee' ), self )

  def test_applicationspecificinputs_othercases( self ):
    self.rod.step_commons[ 'Enable' ] = { 'mydict' : True }
    assertDiracSucceedsWith( self.rod.applicationSpecificInputs(), 'Parameters resolved', self )
    assertEqualsImproved( ( self.rod.enable, self.rod.sel_eff, self.rod.cut_eff, self.rod.add_info ),
                          ( False, 0, 0, '' ), self )

  def test_execute_resolveinput_fails( self ):
    with patch('%s.RegisterOutputData.resolveInputVariables' % MODULE_NAME, new=Mock(return_value=S_ERROR('mock_var_testerr'))):
      assertDiracFailsWith( self.rod.execute(), 'mock_var_testerr', self )

  def test_execute_status_not_ok( self ):
    self.rod.workflowStatus[ 'OK' ] = True
    self.rod.stepStatus[ 'OK' ] = False
    assertDiracSucceedsWith( self.rod.execute(), 'No registration of output data metadata attempted', self )

  def test_execute_no_production_output_data( self ):
    assertDiracSucceedsWith( self.rod.execute(), "No files' metadata to be registered", self )

  def test_execute_minimal( self ):
    self.rod.enable = False
    self.rod.prodOutputLFNs = [ 'myOutput.lfn.stdhep' ]
    with patch('%s.RegisterOutputData.resolveInputVariables' % MODULE_NAME, new=Mock(return_value=S_OK())):
      assertDiracSucceedsWith( self.rod.execute(), 'Output data metadata registered in catalog', self )

  def test_execute_maximal( self ):
    self.rod.nbofevents = 1389
    self.rod.luminosity = 9814.2
    self.rod.sel_eff = 184.2
    self.rod.cut_eff = 13.1
    self.rod.InputData = 'myTestInputFiles.rec'
    self.rod.inputdataMeta[ 'CrossSection' ] = 'myTestCrosssection'
    self.rod.WorkflowStartFrom = 'EventZerotest'
    self.rod.add_info = 'MoreInfo.additional_testme'
    self.rod.workflow_commons[ 'file_number_of_event_relation' ] = { 'myOutput.lfn.stdhep' : 2148 }
    self.rod.prodOutputLFNs = [ '/some/dir/myOutput.lfn.stdhep', 'some_other.file', '/other/dir/lastOne', '' ]
    fcc_mock = Mock()
    fcc_mock.setMetadata.return_value = S_OK()
    fcc_mock.addFileAncestors.return_value = S_OK()
    self.rod.filecatalog = fcc_mock
    with patch('%s.RegisterOutputData.resolveInputVariables' % MODULE_NAME, new=Mock(return_value=S_OK())):
      assertDiracSucceedsWith( self.rod.execute(), 'Output data metadata registered in catalog', self )
      assertMockCalls( fcc_mock.setMetadata, [
        ( '/some/dir/myOutput.lfn.stdhep', { 'NumberOfEvents' : 2148, 'Luminosity' : 9814.2,
                                             'Reduction' : 184.2, 'CutEfficiency' : 13.1, 'AdditionalInfo' : 'MoreInfo.additional_testme',
                                             'CrossSection' : 'myTestCrosssection',
                                             'FirstEventFromInput' : 'EventZerotest' } ),
        ( 'some_other.file', { 'NumberOfEvents' : 1389, 'Luminosity' : 9814.2,
                               'Reduction' : 184.2, 'CutEfficiency' : 13.1, 'AdditionalInfo' : 'MoreInfo.additional_testme',
                               'CrossSection' : 'myTestCrosssection', 'FirstEventFromInput' : 'EventZerotest' } ),
        ( '/other/dir/lastOne', { 'NumberOfEvents' : 1389, 'Luminosity' : 9814.2, 'Reduction' : 184.2,
                                  'CutEfficiency' : 13.1, 'AdditionalInfo' : 'MoreInfo.additional_testme',
                                  'CrossSection' : 'myTestCrosssection', 'FirstEventFromInput' :
                                  'EventZerotest' } ),
        ( '', { 'NumberOfEvents' : 1389, 'Luminosity' : 9814.2, 'Reduction' : 184.2, 'CutEfficiency' : 13.1,
                'AdditionalInfo' : 'MoreInfo.additional_testme', 'CrossSection' : 'myTestCrosssection',
                'FirstEventFromInput' : 'EventZerotest' } ) ], self )
      assertMockCalls( fcc_mock.addFileAncestors, [
        { '/some/dir/myOutput.lfn.stdhep' : { 'Ancestors' : 'myTestInputFiles.rec' } },
        { 'some_other.file' : { 'Ancestors' : 'myTestInputFiles.rec' } },
        { '/other/dir/lastOne' : { 'Ancestors' : 'myTestInputFiles.rec' } },
        { '' : { 'Ancestors' : 'myTestInputFiles.rec' } } ], self )

  def test_execute_maximal_othercase( self ):
    self.rod.nbofevents = 1389
    self.rod.luminosity = 9814.2
    self.rod.sel_eff = 184.2
    self.rod.cut_eff = 13.1
    self.rod.InputData = 'myTestInputFiles.rec'
    self.rod.inputdataMeta[ 'CrossSection' ] = 'myTestCrosssection'
    self.rod.inputdataMeta[ 'AdditionalInfo' ] = 'more_information_testme'
    self.rod.WorkflowStartFrom = 'EventZerotest'
    self.rod.prodOutputLFNs = [ '/some/test/dir/mytestfile.txt' ]
    fcc_mock = Mock()
    fcc_mock.setMetadata.return_value = S_OK()
    fcc_mock.addFileAncestors.return_value = S_OK()
    self.rod.filecatalog = fcc_mock
    with patch('%s.RegisterOutputData.resolveInputVariables' % MODULE_NAME, new=Mock(return_value=S_OK())):
      assertDiracSucceedsWith( self.rod.execute(), 'Output data metadata registered in catalog', self )
      fcc_mock.setMetadata.assert_called_once_with( '/some/test/dir/mytestfile.txt', {
        'NumberOfEvents' : 1389, 'Luminosity' : 9814.2, 'Reduction' : 184.2,
        'CutEfficiency' : 13.1, 'AdditionalInfo' : 'more_information_testme',
        'CrossSection' : 'myTestCrosssection', 'FirstEventFromInput' : 'EventZerotest' } )
      fcc_mock.addFileAncestors.assert_called_once_with( { '/some/test/dir/mytestfile.txt' : { 'Ancestors' : 'myTestInputFiles.rec' } } )

  def test_execute_setmeta_fails( self ):
    self.rod.nbofevents = 1389
    self.rod.luminosity = 9814.2
    self.rod.sel_eff = 184.2
    self.rod.cut_eff = 13.1
    self.rod.InputData = 'myTestInputFiles.rec'
    self.rod.inputdataMeta[ 'CrossSection' ] = 'myTestCrosssection'
    self.rod.inputdataMeta[ 'AdditionalInfo' ] = 'more_information_testme'
    self.rod.WorkflowStartFrom = 'EventZerotest'
    self.rod.prodOutputLFNs = [ '/some/test/dir/mytestfile.txt' ]
    fcc_mock = Mock()
    fcc_mock.setMetadata.return_value = S_ERROR( 'test_err_metadata' )
    fcc_mock.addFileAncestors.return_value = S_OK()
    self.rod.filecatalog = fcc_mock
    with patch('%s.RegisterOutputData.resolveInputVariables' % MODULE_NAME, new=Mock(return_value=S_OK())):
      assertDiracFailsWith( self.rod.execute(), 'test_err_metadata', self )

  def test_execute_maximal_addancestors_fails( self ):
    self.rod.nbofevents = 1389
    self.rod.luminosity = 9814.2
    self.rod.sel_eff = 184.2
    self.rod.cut_eff = 13.1
    self.rod.InputData = 'myTestInputFiles.rec'
    self.rod.inputdataMeta[ 'CrossSection' ] = 'myTestCrosssection'
    self.rod.inputdataMeta[ 'AdditionalInfo' ] = 'more_information_testme'
    self.rod.WorkflowStartFrom = 'EventZerotest'
    self.rod.prodOutputLFNs = [ '/some/test/dir/mytestfile.txt' ]
    fcc_mock = Mock()
    fcc_mock.setMetadata.return_value = S_OK()
    fcc_mock.addFileAncestors.return_value = S_ERROR( 'testme_addancestors_Err' )
    self.rod.filecatalog = fcc_mock
    with patch('%s.RegisterOutputData.resolveInputVariables' % MODULE_NAME, new=Mock(return_value=S_OK())):
      assertDiracFailsWith( self.rod.execute(), 'testme_addancestors_Err', self )
