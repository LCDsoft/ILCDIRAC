"""
Tests for the Whizard module

"""
import unittest
import os
from mock import mock_open, patch, call, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Interfaces.API.NewInterface.Applications import Whizard
from ILCDIRAC.Tests.Utilities.FileUtils import FileUtil
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsXml, \
  assertEqualsImproved, assertDiracFailsWith, assertDiracSucceeds, \
  assertDiracSucceedsWith_equals

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.Applications.Whizard'

#pylint: disable=protected-access
class TestWhizard( unittest.TestCase ):
  """ Tests the Whizard class """

  EXPECTED_PRINTOUT = '&process_input\n process_id = ""\n cm_frame = T\n sqrts = 3000\n luminosity = 0\n polarized_beams = T\n structured_beams = T\n beam_recoil = F\n recoil_conserve_momentum = F\n filename = "whizard"\n directory = ""\n input_file = ""\n input_slha_format = F\n/\n&integration_input\n calls = 1 50000 10 50000 1 1500000\n seed = \n reset_seed_each_process = F\n accuracy_goal = 0\n efficiency_goal = 100\n time_limit_adaptation = 0\n stratified = T\n use_efficiency = F\n weights_power = 0.25\n min_bins = 3\n max_bins = 20\n min_calls_per_bin = 10\n min_calls_per_channel = 0\n write_grids = T\n write_grids_raw = F\n write_grids_file = ""\n write_all_grids = F\n write_all_grids_file = ""\n read_grids = F\n read_grids_raw = F\n read_grids_force = T\n read_grids_file = ""\n generate_phase_space = T\n read_model_file = ""\n write_phase_space_file = ""\n read_phase_space = T\n read_phase_space_file = ""\n phase_space_only = F\n use_equivalences = T\n azimuthal_dependence = F\n write_phase_space_channels_file = ""\n off_shell_lines = 1\n extra_off_shell_lines = 1\n splitting_depth = 1\n exchange_lines = 3\n show_deleted_channels = F\n single_off_shell_decays = T\n double_off_shell_decays = F\n single_off_shell_branchings = T\n double_off_shell_branchings = T\n massive_fsr = T\n threshold_mass = -10\n threshold_mass_t = -10\n default_jet_cut = 10\n default_mass_cut = 4\n default_energy_cut = 10\n default_q_cut = 4\n write_default_cuts_file = ""\n read_cuts_file = ""\n user_cut_mode = 0\n user_weight_mode = 0\n/\n&simulation_input\n n_events = 0\n n_calls = 0\n n_events_warmup = 0\n unweighted = T\n normalize_weight = T\n write_weights = F\n write_weights_file = ""\n safety_factor = 1\n write_events = T\n write_events_format = 20\n write_events_file = ""\n events_per_file = 5000000\n bytes_per_file = 0\n min_file_count = 1\n max_file_count = 999\n write_events_raw = F\n write_events_raw_file = ""\n read_events = F\n read_events_force = T\n read_events_raw_file = ""\n keep_beam_remnants = T\n keep_initials = T\n guess_color_flow = F\n recalculate = F\n fragment = T\n fragmentation_method = 3\n user_fragmentation_mode = 0\n pythia_parameters = "PMAS(25,1)=120.; PMAS(25,2)=0.3605E-02; MSTU(22)=20 ;MSTJ(28)=2 ;PARJ(21)=0.40000;PARJ(41)=0.11000; PARJ(42)=0.52000; PARJ(81)=0.25000; PARJ(82)=1.90000; MSTJ(11)=3; PARJ(54)=-0.03100; PARJ(55)=-0.00200;PARJ(1)=0.08500; PARJ(3)=0.45000; PARJ(4)=0.02500; PARJ(2)=0.31000; PARJ(11)=0.60000; PARJ(12)=0.40000; PARJ(13)=0.72000;PARJ(14)=0.43000; PARJ(15)=0.08000; PARJ(16)=0.08000; PARJ(17)=0.17000; MSTP(3)=1;"\n pythia_processes = ""\n shower = F\n shower_nf = 5\n shower_running_alpha_s = F\n shower_alpha_s = 0.2\n shower_lambda = 0.29\n shower_t_min = 1.0\n shower_md = 0.330\n shower_mu = 0.330\n shower_ms = 0.500\n shower_mc = 1.5\n shower_mb = 4.8\n/\n&diagnostics_input\n chattiness = 4\n catch_signals = T\n time_limit = 0\n warn_empty_channel = F\n screen_events = F\n screen_histograms = F\n screen_diagnostics = F\n show_pythia_banner = T\n show_pythia_initialization = T\n show_pythia_statistics = T\n write_logfile = T\n write_logfile_file = ""\n show_input = T\n show_results = T\n show_phase_space = F\n show_cuts = T\n show_histories = F\n show_history = T\n show_weights = T\n show_event = F\n show_histograms = F\n show_overflow = F\n show_excess = T\n read_analysis_file = ""\n plot_width = 130\n plot_height = 90\n plot_excess = T\n plot_history = T\n plot_grids_channels = ""\n plot_grids_logscale = 10\n slha_rewrite_input = T\n slha_ignore_errors = F\n/\n&parameter_input\n/\n&beam_input\n energy = 0\n angle = 0\n direction = 0 0 0\n vector_polarization = F\n polarization = 0.0 0.0\n particle_code = 0\n particle_name = "e1"\n USER_spectrum_on = T\n USER_spectrum_mode = 11\n ISR_on = T\n ISR_alpha = 0.0072993\n ISR_m_in = 0.000511\n ISR_LLA_order = 3\n ISR_map = T\n EPA_on = F\n EPA_map = T\n EPA_alpha = 0.0072993\n EPA_m_in = 0.000511\n EPA_mX = 4\n EPA_Q_max = 4\n EPA_x0 = 0\n EPA_x1 = 0\n/\n&beam_input\n energy = 0\n angle = 0\n direction = 0 0 0\n vector_polarization = F\n polarization = 0.0 0.0\n particle_code = 0\n particle_name = "E1"\n USER_spectrum_on = T\n USER_spectrum_mode = -11\n ISR_on = T\n ISR_alpha = 0.0072993\n ISR_m_in = 0.000511\n ISR_LLA_order = 3\n ISR_map = T\n EPA_on = F\n EPA_map = T\n EPA_alpha = 0.0072993\n EPA_m_in = 0.000511\n EPA_mX = 4\n EPA_Q_max = 4\n EPA_x0 = 0\n EPA_x1 = 0\n/'

  def setUp(self):
    self.whiz = Whizard()

  def test_getters( self ):
    from ILCDIRAC.Core.Utilities.WhizardOptions import WhizardOptions
    expected_pdict = { 'OK' : True, 'Value' : { 'integration_input' : {},
                                                'simulation_input' : {},
                                                'diagnostics_input' : {},
                                                'process_input' : {},
                                                'beam_input_1' : {},
                                                'beam_input_2' : {} } }
    pdict = self.whiz.getPDict()
    self.whiz.setEvtType( 'myevent_test' )
    self.assertIsNotNone( pdict )
    self.whiz.setGlobalEvtType( 'test_myglobalevt' )
    self.whiz.setLuminosity( 138.312 )
    self.whiz.setRandomSeed( 9024 )
    self.whiz.setParameterDict( { 'mytestval' : True, 'more_entres' : 'value',
                                  'something' : 9103 } )
    self.whiz.setGeneratorLevelCuts( { 'generator' : False, 'cuts' : 123,
                                       'level' : 'OK' } )
    assert not self.whiz.willBeCut
    self.whiz.willCut()
    assert not self.whiz.useGridFiles
    self.whiz.usingGridFiles()
    self.whiz.setJobIndex( 'mytestJobIndexS&P500' )
    self.whiz.setModel( 'mytestSModel' )
    self.whiz._wo = WhizardOptions( self.whiz.model )
    with patch('__builtin__.open', mock_open(), create=True) as mo:
      self.whiz.addedtojob = True
      self.whiz.dumpWhizardDotIn( 'someFile.in' )
      self.whiz.addedtojob = False
      mo.assert_called_once_with( 'someFile.in', 'w' )
      assertEqualsImproved( mo().write.mock_calls,
                            [ call( TestWhizard.EXPECTED_PRINTOUT ),
                              call( '\n' )], self )
    assertEqualsImproved( ( pdict, self.whiz.eventType,
                            self.whiz.globalEventType, self.whiz.luminosity,
                            self.whiz.randomSeed, self.whiz.parameterDict,
                            self.whiz.generatorLevelCuts, self.whiz.willBeCut,
                            self.whiz.useGridFiles, self.whiz.jobIndex,
                            self.whiz.model ),
                          ( expected_pdict, 'myevent_test', 'test_myglobalevt',
                            138.312, 9024,
                            { 'mytestval' : True, 'more_entres' : 'value',
                              'something' : 9103 },
                            { 'generator' : False, 'cuts' : 123,
                              'level' : 'OK' }, True, True,
                            'mytestJobIndexS&P500', 'mytestSModel' ) , self )
    assert not self.whiz._errorDict

  def test_setevttype_nostring( self ):
    assert not self.whiz._errorDict
    self.whiz.setEvtType( 123 )
    assertEqualsImproved( self.whiz.eventType, 123, self )
    assert self.whiz._errorDict

  def test_setevttype_already_added( self ):
    self.whiz.addedtojob = True
    assertDiracFailsWith( self.whiz.setEvtType( 'myothertestevent' ), \
                          'cannot modify', self )

  def test_dumpwhizarddotin( self ):
    assert not self.whiz._errorDict
    self.whiz.dumpWhizardDotIn()
    assert self.whiz._errorDict

  def test_setfullparamdict( self ):
    self.whiz.setFullParameterDict( { 'some_entry' : True, 'myParamDict' : {},
                                      123 : 2 } )
    assert not self.whiz._errorDict
    assertEqualsImproved( self.whiz.fullParameterDict,
                          { 'some_entry' : True, 'myParamDict' : {}, 123 : 2 },
                          self  )

  def test_setfullparamdict_wrongtype( self ):
    assert not self.whiz._errorDict
    self.whiz.setFullParameterDict( 'eihnff' )
    assert self.whiz._errorDict

  def test_checkconsistency( self ):
    self.whiz.setFullParameterDict( { 'process_input' :
                                      { 'process_id' : 'myprocessid',
                                        'sqrts' : 81 },
                                      'simulation_input' :
                                      { 'n_events' : 41 } } )
    plist_mock = Mock()
    plist_mock.existsProcess.return_value = S_OK( True )
    plist_mock.getCSPath.return_value = 'some_dir/some_cs_path/whizardv874.tgz'
    self.whiz._processlist = plist_mock
    self.whiz._generatormodels.models['sm'] = True
    assertDiracSucceeds( self.whiz._checkConsistency(), self )

  def test_checkconsistency_noenergy( self ):
    assertDiracFailsWith( self.whiz._checkConsistency(), 'energy not set',
                          self )

  def test_checkconsistency_nonbevts( self ):
    self.whiz.energy = 'blabla'
    assertDiracFailsWith( self.whiz._checkConsistency(),
                          'number of events not set', self )

  def test_checkconsistency_noprocess( self ):
    self.whiz.energy = 'blabla'
    self.whiz.numberOfEvents = 13
    assertDiracFailsWith( self.whiz._checkConsistency(), 'process not defined',
                          self )

  def test_checkconsistency_noprocesslist( self ):
    self.whiz.energy = 'blabla'
    self.whiz.numberOfEvents = 13
    self.whiz.eventType = 'mytype'
    assertDiracFailsWith( self.whiz._checkConsistency(),
                          'process list was not given', self )

  def test_checkconsistency_checkfields_fails( self ):
    self.whiz.setFullParameterDict( { 'something' : 'something_else' } )
    assertDiracFailsWith( self.whiz._checkConsistency(),
                          'element something is not in the allowed parameters',
                          self )

  def test_checkconsistency_noevttype( self ):
    self.whiz.setFullParameterDict( { 'process_input' :
                                      { 'process_id' : '' } } )
    assertDiracFailsWith( self.whiz._checkConsistency(),
                          'event type not specified', self )

  def test_checkconsistency_energyzero( self ):
    self.whiz.setFullParameterDict( { 'process_input' :
                                      { 'process_id' : '',
                                        'sqrts' : 0 } } )
    self.whiz.eventType = 'some_test_evttype'
    self.whiz.energy = 0
    assertDiracFailsWith( self.whiz._checkConsistency(), 'energy set to 0',
                          self )

  def test_checkconsistency_nbevts_zero( self ):
    self.whiz.setFullParameterDict( { 'process_input' :
                                      { 'process_id' : '',
                                        'sqrts' : 0 },
                                      'simulation_input' :
                                      { 'n_events' : 0 } } )
    self.whiz.eventType = 'some_test_evttype'
    self.whiz.energy = 341
    self.whiz.numberOfEvents = 0
    assertDiracFailsWith( self.whiz._checkConsistency(),
                          'number of events set to 0', self )

  def test_checkconsistency_genlevelcut_invalid( self ):
    self.whiz.setFullParameterDict( { 'process_input' :
                                      { 'process_id' : '',
                                        'sqrts' : 0 },
                                      'simulation_input' :
                                      { 'n_events' : 0 } } )
    self.whiz.eventType = 'some_test_evttype'
    self.whiz.energy = 341
    self.whiz.numberOfEvents = 1235
    self.whiz._processlist = Mock()
    self.whiz.setGeneratorLevelCuts( { 'something' : True } )
    assertDiracFailsWith( self.whiz._checkConsistency(),
                          'type of True is not a list', self )

  def test_checkconsistency_noglobalevt( self ):
    self.whiz.setFullParameterDict(
      { 'process_input' :
        { 'process_id' : 'process_1 some_test_process more_stuff',
          'sqrts' : 0 }, 'simulation_input' : { 'n_events' : 0 } } )
    self.whiz.setEvtType( 'some_test_evttype other_types ignore_us' )
    self.whiz.setGlobalEvtType( '' )
    self.whiz.energy = 341
    self.whiz.numberOfEvents = 1235
    self.whiz._processlist = Mock()
    self.whiz.setGeneratorLevelCuts( { 'some_test_process' : [ 'testlist' ] } )
    assertDiracFailsWith( self.whiz._checkConsistency(),
                          'global name must be defined', self )

  def test_checkconsistency_process_not_found( self ):
    self.whiz.setFullParameterDict(
      { 'process_input' :
        { 'process_id' : 'process_1 some_test_process more_stuff',
          'sqrts' : 0 }, 'simulation_input' : { 'n_events' : 0 } } )
    self.whiz.setEvtType( 'some_test_evttype other_types ignore_us' )
    self.whiz.setGlobalEvtType( 'myGlobalTestEvent' )
    self.whiz.energy = 341
    self.whiz.numberOfEvents = 1235
    exists_dict = { 'process_1' : True, 'some_test_process' : False }
    plist_mock = Mock()
    plist_mock.existsProcess.side_effect = lambda process : S_OK(
      exists_dict[process] )
    self.whiz._processlist = plist_mock
    self.whiz.setGeneratorLevelCuts( { 'some_test_process' : [ 'testlist' ] } )
    assertDiracFailsWith( self.whiz._checkConsistency(),
                          'process some_test_process does not exist', self )

  def test_checkconsistency_process_version_incompatible( self ):
    self.whiz.setFullParameterDict(
      { 'process_input' :
        { 'process_id' : 'process_1 some_test_process more_stuff',
          'sqrts' : 0 }, 'simulation_input' : { 'n_events' : 0 } } )
    self.whiz.setEvtType( 'some_test_evttype other_types ignore_us' )
    self.whiz.setGlobalEvtType( 'myGlobalTestEvent' )
    self.whiz.energy = 341
    self.whiz.version = 'v248'
    self.whiz.numberOfEvents = 1235
    exists_dict = { 'process_1' : True, 'some_test_process' : True }
    plist_mock = Mock()
    plist_mock.existsProcess.side_effect = lambda process : S_OK(
      exists_dict[process] )
    plist_mock.getCSPath.side_effect = [ 'some_dir/whizardv248.tgz',
                                         'whizard418.tar.gz' ]
    self.whiz._processlist = plist_mock
    self.whiz.setGeneratorLevelCuts( { 'some_test_process' : [ 'testlist' ] } )
    assertDiracFailsWith( self.whiz._checkConsistency(),
                          'not available in the same whizard version', self )

  def test_checkconsistency_unknownmodel( self ):
    self.whiz.setFullParameterDict(
      { 'process_input' :
        { 'process_id' : 'process_1 some_test_process more_stuff',
          'sqrts' : 0 }, 'simulation_input' : { 'n_events' : 0 } } )
    self.whiz.setEvtType( 'some_test_evttype other_types ignore_us' )
    self.whiz.setGlobalEvtType( 'myGlobalTestEvent' )
    self.whiz.energy = 341
    self.whiz.model = 'myObscureModel'
    self.whiz.version = '248'
    self.whiz.numberOfEvents = 1235
    exists_dict = { 'process_1' : True, 'some_test_process' : True,
                    'more_stuff' : True }
    plist_mock = Mock()
    plist_mock.existsProcess.side_effect = lambda process : S_OK(
      exists_dict[process] )
    plist_mock.getCSPath.return_value = 'some_dir/whizard248.tgz'
    self.whiz._generatormodels.models = {}
    self.whiz._processlist = plist_mock
    self.whiz.setGeneratorLevelCuts( { 'some_test_process' : [ 'testlist' ] } )
    assertDiracFailsWith( self.whiz._checkConsistency(),
                          'unknown model myobscuremodel', self )

  def test_checkconsistency_illegaloutputfile( self ):
    self.whiz.setFullParameterDict( { 'integration_input' : {} } )
    self.whiz.setEvtType( 'some_type' )
    self.whiz.globalEventType = ''
    self.whiz.energy = 341
    self.whiz.model = ''
    self.whiz.version = '248'
    self.whiz.outputFile = 'some_dir/more_dirs/output.file'
    self.whiz.numberOfEvents = 1235
    exists_dict = { 'some_type' : True }
    plist_mock = Mock()
    plist_mock.existsProcess.side_effect = lambda process : S_OK(
      exists_dict[process] )
    plist_mock.getCSPath.return_value = 'some_dir/whizard248.tgz'
    self.whiz._generatormodels.models = {}
    self.whiz._processlist = plist_mock
    self.whiz.setGeneratorLevelCuts( { 'some_test_process' : [ 'testlist' ] } )
    assertDiracFailsWith( self.whiz._checkConsistency(),
                          'is a file name, not a path', self )

  def test_checkconsistency_unknownparam( self ):
    self.whiz.setFullParameterDict( {} )
    self.whiz.setEvtType( 'some_type' )
    self.whiz.setParameterDict( { 'illegal_key' : 'illegal_value' } )
    self.whiz.globalEventType = ''
    self.whiz.energy = 341
    self.whiz.model = ''
    self.whiz.version = '248'
    self.whiz.outputFile = ''
    self.whiz.numberOfEvents = 1235
    exists_dict = { 'some_type' : True }
    plist_mock = Mock()
    plist_mock.existsProcess.side_effect = lambda process : S_OK(
      exists_dict[process] )
    plist_mock.getCSPath.return_value = 'some_dir/whizard248.tgz'
    self.whiz._generatormodels.models = {}
    self.whiz._jobtype = 'User'
    self.whiz._processlist = plist_mock
    self.whiz.setGeneratorLevelCuts( { 'some_test_process' : [ 'testlist' ] } )
    assertDiracFailsWith( self.whiz._checkConsistency(),
                          'unknown parameter illegal_key', self )

  def test_checkconsistency_cornercases( self ):
    self.whiz.setFullParameterDict( {} )
    self.whiz.setEvtType( 'some_type' )
    self.whiz.setParameterDict( { 'PNAME1' : 'mytestpname' } )
    self.whiz.globalEventType = ''
    self.whiz.energy = 341
    self.whiz.model = ''
    self.whiz.version = '248'
    self.whiz.outputFile = 'output.file'
    self.whiz.numberOfEvents = 1235
    exists_dict = { 'some_type' : True }
    plist_mock = Mock()
    plist_mock.existsProcess.side_effect = lambda process : S_OK(
      exists_dict[process] )
    plist_mock.getCSPath.return_value = 'some_dir/whizard248.tgz'
    self.whiz._generatormodels.models = {}
    self.whiz._jobtype = 'User'
    self.whiz._processlist = plist_mock
    self.whiz.setGeneratorLevelCuts( { 'some_test_process' : [ 'testlist' ] } )
    assertDiracSucceeds( self.whiz._checkConsistency(), self )

  def test_applicationmodule( self ):
    pass








