#!/usr/bin/env python
"""Test the WhizardOption class"""

import unittest
from xml.etree.ElementTree import fromstring
from mock import mock_open, patch, MagicMock as Mock

from ILCDIRAC.Core.Utilities.WhizardOptions import WhizardOptions, getDict, main
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertDiracSucceeds, assertDiracSucceedsWith_equals, assertEqualsXmlTree, assertMockCalls
from ILCDIRAC.Tests.Utilities.FileUtils import FileUtil

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Core.Utilities.WhizardOptions'

class TestWhizardOptions( unittest.TestCase ): #pylint: disable=too-many-public-methods
  """ Test the different methods of the class
  """

  def setUp( self ):
    self.whop = WhizardOptions()

  def test_getdict( self ):
    result = getDict()
    assertDiracSucceedsWith_equals( result, { 'process_input' : {}, 'integration_input' : {},
                                              'simulation_input' : {}, 'diagnostics_input' : {},
                                              'beam_input_1' : {}, 'beam_input_2' : {} }, self )

  def test_getinputfiles( self ):
    self.whop.paramdict = getDict()['Value']
    self.whop.paramdict['process_input']['input_file'] = 'mytestfile.input'
    old_state = self.whop.paramdict.copy()
    self.assertIsNone( self.whop.getInputFiles( 'model' ) )
    assertEqualsImproved( self.whop.paramdict, old_state, self )

  def test_toxml( self ):
    with patch('__builtin__.open', mock_open()) as open_mock:
      assertDiracSucceeds( self.whop.toXML( 'mytestOutputFile.xml' ), self )
      open_mock.assert_any_call( 'mytestOutputFile.xml', 'wb' )
      self.assertTrue( len(open_mock().write.mock_calls) > 30 )

  def test_getmainfields( self ):
    result = self.whop.getMainFields()
    assertDiracSucceedsWith_equals( result, [ 'process_input', 'integration_input', 'simulation_input',
                                              'diagnostics_input', 'parameter_input', 'beam_input_1',
                                              'beam_input_2' ], self )

  def test_getoptionsforfield( self ):
    assertDiracSucceedsWith_equals( self.whop.getOptionsForField( 'process_input' ),
                                    [ 'process_id', 'cm_frame', 'sqrts', 'luminosity', 'polarized_beams',
                                      'structured_beams', 'beam_recoil', 'recoil_conserve_momentum',
                                      'filename','directory', 'input_file', 'input_slha_format' ], self )
    assertDiracSucceedsWith_equals( self.whop.getOptionsForField( 'parameter_input' ),
                                    ['GF', 'mZ', 'mW', 'mH', 'alphas', 'me', 'mmu', 'mtau', 'ms', 'mc', 'mb',
                                     'mtop', 'wtop', 'wZ', 'wW', 'wH', 'vckm11', 'vckm12', 'vckm13',
                                     'vckm21', 'vckm22', 'vckm23', 'vckm31', 'vckm32', 'vckm33', 'khgaz',
                                     'khgaga', 'khgg'], self )

  def test_getoptionsforfield_fails( self ):
    assertDiracFailsWith( self.whop.getOptionsForField( 'testFieldDoesntExist' ),
                          'field testfielddoesntexist does not exist', self )

  def test_whgetasdict( self ):
    expected_dict = { 'parameter_input' : { 'vckm21' : '-0.2271', 'vckm23': '0.04221', 'vckm22': '0.97296',
                                            'mtop': '174', 'mtau': '1.777', 'vckm33': '0.99910',
                                            'khgaz': '1.000', 'khgg': '1.000', 'mZ': '91.1882',
                                            'wtop': '1.523', 'mmu': '0.1066', 'mH': '120', 'khgaga': '1.000',
                                            'vckm32': '-0.04161', 'GF': '1.16639E-5', 'mW': '80.419',
                                            'vckm31': '0.00814', 'vckm11': '0.97383', 'vckm12': '0.2272',
                                            'vckm13': '0.00396', 'me': '0.', 'alphas': '0.1178',
                                            'mc': '0.54', 'mb': '2.9', 'wH': '0.3605E-02', 'wW': '2.049',
                                            'ms': '0.', 'wZ': '2.443'},
                      'integration_input': { 'exchange_lines': '3', 'max_bins': '20', 'read_cuts_file': '',
                                             'write_grids': 'T', 'min_bins': '3', 'threshold_mass': '-10',
                                             'double_off_shell_branchings': 'T', 'extra_off_shell_lines': '1',
                                             'phase_space_only': 'F', 'use_efficiency': 'F', 'seed': '',
                                             'reset_seed_each_process': 'F', 'accuracy_goal': '0',
                                             'default_energy_cut': '10', 'single_off_shell_decays': 'T',
                                             'write_all_grids_file': '', 'default_jet_cut': '10',
                                             'massive_fsr': 'T', 'write_phase_space_channels_file': '',
                                             'write_all_grids': 'F', 'weights_power': '0.25',
                                             'default_mass_cut': '4', 'read_grids_force': 'T',
                                             'write_default_cuts_file': '', 'time_limit_adaptation': '0',
                                             'efficiency_goal': '100', 'splitting_depth': '1',
                                             'azimuthal_dependence': 'F', 'read_model_file': '',
                                             'double_off_shell_decays': 'F', 'generate_phase_space': 'T',
                                             'read_grids': 'F', 'write_phase_space_file': '',
                                             'read_phase_space': 'T', 'write_grids_file': '',
                                             'threshold_mass_t': '-10', 'read_phase_space_file': '',
                                             'min_calls_per_channel': '0', 'read_grids_file': '',
                                             'calls': '1 50000 10 50000 1 1500000', 'use_equivalences': 'T',
                                             'single_off_shell_branchings': 'T', 'show_deleted_channels': 'F',
                                             'write_grids_raw': 'F', 'user_cut_mode': '0',
                                             'min_calls_per_bin': '10', 'stratified': 'T',
                                             'off_shell_lines': '1', 'user_weight_mode': '0',
                                             'read_grids_raw': 'F', 'default_q_cut': '4'},
                      'simulation_input': { 'read_events': 'F', 'min_file_count': '1', 'shower_t_min': '1.0',
                                            'unweighted': 'T', 'keep_beam_remnants': 'T', 'recalculate': 'F',
                                            'shower_md': '0.330', 'shower_mc': '1.5', 'shower_mb': '4.8',
                                            'write_events_raw_file': '', 'n_calls': '0',
                                            'fragmentation_method': '3', 'shower_alpha_s': '0.2',
                                            'fragment': 'T', 'shower_mu': '0.330', 'shower_ms': '0.500',
                                            'write_events_file': '', 'write_events': 'T',
                                            'bytes_per_file': '0', 'n_events_warmup': '0',
                                            'write_events_raw': 'F', 'safety_factor': '1',
                                            'guess_color_flow': 'F', 'shower_running_alpha_s': 'F',
                                            'keep_initials': 'T',
                                            'pythia_parameters': 'PMAS(25,1)=120.; PMAS(25,2)=0.3605E-02; MSTU(22)=20 ;MSTJ(28)=2 ;PARJ(21)=0.40000;PARJ(41)=0.11000; PARJ(42)=0.52000; PARJ(81)=0.25000; PARJ(82)=1.90000; MSTJ(11)=3; PARJ(54)=-0.03100; PARJ(55)=-0.00200;PARJ(1)=0.08500; PARJ(3)=0.45000; PARJ(4)=0.02500; PARJ(2)=0.31000; PARJ(11)=0.60000; PARJ(12)=0.40000; PARJ(13)=0.72000;PARJ(14)=0.43000; PARJ(15)=0.08000; PARJ(16)=0.08000; PARJ(17)=0.17000; MSTP(3)=1;',
                                            'read_events_force': 'T', 'write_events_format': '20',
                                            'events_per_file': '5000000', 'shower_nf': '5',
                                            'pythia_processes': '', 'read_events_raw_file': '',
                                            'user_fragmentation_mode': '0', 'normalize_weight': 'T',
                                            'max_file_count': '999', 'write_weights_file': '', 'shower': 'F',
                                            'n_events': '0', 'shower_lambda': '0.29', 'write_weights': 'F'},
                      'diagnostics_input': { 'show_event': 'F', 'time_limit': '0', 'screen_diagnostics': 'F',
                                             'show_histograms': 'F', 'screen_events': 'F',
                                             'screen_histograms': 'F', 'plot_width': '130',
                                             'plot_height': '90', 'show_overflow': 'F', 'show_cuts': 'T',
                                             'plot_history': 'T', 'slha_ignore_errors': 'F',
                                             'show_excess': 'T', 'show_results': 'T', 'show_history': 'T',
                                             'show_pythia_statistics': 'T', 'warn_empty_channel': 'F',
                                             'catch_signals': 'T', 'write_logfile_file': '',
                                             'show_phase_space': 'F', 'write_logfile': 'T',
                                             'slha_rewrite_input': 'T', 'show_histories': 'F',
                                             'chattiness': '4', 'show_pythia_initialization': 'T',
                                             'plot_grids_channels': '', 'plot_grids_logscale': '10',
                                             'show_input': 'T', 'read_analysis_file': '',
                                             'show_pythia_banner': 'T', 'plot_excess': 'T',
                                             'show_weights': 'T'},
                      'process_input': {'polarized_beams': 'T', 'recoil_conserve_momentum': 'F',
                                        'input_file': '', 'sqrts': '3000', 'luminosity': '0',
                                        'input_slha_format': 'F', 'filename': 'whizard',
                                        'structured_beams': 'T', 'process_id': '', 'cm_frame': 'T',
                                        'directory': '', 'beam_recoil': 'F'},
                      'beam_input_1': {'ISR_LLA_order': '3', 'energy': '0', 'EPA_on': 'F',
                                       'particle_code': '0', 'ISR_Q_max': 'sqrts', 'angle': '0',
                                       'ISR_on': 'T', 'polarization': '0.0 0.0', 'EPA_alpha': '0.0072993',
                                       'ISR_map': 'T', 'particle_name': 'e1', 'USER_spectrum_on': 'T',
                                       'EPA_x1': '0', 'EPA_x0': '0', 'direction': '0 0 0',
                                       'ISR_alpha': '0.0072993', 'EPA_Q_max': '4', 'ISR_m_in': '0.000511',
                                       'USER_spectrum_mode': '11', 'EPA_m_in': '0.000511',
                                       'vector_polarization': 'F', 'EPA_map': 'T', 'EPA_mX': '4'},
                      'beam_input_2': {'ISR_LLA_order': '3', 'energy': '0', 'EPA_on': 'F',
                                       'particle_code': '0', 'ISR_Q_max': 'sqrts', 'angle': '0',
                                       'ISR_on': 'T', 'polarization': '0.0 0.0', 'EPA_alpha': '0.0072993',
                                       'ISR_map': 'T', 'particle_name': 'E1', 'USER_spectrum_on': 'T',
                                       'EPA_x1': '0', 'EPA_x0': '0', 'direction': '0 0 0',
                                       'ISR_alpha': '0.0072993', 'EPA_Q_max': '4', 'ISR_m_in': '0.000511',
                                       'USER_spectrum_mode': '-11', 'EPA_m_in': '0.000511',
                                       'vector_polarization': 'F', 'EPA_map': 'T', 'EPA_mX': '4' } }
    result = self.whop.getAsDict()
    assertDiracSucceedsWith_equals( result, expected_dict, self )

  def test_checkfields( self ):
    self.whop.whizardxml = fromstring(
      '<whizard> <process_input> <mytestelement type="floatarray"> </mytestelement> <test_bool type="T/F"> </test_bool> <test_bool2 type="T/F"> </test_bool2> <test_integer type="integer"> </test_integer> </process_input> <test_superelem> <test_float type="float"> </test_float> <test_float2 type="float"> </test_float2> <test_string type="string"> </test_string> <test_ignoreme type="invalid_type"></test_ignoreme> </test_superelem> </whizard>' )
    assertDiracSucceeds( self.whop.checkFields(
      { 'process_input' : { 'mytestelement' : '842 021.2 123 98724 81', 'test_bool' : 'T', 'test_bool2' : 'F',
                            'test_integer' : 8492 },
        'test_superelem' : { 'test_float' : 824.2, 'test_float2' : 98421, 'test_string' : 'oijrsg' }
      } ), self )

  def test_checkfields_missing_field( self ):
    assertDiracFailsWith( self.whop.checkFields(
      { 'parameter_input' : {}, 'nonexistent_element_findandtestme' : True } ),
                          'Element nonexistent_element_findandtestme is not in the allowed parameters', self )

  def test_checkfields_missing_subfield( self ):
    assertDiracFailsWith( self.whop.checkFields(
      { 'parameter_input' : { 'nonexistent_element_findandtestme' : True } } ),
                          'key parameter_input/nonexistent_element_findandtestme is not in the allowed parameters',
                          self )

  def test_checkfields_fake_float( self ):
    self.whop.whizardxml = fromstring(
      '<whizard> <process_input> <mytestelement type="float"> </mytestelement> </process_input> </whizard>' )
    assertDiracFailsWith( self.whop.checkFields(
      { 'process_input' : { 'mytestelement' : True } } ), 'process_input/mytestelement should be a float',
                          self )

  def test_checkfields_fake_bool( self ):
    self.whop.whizardxml = fromstring(
      '<whizard> <process_input> <mytestelement type="T/F"> </mytestelement> </process_input> </whizard>' )
    assertDiracFailsWith( self.whop.checkFields(
      { 'process_input' : { 'mytestelement' : 125311 } } ),
                          "process_input/mytestelement should be either 'T' or 'F'", self )

  def test_checkfields_fake_integer( self ):
    self.whop.whizardxml = fromstring(
      '<whizard> <process_input> <mytestelement type="integer"> </mytestelement> </process_input> </whizard>' )
    assertDiracFailsWith( self.whop.checkFields(
      { 'process_input' : { 'mytestelement' : 'mystring' } } ),
                          'process_input/mytestelement should be an integer', self )

  def test_checkfields_fake_string( self ):
    self.whop.whizardxml = fromstring(
      '<whizard> <process_input> <mytestelement type="string"> </mytestelement> </process_input> </whizard>' )
    assertDiracFailsWith( self.whop.checkFields(
      { 'process_input' : { 'mytestelement' : True } } ),
                          'process_input/mytestelement should be a string', self )

  def test_checkfields_fake_floatarray( self ):
    self.whop.whizardxml = fromstring(
      '<whizard> <process_input> <mytestelement type="floatarray"> </mytestelement> </process_input> </whizard>' )
    assertDiracFailsWith( self.whop.checkFields(
      { 'process_input' : { 'mytestelement' : 892354.3 } } ),
                          'process_input/mytestelement should be a string with spaces', self )

  def test_checkfields_empty_floatarray( self ):
    self.whop.whizardxml = fromstring(
      '<whizard> <process_input> <mytestelement type="floatarray"> </mytestelement> </process_input> </whizard>' )
    assertDiracFailsWith( self.whop.checkFields(
      { 'process_input' : { 'mytestelement' : '' } } ),
                          'process_input/mytestelement should be a string with spaces', self )

  def test_checkfields_floatarray_len1( self ):
    self.whop.whizardxml = fromstring(
      '<whizard> <process_input> <mytestelement type="floatarray"> </mytestelement> </process_input> </whizard>' )
    assertDiracFailsWith( self.whop.checkFields(
      { 'process_input' : { 'mytestelement' : '123' } } ),
                          'process_input/mytestelement should be a string with spaces', self )

  def test_towhizarddotin( self ):
    self.whop.whizardxml = fromstring(
      '<whizard> <process_input> <mytestelement type="floatarray" value="sqrts"> </mytestelement> <test_bool type="T/F" value="000"> </test_bool> <test_bool2 type="T/F" value="0.0.0"> </test_bool2> <test_integer type="integer" value="0..0..0"> </test_integer> </process_input> <lalabeam_input1238> <test_float type="float" value="dontchangeanything"> </test_float> <test_string type="string" value="some_teststring"> </test_string> <test_noval></test_noval> </lalabeam_input1238> </whizard>' )
    with patch('%s.open' % MODULE_NAME, mock_open()) as open_mock:
      result = self.whop.toWhizardDotIn( 'mytestfile.xml' )
      assertDiracSucceeds( result, self )
      open_mock.assert_any_call( 'mytestfile.xml', 'w' )
      assertMockCalls( open_mock().write, [ '&process_input\n test_bool = 0 0 0\n test_bool2 = 0.0 0.0\n test_integer = \n 1 20000\n 10 20000\n 1 20000\n/\n&beam_input\n test_float = dontchangeanything\n test_string = "some_teststring"\n test_noval = None\n/', '\n'], self )

  def test_fromwhizarddotin( self ):
    self.whop.paramdict = {}
    self.whop.whizardxml = fromstring(
      '<whizardxml> <beam_input_1> <beam_intensity type="float"> </beam_intensity> <beam_density type="0/1/2/3"> </beam_density> </beam_input_1> <beam_input_2> <FcoolTtestkey type="string"> </FcoolTtestkey> </beam_input_2> <myotherkey> </myotherkey> <testkey> <testcurkey type="string"> </testcurkey> </testkey> <lasttestkey> <myfloatarr type="floatarray"> </myfloatarr> <lasttestvalue type="T/F"> </lasttestvalue> </lasttestkey> </whizardxml>' )
    expected_tree = fromstring(
      '<whizardxml> <beam_input_1> <beam_intensity type="float" value="84.2"> </beam_intensity> <beam_density type="0/1/2/3" value="0"> </beam_density> </beam_input_1> <beam_input_2> <FcoolTtestkey type="string" value="TmyFstring"> </FcoolTtestkey> </beam_input_2> <myotherkey> </myotherkey> <testkey> <testcurkey type="string" value="teststring_dontloseme."> </testcurkey> </testkey> <lasttestkey> <myfloatarr type="floatarray" value="943 0.1 01.2 9024.4"> </myfloatarr> <lasttestvalue type="T/F" value="F"> </lasttestvalue> </lasttestkey> </whizardxml>' )
    expected_tree.find( 'beam_input_1/beam_intensity' ).attrib['value'] = 84.2
    expected_tree.find( 'beam_input_1/beam_density' ).attrib['value'] = 0
    file_contents = [ [ '     !initial Comment, ignore this line   ', '/  Other comment.               ',
                        '                                 ', 'ignoreThisLineTOO', '&beam_input',
                        'beam_intensity=84.2', 'beam_density=0', '&myotherkey', '&testkey',
                        'testcurkey="teststring_dontloseme."', '&beam_input  ',
                        '  FcoolTtestkey  =  "TmyFstring"   ', '&lasttestkey',
                        'myfloatarr= 943 0.1 01.2 9024.4', 'lasttestvalue=F' ] ]
    handles = FileUtil.getMultipleReadHandles( file_contents )
    with patch('%s.open' % MODULE_NAME, mock_open(), create=True) as mo:
      mo.side_effect = (h for h in handles)
      result = self.whop.fromWhizardDotIn( 'filename.txt' )
      assertDiracSucceeds( result, self )
      assertEqualsXmlTree( result['Value'], expected_tree, self )

  def test_main( self ):
    print_mock = Mock()
    pprint_mock = Mock(return_value=print_mock)
    file_contents = [ [ '     !initial Comment, ignore this line   ', '/  Other comment.               ',
                        '                                 ', 'ignoreThisLineTOO', '&process_input',
                        'process_id=myprocessid', 'cm_frame=F', '&integration_input', '&simulation_input',
                        'unweighted=F', '&diagnostics_input  ',
                        '  write_logfile_file  =  "my_diagnostics_logfile.txt"   ' ] ]
    handles = FileUtil.getMultipleReadHandles( file_contents )
    with patch('sys.argv', [ 'scriptname', 'filename.xml' ]), \
         patch('%s.open' % MODULE_NAME, mock_open(), create=True ) as mo, \
         patch.dict('sys.modules', { 'pprint' : pprint_mock }):
      mo.side_effect = (h for h in handles)
      assertEqualsImproved( main(), 0, self )
      mo.assert_called_once_with( 'filename.xml', 'r' )
      #if not running_on_docker():
      #  xml_dict = pprint_mock.PrettyPrinter.return_value.pprint.call_args[0][0]
      #  assertEqualsImproved( xml_dict[ 'process_input' ][ 'process_id' ], 'myprocessid', self )
      #  assertEqualsImproved( xml_dict[ 'process_input' ][ 'cm_frame' ], 'F', self )
      #  assertEqualsImproved( xml_dict[ 'simulation_input' ][ 'unweighted' ], 'F', self )
      #  assertEqualsImproved( xml_dict[ 'diagnostics_input' ][ 'write_logfile_file' ],
      #                        'my_diagnostics_logfile.txt', self )

  def test_main_other_model( self ):
    print_mock = Mock()
    pprint_mock = Mock(return_value=print_mock)
    file_contents = [ [ '     !initial Comment, ignore this line   ', '/  Other comment.               ',
                        '                                 ', 'ignoreThisLineTOO', '&process_input',
                        'process_id=myprocessid', 'cm_frame=F', '&integration_input', '&simulation_input',
                        'unweighted=F', '&diagnostics_input  ',
                        '  write_logfile_file  =  "my_diagnostics_logfile.txt"   ' ] ]
    handles = FileUtil.getMultipleReadHandles( file_contents )
    with patch('sys.argv', [ 'scriptname', 'filename.xml', 'mymodel' ]), \
         patch('%s.open' % MODULE_NAME, mock_open(), create=True ) as mo, \
         patch.dict('sys.modules', { 'pprint' : pprint_mock }):
      mo.side_effect = (h for h in handles)
      assertEqualsImproved( main(), 0, self )
      mo.assert_called_once_with( 'filename.xml', 'r' )
      #if not running_on_docker():
      #  xml_dict = pprint_mock.PrettyPrinter.return_value.pprint.call_args[0][0]
      #  assertEqualsImproved( xml_dict[ 'process_input' ][ 'process_id' ], 'myprocessid', self )
      #  assertEqualsImproved( xml_dict[ 'process_input' ][ 'cm_frame' ], 'F', self )
      #  assertEqualsImproved( xml_dict[ 'simulation_input' ][ 'unweighted' ], 'F', self )
      #  assertEqualsImproved( xml_dict[ 'diagnostics_input' ][ 'write_logfile_file' ],
      #                        'my_diagnostics_logfile.txt', self )

  def test_main_wrong_type( self ):
    print_mock = Mock()
    pprint_mock = Mock(return_value=print_mock)
    file_contents = [ [ '&process_input', 'process_id=myprocessid', 'cm_frame=02.2', '&integration_input',
                        '&simulation_input', 'unweighted=8924', '&diagnostics_input  ',
                        '  write_logfile_file  =  "my_diagnostics_logfile.txt"   ' ] ]
    handles = FileUtil.getMultipleReadHandles( file_contents )
    with patch('sys.argv', [ 'scriptname', 'filename.xml', 'mymodel' ]), \
         patch('%s.open' % MODULE_NAME, mock_open(), create=True ) as mo, \
         patch.dict('sys.modules', { 'pprint' : pprint_mock }):
      mo.side_effect = (h for h in handles)
      assertEqualsImproved( main(), 1, self )
      mo.assert_called_once_with( 'filename.xml', 'r' )

  def test_main_wrong_paraminput( self ):
    print_mock = Mock()
    pprint_mock = Mock(return_value=print_mock)
    file_contents = [ [ '&process_input', 'parameter_input=123', 'process_id=myprocessid', 'cm_frame=F',
                        '&integration_input', '&simulation_input', 'unweighted=F', '&diagnostics_input  ',
                        '  write_logfile_file  =  "my_diagnostics_logfile.txt"   ' ] ]
    handles = FileUtil.getMultipleReadHandles( file_contents )
    with patch('sys.argv', [ 'scriptname', 'filename.xml', 'mymodel' ]), \
         patch('%s.open' % MODULE_NAME, mock_open(), create=True ) as mo, \
         patch.dict('sys.modules', { 'pprint' : pprint_mock }):
      mo.side_effect = (h for h in handles)
      assertEqualsImproved( main(), 1, self )
      mo.assert_called_once_with( 'filename.xml', 'r' )


class TestWhOptCustomTree( unittest.TestCase ):
  """ Test critical methods with a custom xml tree
  """

  DEFAULT_TREE = '<mytree>    <testchild1 type="asd" value="10">        <testoption type="test" value="123"></testoption>        <other_option></other_option>        <lastoneIpromise></lastoneIpromise>    </testchild1>    <dontforgetme></dontforgetme>    <onlychild type="floatarray" value="asd" some_other_field="test"></onlychild></mytree>'

  def setUp( self ):
    self.whop = WhizardOptions()
    self.whop.paramdict = { 'testchild1' : { 'testoption' : '123', 'other_option' : None,
                                             'lastoneIpromise' : None }, 'dontforgetme' : {},
                            'onlychild' : {} }
    self.whop.whizardxml = fromstring( TestWhOptCustomTree.DEFAULT_TREE )

  def test_getoptions_nontrivial( self ):
    result = self.whop.getOptionsForField( 'testchild1' )
    assertDiracSucceedsWith_equals( result, [ 'testoption', 'other_option', 'lastoneIpromise' ], self )

  def test_changeandreturn_nontrivial( self ):
    with self.assertRaises( KeyError ) as ke:
      self.whop.changeAndReturn( {} )
    assertEqualsImproved( ke.exception.message, 'type', self )

  def test_changeandreturn_updatechecks( self ):
    missing_values = { 'testchild1' : [ 'other_option', 'lastoneIpromise' ] }
    for key, list_of_subelems in missing_values.iteritems():
      for subelem in list_of_subelems:
        self.whop.whizardxml.find( '%s/%s' % ( key, subelem ) ).attrib['type'] = 'default'
    result = self.whop.changeAndReturn( { 'testchild1' : { 'lastoneIpromise' : 242 } } )
    assertDiracSucceeds( result, self )
    root = result[ 'Value' ]
    assertEqualsImproved( root.find( 'testchild1/lastoneIpromise' ).attrib['value'], 242, self )

  def test_getasdict_nontrivial( self ):
    missing_values = { 'testchild1' : [ 'other_option', 'lastoneIpromise' ], 'dontforgetme' : [],
                       'onlychild' : [] }
    root = self.whop.whizardxml
    for key, list_of_subelems in missing_values.iteritems():
      for subelem in list_of_subelems:
        root.find( '%s/%s' % ( key, subelem ) ).attrib['value'] = 'default'
    result = self.whop.getAsDict()
    assertDiracSucceedsWith_equals( result, { 'dontforgetme' : {}, 'onlychild' : {},
                                              'testchild1': { 'lastoneIpromise' : 'default',
                                                              'other_option' : 'default',
                                                              'testoption' : '123' } }, self )
