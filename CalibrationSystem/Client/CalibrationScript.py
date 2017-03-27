#! /usr/bin/env python
"""
The calibration script performs the actual calibration on the worker nodes.
"""

#FIXME:add the comments from the bash script back

import errno
import os
import shutil
import stat
import sys
import subprocess

#Parameters etc.

ILCSOFT_PATH = "/cvmfs/clicdp.cern.ch/iLCSoft/builds/2017-02-17/x86_64-slc6-gcc62-opt"
PANDORA_ANALYSIS_PATH = "%s/PandoraAnalysis/HEAD/" % ILCSOFT_PATH  # change if not HEAD
SOURCE_FILE = "%s/init_ilcsoft.sh" % ILCSOFT_PATH
GEOMETRY_MODEL_NAME = 'CLIC_o3_v07'
SLCIO_PATH = "/afs/cern.ch/user/j/jebbing/particles/%s/" % GEOMETRY_MODEL_NAME
#Derive slcio file locations
GAMMA_PATH = "%s/gamma/10/" % SLCIO_PATH
MUON_PATH = "%s/mu-/10/" % SLCIO_PATH
KAON_PATH = "%s/kaon0L/50/" % SLCIO_PATH
SLCIO_FORMAT = ".*_([0-9]+).slcio"
OUTPUT_PATH = "%s/output/" % SLCIO_PATH
CALIBR_RESULTS_FILE = '%s/calib.%s.txt' % (OUTPUT_PATH, GEOMETRY_MODEL_NAME)
HCAL_ENDCAP_TIME_WINDOW_MAX = 10
HCAL_BARREL_TIME_WINDOW_MAX = 10
ECAL_ENDCAP_TIME_WINDOW_MAX = 20
ECAL_BARREL_TIME_WINDOW_MAX = 20
MHHHE = 1
NUMBER_HCAL_LAYERS = 60
PANDORA_SETTINGS_FILE = "PandoraSettingsDefault.xml"
HADRONIC_SCALE_SETTING_PANDORA = "CSM"  # TEM or CSM
DIGITISATION_ACCURACY = 0.05                       # Fractional accuracy targetted by digitisation stage
PANDORA_PFA_ACCURACY = 0.005                        # Fractional accuracy targeted by PandoraPFA calibration stage
KAON_L_ENERGY_CALIBRATION = 50
PHOTON_ENERGY_CALIBRATION = 10
KAON_L_ENERGIES = [1, 2, 5, 10, 20, 50, 100]
KAON_L_MASS = 0.497614
KAON_L_KINETIC_ENERGY_CALIBRATION = KAON_L_ENERGY_CALIBRATION - KAON_L_MASS  # FIXME: ACCURACY ENOUGH?
MUON_ENERGY_CALIBRATION = 10
CALIBR_ECAL = 40.17
CALIBR_HCAL_BARREL = 46.59
CALIBR_HCAL_ENDCAP = 49.32
CALIBR_HCAL_OTHER = 57.23
#CalibrHCALBarrel = execute_and_return_output( 'echo "scale=10; ${CalibrHCALBarrel} * 48 / ${numberHCalLayers}" | bc')
# mit genauigkeit 10 bzw 10 nachkommastellen berechne das
#CalibrHCALEndCap = execute_and_return_output( 'echo "scale=10; ${CalibrHCALEndCap} * 48 / ${numberHCalLayers}" | bc' )
# FIXME: Is accuracy enough? (10 significant digits after the comma in original code)
CALIBR_HCAL_BARREL = CALIBR_HCAL_BARREL * 48 / NUMBER_HCAL_LAYERS
CALIBR_HCAL_ENDCAP = CALIBR_HCAL_ENDCAP * 48 / NUMBER_HCAL_LAYERS
ECAL_TO_EM = 1.000
HCAL_TO_EM = 1.000
ECAL_TO_HAD = 0.97
HCAL_TO_HAD = 1.059
ECAL_GEV_TO_MIP = 163.93
HCAL_GEV_TO_MIP = 43.29
MUON_GEV_TO_MIP = 100.0
ECAL_MIPMPV = 0.00015
HCAL_MIPMPV = 0.00004
GEAR_FILE = 'GearOutput.xml'
CALIBRATION_FILE = "%s/Calibration.txt" % OUTPUT_PATH
DD4HEP_COMPACT_XML = ''
PANDORA_SETTINGS_FILE_PATH = ''
PANDORA_LIKELIHOOD_DATAFILE = ''
GAMMA_FILES = ''
MUON_FILES = ''
KAON_FILES = ''
PATH = ''
PYTHON_READ_SCRIPTS = ''
MARLIN_XML = ''
ROOT_FILE_GENERATION = ''
ROOT_FILES = ''
XML_GENERATION = ''
KAON_ROOT_FILES = ''
PHOTON_ROOT_FILES = ''
MUON_ROOT_FILES = ''
HCAL_ENDCAP_MEAN = ''
FRACTIONAL_ERROR_ECAL_MEAN = ''
FRACTIONAL_ERROR_HCAL_BARREL_MEAN = ''
FRACTIONAL_ERROR_HCAL_ENDCAP_MEAN = ''
ECAL_MEAN = ''
ABSORBER_THICKNESS_ENDCAP = 20.0
SCINTILLATOR_THICKNESS_RING = 3.0
ABSORBER_THICKNESS_RING = 20.0
SCINTILLATOR_THICKNESS_ENDCAP = 3.0
FRACTIONAL_EM_ERROR = ''
UPPER_LIMIT = 1 + PANDORA_PFA_ACCURACY
LOWER_LIMIT = 1 - PANDORA_PFA_ACCURACY
HCAL_TO_HAD_FOM = ''
ECAL_TO_HAD_FOM = ''
ECAL_MIP_MPV = ''
HCAL_MIP_MPV = ''
HCAL_TO_HAD_UL = ''
HCAL_TO_HAD_LL = ''
ECAL_TO_HAD_UL = ''
ECAL_TO_HAD_LL = ''

#pylint: disable=too-many-statements,global-statement


def init():
  """ Defines the basic parameters for the calibration

  :returns: nothing
  :rtype: None
  """
  # access outer variables - ugly
  global DD4HEP_COMPACT_XML, PANDORA_SETTINGS_FILE_PATH, PANDORA_LIKELIHOOD_DATAFILE, \
      GAMMA_FILES, MUON_FILES, KAON_FILES, PATH, PYTHON_READ_SCRIPTS, MARLIN_XML, ROOT_FILE_GENERATION, \
      ROOT_FILES, XML_GENERATION, KAON_ROOT_FILES, PHOTON_ROOT_FILES, MUON_ROOT_FILES

  ilcsoft_dir = os.environ.get('ILCSOFT', '')
  source_script(SOURCE_FILE)
  os.environ['PATH'] = '%s/bin:%s' % (PANDORA_ANALYSIS_PATH, os.environ.get('PATH', ''))
  if not os.path.isdir(PANDORA_ANALYSIS_PATH):
    print 'Error! PandoraAnalysis does not exist!'
    exit(1)
  DD4HEP_COMPACT_XML = '%s/CLIC/compact/%s/%s.xml' % (os.environ.get('lcgeo_DIR', ''),
                                                      GEOMETRY_MODEL_NAME, GEOMETRY_MODEL_NAME)
  if not os.path.isfile(DD4HEP_COMPACT_XML):
    print 'Error! Compact XML file at %s not accessible!' % DD4HEP_COMPACT_XML
    exit(1)
  PANDORA_SETTINGS_FILE_PATH = execute_and_return_output(['find', '%s/MarlinPandora/' % ilcsoft_dir,
                                                          '-name', '%s' % PANDORA_SETTINGS_FILE]).rstrip()
  PANDORA_LIKELIHOOD_DATAFILE = execute_and_return_output(['find', '%s/MarlinPandora/' % ilcsoft_dir,
                                                           '-name', 'PandoraLikelihoodData9EBin.xml']).rstrip()
  GAMMA_FILES = execute_and_return_output(
      ['python', 'Xml_Generation/countMatches.py', GAMMA_PATH, SLCIO_FORMAT]).split(' ')
  MUON_FILES = execute_and_return_output(
      ['python', 'Xml_Generation/countMatches.py', MUON_PATH, SLCIO_FORMAT]).split(' ')
  KAON_FILES = execute_and_return_output(
      ['python', 'Xml_Generation/countMatches.py', KAON_PATH, SLCIO_FORMAT]).split(' ')
  if not GAMMA_FILES[0] or not MUON_FILES[0] or not KAON_FILES[0]:
    print 'Error! Some or all of the calibration files are missing!'
    print 'Gamma: %s Muon: %s Kaon %s' % (GAMMA_FILES, MUON_FILES, KAON_FILES)
    exit(1)
  if GAMMA_FILES[2] == GAMMA_FILES[4] or MUON_FILES[2] == MUON_FILES[4] or KAON_FILES[2] == KAON_FILES[4]:
    print 'Warning! File indices for one or more particles are the same, not matched or not existing.'
    print '%s\n%s\n%s' % (GAMMA_FILES, MUON_FILES, KAON_FILES)
    print 'Could mess up ROOT-file generation later! Proceed at your own peril.'
  execute_and_return_output(['convertToGear', 'GearForCLIC', DD4HEP_COMPACT_XML, GEAR_FILE])
  shutil.rmtree('Root_Files')
  shutil.rmtree('Marlin_Xml')
  os.mkdir('Root_Files')
  os.mkdir('Marlin_Xml')
  try:
    os.remove(CALIBRATION_FILE)
  except OSError:
    pass  # it's ok if it doesn't exist
  try:
    os.makedirs(OUTPUT_PATH)
  except OSError as ose:
    if ose.errno == errno.EEXIST and os.path.isdir(OUTPUT_PATH):
      pass  # it's ok if it already exists
  open(CALIBRATION_FILE, 'w').close()
  PATH = os.getcwd()
  PYTHON_READ_SCRIPTS = "%s/Python_Read_Scripts/" % PATH
  MARLIN_XML = "%s/Marlin_Xml/" % PATH
  ROOT_FILE_GENERATION = "%s/Root_File_Generation/" % PATH
  ROOT_FILES = "%s/Root_Files/" % PATH
  XML_GENERATION = "%s/Xml_Generation/" % PATH
  shutil.copy(PANDORA_SETTINGS_FILE_PATH, XML_GENERATION)
  shutil.copy(PANDORA_LIKELIHOOD_DATAFILE, XML_GENERATION)
  KAON_ROOT_FILES = "%s/Root_Files/pfoAnalysis_%s_GeV_Energy_K0L_SN_*.root" % (PATH, KAON_L_ENERGY_CALIBRATION)
  PHOTON_ROOT_FILES = "%s/Root_Files/pfoAnalysis_%s_GeV_Energy_gamma_SN_*.root" % (PATH, PHOTON_ENERGY_CALIBRATION)
  MUON_ROOT_FILES = "%s/Root_Files/pfoAnalysis_%s_GeV_Energy_mu-_SN_*.root" % (PATH, MUON_ENERGY_CALIBRATION)
  source_script(SOURCE_FILE)
  os.chdir(ROOT_FILE_GENERATION)
  for script_file in ['condorSupervisor_Calibration.sh', 'DummyCondorSupervisor.sh', 'Marlin_Calibration.sh']:
    _make_executable(script_file)


def ecal_digitisation():
  """ Executes the ECAL Digitisation

  :returns: nothing
  :rtype: None
  """
  global CALIBR_ECAL, ECAL_MEAN, FRACTIONAL_ERROR_ECAL_MEAN
  os.chdir(XML_GENERATION)
  call_list = ['python', 'Xml_Generate.py', PHOTON_ENERGY_CALIBRATION, 'gamma', ECAL_TO_EM, HCAL_TO_EM,
               ECAL_TO_HAD, HCAL_TO_HAD, CALIBR_ECAL, CALIBR_HCAL_BARREL, CALIBR_HCAL_ENDCAP,
               CALIBR_HCAL_OTHER, MHHHE, GAMMA_PATH, SLCIO_FORMAT, GEAR_FILE, PANDORA_SETTINGS_FILE,
               ECAL_GEV_TO_MIP, HCAL_GEV_TO_MIP, MUON_GEV_TO_MIP, HCAL_BARREL_TIME_WINDOW_MAX,
               HCAL_ENDCAP_TIME_WINDOW_MAX, ECAL_BARREL_TIME_WINDOW_MAX, ECAL_ENDCAP_TIME_WINDOW_MAX,
               DD4HEP_COMPACT_XML]
  execute_and_return_output(call_list)
  os.chdir(ROOT_FILE_GENERATION)
  runfile = 'Marlin_Runfile_%s_GeV_Energy_gamma.txt' % PHOTON_ENERGY_CALIBRATION
  #FIXME: Run marlin on the contents of this file
  for line in open(runfile):
    execute_and_return_output(['Marlin', line])
  execute_and_return_output(['ECalDigitisation_ContainedEvents', '-a', PHOTON_ROOT_FILES, '-b',
                             PHOTON_ENERGY_CALIBRATION, '-c', 'DIGITISATION_ACCURACY', '-d',
                             OUTPUT_PATH, '-e', '90'])
  os.chdir('PYTHON_READ_SCRIPTS')
  CALIBR_ECAL = execute_and_return_output(['python', 'ECal_Digi_Extract.py', CALIBRATION_FILE,
                                           PHOTON_ENERGY_CALIBRATION, CALIBR_ECAL, 'Calibration_Constant'])
  ECAL_MEAN = execute_and_return_output(['python', 'ECal_Digi_Extract.py', CALIBRATION_FILE,
                                         PHOTON_ENERGY_CALIBRATION, CALIBR_ECAL, 'Mean'])
  FRACTIONAL_ERROR_ECAL_MEAN = abs((PHOTON_ENERGY_CALIBRATION - ECAL_MEAN) / PHOTON_ENERGY_CALIBRATION)


def hcal_digitisation():
  """ Executes the HCAL Digitisation

  :returns: nothing
  :rtype: None
  """
  global CALIBR_HCAL_BARREL, CALIBR_HCAL_ENDCAP, HCAL_ENDCAP_MEAN, FRACTIONAL_ERROR_HCAL_BARREL_MEAN, \
      FRACTIONAL_ERROR_HCAL_ENDCAP_MEAN
  os.chdir(XML_GENERATION)
  call_list = ['python', 'Xml_Generate.py', KAON_L_ENERGY_CALIBRATION, 'K0L', ECAL_TO_EM, HCAL_TO_EM,
               ECAL_TO_HAD, HCAL_TO_HAD, CALIBR_ECAL, CALIBR_HCAL_BARREL, CALIBR_HCAL_ENDCAP,
               CALIBR_HCAL_OTHER, MHHHE, KAON_PATH, SLCIO_FORMAT, GEAR_FILE, PANDORA_SETTINGS_FILE,
               ECAL_GEV_TO_MIP, HCAL_GEV_TO_MIP, MUON_GEV_TO_MIP, HCAL_BARREL_TIME_WINDOW_MAX,
               HCAL_ENDCAP_TIME_WINDOW_MAX, ECAL_BARREL_TIME_WINDOW_MAX, ECAL_ENDCAP_TIME_WINDOW_MAX,
               DD4HEP_COMPACT_XML]
  execute_and_return_output(call_list)
  os.chdir(ROOT_FILE_GENERATION)
  runfile = 'Marlin_Runfile_%s_GeV_Energy_K0L.txt' % KAON_L_ENERGY_CALIBRATION
  #FIXME: Run marlin on the contents of this file
  for line in open(runfile):
    execute_and_return_output(['Marlin', line])
  execute_and_return_output(['HCalDigitisation_ContainedEvents', '-a', KAON_ROOT_FILES, '-b',
                             KAON_L_ENERGY_CALIBRATION, '-c', DIGITISATION_ACCURACY, '-d', OUTPUT_PATH,
                             '-e', '90', '-f', NUMBER_HCAL_LAYERS, '-g', 'Barrel', '-i', '0.2', '-j', '0.6'])
  execute_and_return_output(['HCalDigitisation_ContainedEvents', '-a', KAON_ROOT_FILES, '-b',
                             KAON_L_ENERGY_CALIBRATION, '-c', DIGITISATION_ACCURACY, '-d', OUTPUT_PATH,
                             '-e', '90', '-f', NUMBER_HCAL_LAYERS, '-g', 'EndCap', '-i', '0.8', '-j', '0.9'])
  os.chdir(PYTHON_READ_SCRIPTS)
  CALIBR_HCAL_BARREL = execute_and_return_output(['python', 'HCal_Digi_Extract.py', CALIBRATION_FILE,
                                                  KAON_L_KINETIC_ENERGY_CALIBRATION, CALIBR_HCAL_BARREL,
                                                  'Barrel', 'Calibration_Constant'])
  CALIBR_HCAL_ENDCAP = execute_and_return_output(['python', 'HCal_Digi_Extract.py', CALIBRATION_FILE,
                                                  KAON_L_KINETIC_ENERGY_CALIBRATION, CALIBR_HCAL_ENDCAP,
                                                  'EndCap', 'Calibration_Constant'])
  hcal_barrel_mean = execute_and_return_output(['python', 'HCal_Digi_Extract.py', CALIBRATION_FILE,
                                                KAON_L_KINETIC_ENERGY_CALIBRATION, CALIBR_HCAL_BARREL,
                                                'Barrel', 'Mean'])
  HCAL_ENDCAP_MEAN = execute_and_return_output(['python', 'HCal_Digi_Extract.py', CALIBRATION_FILE,
                                                KAON_L_KINETIC_ENERGY_CALIBRATION, CALIBR_HCAL_ENDCAP,
                                                'EndCap', 'Mean'])
  FRACTIONAL_ERROR_HCAL_BARREL_MEAN = abs(
      (KAON_L_KINETIC_ENERGY_CALIBRATION - hcal_barrel_mean) / KAON_L_KINETIC_ENERGY_CALIBRATION)
  FRACTIONAL_ERROR_HCAL_ENDCAP_MEAN = abs(
      (KAON_L_KINETIC_ENERGY_CALIBRATION - HCAL_ENDCAP_MEAN) / KAON_L_KINETIC_ENERGY_CALIBRATION)


def muon_digitisation():
  """ Executes the Muon Digitisation

  :returns: nothing
  :rtype: None
  """
  global CALIBR_HCAL_OTHER
  os.chdir(XML_GENERATION)
  call_list = ['python', 'Xml_Generate.py', MUON_ENERGY_CALIBRATION, 'mu-', ECAL_TO_EM, HCAL_TO_EM,
               ECAL_TO_HAD, HCAL_TO_HAD, CALIBR_ECAL, CALIBR_HCAL_BARREL, CALIBR_HCAL_ENDCAP,
               CALIBR_HCAL_OTHER, MHHHE, MUON_PATH, SLCIO_FORMAT, GEAR_FILE, PANDORA_SETTINGS_FILE,
               ECAL_GEV_TO_MIP, HCAL_GEV_TO_MIP, MUON_GEV_TO_MIP, HCAL_BARREL_TIME_WINDOW_MAX,
               HCAL_ENDCAP_TIME_WINDOW_MAX, ECAL_BARREL_TIME_WINDOW_MAX, ECAL_ENDCAP_TIME_WINDOW_MAX,
               DD4HEP_COMPACT_XML]
  execute_and_return_output(call_list)
  os.chdir(ROOT_FILE_GENERATION)
  runfile = 'Marlin_Runfile_%s_GeV_Energy_mu-.txt' % MUON_ENERGY_CALIBRATION
  #FIXME: Run marlin on the contents of this file
  for line in open(runfile):
    execute_and_return_output(['Marlin', line])
  execute_and_return_output(['HCalDigitisation_DirectionCorrectionDistribution', '-a', KAON_ROOT_FILES,
                             '-B', KAON_L_ENERGY_CALIBRATION, '-c', OUTPUT_PATH])
  execute_and_return_output(['SimCaloHitEnergyDistribution', '-a', MUON_ROOT_FILES, '-b',
                             MUON_ENERGY_CALIBRATION, '-c', OUTPUT_PATH])
  os.chdir(PYTHON_READ_SCRIPTS)
  absorber_scintillator_ratio = (ABSORBER_THICKNESS_ENDCAP * SCINTILLATOR_THICKNESS_RING) / \
      (ABSORBER_THICKNESS_RING * SCINTILLATOR_THICKNESS_ENDCAP)
  mip_peak_ratio = execute_and_return_output(['python', 'HCal_Ring_Digi_Extract.py',
                                              CALIBRATION_FILE, MUON_ENERGY_CALIBRATION])
  direction_correction_ratio = execute_and_return_output(['python', 'HCal_Direction_Corrections_Extract.py',
                                                          CALIBRATION_FILE, KAON_L_ENERGY_CALIBRATION])
  CALIBR_HCAL_OTHER = direction_correction_ratio * mip_peak_ratio * absorber_scintillator_ratio * \
      CALIBR_HCAL_ENDCAP * KAON_L_KINETIC_ENERGY_CALIBRATION / HCAL_ENDCAP_MEAN
  if not CALIBR_HCAL_OTHER:
    CALIBR_HCAL_OTHER = CALIBR_HCAL_ENDCAP
    print 'WARNING! CalibrHCALOther is NOT CALCULATED! SET MANUALLY TO: %s' % CALIBR_HCAL_OTHER


def gev_to_mip_constants():
  """ Calculates the GeV to MIP constants of the detector

  :returns: nothing
  :rtype: None
  """
  global ECAL_GEV_TO_MIP, HCAL_GEV_TO_MIP, MUON_GEV_TO_MIP, ECAL_MIP_MPV, HCAL_MIP_MPV
  execute_and_return_output(['PandoraPFACalibrate_MipResponse', '-a', MUON_ROOT_FILES, '-b',
                             MUON_ENERGY_CALIBRATION, '-c', 'outputPath'])
  os.chdir(PYTHON_READ_SCRIPTS)
  ECAL_GEV_TO_MIP = execute_and_return_output(['python', 'Extract_GeVToMIP.py', CALIBRATION_FILE,
                                               MUON_ENERGY_CALIBRATION, 'ECal'])
  HCAL_GEV_TO_MIP = execute_and_return_output(['python', 'Extract_GeVToMIP.py', CALIBRATION_FILE,
                                               MUON_ENERGY_CALIBRATION, 'HCal'])
  MUON_GEV_TO_MIP = execute_and_return_output(['python', 'Extract_GeVToMIP.py', CALIBRATION_FILE,
                                               MUON_ENERGY_CALIBRATION, 'Muon'])
  ECAL_MIP_MPV = execute_and_return_output(['python', 'Extract_SimCaloHitMIPMPV.py', CALIBRATION_FILE, 'ECal'])
  HCAL_MIP_MPV = execute_and_return_output(['python', 'Extract_SimCaloHitMIPMPV.py', CALIBRATION_FILE, 'HCal'])


def electromagnetic_energy_scale():
  """ Calibrates using the gamma events

  :returns: nothing
  :rtype: None
  """
  global ECAL_TO_EM, HCAL_TO_EM, FRACTIONAL_EM_ERROR
  os.chdir(XML_GENERATION)
  call_list = ['python', 'Xml_Generate.py', PHOTON_ENERGY_CALIBRATION, 'gamma', ECAL_TO_EM, HCAL_TO_EM,
               ECAL_TO_HAD, HCAL_TO_HAD, CALIBR_ECAL, CALIBR_HCAL_BARREL, CALIBR_HCAL_ENDCAP,
               CALIBR_HCAL_OTHER, MHHHE, GAMMA_PATH, SLCIO_FORMAT, GEAR_FILE, PANDORA_SETTINGS_FILE,
               ECAL_GEV_TO_MIP, HCAL_GEV_TO_MIP, MUON_GEV_TO_MIP, HCAL_BARREL_TIME_WINDOW_MAX,
               HCAL_ENDCAP_TIME_WINDOW_MAX, ECAL_BARREL_TIME_WINDOW_MAX, ECAL_ENDCAP_TIME_WINDOW_MAX,
               DD4HEP_COMPACT_XML]
  execute_and_return_output(call_list)
  os.chdir(ROOT_FILE_GENERATION)
  runfile = 'Marlin_Runfile_%s_GeV_Energy_gamma.txt' % PHOTON_ENERGY_CALIBRATION
  #FIXME: Run marlin on the contents of this file
  for line in open(runfile):
    execute_and_return_output(['Marlin', line])
  execute_and_return_output(['PandoraPFACalibrate_EMScale', '-a', PHOTON_ROOT_FILES, '-b',
                             PHOTON_ENERGY_CALIBRATION, '-c', PANDORA_PFA_ACCURACY, '-d',
                             OUTPUT_PATH, '-e', 90])
  os.chdir(PYTHON_READ_SCRIPTS)
  ECAL_TO_EM = execute_and_return_output(['python', 'EM_Extract.py', CALIBRATION_FILE,
                                          PHOTON_ENERGY_CALIBRATION, ECAL_TO_EM, 'Calibration_Constant'])
  HCAL_TO_EM = ECAL_TO_EM
  em_mean = execute_and_return_output(['python', 'EM_Extract.py', CALIBRATION_FILE,
                                       PHOTON_ENERGY_CALIBRATION, ECAL_TO_EM, 'Mean'])
  FRACTIONAL_EM_ERROR = abs((PHOTON_ENERGY_CALIBRATION - em_mean) / PHOTON_ENERGY_CALIBRATION)


def hadronic_energy_scale():
  """ Calibrates using the kaonL events

  :returns: nothing
  :rtype: None
  """
  global HCAL_TO_HAD_FOM, ECAL_TO_HAD_FOM, HCAL_TO_HAD, ECAL_TO_HAD, HCAL_TO_HAD_UL, \
      HCAL_TO_HAD_LL, ECAL_TO_HAD_LL, ECAL_TO_HAD_UL
  os.chdir(XML_GENERATION)
  call_list = ['python', 'Xml_Generate.py', KAON_L_ENERGY_CALIBRATION, 'K0L', ECAL_TO_EM, HCAL_TO_EM,
               ECAL_TO_HAD, HCAL_TO_HAD, CALIBR_ECAL, CALIBR_HCAL_BARREL, CALIBR_HCAL_ENDCAP,
               CALIBR_HCAL_OTHER, MHHHE, KAON_PATH, SLCIO_FORMAT, GEAR_FILE, PANDORA_SETTINGS_FILE,
               ECAL_GEV_TO_MIP, HCAL_GEV_TO_MIP, MUON_GEV_TO_MIP, HCAL_BARREL_TIME_WINDOW_MAX,
               HCAL_ENDCAP_TIME_WINDOW_MAX, ECAL_BARREL_TIME_WINDOW_MAX, ECAL_ENDCAP_TIME_WINDOW_MAX,
               DD4HEP_COMPACT_XML]
  execute_and_return_output(call_list)
  os.chdir(ROOT_FILE_GENERATION)
  runfile = 'Marlin_Runfile_%s_GeV_Energy_K0L.txt' % PHOTON_ENERGY_CALIBRATION
  #FIXME: Run marlin on the contents of this file
  for line in open(runfile):
    execute_and_return_output(['Marlin', line])
    # HCalToHad and ECalToHad Calibration
  if HADRONIC_SCALE_SETTING_PANDORA == 'CSM':
    execute_and_return_output(['PandoraPFACalibrate_HadronicScale_ChiSquareMethod', '-a', KAON_ROOT_FILES,
                               '-b', KAON_L_ENERGY_CALIBRATION, '-c', PANDORA_PFA_ACCURACY, '-d',
                               OUTPUT_PATH, '-e', NUMBER_HCAL_LAYERS])
  elif HADRONIC_SCALE_SETTING_PANDORA == 'TEM':
    execute_and_return_output(['PandoraPFACalibrate_HadronicScale_TotalEnergyMethod', '-a', KAON_ROOT_FILES,
                               '-b', KAON_L_ENERGY_CALIBRATION, '-c', PANDORA_PFA_ACCURACY, '-d',
                               OUTPUT_PATH, '-e', '90', '-f', NUMBER_HCAL_LAYERS])
  else:
    print 'Select a calibration method.'
  # Update HCTH and ECTH
  os.chdir(PYTHON_READ_SCRIPTS)
  HCAL_TO_HAD = execute_and_return_output(['python', 'Had_Extract.py', CALIBRATION_FILE,
                                           KAON_L_ENERGY_CALIBRATION, 'HCTH', HCAL_TO_HAD,
                                           'Calibration_Constant', HADRONIC_SCALE_SETTING_PANDORA])
  ECAL_TO_HAD = execute_and_return_output(['python', 'Had_Extract.py', CALIBRATION_FILE,
                                           KAON_L_ENERGY_CALIBRATION, 'ECTH', ECAL_TO_HAD,
                                           'Calibration_Constant', HADRONIC_SCALE_SETTING_PANDORA])
  HCAL_TO_HAD_FOM = execute_and_return_output(['python', 'Had_Extract.py', CALIBRATION_FILE,
                                               KAON_L_ENERGY_CALIBRATION, 'HCTH', HCAL_TO_HAD,
                                               'FOM', HADRONIC_SCALE_SETTING_PANDORA])
  ECAL_TO_HAD_FOM = execute_and_return_output(['python', 'Had_Extract.py', CALIBRATION_FILE,
                                               KAON_L_ENERGY_CALIBRATION, 'ECTH', ECAL_TO_HAD,
                                               'FOM', HADRONIC_SCALE_SETTING_PANDORA])
  # Limits on reconstruction
  if HADRONIC_SCALE_SETTING_PANDORA == 'CSM':
    HCAL_TO_HAD_UL = KAON_L_KINETIC_ENERGY_CALIBRATION * UPPER_LIMIT
    HCAL_TO_HAD_LL = KAON_L_KINETIC_ENERGY_CALIBRATION * LOWER_LIMIT
    ECAL_TO_HAD_UL = HCAL_TO_HAD_UL
    ECAL_TO_HAD_LL = HCAL_TO_HAD_LL
  elif HADRONIC_SCALE_SETTING_PANDORA == 'TEM':
    HCAL_TO_HAD_UL = UPPER_LIMIT
    HCAL_TO_HAD_LL = LOWER_LIMIT
    ECAL_TO_HAD_UL = HCAL_TO_HAD_UL
    ECAL_TO_HAD_LL = HCAL_TO_HAD_LL


def finalize_calibration():
  """ Calculates the final calibration numbers

  :returns: nothing
  :rtype: None
  """
  placeholder = '#FIXME: this is buggy/energies array does not exist in original script'
  print 'All jobs done. Calculating final calibration numbers'
  os.chdir(PYTHON_READ_SCRIPTS)
  call_list = ['python', 'Final_Calibration.py', ECAL_TO_EM, HCAL_TO_EM,
               ECAL_TO_HAD, HCAL_TO_HAD, CALIBR_ECAL, CALIBR_HCAL_BARREL, CALIBR_HCAL_ENDCAP,
               CALIBR_HCAL_OTHER, MHHHE, ECAL_GEV_TO_MIP, HCAL_GEV_TO_MIP, MUON_GEV_TO_MIP,
               HCAL_BARREL_TIME_WINDOW_MAX, HCAL_ENDCAP_TIME_WINDOW_MAX, ECAL_BARREL_TIME_WINDOW_MAX,
               ECAL_ENDCAP_TIME_WINDOW_MAX, OUTPUT_PATH]
  execute_and_return_output(call_list)
  os.chdir(XML_GENERATION)
  call_list = ['python', 'Xml_Generate_Output.py', ECAL_TO_EM, HCAL_TO_EM, ECAL_TO_HAD, HCAL_TO_HAD,
               CALIBR_ECAL, CALIBR_HCAL_BARREL, CALIBR_HCAL_ENDCAP, CALIBR_HCAL_OTHER, MHHHE,
               ECAL_GEV_TO_MIP, HCAL_GEV_TO_MIP, MUON_GEV_TO_MIP, HCAL_BARREL_TIME_WINDOW_MAX,
               HCAL_ENDCAP_TIME_WINDOW_MAX, ECAL_BARREL_TIME_WINDOW_MAX, ECAL_ENDCAP_TIME_WINDOW_MAX,
               OUTPUT_PATH]
  execute_and_return_output(call_list)
  with open(CALIBR_RESULTS_FILE) as res_file:
    res_file.write("""#Calibration data for %s with Cassette
#Commands used: %s

#Digitization constants
CALIBRECAL=%s 
CALIBRHCALBARREL=%s
CALIBRHCALENDCAP=%s
CALIBRHCALOTHER=%s
CALIBRMUON="70.1"
ECALTOMIP=%s
HCALTOMIP=%s
MUONTOMIP=%s

#Pandora Constants
ECALTOEM=%s
HCALTOEM=%s
ECALTOHADBARREL=%s
ECALTOHADENDCAP=%s
HCALTOHAD=%s
MHHHE=%s

DO_NLC=0
INPUT_ENERGY_CORRECTION_POINTS=%s
OUTPUT_ENERGY_CORRECTION_POINTS=%s""" % (GEOMETRY_MODEL_NAME, sys.argv, CALIBR_ECAL, CALIBR_HCAL_BARREL, CALIBR_HCAL_ENDCAP, CALIBR_HCAL_OTHER, ECAL_GEV_TO_MIP, HCAL_GEV_TO_MIP, MUON_GEV_TO_MIP, ECAL_TO_EM, HCAL_TO_EM, ECAL_TO_HAD, ECAL_TO_HAD, HCAL_TO_HAD, MHHHE, placeholder, placeholder))  # original: ENERGIES, ENERGIES
  print 'All done!'


def _make_executable(filename):
  """ Adds the user executable flag to the given file.

  :param basestring filename: path to the file (absolute or relative)
  :returns: nothing
  :rtype: None
  """
  st = os.stat(filename)
  os.chmod(filename, st.st_mode | stat.S_IXUSR)


def source_script(scriptname):
  """ Uses bash to source the given script

  :param basestring scriptname: path to the script
  :returns: nothing
  :rtype: None
  """
  try:
    command = ['bash', '-c', 'source %s  && env' % scriptname]
    proc = subprocess.Popen(command, stdout=subprocess.PIPE)

    for line in proc.stdout:
      (key, _, value) = line.partition("=")
      os.environ[key] = value.rstrip()
    proc.communicate()
  except OSError as oe:
    print oe
    exit(1)
  except subprocess.CalledProcessError as cpe:
    print cpe
    exit(1)
  except:  # pylint: disable=bare-except
    print sys.exc_info()[0]
    exit(1)


def execute_and_return_output(command):
  """ Executes the given command with the given arguments and returns the output as a string.

  :param basestring or list command: command to be executed, with arguments in the list
  :returns: output of the command
  :rtype: basestring
  """
  res = ''
  try:
    res = subprocess.check_output(command)
  except subprocess.CalledProcessError as cpe:
    print cpe
    exit(1)
  return res


def main():
  """ Runs the actual calibration.

  :returns: Nothing
  :rtype: None
  """
  init()
  while True:  # Do-while loop
    ecal_digitisation()
    if FRACTIONAL_ERROR_ECAL_MEAN < DIGITISATION_ACCURACY:
      break
  print 'ecal_digit finished'
  while True:  # Do-while loop
    hcal_digitisation()
    if FRACTIONAL_ERROR_HCAL_BARREL_MEAN < DIGITISATION_ACCURACY and FRACTIONAL_ERROR_HCAL_ENDCAP_MEAN < DIGITISATION_ACCURACY:
      break
  print 'hcal_digit finished'
  muon_digitisation()  # executed once
  print 'muon_digit finished'
  while True:  # Do-while loop
    electromagnetic_energy_scale()
    if FRACTIONAL_EM_ERROR < PANDORA_PFA_ACCURACY:
      break
  print 'ecal_digit finished'
  while True:  # Do-while loop
    hadronic_energy_scale()
    if HCAL_TO_HAD_FOM >= HCAL_TO_HAD_UL or HCAL_TO_HAD_FOM <= HCAL_TO_HAD_LL \
       or ECAL_TO_HAD_FOM >= ECAL_TO_HAD_UL or ECAL_TO_HAD_FOM <= ECAL_TO_HAD_LL:
      break
  print 'hadronic_energy finished'
  finalize_calibration()


if __name__ == '__main__':
  main()
