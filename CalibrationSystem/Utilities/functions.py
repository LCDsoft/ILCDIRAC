""" functions from the calibration system """

import re
import os

#pylint: disable=invalid-name,too-many-locals,too-many-statements


def xml_generate(baseFileName='CLIC_PfoAnalysis_AAAA_SN_BBBB.xml',
                 workerID=0,
                 inputFile=None,
                 *args
                 ):
  """ replace parameters in XML steering file used for calibration
  run on a single XML file, as now used in each job

  """
  print "Shall run Xml_Generate.py with these arguments of length", len(args)

  Energy = args[1]

  Particle = args[2]

  ECALTOEM = args[3]
  HCALTOEM = args[4]
  ECALTOHAD = args[5]
  HCALTOHAD = args[6]

  CALIBR_ECAL_INPUT = args[7]
  CALIBR_ECAL_INPUT2 = 2 * float(CALIBR_ECAL_INPUT)

  CALIBR_ECAL = str(CALIBR_ECAL_INPUT) + ' ' + str(CALIBR_ECAL_INPUT2)
  #Of the form: <parameter name="CalibrECAL" type="FloatVec">42.91 85.82 </parameter>

  CALIBR_HCAL_BARREL = args[8]
  CALIBR_HCAL_ENDCAP = args[9]
  CALIBR_HCAL_OTHER = args[10]

  MHHHE = args[11]

  ##Slcio_Path = args[12]

  #Slcio_Format = args[13]

  Gear_File_And_Path = args[14]

  Pandora_Settings_File = args[15]

  ECalGeVToMIP = args[16]
  HCalGeVToMIP = args[17]
  MuonGeVToMIP = args[18]

  HCALBarrelTimeWindowMax = args[19]
  HCALEndcapTimeWindowMax = args[20]
  ECALBarrelTimeWindowMax = args[21]
  ECALEndcapTimeWindowMax = args[22]
  dd4hep_compactxml_File = args[23]

  #===========================
  # Slcio_Format = re.sub('ENERGY',Energy,Slcio_Format)
  # Slcio_Format = re.sub('PARTICLE',Particle,Slcio_Format)

  job_name = Energy + '_GeV_Energy_' + Particle

  ECAL_MIP_THRESHOLD = '0.3'
  HCAL_MIP_THRESHOLD = '0.5'

  Marlin_Path = os.getcwd()

  with open(baseFileName, 'r') as base:
    newContent = base.read()

  baseFileName = re.sub('AAAA', job_name, baseFileName)

  SN = workerID  # workerID ??

  #print SN
  newContent = re.sub('ECALTOEM_XXXX', ECALTOEM, newContent)
  newContent = re.sub('HCALTOEM_XXXX', HCALTOEM, newContent)
  newContent = re.sub('ECALTOHAD_XXXX', ECALTOHAD, newContent)
  newContent = re.sub('HCALTOHAD_XXXX', HCALTOHAD, newContent)
  newContent = re.sub('MHHHE_XXXX', MHHHE, newContent)
  newContent = re.sub('slcio_XXXX', inputFile, newContent)
  newContent = re.sub('Gear_XXXX', Gear_File_And_Path, newContent)
  newContent = re.sub('CALIBR_ECAL_XXXX', CALIBR_ECAL, newContent)
  newContent = re.sub('CALIBR_HCAL_BARREL_XXXX', CALIBR_HCAL_BARREL, newContent)
  newContent = re.sub('CALIBR_HCAL_ENDCAP_XXXX', CALIBR_HCAL_ENDCAP, newContent)
  newContent = re.sub('CALIBR_HCAL_OTHER_XXXX', CALIBR_HCAL_OTHER, newContent)
  newContent = re.sub('EMT_XXXX', ECAL_MIP_THRESHOLD, newContent)
  newContent = re.sub('HMT_XXXX', HCAL_MIP_THRESHOLD, newContent)
  newContent = re.sub('ECalGeVToMIP_XXXX', ECalGeVToMIP, newContent)
  newContent = re.sub('HCalGeVToMIP_XXXX', HCalGeVToMIP, newContent)
  newContent = re.sub('MuonGeVToMIP_XXXX', MuonGeVToMIP, newContent)
  newContent = re.sub('HCALBarrelTimeWindowMax_XXXX', HCALBarrelTimeWindowMax, newContent)
  newContent = re.sub('HCALEndcapTimeWindowMax_XXXX', HCALEndcapTimeWindowMax, newContent)
  newContent = re.sub('ECALBarrelTimeWindowMax_XXXX', ECALBarrelTimeWindowMax, newContent)
  newContent = re.sub('ECALEndcapTimeWindowMax_XXXX', ECALEndcapTimeWindowMax, newContent)

  newContent = re.sub('PSF_XXXX', Pandora_Settings_File, newContent)
  newContent = re.sub('COMPACTXML_XXXX', dd4hep_compactxml_File, newContent)

  newContent = re.sub('pfoAnalysis_XXXX.root ', 'pfoAnalysis_' + job_name + '_SN_' + SN + '.root', newContent)

  newFileName = re.sub('BBBB', str(SN), baseFileName)

  Fullpath = os.path.join(Marlin_Path, newFileName)

  with open(Fullpath, 'w') as newXML:
    newXML.write(newContent)

  return newFileName
