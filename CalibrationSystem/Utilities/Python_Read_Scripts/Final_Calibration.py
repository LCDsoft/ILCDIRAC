"""Create text file with final calibration constants."""
import sys

ECALTOEM = sys.argv[1]
HCALTOEM = sys.argv[2]
ECALTOHAD = sys.argv[3]
HCALTOHAD = sys.argv[4]
CALIBR_ECAL_BARREL_INPUT = sys.argv[5]
CALIBR_ECAL_BARREL_INPUT2 = float(CALIBR_ECAL_BARREL_INPUT)
CALIBR_ECAL_BARREL = str(CALIBR_ECAL_BARREL_INPUT) + ' ' + str(CALIBR_ECAL_BARREL_INPUT2)
CALIBR_ECAL_ENDCAP = sys.argv[6]
CALIBR_ECAL_ENDCAP_CORR = sys.argv[7]
CALIBR_HCAL_BARREL = sys.argv[8]
CALIBR_HCAL_ENDCAP = sys.argv[9]
CALIBR_HCAL_OTHER = sys.argv[10]
MHHHE = sys.argv[11]
ECalGeVToMIP = sys.argv[12]
HCalGeVToMIP = sys.argv[13]
MuonGeVToMIP = sys.argv[14]
HCALBarrelTimeWindowMax = sys.argv[15]
HCALEndcapTimeWindowMax = sys.argv[16]
ECALBarrelTimeWindowMax = sys.argv[17]
ECALEndcapTimeWindowMax = sys.argv[18]
Output_Path = sys.argv[19]

jobList = ''

jobList += 'CalibrECALBarrel was found to be:                         '
jobList += str(CALIBR_ECAL_BARREL) + '\n'

jobList += 'CalibrECALEndcap was found to be:                         '
jobList += str(CALIBR_ECAL_ENDCAP) + '\n'

jobList += 'CalibrECALEndcapCorr was found to be:                         '
jobList += str(CALIBR_ECAL_ENDCAP_CORR) + '\n'

jobList += 'CalibrHCALBarrel was found to be:                   '
jobList += str(CALIBR_HCAL_BARREL) + '\n'

jobList += 'CalibrHCALEndcap was found to be:                   '
jobList += str(CALIBR_HCAL_ENDCAP) + '\n'

jobList += 'CalibrHCALOther was found to be:                    '
jobList += str(CALIBR_HCAL_OTHER) + '\n'

jobList += 'ECalGeVToMIP was found to be:                       '
jobList += str(ECalGeVToMIP) + '\n'

jobList += 'HCalGeVToMIP was found to be:                       '
jobList += str(HCalGeVToMIP) + '\n'

jobList += 'MuonGeVToMIP was found to be:                       '
jobList += str(MuonGeVToMIP) + '\n'

jobList += 'MaxHCalHitHadronicEnergy was found to be:           '
jobList += MHHHE + '\n'

jobList += 'ECalToEMGeVCalibration was found to be:             '
jobList += str(ECALTOEM) + '\n'

jobList += 'HCalToEMGeVCalibration was found to be:             '
jobList += str(HCALTOEM) + '\n'

jobList += 'ECalToHadGeVCalibrationBarrel was found to be:      '
jobList += str(ECALTOHAD) + '\n'

jobList += 'HCalToHadGeVCalibration was found to be:            '
jobList += str(HCALTOHAD) + '\n'

jobList += 'HCALBarrelTimeWindowMax is:                         '
jobList += HCALBarrelTimeWindowMax + '\n'

jobList += 'HCALEndcapTimeWindowMax is:                         '
jobList += HCALEndcapTimeWindowMax + '\n'

jobList += 'ECALBarrelTimeWindowMax is:                         '
jobList += ECALBarrelTimeWindowMax + '\n'

jobList += 'ECALEndcapTimeWindowMax is:                         '
jobList += ECALEndcapTimeWindowMax + '\n'

file = open(Output_Path + 'Final_Calibration.txt', 'w')
file.write(jobList)
file.close()
