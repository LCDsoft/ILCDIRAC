"""Retrieve GeVToMIP calibration constants from Calibration.txt file."""

from __future__ import print_function
import sys
from Helper_Functions import find_between


Calibration_File_And_Path = sys.argv[1]
Energy_To_Calibrate = sys.argv[2]
HCal_Or_ECal_Or_Muon = sys.argv[3]

GeVToMIP = ''

with open(Calibration_File_And_Path, 'r') as f:
    searchlines = f.readlines()
    for line in searchlines:
        if HCal_Or_ECal_Or_Muon == 'HCal':
            if 'HCalGeVToMIP' in line:
                GeVToMIP = find_between(line, ' : ', ' :')
        elif HCal_Or_ECal_Or_Muon == 'ECal':
            if 'ECalGeVToMIP' in line:
                GeVToMIP = find_between(line, ' : ', ' :')
        elif HCal_Or_ECal_Or_Muon == 'Muon':
            if 'MuonGeVToMIP' in line:
                GeVToMIP = find_between(line, ' : ', ' :')
        else:
            print('Please select HCal, ECal or Muon')

Calibration_Text = '_____________________________________________________________________________________' + '\n'

Calibration_Text += 'Extract_GeVToMIP.py extracting ' + HCal_Or_ECal_Or_Muon + 'GeVToMIP.' + '\n'

Calibration_Text += 'For Muon events with energy                        :' + str(Energy_To_Calibrate) + ' /GeV \n\n'

Calibration_Text += HCal_Or_ECal_Or_Muon + 'GeVToMIP is:                                   :' + GeVToMIP + ' \n\n'

with open(Calibration_File_And_Path, 'a') as myfile:
    myfile.write(Calibration_Text)

print(str(GeVToMIP))
