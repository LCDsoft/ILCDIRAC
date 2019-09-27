"""Extract mean direction corrections ratio."""

from __future__ import print_function
import sys
from Helper_Functions import find_between


Calibration_File_And_Path = sys.argv[1]
Energy_To_Calibrate = sys.argv[2]

Ring_Mean_Dir_Corr = ''
EndCap_Mean_Dir_Corr = ''

with open(Calibration_File_And_Path, 'r') as f:
    searchlines = f.readlines()
    for line in searchlines:
        if 'Mean Direction Correction HCalEndCap:' in line:
            EndCap_Mean_Dir_Corr = float(find_between(line, ' : ', ' :'))
        if 'Mean Direction Correction HCalOther:' in line:
            Ring_Mean_Dir_Corr = float(find_between(line, ' : ', ' :'))

Calibration_Text = '_____________________________________________________________________________________' + '\n'

Calibration_Text += 'HCal_Direction_Corrections_Extract.py finding mean direction corrections ratio ' + '\n'
Calibration_Text += '(Ring/Other to EndCap) for HCal.' + '\n'

Calibration_Text += 'For KaonL events with energy                       :' + str(Energy_To_Calibrate) + ' /GeV \n\n'

Calibration_Text += 'Ring Mean Direction Correction                     :' + str(Ring_Mean_Dir_Corr) + ' \n\n'

Calibration_Text += 'EndCap Mean Direction Correction                   :' + str(EndCap_Mean_Dir_Corr) + ' \n\n'

with open(Calibration_File_And_Path, 'a') as myfile:
    myfile.write(Calibration_Text)

print(str(EndCap_Mean_Dir_Corr / Ring_Mean_Dir_Corr))
