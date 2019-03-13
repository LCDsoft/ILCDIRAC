import os
import re
import random
import dircache
import sys
import math


def find_between(s, first, last):
    try:
        start = s.index(first) + len(first)
        end = s.index(last, start)
        return s[start:end]
    except ValueError:
        return ''


Calibration_File_And_Path = sys.argv[1]
Gear_File_And_Path = sys.argv[2]
Energy_To_Calibrate = sys.argv[3]

Check_Ring_One = 'Off'
Check_Ring_Two = 'Off'

Absorber_Thickness_Ring = 0
Scintillator_Thickness_Ring = 0

Check_EndCap_One = 'Off'
Check_EndCap_Two = 'Off'

Absorber_Thickness_EndCap = 0
Scintillator_Thickness_EndCap = 0

Check_EndCap = 'Off'

with open(Gear_File_And_Path, 'r') as f:
    searchlines = f.readlines()
    for line in searchlines:
        if 'detector name="HcalRing"' in line:
            Check_Ring_One = 'On'
            Check_Ring_Two = 'On'

        if Check_Ring_One == 'On':
            if 'absorberThickness' in line:
                Number = float(find_between(line, 'absorberThickness="', 'e'))
		trimmedLine = find_between(line, 'absorberThickness="', 'Size')
                Power_Of_Ten = float(find_between(trimmedLine, 'e+', '" cell'))
                Absorber_Thickness_Ring = Number * pow(10, Power_Of_Ten)
                Check_Ring_One = 'Off'

        if Check_Ring_Two == 'On':
            if 'Hcal_scintillator_thickness' in line:
                Number = float(find_between(line, 'type="double" value="', 'e'))
                Power_Of_Ten = float(find_between(line, 'e+', '"'))
                Scintillator_Thickness_Ring = Number * pow(10, Power_Of_Ten)
                Check_Ring_Two = 'Off'

    for line in searchlines:
        if 'detector name="HcalEndcap"' in line:
            Check_EndCap_One = 'On'
            Check_EndCap_Two = 'On'

        if Check_EndCap_One == 'On':
            if 'absorberThickness' in line:
                Number = float(find_between(line, 'absorberThickness="', 'e'))
                trimmedLine = find_between(line, 'absorberThickness="', 'Size')
                Power_Of_Ten = float(find_between(trimmedLine, 'e+', '" cell'))
                Absorber_Thickness_EndCap = Number * pow(10, Power_Of_Ten)
                Check_EndCap_One = 'Off'

        if Check_EndCap_Two == 'On':
            if 'Hcal_scintillator_thickness' in line:
                Number = float(find_between(line, 'type="double" value="', 'e'))
                Power_Of_Ten = float(find_between(line, 'e+', '"'))
                Scintillator_Thickness_EndCap = Number * pow(10, Power_Of_Ten)
                Check_EndCap_Two = 'Off'

Calibration_Text = '_____________________________________________________________________________________' + '\n'

Calibration_Text += 'HCal_Ring_Gear_information.py retrieving GEAR information for HCal Ring Digitisation' + '\n'
Calibration_Text += 'For Muons with energy                              :' + str(Energy_To_Calibrate) + ' /GeV \n'
Calibration_Text += 'Absorber_Thickness_Ring                            : ' + str(Absorber_Thickness_Ring) + ' /mm \n'
Calibration_Text += 'Scintillator_Thickness_Ring                        : ' + \
    str(Scintillator_Thickness_Ring) + ' /mm \n'
Calibration_Text += 'Absorber_Thickness_EndCap                          : ' + str(Absorber_Thickness_EndCap) + ' /mm \n'
Calibration_Text += 'Scintillator_Thickness_EndCap                      : ' + \
    str(Scintillator_Thickness_EndCap) + ' /mm \n\n'

print Calibration_Text
ratio = (Absorber_Thickness_EndCap * Scintillator_Thickness_Ring) / \
    (Absorber_Thickness_Ring * Scintillator_Thickness_EndCap)

Calibration_Text += 'Ratio used for HCal Ring Digitisation is           : ' + '\n'
Calibration_Text += 'Absorber_Thickness_EndCap x Scintillator_Thickness_Ring' + '\n'
Calibration_Text += '-------------------------------------------------------     = ' + str(ratio) + '\n'
Calibration_Text += 'Scintillator_Thickness_EndCap x Absorber_Thickness_Ring' + '\n\n'

with open(Calibration_File_And_Path, 'a') as myfile:
    myfile.write(Calibration_Text)

print ratio
