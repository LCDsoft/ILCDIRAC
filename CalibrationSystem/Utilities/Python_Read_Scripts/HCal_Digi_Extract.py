# -*- coding: utf-8 -*-
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
Energy_To_Calibrate = sys.argv[2]
Initial_Calibration_Constant = sys.argv[3]
HCal_Barrel_Or_EndCap = sys.argv[4].lower()
Mean_Or_Calibration_Constant = sys.argv[5]

# Mean is the last value for mean of Gaussian fit written to the Calibration.txt file.

Mean = '1.0'

with open(Calibration_File_And_Path, 'r') as f:
    searchlines = f.readlines()
    for line in searchlines:
        if HCal_Barrel_Or_EndCap == 'barrel':
            if 'HCal Barrel Digi Mean' in line:
                Mean = float(find_between(line, ' : ', ' :'))
        elif HCal_Barrel_Or_EndCap == 'endcap':
            if 'HCal EndCap Digi Mean' in line:
                Mean = float(find_between(line, ' : ', ' :'))
        else:
            print 'Please select Barrel or Endcap'

if Mean_Or_Calibration_Constant == 'Mean':
    print Mean

elif Mean_Or_Calibration_Constant == 'Calibration_Constant':
    print float(Energy_To_Calibrate) * float(Initial_Calibration_Constant) / float(Mean)

    Calibration_Text = '_____________________________________________________________________________________' + '\n'
    Calibration_Text += 'HCal_Digi_Extract.py retrieving '

    if HCal_Barrel_Or_EndCap == 'barrel':
        Calibration_Text += 'CalibrHCalBarrel '
    elif HCal_Barrel_Or_EndCap == 'endcap':
        Calibration_Text += 'CalibrHCalEndCap '

    Calibration_Text += 'from digitisation program.' + '\n'
    Calibration_Text += 'KaonL Energy To Calibrate                          : ' + str(Energy_To_Calibrate) + ' /GeV\n'
    Calibration_Text += 'Initial Calibration Constant                       : ' + \
        str(Initial_Calibration_Constant) + '\n'
    Calibration_Text += 'CalibrHCal' + HCal_Barrel_Or_EndCap + \
        ' Mean                              : ' + str(Mean) + ' /GeV \n\n'
    Calibration_Text += 'CalibrHCal' + HCal_Barrel_Or_EndCap + '                                   : ' + \
        str(float(Energy_To_Calibrate) * float(Initial_Calibration_Constant) / float(Mean)) + '\n\n'

    with open(Calibration_File_And_Path, 'a') as myfile:
        myfile.write(Calibration_Text)

else:
    print 'Please select Mean or Calibration_Constant to extract.'
