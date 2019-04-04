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
Mean_Or_Calibration_Constant = sys.argv[4]
ECal_Barrel_Or_EndCap = sys.argv[5].lower()

# Mean is the last value for mean of Gaussian fit written to the Calibration.txt file.

Mean = '1.0'

with open(Calibration_File_And_Path, 'r') as f:
    searchlines = f.readlines()
    startOfTheBlock = False
    blockType = ''
    for line in searchlines:
        if startOfTheBlock:
            if 'endcap' in line.lower():
                blockType = 'endcap'
            elif 'barrel' in line.lower():
                blockType = 'barrel'
            else:
                blockType = ''

        if "___" in line:
            startOfTheBlock = True
        else:
            startOfTheBlock = False

        if ECal_Barrel_Or_EndCap == blockType:
            if 'ECal Digi Mean' in line:
                Mean = float(find_between(line, ' : ', ' :'))


if Mean_Or_Calibration_Constant == 'Mean':
    print Mean

elif Mean_Or_Calibration_Constant == 'Calibration_Constant':
    print str(float(Energy_To_Calibrate) * float(Initial_Calibration_Constant) / float(Mean))

    Calibration_Text = '_____________________________________________________________________________________' + '\n'
    Calibration_Text += 'ECal_Digi_Extract.py retrieving CalibrECal from digitisation program. ' + '\n'
    Calibration_Text += 'Photon Energy To Calibrate                         : ' + str(Energy_To_Calibrate) + ' /GeV \n'
    Calibration_Text += 'Initial Calibration Constant                       : ' + \
        str(Initial_Calibration_Constant) + '\n'
    Calibration_Text += 'CalibrECal Mean                                    : ' + str(Mean) + ' /GeV\n\n'
    Calibration_Text += 'CalibrECal                                         : ' + \
        str(float(Energy_To_Calibrate) * float(Initial_Calibration_Constant) / float(Mean)) + '\n\n'

    if ECal_Barrel_Or_EndCap == 'barrel':
        Calibration_Text += 'CalibrECalBarrel '
    elif ECal_Barrel_Or_EndCap == 'endcap':
        Calibration_Text += 'CalibrECalEndCap '

    Calibration_Text += 'from digitisation program.' + '\n'
    Calibration_Text += 'Photon Energy To Calibrate                          : ' + str(Energy_To_Calibrate) + ' /GeV\n'
    Calibration_Text += 'Initial Calibration Constant                       : ' + \
        str(Initial_Calibration_Constant) + '\n'
    Calibration_Text += 'CalibrECal' + ECal_Barrel_Or_EndCap + \
        ' Mean                              : ' + str(Mean) + ' /GeV \n\n'
    Calibration_Text += 'CalibrECal' + ECal_Barrel_Or_EndCap + '                                   : ' + \
        str(float(Energy_To_Calibrate) * float(Initial_Calibration_Constant) / float(Mean)) + '\n\n'

    with open(Calibration_File_And_Path, 'a') as myfile:
        myfile.write(Calibration_Text)

else:
    print 'Please select Mean or Calibration_Constant to extract.'
