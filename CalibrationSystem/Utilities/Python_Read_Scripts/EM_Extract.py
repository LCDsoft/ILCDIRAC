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

# Mean is the last value for Mean of Gaussian fit written to the Calibration.txt file.

Mean = ''

with open(Calibration_File_And_Path, 'r') as f:
    searchlines = f.readlines()
    for line in searchlines:
        if 'ECalToEM Mean' in line:
            Mean = float(find_between(line, ' : ', ' : '))

if Mean_Or_Calibration_Constant == 'Mean':
    print Mean

elif Mean_Or_Calibration_Constant == 'Calibration_Constant':
    print float(Energy_To_Calibrate) * float(Initial_Calibration_Constant) / float(Mean)

    Calibration_Text = '_____________________________________________________________________________________' + '\n'
    Calibration_Text += 'EM_Extract.py retrieving ECTE and HCTE from PandoraPFA calibration program.' + '\n'
    Calibration_Text += 'Photon Energy To Calibrate                         : ' + str(Energy_To_Calibrate) + '\n'
    Calibration_Text += 'Initial Calibration Constant                       : ' + \
        str(Initial_Calibration_Constant) + '\n'
    Calibration_Text += 'Mean                                               : ' + str(Mean) + '\n\n'
    Calibration_Text += 'Updated => ECalToHad = HCalToHad =                 : ' + \
        str(float(Energy_To_Calibrate) * float(Initial_Calibration_Constant) / float(Mean)) + '\n\n'

    with open(Calibration_File_And_Path, 'a') as myfile:
        myfile.write(Calibration_Text)

else:
    print 'Please select Mean or Calibration_Constant to extract.'
