# -*- coding: utf-8 -*-
import sys


def find_between(s, first, last):
    try:
        start = s.index(first) + len(first)
        end = s.index(last, start)
        return s[start:end]
    except ValueError:
        return ''


Calibration_File_And_Path = sys.argv[1]
Energy_To_Calibrate = sys.argv[2]
ECTH_or_HCTH = sys.argv[3]
Initial_Calibration_Constant = sys.argv[4]
FOM_Or_Calibration_Constant = sys.argv[5]
CSM_or_TEM = sys.argv[6]

KaonL_Mass = 0.497672

Kinetic_Energy_To_Calibrate = float(Energy_To_Calibrate) - KaonL_Mass

# Intercept is the last value for intercept written to the Calibration.txt file.

CSM_Intercept = ''
TEM_Multiplier = ''

if CSM_or_TEM == 'CSM':
    if ECTH_or_HCTH == 'ECTH':
        with open(Calibration_File_And_Path, 'r') as f:
            searchlines = f.readlines()
            for line in searchlines:
                if 'm_eCalToHadInterceptMinChi2' in line:
                    CSM_Intercept = float(find_between(line, ' : ', ' : '))

    elif ECTH_or_HCTH == 'HCTH':
        with open(Calibration_File_And_Path, 'r') as f:
            searchlines = f.readlines()
            for line in searchlines:
                if 'm_hCalToHadInterceptMinChi2' in line:
                    CSM_Intercept = float(find_between(line, ' : ', ' : '))

    else:
        print 'Please select ECTH or HCTH CSM_Intercept to extract.'

    if FOM_Or_Calibration_Constant == 'FOM':
        print CSM_Intercept

    elif FOM_Or_Calibration_Constant == 'Calibration_Constant':
        print str(float(Kinetic_Energy_To_Calibrate) * float(Initial_Calibration_Constant) / float(CSM_Intercept))

        Calibration_Text = '_____________________________________________________________________________________' + '\n'
        Calibration_Text += 'Had_Extract.py extracting '
        if ECTH_or_HCTH == 'HCTH':
            Calibration_Text += 'HCalToHad'
        elif ECTH_or_HCTH == 'ECTH':
            Calibration_Text += 'ECalToHad'
        Calibration_Text += ' from CSM PandoraPFA calibration program. ' + '\n'
        Calibration_Text += 'Kinetic Energy To Calibrate                        : ' + \
            str(Kinetic_Energy_To_Calibrate) + '\n'
        Calibration_Text += 'Initial Calibration Constant                       : ' + \
            str(Initial_Calibration_Constant) + '\n'
        Calibration_Text += 'CSM_Intercept                                      : ' + str(CSM_Intercept) + '\n\n'
        Calibration_Text += 'New Calibration Constant                           : ' + \
            str(float(Kinetic_Energy_To_Calibrate) * float(Initial_Calibration_Constant) / float(CSM_Intercept)) + '\n\n'

        with open(Calibration_File_And_Path, 'a') as myfile:
            myfile.write(Calibration_Text)

    else:
        print 'Please select Intercept or Calibration_Constant to extract.'

elif CSM_or_TEM == 'TEM':

    if ECTH_or_HCTH == 'ECTH':
        with open(Calibration_File_And_Path, 'r') as f:
            searchlines = f.readlines()
            for line in searchlines:
                if 'Minimum_RMS_ECal_Multiplier' in line:
                    TEM_Multiplier = float(find_between(line, ' : ', ' :'))

    elif ECTH_or_HCTH == 'HCTH':
        with open(Calibration_File_And_Path, 'r') as f:
            searchlines = f.readlines()
            for line in searchlines:
                if 'Minimum_RMS_HCal_Multiplier' in line:
                    TEM_Multiplier = float(find_between(line, ' : ', ' :'))

    else:
        print 'Please select ECTH or HCTH TEM_Multiplier to extract.'

    if FOM_Or_Calibration_Constant == 'FOM':
        print TEM_Multiplier

    elif FOM_Or_Calibration_Constant == 'Calibration_Constant':
        print str(float(TEM_Multiplier) * float(Initial_Calibration_Constant))

        Calibration_Text = '_____________________________________________________________________________________' + '\n'
        Calibration_Text += 'Had_Extract.py extracting '
        if ECTH_or_HCTH == 'HCTH':
            Calibration_Text += 'HCalToHad'
        elif ECTH_or_HCTH == 'ECTH':
            Calibration_Text += 'ECalToHad'
        Calibration_Text += ' from TEM PandoraPFA calibration program. ' + '\n'
        Calibration_Text += 'Kinetic Energy To Calibrate                        : ' + \
            str(Kinetic_Energy_To_Calibrate) + '\n'
        Calibration_Text += 'Initial Calibration Constant                       : ' + \
            str(Initial_Calibration_Constant) + '\n'
        Calibration_Text += 'TEM_Multiplier                                     : ' + str(TEM_Multiplier) + '\n\n'
        Calibration_Text += 'New Calibration Constant                           : ' + \
            str(float(TEM_Multiplier) * float(Initial_Calibration_Constant)) + '\n\n'

        with open(Calibration_File_And_Path, 'a') as myfile:
            myfile.write(Calibration_Text)

    else:
        print 'Please select FOM or Calibration_Constant to extract.'

else:
    print 'Please select CSM or TEM.'
