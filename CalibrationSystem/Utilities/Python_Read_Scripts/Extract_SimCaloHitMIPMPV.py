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
ECal_Or_HCal = sys.argv[2]

HCalMIPMPV = ''
HCalMIPMPVBarrel = ''
HCalMIPMPVEndCap = ''

with open(Calibration_File_And_Path, 'r') as f:
    searchlines = f.readlines()
    for line in searchlines:
        if 'HCal Barrel MIP Peak' in line:
            HCalMIPMPVBarrel = find_between(line, ' : ', ' :')
        if 'HCal EndCap MIP Peak' in line:
            HCalMIPMPVEndCap = find_between(line, ' : ', ' :')
        if 'ECal MIP Peak' in line:
            ECalMIPMPV = find_between(line, ' : ', ' :')

#HCalMIPMPV = str( float(HCalMIPMPVBarrel) + float (HCalMIPMPVEndCap) / 2)

if 'ECal' in ECal_Or_HCal:
    print str(ECalMIPMPV)
elif 'HCal' in ECal_Or_HCal:
    print str(HCalMIPMPVBarrel)
