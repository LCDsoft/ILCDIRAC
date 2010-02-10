# $HeadURL$
# $Id$
'''
LCDDIRAC.Core.Utilities.SplitByFilesAndEvents

Based on Dirac.SplitByFiles, but adding the splitting by number of events
Gives a list of dictionaries


Created on Feb 10, 2010

@author: sposs
'''

def SplitByFilesAndEvents(listoffiles,evtsperfile):
  list = []
  startfromevt = 1
  for files in listoffiles:
    dict = {}
    dict['file']=files
    dict['startFrom'] = startfromevt
    startfromevt+=evtsperfile
    startfromevt+=1
    list.append(dict)
  return list