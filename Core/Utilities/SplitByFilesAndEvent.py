# $HeadURL$
# $Id$
'''
ILCDIRAC.Core.Utilities.SplitByFilesAndEvents

Based on Dirac.SplitByFiles idea, but doing the splitting by number of events
Gives a list of dictionaries


Created on Feb 10, 2010

@author: sposs
'''

def SplitByFilesAndEvents(listoffiles,evtsperjob):
  list = []
  startfromevt = 1
  for files in listoffiles:
    dict = {}
    dict['file']=files
    dict['startFrom'] = startfromevt
    startfromevt+=evtsperjob
    startfromevt+=1
    list.append(dict)
  return list