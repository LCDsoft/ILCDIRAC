# $HeadURL$
# $Id$
'''
ILCDIRAC.Core.Utilities.SplitByFilesAndEvents

Based on Dirac.SplitByFiles idea, but doing the splitting by number of events
Gives a list of dictionaries


Created on Feb 10, 2010

@author: sposs
'''

def SplitByFilesAndEvents(listoffiles, evtsperjob):
  mylist = []
  startfromevt = 1
  for files in listoffiles:
    mydict = {}
    mydict['file'] = files
    mydict['startFrom'] = startfromevt
    startfromevt += evtsperjob
    startfromevt += 1
    mylist.append(dict)
  return mylist