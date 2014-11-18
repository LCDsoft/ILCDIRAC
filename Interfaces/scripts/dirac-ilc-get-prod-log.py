#!/bin/env python
'''
Get production logs

Created on Mar 21, 2013

@author: stephane
'''
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC import gLogger, S_OK, S_ERROR, exit as dexit

class Params(object):
  """Parameter object"""
  def __init__(self):
    self.logD = ''
    self.logF = ''
    self.outputdir = './'
  def setLogFileD(self,opt):
    self.logD = opt
    return S_OK()
  def setLogFileF(self,opt):
    self.logF = opt
    return S_OK()
  def setOutputDir(self,opt):
    self.outputdir = opt
    return S_OK()
  def registerSwitch(self):
    """registers switches"""
    Script.registerSwitch('D:', 'LogFileDir=', 'Production log dir to download', self.setLogFileD)
    Script.registerSwitch('F:', 'LogFile=', 'Production log to download', self.setLogFileF)
    Script.registerSwitch('O:', 'OutputDir=', 'Output directory (default %s)' % self.outputdir, 
                          self.setOutputDir)
    Script.setUsageMessage('%s -F /ilc/prod/.../LOG/.../somefile' % Script.scriptName)


def printErrorReport(res):
  """Print the failures for the call to getFile or Directory"""
  if res['Value'] and res['Value']['Failed']:
    for lfn in res['Value']['Failed']:
      gLogger.error("%s %s" % (lfn,res['Value']['Failed'][lfn]) )
      return S_ERROR()
  return S_OK()

def getProdLogs():
  """get production log files from LogSE"""
  clip = Params()
  clip.registerSwitch()
  Script.parseCommandLine()
  if not clip.logF and not clip.logD:
    Script.showHelp()
    dexit(1)
  from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
  ops = Operations()
  storageElementName = ops.getValue('/LogStorage/LogSE', 'LogSE')
  from DIRAC.Resources.Storage.StorageElement import StorageElement
  logSE = StorageElement(storageElementName)

  from DIRAC.Core.Utilities.PromptUser import promptUser
  if clip.logD:
    res = promptUser('Are you sure you want to get ALL the files in this directory?')
    if not res['OK']:
      dexit()
    choice = res['Value']
    if choice.lower()=='n':
      dexit(0)
    res = logSE.getDirectory(clip.logD, localPath=clip.outputdir)
    printErrorReport(res)

  if clip.logF:
    res = logSE.getFile(clip.logF, localPath = clip.outputdir)
    printErrorReport(res)

if __name__ == '__main__':
  getProdLogs()
