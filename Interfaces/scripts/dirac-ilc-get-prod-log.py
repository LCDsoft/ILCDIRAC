#!/bin/env python
'''
Get production logs

Created on Mar 21, 2013

@author: stephane
'''
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC import gLogger, S_OK, exit as dexit

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
  
  from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
  rm = ReplicaManager()
  from DIRAC.Core.Utilities.PromptUser import promptUser
  if clip.logD:
    res = promptUser('Are you sure you want to get ALL the files in this directory?')
    if not res['OK']:
      dexit()
    choice = res['Value']
    if choice.lower()=='n':
      dexit(0)
    res = rm.getStorageDirectory(clip.logD, storageElementName, clip.outputdir, singleDirectory=True)
    if not res['OK']:
      gLogger.error(res['Message'])
      dexit(1)
  if clip.logF:
    res = rm.getStorageFile(clip.logF, storageElementName, clip.outputdir, singleFile = True)
    if not res['OK']:
      gLogger.error(res['Message'])
      dexit(1)


if __name__ == '__main__':
  getProdLogs()
