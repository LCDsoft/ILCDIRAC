#!/bin/env python
'''
Get production logs

Created on Mar 21, 2013

@author: stephane
'''
from DIRAC.Core.Base import Script
from DIRAC import gLogger, S_OK, exit as dexit
from DIRAC.Core.Utilities import PromptUser

class Params(object):
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
    Script.registerSwitch('D:', 'LogFileDir=', 'Production log dir to download', self.setLogFileD)
    Script.registerSwitch('F:', 'LogFile=', 'Production log to download', self.setLogFileF)
    Script.registerSwitch('O:', 'OutputDir=', 'Output directory (default %s)' % self.outputdir, 
                          self.setOutputDir)
    Script.setUsageMessage('%s -F /ilc/prod/.../LOG/.../somefile' % Script.scriptName)

if __name__ == '__main__':
  clip = Params()
  clip.registerSwitch()
  Script.parseCommandLine()
  if not clip.logF and not clip.logD:
    Script.showHelp()
    dexit(1)
  from DIRAC import gConfig
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
  