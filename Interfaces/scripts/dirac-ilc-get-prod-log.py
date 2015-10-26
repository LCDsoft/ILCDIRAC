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
    self.prodid = ''
  def setLogFileD(self,opt):
    self.logD = opt
    return S_OK()
  def setLogFileF(self,opt):
    self.logF = opt
    return S_OK()
  def setOutputDir(self,opt):
    self.outputdir = opt
    return S_OK()
  def setProdID(self,opt):
    self.prodid = opt
    return S_OK()
  def registerSwitch(self):
    """registers switches"""
    Script.registerSwitch('D:', 'LogFileDir=', 'Production log dir to download', self.setLogFileD)
    Script.registerSwitch('F:', 'LogFile=', 'Production log to download', self.setLogFileF)
    Script.registerSwitch('O:', 'OutputDir=', 'Output directory (default %s)' % self.outputdir, 
                          self.setOutputDir)
    Script.registerSwitch('P:', 'ProdID=', 'Production ID', self.setProdID)
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
  if not ( clip.logF or clip.logD or clip.prodid ):
    Script.showHelp()
    dexit(1)
  from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
  ops = Operations()
  storageElementName = ops.getValue('/LogStorage/LogSE', 'LogSE')
  from DIRAC.Resources.Storage.StorageElement import StorageElementItem as StorageElement
  logSE = StorageElement(storageElementName)

  from DIRAC.Core.Utilities.PromptUser import promptUser
  if clip.prodid and not ( clip.logD or clip.logF ):
    from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
    from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
    server = TransformationClient()
    result = server.getTransformation( clip.prodid )
    if not result['OK']:
      gLogger.error( result['Message'] )
      return S_ERROR()

    query = { 'ProdID' : clip.prodid }
    fc = FileCatalogClient()
    result = fc.findFilesByMetadata(query, '/')
    if not result['OK']:
      gLogger.error( result['Message'] )
      return S_ERROR()
    elif result['Value']:
      lfn = result['Value'][0]
      lfn_split = lfn.split('/')[:-2]
      lfn_split.extend( ['LOG', lfn.split('/')[-2] ])
      clip.logD = '/'.join(lfn_split)
      print 'Set logdir to %s' %clip.logD
    else:
      print "Cannot discover the LogFilePath: No output files yet"

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
