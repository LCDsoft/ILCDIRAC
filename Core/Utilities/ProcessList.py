'''
Created on Sep 21, 2010

@author: sposs
'''
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.CFG import CFG
import os, tempfile, shutil

class ProcessList:
  def __init__(self,location):
    self.cfg = CFG()
    self.location = location
    self.OK = True
    if os.path.exists(self.location):
      self.cfg.loadFromFile(self.location)
      if not self.cfg.existsKey('Processes'):
        self.cfg.createNewSection('Processes')
    else:
      self.OK = False  
    written = self._writeProcessList(self.location)
    if not written:
      self.OK = False
      
  def _writeProcessList(self,path):
    handle,tmpName = tempfile.mkstemp()
    written = self.repo.writeToFile(tmpName)
    os.close(handle)
    if not written:
      if os.path.exists(tmpName):
        os.remove(tmpName)
      return written
    if os.path.exists(path):
      gLogger.debug("Replacing %s" % path)
    try:
      shutil.move(tmpName, path)
      return True
    except Exception,x:
      gLogger.error("Failed to overwrite process list.", x)
      gLogger.info("If your process list is corrupted a backup can be found %s" % tmpName)
      return False
    
  def isOK(self):
    return self.OK
    
  def setCSPath(self,processdic,path):
    processExists = self._existsProcess(processdic['process'])
    if not processExists:
      res = self._addEntry(processdic, path)
      return res
    else:
      gLogger.info("Process %s already defined in ProcessList, will replace it"%processdic['process'])
      self.cfg.deleteKey("Processes/%s"%processdic['process'])
      res = self._addEntry(processdic, path)
      return res
    
  def _addEntry(self,processdic,path):
    self.cfg.createNewSection("Processes/%s"%processdic['process'])
    self.cfg.setOption("Processes/%s/TarBallCSPath"%processdic['process'], path)
    self.cfg.setOption("Processes/%s/Detail"%processdic['process'], processdic['detail'])
    self.cfg.setOption("Processes/%s/Generator"%processdic['process'], processdic['generator'])

    return    
  
  def getCSPath(self,process):
    return self.cfg.getOption("Processes/%s/TarBallCSPath"%process, None)
  
  def existsProcess(self,process):
    return S_OK(self._existsProcess(process))

  def _existsProcess(self,process):
    return self.cfg.isSection('Processes/%s' % process)

  def writeProcessList(self, alternativePath=None):
    destination = self.location
    if alternativePath:
      destination = alternativePath
    written = self._writeProcessList(destination)
    if not written:
      return S_ERROR("Failed to write repository")
    return S_OK(destination) 
  