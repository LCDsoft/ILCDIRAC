'''
Created on Sep 21, 2010

@author: sposs
'''
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.CFG import CFG
from pprint import pprint
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
    written = self.cfg.writeToFile(tmpName)
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
  
  def updateProcessList(self,processes):
    for process,dict in processes.items():
      if not self._existsProcess(process):
        res = self._addEntry(process,dict)
        return res
      else:
        gLogger.info("Process %s already defined in ProcessList, will replace it"%process)
        self.cfg.deleteKey("Processes/%s"%process)
        res = self._addEntry(process,dict)
        return res
    return S_OK()
    
  def _addEntry(self,process,processdic):
    if not self.cfg.isSection("Processes/%s"%process):
      self.cfg.createNewSection("Processes/%s"%process)
    self.cfg.setOption("Processes/%s/TarBallCSPath"%process, processdic['TarBallPath'])
    self.cfg.setOption("Processes/%s/Detail"%process, processdic['detail'])
    self.cfg.setOption("Processes/%s/Generator"%process, processdic['generator'])
    self.cfg.setOption("Processes/%s/Model"%process, processdic['model'])
    self.cfg.setOption("Processes/%s/Restrictions"%process, processdic['restrictions'])
    self.cfg.setOption("Processes/%s/InFile"%process, processdic['in_file'])
    cross_section = 0
    if processdic.has_key("cross_section"):
      cross_section=processdic["cross_section"]
    self.cfg.setOption("Processes/%s/CrossSection"%process, cross_section)
    return    
  
  def getCSPath(self,process):
    return self.cfg.getOption("Processes/%s/TarBallCSPath"%process, None)

  def getInFile(self,process):
    return self.cfg.getOption("Processes/%s/InFile"%process, None)

  def getProcesses(self):
    processesdict = self.cfg.getAsDict("Processes")
    processes = processesdict.keys()
    return processes
  
  def getProcessesDict(self):
    return self.cfg.getAsDict("Processes")
    
  
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
  
  def printProcesses(self):
    processesdict = self.cfg.getAsDict("Processes")
    #for key,value in processesdict.items():
    #  print "%s: [%s], generated with '%s' with the model '%s' using diagram restrictions %s"%(key,value['Detail'],value['Generator'],value['Model'],value['Restrictions'])
    pprint(processesdict)
  