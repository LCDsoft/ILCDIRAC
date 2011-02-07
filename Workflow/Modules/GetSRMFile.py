'''
Created on Aug 27, 2010

@author: sposs
'''
__RCSID__ = "$Id: GetSRMFile.py sposs$"

from DIRAC.DataManagementSystem.Client.ReplicaManager      import ReplicaManager

from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
import DIRAC
import os,types,tempfile

class GetSRMFile(ModuleBase):
  def __init__(self):
    """Module initialization.
    """
    ModuleBase.__init__(self)
    self.version = __RCSID__
    self.log = gLogger.getSubLogger('GetSRMFile')
    self.rm = ReplicaManager()
    self.filestxt = ""
    self.files = []
    self.counter=1
    
  def applicationSpecificInputs(self):
    if self.step_commons.has_key("srmfiles"):
      self.filestxt = self.step_commons["srmfiles"]
    if self.filestxt:
      listoffiles = self.filestxt.split(";")
      for f in listoffiles:
        self.files.append(eval(f))
    return S_OK()
  
  def execute(self):
    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' %(self.workflowStatus['OK'],self.stepStatus['OK']))
      return S_OK('Workflow status is not OK')
    result = self.resolveInputVariables()
    if not result['OK']:
      return result
    if not self.filestxt:
      self.log.error('Files txt where not found correctly: %s'%self.filestxt)
      return S_ERROR('Files txt where not found correctly: %s'%self.filestxt)
    
    if not type(self.files[0]) is type({}):
      self.log.error('Files where not found correctly: %s'%self.files)
      return S_ERROR('Files where not found correctly: %s'%self.files)
    
    for filed in self.files:
      if not filed.has_key('file') or not filed.has_key('site'):
        self.log.error('Dictionnary does not contain correct keys')
        return S_ERROR('Dictionnary does not contain correct keys')
      start = os.getcwd()
      downloadDir = tempfile.mkdtemp(prefix='InputData_%s' %(self.counter), dir=start)
      os.chdir(downloadDir)
      result = self.rm.getStorageFile(filed['file'], filed['site'], singleFile=True)
      os.chdir(start)
      if not result['OK']:
        return result
      self.counter+=1
      
       
    return S_OK()
