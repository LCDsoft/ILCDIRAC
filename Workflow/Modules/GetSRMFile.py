#####################################################
# $HeadURL: $
#####################################################
'''
Module that gets a file from its SRM definition

Created on Aug 27, 2010

@author: sposs
'''
__RCSID__ = "$Id: $"

from DIRAC.DataManagementSystem.Client.ReplicaManager      import ReplicaManager
from DIRAC.Core.DISET.RPCClient                            import RPCClient

from ILCDIRAC.Workflow.Modules.ModuleBase                  import ModuleBase
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
import DIRAC
import os,tempfile,time

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

    ##Now need to check that there are not that many concurrent jobs getting the overlay at the same time
    res = gConfig.getOption('/Operations/GetSRM/MaxConcurrentRunning',100)
    max_concurrent_running = res['Value']
    error_count = 0
    while 1:
      if error_count > 10 :
        self.log.error('JobDB Content does not return expected dictionary')
        return S_ERROR('Failed to get number of concurrent overlay jobs')
      jobMonitor = RPCClient('WorkloadManagement/JobMonitoring',timeout=60)
      res = jobMonitor.getCurrentJobCounters({'ApplicationStatus':'Downloading SRM files'})
      if not res['OK']:
        error_count += 1 
        time.sleep(60)
        continue
      running = 0
      if res['Value'].has_key('Running'):
        running = res['Value']['Running']
      if running < max_concurrent_running:
        break
      else:
        time.sleep(60)        

    self.setApplicationStatus('Downloading SRM files')
    for filed in self.files:
      if not filed.has_key('file') or not filed.has_key('site'):
        self.log.error('Dictionnary does not contain correct keys')
        return S_ERROR('Dictionnary does not contain correct keys')
      start = os.getcwd()
      downloadDir = tempfile.mkdtemp(prefix='InputData_%s' %(self.counter), dir=start)
      os.chdir(downloadDir)

      result = self.rm.getStorageFile(filed['file'], filed['site'], singleFile=True)
      if not result['OK']:
        result = self.rm.getStorageFile(filed['file'], filed['site'], singleFile=True)
      os.chdir(start)
      if not result['OK']:
        return result
      self.counter+=1
      
       
    return S_OK()
