'''
Module that gets a file from its SRM definition

@since: Aug 27, 2010

@author: sposs
'''
__RCSID__ = "$Id$"

from DIRAC.Core.DISET.RPCClient                            import RPCClient
from DIRAC.Resources.Storage.StorageElement import StorageElementItem as StorageElement
from ILCDIRAC.Workflow.Modules.ModuleBase                  import ModuleBase
from DIRAC import S_OK, S_ERROR, gLogger
import os, tempfile, time

class GetSRMFile(ModuleBase):
  """ When a file is not in the FileCatalog, it can still be obtained using this. and specifying the srm path.
  """
  def __init__(self):
    """Module initialization.
    """
    super(GetSRMFile, self).__init__()
    self.version = __RCSID__
    self.log = gLogger.getSubLogger('GetSRMFile')
    self.srmfiles = []
    self.files = []
    self.counter = 1
    
  def applicationSpecificInputs(self):
    """ Resolve the srm files to get
    """
    if not self.srmfiles:
      return S_ERROR("List of files to treat is not set")
    self.files = self.srmfiles
    return S_OK()
  
  def execute(self):
    """ Run this.
    """
    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' %(self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('Workflow status is not OK')
    result = self.resolveInputVariables()
    if not result['OK']:
      self.log.error("Failed to resolve input parameters:", result["Message"])
      return result
    if not self.srmfiles:
      self.log.error('Files txt where not found correctly: %s' % self.srmfiles)
      return S_ERROR('Files txt where not found correctly: %s' % self.srmfiles)
    
    if not type(self.files[0]) is type({}):
      self.log.error('Files where not found correctly: %s' % self.files)
      return S_ERROR('Files where not found correctly: %s' % self.files)

    ##Now need to check that there are not that many concurrent jobs getting the overlay at the same time
    max_concurrent_running = self.ops.getValue('/GetSRM/MaxConcurrentRunning', 100)
    error_count = 0
    while 1:
      if error_count > 10 :
        self.log.error('JobDB Content does not return expected dictionary')
        return S_ERROR('Failed to get number of concurrent overlay jobs')
      jobMonitor = RPCClient('WorkloadManagement/JobMonitoring', timeout = 60)
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
      downloadDir = tempfile.mkdtemp(prefix = 'InputData_%s' % (self.counter), dir = start)
      os.chdir(downloadDir)
      storageElement = StorageElement( filed['site'] )
      result = storageElement.getFile( filed['file'] )
      if result['Value']['Failed']:
        result = storageElement.getFile( filed['file'] )
      os.chdir(start)
      if result['Value']['Failed']:
        self.log.error("Failed to get the file from storage:", result['Value']['Failed'])
        return result
      self.counter += 1
      
       
    return S_OK()
