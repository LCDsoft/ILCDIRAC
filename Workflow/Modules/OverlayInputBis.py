#####################################################
# $HeadURL: $
#####################################################
'''
Created on Jun 20, 2011

@author: Stephane Poss
'''
__RCSID__ = "$Id: $"

from ILCDIRAC.Workflow.Modules.ModuleBase                    import ModuleBase
from DIRAC.DataManagementSystem.Client.ReplicaManager        import ReplicaManager
from DIRAC.Resources.Catalog.FileCatalogClient               import FileCatalogClient
from DIRAC                                                   import S_OK, S_ERROR, gLogger, gConfig
import DIRAC,os,time


class OverlayInput(ModuleBase):
  def __init__(self):
    ModuleBase.__init__(self)
    self.enable = True
    self.STEP_NUMBER = ''
    self.log = gLogger.getSubLogger( "OverlayInput" )
    self.applicationName = 'OverlayInput'
    self.applicationLog = 'Overlay.log'
    self.printoutflag = ''
    self.prodid = 0
    self.detector = ""
    self.energy='3tev'
    self.rm = ReplicaManager()
    self.fc = FileCatalogClient()
    self.site = DIRAC.siteName()
    
  def applicationSpecificInputs(self):
    
    if self.step_commons.has_key('Detector'):
      self.detector = self.step_commons['Detector']
    else:
      return S_ERROR('Detector model not defined')
    
    if self.step_commons.has_key('Energy'):
      self.energy = self.step_commons['Energy']
    
    return S_OK("Input variables resolved")
  
  def getFile(self,fname,location):
    res = self.rm.getFile(fname,location)
    if not res['OK']:
      return res
    return S_OK()
  
  def checkFile(self):
    res = gConfig.getOption("/Operations/Overlay/%s/%s/FileName"%(self.detector,self.energy),'')
    if not res['OK']:
      return S_ERROR("File Name not found")
    if not res['Value']:
      return S_ERROR("File Name not specified")    
    fname = res['Value']
    localname = os.path.basename(fname)
    
    res = gConfig.getOption("/Operations/Overlay/%s/%s/FileSize"%(self.detector,self.energy),0)
    if not res['OK']:
      return S_ERROR("File size not found")
    if not res['Value']:
      return S_ERROR("File size not specified")    
    fsize = res['Value']
    res = gConfig.getOption("/Operations/Overlay/MaxFailedAllowed",20)
    if not res['OK']:
      return S_ERROR("Max Failed allowed not found")
    if not res['Value']:
      return S_ERROR("Max Failed allowed not specified")    
    max_stall_count = res['Value']
    
    location = 'SomeLocationTObeDefined/'
    
    failing = False
    nsize = 0
    count_stall = 0
    if not os.path.exists(location+localname):
      res = self.getFile(fname,location)
      if not res['OK']:
        return S_ERROR("File %s could not be obtained"%res['Value'])
    else:
      size = os.path.getsize(location+localname)
      if not size == fsize:
        while 1:
          time.sleep(20)
          size = os.path.getsize(location+localname)
          if size == fsize:
            failing = False
            break
          
          if size>nsize:
            nsize = size
          else:
            count_stall+=1
            if count_stall>max_stall_count:
              failing = True
              break
          
    if failing:
      return S_ERROR("File seems not to be downloaded")
    return S_OK()
  
  def getFileLocation(self):
    return S_OK()
  
  def execute(self):
    self.result = self.resolveInputVariables()
    if not self.result['OK']:
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' %(self.workflowStatus['OK'],self.stepStatus['OK']))
      return S_OK('OverlayInput should not proceed as previous step did not end properly')
    self.setApplicationStatus('Starting up Overlay')
    ###Don't check for CPU time as other wise, job can get killed
    if os.path.exists('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK'):
      os.remove('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK')
    f = file('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK','w')
    f.write('Dont look at cpu')
    f.close()

    os.remove('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK')
    
    res = self.getFilelocation()
    
    if not res['OK']:
      self.setApplicationStatus('OverlayProcessor failed to get files locally with message %s'%res['Message'])
      return S_ERROR('OverlayProcessor failed to get files locally')
    self.setApplicationStatus('Overlay processor finished getting all files successfully')
    return S_OK('Overlay input finished successfully')