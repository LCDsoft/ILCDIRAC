'''
Created on Sep 8, 2010

@author: sposs
'''
from ILCDIRAC.Workflow.Modules.ModuleBase                  import ModuleBase
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
import string,os

class RegisterOutputData(ModuleBase):
  def __init__(self):
    ModuleBase.__init__(self)
    self.version = "RegisterOutputData v1"
    self.log = gLogger.getSubLogger( "RegisterOutputData" )
    self.commandTimeOut = 10*60
    self.jobID = ''
    self.enable=True
    self.prodOutputLFNs =[]
    self.fc = FileCatalogClient()

  def resolveInputVariables(self):
    if self.step_commons.has_key('Enable'):
      self.enable=self.step_commons['Enable']
      if not type(self.enable)==type(True):
        self.log.warn('Enable flag set to non-boolean value %s, setting to False' %self.enable)
        self.enable=False
        
    if self.workflow_commons.has_key('ProductionOutputData'):
      self.prodOutputLFNs=self.workflow_commons['ProductionOutputData'].split(";")
    else:
      self.prodOutputLFNs = []
    return S_OK('Parameters resolved')
  
  def execute(self):
    self.log.info('Initializing %s' %self.version)
    result = self.resolveInputVariables()
    if not result['OK']:
      self.log.error(result['Message'])
      return result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' %(self.workflowStatus['OK'],self.stepStatus['OK']))
      return S_OK('No registration of output data metadata attempted')

    if len(self.prodOutputLFNs)==0:
      self.log.info('No production data found, so no metadata registration to be done')  
      return S_OK("No files' metadata to be registered")
    
    self.log.verbose("Will try to set the metadata for the following files: \n %s"%string.join(self.prodOutputLFNs,"\n"))

    for files in self.prodOutputLFNs:
      elements = files.split("/")
      meta = {}
      meta['Machine']=elements[3]
      meta['Energy']=elements[4]
      meta['EvtType']=elements[5]
      if elements[6].lower() == 'gen':
        meta['Datatype']=elements[6]
        meta['ProdID'] = elements[7]
      else:
        meta['DetectorType']=elements[6]
        meta['Datatype']=elements[7]
        meta['ProdID'] = elements[8]
      for key,value in meta.items():
        res = self.fc.setMetadata(os.path.dirname(files),key,value)
        if not res['OK']:
          self.log.error('Could not register metadata %s, with value %s for %s'%(key, value, files))
          return S_ERROR('Error registering file metadata')
    
    return S_OK('Output data metadata registered in catalog')
  
  