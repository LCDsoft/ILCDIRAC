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
    self.swpackages = []
    self.nbofevents = 0
    self.luminosity = 0
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
      
    if self.workflow_commons.has_key('SoftwarePackages'):
      self.swpackages = self.workflow_commons['SoftwarePackages'].split(";")

    if self.workflow_commons.has_key('NbOfEvents'):
      self.nbofevents = self.workflow_commons['NbOfEvents']
    if self.workflow_commons.has_key('Luminosity'):
      self.luminosity = self.workflow_commons['Luminosity']
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
      machine = string.join(elements[0:4],"/")
      res = self.fc.setMetadata(machine,"Machine",elements[3])
      if not res['OK']:
        self.log.error('Could not register metadata Machine, with value %s for %s'%(elements[3],machine))
        return res
      meta['Energy']=elements[4]
      energy = string.join(elements[0:5],"/")
      res = self.fc.setMetadata(energy,"Energy",elements[4])
      if not res['OK']:
        self.log.error('Could not register metadata Energy, with value %s for %s'%(elements[4],energy))
        return res      
      meta['EvtType']=elements[5]
      evttype = string.join(elements[0:6],"/")
      res = self.fc.setMetadata(evttype,"EvtType",elements[5])
      if not res['OK']:
        self.log.error('Could not register metadata EvtType, with value %s for %s'%(elements[5],evttype))
        return res
      prodid = ''
      if elements[6].lower() == 'gen':
        meta['Datatype']=elements[6]
        datatype = string.join(elements[0:7],"/")
        res = self.fc.setMetadata(datatype,"Datatype",elements[6])
        if not res['OK']:
          self.log.error('Could not register metadata Datatype, with value %s for %s'%(elements[6],datatype))
          return res
        meta['ProdID'] = elements[7]
        prodid = string.join(elements[0:8],"/")
        res = self.fc.setMetadata(prodid,"ProdID",elements[7])
        if not res['OK']:
          self.log.error('Could not register metadata ProdID, with value %s for %s'%(elements[7],prodid))
          return res
        
      else:
        meta['DetectorType']=elements[6]
        detectortype=string.join(elements[0:7],"/")
        res = self.fc.setMetadata(detectortype,"DetectorType",elements[6])
        if not res['OK']:
          self.log.error('Could not register metadata DetectorType, with value %s for %s'%(elements[6],detectortype))
          return res
        meta['Datatype']=elements[7]
        datatype = string.join(elements[0:8],"/")
        res = self.fc.setMetadata(datatype,"Datatype",elements[7])
        if not res['OK']:
          self.log.error('Could not register metadata Datatype, with value %s for %s'%(elements[7],datatype))
          return res 
        meta['ProdID'] = elements[8]
        prodid = string.join(elements[0:9],"/")
        res = self.fc.setMetadata(prodid,"ProdID",elements[8])
        if not res['OK']:
          self.log.error('Could not register metadata ProdID, with value %s for %s'%(elements[8],prodid))
          return res
      if self.nbofevents:
        meta['NumberOfEvents']=self.nbofevents
        res = self.fc.setMetadata(prodid,'NumberOfEvents',self.nbofevents)
        if not res['OK']:
          self.log.error('Could not register metadata NumberOfEvents, with value %s for %s'%(self.nbofevents,prodid))
          return res
      if self.luminosity:
        meta['Luminosity']=self.luminosity
        res = self.fc.setMetadata(prodid,'Luminosity',self.luminosity)
        if not res['OK']:
          self.log.error('Could not register metadata Luminosity, with value %s for %s'%(self.luminosity,prodid))
          return res
      self.log.info("Registered %s with tags %s"%(files,meta))
      # FIXME: in next DIRAC release, remove loop and replace key,value below by meta  
      #for key,value in meta.items():
      #  res = self.fc.setMetadata(os.path.dirname(files),key,value)
      #  if not res['OK']:
      #    self.log.error('Could not register metadata %s, with value %s for %s'%(key, value, files))
      #    return res
    
    return S_OK('Output data metadata registered in catalog')
  
  