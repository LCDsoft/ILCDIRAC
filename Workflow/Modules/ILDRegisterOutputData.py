'''
ILD specific registration of file meta data

:since: Mar 21, 2013
:author: sposs
'''
__RCSID__ = "$Id$"

from ILCDIRAC.Workflow.Modules.ModuleBase                  import ModuleBase
from DIRAC.Resources.Catalog.FileCatalogClient             import FileCatalogClient

from DIRAC import S_OK, gLogger
import os

class ILDRegisterOutputData(ModuleBase):
  """ Register output data in the FC for the ILD productions 
  """
  def __init__(self):
    super(ILDRegisterOutputData, self).__init__()
    self.version = "ILDRegisterOutputData v1"
    self.log = gLogger.getSubLogger( "ILDRegisterOutputData" )
    self.commandTimeOut = 10 * 60
    self.enable = True
    self.prodOutputLFNs = []
    self.nbofevents = 0
    self.filecatalog = FileCatalogClient()
    self.ildconfig = ''
    self.swpackages = []
    
  def applicationSpecificInputs(self):
    if 'Enable' in self.step_commons:
      self.enable = self.step_commons['Enable']
      if not type(self.enable) == type(True):
        self.log.warn('Enable flag set to non-boolean value %s, setting to False' % self.enable)
        self.enable = False
        
    if 'ProductionOutputData' in self.workflow_commons:
      self.prodOutputLFNs = self.workflow_commons['ProductionOutputData'].split(";")
    else:
      self.prodOutputLFNs = []
      
    if 'SoftwarePackages' in self.workflow_commons:
      self.swpackages = self.workflow_commons['SoftwarePackages'].split(";")

    self.nbofevents = self.NumberOfEvents #comes from ModuleBase
    if 'ILDConfigPackage' in self.workflow_commons:
      self.ildconfig = self.workflow_commons['ILDConfigPackage']
    return S_OK('Parameters resolved')
  
  def execute(self):
    self.log.info('Initializing %s' % self.version)
    result = self.resolveInputVariables()
    if not result['OK']:
      self.log.error("Failed to resolve input parameters:", result['Message'])
      return result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('No registration of output data metadata attempted')

    if len(self.prodOutputLFNs) == 0:
      self.log.info('No production data found, so no metadata registration to be done')  
      return S_OK("No files' metadata to be registered")
    
    self.log.verbose("Will try to set the metadata for the following files: \n %s"% "\n".join(self.prodOutputLFNs))

    #TODO: What meta data should be stored at file level?

    for files in self.prodOutputLFNs:
      meta = {}  

      if self.nbofevents:
        nbevts = {}
        nbevts['NumberOfEvents'] = self.nbofevents
        if 'file_number_of_event_relation' in self.workflow_commons:
          if os.path.basename(files) in self.workflow_commons['file_number_of_event_relation']:
            nbevts['NumberOfEvents'] = self.workflow_commons['file_number_of_event_relation'][os.path.basename(files)]
        meta.update(nbevts) 
        
      if 'CrossSection' in self.inputdataMeta:
        xsec = {'CrossSection':self.inputdataMeta['CrossSection']}
        meta.update(xsec)
        
      if 'CrossSectionError' in self.inputdataMeta:
        xsec = {'CrossSectionError':self.inputdataMeta['CrossSectionError']}
        meta.update(xsec)
        
      if 'GenProcessID' in self.inputdataMeta:
        fmeta = {'GenProcessID':self.inputdataMeta['GenProcessID']}
        meta.update(fmeta)
        
      if 'GenProcessType' in self.inputdataMeta:
        fmeta = {'GenProcessType':self.inputdataMeta['GenProcessType']}
        meta.update(fmeta)
        
      if 'GenProcessName' in self.inputdataMeta:
        fmeta = {'GenProcessName':self.inputdataMeta['GenProcessName']}
        meta.update(fmeta)
        
      if 'Luminosity' in self.inputdataMeta:
        fmeta = {'Luminosity':self.inputdataMeta['Luminosity']}
        meta.update(fmeta)
        
      if 'BeamParticle1' in self.inputdataMeta:
        fmeta = {'BeamParticle1':self.inputdataMeta['BeamParticle1'],
                 'BeamParticle2':self.inputdataMeta['BeamParticle2']}
        meta.update(fmeta)
        
      if 'PolarizationB1' in self.inputdataMeta:
        fmeta = {'PolarizationB1':self.inputdataMeta['PolarizationB1'],
                 'PolarizationB2':self.inputdataMeta['PolarizationB2']}
        meta.update(fmeta)
        
      if self.ildconfig:
        fmeta = {'ILDConfig' : self.ildconfig}
        meta.update(fmeta)
      
      if self.WorkflowStartFrom:
        meta.update({"FirstEventFromInput":self.WorkflowStartFrom})

        
      if self.enable:
        res = self.filecatalog.setMetadata(files, meta)
        if not res['OK']:
          self.log.error('Could not register metadata:', res['Message'])
          return res
      self.log.info("Registered %s with tags %s"%(files, meta))
      
      ###Now, set the ancestors
      if self.InputData:
        inputdata = self.InputData
        if self.enable:
          res = self.filecatalog.addFileAncestors({files : {'Ancestors' : inputdata}})
          if not res['OK']:
            self.log.error('Registration of Ancestors failed for:', str(files) )
            self.log.error('because of', res['Message'])
            return res

    return S_OK('Output data metadata registered in catalog')
  
  