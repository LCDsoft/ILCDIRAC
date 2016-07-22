'''
SID DBD specific registration of file meta data

Created on Sep 8, 2010

:author: sposs
:author: jmccormi
'''
__RCSID__ = "$Id$"

from ILCDIRAC.Workflow.Modules.ModuleBase                  import ModuleBase
from DIRAC.Resources.Catalog.FileCatalogClient             import FileCatalogClient

from DIRAC import S_OK, gLogger

class SIDRegisterOutputData(ModuleBase):
  """ Register output data in the FC for the SID productions 
  """
  def __init__(self):
    super(SIDRegisterOutputData, self).__init__()
    self.version = "SIDRegisterOutputData v1"
    self.log = gLogger.getSubLogger( "SIDRegisterOutputData" )
    self.commandTimeOut = 10 * 60
    self.enable = True
    self.prodOutputLFNs = []
    self.swpackages = []
    self.nbofevents = 0
    self.luminosity = 0
    self.filecatalog = FileCatalogClient()

  def applicationSpecificInputs(self):
    if 'Enable' in self.step_commons:
      self.enable = self.step_commons['Enable']
      if not isinstance( self.enable, bool ):
        self.log.warn('Enable flag set to non-boolean value %s, setting to False' % self.enable)
        self.enable = False
        
    if 'ProductionOutputData' in self.workflow_commons:
      self.prodOutputLFNs = self.workflow_commons['ProductionOutputData'].split(";")
    else:
      self.prodOutputLFNs = []
      
    if 'SoftwarePackages' in self.workflow_commons:
      self.swpackages = self.workflow_commons['SoftwarePackages'].split(";")

    self.nbofevents = self.NumberOfEvents #comes from ModuleBase
    if 'Luminosity' in self.workflow_commons:
      self.luminosity = self.workflow_commons['Luminosity']
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
    
    self.log.verbose("Will try to set the metadata for the following files: \n %s" % '\n'.join(self.prodOutputLFNs))

    for files in self.prodOutputLFNs:

      meta = {}  

      if self.nbofevents:
        nbevts = {}
        nbevts['NumberOfEvents'] = self.nbofevents
        if self.enable:
          res = self.filecatalog.setMetadata(files, nbevts)
          if not res['OK']:
            self.log.error('Could not register metadata NumberOfEvents, with value %s for %s' % (self.nbofevents, 
                                                                                                 files))
            return res
        meta.update(nbevts)
      if self.luminosity:
        lumi = {}
        lumi['Luminosity'] = self.luminosity
        if self.enable:
          res = self.filecatalog.setMetadata(files, lumi)
          if not res['OK']:
            self.log.error('Could not register metadata Luminosity, with value %s for %s'%(self.luminosity, files))
            return res
        meta.update(lumi)
#      meta.update(metaprodid)
      
      if self.WorkflowStartFrom:
        meta.update({"FirstEventFromInput":self.WorkflowStartFrom})

      
      self.log.info("Registered %s with tags %s"%(files, meta))
      
      ###Now, set the ancestors
      if self.InputData:
        inputdata = self.InputData
        if self.enable:
          res = self.filecatalog.addFileAncestors({files : {'Ancestors' : inputdata}})
          if not res['OK']:
            self.log.error('Registration of Ancestors for %s failed' % files)
            self.log.error("Because of", res['Message'])
            return res
    
    return S_OK('Output data metadata registered in catalog')
  
  