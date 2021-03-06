'''
Register the meta data of the production files

:since: Sep 8, 2010

:author: sposs
'''

import os

from DIRAC.Resources.Catalog.FileCatalogClient    import FileCatalogClient
from DIRAC.Core.Utilities                         import DEncode
from DIRAC import S_OK, gLogger

from ILCDIRAC.Workflow.Modules.ModuleBase         import ModuleBase

__RCSID__ = '$Id$'
LOG = gLogger.getSubLogger(__name__)


class RegisterOutputData( ModuleBase ):
  """ At the end of a production Job, we need to register meta data info for the files. 
  """
  def __init__(self):
    super(RegisterOutputData, self).__init__()
    self.version = "RegisterOutputData v1"
    self.commandTimeOut = 10*60
    self.enable = True
    self.prodOutputLFNs = []
    self.nbofevents = 0
    self.luminosity = 0
    self.sel_eff = 0
    self.cut_eff = 0
    self.add_info = ''
    self.filecatalog = FileCatalogClient()
    self.filemeta = {}

  def applicationSpecificInputs(self):
    if 'Enable' in self.step_commons:
      self.enable = self.step_commons['Enable']
      if not isinstance( self.enable, bool):
        LOG.warn('Enable flag set to non-boolean value %s, setting to False' % self.enable)
        self.enable = False
        
    if 'ProductionOutputData' in self.workflow_commons:
      self.prodOutputLFNs = self.workflow_commons['ProductionOutputData'].split(";")
    else:
      self.prodOutputLFNs = []
      
    self.nbofevents = self.NumberOfEvents
    if 'Luminosity' in self.workflow_commons:
      self.luminosity = self.workflow_commons['Luminosity']
    
    ##Additional info: cross section only for the time being, comes from WHIZARD
    if 'Info' in self.workflow_commons:
      if 'stdhepcut' in self.workflow_commons['Info']:
        self.sel_eff = self.workflow_commons['Info']['stdhepcut']['Reduction']
        self.cut_eff = self.workflow_commons['Info']['stdhepcut']['CutEfficiency']
        del self.workflow_commons['Info']['stdhepcut']
      self.add_info = DEncode.encode(self.workflow_commons['Info'])
    
    return S_OK('Parameters resolved')
  
  def execute(self):
    """ Run the module
    """
    LOG.info('Initializing %s' % self.version)
    result = self.resolveInputVariables()
    if not result['OK']:
      LOG.error("failed to resolve input parameters:", result['Message'])
      return result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      LOG.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('No registration of output data metadata attempted')

    if len(self.prodOutputLFNs) == 0:
      LOG.info('No production data found, so no metadata registration to be done')
      return S_OK("No files' metadata to be registered")
    
    LOG.verbose("Will try to set the metadata for the following files: \n %s" % "\n".join(self.prodOutputLFNs))

    for files in self.prodOutputLFNs:
      metafiles = {}

      if self.nbofevents:
        nbevts = {}
        nbevts['NumberOfEvents'] = self.nbofevents
        if 'file_number_of_event_relation' in self.workflow_commons:
          if os.path.basename(files) in self.workflow_commons['file_number_of_event_relation']:
            nbevts['NumberOfEvents'] = self.workflow_commons['file_number_of_event_relation'][os.path.basename(files)]
        metafiles.update(nbevts)  
      if self.luminosity:
        metafiles.update({'Luminosity': self.luminosity})
      
      if self.sel_eff:
        metafiles.update({'Reduction':self.sel_eff})
      if self.cut_eff:
        metafiles.update({'CutEfficiency': self.cut_eff})  
      if self.add_info:
        metafiles.update({'AdditionalInfo':self.add_info})
        
      elif 'AdditionalInfo' in self.inputdataMeta:
        metafiles.update({'AdditionalInfo':self.inputdataMeta['AdditionalInfo']})
      
      if 'CrossSection' in self.inputdataMeta:
        metafiles.update({'CrossSection':self.inputdataMeta['CrossSection']})
      
      if self.WorkflowStartFrom:
        metafiles.update({"FirstEventFromInput":self.WorkflowStartFrom})
      
      if self.enable:
        res = self.filecatalog.setMetadata(files, metafiles)
        if not res['OK']:
          LOG.error(res['Message'])
          LOG.error('Could not register metadata for %s' % files)
          return res
        
      LOG.info("Registered %s with tags %s" % (files, metafiles))
      
      ###Now, set the ancestors
      if self.InputData:
        if self.enable:
          res = self.filecatalog.addFileAncestors({ files : {'Ancestors' : self.InputData } })
          if not res['OK']:
            LOG.error('Registration of Ancestors for %s failed' % files)
            LOG.error('Because of ', res['Message'])
            return res

    return S_OK('Output data metadata registered in catalog')
