'''
Goes with DBDGeneration, not used

:since: Jan 26, 2012

:author: Stephane Poss
'''
__RCSID__ = "$Id$"
from ILCDIRAC.Workflow.Modules.ModuleBase                  import ModuleBase

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

from DIRAC import S_OK, gLogger

class DBDGenRegisterOutputData(ModuleBase):
  """ Normally, was supposed to be used to produce the DBD gen level files. Dropped in the end.
  """
  def __init__(self):
    super(DBDGenRegisterOutputData).__init__()
    self.version = "DBDGenRegisterOutputData v1"
    self.log = gLogger.getSubLogger( "DBDGenRegisterOutputData" )
    self.commandTimeOut = 10 * 60
    self.enable = True
    self.fcc = FileCatalogClient()
    self.nbofevents = 0
    self.prodOutputLFNs = []
    
  def applicationSpecificInputs(self):
    if 'ProductionOutputData' in self.workflow_commons:
      self.prodOutputLFNs = self.workflow_commons['ProductionOutputData'].split(";")
      
    if 'NbOfEvts' in self.workflow_commons:
      self.nbofevents = self.workflow_commons[ 'NbOfEvts']          
    return S_OK("Parameters resolved")
      
  def execute(self):
    self.log.info('Initializing %s' % self.version)
    result = self.resolveInputVariables()
    if not result['OK']:
      self.log.error(result['Message'])
      return result
    
    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('No registration of output data metadata attempted')

    if not len(self.prodOutputLFNs):
      self.log.info('No production data found, so no metadata registration to be done')  
      return S_OK("No files' metadata to be registered")
    
    self.log.verbose("Will try to set the metadata for the following files: \n %s" % '\n'.join(self.prodOutputLFNs))
    
    for files in self.prodOutputLFNs:
      metadict = {}
      metadict['NumberOfEvents'] = self.nbofevents
      path = files
      
      res = self.fcc.setMetadata(files, metadict)
      if not res['OK']:
        self.log.error("Could not register %s for %s"%(metadict, path))
        return res
    
    return S_OK()
