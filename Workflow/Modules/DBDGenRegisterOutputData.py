'''
Created on Jan 26, 2012

@author: Stephane Poss
'''
from ILCDIRAC.Workflow.Modules.ModuleBase                  import ModuleBase

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

from DIRAC import S_OK,S_ERROR,gLogger


class DBDGenRegisterOutputData(ModuleBase):
  def __init__(self):
    ModuleBase.__init__(self)
    self.version = "DBDGenRegisterOutputData v1"
    self.log = gLogger.getSubLogger( "DBDGenRegisterOutputData" )
    self.commandTimeOut = 10*60
    self.enable=True
    self.fc = FileCatalogClient()
    
  def execute(self):
    
    metadict = {}
    path =''
    
    res = self.fc.setMetadata(path,metadict)
    if not res['OK']:
      self.log.error("Could not register %s for %s"%(metadict,path))
      return res
    
    return S_OK()