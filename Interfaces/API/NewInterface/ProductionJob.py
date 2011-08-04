'''
Created on Jul 28, 2011

@author: Stephane Poss
'''

from ILCDIRAC.Interfaces.API.NewInterface.Job import Job
from DIRAC.Resources.Catalog.Client.FileCatalogClient import FileCatalogClient
from DIRAC import S_OK,S_ERROR

class ProductionJob(Job):
  def __init__(self):
    Job.__init__(self)
    self.fc = FileCatalogClient()
    
  def setInputDataQuery(self,metadict):
    """ Define the input data query needed
    """
    res = self.fc.findFilesByMetadata(metadict)
    if not res['OK']:
      return res
    """ Also get the compatible metadata such as energy, evttype, etc, populate dictionary
    """
    return S_OK()
  
  def createProd(self):
    """ Create production.
    """
    return S_OK()
  
  def finalizeProd(self):
    """ Finalize definition: submit to Transformation service
    """
    return S_OK()  
  
  def _jobSpecificParams(self,app):
    """ For production additional checks are needed: ask the user
    """
    if not app.logfile:
      logf = app.appname+"_"+app.version+".log"
      app.setLogFile(logf)
      #in fact a bit more tricky as the log files have the prodID and jobID in them
    return S_OK()
  