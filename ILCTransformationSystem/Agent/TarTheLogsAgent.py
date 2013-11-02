##################################################################################################################
# $HeadURL$
##################################################################################################################
'''
Created on Nov 2, 2013

@author: sposs
'''

from DIRAC.Core.Base.AgentModule                         import AgentModule
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

from DIRAC import S_OK, S_ERROR, gLogger

import glob

__RCSID__ = "$Id$"

class TarTheProdLogsAgent( AgentModule ):
  '''
  Tar the prod logs, and send them to CASTOR
  '''


  def __init__( self, *args, **kwargs ):
    '''
    Constructor
    '''
    AgentModule.__init__( self, *args, **kwargs )
    self.name = "TarTheProdLogsAgent"
    self.log = gLogger
    self.basepath = ""

  def initialize(self):
    """Sets defaults
    """
    self.am_setModuleParam("shifterProxy", "ProductionManager")
    self.basepath = self.am_getOption("BasePath", "")
    if not self.basepath:
      return S_ERROR("Missing mandatory option Basepath")

    self.ops = Operations()
    self.dest_se = self.ops.getValue("Transformations/ArchiveSE", "")
    if not self.dest_se:
      return S_ERROR("Archival SE option not defined")
    
    self.log.info("Running ")
    return S_OK()
  
  def execute(self):
    """ Run it!
    """
    res = self.getDirectories()
    if not res["OK"]:
      return res
    
    res = self.getProductionIDs(res["Value"])
    if not res["OK"]:
      return res
    
    prods = res['Value']
    for prod in prods:

      if not self.transIsStopped(prod):
        continue
      
      prodFiles = self.getFiles(prod)
      if not prodFiles:
        continue
      res = self.createTarBall(prodFiles)
      if not res["OK"]:
        self.log.error("Could not get the tar ball:", res["Message"])
        continue 
      tarBall = res["Value"]
      res = self.uploadToStorage(tarBall)
      if not res["OK"]:
        self.log.error("Failed putting the file to storage:", res["Message"])

      
      res = self.cleanTarBall(tarBall)
      if not res["OK"]:
        self.log.error("Failed removong the Tar Ball", res["Message"])
          
      
    return S_OK()
  
  def getDirectories(self):
    """ List the directories below the base
    """
    dirs = glob.glob(self.basepath+"/*/*/*")
    return S_OK(dirs)

  def getProductionIDs(self, directories):
    """ Given input directories, get the prods
    """
    prods = []
    return S_OK(prods)
  
  def transIsStopped(self, prod):
    """ Check from the TS if the prod is Active or not
    """
    return False
  
  def getFiles(self, prod):
    """ get the files in the directory of the prod
    """
    flist = []
    return S_OK(flist)
  
  def cleanTarBall(self, tarballpath):
    """ Physically remove the tar ball that was created to free disk space
    """
    return S_OK()
  
  def uploadToStorage(self, tarballpath):
    """ Put the file to the Storage Element
    """
    return S_OK()
  
  def createTarBall(self, prodFiles):
    """ Create and return the path to the tar ball contanining aall the prod files
    """
    return S_OK()
#################################################################"  
  