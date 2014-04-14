#######################################################################################################################
# $HeadURL$
#######################################################################################################################
'''
This agent takes care of uploading production logs for Inactive productions.

Get the files by walking the tree from the BaseLogPath option (in the CS, under the Agents sections). 
Puts them in tar files that are created in the BasePath(same as BaseLogPath)/LogsTar folder, created if needed.
Deletes the logs once the tar file exists.
Uploads them to the ArchivalSE: defined in Operations, under Transformations/ArchivalSE
Deletes the tar files.
The LFN path is given in the CS, Operations, under Transformations/BaseLogLFN


@since: Nov 2, 2013

@author: sposs
'''

from DIRAC.Core.Base.AgentModule                                import AgentModule
from DIRAC.ConfigurationSystem.Client.Helpers.Operations        import Operations
from DIRAC.TransformationSystem.Client.TransformationClient     import TransformationClient
from DIRAC.Resources.Storage.StorageElement                     import StorageElement

from DIRAC import S_OK, S_ERROR, gLogger

import os, tarfile

__RCSID__ = "$Id$"

ACTIVE_STATUS = ["Active", 'Completing']

class TarTheProdLogsAgent( AgentModule ):
  '''
  Tar the prod logs, and send them to whatever storage element you want
  '''


  def __init__( self, *args, **kwargs ):
    '''
    Constructor
    '''
    AgentModule.__init__( self, *args, **kwargs )
    self.name = "TarTheProdLogsAgent"
    self.log = gLogger
    self.basepath = ""
    self.baselogpath = ""
    self.transclient = None
    self.ops = None
    self.storageElement = None
    self.baselfn = ""

  def initialize(self):
    """Sets defaults
    """
    self.am_setModuleParam("shifterProxy", "ProductionManager")

    self.basepath = self.am_getOption("BasePath", "")
    if not self.basepath:
      return S_ERROR("Missing mandatory option BasePath")

    self.baselogpath = self.am_getOption("BaseLogPath", "")
    if not self.baselogpath:
      return S_ERROR("Missing mandatory option BaseLogPath")

    self.ops = Operations()

    dest_se = self.ops.getValue("Transformations/ArchiveSE", "")
    if not dest_se:
      return S_ERROR("Missing mandatory option ArchiveSE")
    self.storageElement = StorageElement( dest_se )
    
    baselfn = self.ops.getValue("Transformations/BaseLogLFN", "")
    if not baselfn:
      return S_ERROR("Missing mandatory option Transformations/BaseLogLFN")
    self.baselfn = baselfn
    
    self.transclient = TransformationClient()
    
    self.log.info("Running ")
    return S_OK()
  
  def execute(self):
    """ Run it!
    """
    res = self.cleanupPrevious()
    if not res["OK"]:
      self.log.error("Failed to clean up previous run:", res["Message"])
      return res
    
    res = self.getDirectoriesAndFiles()
    if not res["OK"]:
      return res
    
    res = self.getProductionIDs(res["Value"])
    if not res["OK"]:
      return res
    
    prods = res['Value']
    for prod, files in prods.items():
      res = self.transIsStopped(prod)
      if not res['OK']:
        continue
      if not res["Value"]:
        continue
      
      res = self.createTarBallAndCleanTheLogs(prod, files)
      if not res["OK"]:
        self.log.error("Could not get the tar ball:", res["Message"])
        continue 
      
      tarBall = res["Value"]
      res = self.uploadToStorage(prod, tarBall)
      if not res["OK"]:
        self.log.error("Failed putting the file to storage:", res["Message"])
      else:
        res = self.cleanTarBall(tarBall)
        if not res["OK"]:
          self.log.error("Failed removing the Tar Ball", res["Message"])
          
      
    return S_OK()
  
  def cleanupPrevious(self):
    """ Look for previously created tar files, and try to upload them again.
    Also, create the work dir
    """
    logs_dir = os.path.join(self.basepath, "LogsTars")
    if not os.path.isdir(logs_dir):
      try:
        os.mkdir(logs_dir)
      except OSError:
        return S_ERROR("Could not produce the directory")
    
    for root, dirs, files in os.walk(logs_dir):
      if not len(files):
        continue
      prod = root.rstrip("/").split("/")[-1]
      for tfile in files:
        tarballpath = os.path.join(root, tfile)
        res = self.uploadToStorage(prod, tarballpath)
        if not res['OK']:
          self.log.error("Failed to upload again %s to the SE")
          continue
        res = self.cleanTarBall(tarballpath)
        if not res["OK"]:
          self.log.error("Failed to remove the tar ball")
            
    return S_OK()
  
  def getDirectoriesAndFiles(self):
    """ List the directories below the base
    """
    final_dirs = {}
    for root, dirs, files in os.walk(self.baselogpath):
      if root.count("software"):
        continue
      if not len(files):
        continue
      final_dirs[root] = files
        
    return S_OK(final_dirs)

  def getProductionIDs(self, directories_and_files):
    """ Given input directories, get the prods_dict {prod:files}, where files is sorted per taskID
    This is used for he tar ball naming.
    """
    prods_dict = {}
    for path, files in directories_and_files.items():
      f_list = []
      for logfile in files:
        f_name = os.path.join(path, logfile)
        prodid = int(logfile.split("_")[2])
        if prodid not in prods_dict:
          prods_dict[prodid] = []
        f_list.append(f_name)
        
      prods_dict[prodid] = sorted(f_list, key = lambda x: self.__sortbyJob(x))
    

    return S_OK(prods_dict)
  
  def __sortbyJob(self, f_name):
    """ returns the taskID given a file name. Used for the sorting above
    """
    return int(f_name.split("_")[-2])
  
  def transIsStopped(self, prod):
    """ Check from the TS if the prod is Active or not
    """
    res = self.transclient.getTransformation(prod)
    if not res['OK']:
      return res
    
    trans = res["Value"]
    if trans["Status"] in ACTIVE_STATUS:
      return S_OK(False)
    #meaning the prods are neither Active nor Completing
    return S_OK(True)
  
  def cleanTarBall(self, tarballpath):
    """ Physically remove the tar ball that was created to free disk space
    """
    try:
      os.unlink(tarballpath)
    except OSError, x:
      return S_ERROR("Failed with %s" % str(x))
    if os.path.exists(tarballpath):
      self.log.error("The tar ball still exists while it should have be removed: ", tarballpath)
    return S_OK()
  
  def uploadToStorage(self, prod, tarballpath):
    """ Put the file to the Storage Element
    """
    
    final_lfn_path = self.baselfn+"/"+prod
    res = self.storageElement.getPfnForLfn(final_lfn_path)
    if not res["OK"]:
      return res
    final_pfn_path = res["Value"]
    
    pfn = final_pfn_path + "/" + os.path.basename(tarballpath)
    res = self.storageElement.isFile(final_pfn_path)
    if not res["OK"]:
      return res
    
    if pfn in res['Value']["Successful"]:
      #the file is on the storage, make a new one
      #AS: What if _2.tgz already exists?
      tarballname = os.path.basename(tarballpath)
      pfn = final_pfn_path + "/" + tarballname.rstrip(".tgz") + "_2.tgz" 
    
    
    fileDict = {pfn : tarballpath}
    self.log.info( "putFile", fileDict )
    res = self.storageElement.putFile( fileDict )
    if res['OK']:
      if res['Value']['Failed']:
        self.log.error( "putFile", res['Value']['Failed'] )
    else:
      self.log.error( "putFile", res['Message'] )
      
    return S_OK()
  
  def createTarBallAndCleanTheLogs(self, prod, prodFiles):
    """ Create and return the path to the tar ball containing all the prod files
    The file name contains the first and last taskID included. Allows easy finding of
    the right tar ball.
    """
    starttaskid = int(prodFiles[0].split("_")[-2])
    lasttaskid = int(prodFiles[-1].split("_")[-2])
    name = os.path.join(self.baselogpath, prod, prod+"_"+ starttaskid + "_to_"+ lasttaskid +"_logs.tgz")
    try: 
      tarFile = tarfile.open( name, "w:gz" )
      for fd in prodFiles:
        tarFile.add( fd )
      tarFile.close()
      for fd in prodFiles:
        os.remove( fd )
    except Exception as e:
      return S_ERROR("Failed with %s" % str(e))
    return S_OK(name)
#################################################################"  
  
