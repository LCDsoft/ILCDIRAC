""" 
"""
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation  import LocalArea,SharedArea

from ILCDIRAC.ProcessProductionSystem.Client.ProcessProdClient import ProcessProdClient

from DIRAC import S_OK,S_ERROR, gConfig, gLogger
import os,shutil

class InstallSoftModule():
  def __init__(self):
    self.appsToRemoveStr = ''
    self.appsToInstallStr = ''
    self.appsToRemove = []
    self.appsToInstall  = []
    self.log = gLogger.getSubLogger( "InstallSoft" )
    self.systemConfig = ''

    self.ppc = ProcessProdClient()

  def execute(self):
    """ Look in folders (Shared Area and Local Area) and try ot remove the applications specified.
    """
    
    if self.appsToInstallStr:
      self.appsToInstall = self.appsToInstallStr.split(';')
    if self.appsToRemoveStr:
      self.appsToRemove = self.appsToRemoveStr.split(';')
    
    if self.workflow_commons.has_key('SystemConfig'):
      self.systemConfig = self.workflow_commons['SystemConfig']
    else:
      return S_ERROR('System Config not defined')
    

    self.log.info("Will check Installed %s"%self.appsToInstall)
    for app in self.appsToInstall:
      if not app:
        continue
      
      appname = app.split(".")[0]
      appversion = app.split(".")[1]
      appDir = gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s/TarBall'%(self.systemConfig,appname,appversion),'')
      appDir = appDir.replace(".tgz","").replace(".tar.gz","")
      mySoftwareRoot = ''
      sharedArea = SharedArea()
      if os.path.exists('%s%s%s' %(sharedArea,os.sep,appDir)):
        mySoftwareRoot = sharedArea
        self.ppc.reportOK(self.workflow_commons["JOB_ID"],appname,appversion,self.systemConfig)
      else:
        self.ppc.reportFailed(self.workflow_commons["JOB_ID"],appname,appversion,self.systemConfig)

    self.log.info("Will delete %s"%self.appsToRemove)
    failed = []
    for app in self.appsToRemove:
      if not app:
        continue
      
      appname = app.split(".")[0]
      appversion = app.split(".")[1]
      appDir = gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s/TarBall'%(self.systemConfig,appname,appversion),'')
      appDir = appDir.replace(".tgz","").replace(".tar.gz","")
      mySoftwareRoot = ''
      localArea = LocalArea()
      sharedArea = SharedArea()
      if os.path.exists('%s%s%s' %(localArea,os.sep,appDir)):
        mySoftwareRoot = localArea
      elif os.path.exists('%s%s%s' %(sharedArea,os.sep,appDir)):
        mySoftwareRoot = sharedArea
      else:
        self.log.error('%s: Could not find neither local area not shared area install'%app)
        continue
      myappDir = os.path.join(mySoftwareRoot,appDir)
      
      #### Hacky hack needed when the DB was in parallel to the Mokka version
      if appname.lower()=='mokka':
        dbloc = os.path.join(mySoftwareRoot,"CLICMokkaDB.sql")
        if os.path.exists(dbloc):
          try:
            os.remove(dbloc)
          except Exception, x:
            self.log.error("Could not delete SQL DB file : %s"%(str(x)))  
      if os.path.isdir(myappDir):
        try:
          shutil.rmtree(myappDir)
        except Exception, x:
          self.log.error("Could not delete %s : %s"%(app,str(x)))  
          failed.append(app)
      else:
        try:
          os.remove(myappDir)
        except Exception, x:
          self.log.error("Could not delete %s"%(myappDir,str(x)))
        
    if len(failed):
      return S_ERROR("Failed deleting applications %s"%failed)
    self.log.info("Successfully deleted %s"%self.appsToRemove)
    return S_OK()