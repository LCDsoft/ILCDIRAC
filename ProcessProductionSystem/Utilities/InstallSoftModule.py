""" 
Created by S Poss 

May 2011
"""
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation  import LocalArea, SharedArea, listAreaDirectory

from ILCDIRAC.ProcessProductionSystem.Client.ProcessProdClient import ProcessProdClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations            import Operations

from DIRAC import S_OK, S_ERROR, gLogger
import os, shutil

class InstallSoftModule():
  """ Module to install software, not used yet
  """
  def __init__(self):
    self.ops = Operations()
    self.appsToRemoveStr = ''
    self.appsToInstallStr = ''
    self.appsToRemove = []
    self.appsToInstall  = []
    self.log = gLogger.getSubLogger( "InstallSoft" )
    self.platform = ''

    self.ppc = ProcessProdClient()
    #Those too are supposed to be set from the Workflow
    self.step_commons = {}
    self.workflow_commons = {}
    
  def execute(self):
    """ Look in folders (Shared Area and Local Area) and try ot remove the applications specified.
    """
    
    if self.appsToInstallStr:
      self.appsToInstall = self.appsToInstallStr.split(';')
    if self.appsToRemoveStr:
      self.appsToRemove = self.appsToRemoveStr.split(';')
    
    self.platform = self.workflow_commons.get('Platform', None)
    if not self.platform:
      return S_ERROR('System Config not defined')
    

    self.log.info("Will check Installed %s" % self.appsToInstall)
    for app in self.appsToInstall:
      if not app:
        continue
      jobdict = {}
      jobdict['JobID'] = self.workflow_commons["JOB_ID"]
      appname = app.split(".")[0]
      appversion = app.split(".")[1]
      jobdict['AppName'] = appname
      jobdict['AppVersion'] = appversion
      jobdict['Platform'] = self.platform
      appDir = self.ops.getValue('/AvailableTarBalls/%s/%s/%s/TarBall' % (self.platform, appname,
                                                                          appversion), '')
      appDir = appDir.replace(".tgz","").replace(".tar.gz","")
      mySoftwareRoot = ''
      sharedArea = SharedArea()
      if os.path.exists('%s%s%s' % (sharedArea, os.sep, appDir)):
        mySoftwareRoot = sharedArea
        self.ppc.reportOK(jobdict)
      else:
        self.ppc.reportFailed(jobdict)

    self.log.info("Will delete %s" % self.appsToRemove)
    failed = []
    for app in self.appsToRemove:
      if not app:
        continue
      jobdict = {}
      jobdict['JobID'] = self.workflow_commons["JOB_ID"]
      appname = app.split(".")[0]
      appversion = app.split(".")[1]
      jobdict['AppName'] = appname
      jobdict['AppVersion'] = appversion
      jobdict['Platform'] = self.platform
      appDir = self.ops.getValue('/AvailableTarBalls/%s/%s/%s/TarBall' % (self.platform,
                                                                          appname, appversion), '')
      appDir = appDir.replace(".tgz", "").replace(".tar.gz", "")
      mySoftwareRoot = ''
      sharedArea = SharedArea()
      self.log.info("Shared Area is %s" % sharedArea)
      listAreaDirectory(sharedArea)
      if os.path.exists('%s%s%s' % (sharedArea, os.sep, appDir)):
        mySoftwareRoot = sharedArea
      if not mySoftwareRoot:
        self.log.error('%s: Could not find in shared area' % app)
        continue
      myappDir = os.path.join(mySoftwareRoot, appDir)
      self.log.info("Will attempt to remove %s " % myappDir)
      #### Hacky hack needed when the DB was in parallel to the Mokka version
      if appname.lower() == 'mokka':
        dbloc = os.path.join(mySoftwareRoot, "CLICMokkaDB.sql")
        if os.path.exists(dbloc):
          try:
            os.remove(dbloc)
          except Exception, x:
            self.log.error("Could not delete SQL DB file : %s" % (str(x)))  
      if os.path.isdir(myappDir):
        try:
          shutil.rmtree(myappDir)
        except Exception, x:
          self.log.error("Could not delete %s : %s" % (app, str(x)))  
          failed.append(app)
      else:
        try:
          os.remove(myappDir)
        except Exception, x:
          self.log.error("Could not delete %s: %s" % (myappDir, str(x)))
        
    if len(failed):
      return S_ERROR("Failed deleting applications %s" % failed)
    self.log.info("Successfully deleted %s" % self.appsToRemove)
    return S_OK()