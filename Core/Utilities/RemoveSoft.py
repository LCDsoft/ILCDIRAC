'''
Module to remove software. Not used if using the ProcessProductionSystem

@author: Stephane Poss
@since: Jul 14, 2011
'''
__RCSID__ = "$Id$"

from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation  import LocalArea, SharedArea
from DIRAC.ConfigurationSystem.Client.Helpers.Operations            import Operations

from DIRAC import S_OK, S_ERROR, gLogger
import os, shutil

class RemoveSoft(object):
  """ Utility to remove software from a site.
  """
  def __init__(self):
    self.softs = ''
    self.apps = []
    self.log = gLogger.getSubLogger( "RemoveSoft" )
    self.platform = ''
    self.step_commons = {}
    self.workflow_commons = {}
    self.ops = Operations()
    
  def execute(self):
    """ Look in folders (Shared Area and Local Area) and try ot remove the applications specified.
    """
    self.softs = self.step_commons.get('Apps', None)
    if not self.softs:
      return S_ERROR('Applications to remove were not defined')

    self.platform = self.workflow_commons.get('Platform', None)
    if not self.platform:
      return S_ERROR('Platform, formerly known as SystemConfig not defined')
    
    self.softs.rstrip(";")
    self.apps = self.softs.split(';')
    self.log.info("Will delete %s" % self.apps)
    failed = []
    for app in self.apps:
      if not app:
        continue
      
      appname = app.split(".")[0]
      appversion = app.split(".")[1]
      appDir = self.ops.getValue('/AvailableTarBalls/%s/%s/%s/TarBall' % (self.platform, appname, appversion), '')
      appDir = appDir.replace(".tgz", "").replace(".tar.gz", "")
      mySoftwareRoot = ''
      localArea = LocalArea()
      sharedArea = SharedArea()
      if os.path.exists('%s%s%s' %(localArea, os.sep, appDir)):
        mySoftwareRoot = localArea
      elif os.path.exists('%s%s%s' %(sharedArea, os.sep, appDir)):
        mySoftwareRoot = sharedArea
      else:
        self.log.error('%s: Could not find neither local area not shared area install' % app)
        continue
      myappDir = os.path.join(mySoftwareRoot, appDir)
      
      #### Hacky hack needed when the DB was in parallel to the Mokka version
      if appname.lower() == 'mokka':
        dbloc = os.path.join(mySoftwareRoot, "CLICMokkaDB.sql")
        if os.path.exists(dbloc):
          try:
            os.remove(dbloc)
          except OSError as x:
            self.log.error("Could not delete SQL DB file : %s" % (str(x)))  
      if os.path.isdir(myappDir):
        try:
          shutil.rmtree(myappDir)
        except OSError as x:
          self.log.error("Could not delete %s : %s" % (app, str(x)))  
          failed.append(app)
      else:
        try:
          os.remove(myappDir)
        except OSError as x:
          self.log.error("Could not delete %s: %s" % (myappDir, str(x)))
        
    if len(failed):
      return S_ERROR("Failed deleting applications %s" % failed)
    self.log.info("Successfully deleted %s" % self.apps)
    return S_OK()
