'''
Created on Jul 14, 2011

@author: Stephane Poss
'''
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation  import LocalArea,SharedArea

from DIRAC import S_OK,S_ERROR, gConfig, gLogger
import os

class RemoveSoft():
  def __init__(self):
    self.softs = ''
    self.apps = []
    self.log = gLogger.getSubLogger( "RemoveSoft" )
    
  def execute(self):
    if self.step_commons.has_key('Apps'):
      self.softs = self.step_commons['Apps']
    else:
      return S_ERROR('Applications to remove were not defined')  
    
    self.apps = self.softs.split(';')
    for app in self.apps:
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
      try:
        os.rmdir(myappDir)
      except Exception, x:
        self.log.error("Could not delete %s : %s"%(app,str(x)))  
    
    return S_OK()