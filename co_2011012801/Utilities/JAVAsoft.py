'''

Install SiD software

Created on Apr 7, 2010

@author: sposs
'''
import DIRAC
import os, urllib, tarfile
TarBallURL = "http://www.lcsim.org/dist/slic/"
def SiDinstall(app,config,area):
  os.chdir(area)
  appName    = app[0]
  appVersion = app[1]
  appName = appName.lower()
  app_tar = DIRAC.gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s'%(config,appName,appVersion),'')
  if not app_tar:
    DIRAC.gLogger.error('Could not find tar ball for %s %s'%(appName,appVersion))
    return DIRAC.S_ERROR('Could not find tar ball for %s %s'%(appName,appVersion))
  return DIRAC.S_OK()

def remove():
  pass