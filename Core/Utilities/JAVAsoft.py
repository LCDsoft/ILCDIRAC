'''

Install SiD software

Created on Apr 7, 2010

@author: sposs
'''
from DIRAC import S_OK, S_ERROR, gConfig, gLogger
import os
TarBallURL = "http://www.lcsim.org/dist/slic/"

def SiDinstall(app, config, area):
  """ Install the Java soft, NOT USED
  """
  os.chdir(area)
  appName    = app[0]
  appVersion = app[1]
  appName = appName.lower()
  app_tar = gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s' % (config, appName, appVersion), '')
  if not app_tar:
    gLogger.error('Could not find tar ball for %s %s' % (appName, appVersion))
    return S_ERROR('Could not find tar ball for %s %s' % (appName, appVersion))
  return S_OK()

def remove():
  """ Remove the software
  """
  pass