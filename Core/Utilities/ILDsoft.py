'''
:since: Apr 7, 2010
:author: sposs
'''
import DIRAC

import os, urllib, tarfile

TarBallURL = "http://www.cern.ch/lcd-data/software/"

def install(app, config, area):
  """ Install the ILD soft
  """
  os.chdir(area)
  appName    = app[0]
  appVersion = app[1]
  appName = appName.lower()
  app_tar = DIRAC.gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s' % (config, appName, appVersion), '')
  if not app_tar:
    DIRAC.gLogger.error('Could not find tar ball for %s %s' % (appName, appVersion))
    return False

  #downloading file from url, but don't do if file is already there.
  if not os.path.exists("%s/%s" % (os.getcwd(), app_tar)):
    try :
      DIRAC.gLogger.debug("Downloading software", '%s_%s' % (appName, appVersion))
      #Copy the file locally, don't try to read from remote, soooo slow
      #Use string conversion %s%s to set the address, makes the system more stable
      urllib.urlretrieve("%s%s" % (TarBallURL, app_tar), app_tar)
    except:
      DIRAC.gLogger.exception()
      return False
  if not os.path.exists("%s/%s" % (os.getcwd(), app_tar)):
    DIRAC.gLogger.error('Failed to download software','%s_%s' % (appName, appVersion))
    return False

  app_tar_to_untar = tarfile.open(app_tar)
  app_tar_to_untar.extractall()
  
  #remove now useless tar ball
  try:
    os.unlink(app_tar)
  except:
    DIRAC.gLogger.exception()
  return DIRAC.S_OK()

def remove():
  """ Remove the application
  """ 
  pass
