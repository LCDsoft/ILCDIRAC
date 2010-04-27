'''
Function to dowload and untar the applications, called from CombinedSoftwareInstallation

Created on Apr 7, 2010

@author: Stephane Poss
'''
import DIRAC
from ILCDIRAC.Core.Utilities.ResolveDependencies import resolveDeps
import os, urllib, tarfile

def TARinstall(app,config,area):
  appName    = app[0].lower()
  appVersion = app[1]
  deps = resolveDeps(config,appName,appVersion)
  depapp = []
  for dep in deps:
    depapp.append(dep["app"])
    depapp.append(dep["version"])
    res = install(depapp,config,area)
    if not res['OK']:
      DIRAC.gLogger.error("Could not install dependency %s %s"%(dep["app"],dep["version"]))
  res = install(app,config,area)
  return res

def install(app,config,area):  
  os.chdir(area)
  appName    = app[0]
  appVersion = app[1]
  appName = appName.lower()
  app_tar = DIRAC.gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s/TarBall'%(config,appName,appVersion),'')
  if not app_tar:
    DIRAC.gLogger.error('Could not find tar ball for %s %s'%(appName,appVersion))
    return DIRAC.S_ERROR('Could not find tar ball for %s %s'%(appName,appVersion))
  TarBallURL = DIRAC.gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/TarBallURL'%(config,appName),'')
  if not TarBallURL:
    DIRAC.gLogger.error('Could not find TarBallURL in CS for %s %s'%(appName,appVersion))
    return DIRAC.S_ERROR('Could not find TarBallURL in CS')
  #downloading file from url, but don't do if file is already there.
  app_tar_base=os.path.basename(app_tar)  
  if not os.path.exists("%s/%s"%(os.getcwd(),app_tar_base)):
    try :
      DIRAC.gLogger.debug("Downloading software", '%s_%s' %(appName,appVersion))
      #Copy the file locally, don't try to read from remote, soooo slow
      #Use string conversion %s%s to set the address, makes the system more stable
      tarball,headers = urllib.urlretrieve("%s%s"%(TarBallURL,app_tar),app_tar_base)
    except:
      DIRAC.gLogger.exception()
      return DIRAC.S_ERROR('Exception during url retrieve')

  if not os.path.exists("%s/%s"%(os.getcwd(),app_tar_base)):
    DIRAC.gLogger.error('Failed to download software','%s_%s' %(appName,appVersion))
    return DIRAC.S_ERROR('Failed to download software')

  if tarfile.is_tarfile(app_tar_base):##needed because LCSIM is jar file
    app_tar_to_untar = tarfile.open(app_tar_base)
    app_tar_to_untar.extractall()
    if appName=="slic":
      members = app_tar_to_untar.getmembers()
      fileexample = members[0].name
      basefolder = fileexample.split("/")[0]
      os.environ['SLIC_DIR']= basefolder
      slicv = ''
      lcddv = ''
      xercesv = ''
      for mem in members:
        if mem.name.find('/packages/slic/')>0:
          slicv = mem.name.split("/")[3]
        if mem.name.find('/packages/lcdd/')>0:
          lcddv = mem.name.split("/")[3]
        if mem.name.find('/packages/xerces/')>0:
          xercesv = mem.name.split("/")[3]
      if slicv:
        os.environ['SLIC_VERSION'] = slicv
      if xercesv:
        os.environ['XERCES_VERSION']= xercesv
      if lcddv:
        os.environ['LCDD_VERSION'] = lcddv

  #remove now useless tar ball
  #try:
  #  os.unlink(app_tar)
  #except:
  #  DIRAC.gLogger.exception()
  return DIRAC.S_OK()

def remove():
  pass
