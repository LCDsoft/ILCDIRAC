'''
Function to download and untar the applications, called from CombinedSoftwareInstallation

Also installs all dependencies for the applications

@since:  Apr 7, 2010

@author: Stephane Poss
'''
from DIRAC import gLogger, S_OK, S_ERROR
from ILCDIRAC.Core.Utilities.ResolveDependencies      import resolveDeps
from ILCDIRAC.Core.Utilities.PrepareLibs              import removeLibc
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.ConfigurationSystem.Client.Helpers.Operations            import Operations
from ILCDIRAC.Core.Utilities.WasteCPU import WasteCPUCycles
import os, urllib, tarfile, subprocess, shutil, time

def createLock(lockname):
  """ Need to lock the area to prevent 2 jobs to write in the same area
  """
  try:
    lock = file(lockname,"w")
    lock.write("Locking this directory\n")
    lock.close()
  except Exception, x :
    return S_ERROR("Not allowed to write here: %s %s" % (Exception, str(x)))
  return S_OK()

def checkLockAge(lockname):
  """ Check if there is a lock, and in that case deal with it, potentially remove it after n minutes
  """
  count = 0
  while (1):
    if not os.path.exists(lockname):
      break
    count += 1
    res = WasteCPUCycles(60)
    if not res['OK']:
      continue
    last_touch = time.time()
    try:
      stat = os.stat(lockname)
      last_touch = stat.st_atime
    except Exception, x:
      gLogger.warn("File not available: %s %s, assume removed" % (Exception, str(x))) 
      break
    loc_time = time.time()
    if loc_time-last_touch > 30*60: ##this is where I say the file is too old to still be valid (30 minutes)
      res = clearLock(lockname)
      if res['OK']:
        break
      
  return S_OK()
  
def clearLock(lockname):
  """ And we need to clear the lock once the operation is done
  """
  try:
    os.unlink(lockname)
  except Exception, x:
    return S_ERROR("Failed to clear lock: %s %s" % (Exception, str(x)) )
  return S_OK()

def TARinstall(app, config, area):
  """ For the specified app, install all dependencies
  """
  curdir = os.getcwd()
  appName    = app[0].lower()
  appVersion = app[1]
  deps = resolveDeps(config, appName, appVersion)
  
  for dep in deps:
    depapp = []
    depapp.append(dep["app"])
    depapp.append(dep["version"])
    gLogger.info("Installing dependency %s %s" % (dep["app"], dep["version"]))
    
    res = install(depapp, config, area)
    os.chdir(curdir)
    if not res['OK']:
      gLogger.error("Could not install dependency %s %s: %s" % (dep["app"], dep["version"], res['Message']))
      return S_ERROR('Failed to install software')
    res_from_install = res['Value']
    
    res = configure(depapp, area, res_from_install)
    os.chdir(curdir)
    if not res['OK']:
      gLogger.error("Failed to configure dependency %s %s" % (dep["app"], dep["version"]))
      return S_ERROR('Failed to configure software')
    
    res = clean(area, res_from_install)
    if not res['OK']:
      gLogger.error("Failed to clean useless tar balls, deal with it: %s %s" % (dep["app"], dep["version"]))
      
    os.chdir(curdir)
    
  res = install(app, config, area)
  os.chdir(curdir)
  if not res['OK']:
    gLogger.error("Could not install software %s %s: %s" % (appName, appVersion, res['Message']))
    return S_ERROR('Failed to install software')
  
  res_from_install = res['Value']
  res = configure(app, area, res_from_install)
  os.chdir(curdir)
  if not res['OK']:
    gLogger.error("Failed to configure software %s %s" % (appName, appVersion))
    return S_ERROR('Failed to configure software')
  
  res = clean(area, res_from_install)
  os.chdir(curdir)
  if not res['OK']:
    gLogger.error("Failed to clean useless tar balls, deal with it")
    return S_OK()
  
  return res

def install(app, config, area):
  """ Actually install the applications. 
  """
  ops = Operations()
  appName    = app[0]
  appVersion = app[1]
  appName = appName.lower()
  app_tar = ops.getValue('/AvailableTarBalls/%s/%s/%s/TarBall' % (config, appName, appVersion), '')
  overwrite = ops.getValue('/AvailableTarBalls/%s/%s/%s/Overwrite' % (config, appName, appVersion), False)

  if not app_tar:
    gLogger.error('Could not find tar ball for %s %s'%(appName, appVersion))
    return S_ERROR('Could not find tar ball for %s %s'%(appName, appVersion))
  
  TarBallURL = ops.getValue('/AvailableTarBalls/%s/%s/TarBallURL' % (config, appName), '')
  if not TarBallURL:
    gLogger.error('Could not find TarBallURL in CS for %s %s' % (appName, appVersion))
    return S_ERROR('Could not find TarBallURL in CS')

  #Check if folder is already there:
  folder_name = app_tar.replace(".tgz", "").replace(".tar.gz", "")
  if appName == "slic":
    folder_name = "%s%s" % (appName, appVersion)
  app_tar_base = os.path.basename(app_tar)

    
  appli_exists = False
  
  ###########################################
  ###Go where the software is to be installed
  os.chdir(area)
  #We go back to the initial place either at the end of the installation or at any error
  ###########################################
  ##Handle the locking
  lockname = folder_name+".lock"
  #Make sure the lock is not too old, or wait until it's gone
  res = checkLockAge(lockname)
  if not res['OK']:
    gLogger.error("Something uncool happened with the lock, will try to proceed anyway")
  
  #Check if the application is here and not to be overwritten
  if os.path.exists(folder_name): #This should include a checksum verification of some sort
    # and not appName =="slic":
    appli_exists = True #this basically makes that all the following ifs are not true
    if not overwrite:
      gLogger.info("Folder or file %s found in %s, skipping install !" % (folder_name, area))
      return S_OK([folder_name, app_tar_base])
    
  #Now lock the area
  res = createLock(lockname)##This will fail if not allowed to write here
  if not res['OK']:
    gLogger.error(res['Message'])
    return res
  
  if appli_exists and overwrite:
    gLogger.info("Overwriting %s found in %s" % (folder_name, area))
    appli_exists = False
    if CanWrite(area):
      gLogger.info("First we delete existing version %s" % folder_name)
      if os.path.exists(folder_name):
        if os.path.isdir(folder_name):
          try:
            shutil.rmtree(folder_name)
          except Exception, x:
            gLogger.error("Failed deleting %s because %s,%s" % (folder_name, Exception, str(x)))
        else:
          try:
            os.remove(folder_name)
          except Exception, x:
            gLogger.error("Failed deleting %s because %s,%s"%(folder_name, Exception, str(x)))
      if os.path.exists(folder_name):
        gLogger.error("Oh Oh, something was not right, the directory %s is still here" % folder_name)  

  if not appli_exists:
    if not CanWrite(area):
      #res = clearLock(lockname) ##no need to clean as it was not created in the first place
      #if not res['OK']:
      #    gLogger.error("Lock file could not be cleared")
      return S_ERROR("Not allowed to write in %s" % area)
    
    
  #downloading file from url, but don't do if file is already there.
  if not os.path.exists("%s/%s"%(os.getcwd(), app_tar_base)) and not appli_exists:
    if TarBallURL.find("http://")>-1:
      try :
        gLogger.debug("Downloading software", '%s_%s' % (appName, appVersion))
        #Copy the file locally, don't try to read from remote, soooo slow
        #Use string conversion %s%s to set the address, makes the system more stable
        tarball, headers = urllib.urlretrieve("%s%s" % (TarBallURL, app_tar), app_tar_base)
      except:
        gLogger.exception()
        res = clearLock(lockname)
        if not res['OK']:
          gLogger.error("Lock file could not be cleared")
        return S_ERROR('Exception during url retrieve')
    else:
      rm = ReplicaManager()
      resget = rm.getFile("%s%s" % (TarBallURL, app_tar))
      if not resget['OK']:
        gLogger.error("File could not be downloaded from the grid")
        res = clearLock(lockname)
        if not res['OK']:
          gLogger.error("Lock file could not be cleared")
        return resget

  if not os.path.exists("%s/%s" % (os.getcwd(), app_tar_base)) and not appli_exists:
    gLogger.error('Failed to download software','%s_%s' % (appName, appVersion))
    res = clearLock(lockname)
    if not res['OK']:
      gLogger.error("Lock file could not be cleared")
    return S_ERROR('Failed to download software')

  if not appli_exists:    
    if tarfile.is_tarfile(app_tar_base):##needed because LCSIM is jar file
      app_tar_to_untar = tarfile.open(app_tar_base)
      try:
        app_tar_to_untar.extractall()
      except Exception, e:
        gLogger.error("Could not extract tar ball %s because of %s, cannot continue !" % (app_tar_base, e))
        res = clearLock(lockname)
        if not res['OK']:
          gLogger.error("Lock file could not be cleared")
        return S_ERROR("Could not extract tar ball %s because of %s, cannot continue !"%(app_tar_base, e))
      if appName == "slic":
        slicname = "%s%s" % (appName, appVersion)
        members = app_tar_to_untar.getmembers()
        fileexample = members[0].name
        basefolder = fileexample.split("/")[0]
        try:
          os.rename(basefolder, slicname)
        except:
          res = clearLock(lockname)
          if not res['OK']:
            gLogger.error("Lock file could not be cleared")
          return S_ERROR("Could not rename slic directory")
    try:
      dircontent = os.listdir(folder_name)
      if not len(dircontent):
        res = clearLock(lockname)
        if not res['OK']:
          gLogger.error("Lock file could not be cleared")
        return S_ERROR("Folder %s is empty, considering install as failed" % folder_name)
    except:
      pass
  
  #Everything went fine, we try to clear the lock  
  res = clearLock(lockname)
  if not res['OK']:
    gLogger.error("Lock file could not be cleared")
  return S_OK([folder_name, app_tar_base]) 
 
def configure(app, area, res_from_install):
  """ Configure our applications: set the proper env variables
  """
  ###########################################
  ###Go where the software is to be installed
  os.chdir(area)
  #We go back to the initial place either at the end of the installation or at any error
  ###########################################
  
  appName = app[0].lower()
  ### Set env variables  
  basefolder = res_from_install[0]
  removeLibc(os.path.join(os.getcwd(), basefolder) + "/LDLibs")
  if os.path.isdir(os.path.join(os.getcwd(), basefolder) + "/lib"):
    removeLibc(os.path.join(os.getcwd(), basefolder) + "/lib")
  if appName == "slic":
    os.environ['SLIC_DIR'] = basefolder
    slicv = ''
    lcddv = ''
    xercesv = ''
    try:
      slicv = os.listdir(os.path.join(basefolder, 'packages/slic/'))[0]
      lcddv = os.listdir(os.path.join(basefolder, 'packages/lcdd/'))[0]
      if os.path.exists(os.path.join(basefolder, 'packages/xerces/')):
        xercesv = os.listdir(os.path.join(basefolder, 'packages/xerces/'))[0]
    except:
      return S_ERROR("Could not resolve slic env variables, folder content does not match usual pattern")
    #for mem in members:
    #  if mem.name.find('/packages/slic/')>0:
    #    slicv = mem.name.split("/")[3]
    #  if mem.name.find('/packages/lcdd/')>0:
    #    lcddv = mem.name.split("/")[3]
    #  if mem.name.find('/packages/xerces/')>0:
    #    xercesv = mem.name.split("/")[3]
    if slicv:
      os.environ['SLIC_VERSION'] = slicv
    if xercesv:
      os.environ['XERCES_VERSION'] = xercesv
    if lcddv:
      os.environ['LCDD_VERSION'] = lcddv
  elif appName == "root":
    #members = app_tar_to_untar.getmembers()
    #fileexample = members[0].name
    #fileexample.split("/")[0]
    os.environ['ROOTSYS'] = os.path.join(os.getcwd(), basefolder)
    if os.environ.has_key('LD_LIBRARY_PATH'):
      os.environ['LD_LIBRARY_PATH'] = os.environ['ROOTSYS'] + "/lib:" + os.environ['LD_LIBRARY_PATH']
    else:
      os.environ['LD_LIBRARY_PATH'] = os.environ['ROOTSYS'] + "/lib"
    os.environ['PATH'] = os.environ['ROOTSYS'] + "/bin:" + os.environ['PATH']
    os.environ['PYTHONPATH'] = os.environ['ROOTSYS'] + "/lib:" + os.environ["PYTHONPATH"]
  elif appName == 'java':
    os.environ['PATH'] = os.path.join(os.getcwd(), basefolder) + "/bin:" + os.environ['PATH']
    if os.environ.has_key('LD_LIBRARY_PATH'):
      os.environ['LD_LIBRARY_PATH'] = os.path.join(os.getcwd(), basefolder) + "/lib:" + os.environ['LD_LIBRARY_PATH']
    else:
      os.environ['LD_LIBRARY_PATH'] = os.path.join(os.getcwd(), basefolder) + "/lib"
  elif appName == "lcio":
    os.environ['LCIO'] = os.path.join(os.getcwd(), basefolder)
    os.environ['PATH'] = os.path.join(os.getcwd(), basefolder) + "/bin:" + os.environ['PATH']
    res = checkJava()
    if not res['OK']:
      return res
  elif appName == "lcsim":
    res = checkJava()
    if not res['OK']:
      return res
  return S_OK()  

def clean(area, res_from_install):
  """ After install, clean the tar balls and go back to initial directory
  """
  ###########################################
  ###Go where the software is to be installed
  os.chdir(area)
  #We go back to the initial place either at the end
  ###########################################
  app_tar_base = res_from_install[1]
  #remove now useless tar ball
  if os.path.exists("%s/%s" % (os.getcwd(), app_tar_base)):
    if app_tar_base.find(".jar") < 0:
      try:
        os.unlink(app_tar_base)
      except:
        gLogger.error("Could not remove tar ball")
  return S_OK()

def remove():
  """ For the moment, this is done in L{RemoveSoft}
  """
  pass

def CanWrite(area):
  """ Check if user is allowed to write in the area
  """
  curdir = os.getcwd()
  os.chdir(area)
  try:
    f = open("testfile.txt","w")
    f.write("Testing writing\n")
    f.close()
    os.remove("testfile.txt")
  except Exception, x:
    gLogger.error('Problem trying to write in area %s: %s' % (area, str(x)))
    return False
  finally:
    os.chdir(curdir)
  return True
    

def checkJava():
  """ Check if JAVA is availalbe locally.
  """
  args = ['java', "-version"]
  try:
    p = subprocess.check_call(args)
    if p:
      return S_ERROR("Something is wrong with Java")
  except:
    gLogger.error("Java was not found on this machine, cannot proceed")
    return S_ERROR("Java was not found on this machine, cannot proceed")
  return S_OK()
