'''
Function to download and untar the applications, called from CombinedSoftwareInstallation

Also installs all dependencies for the applications

@since:  Apr 7, 2010

@author: Stephane Poss
'''
__RCSID__ = None

from DIRAC import gLogger, S_OK, S_ERROR
from ILCDIRAC.Core.Utilities.ResolveDependencies            import resolveDeps
from ILCDIRAC.Core.Utilities.PrepareLibs                    import removeLibc, getLibsToIgnore
from DIRAC.DataManagementSystem.Client.ReplicaManager       import ReplicaManager
from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations
from ILCDIRAC.Core.Utilities.WasteCPU                       import WasteCPUCycles
import os, urllib, tarfile, subprocess, shutil, time
from tarfile import TarError
try:
  import hashlib as md5
except ImportError:
  import md5

def createLock(lockname):
  """ Need to lock the area to prevent 2 jobs to write in the same area
  """
  try:
    lock = file(lockname,"w")
    lock.write("Locking this directory\n")
    lock.close()
  except IOError as e:
    gLogger.error("Failed creating lock")
    return S_ERROR("Not allowed to write here: IOError %s" % (str(e)))
  return S_OK()

def checkLockAge(lockname):
  """ Check if there is a lock, and in that case deal with it, potentially remove it after n minutes
  """
  overwrite = False
  count = 0
  while True:
    if not os.path.exists(lockname):
      break
    count += 1
    gLogger.warn("Will wait one minute before proceeding")
    res = WasteCPUCycles(60)
    if not res['OK']:
      continue
    last_touch = time.time()
    try:
      stat = os.stat(lockname)
      last_touch = stat.st_atime
    except OSError as x:
      gLogger.warn("File not available: %s %s, assume removed" % (OSError, str(x))) 
      break
    loc_time = time.time()
    if loc_time-last_touch > 30*60: ##this is where I say the file is too old to still be valid (30 minutes)
      gLogger.info("File is %s seconds old" % loc_time-last_touch)
      overwrite = True
      res = clearLock(lockname)
      if res['OK']:
        break
    if count > 60: #We have been waiting for 60 minutes, something is wrong, kill it
      gLogger.error("Seems file stat is wrong, assume buggy, will fail installation")
      #overwrite = True
      res = clearLock(lockname)
      return S_ERROR("Buggy lock, removed: %s" % res['OK'])
      
  return S_OK(overwrite)
  
def clearLock(lockname):
  """ And we need to clear the lock once the operation is done
  """
  try:
    os.unlink(lockname)
  except OSError as e:
    gLogger.error("Failed cleaning lock: OSError", "%s" % (str(e)))
    return S_ERROR("Failed to clear lock: %s" % (str(e)) )
  return S_OK()

def deleteOld(folder_name):
  """ Remove directories
  """
  gLogger.info("Deleting existing version %s" % folder_name)
  if os.path.exists(folder_name):
    if os.path.isdir(folder_name):
      try:
        shutil.rmtree(folder_name)
      except OSError as x:
        gLogger.error("Failed deleting %s because :" % (folder_name), "%s %s" % ( OSError, str(x)))
    else:
      try:
        os.remove(folder_name)
      except OSError as e:
        gLogger.error("Failed deleting %s because :" % (folder_name),"OSError %s" % ( str(e)))
  if os.path.exists(folder_name):
    gLogger.error("Oh Oh, something was not right, the directory %s is still here" % folder_name) 
  return S_OK()

def downloadFile(tarballURL, app_tar, folder_name):
  """ Get the file locally.
  """
  #need to make sure the url ends with /, other wise concatenation below returns bad url
  if tarballURL[-1] != "/":
    tarballURL += "/"

  app_tar_base = os.path.basename(app_tar)
  if tarballURL.find("http://")>-1:
    try :
      gLogger.debug("Downloading software", '%s' % (folder_name))
      #Copy the file locally, don't try to read from remote, soooo slow
      #Use string conversion %s%s to set the address, makes the system more stable
      urllib.urlretrieve("%s%s" % (tarballURL, app_tar), app_tar_base)
    except IOError as err:
      gLogger.exception(str(err))
      return S_ERROR('Exception during url retrieve: %s' % str(err))
  else:
    rm = ReplicaManager()
    resget = rm.getFile("%s%s" % (tarballURL, app_tar))
    if not resget['OK']:
      gLogger.error("File could not be downloaded from the grid")
      return resget
  return S_OK()

def tarMd5Check(app_tar_base, md5sum ):
  """ Check the tar ball md5 sum 
  """
  ##Tar ball is obtained, need to check its md5 sum
  tar_ball_md5 = ''
  try:
    with open(app_tar_base) as myFile:
      tar_ball_md5 = md5.md5(myFile.read()).hexdigest()
  except IOError:
    gLogger.warn("Failed to get tar ball md5, try without")
    md5sum = ''
  if md5sum and md5sum != tar_ball_md5:
    gLogger.error('Hash does not correspond, found %s, expected %s, cannot continue' % (tar_ball_md5, md5sum))
    return S_ERROR("Hash does not correspond")
  return S_OK()

def tarInstall(app, config, area):
  """ For the specified app, install all dependencies
  """
  curdir = os.getcwd()
  appName    = app[0].lower()
  appVersion = app[1]

  deps = resolveDeps(config, appName, appVersion)
  for dep in deps:
    depapp = [ dep["app"], dep["version"] ]
    gLogger.info("Installing dependency %s %s" % (dep["app"], dep["version"]))
    res = installPackage(depapp, config, area, curdir)
    if not res['OK']:
      return res

  res = installPackage(app, config, area, curdir)
  return res

def getTarBallLocation(app, config, dummy_area):
  """ Get the tar ball location. 
  """
  ops = Operations()
  appName    = app[0]
  appVersion = app[1]
  appName = appName.lower()
  app_tar = ops.getValue('/AvailableTarBalls/%s/%s/%s/TarBall' % (config, appName, appVersion), '')
  overwrite = ops.getValue('/AvailableTarBalls/%s/%s/%s/Overwrite' % (config, appName, appVersion), False)
  md5sum = ops.getValue('/AvailableTarBalls/%s/%s/%s/Md5Sum' % (config, appName, appVersion), '')
  gLogger.info("Looking for application %s%s for config %s:" % (appName, appVersion, config) )
  if not app_tar:
    gLogger.error('Could not find tar ball for %s %s'%(appName, appVersion))
    return S_ERROR('Could not find tar ball for %s %s'%(appName, appVersion))
  
  tarballURL = ops.getValue('/AvailableTarBalls/%s/%s/TarBallURL' % (config, appName), '')
  if not tarballURL:
    gLogger.error('Could not find tarballURL in CS for %s %s' % (appName, appVersion))
    return S_ERROR('Could not find tarballURL in CS')

  return S_OK([app_tar, tarballURL, overwrite, md5sum])

def install(app, app_tar, tarballURL, overwrite, md5sum, area):
  """ Install the software
  """
  appName    = app[0]
  appVersion = app[1]
  folder_name = app_tar.replace(".tgz", "").replace(".tar.gz", "")
  #jar file does not contain .tgz nor tar.gz so the file name is untouched and folder_name = app_tar
  if appName == "slic":
    folder_name = "%s%s" % (appName, appVersion)
  
  appli_exists = False
  app_tar_base = os.path.basename(app_tar)

  ###########################################
  ###Go where the software is to be installed
  os.chdir(area)
  #We go back to the initial place at any return
  ###########################################
  ##Handle the locking
  lockname = folder_name+".lock"
  #Make sure the lock is not too old, or wait until it's gone
  res = checkLockAge(lockname)
  if not res['OK']:
    gLogger.error("Something uncool happened with the lock, will kill installation")
    gLogger.error("Message: %s" % res['Message'])
    return S_ERROR("Failed lock checks")

  if 'Value' in res and res['Value']: #this means the lock file was very old, meaning that the installation failed elsewhere
    overwrite = True

  #Check if the application is here and not to be overwritten
  if os.path.exists(folder_name):
    appli_exists = True
    if not overwrite:
      gLogger.info("Folder or file %s found in %s, skipping install !" % (folder_name, area))
      return S_OK([folder_name, app_tar_base])
  
  ## If we are here, it means the application was never installed OR its overwrite flag is true
    
  #Now lock the area
  res = createLock(lockname)##This will fail if not allowed to write here
  if not res['OK']:
    gLogger.error(res['Message'])
    return res
  
  ## Cleanup old version in case it has to be overwritten (implies it's already here)
  ## In particular the jar file of LCSIM
  if appli_exists and overwrite:
    gLogger.info("Overwriting %s found in %s" % (folder_name, area))
    res = deleteOld(folder_name) 
    if not res['OK']:#should be always OK for the time being
      clearLock(lockname)
      return res
    ## Now application must have been removed
  
  ## If here, the application DOES NOT exist locally: either it was here and the overwrite flag was false and 
  ## we returned earlier, either it was here and the overwrite flag was true and it was removed, or finally it
  ## was never here so here appli_exists=False always

  ## Now we can get the files and unpack them
    
  ## Downloading file from url
  res = downloadFile(tarballURL, app_tar, folder_name)
  if not res['OK']:
    clearLock(lockname)
    return res
  
  ## Check that the tar ball is there. Should never happen as download file catches the errors
  if not os.path.exists("%s/%s" % (os.getcwd(), app_tar_base)):
    gLogger.error('Failed to download software','%s' % (folder_name))
    clearLock(lockname)
    return S_ERROR('Failed to download software')

  ## Check that the downloaded file (or existing one) has the right checksum
  res = tarMd5Check(app_tar_base, md5sum)
  if not res['OK']:
    gLogger.error("Will try getting the file again, who knows")
    try:#Remove tar ball that we just got
      os.unlink("%s/%s" % (os.getcwd(), app_tar_base))
    except OSError:
      gLogger.error("Failed to clean tar ball, something bad is happening")
    ## Clean up existing stuff (if any, in particular the jar file)
    res = deleteOld(folder_name)
    if not res['OK']:#should be always OK for the time being
      clearLock(lockname)
      return res
    res = downloadFile(tarballURL, app_tar, folder_name)
    if not res['OK']:
      clearLock(lockname)
      return res
    res = tarMd5Check(app_tar_base, md5sum)
    if not res['OK']:
      gLogger.error("Hash failed again, something is really wrong, cannot continue.")
      clearLock(lockname)
      return S_ERROR("MD5 check failed")
  

  if tarfile.is_tarfile(app_tar_base):##needed because LCSIM is jar file
    app_tar_to_untar = tarfile.open(app_tar_base)
    try:
      app_tar_to_untar.extractall()
    except TarError as e:
      gLogger.error("Could not extract tar ball %s because of %s, cannot continue !" % (app_tar_base, str(e)))
      clearLock(lockname)
      return S_ERROR("Could not extract tar ball %s because of %s, cannot continue !"%(app_tar_base, str(e)))
    if folder_name.count("slic"):
      slicname = folder_name
      members = app_tar_to_untar.getmembers()
      fileexample = members[0].name
      basefolder = fileexample.split("/")[0]
      try:
        os.rename(basefolder, slicname)
      except OSError as e:
        gLogger.error("Failed renaming slic:", str(e))
        clearLock(lockname)
        return S_ERROR("Could not rename slic directory")
  try:
    dircontent = os.listdir(folder_name)
    if not len(dircontent):
      clearLock(lockname)
      return S_ERROR("Folder %s is empty, considering install as failed" % folder_name)
  except OSError:
    pass
  
  #Everything went fine, we try to clear the lock  
  clearLock(lockname)
    
  return S_OK([folder_name, app_tar_base]) 

def check(app, area, res_from_install):
  """ Now that the tar ball is here, we need to check that all is there
  """
  ###########################################
  ###Go where the software is to be installed
  os.chdir(area)
  #We go back to the initial place either at the end of the installation or at any error
  ###########################################
  
  basefolder = res_from_install[0]
  if os.path.isfile(basefolder):
    #This is the case of LCSIM that's a jar file
    return S_OK([basefolder])
  
  if os.path.exists(os.path.join(basefolder,'md5_checksum.md5')):
    md5file = file(os.path.join(basefolder,'md5_checksum.md5'), 'r')
    for line in md5file:
      line = line.rstrip()
      md5sum, fin = line.split()
      if fin=='-' or fin.count("md5_checksum.md5"):
        continue
      found_lib_to_ignore = False
      for lib in getLibsToIgnore():
        if fin.count(lib):
          found_lib_to_ignore = True
      if found_lib_to_ignore:
        continue
      fin = os.path.join(basefolder, fin.replace("./",""))
      if not os.path.exists(fin):
        gLogger.error("File missing :", fin)
        return S_ERROR("Incomplete install: The file %s is missing" % fin)
      fmd5 = ''
      try:
        fmd5 = md5.md5(file(fin).read()).hexdigest()
      except IOError:
        gLogger.error("Failed to compute md5 sum")
        return S_ERROR("Failed to compute md5 sum")
      if md5sum != fmd5:
        gLogger.error("File has wrong checksum :", fin)
        gLogger.error("Found %s, expected %s" % ( fmd5, md5sum ))
        return S_ERROR("Corrupted install: File %s has a wrong sum" % fin)
  else:
    gLogger.warn("The application does not come with md5 checksum file:", app)
  
  return S_OK([basefolder])

def configure(app, area, res_from_check):
  """ Configure our applications: set the proper env variables
  """
  ###########################################
  ###Go where the software is to be installed
  os.chdir(area)
  #We go back to the initial place either at the end of the installation or at any error
  ###########################################
  
  appName = app[0].lower()
  ### Set env variables  
  basefolder = res_from_check[0]
  removeLibc( os.path.join( os.getcwd(), basefolder, "LDLibs" ) )
  libFolder = os.path.join( os.getcwd(), basefolder, "lib" )
  if os.path.isdir( libFolder ):
    removeLibc( os.path.join( libFolder ) )
    addFolderToLdLibraryPath( libFolder )

  if appName == "slic":
    configureSlic(basefolder)
  elif appName == "root":
    configureRoot(basefolder)
  elif appName == 'java':
    configureJava(basefolder)
  elif appName == "lcio":
    res = checkJava()
    if not res['OK']:
      return res
    configureLCIO(basefolder)
  elif appName in ["lcsim",'stdhepcutjava']:
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
  #We go back to the initial place at the end
  ###########################################
  app_tar_base = res_from_install[1]
  #remove now useless tar ball
  if os.path.exists("%s/%s" % (os.getcwd(), app_tar_base)):
    if app_tar_base.find(".jar") < 0:
      try:
        os.unlink(app_tar_base)
      except OSError as e:
        gLogger.error("Could not remove tar ball:",str(e))
  return S_OK()

def remove():
  """ For the moment, this is done in L{RemoveSoft}
  """
  pass
    
def checkJava():
  """ Check if JAVA is available locally.
  """
  args = ['java', "-Xmx1536m", "-Xms256m", "-version"]
  try:
    res = subprocess.check_call(args)
    if res:
      return S_ERROR("Something is wrong with Java")
  except subprocess.CalledProcessError:
    gLogger.error("Java was not found on this machine, cannot proceed")
    return S_ERROR("Java was not found on this machine, cannot proceed")
  return S_OK()



def configureSlic(basefolder):
  """sets environment variables for SLIC"""
  os.environ['SLIC_DIR'] = basefolder
  slicv = ''
  lcddv = ''
  xercesv = ''
  try:
    slicv = os.listdir(os.path.join(basefolder, 'packages/slic/'))[0]
    lcddv = os.listdir(os.path.join(basefolder, 'packages/lcdd/'))[0]
    if os.path.exists(os.path.join(basefolder, 'packages/xerces/')):
      xercesv = os.listdir(os.path.join(basefolder, 'packages/xerces/'))[0]
  except OSError:
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

def configureRoot(basefolder):
  """Sets environment variables for root"""
  #members = app_tar_to_untar.getmembers()
  #fileexample = members[0].name
  #fileexample.split("/")[0]
  rootFolder = os.path.join( os.getcwd(), basefolder )
  rootLibFolder = os.path.join( rootFolder, "lib" )
  rootBinFolder = os.path.join( rootFolder, "bin" )
  os.environ['ROOTSYS'] = rootFolder
  addFolderToLdLibraryPath( rootLibFolder )
  os.environ['PATH'] = ":".join ( ( rootBinFolder , os.environ['PATH'] ) )
  os.environ['PYTHONPATH'] = ":".join( ( rootLibFolder, os.environ["PYTHONPATH"]) )

def configureJava(basefolder):
  """sets the environment variables for java"""
  binFolder = os.path.join(os.getcwd(), basefolder, "bin")
  libFolder = os.path.join(os.getcwd(), basefolder, "lib")
  os.environ['PATH'] = ":".join( ( binFolder, os.environ['PATH'] ) )
  addFolderToLdLibraryPath( libFolder )

def configureLCIO(basefolder):
  """sets the environment variables for LCIO"""
  lcioFolder = os.path.join( os.getcwd(), basefolder )
  os.environ['LCIO'] = lcioFolder
  os.environ['PATH'] = ":".join( ( os.path.join( lcioFolder, "bin" ), os.environ['PATH'] ) )


def addFolderToLdLibraryPath(folder):
  """insert folder to os.environ variables"""
  if 'LD_LIBRARY_PATH' in os.environ:
    os.environ['LD_LIBRARY_PATH'] = ":".join( ( folder, os.environ['LD_LIBRARY_PATH'] ) )
  else:
    os.environ['LD_LIBRARY_PATH'] = folder



def installPackage(app, config, area, curdir):
  """Installs the package app"""
  appName = app[0]
  appVersion = app[1]
  res = getTarBallLocation(app, config, area)
  if not res['OK']:
    gLogger.error("Could not install software/dependency %s %s: %s" % (appName, appVersion, res['Message']))
    return S_ERROR('Failed to install software')
  app_tar, tarballURL, overwrite, md5sum = res['Value']

  res = install(app, app_tar, tarballURL, overwrite, md5sum, area)
  os.chdir(curdir)
  if not res['OK']:
    gLogger.error("Could not install software/dependency %s %s: %s" % (appName,appVersion, res['Message']))
    return S_ERROR('Failed to install software')
  res_from_install = res['Value']

  res = check(app, area, res_from_install)
  os.chdir(curdir)
  if not res['OK']:
    gLogger.error("Failed to check software/dependency %s %s" % (appName,appVersion))
    return S_ERROR('Failed to check integrity of software')
  res_from_check = res['Value']

  res = configure(app, area, res_from_check)
  os.chdir(curdir)
  if not res['OK']:
    gLogger.error("Failed to configure software/dependency %s %s" % (appName,appVersion))
    return S_ERROR('Failed to configure software')

  res = clean(area, res_from_install)
  os.chdir(curdir)
  if not res['OK']:
    gLogger.error("Failed to clean useless tar balls, deal with it: %s %s" % (appName,appVersion))

  os.chdir(curdir)
  gLogger.notice("Successfully installed %s %s in %s" % (appName,appVersion, area))
  return S_OK()
