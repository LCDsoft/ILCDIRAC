'''
Function to download and untar the applications, called from CombinedSoftwareInstallation

Also installs all dependencies for the applications

@since:  Apr 7, 2010

@author: Stephane Poss
'''
import DIRAC
from ILCDIRAC.Core.Utilities.ResolveDependencies import resolveDeps
from ILCDIRAC.Core.Utilities.PrepareLibs import removeLibc
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
import os, urllib, tarfile, subprocess, shutil

def TARinstall(app,config,area):
  """ For the specified app, install all dependencies
  """
  appName    = app[0].lower()
  appVersion = app[1]
  deps = resolveDeps(config,appName,appVersion)
  for dep in deps:
    depapp = []
    depapp.append(dep["app"])
    depapp.append(dep["version"])
    DIRAC.gLogger.info("Installing dependency %s %s"%(dep["app"],dep["version"]))
    res = install(depapp,config,area)
    if not res['OK']:
      DIRAC.gLogger.error("Could not install dependency %s %s"%(dep["app"],dep["version"]))
      return DIRAC.S_ERROR('Failed to install software')
  res = install(app,config,area)
  return res

def install(app,config,area):
  """ Actually install the applications. Set the environment for some of them.
  """
  curdir = os.getcwd()

  appName    = app[0]
  appVersion = app[1]
  appName = appName.lower()
  app_tar = DIRAC.gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s/TarBall'%(config,appName,appVersion),'')
  overwrite = DIRAC.gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s/Overwrite'%(config,appName,appVersion),False)

  if not app_tar:
    DIRAC.gLogger.error('Could not find tar ball for %s %s'%(appName,appVersion))
    return DIRAC.S_ERROR('Could not find tar ball for %s %s'%(appName,appVersion))
  TarBallURL = DIRAC.gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/TarBallURL'%(config,appName),'')
  if not TarBallURL:
    DIRAC.gLogger.error('Could not find TarBallURL in CS for %s %s'%(appName,appVersion))
    return DIRAC.S_ERROR('Could not find TarBallURL in CS')

  #Check if folder is already there:
  folder_name = app_tar.replace(".tgz","").replace(".tar.gz","")
  if appName =="slic":
    folder_name= "%s%s"%(appName,appVersion)
  appli_exists = False
  os.chdir(area)

  if os.path.exists(folder_name):
    # and not appName =="slic":
    if not overwrite:
      DIRAC.gLogger.info("Folder or file %s found in %s, skipping install !"%(folder_name,area))
    else:
      DIRAC.gLogger.info("Overwriting %s found in %s"%(folder_name,area))  
    appli_exists = True
    if overwrite:
      appli_exists = False
    #os.chdir(curdir)
    #return DIRAC.S_OK()
  if not appli_exists:
    if not CanWrite(area):
      os.chdir(curdir)
      return DIRAC.S_ERROR("Not allowed to write in %s"%area)
  #downloading file from url, but don't do if file is already there.
  app_tar_base=os.path.basename(app_tar)
  if not os.path.exists("%s/%s"%(os.getcwd(),app_tar_base)) and not appli_exists:
    if TarBallURL.find("http://")>-1:
      try :
        DIRAC.gLogger.debug("Downloading software", '%s_%s' %(appName,appVersion))
        #Copy the file locally, don't try to read from remote, soooo slow
        #Use string conversion %s%s to set the address, makes the system more stable
        tarball,headers = urllib.urlretrieve("%s%s"%(TarBallURL,app_tar),app_tar_base)
      except:
        DIRAC.gLogger.exception()
        os.chdir(curdir)
        return DIRAC.S_ERROR('Exception during url retrieve')
    else:
      rm = ReplicaManager()
      res = rm.getFile("%s%s"%(TarBallURL,app_tar))
      if not res['OK']:
        os.chdir(curdir)
        return res

  if not os.path.exists("%s/%s"%(os.getcwd(),app_tar_base)) and not appli_exists:
    DIRAC.gLogger.error('Failed to download software','%s_%s' %(appName,appVersion))
    os.chdir(curdir)
    return DIRAC.S_ERROR('Failed to download software')

  if not appli_exists:
    if overwrite:
      DIRAC.gLogger.info("First we delete existing version %s"%folder_name)
      if os.path.exists(folder_name):
        if os.path.isdir(folder_name):
          try:
            shutil.rmtree(folder_name)
          except Exception,x:
            DIRAC.gLogger.error("Failed deleting %s because %s,%s"%(folder_name,Exception,str(x)))
        else:
          try:
            os.remove(folder_name)
          except Exception,x:
            DIRAC.gLogger.error("Failed deleting %s because %s,%s"%(folder_name,Exception,str(x)))
      if os.path.exists(folder_name):
        DIRAC.gLogger.error("Oh Oh, something was not right, the directory %s is still here"%folder_name)      
    if tarfile.is_tarfile(app_tar_base):##needed because LCSIM is jar file
      app_tar_to_untar = tarfile.open(app_tar_base)
      try:
        app_tar_to_untar.extractall()
      except Exception, e:
        DIRAC.gLogger.error("Could not extract tar ball %s because of %s, cannot continue !"%(app_tar_base,e))
        os.chdir(curdir)
        return DIRAC.S_ERROR("Could not extract tar ball %s because of %s, cannot continue !"%(app_tar_base,e))
      if appName=="slic":
        slicname = "%s%s"%(appName,appVersion)
        members = app_tar_to_untar.getmembers()
        fileexample = members[0].name
        basefolder = fileexample.split("/")[0]
        try:
          os.rename(basefolder, slicname)
        except:
          os.chdir(curdir)
          return DIRAC.S_ERROR("Could not rename slic directory")
    try:
      dircontent = os.listdir(folder_name)
      if not len(dircontent):
        os.chdir(curdir)
        return DIRAC.S_ERROR("Folder %s is empty, considering install as failed"%folder_name)
    except:
      pass
    
  ### Set env variables  
  basefolder = folder_name
  removeLibc(os.path.join(os.getcwd(),basefolder)+"/LDLibs")
  if os.path.isdir(os.path.join(os.getcwd(),basefolder)+"/lib"):
    removeLibc(os.path.join(os.getcwd(),basefolder)+"/lib")
  if appName=="slic":
    os.environ['SLIC_DIR']= basefolder
    slicv = ''
    lcddv = ''
    xercesv = ''
    try:
      slicv = os.listdir(os.path.join(basefolder,'packages/slic/'))[0]
      lcddv = os.listdir(os.path.join(basefolder,'packages/lcdd/'))[0]
      if os.path.exists(os.path.join(basefolder,'packages/xerces/')):
        xercesv =os.listdir(os.path.join(basefolder,'packages/xerces/'))[0]
    except:
      os.chdir(curdir)
      return DIRAC.S_ERROR("Could not resolve slic env variables, folder content does not match usual pattern")
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
      os.environ['XERCES_VERSION']= xercesv
    if lcddv:
      os.environ['LCDD_VERSION'] = lcddv
  elif appName=="root":
    #members = app_tar_to_untar.getmembers()
    #fileexample = members[0].name
    #fileexample.split("/")[0]
    os.environ['ROOTSYS']= os.path.join(os.getcwd(),basefolder)
    if os.environ.has_key('LD_LIBRARY_PATH'):
      os.environ['LD_LIBRARY_PATH'] = os.environ['ROOTSYS']+"/lib:"+ os.environ['LD_LIBRARY_PATH']
    else:
      os.environ['LD_LIBRARY_PATH'] = os.environ['ROOTSYS']+"/lib"
    os.environ['PATH'] = os.environ['ROOTSYS']+"/bin:"+os.environ['PATH']
    os.environ['PYTHONPATH']=os.environ['ROOTSYS']+"/lib"+":"+os.environ["PYTHONPATH"]
  elif appName=='java':
    os.environ['PATH'] = os.path.join(os.getcwd(),basefolder)+"/Executable:"+os.environ['PATH']
    if os.environ.has_key('LD_LIBRARY_PATH'):
      os.environ['LD_LIBRARY_PATH'] = os.path.join(os.getcwd(),basefolder)+"/LDLibs:"+os.environ['LD_LIBRARY_PATH']
    else:
      os.environ['LD_LIBRARY_PATH'] = os.path.join(os.getcwd(),basefolder)+"/LDLibs"
  elif appName=="lcio":
    os.environ['LCIO']= os.path.join(os.getcwd(),basefolder)
    os.environ['PATH'] = os.path.join(os.getcwd(),basefolder)+"/bin:"+os.environ['PATH']
    res = checkJava(curdir)
    if not res['OK']:
      return res
  elif appName=="lcsim":
    res = checkJava(curdir)
    if not res['OK']:
      return res

  #remove now useless tar ball
  if os.path.exists("%s/%s"%(os.getcwd(),app_tar_base)):
    if app_tar_base.find(".jar")<0:
      try:
        os.unlink(app_tar_base)
      except:
        DIRAC.gLogger.error("Could not remove tar ball")
  os.chdir(curdir)
  return DIRAC.S_OK()

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
  except Exception,x:
    DIRAC.gLogger.error('Problem trying to write in area %s: '%area,str(x))
    return False
  finally:
    os.chdir(curdir)
  return True
    

def checkJava(dir):
  """ Check if JAVA is availalbe locally.
  """
  args = ['java',"-version"]
  try:
    p = subprocess.check_call(args)
    if p:
      os.chdir(dir)
      return DIRAC.S_ERROR("Something is wrong with Java")
  except:
    DIRAC.gLogger.error("Java was not found on this machine, cannot proceed")
    os.chdir(dir)
    return DIRAC.S_ERROR("Java was not found on this machine, cannot proceed")

  return DIRAC.S_OK()
