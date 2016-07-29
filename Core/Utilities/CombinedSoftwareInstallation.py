"""
Installs properly ILD soft and SiD soft, and all dependencies

:since: Feb 4, 2010
:author: Stephane Poss
:author: Przemyslaw Majewski
"""
import os
import zipfile
import DIRAC

from DIRAC                             import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.Subprocess   import systemCall

from ILCDIRAC.Core.Utilities.DetectOS  import NativeMachine
from ILCDIRAC.Core.Utilities.ResolveDependencies            import resolveDeps
from ILCDIRAC.Core.Utilities.TARsoft   import installInAnyArea, installDependencies

__RCSID__ = "$Id$"

natOS = NativeMachine()

class CombinedSoftwareInstallation(object):
  """ Combined means that it will try to install in the Shared area and in the LocalArea,
  depending on the user's rights
  """
  def __init__(self, argumentsDict):
    """ Standard constructor
    
    Defines, from dictionary of job parameters passed, a set of members to hold e.g. the
    applications and the system config.
    
    Also determines the SharedArea and LocalArea.
    """
    
    self.ops = Operations()
    
    self.job = argumentsDict.get('Job', {})
    self.ce = argumentsDict.get('CE', {})
    self.source = argumentsDict.get('Source', {})

    apps = []
    if 'SoftwarePackages' in self.job:
      if isinstance( self.job['SoftwarePackages'], basestring):
        apps = [self.job['SoftwarePackages']]
      elif isinstance( self.job['SoftwarePackages'], list ):
        apps = self.job['SoftwarePackages']


    self.jobConfig = ''
    if 'SystemConfig' in self.job:
      self.jobConfig = self.job['SystemConfig']
    elif 'Platform' in self.job:
      self.jobConfig = self.job['Platform']
    else:
      self.jobConfig = natOS.CMTSupportedConfig()[0]
    
    self.apps = []
    for app in apps:
      DIRAC.gLogger.verbose( 'Requested Package %s' % app )
      app = tuple(app.split('.'))
      if len(app) > 2:
        tempapp = app
        app = []
        app.append(tempapp[0])
        app.append(".".join(tempapp[1:]))

      deps = resolveDeps(self.jobConfig, app[0], app[1])
      for dep in deps:
        depapp = (dep['app'], dep['version'])
        self.apps.append(depapp)
      self.apps.append(app)

      

    self.ceConfigs = []
    if 'CompatiblePlatforms' in self.ce:
      self.ceConfigs = self.ce['CompatiblePlatforms']
      if isinstance( self.ceConfigs, basestring ):
        self.ceConfigs = [self.ceConfigs]
    #else:
    ### Use always the list of compatible platform.
    self.ceConfigs = natOS.CMTSupportedConfig()
    
    self.sharedArea = getSharedAreaLocation()
    DIRAC.gLogger.info("SharedArea is %s" % self.sharedArea)
    self.localArea  = getLocalAreaLocation()
    DIRAC.gLogger.info("LocalArea is %s" % self.localArea)
    
  def execute(self):
    """ Main method of the class executed by DIRAC jobAgent

    Executes the following:
      - look for the compatible platforms in the CS, see if one matches request
      - install the applications, calls :mod:`~ILCDIRAC.Core.Utilities.TARsoft`

    :return: S_OK(), S_ERROR()
    """
    if not self.apps:
      # There is nothing to do
      return DIRAC.S_OK()
    if not self.jobConfig:
      DIRAC.gLogger.error( 'No architecture requested' )
      return DIRAC.S_ERROR( 'No architecture requested' )

    found_config = False
        
    DIRAC.gLogger.info("Found CE Configs %s, compatible with system reqs %s" % (",".join(self.ceConfigs), 
                                                                                self.jobConfig))
    res = self.ops.getSections('/AvailableTarBalls')
    if not res['OK']:
      return res
    else:
      supported_systems = res['Value']
      ###look in CS if specified platform has software available. Normally consistency check is done at submission time
      for ceConfig in self.ceConfigs:
        for supp_systems in supported_systems:
          if ceConfig == supp_systems:
            self.jobConfig = ceConfig
            found_config = True
            break
    if not found_config:
      if self.ceConfigs:  # redundant check as this is done in the job agent, if locally running option might not be defined
        DIRAC.gLogger.error( 'Requested architecture not supported by CE' )
        return DIRAC.S_ERROR( 'Requested architecture not supported by CE' )
      else:
        DIRAC.gLogger.info( 'Assume locally running job, will install software in ' )
          
    areas = []
    ###Deal with shared/local area: first try to see if the Shared area exists and if not create it (try to). If it fails, fall back to local area
    if not self.sharedArea:
      if createSharedArea():
        self.sharedArea = getSharedAreaLocation()
        if self.sharedArea:
          areas.append(self.sharedArea)
          DIRAC.gLogger.info("Will attempt to install in shared area")
    else:
      areas.append(self.sharedArea)
    areas.append(self.localArea)       
       
    
    
    
    for app in self.apps:
      res = checkCVMFS(self.jobConfig, app)
      if res['OK']:
        DIRAC.gLogger.notice('Software %s is available on CVMFS, skipping' % ", ".join(app) )
        continue

      ## First install the original package in any of the areas
      resInstall = installInAnyArea(areas, app, self.jobConfig)
      if not resInstall['OK']:
        return resInstall

      ## Then install the dependencies, which can be in a different area
      resDeps = installDependencies(app, self.jobConfig, areas)
      if not resDeps['OK']:
        DIRAC.gLogger.error("Failed to install dependencies: ", resDeps['Message'])
        return S_ERROR("Failed to install dependencies")

    if self.sharedArea:  
      #List content  
      listAreaDirectory(self.sharedArea)
      
    return DIRAC.S_OK()

def listAreaDirectory(area):
  """ List the content of the given area
  """
  DIRAC.gLogger.info("Listing content of area %s :" % (area))
  res = systemCall( 5, ['ls', '-al', area] )
  if not res['OK']:
    DIRAC.gLogger.error( 'Failed to list the area directory', res['Message'] )
  elif res['Value'][0]:
    DIRAC.gLogger.error( 'Failed to list the area directory', res['Value'][2] )
  else:
    # no timeout and exit code is 0
    DIRAC.gLogger.info( res['Value'][1] )
  
def log( n, line ):
  """ print line
  """
  DIRAC.gLogger.info( line )

def getSharedAreaLocation():
  """
  Discover location of Shared SW area
  based on the list of "Operations/Software/SharedAreaLocations". Environment variables are allowed as well.
  First match is returned. Default list is:
  "/cvmfs/ilc.desy.de/clic", "$VO_ILC_SW_DIR", "$OSG_APP", "/cvmfs/oasis.opensciencegrid.org/ilc/clic"

  :returns: path to shared area
  :rtype: string
  """

  listOfSharedAreas = Operations().getValue( "Software/SharedAreaLocations",
                                             [ "/cvmfs/ilc.desy.de/clic",
                                               "$VO_ILC_SW_DIR",
                                               "$OSG_APP",
                                               "/cvmfs/oasis.opensciencegrid.org/ilc/clic",
                                             ] )  
  if not isinstance( listOfSharedAreas , list ):
    listOfSharedAreas = [ listOfSharedAreas ]
  DIRAC.gLogger.debug( "ListOfSharedAreas: %s " % ", ".join(listOfSharedAreas) )
  sharedArea = ''
  for area in listOfSharedAreas:
    DIRAC.gLogger.debug( "Checking SharedArea %s " % area )
    if area.startswith( "$" ): ## is an environment variable
      envVar = area[1:]
      if envVar in os.environ:
        sharedArea = os.path.join( os.environ[envVar], 'clic' )
    else:
      sharedArea = area
    
    if os.path.exists( sharedArea ):
      DIRAC.gLogger.info( "Using shared area %s based on %s " %( sharedArea, area ) )
      break

  if DIRAC.gConfig.getValue('/LocalSite/SharedArea',''):
    sharedArea = DIRAC.gConfig.getValue('/LocalSite/SharedArea')
    DIRAC.gLogger.debug( 'Using CE SharedArea at "%s"' % sharedArea )
  
  if len(sharedArea):
    # if defined, check that it really exists
    if not os.path.isdir( sharedArea ):
      DIRAC.gLogger.warn( 'Missing Shared Area Directory:', sharedArea )
      sharedArea = ''
  
  return sharedArea

def createSharedArea():
  """
   Method to be used by SAM jobs to make sure the proper directory structure is created
   if it does not exists
  """
  if 'VO_ILC_SW_DIR' not in os.environ and "OSG_APP" not in os.environ:
    DIRAC.gLogger.info( 'VO_ILC_SW_DIR and OSG_APP not defined.' )
    return False
  sharedArea = '.'
  if 'VO_ILC_SW_DIR' in os.environ:
    sharedArea = os.environ['VO_ILC_SW_DIR']
  elif "OSG_APP" in os.environ:
    sharedArea = os.environ["OSG_APP"]
  if sharedArea == '.':
    DIRAC.gLogger.info( 'VO_ILC_SW_DIR or OSG_APP points to "."' )
    return False

  #if not os.path.isdir( sharedArea ):
  #  DIRAC.gLogger.error( 'VO_ILC_SW_DIR="%s" is not a directory' % sharedArea )
  #  return False

  sharedArea = os.path.join( sharedArea, 'clic' )
  try:
    if os.path.isdir( sharedArea ) and not os.path.islink( sharedArea ) :
      return True
    if not os.path.exists( sharedArea ):
      os.makedirs( sharedArea )
      return True
    os.remove( sharedArea )
    os.makedirs( sharedArea )
    return True
  except OSError as x:
    DIRAC.gLogger.error('Problem trying to create shared area', str(x))
    return False

def getLocalAreaLocation():
  """
   Discover Location of Local SW Area.
   This area is populated by DIRAC job Agent for jobs needing SW not present
   in the Shared Area.
   Currently is always the location used as the software is not in the shared area.
  """
  if DIRAC.gConfig.getValue('/LocalSite/LocalArea', ''):
    localArea = DIRAC.gConfig.getValue('/LocalSite/LocalArea')
  else:
    localArea = os.path.join( DIRAC.rootPath, 'LocalArea' )

  # check if already existing directory
  if not os.path.isdir( localArea ):
    # check if we can create it
    if os.path.exists( localArea ):
      try:
        os.remove( localArea )
      except OSError as err:
        DIRAC.gLogger.error( 'Cannot remove:', localArea + " because " + str(err) )
        localArea = ''
    else:
      try:
        os.mkdir( localArea )
      except OSError as err:
        DIRAC.gLogger.error( 'Cannot create:', localArea + " because " + str(err) )
        localArea = ''
  return localArea

def getSoftwareFolder(platform, appname, appversion):
  """ 
  Discover location of a given folder, either the local or the shared area

  :param string platform: platform
  :param string appname: name of the application
  :param string appversion: version of the application

  """
  res = checkCVMFS(platform, [appname, appversion])
  if res["OK"]:
    return S_OK(res['Value'][0])
  
  app_tar = Operations().getValue('/AvailableTarBalls/%s/%s/%s/TarBall'%(platform, appname, appversion), '')  
  if not app_tar:
    return S_ERROR("Could not find %s, %s name from CS" % (appname, appversion) )
  if app_tar.count("gz"):
    folder = app_tar.replace(".tgz","").replace(".tar.gz", "")
  else:
    folder = app_tar
    
  localArea = getLocalAreaLocation()
  sharedArea = getSharedAreaLocation()
  if os.path.exists(os.path.join(localArea, folder)):
    mySoftwareRoot = localArea
  elif os.path.exists(os.path.join(sharedArea, folder)):
    mySoftwareRoot = sharedArea
  else:
    return S_ERROR('Missing installation of %s!' % folder)
  mySoftDir = os.path.join(mySoftwareRoot, folder)
  return S_OK(mySoftDir)

def getEnvironmentScript(platform, appname, appversion, fcn_env):
  """Return the path to the environment script, either from CVMFS, or from the fcn_env function

  :param string platform: platform
  :param string appname: name of the application
  :param string appversion: version of the application

  :param function fcn_env: function provided by the Application daughter class
    to create its own environment in case there is no environment script in the
    application version. Signature has to be *fcn_env(platform, appname,
    appversion)* and has to return *S_OK(pathToScript)*

  """
  res = checkCVMFS(platform, [appname, appversion])
  if res["OK"]:
    cvmfsenv = res['Value'][1]
    DIRAC.gLogger.info("SoftwareInstallation: CVMFS Script found: %s" % cvmfsenv)
    if cvmfsenv:
      return S_OK( cvmfsenv )
  #if CVMFS script is not here, the module produces its own.
  return fcn_env(platform, appname, appversion)

def checkCVMFS(platform, app):
  """ Check the existence of the CVMFS path for given platform and application

  :param string platform: platform
  :param tuple app: tuple off application name and version
  :returns: S_OK of tuple of path and environmen stript
  """
  name, version = app
  csPath = "/AvailableTarBalls/%s/%s/%s" % (platform, name, version)
  cvmfspath = Operations().getValue(csPath + "/CVMFSPath" ,"")
  envScript = Operations().getValue(csPath + "/CVMFSEnvScript" ,"")
  if cvmfspath and os.path.exists(cvmfspath):
    return S_OK((cvmfspath, envScript))
  DIRAC.gLogger.info("Cannot find package on cvmfs:", Operations().getOptionsDict(csPath))
  return S_ERROR('Missing CVMFS!')

def unzip_file_into_dir(myfile, mydir):
  """Used to unzip the downloaded detector model
  """
  zfobj = zipfile.ZipFile(myfile)
  zfobj.extractall(path=mydir)
