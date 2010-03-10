# $HeadURL$
# $Id$

'''

Based on LHCbDIRAC.Core.Utilities.CombinedSoftwareInstalation module, has
more or less the same functionality : installs software

BE PARANOIAC !!!

Created on Jan 15, 2010

@author: sposs
@aauthor: pmajewsk
'''
import os, urllib
#from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
import DIRAC
import tarfile
from LCDDIRAC.Core.Utilities.DetectOS import NativeMachine
natOS = NativeMachine()


#SoftTarBallLFN = "/lcd/.../.../.../.../"
#SoftTarBallLFN = "/afs/cern.ch/eng/clic/data/software/"
TarBallURL = "http://www.cern.ch/lcd-data/software/"

class CombinedSoftwareInstallation:

  def __init__(self,argumentsDict):
    """ Standard constructor
    """
    self.job = {}
    if argumentsDict.has_key('Job'):
      self.job = argumentsDict['Job']
    self.ce = {}
    if argumentsDict.has_key('CE'):
      self.ce = argumentsDict['CE']
    self.source = {}
    if argumentsDict.has_key('Source'):
      self.source = argumentsDict['Source']

    apps = []
    if self.job.has_key('SoftwarePackages'):
      if type( self.job['SoftwarePackages'] ) == type(''):
        apps = [self.job['SoftwarePackages']]
      elif type( self.job['SoftwarePackages'] ) == type([]):
        apps = self.job['SoftwarePackages']

    self.apps = []
    for app in apps:
      DIRAC.gLogger.verbose( 'Requested Package %s' % app )
      app = tuple(app.split('.'))
      self.apps.append(app)

    self.jobConfig = ''
    if self.job.has_key( 'SystemConfig' ):
      self.jobConfig = self.job['SystemConfig']
    else:
      self.jobConfig = natOS.CMTSupportedConfig()[0]
      
    self.ceConfigs = []
    if self.ce.has_key('CompatiblePlatforms'):
      self.ceConfigs = self.ce['CompatiblePlatforms']
      if type(self.ceConfigs) == type(''):
        self.ceConfigs = [self.ceConfigs]
    #else:
    self.ceConfigs = natOS.CMTSupportedConfig()

    self.sharedArea = SharedArea()
    self.localArea  = LocalArea()
    
  def execute(self):
    """
     Main method of the class executed by DIRAC jobAgent
    """
    if not self.apps:
      # There is nothing to do
      return DIRAC.S_OK()
    if not self.jobConfig:
      DIRAC.gLogger.error( 'No architecture requested' )
      return DIRAC.S_ERROR( 'No architecture requested' )
    
    if not self.jobConfig in self.ceConfigs:
      if self.ceConfigs:  # redundant check as this is done in the job agent, if locally running option might not be defined
        DIRAC.gLogger.error( 'Requested architecture not supported by CE' )
        return DIRAC.S_ERROR( 'Requested architecture not supported by CE' )
      else:
        DIRAC.gLogger.info( 'Assume locally running job, will install software in /LocalSite/LocalArea=%s' %(self.localArea))

    for app in self.apps:
      DIRAC.gLogger.info('Attempting to install %s_%s for %s' %(app[0],app[1],self.jobConfig))
      result = CheckInstallSoftware(app,self.jobConfig,self.localArea)
      if not result:
        DIRAC.gLogger.error('Failed to install software','%s_%s' %(app))
        return DIRAC.S_ERROR('Failed to install software')
      else:
        DIRAC.gLogger.info('%s was successfully installed for %s' %(app,self.jobConfig))

    return DIRAC.S_OK()

def MySiteRoot():
    """Returns the MySiteRoot for the current local and / or shared areas.
    Needed by MokkaAnalysis and MarlinAnalysis modules
    """
    mySiteRoot = ''
    localArea=LocalArea()
    if not localArea:
        DIRAC.gLogger.error( 'Failed to determine Local SW Area' )
        return mySiteRoot
    sharedArea=SharedArea()
    if not sharedArea:
        DIRAC.gLogger.error( 'Failed to determine Shared SW Area' )
        return localArea
    mySiteRoot = '%s:%s' %(localArea,sharedArea)
    return mySiteRoot

def CheckInstallSoftware(app,config,area):
  """Will perform a local area installation
  """
  os.chdir(area)
  appName    = app[0]
  appVersion = app[1]
  appName = appName.lower()
  app_tar = DIRAC.gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s'%(config,appName,appVersion),'')
  if not app_tar:
    DIRAC.gLogger.error('Could not find tar ball for %s %s'%(appName,appVersion))
    return False
  #app_tar = appName+appVersion+".tar.gz"
  #app_tar = appName+appVersion+".tgz"

  
  #rm = ReplicaManager()
  
  #NOTE: must cd to LOCAL area directory (install_project requirement)
#  if not os.path.exists('%s/%s' %(os.getcwd(),app_tar)):
#    #res = rm.getFile('%s%s' %(SoftTarBallLFN,app_tar))
#    res = {}
#    res['OK'] = True
#    if not res["OK"]:
#        return res
#  if not os.path.exists('%s/%s' %(os.getcwd(),app_tar)):
#    DIRAC.gLogger.error('%s%s could not be downloaded' %(SoftTarBallLFN,app_tar))
#    #print('%s/%s' %(os.getcwd(),app_tar))
#    return False

#downloading file from url, but don't do if file is already there.
  if not os.path.exists("%s/%s"%(os.getcwd(),app_tar)):
    try :
      DIRAC.gLogger.debug("Downloading software", '%s_%s' %(appName,appVersion))
      #Copy the file locally, don't try to read from remote, soooo slow
      #Use string conversion %s%s to set the address, makes the system more stable
      tarball,headers = urllib.urlretrieve("%s%s"%(TarBallURL,app_tar),app_tar)
    except:
      DIRAC.gLogger.exception()
      return False
  if not os.path.exists("%s/%s"%(os.getcwd(),app_tar)):
    DIRAC.gLogger.error('Failed to download software','%s_%s' %(appName,appVersion))
    return False

  app_tar_to_untar = tarfile.open(app_tar)
  app_tar_to_untar.extractall()
  
  #remove now useless tar ball
  try:
    os.unlink(app_tar)
  except:
    DIRAC.gLogger.exception()

  return True

          
def log( n, line ):
  DIRAC.gLogger.info( line )
    
def SharedArea():
  """
   Discover location of Shared SW area
   This area is populated by a tool independent of the DIRAC jobs
  """
  sharedArea = ''
  if os.environ.has_key('VO_ILC_SW_DIR'):
    sharedArea = os.path.join(os.environ['VO_ILC_SW_DIR'],'lcd')
    DIRAC.gLogger.debug( 'Using VO_ILC_SW_DIR at "%s"' % sharedArea )
    if os.environ['VO_ILC_SW_DIR'] == '.':
      if not os.path.isdir( 'lcd' ):
        os.mkdir( 'lcd' )
  elif DIRAC.gConfig.getValue('/LocalSite/SharedArea',''):
    sharedArea = DIRAC.gConfig.getValue('/LocalSite/SharedArea')
    DIRAC.gLogger.debug( 'Using CE SharedArea at "%s"' % sharedArea )

  if sharedArea:
    # if defined, check that it really exists
    if not os.path.isdir( sharedArea ):
      DIRAC.gLogger.error( 'Missing Shared Area Directory:', sharedArea )
      sharedArea = ''

  return sharedArea

def CreateSharedArea():
  """
   Method to be used by SAM jobs to make sure the proper directory structure is created
   if it does not exists
  """
  if not os.environ.has_key('VO_ILC_SW_DIR'):
    DIRAC.gLogger.info( 'VO_ILC_SW_DIR not defined.' )
    return False

  sharedArea = os.environ['VO_ILC_SW_DIR']
  if sharedArea == '.':
    DIRAC.gLogger.info( 'VO_ILC_SW_DIR points to "."' )
    return False

  if not os.path.isdir( sharedArea ):
    DIRAC.gLogger.error( 'VO_ILC_SW_DIR="%s" is not a directory' % sharedArea )
    return False

  sharedArea = os.path.join( sharedArea, 'lcd' )
  try:
    if os.path.isdir( sharedArea ) and not os.path.islink( sharedArea ) :
      return True
    if not os.path.exists( sharedArea ):
      os.mkdir( sharedArea )
      return True
    os.remove( sharedArea )
    os.mkdir( sharedArea )
    return True
  except Exception,x:
    DIRAC.gLogger.error('Problem trying to create shared area',str(x))
    return False

def LocalArea():
  """
   Discover Location of Local SW Area.
   This area is populated by DIRAC job Agent for jobs needing SW not present
   in the Shared Area.
  """
  if DIRAC.gConfig.getValue('/LocalSite/LocalArea',''):
    localArea = DIRAC.gConfig.getValue('/LocalSite/LocalArea')
  else:
    localArea = os.path.join( DIRAC.rootPath, 'LocalArea' )

  # check if already existing directory
  if not os.path.isdir( localArea ):
    # check if we can create it
    if os.path.exists( localArea ):
      try:
        os.remove( localArea )
      except Exception, x:
        DIRAC.gLogger.error( 'Cannot remove:', localArea )
        localArea = ''
    else:
      try:
        os.mkdir( localArea )
      except Exception, x:
        DIRAC.gLogger.error( 'Cannot create:', localArea )
        localArea = ''
  return localArea
