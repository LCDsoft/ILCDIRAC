
'''

Based on LHCbDIRAC.Core.Utilities.CombinedSoftwareInstalation module, 

New version of CombinedSoftwareInstallation, installs properly ILD soft and SiD soft

@author: sposs
@aauthor: pmajewsk
'''
import os, string
#from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
import DIRAC 
from ILCDIRAC.Core.Utilities.TARsoft import TARinstall
#from ILCDIRAC.Core.Utilities.JAVAsoft import JAVAinstall 
from ILCDIRAC.Core.Utilities.DetectOS import NativeMachine
natOS = NativeMachine()

class CLICCombinedSoftwareInstallation:
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

    found_config = False
        
    DIRAC.gLogger.info("Found CE Configs %s, compatible with system reqs %s"%(string.join(self.ceConfigs,","),self.jobConfig))
    res = DIRAC.gConfig.getSections('/Operations/AvailableTarBalls')
    if not res['OK']:
      return res
    else:
      supported_systems = res['Value']
      for ceConfig in self.ceConfigs:
        for supp_systems in supported_systems:
          if ceConfig == supp_systems:
            self.jobConfig = ceConfig
            found_config=True
            break
          
    if not found_config:
      if self.ceConfigs:  # redundant check as this is done in the job agent, if locally running option might not be defined
        DIRAC.gLogger.error( 'Requested architecture not supported by CE' )
        return DIRAC.S_ERROR( 'Requested architecture not supported by CE' )
      else:
        DIRAC.gLogger.info( 'Assume locally running job, will install software in /LocalSite/LocalArea=%s' %(self.localArea))
    for app in self.apps:
      DIRAC.gLogger.info('Attempting to install %s_%s for %s' %(app[0],app[1],self.jobConfig))
      #if app[0].lower()=="marlin" or app[0].lower()=="mokka" or app[0].lower()=="slic" :
      res = TARinstall(app,self.jobConfig,self.localArea)
      #if app[0].lower()=="lcsim":
      #  res = JAVAinstall(app,self.jobConfig,self.localArea)
      if not res['OK']:
        DIRAC.gLogger.error('Failed to install software','%s_%s' %(app))
        return DIRAC.S_ERROR('Failed to install software')
      else:
        DIRAC.gLogger.info('%s was successfully installed for %s' %(app,self.jobConfig))
    return DIRAC.S_OK()
  
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
      