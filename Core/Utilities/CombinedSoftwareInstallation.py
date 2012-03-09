# $HeadURL$
# $Id$
"""
Based on LHCbDIRAC.Core.Utilities.CombinedSoftwareInstalation module, 

New version of CombinedSoftwareInstallation, installs properly ILD soft and SiD soft, and all dependencies

@since: Feb 4, 2010

@author: Stephane Poss and Przemyslaw Majewski
"""
import os, string
#from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
import DIRAC 
from ILCDIRAC.Core.Utilities.TARsoft import TARinstall
#from ILCDIRAC.Core.Utilities.JAVAsoft import JAVAinstall 
from ILCDIRAC.Core.Utilities.DetectOS import NativeMachine
from DIRAC.Core.Utilities.Subprocess                                import systemCall

from DIRAC import S_OK,S_ERROR

natOS = NativeMachine()

class CombinedSoftwareInstallation(object):
  def __init__(self,argumentsDict):
    """ Standard constructor
    
    Defines, from dictionary of job parameters passed, a set of members to hold e.g. the applications and the system config
    
    Also determines the SharedArea and LocalArea.
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
      if len(app)>2:
        tempapp = app
        app=[]
        app.append(tempapp[0])
        app.append(string.join(tempapp[1:], "."))
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
    ### Use always the list of compatible platform.
    self.ceConfigs = natOS.CMTSupportedConfig()

    self.sharedArea = SharedArea()
    DIRAC.gLogger.info("SharedArea is %s"%self.sharedArea)
    self.localArea  = LocalArea()
    DIRAC.gLogger.info("LocalArea is %s"%self.localArea)
    
  def execute(self):
    """ Main method of the class executed by DIRAC jobAgent

    Executes the following:
      - look for the compatible platforms in the CS, see if one matches request
      - install the applications, calls L{TARsoft}

    @return: S_OK(), S_ERROR()
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
      ###look in CS if specified platform has software available. Normally consistency check is done at submission time
      for ceConfig in self.ceConfigs:
        for supp_systems in supported_systems:
          if ceConfig == supp_systems:
            self.jobConfig = ceConfig
            found_config=True
            break
          
    areas = []
    ###Deal with shared/local area: first try to see if the Shared area exists and if not create it (try to). If it fails, fall back to local area
    if not self.sharedArea:
      if CreateSharedArea():
        self.sharedArea = SharedArea()
        if self.sharedArea:
          areas.append(self.sharedArea)
          DIRAC.gLogger.info("Will attempt to install in shared area")
    else:
      areas.append(self.sharedArea)
    areas.append(self.localArea)       
       
    if not found_config:
      if self.ceConfigs:  # redundant check as this is done in the job agent, if locally running option might not be defined
        DIRAC.gLogger.error( 'Requested architecture not supported by CE' )
        return DIRAC.S_ERROR( 'Requested architecture not supported by CE' )
      else:
        DIRAC.gLogger.info( 'Assume locally running job, will install software in ' )
    
    for app in self.apps:
      failed = False    
      for area in areas:
        DIRAC.gLogger.info('Attempting to install %s_%s for %s in %s' %(app[0],app[1],self.jobConfig,area))
        res = TARinstall(app,self.jobConfig,area)
        if not res['OK']:
          DIRAC.gLogger.error('Failed to install software in %s: %s'%(area,res['Message']),'%s_%s' %(app[0],app[1]))
          failed = True
          continue
        else:
          DIRAC.gLogger.info('%s was successfully installed for %s in %s' %(app,self.jobConfig,area))
          failed = False
          break
      if failed:
        return DIRAC.S_ERROR("Failed to install software")
      
    if self.sharedArea:  
      #List content  
      DIRAC.gLogger.info("Listing content of shared area :")
      res = systemCall( 5, ['ls', '-al',self.sharedArea] )
      if not res['OK']:
        DIRAC.gLogger.error( 'Failed to list the shared area directory', res['Message'] )
      elif res['Value'][0]:
        DIRAC.gLogger.error( 'Failed to list the shared area directory', res['Value'][2] )
      else:
        # no timeout and exit code is 0
        DIRAC.gLogger.info( res['Value'][1] )
      
    return DIRAC.S_OK()
  
def log( n, line ):
  DIRAC.gLogger.info( line )

def SharedArea():
  """
   Discover location of Shared SW area
   This area is populated by a tool independent of the DIRAC jobs
   Not used yet in ILC DIRAC, but should be
  """
  sharedArea = ''
  if os.environ.has_key('VO_ILC_SW_DIR'):
    sharedArea = os.path.join(os.environ['VO_ILC_SW_DIR'],'clic')
    DIRAC.gLogger.debug( 'Using VO_ILC_SW_DIR at "%s"' % sharedArea )
    if os.environ['VO_ILC_SW_DIR'] == '.':
      if not os.path.isdir( 'clic' ):
        os.mkdir( 'clic' )
  elif DIRAC.gConfig.getValue('/LocalSite/SharedArea',''):
    sharedArea = DIRAC.gConfig.getValue('/LocalSite/SharedArea')
    DIRAC.gLogger.debug( 'Using CE SharedArea at "%s"' % sharedArea )

  if len(sharedArea):
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
  if not os.environ.has_key('VO_ILC_SW_DIR') and not os.environ.has_key("OSG_APP"):
    DIRAC.gLogger.info( 'VO_ILC_SW_DIR and OSG_APP not defined.' )
    return False
  sharedArea = '.'
  if os.environ.has_key('VO_ILC_SW_DIR'):
    sharedArea = os.environ['VO_ILC_SW_DIR']
  elif os.environ.has_key("OSG_APP"):
    sharedArea = os.environ["OSG_APP"]
  if sharedArea == '.':
    DIRAC.gLogger.info( 'VO_ILC_SW_DIR or OSG_APP points to "."' )
    return False

  if not os.path.isdir( sharedArea ):
    DIRAC.gLogger.error( 'VO_ILC_SW_DIR="%s" is not a directory' % sharedArea )
    return False

  sharedArea = os.path.join( sharedArea, 'clic' )
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
   Currently is always the location used as the software is not in the shared area.
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

def getSoftwareFolder(folder):
  """ 
  Discover location of a given folder, either the local or the shared area
  """
  localArea = LocalArea()
  sharedArea = SharedArea()
  if os.path.exists('%s%s%s' %(localArea,os.sep,folder)):
    mySoftwareRoot = localArea
  elif os.path.exists('%s%s%s' %(sharedArea,os.sep,folder)):
    mySoftwareRoot = sharedArea
  else:
    return S_ERROR('Missing installation of %s!'%folder)
  mySoftDir = os.path.join(mySoftwareRoot,folder)
  return S_OK(mySoftDir)    