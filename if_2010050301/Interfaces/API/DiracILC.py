"""
DiracILC is the API to use to submit jobs in the ILC VO

@since:  Apr 13, 2010

@author: Stephane Poss
"""
from DIRAC.Interfaces.API.Dirac                     import Dirac
from DIRAC.Interfaces.API.Job                       import Job
from DIRAC.Core.Utilities.List                      import breakListIntoChunks, sortList

from DIRAC import gConfig, S_ERROR, S_OK, gLogger
import string


COMPONENT_NAME='DiracILC'

class DiracILC(Dirac):
  """DiracILC is VO specific API Dirac
  
  Adding specific ILC functionalities to the Dirac class, and implement the preSubmissionChecks method
  """
  def __init__(self, WithRepo=False, RepoLocation=''):
    """Internal initialization of the ILCDIRAC API.
    """
    #self.dirac = Dirac(WithRepo=WithRepo, RepoLocation=RepoLocation)
    Dirac.__init__(self,WithRepo=WithRepo, RepoLocation=RepoLocation)
    self.log = gLogger
    
  def preSubmissionChecks(self,job,mode):
    """Overridden method from DIRAC.Interfaces.API.Dirac
    
    Checks from CS that required software packages are available.
    @param job: job definition.
    @param mode: submission mode, not used here. 
    
    @return: S_OK() or S_ERROR()
    """
    return self._do_check(job)
    
  def checkparams(self,job):
    """Helper method
    
    Method used for stand alone checks of job integrity. Calls the formulation error checking of the job
    
    Actually checks that all input are available and checks that the required software packages are available in the CS
    @param job: job object
    @return: S_OK() or S_ERROR()  
    """
    try:
      formulationErrors = job._getErrors()
    except Exception, x:
      self.log.verbose( 'Could not obtain job errors:%s' % ( x ) )
      formulationErrors = {}

    if formulationErrors:
      for method, errorList in formulationErrors.items():
        self.log.error( '>>>> Error in %s() <<<<\n%s' % ( method, string.join( errorList, '\n' ) ) )
      return S_ERROR( formulationErrors )
    return self._do_check(job)

  def retrieveRepositoryOutputDataLFNs(self,requestedStates = ['Done']):
    """Helper function
    
    Get the list of uploaded output data for a set of jobs in a repository
    
    @param requestedStates: List of states requested for filtering the list
    @type requestedStates: list of strings
    @return: list
    """
    list = []
    if not self.jobRepo:
      gLogger.warn( "No repository is initialised" )
      return S_OK()
    jobs = self.jobRepo.readRepository()['Value']
    for jobID in sortList( jobs.keys() ):
      jobDict = jobs[jobID]
      if jobDict.has_key( 'State' ) and ( jobDict['State'] in requestedStates ):
        if ( jobDict.has_key( 'OutputData' ) and ( not int( jobDict['OutputData'] ) ) ) or ( not jobDict.has_key( 'OutputData' ) ):
          params = self.parameters(int(jobID))
          if params['OK']:
            if params['Value'].has_key('UploadedOutputData'):
              lfn = params['Value']['UploadedOutputData']
              list.append(lfn)
    return list
  
  def _do_check(self,job):  
    sysconf = job.systemConfig
    apps = job.workflow.findParameter("SoftwarePackages").getValue()
    for appver in apps.split(";"):
      app = appver.split(".")[0].lower()#first element
      vers = appver.split(".")[1:]#all the others
      vers = string.join(vers,".")
      res = self._checkapp(sysconf,app,vers)
      if not res['OK']:
        return res
    return res
  
  def _checkapp(self,config,appName,appVersion):
    app_version= gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s/TarBall'%(config,appName,appVersion),'')
    if not app_version:
      self.log.error("Could not find the specified software %s_%s for %s, check in CS"%(appName,appVersion,config))
      return S_ERROR("Could not find the specified software %s_%s for %s, check in CS"%(appName,appVersion,config))
    return S_OK()
  