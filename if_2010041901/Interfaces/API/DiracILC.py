'''

DiracILC is the API to use to submit jobs in the ILC VO

Created on Apr 13, 2010

@author: sposs
'''
from DIRAC.Interfaces.API.Dirac                     import Dirac
from DIRAC.Interfaces.API.Job                     import Job
from DIRAC import gConfig, S_ERROR, S_OK, gLogger
import string


COMPONENT_NAME='DiracILC'

class DiracILC(Dirac):
  """ 
  """
  def __init__(self, WithRepo=False, RepoLocation=''):
    """Internal initialization of the DIRAC API.
    """
    #self.dirac = Dirac(WithRepo=WithRepo, RepoLocation=RepoLocation)
    Dirac.__init__(self,WithRepo=WithRepo, RepoLocation=RepoLocation)
    self.log = gLogger
    
  def preSubmissionChecks(self,job,mode):
    return self._do_check(job)
    
  def checkparams(self,job):
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
  
  def _do_check(self,job):  
    sysconf = job.systemConfig
    apps = job.workflow.findParameter("SoftwarePackages").getValue()
    for appver in apps.split(";"):
      app = appver.split(".")[0].lower()#first element
      vers = appver.split(".")[1:]#all the others
      vers = string.join(vers,".").lower()
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