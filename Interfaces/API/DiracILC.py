'''

DiracILC is the API to use to submit jobs in the ILC VO

Created on Apr 13, 2010

@author: sposs
'''
from DIRAC.Interfaces.API.Dirac                     import Dirac
from DIRAC.Interfaces.API.Job                     import Job
from DIRAC import gConfig, S_ERROR, S_OK
import string


COMPONENT_NAME='DiracILC'

class DiracILC(Dirac):
  """ 
  """
  def __init__(self, WithRepo=False, RepoLocation=''):
    """Internal initialization of the DIRAC API.
    """
    Dirac.__init__(self,WithRepo=WithRepo, RepoLocation=RepoLocation)
    
  def submit(self,job,mode = 'wms'):
    sysconf = job.systemConfig
    apps = job.workflow.findParameter("SoftwarePackages").getValue()
    res = S_OK()
    for appver in apps.slit(";"):
      app = appver.slit(".")[0]#first element
      vers = appver.slit(".")[1:]#all the others
      vers = string.join(vers,".")
      res = self._checkapp(sysconf,app,vers)
    if not res['OK']:
      return res
    return Dirac.submit(job,mode)
  
  def _checkapp(self,config,appName,appVersion):
    app_version= gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s/TarBall'%(config,appName,appVersion),'')
    if not app_version:
      return S_ERROR("Could not find the specified software %s_%s for %s"%(appName,appVersion,config))
    return S_OK()