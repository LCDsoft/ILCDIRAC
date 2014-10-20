'''
Created on Feb 17, 2012

@author: Stephane Poss
'''
#pylint: skip-file
__RCSID__ = "$ Id: $"

from DIRAC                                                                import S_OK, gMonitor
from DIRAC.Core.Base.AgentModule                                          import AgentModule
from DIRAC.Core.Security.ProxyInfo                                        import getProxyInfo

from DIRAC.Interfaces.API.DiracAdmin                                      import DiracAdmin
from ILCDIRAC.ProcessProductionSystem.Client.ProcessProdClient            import ProcessProdClient
from ILCDIRAC.ProcessProductionSystem.Utilities.SoftwareInstall           import SoftwareInstall

from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob
from ILCDIRAC.Interfaces.API.DiracILC import DiracILC


AGENT_NAME = 'ProcessProduction/SoftwareManagementAgent'

class SoftwareManagementAgent( AgentModule ):
  """ Agent to run software management things
  """
  def initialize(self):
    self.pollingTime = self.am_getOption('PollingTime', 86400)
    gMonitor.registerActivity("Iteration", "Agent Loops", AGENT_NAME, "Loops/min", gMonitor.OP_SUM)
    self.ppc = ProcessProdClient()
    self.dirac = DiracILC()
    self.diracadmin = DiracAdmin()
    self.am_setOption( 'shifterProxy', 'Admin' )
    
    return S_OK()

  ##############################################################################
  def execute(self):
    """  First we update the site list and banned site
    """
    res = getProxyInfo(False, False)
    if not res['OK']:
      self.log.error("submitTasks: Failed to determine credentials for submission", res['Message'])
      return res
    proxyInfo = res['Value']
    owner = proxyInfo['username']
    ownerGroup = proxyInfo['group']
    self.log.info("submitTasks: Jobs will be submitted with the credentials %s:%s" % (owner, ownerGroup))    
    
    sites = self.diracadmin.getSiteMask()['Value']
    for site in sites:
      res = self.ppc.changeSiteStatus( {'SiteName' : site, 'Status' : 'OK'} )
      if not res['OK']:
        self.log.error('Cannot add or update site %s' % site)
        
    banned_sites = self.diracadmin.getBannedSites()['Value']
    for banned_site in banned_sites:
      self.ppc.changeSiteStatus( {'SiteName' : banned_site, 'Status' : 'Banned'} )
      if not res['OK']:
        self.log.error('Cannot mark as banned site %s' % banned_site)
        
    ##Then we need to get new installation tasks
    res = self.ppc.getInstallSoftwareTask()
    if not res['OK']:
      self.log.error('Failed to obtain task')
    task_dict = res['Value']
    for softdict in task_dict.values():
      self.log.info('Will install %s %s at %s' % (softdict['AppName'], softdict['AppVersion'], softdict['Sites']))
      for site in softdict['Sites']:
        j = UserJob()
        j.setPlatform(softdict['Platform'])
        j.dontPromptMe()
        j.setDestination(site)
        j.setJobGroup("Installation")
        j.setName('install_%s' % site)
        j._addSoftware(softdict['AppName'], softdict['AppVersion'])
        #Add the application here somehow.
        res  = j.append(SoftwareInstall())
        if not res['OK']:
          self.log.error(res['Message'])
          continue
        res = j.submit(self.dirac)
        #res = self.dirac.submit(j)
        if not res['OK']:
          self.log.error('Could not create the job')
          continue
        jobdict = {}
        jobdict['AppName'] = softdict['AppName']
        jobdict['AppVersion'] = softdict['AppVersion']
        jobdict['Platform'] = softdict['Platform']
        jobdict['JobID'] = res['Value']
        jobdict['Status'] = 'Waiting'
        jobdict['Site'] = site
        res = self.ppc.addOrUpdateJob(jobdict)
        if not res['OK']:
          self.log.error('Could not add job %s: %s' % (jobdict['JobID'], res['Message']))
    
    ##Monitor jobs
    jobs = {}
    res = self.ppc.getJobs()
    if not res['OK']:
      self.log.error('Could not retrieve jobs')
    else:
      jobs = res['Value']
      for job in jobs:
        res = self.dirac.status(job['JobID'])
        if res['OK']:
          jobstatuses = res['Value'] 
          job['Status'] = jobstatuses['JobID']['Status']
          res = self.ppc.addOrUpdateJob(job)
          if not res['OK']:
            self.log.error("Failed to updated job %s: %s" % (job['JobID'], res['Message']))
        else:
          self.log.error("Failed to update job %s status" % job['JobID'])
          
    return S_OK()
  
  
  