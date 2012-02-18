'''
Created on Feb 17, 2012

@author: Stephane Poss
'''
__RCSID__ = "$ Id: $"

from DIRAC                                                                import S_OK, S_ERROR, gConfig, gLogger, gMonitor
from DIRAC.Core.Base.AgentModule                                          import AgentModule
from DIRAC.Core.DISET.RPCClient                                           import RPCClient
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
from ILCDIRAC.ProcessProductionSystem.Client                              import ProcessProdClient
from ILCDIRAC.ProcessProductionSystem.Utilities.SoftwareInstall import SoftwareInstall

from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob
from ILCDIRAC.Interfaces.API.DiracILC import DiracILC


AGENT_NAME = 'ProcessProduction/SoftwareManagementAgent'

class SoftwareManagementAgent(AgentModule):
  """ Agent to run software management things
  """
  def initialize(self):
    self.pollingTime = self.am_getOption('PollingTime',86400)
    gMonitor.registerActivity("Iteration","Agent Loops",AGENT_NAME,"Loops/min",gMonitor.OP_SUM)
    self.ppc = ProcessProdClient()
    self.dirac = DiracILC()
    self.diracadmin = DiracAdmin()
    return S_OK()

  ##############################################################################
  def execute(self):
    """  First we update the site list and banned site
    """
    sites= self.diracadmin.getSiteMask()['Value']
    for site in sites:
      res =self.ppc.changeSiteStatus({'SiteName':site,'Status':'OK'})
      if not res['OK']:
        self.log.error('Cannot add or update site %s'%site)
        
    banned_sites = self.diracadmin.getBannedSites()
    for banned_site in banned_sites:
      self.ppc.changeSiteStatus({'SiteName':banned_site,'Status':'Banned'})
      if not res['OK']:
        self.log.error('Cannot mark as banned site %s'%site)    
        
    ##Then we need to get new installation tasks
    res = self.ppc.getInstallSoftwareTask()
    if not res['OK']:
      self.log.error('Failed to obtain task')
    task_dict = res['Value']
    for softdict in task_dict.values():
      for site in softdict['Site']:
        j = UserJob()
        j.setSystemConfig(softdict['Platform'])
        j.dontPromptMe()
        j.setDestination(site)
        j._addSoftware(softdict['AppName'],softdict['AppVersion'])
        #Add the application here somehow.
        res  = j.append(SoftwareInstall())
        if not res['OK']:
          self.log.error(res['Message'])
          continue
        res = self.dirac.submit(j)
        if not res['OK']:
          self.log.error('Could not create the job')
          continue
        jobdict = {}
        jobdict.update(softdict)
        jobdict['JobID'] = res['Value']
        jobdict['Status'] = 'Waiting'
        
        res = self.ppc.addOrUpdateJob(jobdict)
        if not res['OK']:
          self.log.error('Could not add job:%s'%res['Message'])
    
    return S_OK()
  
  
  