'''
Created on Feb 17, 2012

@author: Stephane Poss
'''
__RCSID__ = "$ Id: $"

from DIRAC                                                                import S_OK, S_ERROR, gConfig, gLogger, gMonitor
from DIRAC.Core.Base.AgentModule                                          import AgentModule

from ILCDIRAC.ProcessProductionSystem.Client                              import ProcessProdClient

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
    return S_OK()

  ##############################################################################
  def execute(self):
    ##First we need to set the site statuses according to what is in the CS
    self.ppc.changeSiteStatus()
    
    ##Then we need to get new installation tasks
    res = self.ppc.getInstallSoftwareTask()
    if not res['OK']:
      self.log.error('Failed to obtain task')
    task_dict = res['Value']
    for softdict in task_dict.values():
      for site in softdict['Site']:
        j = UserJob()
        j.setSystemConfig(softdict['Platform'])
        j.dontPrompMe()
        j.setDestination(site)
        
        #Add the application here somehow.
        
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
  
  
  