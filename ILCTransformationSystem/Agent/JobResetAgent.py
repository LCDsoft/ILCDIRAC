"""
documentation
"""

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.PrettyPrint import printTable

from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.Resources.Storage.StorageElement import StorageElement

__RCSID__ = "$Id$"

AGENT_NAME = 'ILCTransformation/JobResetAgent'

class JobResetAgent(AgentModule):
  """ JobResetAgent """

  def __init__(self, *args, **kwargs):
    AgentModule.__init__(self, *args, **kwargs)
    self.name = 'JobResetAgent'
    self.enabled = False
    self.shifterProxy = 'DataManager'

    self.addressTo = ["andre.philippe.sailer@cern.ch", "hamza.zafar@cern.ch"]
    self.addressFrom = "ilcdirac-admin@cern.ch"
    self.emailSubject = "JobResetAgent"

    self.accounting = defaultdict(list)
    self.errors = []

    self.nClient = NotificationClient()
    self.reqClient = ReqClient()

  def beginExecution(self):
    """ Reload the configurations before every cycle """
    self.enabled = self.am_getOption('EnableFlag', False)
    self.shifterProxy = self.am_setOption('shifterProxy', 'DataManager')

    self.addressTo = self.am_getOption('MailTo', ["andre.philippe.sailer@cern.ch", "hamza.zafar@cern.ch"])
    self.addressFrom = self.am_getOption('MailFrom', "ilcdirac-admin@cern.ch")

    self.accounting.clear()

    return S_OK()

  def sendNotification(self):
    """ sends email notification about jobs reset"""
    pass

  def logError(self, errStr, varMsg=''):
    self.log.error(errStr, varMsg)
    self.errors.append(errStr + varMsg)

  def execute(self):
    """ main execution loop of Agent """
    pass
