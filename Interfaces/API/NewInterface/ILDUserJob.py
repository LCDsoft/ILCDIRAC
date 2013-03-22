'''
User Job class. Used to define (guess what?) user jobs!


@author: Stephane Poss
@author: Remi Ete
@author: Ching Bon Lam
'''

from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC import S_OK


class ILDUserJob(UserJob):
  """ User job class. To be used by users, not for production.
  """
  def __init__(self, script = None):
    super(ILDUserJob, self).__init__( script )
    self.type = 'User'
    self.diracinstance = None
    
  def setILDConfig(self,Version):
    """ Define the Configuration package to obtain
    """
    appName = 'ildconfig'
    self._addSoftware(appName, Version)
    
    self._addParameter( self.workflow, 'ILDConfigPackage', 'JDL', appName+Version, 'ILDConfig package' )
    return S_OK()