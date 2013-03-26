'''
User Job class. Used to define (guess what?) user jobs!


@author: Stephane Poss
@author: Remi Ete
@author: Ching Bon Lam
'''
from DIRAC import gLogger
class ILDUserJob(object):
  """ User job class. To be used by users, not for production.
  """
  def __init__(self, script = None):
    gLogger.notice("Use UserJob instead")
    
