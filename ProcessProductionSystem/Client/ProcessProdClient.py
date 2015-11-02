'''
Created on Apr 19, 2011

:author: Stephane Poss
'''
__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client                         import Client
#from DIRAC                                          import S_OK, S_ERROR

class ProcessProdClient(Client):
  """ Client of the ProcessProdHandler
  """
  def __init__(self, **kwargs ):
    Client.__init__(self, **kwargs )
    self.setServer('ProcessProduction/ProcessManager')
