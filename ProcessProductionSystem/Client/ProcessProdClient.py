####################################################################
# $HeadURL: $
####################################################################
'''
Created on Apr 19, 2011

@author: Stephane Poss
'''
__RCSID__ = "$Id: $"

from DIRAC.Core.Base.Client                         import Client
#from DIRAC                                          import S_OK, S_ERROR

class ProcessProdClient(Client):
  def __init__(self, name = 'ProcessProdClient'):
    self.setServer('ProcessProduction/ProcessManager')
