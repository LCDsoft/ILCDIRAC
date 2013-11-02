#####################################################################################
# $HeadURL$
######################################################################################
'''
Created on Nov 2, 2013

@author: sposs
'''

from DIRAC.Core.Base.AgentModule                               import AgentModule


__RCSID__ = "$Id$"

class TarTheProdLogs( AgentModule ):
  '''
  Tar the prod logs, and send them to CASTOR
  '''


  def __init__( self ):
    '''
    Constructor
    '''
        