###########################################################################
# $HeadURL: $
###########################################################################

'''
Created on Jan 31, 2012

@author: Stephane Poss
'''
from DIRAC.Core.Base.DB                                                import DB
from DIRAC.Core.Utilities.List                                         import stringListToString, intListToString, sortList

__RCSID__ = " $Id: $ "

class SoftwareManagementDB(DB):
  '''
  classdocs
  '''


  def __init__(self, maxQueueSize = 10):
    '''
    Constructor
    '''
    self.dbname = 'SoftwareManagementDB'
    DB.__init__( self, self.dbname, 'SoftwareManagement/SoftwareManagementDB', maxQueueSize  )
    