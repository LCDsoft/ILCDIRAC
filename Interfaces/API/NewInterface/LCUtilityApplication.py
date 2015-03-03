'''
Created on Nov 7, 2013

@author: sposs
'''

__RCSID__ = "$Id$"

from ILCDIRAC.Interfaces.API.NewInterface.Application import Application
from DIRAC import S_OK
from types import FloatType, IntType

class LCUtilityApplication( Application ):
  '''
  Utility applications
  '''

  def __init__(self, paramdict = None):
    super(LCUtilityApplication, self).__init__(paramdict)
    #Number of events to process
    self.NbEvts = 0
    #Energy to use (duh! again)
    self.Energy = 0
    self._importLocation = "ILCDIRAC.Workflow.Modules"
    
  def setNbEvts(self, nbevts):
    """ Set the number of events to process
    
    @param nbevts: Number of events to process (or generate)
    @type nbevts: int
    """
    self._checkArgs({ 'nbevts' : IntType })
    self.NbEvts = nbevts  
    return S_OK()  

  def setNumberOfEvents(self, nbevts):
    """ Set the number of events to process, alias to setNbEvts
    """
    return self.setNbEvts(nbevts)
    

  def setEnergy(self, Energy):
    """ Set the energy to use
    
    @param Energy: Energy used in GeV
    @type Energy: float
    """
    if not type(Energy) == type(1.1):
      Energy = float(Energy)
    self._checkArgs({ 'Energy' : FloatType })
    self.Energy = Energy
    return S_OK()  

