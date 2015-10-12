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
    self.numberOfEvents = 0
    #Energy to use (duh! again)
    self.energy = 0
    self._importLocation = "ILCDIRAC.Workflow.Modules"
    
  def setNbEvts(self, numberOfEvents):
    """ Set the number of events to process
    
    @param numberOfEvents: Number of events to process (or generate)
    @type numberOfEvents: int
    """
    self._checkArgs({ 'numberOfEvents' : IntType })
    self.numberOfEvents = numberOfEvents
    return S_OK()  

  def setNumberOfEvents(self, numberOfEvents):
    """ Set the number of events to process, alias to setNbEvts
    """
    return self.setNbEvts(numberOfEvents)
    

  def setEnergy(self, energy):
    """ Set the energy to use
    
    @param energy: Energy used in GeV
    @type energy: float
    """
    if not type(energy) == type(1.1):
      energy = float(energy)
    self._checkArgs({ 'energy' : FloatType })
    self.energy = energy
    return S_OK()  

