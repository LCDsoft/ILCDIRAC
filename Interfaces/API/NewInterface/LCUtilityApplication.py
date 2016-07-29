'''
:author: sposs
:since: Nov 7, 2013
'''

from types import FloatType, IntType

from DIRAC import S_OK

from ILCDIRAC.Interfaces.API.NewInterface.Application import Application

__RCSID__ = "$Id$"

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

  def setNumberOfEvents(self, numberOfEvents):
    """ Set the number of events to process, alias to :func:`setNbEvts`
    """
    return self.setNbEvts(numberOfEvents)
    
  def setEnergy(self, energy):
    """ Set the energy to use
    
    :param float energy: Energy used in GeV
    """
    if not isinstance( energy, float ):
      energy = float(energy)
    self._checkArgs({ 'energy' : FloatType })
    self.energy = energy
    return S_OK()  


#### DEPRECATED ################################################################

  def setNbEvts(self, numberOfEvents):
    """ Set the number of events to process

    .. deprecated:: v23r0p0
       use :func:`setNumberOfEvents`

    :param int numberOfEvents: Number of events to process (or generate)
    """
    self._checkArgs({ 'numberOfEvents' : IntType })
    self.numberOfEvents = numberOfEvents
    return S_OK()
