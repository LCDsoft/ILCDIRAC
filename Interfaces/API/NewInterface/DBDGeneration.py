'''
Created on Jan 26, 2012

@author: Stephane Poss
'''

from ILCDIRAC.Interfaces.API.NewInterface.ProductionJob import ProductionJob
from DIRAC                                                  import S_OK, S_ERROR, gConfig
from decimal import Decimal

class DBDGeneration(ProductionJob):
  def __init__(self, script = None):
    ProductionJob.__init__(self, script)
    
  def _jobSpecificParams(self,application):  
    if self.created:
      return S_ERROR("The production was created, you cannot add new applications to the job.")
    
    if not application.logfile:
      logf = "SomeLog.log"
      res = application.setLogFile(logf)
      if not res['OK']:
        return res
    if not self.nbevts:
      self.nbevts = application.nbevts
      if not self.nbevts:
        return S_ERROR("Number of events to process is not defined.")
    if not self.energy:
      if application.energy:
        self.energy = Decimal(str(application.energy))
      else:
        return S_ERROR("Could not find the energy defined, it is needed for the production definition.")    
    if self.energy:
      self._setParameter( "Energy", "float", float(self.energy), "Energy used")      
      self.prodparameters["Energy"] = float(self.energy)
      
    if not self.evttype:
      if hasattr(application,'evttype'):
        self.evttype = application.evttype
      else:
        return S_ERROR("Event type not found nor specified, it's mandatory for the production paths.")  
      self.prodparameters['Process'] = self.evttype

    if not self.outputStorage:
      return S_ERROR("You need to specify the Output storage element")
        
    if self.prodparameters["SWPackages"]:
      self.prodparameters["SWPackages"] +=";%s.%s"%(application.appname,application.version)
    else :
      self.prodparameters["SWPackages"] ="%s.%s"%(application.appname,application.version)
             
    return S_OK()