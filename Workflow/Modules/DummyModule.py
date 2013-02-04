#####################################################
# $HeadURL: $
#####################################################
'''
Dummy module that prints out the workflow parameters

@since: Mar 11, 2011

@author: sposs
'''
__RCSID__ = "$Id: $"

from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from DIRAC                                                import S_OK, S_ERROR, gLogger
from ILCDIRAC.Core.Utilities.InputFilesUtilities          import getNumberOfevents

class DummyModule(ModuleBase):
  """ Dummy module used to check Workflow Parameters (Parametric jobs check)
  """
  def __init__(self):
    super(DummyModule, self).__init__()
    self.result = S_ERROR()
    self.log = gLogger.getSubLogger( "DummyModuleChecking" )
    
  def applicationSpecificInputs(self):
    """ Resolve the parameters
    """
    if self.InputData:
      if not self.workflow_commons.has_key("Luminosity") or not self.workflow_commons.has_key("NbOfEvents"):
        res = getNumberOfevents(self.InputData)
        if res.has_key("nbevts") and not self.workflow_commons.has_key("Luminosity") :
          self.workflow_commons["NbOfEvents"] = res["nbevts"]
        if res.has_key("lumi") and not self.workflow_commons.has_key("NbOfEvents"):
          self.workflow_commons["Luminosity"] = res["lumi"]
    for key, val in self.workflow_commons.items():
      self.log.info("%s=%s" % (key, val))
      
    return S_OK()

  def execute(self):
    """ Not much to do...
    """
    self.result = self.resolveInputVariables()
    return S_OK()  
  