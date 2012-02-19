#####################################################
# $HeadURL: $
#####################################################
'''
Dummy module that prints out the workflow parameters

Created on Mar 11, 2011

@author: sposs
'''
__RCSID__ = "$Id: $"

from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from DIRAC                                                import S_OK, S_ERROR, gLogger

class DummyModule(ModuleBase):
  """ Dummy module used to check Workflow Parameters (Parametric jobs check)
  """
  def __init__(self):
    ModuleBase.__init__(self)
    self.result = S_ERROR()
    self.log = gLogger.getSubLogger( "DummyModuleChecking" )
    
  def applicationSpecificInputs(self):
    for key,val in self.workflow_commons.items():
      self.log.info("%s=%s" % (key, val))
    return S_OK()

  def execute(self):
    self.result = self.resolveInputVariables()
    return S_OK()  
  