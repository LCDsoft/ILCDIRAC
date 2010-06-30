'''
Created on Jun 30, 2010

@author: sposs
'''
__RCSID__ = "$Id: ComputeOutputDataList sposs"
from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase

from DIRAC import gLogger, S_OK

class ComputeOutputDataList(ModuleBase):
  def __init__(self):
    """Module initialization.
    """
    ModuleBase.__init__(self)
    self.version = __RCSID__
    self.log = gLogger.getSubLogger( "ComputeOutputData" )
    self.listoutput = []
    self.jobID = ''

  def resolveInputParameters(self):
    if self.step_commons.has_key('listoutput'):
      self.listoutput = self.step_commons['listoutput']

    if self.workflow_commons.has_key('outputList'):
      self.workflow_commons['outputList'] = self.workflow_commons['outputList'] + self.listoutput
    else:
      self.workflow_commons['outputList'] = self.listoutput
    
    return S_OK()
  
  def execute(self):
    res = self.resolveInputParameters()
    if not res['OK']:
      return res
    return S_OK()