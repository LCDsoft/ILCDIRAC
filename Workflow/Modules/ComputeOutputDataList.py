'''
Compute the outputdata list for production jobs

@since:  Jun 30, 2010

@author: sposs
'''

__RCSID__ = "$Id$"

from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase

from DIRAC import gLogger, S_OK

class ComputeOutputDataList(ModuleBase):
  """ In case the previous module executed properly, add the output to the listoutput. 
  This is used in the prduction context to ensure only the files coming from successful applications
  are added to the output list. Otherwise, there is a risk to register corrupted files.
  """
  def __init__(self):
    """Module initialization.
    """
    super(ComputeOutputDataList, self).__init__()
    self.version = __RCSID__
    self.log = gLogger.getSubLogger( "ComputeOutputData" )
    self.listoutput = []

  def applicationSpecificInputs(self):
    """ Update the workflow_commons dictionary with the current step's output
    """
    if self.step_commons.has_key('listoutput'):
      self.listoutput = self.step_commons['listoutput']

    if self.workflow_commons.has_key('outputList'):
      self.workflow_commons['outputList'] = self.workflow_commons['outputList'] + self.listoutput
    else:
      self.workflow_commons['outputList'] = self.listoutput
    
    return S_OK()
  
  def execute(self):
    """ Not much to do...
    """
    res = self.resolveInputVariables()
    if not res['OK']:
      self.log.error("Failed to resolve input variables:", res['Message'])
      return res
    return S_OK()
