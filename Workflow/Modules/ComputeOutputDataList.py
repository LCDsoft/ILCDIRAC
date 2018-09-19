'''
Compute the outputdata list for production jobs

:since:  Jun 30, 2010

:author: sposs
'''

from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase

from DIRAC import gLogger, S_OK

__RCSID__ = '$Id$'
LOG = gLogger.getSubLogger(__name__)

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
    self.listoutput = []

  def applicationSpecificInputs(self):
    """ Update the workflow_commons dictionary with the current step's output
    """
    if 'listoutput' in self.step_commons:
      self.listoutput = self.step_commons['listoutput']

    if 'outputList' in self.workflow_commons:
      self.workflow_commons['outputList'] = self.workflow_commons['outputList'] + self.listoutput
    else:
      self.workflow_commons['outputList'] = self.listoutput
    
    return S_OK()
  
  def execute(self):
    """ Not much to do...
    """
    res = self.resolveInputVariables()
    if not res['OK']:
      LOG.error("Failed to resolve input variables:", res['Message'])
      return res
    return S_OK()
