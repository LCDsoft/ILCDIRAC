"""
Reports any applications errors at the end of the Workflow execution.

Depends on the 'ErrorDict' of the workflow_commons

:since: June 11, 2018
:author: A. Sailer
"""

from ILCDIRAC.Workflow.Modules.ModuleBase import ModuleBase
from DIRAC import S_OK, gLogger

__RCSID__ = '$Id$'
LOG = gLogger.getSubLogger(__name__)


class ReportErrors(ModuleBase):
  """Reports errors from applications at the end of the workflow execution."""

  def __init__(self):
    """Constructor, no arguments."""
    super(ReportErrors, self).__init__()
    self.result = S_OK()

  def execute(self):
    """Print out the errors from all applications.

    ErrorDict is filled in :func:`ModuleBase.finalStatusReport`, which is called from all modules.
    """
    errorDict = self.workflow_commons.get('ErrorDict', {})
    if not errorDict:
      LOG.info("No errors encountered")

    for app, errorMessages in errorDict.iteritems():
      for message in errorMessages:
        LOG.error(app, message)
    return S_OK()
