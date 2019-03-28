"""Holding helper objects or functions to create transformations."""
from pprint import pformat

from DIRAC import gLogger

LOG = gLogger.getSubLogger(__name__)

class Task(object):
  """Object holding all information for a task."""

  def __init__(self, metaInput, parameterDict, eventsPerJob,
               metaPrev=None, nbTasks=None, sinFile=None, eventsPerBaseFile=None,
               applicationOptions=None,
               ):
    """Initialise task with all the information we need to create a transformation."""
    LOG.notice('Creating task with meta', str(metaInput))
    self.meta = dict(metaInput)
    self.parameterDict = dict(parameterDict)
    self.eventsPerJob = eventsPerJob
    self.metaPrev = dict(metaPrev) if metaPrev else {}
    self.nbTasks = nbTasks
    self.sinFile = sinFile
    self.eventsPerBaseFile = eventsPerBaseFile
    self.applicationOptions = dict(applicationOptions) if applicationOptions is not None else {}
    self.cliReco = ''
    self.taskName = ''
    self.sourceName = ''
    self._updateMeta(self.meta)

  def __str__(self):
    """Return string representation of Task."""
    return pformat(vars(self))

  def __repr__(self):
    """Return string representation of Task."""
    return pformat(vars(self), width=150, indent=10)

  def _updateMeta(self, metaDict):
    """Ensure the meta dict contains the correct NumberOfEvents."""
    if self.eventsPerJob is not None:
      metaDict['NumberOfEvents'] = self.eventsPerJob
    if self.eventsPerBaseFile:
      metaDict['NumberOfEvents'] = self.eventsPerBaseFile

  def getProdName(self, *args):
    """Create the production name."""
    workflowName = '_'.join([self.parameterDict['process'],
                             self.meta['Energy'],
                             '_'.join(args),
                             self.sourceName,
                             self.taskName,
                             ]).strip('_')
    while '__' in workflowName:
      workflowName = workflowName.replace('__', '_')
    return workflowName
