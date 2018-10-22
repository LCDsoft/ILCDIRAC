"""Holding helper objects or functions to create transformations."""
from pprint import pformat


class Task(object):
  """Object holding all information for a task."""

  def __init__(self, metaInput, parameterDict, eventsPerJob,
               metaPrev=None, nbTasks=None, sinFile=None, eventsPerBaseFile=None,
               applicationOptions=None,
               dryRun=True):
    """Initialise task with all the information we need to create a transformation."""
    self.meta = dict(metaInput)
    self.parameterDict = dict(parameterDict)
    self.eventsPerJob = eventsPerJob
    self.metaPrev = dict(metaPrev) if metaPrev else {}
    self.nbTasks = nbTasks
    self.sinFile = sinFile
    self.eventsPerBaseFile = eventsPerBaseFile
    self.dryRun = dryRun
    self.applicationOptions = dict(applicationOptions) if applicationOptions is not None else {}
    self.cliReco = ''
    self.taskName = ''
    self.sourceName = ''

    if self.metaPrev:
      self._updateMeta(self.meta, self.metaPrev, self.eventsPerJob)

  def __str__(self):
    """Return string representation of Task."""
    return pformat(vars(self))

  def __repr__(self):
    """Return string representation of Task."""
    return pformat(vars(self), width=150, indent=10)

  def _updateMeta(self, outputDict, inputDict, eventsPerJob):
    """Add some values from the inputDict to the outputDict.

    Fake the input dataquery result in dryRun mode.
    """
    if not self.dryRun:
      outputDict.clear()
      outputDict.update(inputDict)
      return

    for key, value in inputDict.iteritems():
      if key not in outputDict:
        outputDict[key] = value
    outputDict['NumberOfEvents'] = eventsPerJob

  def getProdName(self, *args):
    """Create the production name."""
    workflowName = '_'.join([self.parameterDict['process'],
                             self.meta['Energy'],
                             '_'.join(args),
                             self.sourceName,
                             self.taskName,
                             ]).strip('_').replace('__', '_')
    return workflowName
