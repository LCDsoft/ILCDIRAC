"""
Add a number of tasks to a production, can only be used on productions that use the 'Limited' or 'Standard' Plugin

Options:

   -p, --ProductionID <value>   Production ID to extend. Deprecated: use positional args
   -t, --Tasks <value>          Number of tasks to add (-1 for all) Deprecated: use positional args
     , --Total                  Set MaxNumberOfTasks to the value of TasksToAdd

Usage:

  dirac-ilc-add-tasks-to-prod ProdID TasksToAdd [--Total]


:since: Mar 26, 2013
:author: S Poss
"""

import textwrap

from DIRAC.Core.Base import Script
from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger, exit as dexit


__RCSID__ = '$Id$'


class _Skip(Exception):
  """Exception to skip further execution of script."""

  pass


class _Params(object):
  def __init__(self):
    self.prod = 0
    self.tasks = 0
    self.total = False

  def setProd(self, opt):
    try:
      self.prod = int(opt)
    except ValueError:
      return S_ERROR('Production ID must be integer')
    return S_OK()

  def setNbTasks(self, opt):
    try:
      self.tasks = int(opt)
    except ValueError:
      return S_ERROR('Number of tasks must be integer')
    return S_OK()

  def setTotal(self, _opt):
    self.total = True
    return S_OK()

  def registerSwitches(self):
    Script.registerSwitch('p:', 'ProductionID=', 'Production ID to extend', self.setProd)
    Script.registerSwitch('t:', 'Tasks=', 'Number of tasks to add (-1 for all)', self.setNbTasks)
    Script.registerSwitch('', 'Total', 'Set MaxNumberOfTasks to the value of TasksToAdd', self.setTotal)
    Script.setUsageMessage(textwrap.dedent("""
                           Usage:

                             %s 2145 200 [--Total]

                           Deprecated Usage:

                             %s -p 2145 -t 200 [--Total]

                           """ % (Script.scriptName, Script.scriptName)))

  def getMaxTasks(self, maxTasks):
    """Return the new max number of tasks."""
    if self.total:
      if self.tasks > maxTasks:
        return self.tasks
      gLogger.notice('MaxNumberOfTasks is already reached for production %s' % self.prod)
      raise _Skip()
    # if not total, add number of tasks to current maxTasks
    gLogger.notice('Adding %s tasks to production %s' % (self.tasks, self.prod))
    return maxTasks + self.tasks


def _getTransformationClient():
  """Return instance of TransformationClient."""
  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
  return TransformationClient()


def _extendLimited(clip, tc, trans):
  """Extend transformations with 'Limited' plugin."""
  if clip.tasks > 0:
    max_tasks = clip.getMaxTasks(trans['MaxNumberOfTasks'])
    groupsize = trans['GroupSize']
    gLogger.notice('Adding %s tasks (%s file(s)) to production %s' % (clip.tasks, clip.tasks * groupsize, clip.prod))
  elif clip.tasks < 0:
    max_tasks = -1
    gLogger.notice('Now all existing files in the DB for production %s will be processed.' % clip.prod)
  else:
    gLogger.error('Number of tasks must be different from 0')
    return 1

  res = tc.setTransformationParameter(clip.prod, 'MaxNumberOfTasks', max_tasks)
  if not res['OK']:
    gLogger.error('Failed to set MaxNumberOfTasks', res['Message'])
    return 1
  gLogger.notice('Production %s extended!' % clip.prod)
  return 0


def _extendStandard(clip, tc, trans):
  """Extend transformations with 'Standard' Plugin."""
  if clip.tasks > 0:
    max_tasks = clip.getMaxTasks(trans['MaxNumberOfTasks'])
  else:
    gLogger.error('Number of tasks must be larger than 0')
    return 1

  res = tc.setTransformationParameter(clip.prod, 'MaxNumberOfTasks', max_tasks)
  if not res['OK']:
    gLogger.error('Failed to set MaxNumberOfTasks', res['Message'])
    return 1

  gLogger.notice('Production %s extended!' % clip.prod)
  return 0


def _extend(clip, posArgs):
  """Extend the production for given number of tasks."""

  if posArgs and (clip.prod or clip.tasks):
    gLogger.error('Do not mix positional and keyword arguments')
    return 1

  if len(posArgs) == 2:
    clip.setProd(posArgs[0])
    clip.setNbTasks(posArgs[1])

  if not (clip.prod or clip.tasks):
    gLogger.error('Production ID is 0 or Tasks is 0, cannot be')
    return 1

  tc = _getTransformationClient()
  res = tc.getTransformation(clip.prod)
  trans = res['Value']
  transp = trans['Plugin']
  gLogger.info('Production "%s" has "%s" tasks registered' % (clip.prod, trans['MaxNumberOfTasks']))

  if not posArgs:
    gLogger.notice('You can also just use positional arguments now!')

  if transp == 'Limited':
    return _extendLimited(clip, tc, trans)

  if transp == 'Standard':
    return _extendStandard(clip, tc, trans)

  gLogger.error("This cannot be used on productions that are not using the 'Limited' or 'Standard' plugin")
  return 1


if __name__ == '__main__':
  CLIP = _Params()
  CLIP.registerSwitches()
  Script.parseCommandLine()
  POS_ARGS = Script.getPositionalArgs()
  try:
    dexit(_extend(CLIP, POS_ARGS))
  except _Skip:
    gLogger.verbose('Skipping extension')
    dexit(1)
