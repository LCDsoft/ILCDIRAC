#!/bin/env python
"""
Add a number of tasks to a production, can only be used on prods that use the Limited Plugin
@since: Mar 26, 2013
@author: S Poss
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC import S_OK, S_ERROR

class Params(object):
  def __init__(self):
    self.prod = 0
    self.tasks = 0
    
  def setProd(self,opt):
    try:
      self.prod = int(opt)
    except ValueError:
      return S_ERROR("Production ID must be integer")
    return S_OK()

  def setNbTasks(self,opt):
    try:
      self.tasks = int(opt)
    except ValueError:
      return S_ERROR("Number of tasks must be integer")
    return S_OK()

  def registerSwitches(self):
    Script.registerSwitch('p:', "ProductionID=", "Production ID to extend", self.setProd)
    Script.registerSwitch("t:", "Tasks=", "Number of tasks to add (-1 for all)", self.setNbTasks)
    Script.setUsageMessage("%s -p 2145 -t 200" % Script.scriptName)
    
def extend():
  """Extends all the tasks"""
  clip = Params()
  clip.registerSwitches()
  Script.parseCommandLine()
  
  from DIRAC import gLogger, exit as dexit
  
  if not clip.prod or not clip.tasks:
    gLogger.error("Production ID is 0 or Tasks is 0, cannot be")
    dexit(1)
    
  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
  tc = TransformationClient()
  res = tc.getTransformation(clip.prod)
  trans= res['Value']
  transp = trans['Plugin']
  if transp != 'Limited':
    gLogger.error("This cannot be used on productions that are not using the 'Limited' plugin")
    dexit(0)
  
  gLogger.info("Prod %s has %s tasks registered" % (clip.prod, trans['MaxNumberOfTasks']) )
  if clip.tasks >0:
    max_tasks = trans['MaxNumberOfTasks'] + clip.tasks  
    groupsize = trans['GroupSize']
    gLogger.notice("Adding %s tasks (%s file(s)) to production %s" %(clip.tasks, clip.tasks*groupsize, clip.prod))
  elif clip.tasks <0:
    max_tasks = -1
    gLogger.notice("Now all existing files in the DB for production %s will be processed." % clip.prod)
  else:
    gLogger.error("Number of tasks must be different from 0")
    dexit(1)
  res = tc.setTransformationParameter(clip.prod, 'MaxNumberOfTasks', max_tasks)
  if not res['OK']:
    gLogger.error(res['Message'])
    dexit(1)
  gLogger.notice("Production %s extended!" % clip.prod)
    
  dexit(0)

if __name__=='__main__':
  extend()
