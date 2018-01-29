#!/bin/env python
"""
Execute the FileStatusTransformationAgent on a given Transformation ID

Example: dirac-ilc-filestatus-transformation <transformationID> -x

Options:
   -x, --enable		Perform actions on databases
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC import exit as dexit


class _Params(object):
  """ parameters object """

  def __init__(self):
    self.transID = None
    self.enabled = False

  def setTransID(self, transID):
    self.transID = transID

  def setEnabled(self, opt):
    self.enabled = True
    return S_OK()

  def registerSwitches(self):
    Script.registerSwitch("x", "enable", "perform delete operations on file catalog", self.setEnabled)
    Script.setUsageMessage("""%s <transformationID> -x""" % Script.scriptName)

  def checkSettings(self):
    """ parse arguments """

    args = Script.getPositionalArgs()
    if len(args) < 1:
      return S_ERROR()

    self.setTransID(args[0])

    return S_OK()


def _runFSTAgent():
  """ read commands line params and run FST agent for a given transformation ID """
  params = _Params()
  params.registerSwitches()
  Script.parseCommandLine()
  if not params.checkSettings()['OK']:
    Script.showHelp()
    dexit(1)

  from ILCDIRAC.ILCTransformationSystem.Agent.FileStatusTransformationAgent import FileStatusTransformationAgent
  fstAgent = FileStatusTransformationAgent('ILCTransformation/FileStatusTransformationAgent',
                                           'ILCTransformation/FileStatusTransformationAgent',
                                           'dirac-ilc-filestatus-transformation')
  fstAgent.log = gLogger
  fstAgent.enabled = params.enabled

  res = fstAgent.getTransformations(transID=params.transID)
  if not res['OK']:
    dexit(1)

  if not res['Value']:
    print "Transformation Not Found"
    dexit(1)

  trans = res['Value'][0]

  res = fstAgent.processTransformation(
      int(params.transID), trans['SourceSE'], trans['TargetSE'], trans['DataTransType'])
  if not res["OK"]:
    dexit(1)

  dexit(0)


if __name__ == "__main__":
  _runFSTAgent()
