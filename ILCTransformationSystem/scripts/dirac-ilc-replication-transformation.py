#!/bin/env python
"""
Create a production to replicate files from one storage elment to another

Example::

  dirac-ilc-replication-transformation <prodID> <TargetSEs> <SourceSEs> {GEN,SIM,REC,DST} -NExtraName

Options:
   -N, --Extraname string      String to append to transformation name in case one already exists with that name
   -S, --GroupSize <value>     Number of Files per transformation task

:since:  May 18, 2015
:author: A. Sailer
"""
from DIRAC.Core.Base import Script
from DIRAC import gLogger, exit as dexit

from ILCDIRAC.ILCTransformationSystem.Utilities.DataParameters import Params

__RCSID__ = "$Id$"


def registerSwitches(script):
  """ register additional switches for replication transformation """
  script.setUsageMessage("""%s <prodID> <TargetSEs> <SourceSEs> {GEN,SIM,REC,DST} -NExtraName""" % script.scriptName)


def _createTrafo():
  """reads command line parameters, makes check and creates replication transformation"""
  clip = Params()
  clip.registerSwitches(Script)
  registerSwitches(Script)
  Script.parseCommandLine()
  if not clip.checkSettings(Script)['OK']:
    gLogger.error("ERROR: Missing settings")
    return 1
  from ILCDIRAC.ILCTransformationSystem.Utilities.DataTransformation import createDataTransformation
  for prodID in clip.prodIDs:
    resCreate = createDataTransformation('Replication', clip.targetSE, clip.sourceSE, prodID,
                                         clip.datatype, clip.extraname, clip.groupSize,
                                        )
    if not resCreate['OK']:
      return 1

  return 0


if __name__ == '__main__':
  dexit(_createTrafo())
