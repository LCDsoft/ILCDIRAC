#!/bin/env python
"""
Create a production to replicate files from one storage element to another

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
from DIRAC.TransformationSystem.Utilities.ReplicationTransformation import createDataTransformation

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
  for prodID in clip.metaValues:
    resCreate = createDataTransformation(flavour='Replication',
                                         targetSE=clip.targetSE,
                                         sourceSE=clip.sourceSE,
                                         metaKey=clip.metaKey,
                                         metaValue=prodID,
                                         extraData={'Datatype': clip.datatype},
                                         extraname=clip.extraname,
                                         plugin=clip.plugin,
                                         groupSize=clip.groupSize,
                                         tGroup=clip.groupName,
                                         enable=clip.enable,
                                        )
    if not resCreate['OK']:
      return 1

  return 0


if __name__ == '__main__':
  dexit(_createTrafo())
