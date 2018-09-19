#!/bin/env python
"""
Create a production to replicate files from one storage element to another

Example::

  dirac-ilc-replication-transformation <prodID> <TargetSEs> <SourceSEs> {GEN,SIM,REC,DST} -NExtraName

Options:
   -N, --Extraname string      String to append to transformation name in case one already exists with that name
   -R, --GroupName <value>     TransformationGroup Name, by itself the group of the prodID
   -S, --GroupSize <value>     Number of Files per transformation task
   -x, --Enable                Enable the transformation creation, otherwise dry-run

:since:  May 18, 2015
:author: A. Sailer
"""
from pprint import pformat

from DIRAC.Core.Base import Script
from DIRAC import gLogger, exit as dexit
from DIRAC.TransformationSystem.Utilities.ReplicationTransformation import createDataTransformation

from ILCDIRAC.ILCTransformationSystem.Utilities.DataParameters import Params, getTransformationGroup

__RCSID__ = "$Id$"

LOG = gLogger.getSubLogger("ReplTrans")

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
    LOG.error("ERROR: Missing settings")
    return 1
  for prodID in clip.metaValues:
    tGroup = getTransformationGroup(prodID, clip.groupName)
    parDict = dict(flavour='Replication',
                   targetSE=clip.targetSE,
                   sourceSE=clip.sourceSE,
                   metaKey=clip.metaKey,
                   metaValue=prodID,
                   extraData={'Datatype': clip.datatype},
                   extraname=clip.extraname,
                   plugin=clip.plugin,
                   groupSize=clip.groupSize,
                   tGroup=tGroup,
                   enable=clip.enable,
                   )
    LOG.debug("Parameters: %s" % pformat(parDict))
    resCreate = createDataTransformation(**parDict)
    if not resCreate['OK']:
      return 1

  return 0


if __name__ == '__main__':
  dexit(_createTrafo())
