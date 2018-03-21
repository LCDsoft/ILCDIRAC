#!/bin/env python
"""
Create a production to move files from one storage elment to another

Example::

  dirac-ilc-moving-transformation <prodID> <TargetSEs> <SourceSEs> {GEN,SIM,REC,DST} -NExtraName
  dirac-ilc-moving-transformation --AllFor="<prodID1>, <prodID2>, ..." <TargetSEs> <SourceSEs> -NExtraName [-F]

Options:
   -N, --Extraname string      String to append to transformation name in case one already exists with that name
   -A, --AllFor    list        Comma separated list of production IDs. For each prodID three moving productions are
                               created: ProdID/Gen, ProdID+1/SIM, ProdID+2/REC
   -S, --GroupSize <value>     Number of Files per transformation task

:since:  Dec 4, 2015
:author: A. Sailer
"""

from DIRAC.Core.Base import Script
from DIRAC import gLogger, exit as dexit

from ILCDIRAC.ILCTransformationSystem.Utilities.DataParameters import Params

__RCSID__ = "$Id$"


def registerSwitches(clip, script):
  """ register additional switches for moving transformations """
  script.registerSwitch("A:", "AllFor=", "Create usual set of moving transformations for prodID/GEN, prodID+1/SIM"
                        " prodID+2/REC", clip.setAllFor)
  script.registerSwitch("F", "Forcemoving", "Move GEN or SIM files even if they do not have descendents",
                        clip.setForcemoving)


def _createTrafo():
  """reads command line parameters, makes check and creates replication transformation"""
  clip = Params()
  clip.registerSwitches(Script)
  registerSwitches(clip, Script)
  Script.parseCommandLine()
  if not clip.checkSettings(Script)['OK']:
    gLogger.error("ERROR: Missing settings")
    return 1
  from ILCDIRAC.ILCTransformationSystem.Utilities.DataTransformation import createDataTransformation
  for index, prodID in enumerate(clip.prodIDs):
    datatype = clip.datatype if clip.datatype else ['GEN', 'SIM', 'REC'][index % 3]
    resCreate = createDataTransformation(transformationType='Moving',
                                         targetSE=clip.targetSE,
                                         sourceSE=clip.sourceSE,
                                         prodID=prodID,
                                         datatype=datatype,
                                         extraname=clip.extraname,
                                         forceMoving=clip.forcemoving,
                                         groupSize=clip.groupSize,
                                        )
    if not resCreate['OK']:
      return 1

  return 0


if __name__ == '__main__':
  dexit(_createTrafo())
