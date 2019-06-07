#!/bin/env python
"""
Create a transformation to move files from one storage element to another

Example::

  dirac-ilc-moving-transformation <prodID> <TargetSEs> <SourceSEs> {GEN,SIM,REC,DST} -NExtraName
  dirac-ilc-moving-transformation --AllFor="<prodID1>, <prodID2>, ..." <TargetSEs> <SourceSEs> -NExtraName [-F]

Options:
   -A, --AllFor    list        Comma separated list of production IDs. For each prodID three moving productions are
                               created: ProdID/Gen, ProdID+1/SIM, ProdID+2/REC
   -F, --Forcemoving           Move GEN or SIM files even if they do not have descendents
   -N, --Extraname string      String to append to transformation name in case one already exists with that name
   -R, --GroupName <value>     TransformationGroup Name, by itself the group of the prodID
   -G, --GroupSize <value>     Number of Files per transformation task
   -x, --Enable                Enable the transformation creation, otherwise dry-run

:since:  Dec 4, 2015
:author: A. Sailer
"""
from pprint import pformat

from DIRAC.Core.Base import Script
from DIRAC import gLogger as LOG, exit as dexit
from DIRAC.TransformationSystem.Utilities.ReplicationTransformation import createDataTransformation

from ILCDIRAC.ILCTransformationSystem.Utilities.DataParameters import Params, checkDatatype, \
    getTransformationGroup

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
  clip.flavour = 'Moving'
  clip.plugin = 'BroadcastProcessed'
  clip.groupSize = 10
  clip.registerSwitches(Script)
  registerSwitches(clip, Script)
  Script.parseCommandLine()
  if not clip.checkSettings(Script)['OK']:
    LOG.error("ERROR: Missing settings")
    return 1
  for index, prodID in enumerate(clip.metaValues):
    datatype = clip.datatype if clip.datatype else ['GEN', 'SIM', 'REC'][index % 3]
    if clip.forcemoving:
      LOG.notice('Forced moving: setting plugin to "Broadcast"')
      clip.plugin = 'Broadcast'
    retData = checkDatatype(prodID, datatype)
    if not retData['OK']:
      LOG.error("ERROR: %s" % retData['Message'])
      return 1
    tGroup = getTransformationGroup(prodID, clip.groupName)
    parDict = dict(flavour=clip.flavour,
                   targetSE=clip.targetSE,
                   sourceSE=clip.sourceSE,
                   metaKey=clip.metaKey,
                   metaValue=prodID,
                   extraData={'Datatype': datatype},
                   extraname=clip.extraname,
                   plugin=clip.plugin,
                   groupSize=clip.groupSize,
                   tGroup=tGroup,
                   enable=clip.enable,
                   )
    LOG.notice('Parameters: %s' % pformat(parDict))
    resCreate = createDataTransformation(**parDict)
    if not resCreate['OK']:
      LOG.error('Failed to create the transformation', resCreate['Message'])
      return 1

  return 0


if __name__ == '__main__':
  dexit(_createTrafo())
