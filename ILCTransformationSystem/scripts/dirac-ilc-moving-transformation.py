#!/bin/env python
"""
Create a production to move files from one storage elment to another

Example::

  dirac-ilc-moving-transformation <prodID> <TargetSEs> <SourceSEs> {GEN,SIM,REC,DST} -NExtraName

Options:
   -N, --Extraname string      String to append to transformation name in case one already exists with that name

:since:  Dec 4, 2015
:author: A. Sailer
"""

from DIRAC.Core.Base import Script
from DIRAC import gLogger, exit as dexit

from ILCDIRAC.ILCTransformationSystem.Utilities.MovingParameters import Params

__RCSID__ = "$Id$"

def _createTrafo():
  """reads command line parameters, makes check and creates replication transformation"""
  clip = Params()
  clip.registerSwitches( Script )
  Script.parseCommandLine()
  if not clip.checkSettings( Script )['OK']:
    gLogger.error("ERROR: Missing settings")
    return 1
  from ILCDIRAC.ILCTransformationSystem.Utilities.MovingTransformation import createMovingTransformation
  resCreate = createMovingTransformation( clip.targetSE,
                                          clip.sourceSE,
                                          clip.prodID,
                                          clip.datatype,
                                          clip.extraname,
                                          clip.forcemoving )
  if not resCreate['OK']:
    return 1
  return 0

if __name__ == '__main__':
  dexit(_createTrafo())
