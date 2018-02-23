#!/bin/env python
"""
Create a production to move files from one storage elment to another

Example::

  dirac-ilc-moving-transformation <prodID> <TargetSEs> <SourceSEs> {GEN,SIM,REC,DST} -NExtraName
  dirac-ilc-moving-transformation --AllFor="<prodID1>, <prodID2>, ..." <TargetSEs> <SourceSEs> -NExtraName [-F]

Options:
   -N, --Extraname string      String to append to transformation name in case one already exists with that name
   -A, --AllFor    list        Comma separated list of production IDs. For each prodID three moving productions are created: ProdID/Gen, ProdID+1/SIM, ProdID+2/REC

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
  for index, prodID in enumerate( clip.prodIDs ):
    datatype = clip.datatype if clip.datatype else ['GEN', 'SIM', 'REC'][ index % 3 ]
    retData = clip.checkDatatype( prodID, datatype )
    if not retData['OK']:
      gLogger.error( "Failed to check datatype", retData['Message'] )
      return 1

    resCreate = createMovingTransformation(clip.targetSE,
                                           clip.sourceSE,
                                           prodID,
                                           datatype,
                                           clip.extraname,
                                           clip.forcemoving,
                                           clip.groupSize,
                                          )
    if not resCreate['OK']:
      return 1

  return 0

if __name__ == '__main__':
  dexit(_createTrafo())
