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
from DIRAC import gLogger, exit as dexit, S_ERROR, S_OK

from ILCDIRAC.ILCTransformationSystem.Utilities.MovingParameters import Params

__RCSID__ = "$Id$"


def checkDatatype( prodID, datatype ):
  """ check if the datatype makes sense for given production """
  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
  tClient = TransformationClient()
  cond = dict( TransformationID=prodID )
  trafo = tClient.getTransformations( cond )
  if not trafo['OK']:
    return trafo
  if len(trafo['Value']) != 1:
    return S_ERROR( "Did not get unique production for this prodID" )

  trafoType = trafo['Value'][0]['Type'].split("_")[0]

  dataTypes = { 'MCGeneration': ['GEN'],
                'MCSimulation': ['SIM'],
                'MCReconstruction': ['REC', 'DST'],
              }.get( trafoType, [] )

  if datatype not in dataTypes:
    return S_ERROR( "Datatype %s doesn't fit production type %s" %( datatype, trafoType ) )

  return S_OK()


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
    retData = checkDatatype( prodID, datatype )
    if not retData['OK']:
      gLogger.error( "Failed to check datatype", retData['Message'] )
      return 1

    resCreate = createMovingTransformation( clip.targetSE,
                                            clip.sourceSE,
                                            prodID,
                                            datatype,
                                            clip.extraname,
                                            clip.forcemoving )
    if not resCreate['OK']:
      return 1

  return 0

if __name__ == '__main__':
  dexit(_createTrafo())
