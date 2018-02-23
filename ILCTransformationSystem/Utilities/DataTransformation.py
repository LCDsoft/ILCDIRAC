"""
Utilities to create Transformations to Move Files
"""

from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

from DIRAC import gLogger, S_OK, S_ERROR


def createMovingTransformation(targetSE, sourceSE, prodID, datatype, extraname='', forceMoving=False,
                               groupSize=1,
                              ):
  """Creates the replication transformation based on the given parameters

  
  :param targetSE: Destination for files
  :type targetSE: python:list or str
  :param str sourceSE: Origin of files. Files will be removed from this SE
  :param int prodID: Production ID of files to be moved
  :param str datatype: DataType of files to be moved
  :param str extraname: addition to the transformation name, only needed if the same transformation was already created
  :param bool forceMoving: Move always, even if GEN/SIM files don't have descendents
  :returns: S_OK, S_ERROR
  """

  metadata = {"Datatype":datatype, "ProdID":prodID}

  if isinstance( targetSE, basestring ):
    targetSE = [ targetSE ]

  trans = Transformation()
  transName = 'Move_%s_%s_%s' % ( datatype, str(prodID), ",".join(targetSE) )
  if extraname:
    transName += "_%s" % extraname

  trans.setTransformationName( transName )
  description = 'Move files for prodID %s to %s' % ( str(prodID), ",".join(targetSE) )
  trans.setDescription( description )
  trans.setLongDescription( description )
  trans.setType( 'Replication' )
  trans.setGroup( 'Moving' )
  trans.setGroupSize(groupSize)
  if datatype in ( 'GEN', 'SIM' ) and not forceMoving:
    trans.setPlugin( 'BroadcastProcessed' )
  else:
    trans.setPlugin( 'Broadcast' )

  transBody = [ ("ReplicateAndRegister", { "SourceSE":sourceSE, "TargetSE":targetSE }),
                ("RemoveReplica", { "TargetSE":sourceSE } ),
              ]

  trans.setBody( transBody )

  res = trans.setSourceSE( sourceSE )
  if not res['OK']:
    return S_ERROR( "SourceSE not valid: %s" % res['Message'] )
  res = trans.setTargetSE( targetSE )
  if not res['OK']:
    return S_ERROR( "TargetSE not valid: %s" % res['Message'] )


  res = trans.addTransformation()
  if not res['OK']:
    gLogger.error("Failed to create Transformation",res['Message'])
    return res
  gLogger.verbose(res)
  trans.setStatus( 'Active' )
  trans.setAgentType( 'Automatic' )
  currtrans = trans.getTransformationID()['Value']
  client = TransformationClient()
  res = client.createTransformationInputDataQuery( currtrans, metadata )
  if res['OK']:
    gLogger.always("Successfully created replication transformation")
    return S_OK()
  else:
    gLogger.error("Failure during replication creation", res['Message'])
    return S_ERROR("Failed to create transformation:%s " % res['Message'])
