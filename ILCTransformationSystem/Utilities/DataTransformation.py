"""
Utilities to create Transformations to Move Files
"""

from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

from DIRAC import gLogger, S_OK, S_ERROR

from ILCDIRAC.ILCTransformationSystem.Utilities.DataParameters import checkDatatype


def createDataTransformation(transformationType, targetSE, sourceSE, prodID, datatype,
                             extraname='', forceMoving=False,
                             groupSize=1,
                            ):
  """Creates the replication transformation based on the given parameters


  :param str transformationType: Replication or Moving transformation
  :param targetSE: Destination for files
  :type targetSE: python:list or str
  :param str sourceSE: Origin of files. Files will be removed from this SE
  :param int prodID: Production ID of files to be moved
  :param str datatype: DataType of files to be moved
  :param str extraname: addition to the transformation name, only needed if the same transformation was already created
  :param bool forceMoving: Move always, even if GEN/SIM files don't have descendents
  :returns: S_OK, S_ERROR
  """

  retData = checkDatatype(prodID, datatype)
  if not retData['OK']:
    gLogger.error("Failed to check datatype", retData['Message'])
    return retData

  metadata = {"Datatype": datatype, "ProdID": prodID}

  if isinstance(targetSE, basestring):
    targetSE = [targetSE]

  if transformationType not in ('Replication', 'Moving'):
    return S_ERROR('Unsupported transformationType %s' % transformationType)

  transType = {'Replication': 'Replicate', 'Moving': 'Move'}[transformationType]
  transGroup = {'Replication': 'Replication', 'Moving': 'Moving'}[transformationType]

  trans = Transformation()
  transName = '%s_%s_%s_%s' % (transType, datatype, str(prodID), ",".join(targetSE))
  if extraname:
    transName += "_%s" % extraname

  trans.setTransformationName(transName)
  description = '%s files for prodID %s to %s' % (transType, str(prodID), ",".join(targetSE))
  trans.setDescription(description)
  trans.setLongDescription(description)
  trans.setType('Replication')
  trans.setGroup(transGroup)
  trans.setGroupSize(groupSize)
  if transformationType == "Replication":
    trans.setPlugin('Broadcast')
  elif datatype in ('GEN', 'SIM') and not forceMoving:
    trans.setPlugin('BroadcastProcessed')
  else:
    trans.setPlugin('Broadcast')

  transBody = {'Moving': [("ReplicateAndRegister", {"SourceSE": sourceSE, "TargetSE": targetSE}),
                          ("RemoveReplica", {"TargetSE": sourceSE})],
               'Replication': ''  # empty body
              }[transformationType]

  trans.setBody(transBody)

  res = trans.setSourceSE(sourceSE)
  if not res['OK']:
    return S_ERROR("SourceSE not valid: %s" % res['Message'])
  res = trans.setTargetSE(targetSE)
  if not res['OK']:
    return S_ERROR("TargetSE not valid: %s" % res['Message'])

  res = trans.addTransformation()
  if not res['OK']:
    gLogger.error("Failed to create Transformation", res['Message'])
    return res
  gLogger.verbose(res)
  trans.setStatus('Active')
  trans.setAgentType('Automatic')
  currtrans = trans.getTransformationID()['Value']
  client = TransformationClient()
  res = client.createTransformationInputDataQuery(currtrans, metadata)
  if res['OK']:
    gLogger.always("Successfully created replication transformation")
    return S_OK()

  gLogger.error("Failure during replication creation", res['Message'])
  return S_ERROR("Failed to create transformation:%s " % res['Message'])
