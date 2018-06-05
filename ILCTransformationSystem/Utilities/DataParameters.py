"""
Command Line Parameters for Replication transformation scripts
"""

import os

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.TransformationSystem.Utilities.ReplicationCLIParameters import Params as DParams

VALIDDATATYPES = ('GEN', 'SIM', 'REC', 'DST')


def checkDatatype(prodID, datatype):
  """Check if the datatype makes sense for given production."""
  # skip data type check when creating replications in development for prod productions this check doesn't work
  if os.environ.get('SKIP_CHECK', False):
    gLogger.warn("Skipping Datatype check!")
    return S_OK()

  tClient = TransformationClient()
  cond = dict(TransformationID=prodID)
  trafo = tClient.getTransformations(cond)
  if not trafo['OK']:
    return trafo
  if len(trafo['Value']) != 1:
    return S_ERROR("Did not get unique production for this prodID")

  trafoType = trafo['Value'][0]['Type'].split("_")[0]

  dataTypes = Operations().getOptionsDict('Production/TransformationDatatypes')
  if not dataTypes['OK']:
    return dataTypes

  dataTypes = dataTypes['Value']
  if trafoType not in dataTypes[datatype]:
    return S_ERROR("Datatype %r doesn't fit production type %r for prodID %s" % (datatype, trafoType, prodID))

  return S_OK()


class Params(DParams):
  """Command line parameter class."""

  def __init__(self):
    super(Params, self).__init__()
    self.datatype = None
    self.forcemoving = False
    self.allFor = []

  def setAllFor(self, allFor):
    self.allFor = allFor
    return S_OK()

  def setDatatype(self, datatype):
    if datatype.upper() not in VALIDDATATYPES:
      self.errorMessages.append("ERROR: Unknown Datatype, use %s " % (",".join(VALIDDATATYPES),))
      return S_ERROR()
    self.datatype = datatype.upper()
    return S_OK()

  def setForcemoving(self, _forcemoving):
    self.forcemoving = True
    return S_OK()

  def registerSwitches(self, script):
    """ register command line arguments

    :param script: Dirac.Core.Base Script Class
    """
    super(Params, self).registerSwitches(script)

    useMessage = []
    useMessage.append("%s <prodID> <TargetSEs> <SourceSEs> {GEN,SIM,REC,DST} -NExtraName [-F] [-S 1]"
                      % script.scriptName)
    useMessage.extend(['', 'or', ''])
    useMessage.append('%s --AllFor="<prodID1>, <prodID2>, ..." <TargetSEs> <SourceSEs> -NExtraName [-F] [-S 1]'
                      % script.scriptName)
    script.setUsageMessage('\n'.join(useMessage))

  def checkSettings(self, script, checkArguments=False):
    """Check if all required parameters are set, print error message and return S_ERROR if not."""
    super(Params, self).checkSettings(script, checkArguments=checkArguments)

    args = script.getPositionalArgs()
    if len(args) == 4:
      self.setMetaValues(args[0])
      # cast the metaValues to int
      self.metaValues = [int(val) for val in self.metaValues]
      self.setTargetSE(args[1])
      self.setSourceSE(args[2])
      self.setDatatype(args[3])
    elif len(args) == 2 and self.allFor:
      self.setMetaValues(self.allFor)
      self.metaValues = [int(val) for val in self.metaValues]
      # place the indiviual entries as well.
      prodTemp = list(self.metaValues)
      for prodID in prodTemp:
        self.metaValues.append(prodID + 1)
        self.metaValues.append(prodID + 2)
      self.metaValues = sorted(self.metaValues)

      self.setTargetSE(args[0])
      self.setSourceSE(args[1])
    else:
      self.errorMessages.append("ERROR: Not enough arguments")

    if not self.errorMessages:
      return S_OK()
    gLogger.error("\n".join(self.errorMessages))
    script.showHelp()
    return S_ERROR()
