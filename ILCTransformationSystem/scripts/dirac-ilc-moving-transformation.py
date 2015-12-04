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
from DIRAC import gLogger, S_OK, S_ERROR

from ILCDIRAC.Core.Utilities.CheckAndGetProdProxy import checkAndGetProdProxy


__RCSID__ = "$Id$"

VALIDDATATYPES = ('GEN','SIM','REC','DST')

class _Params(object):
  """Parameter Object"""
  def __init__(self):
    self.prodID = None
    self.targetSE = []
    self.sourceSE = None
    self.datatype = None
    self.errorMessages = []
    self.extraname = ''

  def setProdID(self,prodID):
    self.prodID = prodID
    return S_OK()

  def setSourceSE(self, sourceSE):
    self.sourceSE = sourceSE
    return S_OK()

  def setTargetSE(self, targetSE):
    self.targetSE = [tSE.strip() for tSE in targetSE.split(",")]
    gLogger.always("TargetSEs: %s" % str(self.targetSE) )
    return S_OK()

  def setDatatype(self, datatype):
    if datatype.upper() not in VALIDDATATYPES:
      self.errorMessages.append("ERROR: Unknown Datatype, use %s " % (",".join(VALIDDATATYPES),) )
      return S_ERROR()
    self.datatype = datatype.upper()
    return S_OK()

  def setExtraname(self, extraname):
    self.extraname = extraname
    return S_OK()

  def registerSwitches(self):
    Script.registerSwitch("N:", "Extraname=", "String to append to transformation name", self.setExtraname)
    Script.setUsageMessage("""%s <prodID> <TargetSEs> <SourceSEs> {GEN,SIM,REC,DST} -NExtraName""" % Script.scriptName)

  def checkSettings(self):
    """check if all required parameters are set, print error message and return S_ERROR if not"""

    args = Script.getPositionalArgs()
    if len(args) < 4:
      self.errorMessages.append("ERROR: Not enough arguments")
    else:
      self.setProdID( args[0] )
      self.setTargetSE( args[1] )
      self.setSourceSE( args[2] )
      self.setDatatype( args[3] )

    ret = checkAndGetProdProxy()
    if not ret['OK']:
      self.errorMessages.append( ret['Message'] )

    if not self.errorMessages:
      return S_OK()
    gLogger.error("\n".join(self.errorMessages))
    Script.showHelp()
    return S_ERROR()

def _createReplication( targetSE, sourceSE, prodID, datatype, extraname=''):
  """Creates the replication transformation based on the given parameters"""

  from DIRAC.TransformationSystem.Client.Transformation import Transformation
  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
  metadata = {"Datatype":datatype, "ProdID":prodID}

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
  trans.setPlugin( 'Broadcast' )

  transBody = [ ("ReplicateAndRegister", { "SourceSE":sourceSE, "TargetSE":targetSE }),
                ("RemoveReplica", { "TargetSE":sourceSE } ),
              ]

  trans.setBody( transBody )

  res = trans.setSourceSE( sourceSE )
  if not res['OK']:
    exit(1)
  res = trans.setTargetSE( targetSE )
  if not res['OK']:
    exit(1)

  res = trans.addTransformation()
  if not res['OK']:
    gLogger.error(res['Message'])
    exit(1)
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
    return S_ERROR("Failed to create transformation")


def _createTrafo():
  """reads command line parameters, makes check and creates replication transformation"""
  from DIRAC import exit as dexit
  clip = _Params()
  clip.registerSwitches()
  Script.parseCommandLine()
  if not clip.checkSettings()['OK']:
    gLogger.error("ERROR: Missing settings")
    dexit(1)
  resCreate = _createReplication( clip.targetSE, clip.sourceSE, clip.prodID, clip.datatype, clip.extraname )
  if not resCreate['OK']:
    dexit(1)
  dexit(0)

if __name__ == '__main__':
  _createTrafo()
