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
from DIRAC import gLogger, S_OK, S_ERROR, exit as dexit

from ILCDIRAC.Core.Utilities.CheckAndGetProdProxy import checkAndGetProdProxy
from ILCDIRAC.ILCTransformationSystem.Utilities.MovingTransformation import createMovingTransformation


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

def _createTrafo():
  """reads command line parameters, makes check and creates replication transformation"""
  clip = _Params()
  clip.registerSwitches()
  Script.parseCommandLine()
  if not clip.checkSettings()['OK']:
    gLogger.error("ERROR: Missing settings")
    return 1
  resCreate = createMovingTransformation( clip.targetSE, clip.sourceSE, clip.prodID, clip.datatype, clip.extraname )
  if not resCreate['OK']:
    return 1
  return 0

if __name__ == '__main__':
  dexit(_createTrafo())
