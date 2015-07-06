#!/bin/env python
'''
Created on May 18, 2015

@author: A. Sailer
'''
__RCSID__ = "$Id$"
from DIRAC.Core.Base import Script
from DIRAC import gLogger, S_OK, S_ERROR
from types import ListType, DictType
from DIRAC.Core.Utilities import uniqueElements
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

VALIDDATATYPES = ('GEN','SIM','REC','DST')

class Params(object):
  """Parameter Object"""
  def __init__(self):
    self.prodID = None
    self.targetSE = []
    self.sourceSE = None
    self.datatype = None

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
    if not datatype.upper() in VALIDDATATYPES:
      return S_ERROR("Unknown Datatype, use %s " % (",".join(VALIDDATATYPES),) )
    self.datatype = datatype
    return S_OK()
  
  def registerSwitches(self):
    Script.registerSwitch("P:", "ProductionID=", "ID of the production to replicate", self.setProdID)
    Script.registerSwitch("T:", "TargetSE=", "Target StorageElement", self.setTargetSE)
    Script.registerSwitch("S:", "SourceSE=", "Source StorageElement", self.setSourceSE)
    Script.registerSwitch("D:", "DataType=", "DataType (GEN,SIM,REC,DST)", self.setDatatype)
    
    Script.setUsageMessage("""%s -P<prodID> -T<TargetSE> -S<SourceSE> -D{GEN,SIM,REC,DST}""" % Script.scriptName)

  def checkSettings(self):
    """check if all required parameters are set, print error message and return S_ERROR if not"""
    allIsGood = True
    if not self.prodID:
      gLogger.error("ProdID is not set")
      allIsGood = False

    if not self.targetSE:
      gLogger.error("TargetSE is not set")
      allIsGood = False

    if not self.sourceSE:
      gLogger.error("SourceSE is not set")
      allIsGood = False

    if not self.datatype:
      gLogger.error("Datatype is not set")
      allIsGood = False
      
    if allIsGood:
      return S_OK()
    Script.showHelp()
    return S_ERROR()

def createReplication( targetSE, sourceSE, prodID, datatype):
  """Creates the replication transformation based on the given parameters"""

  from DIRAC.TransformationSystem.Client.Transformation import Transformation
  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
  metadata = {"Datatype":datatype, "ProdID":prodID}
  
  trans = Transformation()
  trans.setTransformationName( 'replicate_%s_%s' % ( str(prodID), ",".join(targetSE) ) )
  description = 'Replicate files for prodID %s to %s' % ( str(prodID), ",".join(targetSE) )
  trans.setDescription( description )
  trans.setLongDescription( description )
  trans.setType( 'Replication' )
  trans.setPlugin( 'Broadcast' )
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


def createTrafo():
  """reads command line parameters, makes check and creates replication transformation"""
  from DIRAC import exit as dexit
  clip = Params()
  clip.registerSwitches()
  Script.parseCommandLine()
  if not clip.checkSettings()['OK']:
    gLogger.error("ERROR: Missing settings")
    dexit(1)
  resCreate = createReplication( clip.targetSE, clip.sourceSE, clip.prodID, clip.datatype)
  if not resCreate['OK']:
    dexit(1)
  dexit(0)
  
if __name__ == '__main__':
  createTrafo()
