"""
Command Line Parameters for Moving Transformation Script
"""

from DIRAC import S_OK, S_ERROR, gLogger


VALIDDATATYPES = ('GEN','SIM','REC','DST')

class Params(object):
  """Parameter Object"""
  def __init__(self):
    self.prodIDs = []
    self.targetSE = []
    self.sourceSE = None
    self.datatype = None
    self.errorMessages = []
    self.extraname = ''
    self.forcemoving = False
    self.allFor = []

  def setProdIDs(self,prodID):
    if isinstance( prodID, list ):
      self.prodIDs = prodID
    elif isinstance( prodID, int ):
      self.prodIDs = [ prodID ]
    else:
      self.prodIDs = [ int(pID) for pID in prodID.split(",") ]

    return S_OK()

  def setAllFor(self,allFor):
    self.allFor = allFor
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

  def setForcemoving(self, _forcemoving):
    self.forcemoving = True
    return S_OK()

  def registerSwitches(self, script):
    """ register command line arguments

    :param script: Dirac.Core.Base Script Class
    """

    script.registerSwitch("N:", "Extraname=", "String to append to transformation name", self.setExtraname)
    script.registerSwitch("A:", "AllFor=", "Create usual set of moving transformations for prodID/GEN, prodID+1/SIM, prodID+2/REC", self.setAllFor)
    script.registerSwitch("F", "Forcemoving", "Move GEN or SIM files even if they do not have descendents", self.setForcemoving)

    useMessage = []
    useMessage.append("%s <prodID> <TargetSEs> <SourceSEs> {GEN,SIM,REC,DST} -NExtraName [-F]"% script.scriptName)
    useMessage.extend([ '', 'or', ''])
    useMessage.append('%s --AllFor="<prodID1>, <prodID2>, ..." <TargetSEs> <SourceSEs> -NExtraName [-F]' %script.scriptName )
    script.setUsageMessage( '\n'.join( useMessage ) )

  def checkSettings(self, script):
    """check if all required parameters are set, print error message and return S_ERROR if not"""

    args = script.getPositionalArgs()
    if len(args) == 4:
      self.setProdIDs( args[0] )
      self.setTargetSE( args[1] )
      self.setSourceSE( args[2] )
      self.setDatatype( args[3] )
    elif len(args) == 2 and self.allFor:
      self.setProdIDs( self.allFor )
      ## place the indiviual entries as well.
      prodTemp = list( self.prodIDs )
      for prodID in prodTemp:
        self.prodIDs.append( prodID+1 )
        self.prodIDs.append( prodID+2 )
      self.prodIDs = sorted( self.prodIDs )

      self.setTargetSE( args[0] )
      self.setSourceSE( args[1] )
    else:
      self.errorMessages.append("ERROR: Not enough arguments")

    from ILCDIRAC.Core.Utilities.CheckAndGetProdProxy import checkAndGetProdProxy
    ret = checkAndGetProdProxy()
    if not ret['OK']:
      self.errorMessages.append( ret['Message'] )

    if not self.errorMessages:
      return S_OK()
    gLogger.error("\n".join(self.errorMessages))
    script.showHelp()
    return S_ERROR()


  def checkDatatype( self, prodID, datatype ):
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
