"""
Command Line Parameters for Moving Transformation Script
"""

from DIRAC import S_OK, S_ERROR, gLogger


VALIDDATATYPES = ('GEN','SIM','REC','DST')

class Params(object):
  """Parameter Object"""
  def __init__(self):
    self.prodID = None
    self.targetSE = []
    self.sourceSE = None
    self.datatype = None
    self.errorMessages = []
    self.extraname = ''
    self.forcemoving = False

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

  def setForcemoving(self, _forcemoving):
    self.forcemoving = True
    return S_OK()

  def registerSwitches(self, script):
    """ register command line arguments

    :param script: Dirac.Core.Base Script Class
    """

    script.registerSwitch("N:", "Extraname=", "String to append to transformation name", self.setExtraname)
    script.registerSwitch("F", "Forcemoving", "Move GEN or SIM files even if they do not have descendents", self.setForcemoving)
    script.setUsageMessage("""%s <prodID> <TargetSEs> <SourceSEs> {GEN,SIM,REC,DST} -NExtraName [-F]""" % script.scriptName)

  def checkSettings(self, script):
    """check if all required parameters are set, print error message and return S_ERROR if not"""

    args = script.getPositionalArgs()
    if len(args) < 4:
      self.errorMessages.append("ERROR: Not enough arguments")
    else:
      self.setProdID( args[0] )
      self.setTargetSE( args[1] )
      self.setSourceSE( args[2] )
      self.setDatatype( args[3] )
    from ILCDIRAC.Core.Utilities.CheckAndGetProdProxy import checkAndGetProdProxy
    ret = checkAndGetProdProxy()
    if not ret['OK']:
      self.errorMessages.append( ret['Message'] )

    if not self.errorMessages:
      return S_OK()
    gLogger.error("\n".join(self.errorMessages))
    script.showHelp()
    return S_ERROR()
