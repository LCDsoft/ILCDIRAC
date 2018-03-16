"""
Command Line Parameters for Moving Transformation Script
"""

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Security.ProxyInfo import getProxyInfo


VALIDDATATYPES = ('GEN', 'SIM', 'REC', 'DST')


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
    self.groupSize = 1

  def setProdIDs(self, prodID):
    if isinstance(prodID, list):
      self.prodIDs = prodID
    elif isinstance(prodID, int):
      self.prodIDs = [prodID]
    else:
      self.prodIDs = [int(pID) for pID in prodID.split(",")]

    return S_OK()

  def setAllFor(self, allFor):
    self.allFor = allFor
    return S_OK()

  def setSourceSE(self, sourceSE):
    self.sourceSE = sourceSE
    return S_OK()

  def setTargetSE(self, targetSE):
    self.targetSE = [tSE.strip() for tSE in targetSE.split(",")]
    gLogger.always("TargetSEs: %s" % str(self.targetSE))
    return S_OK()

  def setDatatype(self, datatype):
    if datatype.upper() not in VALIDDATATYPES:
      self.errorMessages.append("ERROR: Unknown Datatype, use %s " % (",".join(VALIDDATATYPES),))
      return S_ERROR()
    self.datatype = datatype.upper()
    return S_OK()

  def setExtraname(self, extraname):
    self.extraname = extraname
    return S_OK()

  def setForcemoving(self, _forcemoving):
    self.forcemoving = True
    return S_OK()

  def setGroupSize(self, size):
    self.groupSize = size
    return S_OK()

  def registerSwitches(self, script):
    """ register command line arguments

    :param script: Dirac.Core.Base Script Class
    """

    script.registerSwitch("N:", "Extraname=", "String to append to transformation name", self.setExtraname)
    script.registerSwitch("S:", "GroupSize=", "Number of Files per transformation task", self.setGroupSize)

    useMessage = []
    useMessage.append("%s <prodID> <TargetSEs> <SourceSEs> {GEN,SIM,REC,DST} -NExtraName [-F] [-S 1]"
                      % script.scriptName)
    useMessage.extend(['', 'or', ''])
    useMessage.append('%s --AllFor="<prodID1>, <prodID2>, ..." <TargetSEs> <SourceSEs> -NExtraName [-F] [-S 1]'
                      % script.scriptName)
    script.setUsageMessage('\n'.join(useMessage))

  def checkSettings(self, script):
    """check if all required parameters are set, print error message and return S_ERROR if not"""

    args = script.getPositionalArgs()
    if len(args) == 4:
      self.setProdIDs(args[0])
      self.setTargetSE(args[1])
      self.setSourceSE(args[2])
      self.setDatatype(args[3])
    elif len(args) == 2 and self.allFor:
      self.setProdIDs(self.allFor)
      # place the indiviual entries as well.
      prodTemp = list(self.prodIDs)
      for prodID in prodTemp:
        self.prodIDs.append(prodID + 1)
        self.prodIDs.append(prodID + 2)
      self.prodIDs = sorted(self.prodIDs)

      self.setTargetSE(args[0])
      self.setSourceSE(args[1])
    else:
      self.errorMessages.append("ERROR: Not enough arguments")

    self.checkProxy()

    if not self.errorMessages:
      return S_OK()
    gLogger.error("\n".join(self.errorMessages))
    script.showHelp()
    return S_ERROR()

  def checkProxy(self):
    """checks if the proxy belongs to ilc_prod"""
    proxyInfo = getProxyInfo()
    if not proxyInfo['OK']:
      self.errorMessages.append("ERROR: No Proxy present")
      return False
    proxyValues = proxyInfo.get('Value', {})
    group = proxyValues.get('group')

    if group:
      if not group == "ilc_prod":
        self.errorMessages.append("ERROR: Not allowed to create production, you need a ilc_prod proxy.")
        return False
    else:
      self.errorMessages.append("ERROR: Could not determine group, you do not have the right proxy.")
      return False
    return True
