'''
Otain the related steering file directory given a certain 
software name, version and system config

Created on Feb 10, 2012

@author: Stephane Poss
@since: Feb 10, 2012
'''

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation  import getSoftwareFolder
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

def getSteeringFileDirName(systemConfig, application, applicationVersion):
  """ Locate the path of the steering file directory assigned to the specified application
  """
  ops = Operations()
  version = ops.getValue('/AvailableTarBalls/%s/%s/%s/Dependencies/steeringfiles/version' % (systemConfig, 
                                                                                            application,
                                                                                            applicationVersion), '')
  if not version: 
    return S_ERROR("Could not find attached SteeringFile version")
  TarBall = ops.getValue('/AvailableTarBalls/%s/steeringfiles/%s/TarBall' % (systemConfig, version), '')
  if not TarBall:
    return S_ERROR("Could not find tar ball for SteeringFile")
  mydir = TarBall.replace(".tgz", "").replace(".tar.gz", "")
  res = getSoftwareFolder(mydir)
  if not res['OK']:
    return res
  mySoftDir = res['Value']
  return S_OK(mySoftDir)
