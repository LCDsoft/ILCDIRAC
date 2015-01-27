'''
Obtain the related steering file directory given a certain 
software name, version and platform

Created on Feb 10, 2012

@author: Stephane Poss
@since: Feb 10, 2012
'''

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation  import getSoftwareFolder, checkCVMFS
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from ILCDIRAC.Core.Utilities.TARsoft import check

def getSteeringFileDirName(platform, application, applicationVersion):
  """ Locate the path of the steering file directory assigned to the specified application
  """
  ops = Operations()
  version = ops.getValue('/AvailableTarBalls/%s/%s/%s/Dependencies/steeringfiles/version' % (platform,
                                                                                             application,
                                                                                             applicationVersion), '')
  if not version: 
    return S_ERROR("Could not find attached SteeringFile version")
  
  return getSteeringFileDir(platform, version)

def getSteeringFileDir(platform, version):
  """Return directly the directory, without passing by the dependency resolution
  """
  res = checkCVMFS(platform, ['steeringfiles', version])
  if res['OK']:
    return S_OK(res['Value'][0])
  #Here means CVMFS is not defined, so we need to rely on the tar ball
  res = getSoftwareFolder(platform, 'steeringfiles', version)
  if not res['OK']:
    return res
  mySoftDir = res['Value']
  ##check that all the files are there: software is not corrupted.
  res = check('steeringfiles.%s' % version, '.', [mySoftDir])
  if not res['OK']:
    return res
  return S_OK(mySoftDir)
