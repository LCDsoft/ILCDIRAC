'''
Created on Feb 10, 2012

@author: Stephane Poss
'''

from DIRAC import gConfig,S_OK,S_ERROR

def getSteeringFileDirName(systemConfig,application,applicationVersion):
  dir = ''
  version = gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s/Dependencies/steeringfile/version'%(systemConfig,application,applicationVersion),'')
  if not version:
    return S_ERROR("Could not find attached SteeringFile version")
  TarBall = gConfig.getValue('/Operations/AvailableTarBalls/%s/SteeringFile/%s/TarBall'%(systemConfig,version),'')
  if not TarBall:
    return S_ERROR("Could not find tar ball for SteeringFile")
  dir = TarBall.replace(".tgz","").replace(".tar.gz","")
  return S_OK('')
