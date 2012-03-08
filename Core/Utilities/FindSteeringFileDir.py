'''
Created on Feb 10, 2012

@author: Stephane Poss
'''

from DIRAC import gConfig,S_OK,S_ERROR
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation  import LocalArea,SharedArea
import os

def getSteeringFileDirName(systemConfig,application,applicationVersion):
  dir = ''
  version = gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s/Dependencies/steeringfiles/version'%(systemConfig,application,applicationVersion),'')
  if not version:
    return S_ERROR("Could not find attached SteeringFile version")
  TarBall = gConfig.getValue('/Operations/AvailableTarBalls/%s/steeringfiles/%s/TarBall'%(systemConfig,version),'')
  if not TarBall:
    return S_ERROR("Could not find tar ball for SteeringFile")
  dir = TarBall.replace(".tgz","").replace(".tar.gz","")
  localArea = LocalArea()
  sharedArea = SharedArea()
  if os.path.exists('%s%s%s' %(localArea,os.sep,dir)):
    mySoftwareRoot = localArea
  elif os.path.exists('%s%s%s' %(sharedArea,os.sep,dir)):
    mySoftwareRoot = sharedArea
  else:
    return S_ERROR('Missing installation of Steering files!')
  mySoftDir = os.path.join(mySoftwareRoot,dir)
  return S_OK(mySoftDir)
