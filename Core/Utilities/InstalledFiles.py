'''
Created on Nov 8, 2011

@author: Stephane Poss
'''
from DIRAC import S_OK,S_ERROR

def Exists(file):
  files = ["defaultClicCrossingAngle.mac","clic_ild_cdr500.steer",
           "clic_ild_cdr.steer","clic_cdr_prePandora.lcsim",
           "clic_cdr_postPandora.lcsim","clic_cdr_prePandoraOverlay.lcsim",
           "clic_cdr_postPandoraOverlay.lcsim","clic_ild_cdr.gear",
           "clic_ild_cdr500.gear","clic_ild_cdr_steering_overlay.xml",
           "clic_ild_cdr500_steering_overlay.xml","clic_ild_cdr_steering.xml",
           "clic_ild_cdr500_steering.xml"]
  if file in files:
    return S_OK()
  else:
    return S_ERROR("File %s is not installed with the other steering files."%file)
  