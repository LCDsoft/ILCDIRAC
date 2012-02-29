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
           "clic_ild_cdr500_steering.xml","GearOutput.xml",'cuts_e1e1ff_500gev.txt',
           "cuts_e2e2ff_500gev.txt",'cuts_qq_nunu_1400.txt','cuts_e3e3nn_1400.txt',
           "cuts_e3e3_1400.txt","cuts_e1e1e3e3_o_1400.txt","cuts_aa_e3e3_o_1400.txt",
           "cuts_aa_e3e3nn_1400.txt","cuts_aa_e2e2e3e3_o_1400.txt","cuts_aa_e1e1e3e3_o_1400.txt"]
  if file in files:
    return S_OK()
  else:
    return S_ERROR("File %s is not available locally nor in the software installation."%file)
  