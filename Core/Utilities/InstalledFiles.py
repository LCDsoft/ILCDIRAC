'''
List of files in the SteeringFiles tar ball.

@author: S. Poss
@since: Nov 8, 2011
'''
from DIRAC import S_OK, S_ERROR

def Exists(myfile):
  """check if the file exists"""
  files = ["defaultClicCrossingAngle.mac", "clic_ild_cdr500.steer",
           "clic_ild_cdr.steer", "clic_cdr_prePandora.lcsim",
           "clic_cdr_postPandora.lcsim", "clic_cdr_prePandoraOverlay.lcsim",
           "clic_cdr_prePandoraOverlay_1400.0.lcsim",
           "clic_cdr_prePandoraOverlay_3000.0.lcsim",
           "clic_cdr_postPandoraOverlay.lcsim", "clic_ild_cdr.gear",
           "clic_ild_cdr500.gear", "clic_ild_cdr_steering_overlay.xml",
           "clic_ild_cdr_steering_overlay_3000.0.xml",
           "clic_ild_cdr_steering_overlay_1400.0.xml",
           "clic_ild_cdr500_steering_overlay.xml",
           "clic_ild_cdr500_steering_overlay_350.0.xml",
           "clic_ild_cdr_steering.xml",
           "clic_ild_cdr500_steering.xml", "GearOutput.xml",
           'cuts_e1e1ff_500gev.txt',
           "cuts_e2e2ff_500gev.txt", 'cuts_qq_nunu_1400.txt',
           'cuts_e3e3nn_1400.txt',
           "cuts_e3e3_1400.txt", "cuts_e1e1e3e3_o_1400.txt", 
           "cuts_aa_e3e3_o_1400.txt",
           "cuts_aa_e3e3nn_1400.txt", "cuts_aa_e2e2e3e3_o_1400.txt", 
           "cuts_aa_e1e1e3e3_o_1400.txt", 
           "defaultStrategies_clic_sid_cdr.xml",
           "defaultIlcCrossingAngle.mac",
           "defaultIlcCrossingAngleZSmearing320.mac",
           "defaultIlcCrossingAngleZSmearing225.mac",
           "sid_dbd_pandoraSettings.xml",
           "sid_dbd_postPandora.xml",
           "sid_dbd_prePandora.xml",
           "sid_dbd_prePandora_noOverlay.xml",
           "sid_dbd_vertexing.xml",
           "sidloi3.gear",
           "sidloi3_trackingStrategies.xml",
           "sidloi3_trackingStrategies_default.xml",
           "ild_00.gear",
           "ild_00_steering.xml",
           "ild_00.steer",
           "cuts_quarks_1400.txt","cuts_taus_1400.txt",
           "cuts_h_gammaZ_1400.txt","cuts_h_gammagamma_1400.txt",
           "cuts_h_mumu_3000.txt",]
  if myfile in files:
    return S_OK()
  else:
    return S_ERROR("File %s is not available locally nor in the software installation." % myfile)
  
