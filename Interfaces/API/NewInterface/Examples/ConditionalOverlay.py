"""Create Conditional Overlay."""
from __future__ import print_function
from DIRAC.Core.Base import Script

Script.parseCommandLine()

from ILCDIRAC.Interfaces.API.NewInterface.Applications import OverlayInput, Marlin
from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob
from ILCDIRAC.Interfaces.API.DiracILC import DiracILC

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
fc = FileCatalogClient()

meta={}
meta["ProdID"]=651

res = fc.findFilesByMetadata(meta)
if not res['OK']:
    print("Found no files")
    exit()

lfns = res['Value']    
print("found %s files" % len(lfns))


ovi = OverlayInput() 
ovi.setEnergy(500.)
ovi.setBXOverlay(300)
ovi.setGGToHadInt(0.3)
ovi.setNbSigEvtsPerJob(10)
ovi.setBkgEvtType("gghad")
ovi.setDetectorModel("CLIC_ILD_CDR")

overlay = [True,False]

for ov in overlay:
  d = DiracILC(True,"repo_overlay_%s.rep"%ov)
  for lfn in lfns:
    j = UserJob()
    steeringf = "clic_ild_cdr_steering.xml"
    if ov:
      steeringf = "clic_ild_cdr_steering_overlay.xml"
      res = j.append(ovi)
      if not res['OK']:
        print(res['Message'])
        continue
    ma = Marlin() 
    ma.setVersion("v0111Prod")
    ma.setGearFile("clic_ild_cdr.gear")
    ma.setSteeringFile(steeringf)
    ma.setInputFile("LFN:"+lfn)
    ma.setNbEvts(10)
    ma.setEnergy(500.)
    ma.setOutputRecFile("myrec_overlay_%s.slcio"%ov)
    ma.setOutputDstFile("mydst_overlay_%s.slcio"%ov)
    res =j.append(ma)
    if not res['OK']:
      print(res['Message'])
      exit()

    j.setCPUTime(86400)
    j.setOutputData("myrec_overlay_%s.slcio"%ov,"some/path")
    j.setName("SomeName")
    j.setJobGroup("SomeGroup")
    res = d.checkparams(j)
    if not res['OK']:
      print(res['Message'])
      exit()

    j.submit(d)
