from DIRAC.Core.Base import Script
Script.parseCommandLine()

from ILCDIRAC.Interfaces.API.NewInterface.UserJob import *
from ILCDIRAC.Interfaces.API.NewInterface.Applications import *
from ILCDIRAC.Interfaces.API.DiracILC import *

d = DiracILC(True,"repo.rep")


n_evts = 500
n_evts_per_job= 100
n_jobs = n_evts/n_evts_per_job

for i in range(n_jobs):
    j = UserJob()

    mo = Mokka()
    mo.setEnergy(3000)
    mo.setVersion("0706P08")
    mo.setSteeringFile("clic_ild_cdr.steer")
    mo.setMacFile("particlegun_electron.mac")
    mo.setOutputFile("MyFile.slcio")
    mo.setNbEvts(n_evts_per_job)
    res = j.append(mo)
    if not res['OK']:
        print res['Message']

    ma = Marlin()
    ma.setVersion("v0111Prod")
    ma.setSteeringFile("clic_ild_cdr_steering.xml")
    ma.getInputFromApp(mo)
    ma.setOutputDstFile("mydst_no_ov_%s.slcio"%i)
    res = j.append(ma)
    if not res['OK']:
        print res['Message']

    ov  = OverlayInput()
    ov.setBXOverlay(60)
    ov.setGGToHadInt(3.2)
    ov.setNbSigEvtsPerJob(n_evts_per_job)
    ov.setDetectorType("ILD")

    res = j.append(ov)
    if not res['OK']:
        print res['Message']

    ma2 = Marlin()
    ma2.setVersion("v0111Prod")
    ma2.setSteeringFile("clic_ild_cdr_steering_overlay.xml")
    ma2.getInputFromApp(mo)
    ma2.setOutputDstFile("mydst_ov_%s.slcio"%i)
    res = j.append(ma2)
    if not res['OK']:
        print res['Message']

    j.setOutputSandbox(["mydst_no_ov_%s.slcio"%i,"mydst_ov_%s.slcio"%i,"*.log"])
    j.setName("SingleElectron_%s"%i)
    j.setJobGroup("singleElectrons")

    d.submit(j)
    
