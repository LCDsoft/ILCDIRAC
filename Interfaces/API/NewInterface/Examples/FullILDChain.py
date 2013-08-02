from DIRAC.Core.Base import Script
Script.parseCommandLine()

from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
from ILCDIRAC.Interfaces.API.NewInterface.Applications import Whizard, Mokka, Marlin, OverlayInput
from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob

from DIRAC import exit as dexit

dirac =DiracILC()

#wh.setOutputFile("myfile.stdhep")

j = UserJob()

wh = Whizard(processlist=dirac.getProcessList())
wh.setEnergy(3000)
wh.setEvtType("ee_h_mumu")
wh.setNbEvts(1)
wh.setEnergy(3000)
params = {}
params['USERB1']='F'
wh.setParameterDict(params)
wh.setModel("sm")
res = j.append(wh)
if not res['OK']:
    print res['Message']
    dexit(1)


mo = Mokka()
mo.getInputFromApp(wh)
mo.setVersion("0706P08")
mo.setSteeringFile("clic_ild_cdr.steer")
mo.setNbEvts(1)
mo.setOutputFile("somefile.slcio")
res = j.append(mo)
if not res['OK']:
    print res['Message']
    dexit(1)


ov = OverlayInput()
ov.setDetectorType("ILD")
ov.setBXOverlay(60)
ov.setGGToHadInt(3.2)
ov.setNbSigEvtsPerJob(1)
ov.setBkgEvtType("gghad")
res = j.append(ov)
if not res['OK']:
    print res['Message']
    dexit(1)


ma = Marlin()
ma.setVersion("v0111Prod")
ma.setSteeringFile("clic_ild_cdr_steering_overlay.xml")
ma.setGearFile("clic_ild_cdr.gear")
ma.getInputFromApp(mo)
ma.setDebug(True)
res = j.append(ma)
if not res['OK']:
    print res['Message']
    dexit(1)
#print appplication's attributes.
ma.listAttributes()

j.setName("test")
j.setOutputSandbox("*.log")

res = dirac.checkparams(j)
if not res['OK']:
    print res['Message']
    dexit(1)


