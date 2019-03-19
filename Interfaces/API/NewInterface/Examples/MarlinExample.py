"""This is a Marlin example."""
from __future__ import print_function
from DIRAC.Core.Base import Script
Script.parseCommandLine()
from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob
from ILCDIRAC.Interfaces.API.NewInterface.Applications import Marlin
from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
d= DiracILC(True,"repo.rep")

###In case one wants a loop: comment the folowing.
#for i in range(2):
j = UserJob()
j.setJobGroup("Tutorial")
j.setName("example")#%i)

ma = Marlin()
ma.setVersion("v0111Prod")
ma.setSteeringFile("clic_ild_cdr_steering.xml")
ma.setGearFile("clic_ild_cdr.gear")
ma.setInputFile("LFN:/ilc/prod/clic/3tev/gghad/ILD/SIM/00000187/000/gghad_sim_187_97.slcio")
outdst = "toto.dst.slcio" #% i
outrec = "toto.rec.slcio" #% i
ma.setOutputDstFile(outdst)
ma.setOutputRecFile(outrec)

res = j.append(ma)
if not res['OK']:
    print(res['Message'])
    exit(1)
  
j.setOutputData([outdst,outrec],"some/path","KEK-SRM")
j.setOutputSandbox("*.log")
j.dontPromptMe()
j.submit(d)
