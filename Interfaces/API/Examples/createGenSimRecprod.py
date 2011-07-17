'''
Created on Nov 10, 2010

@author: sposs
'''

from DIRAC.Core.Base import Script
from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
from ILCDIRAC.Interfaces.API.Production import Production

import sys
Script.parseCommandLine()
dirac = DiracILC()
processlist = dirac.giveProcessList()

p = Production()
energy = "3tev"
process = "bb"
basepath = "/ilc/prod/clic/%s/%s/"%(energy,process)
basefile = "%s"%(process)
p.addWhizardStep(processlist,process,nbevts=100,
                 outputpath=basepath+"gen",outputSE="CERN-SRM")
p.addMokkaStep("0705CDR_V2","clic01_ild.steer",detectormodel="",
               outputfile=basefile+"_sim.slcio",outputpath=basepath+"ILD/SIM",outputSE="CERN-SRM")
p.addMarlinStep("010902v3","clic_01_ild_stdreco.xml",
                outputRECfile=basefile+"_rec.slcio",outputRECpath = basepath+"ILD/REC",
                outputDSTfile=basefile+"_dst.slcio",outputDSTpath = basepath+"ILD/DST",
                outputSE="CERN-SRM")
p.addFinalizationStep(True,True,True)
p.setInputSandbox(["LFN:/ilc/prod/software/mokka/steeringfile/20101105/clic_ild_cdr.steer",
                   "LFN:/ilc/prod/software/marlin/xml/20100702/nets.tar.gz",
                   "LFN:/ilc/prod/software/marlin/xml/20100702/clic_01_ild_stdreco.xml"])#need to pass somehow the input steering files
p.setCPUTime(300000)
p.setOutputSandbox(["*.log"])##because the logs are stored on the vo box and can be resurrected if needed, but not yet available strait forwardly.
p.setWorkflowName("%s_%s"%(process,energy))
p.setWorkflowDescription("Generating, simulating, and reconstructing %s events at %s"%(process,energy))
p.setProdType("MCSimulation")
p.setProdGroup("%s_%s"%(process,energy))
res = p.create()
if not res['OK']:
  print res['Message']
  sys.exit(1)
p.setNbOfTasks(10)
