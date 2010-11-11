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
p.addMokkaStep("0705CDR_V2","clic01_ild.steer",detectormodel="",numberofevents=100,
               outputfile=basefile+"_sim.slcio",outputpath=basepath+"ILD/SIM",outputSE="CERN-SRM")
p.addFinalizationStep(True,True,True)
p.setInputSandbox(["LFN:/ilc/prod/software/mokka/steeringfile/20101105/clic_ild_cdr.steer"])#need to pass somehow the input steering files
p.setCPUTime(300000)
p.setOutputSandbox(["*.log"])##because the logs are stored on the vo box and can be resurrected if needed, but not yet available strait forwardly.
p.setWorkflowName("%s_%s"%(process,energy))
p.setWorkflowDescription("Simulating %s events at %s"%(process,energy))
p.setProdType("MCReconstruction")##As it has some input data, is is not generation
p.setProdGroup("%s_%s"%(process,energy))
res = p.create()
if not res['OK']:
  print res['Message']
  sys.exit(1)

meta = {}
meta['Datatype']='gen' ##Can be gen, SIM, REC or DST
meta['Energy']=energy
meta['EvtType']=process
meta['ProdId']=95
p.setInputDataQuery(meta)

###Can also use the following in case there is not metadata entry in the FC
#Get a list of lfns in a list called lfns
#use:
#p.setInputDataLFNs(lfn)
