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
energy = "91gev"
process = "qq"
meta={}
meta['Datatype']='gen' ##Can be gen, SIM, REC or DST
meta['Energy']=energy
meta['EvtType']=process
meta['ProdID']=128

step="sim"

p.defineInputData(meta)
p.addMokkaStep("0706","clic_ild_cdr.steer",detectormodel="",
               outputSE="CERN-SRM")
p.addFinalizationStep(True,True,True,True)
p.setInputSandbox(["LFN:/ilc/prod/software/mokka/steeringfile/20101116/clic_ild_cdr.steer"])#need to pass somehow the input steering files
p.setCPUTime(300000)
p.setOutputSandbox(["*.log"])##because the logs are stored on the vo box and can be resurrected if needed, but not yet available strait forwardly.
p.setWorkflowName("%s_%s_%s"%(process,step,energy))
p.setWorkflowDescription("Simulating %s events at %s"%(process,energy))
p.setProdType("MCReconstruction")##As it has some input data, it is not generation
p.setProdGroup("%s_%s_cdr"%(process,energy))
res = p.create()
if not res['OK']:
  print res['Message']
  sys.exit(1)
p.setInputDataQuery()
p.finalizeProdSubmission()

###Can also use the following in case there is not metadata entry in the FC
#Get a list of lfns in a list called lfns
#use:
#p.setInputDataLFNs(lfn)
