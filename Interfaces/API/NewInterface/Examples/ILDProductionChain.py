'''
Created on Mar 26, 2012

@author: Stephane Poss

Modified to perform ILD reconstruction.

C. Calancha
'''

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations


from ILCDIRAC.Interfaces.API.NewInterface.ILDProductionJob import ILDProductionJob
from ILCDIRAC.Interfaces.API.NewInterface.Applications     import Mokka, Marlin, OverlayInput
from ILCDIRAC.Interfaces.API.NewInterface.Applications     import SLCIOSplit, StdHepSplit
from decimal import Decimal



# TODO: add evttype to the ProdGroup
analysis         = 'ILD-DBD' ##Some analysis: the prods will belong to the ProdGroup
my_evttype       = 'higgs_ffh'
my_evtclass      = 'higgs'
selectedfile     = 1
prodid           = 6556
genprocessname   = 'qqh_ww_4q'
process          = '106730'
energy           = 500. ##This is mostly needed to define easily the steering files and the overlay parameters
analysis += '_' + my_evttype

meta_energy       = str(int(energy)) ##This is needed for the meta data search below
my_mokka_conf_dbd = 'v01-14-01-p00'

# for the overlay: using DBD numbers
BXOverlay      = 1
GGToHadInt250  = 0.2
GGToHadInt350  = 0.33
GGToHadInt500  = 1.7
GGToHadInt1000 = 4.1

MarlinVer    = "ILCSoft-01-16-02-p1"
MokkaVer     = "080003"
banned_sites = [""]
dryrun       = False
# do not register anything nor create anything.
# Should be used once the splitting-at-stdhep-level prods are submitted.

detectorModel = 'ILD_o1_v05'    ##OR anything valid, but be careful with the overlay, the files need to exist
ILDConfig = ''
dbslice = "mokka-08-00-dbdump.sql"

machineParameters = 'TDR_ws'
if energy == 1000.:
  # ILDConfig         = 'v01-14-01-p00' # this is wrong!
  ILDConfig         = ILDConfig or 'v01-16-p03'
  machineParameters = 'B1b_ws'
elif energy == 500.:
  ILDConfig     = ILDConfig or 'v01-16-p05_500'
elif energy == 350.:
  ILDConfig     = ILDConfig or 'v01-16-p09_350'
elif energy == 250.:
  ILDConfig     = ILDConfig or 'v01-16-p10_250'
else:
  print "ILDConfig ILD: No ILDConfig defined for this energy (%.1f GeV)"%energy

additional_name   = '_' + genprocessname + '_20160623_25_' + str(selectedfile) + '_ildconfig-' + ILDConfig

energyMachinePars        = meta_energy + '-' + machineParameters
# Following variables avoid output from stdhepsplit being used
# as input for the same production. Also speed up the job submission.
basepath = Operations().getValue( '/Production/ILC_ILD/BasePath', '/ilc/prod/ilc/mc-dbd/ild/' )

matchToInput_stdhepsplit = '/ilc/prod/ilc/mc-dbd/generated/' + energyMachinePars + '/' + my_evtclass
matchToInput_mokka       = basepath + "splitted/" + energyMachinePars + '/' + my_evttype + '/' + ILDConfig
matchToInput_marlin      = basepath + "sim/" + energyMachinePars + '/' + my_evttype + '/' + detectorModel + '/' + my_mokka_conf_dbd

SE        = "CERN-DST-EOS"
###LCG_SITE  = "LCG.KEK.jp"
input_sand_box = [""]
##This is where magic happens
meta              = {}

meta['Datatype']       = 'gen' # MOKKA or stdhepsplit or MOKKA+MARLIN
#meta['Datatype']      = 'SIM' # JUST MARLIN / MARLIN_OVERLAY

meta['Energy']         = meta_energy
meta['Machine']        = 'ilc'
meta['GenProcessName'] = genprocessname
meta['MachineParams']  = machineParameters
meta['SoftwareTag']    = my_mokka_conf_dbd

# GenProcessID or ProcessID
if meta['Datatype'] == 'gen':
  meta['GenProcessID'] = process
else:
  meta['ProcessID'] = process
  # These parameters automatically retrieved if you run Mokka. If
  # running standalone Marlin you need to specify them
  meta['DetectorModel'] = detectorModel
  meta['EvtClass']      = my_evttype
  meta['MachineParams'] = machineParameters
  meta['ProdID']        = prodid
    
#DoSplit at stdhep level
activesplitstdhep   = True
nbevtsperfilestdhep = 500
nbtasks_split       = -1 # To run overall input stdhep
if activesplitstdhep:
  if selectedfile > 0:
    meta['SelectedFile'] = selectedfile
  else:
    print 'ERROR: stdhepsplit requires SelectedFile in the metadata to prevent output being used as input.'
    exit(1)
else:
  if prodid:
    meta['ProdID'] = prodid
  if selectedfile > 0:
    meta['SelectedFile'] = selectedfile
    print 'Warning: SelectedFile meta field active: this should only happen when debugging.'

#Do Sim
ild_sim  = True
#nbtasks = 1 #Take 10 files from input meta data query result
#It's possible to get this number automatically by getting the number of events per file (if known)
#nbtasks = math.ceil(number_of_events_to_process/nb_events_per_signal_file) #needs import math
#can be extended with dirac-ilc-add-tasks-to-prod

#DoSplit
activesplit   = False
nbevtsperfile = 200

#Do Reco with Overlay
ild_rec_ov    = True
#Do Reco
ild_rec       = False # please, use WITH OVERLAY

###### Whatever is below is not to be touched... Or at least only when something changes

##Split
stdhepsplit = StdHepSplit()
stdhepsplit.setVersion("V2")
stdhepsplit.setNumberOfEventsPerFile(nbevtsperfilestdhep)

##Simulation ILD
mo = Mokka()
mo.setVersion(MokkaVer) ###SET HERE YOUR MOKKA VERSION, the software will come from the ILDConfig
mo.setDetectorModel(detectorModel)
mo.setSteeringFile("bbudsc_3evt.steer")
### Do not include '.tgz'
mo.setDbSlice(dbslice)

##Split
split = SLCIOSplit()
split.setNumberOfEventsPerFile(nbevtsperfile)

##Define the overlay
overlay = OverlayInput()
overlay.setMachine("ilc_dbd")             #Don't touch, this is how the system knows what files to get
overlay.setEnergy(energy)                 #Don't touch, this is how the system knows what files to get
overlay.setDetectorModel(detectorModel) #Don't touch, this is how the system knows what files to get
overlay.setBkgEvtType("aa_lowpt2")
if energy==500.: #here you chose the overlay parameters as this determines how many files you need
  #it does NOT affect the content of the marlin steering file whatsoever, you need to make sure the values
  #there are correct. Only the file names are handled properly so that you don't need to care
  overlay.setBXOverlay(BXOverlay)
  overlay.setGGToHadInt(GGToHadInt500)
elif energy == 1000.:
  overlay.setBXOverlay(BXOverlay)
  overlay.setGGToHadInt(GGToHadInt1000)
elif energy == 350.:
  overlay.setBXOverlay(BXOverlay)
  overlay.setGGToHadInt(GGToHadInt350)
elif energy == 250.:
  overlay.setBXOverlay(BXOverlay)
  overlay.setGGToHadInt(GGToHadInt250)
else:
  print "Overlay ILD: No overlay parameters defined for this energy"

##Reconstruction ILD with overlay
mao = Marlin()
mao.setDebug()
mao.setVersion(MarlinVer) ##PUT HERE YOUR MARLIN VERSION
if ild_rec_ov:
  if energy in [250.0, 350.0, 500.0, 1000.0]:
    mao.setSteeringFile("bbudsc_3evt_stdreco.xml")
    mao.setGearFile("GearOutput.xml")
  else:
    print "Marlin: No reconstruction suitable for this energy"


##Reconstruction ILD w/o overlay
ma = Marlin()
ma.setDebug()
ma.setVersion(MarlinVer)
ma.setEnergy(energy)
if ild_rec:
  if energy in [250.0, 350.0, 500.0, 1000.0]:
    ma.setSteeringFile("stdreco.xml")
    ma.setGearFile("GearOutput.xml")
  else:
    print "Marlin: No reconstruction suitable for this energy %g"%(energy)


###################################################################################
### HERE WE DEFINE THE PRODUCTIONS
if activesplitstdhep and meta:
  pstdhepsplit = ILDProductionJob()
  pstdhepsplit.basepath = basepath + "splitted/"
  pstdhepsplit.matchToInput = matchToInput_stdhepsplit
  pstdhepsplit.setDryRun(dryrun)
  pstdhepsplit.setILDConfig(ILDConfig)
  pstdhepsplit.setEvtClass(my_evtclass)
  pstdhepsplit.setEvtType(my_evttype)
  # pstdhepsplit.setUseSoftTagInPath(False)
  pstdhepsplit.setLogLevel("verbose")
  pstdhepsplit.setProdType('Split')
  pstdhepsplit.setBannedSites(banned_sites)
  pstdhepsplit.setInputSandbox( input_sand_box )
  pstdhepsplit.setProdPlugin('Limited') # exit with error: it seems i need
                                        # to set the Prod. plugin


  # generated files has not SoftwareTag: we exclude it from inputdataquery, but
  # reinserted at the end of the module to be used in the next simulation module
  tmp_softwaretag_val = ''
  if 'SoftwareTag' in meta:
    tmp_softwaretag_val = meta.pop('SoftwareTag')
    print "'SoftwareTag' found in metadata for pstdhepsplit module: excluded from input data query"

  res = pstdhepsplit.setInputDataQuery(meta)
  if not res['OK']:
    print res['Message']
    exit(1)
  pstdhepsplit.setOutputSE(SE)
  wname = process+"_"+str(energy)+"_split"
  wname += additional_name
  pstdhepsplit.setWorkflowName(wname)
  pstdhepsplit.setProdGroup(analysis+"_"+str(energy))

  #Add the application
  res = pstdhepsplit.append(stdhepsplit)
  if not res['OK']:
    print res['Message']
    exit(1)
  pstdhepsplit.addFinalization(True,True,True,True)
  descrp = "Splitting stdhep files"
  if additional_name:
    descrp += ", %s"%additional_name
  pstdhepsplit.setDescription(descrp)

  res = pstdhepsplit.createProduction()
  if not res['OK']:
    print res['Message']

  res = pstdhepsplit.setProcessIDInFinalPath()
  if not res['OK']:
    print res['Message']

  # 20150924 testing if i need this: i found this is mandatory to get the Mokka input
  res = pstdhepsplit.setSoftwareTagInFinalPath(tmp_softwaretag_val)
  if not res['OK']:
    print res['Message']

  res = pstdhepsplit.finalizeProd()
  if not res['OK']:
    print res['Message']
    exit(1)
  pstdhepsplit.setNbOfTasks(nbtasks_split)
  #As before: get the metadata for this production to input into the next
  meta = pstdhepsplit.getMetadata()
  if tmp_softwaretag_val: # reinsert SoftwareTag: used in path construction
    meta.update({'SoftwareTag' : tmp_softwaretag_val})


if ild_sim and meta:
  ####################
  ##Define the second production (simulation). Notice the setInputDataQuery call
  pmo = ILDProductionJob()
  pmo.matchToInput = matchToInput_mokka
  pmo.setDryRun(dryrun)
  pmo.setProdPlugin('Standard')
  pmo.setILDConfig(ILDConfig)
  pmo.setEvtClass(my_evtclass)
  pmo.setUseSoftTagInPath(True)
  pmo.setEvtType(my_evttype)
  pmo.setLogLevel("verbose")
  pmo.setProdType('MCSimulation')
  pmo.setBannedSites(banned_sites)
  pmo.setInputSandbox( input_sand_box )
  # pmo.setDestination(LCG_SITE)

  res = pmo.setInputDataQuery(meta)
  if not res['OK']:
    print res['Message']
    exit(1)
  pmo.setOutputSE(SE)
  wname = process+"_"+str(energy)+"_ild_sim"
  wname += additional_name
  pmo.setWorkflowName(wname)
  pmo.setProdGroup(analysis+"_"+str(energy))
  #Add the application
  res = pmo.append(mo)
  if not res['OK']:
    print res['Message']
    exit(1)
  pmo.addFinalization(True,True,True,True)
  descrp = "%s model" % detectorModel

  if additional_name:
    descrp += ", %s"%additional_name
  pmo.setDescription(descrp)
  res = pmo.createProduction()
  if not res['OK']:
    print res['Message']

  res = pmo.setProcessIDInFinalPath()
  if not res['OK']:
    print res['Message']

  res = pmo.finalizeProd()
  if not res['OK']:
    print res['Message']
    exit(1)
  #As before: get the metadata for this production to input into the next
  meta = pmo.getMetadata()

##Split at slcio level (after sim)
if activesplit and meta:
  #######################
  ## Split the input files.
  psplit =  ILDProductionJob()
  psplit.setDryRun(dryrun)
  psplit.setCPUTime(30000)
  psplit.setLogLevel("verbose")
  psplit.setProdType('Split')
  psplit.setInputSandbox( input_sand_box )
  res = psplit.setInputDataQuery(meta)
  if not res['OK']:
    print res['Message']
    exit(1)
  psplit.setOutputSE(SE)
  wname = process+"_"+str(energy)+"_split"
  wname += additional_name
  psplit.setWorkflowName(wname)
  psplit.setProdGroup(analysis+"_"+str(energy))

  #Add the application
  res = psplit.append(split)
  if not res['OK']:
    print res['Message']
    exit(1)
  psplit.addFinalization(True,True,True,True)
  descrp = "Splitting slcio files"
  if additional_name:
    descrp += ", %s"%additional_name
  psplit.setDescription(descrp)

  res = psplit.createProduction()
  if not res['OK']:
    print res['Message']

  res = psplit.finalizeProd()
  if not res['OK']:
    print res['Message']
    exit(1)
  #As before: get the metadata for this production to input into the next
  meta = psplit.getMetadata()

if ild_rec and meta:
  #######################
  #Define the reconstruction prod
  pma = ILDProductionJob()
  pma.setDryRun(dryrun)
  pma.setILDConfig(ILDConfig)
  pma.setLogLevel("verbose")
  pma.setProdType('MCReconstruction')

  res = pma.setInputDataQuery(meta)
  if not res['OK']:
    print res['Message']
    exit(1)

  pma.setOutputSE(SE)
  wname = process+"_"+str(energy)+"_ild_rec"
  wname += additional_name
  pma.setWorkflowName(wname)
  pma.setProdGroup(analysis+"_"+str(energy))

  #Add the application
  res = pma.append(ma)
  if not res['OK']:
    print res['Message']
    exit(1)
  pma.addFinalization(True,True,True,True)
  descrp = "%s, No overlay" % detectorModel
  if additional_name:
    descrp += ", %s"%additional_name
  pma.setDescription(descrp)

  res = pma.createProduction()
  if not res['OK']:
    print res['Message']
  res = pma.finalizeProd()
  if not res['OK']:
    print res['Message']
    exit(1)

if ild_rec_ov and meta:
  #######################
  #Define the reconstruction prod
  pmao = ILDProductionJob()
  pmao.matchToInput = matchToInput_marlin
  pmao.setDryRun(dryrun)
  pmao.setILDConfig(ILDConfig)
  pmao.setEvtClass(my_evtclass)
  # pmao.setUseSoftTagInPath(False)
  pmao.setEvtType(my_evttype)
  pmao.setLogLevel("verbose")
  pmao.setProdType('MCReconstruction_Overlay')
  pmao.setBannedSites(banned_sites)
  pmao.setOutputSandbox(["*.log"])
  pmao.setInputSandbox( input_sand_box )

  res = pmao.setInputDataQuery(meta)
  if not res['OK']:
    print res['Message']
    exit(1)
  pmao.setOutputSE(SE)
  wname = process+"_"+str(energy)+"_ild_rec_overlay"
  wname += additional_name
  pmao.setWorkflowName(wname)
  pmao.setProdGroup(analysis+"_"+str(energy))

  #Add the application
  res = pmao.append(overlay)
  if not res['OK']:
    print res['Message']
    exit(1)
  #Add the application
  res = pmao.append(mao)
  if not res['OK']:
    print res['Message']
    exit(1)
  pmao.addFinalization(True,True,True,True)
  descrp = "%s, Overlay" % detectorModel

  if additional_name:
    descrp += ", %s"%additional_name
  pmao.setDescription(descrp)
  res = pmao.createProduction()
  if not res['OK']:
    print res['Message']

  res = pmao.setProcessIDInFinalPath()
  if not res['OK']:
    print res['Message']

  res = pmao.finalizeProd()
  if not res['OK']:
    print res['Message']
    exit(1)
