'''

Create Chain of productions for ILD

:author: S. Poss, A.Sailer, C. Calancha
:since: Mar 26, 2012

'''

#pylint: disable=invalid-name, wrong-import-position, bad-whitespace, line-too-long
#pylint: disable=bad-indentation
from __future__ import print_function
import os
import pprint

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations

from ILCDIRAC.Interfaces.API.NewInterface.ILDProductionJobOpt2017 import ILDProductionJobOpt2017
from ILCDIRAC.Interfaces.API.NewInterface.Applications     import Mokka, Marlin, OverlayInput, DDSim
from ILCDIRAC.Interfaces.API.NewInterface.Applications     import SLCIOSplit, StdHepSplit


####################################################################################
'''
Example to run ILDProductionChainOpt2017.py. This script creates 2 transformations
Mokka  or DD4Sim simulation, and Marlin reconsturuction with Overlay.

stdhepsplit won't work for the moment due to an innconsistensy
in creating output filename. This example output sim/rec output to
a temporary directory in /ilc/prod/ild/test/temp1

Simulation and recconstruction step could process more than one GenProcessID.
prodid meta key should be defined to select input file properly.

Define Input serch path, output and input file path properly so that
proper files can be found and processed. This script used prodid = 500002,
for input data query. prodid=500002 is selected, because it will be not used
for the time being.  prodid should be defined to the directory, not as a file meta key
because it is defined as a indexed meta key of directory.

This script limits nb_task for sim and rec to 5. This should be increased to process
more files. dirac-ilc-add-tasks-to-prod command can be used for this purpose.

'''
#####################################################################################



dryrun       = False
# TODO: add evttype to the ProdGroup
analysis         = 'ILD-Opt2017-Test' ##Some analysis: the prods will belong to the ProdGroup
my_evttype       = '3f'
my_evtclass      = '3f'
selectedfile     = 0
prodid           = 500002
genprocessname   = 'ea_lvv'
process          = ''
energy           = 500. ##This is mostly needed to define easily the steering files and the overlay parameters
analysis += '_' + my_evttype

meta_energy       = str(int(energy)) ##This is needed for the meta data search below

# for the overlay: using DBD numbers
BXOverlay      = 1
GGToHadInt250  = 0.2
GGToHadInt350  = 0.33
GGToHadInt500  = 1.7
GGToHadInt1000 = 4.1

MarlinVer    = "ILCSoft-01-19_gcc48"
DDSimVer     = "ILCSoft-01-19_gcc48"
DDSimILDConfig = "v01-19_lcgeo"

ILDConfig = '' ## Set below for different energies
MokkaVer     = "080003"
# MokkaILDConfig = "v01-17-11-p01"
MokkaILDConfig = "v01-14-01-p00"

banned_sites = [""]
# do not register anything nor create anything.
# Should be used once the splitting-at-stdhep-level prods are submitted.

detectorModel = 'ILD_o1_v05'    ##OR anything valid, but be careful with the overlay, the files need to exist
dbslice = "mokka-08-00-dbdump.sql"

machineParameters = 'TDR_ws'

UseDD4hepGeometry = False


## This needs to be adapted when using lcgeo geometry
# if energy == 1000.:
#   # ILDConfig         = 'v01-14-01-p00' # this is wrong!
#   ILDConfig         = ILDConfig or 'v01-16-p03'
#   machineParameters = 'B1b_ws'
# elif energy == 500.:
#   ILDConfig     = ILDConfig or 'v01-16-p05_500'
# elif energy == 350.:
#   ILDConfig     = ILDConfig or 'v01-16-p09_350'
# elif energy == 250.:
#   ILDConfig     = ILDConfig or 'v01-16-p10_250'
# else:
#   print "ILDConfig ILD: No ILDConfig defined for this energy (%.1f GeV)"%energy

## ILDConfig for Marlin with or without DD4hep Geometry
ILDConfigRec_DD = "v01-19_lcgeo"
MarlinVer_DD = "ILCSoft-01-19_gcc48"
# ILDConfigRec    = "v01-17-11-p01"
# MarlinVer_DBD = "ILCSoft-01-17-11_gcc48"
ILDConfigRec = "v01-16-p05_500"
MarlinVer_DBD = "v01-16-02"

ILDConfig = ILDConfigRec_DD if UseDD4hepGeometry else ILDConfigRec
ILDConfigSim = DDSimILDConfig if UseDD4hepGeometry else MokkaILDConfig
MarlinVer = MarlinVer_DD if UseDD4hepGeometry else MarlinVer_DBD

additional_name   = '_' + analysis + '_' + genprocessname + '_20170411_1_' + str(selectedfile) + '_ildconfig-' + ILDConfig

energyMachinePars        = meta_energy + '-' + machineParameters
# Following variables avoid output from stdhepsplit being used
# as input for the same production. Also speed up the job submission.

# basepath = Operations().getValue( '/Production/ILC_ILD/BasePath', '/ilc/prod/ilc/mc-dbd/ild' ).rstrip("/")
# matchToInput_stdhepsplit = '/ilc/prod/ilc/mc-dbd/generated/' + energyMachinePars + '/' + my_evtclass
# matchToInput_mokka       = '/ilc/prod/ilc/mc-dbd.generated/' + energyMachinePars + '/' + my_evttype
# matchToInput_marlin      = basepath + "sim/" + energyMachinePars + '/' + my_evttype + '/' + detectorModel + '/' + ILDConfigSim
# recPath = '/ilc/prod/ilc/mc-dbd/ild/'
# dstPath = '/ilc/prod/ilc/mc-dbd.log/ild/'

# grand_base = '/ilc/prod/ilc/'
grand_base = '/ilc/prod/ilc/ild/test/temp1/'  # Test area for development
basepath = grand_base + 'mc-opt/ild/'
diskpath = grand_base + 'mc-opt.disk/ild/'
my_basepath = basepath # my_basepath is a base path for Mokka and Marlin

matchToInput_stdhepsplit = '/ilc/prod/ilc/mc-dbd/generated/' + energyMachinePars + '/' + my_evtclass
# matchToInput_stdhepsplit is where input stdhep files are for stdhepsplit valid when stdhepsplit production is included.
stdhepsplit_basepath = diskpath + 'splitted/' # Output directory of stdhepsplit, equal to matchToInput_mokka,


matchToInput_mokka = grand_base + 'gensplit/' + energyMachinePars + '/' + my_evttype  + '/run002' # Where to find input for Mokka
# matchToInput_mokka is input directory for simulation. This example uses Mokka input prepared by other script
matchToInput_marlin = basepath + "sim/" + energyMachinePars + '/' + my_evttype + '/' + detectorModel + '/' + ILDConfigSim
recPath = basepath
dstPath = diskpath

# Define output SE depending on expected data size
SE_stdhepsplit = "PNNL-SRM"
SE_sim = "KEK-SRM"
SE_psplit = "PNNL-SRM"
SE_rec = "DESY-SRM"


###LCG_SITE  = "LCG.KEK.jp"
input_sand_box = [""]
##This is where magic happens
meta              = {}

meta['Datatype']       = 'gensplit' # MOKKA or stdhepsplit or MOKKA+MARLIN
#meta['Datatype']      = 'SIM' # JUST MARLIN / MARLIN_OVERLAY

meta['Energy']         = meta_energy
meta['Machine']        = 'ilc'
meta['GenProcessName'] = genprocessname
meta['MachineParams']  = machineParameters
meta['ProdID']         = prodid

# GenProcessID or ProcessID
if meta.get('Datatype', None) in ('gen', 'gensplit'):
  #meta['GenProcessID'] = process
  pass
else:
  meta['ProcessID'] = process
  # These parameters automatically retrieved if you run Mokka. If
  # running standalone Marlin you need to specify them
  meta['DetectorModel'] = detectorModel
  meta['EvtClass']      = my_evttype
  meta['MachineParams'] = machineParameters
  meta['ProdID']        = prodid

# inputFileFolder= "/ilc/prod/ilc/ild/test/temp1/gensplit/500-TDR_ws/3f/run001"

#DoSplit at stdhep level
activesplitstdhep   = False
nbevtsperfilestdhep = 500
nbtasks_split       = -1 # To run over all input stdhep
if activesplitstdhep:
  if selectedfile > 0:
    meta['SelectedFile'] = selectedfile
  else:
    print('ERROR: stdhepsplit requires SelectedFile in the metadata to prevent output being used as input.')
    exit(1)
else:
  if prodid:
    meta['ProdID'] = prodid
  if selectedfile > 0:
    meta['SelectedFile'] = selectedfile
    print('Warning: SelectedFile meta field active: this should only happen when debugging.')

#Do Sim
ild_sim  = True
nbtasks_sim = 3
#nbtasks_sim = 10 # create 10 jobs from input meta data query result. -1 to create for all files.
#It can be extended with dirac-ilc-add-tasks-to-prod
#It's possible to get this number automatically by getting the number of events per file (if known)
#nbtasks = math.ceil(number_of_events_to_process/nb_events_per_signal_file) #needs import math

#DoSplit
activesplit   = False
nbevtsperfile = 200

#Do Reco with Overlay
ild_rec_ov    = True
nbtasks_rec_ov = 3 # See comment on nbtasks_sim
#Do Reco
ild_rec       = False # please, use WITH OVERLAY
nbtasks_rec = -1 # See comment on nbtasks_sim

print("####################################################################################")
if activesplitstdhep:
  print("matchToInput_stdhepsplit (input)      = %s  (Valid when stdhep production is included)"
        % matchToInput_stdhepsplit)
  print("stdhepsplit_basepath (output)         = %s  (Valid when stdhep production is included)"
        % stdhepsplit_basepath)

print("matchToInput_mokka (input)            = " + matchToInput_mokka)
print("matchToInput_marlin (input)           = " + matchToInput_marlin)
print("Basepath for sim&rec (output)         = " + basepath)
print("Diskpath for stdhepsplit&dst (output) = " + diskpath)
print("Outtput SE : stdhepsplit(%s), SUM(%s), psplit(%s), REC&DST(%s)" % (SE_stdhepsplit, SE_sim, SE_psplit, SE_rec))
print("####################################################################################")
##############################################################################################
## [PART2] Below is not to be touched
##############################################################################################
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

##Simulation ILD
ddsim = None
if UseDD4hepGeometry:
  ddsim = DDSim()
  ddsim.setVersion(DDSimVer) ###SET HERE YOUR MOKKA VERSION, the software will come from the ILDConfig
  ddsim.setDetectorModel(detectorModel)
  ddsim.setSteeringFile("ddsim_steer.py")


##Split
split = SLCIOSplit()
split.setNumberOfEventsPerFile(nbevtsperfile)

##Define the overlay
overlay = OverlayInput()
overlay.setMachine("ilc_dbd")             #Don't touch, this is how the system knows what files to get
overlay.setEnergy(energy)                 #Don't touch, this is how the system knows what files to get
overlay.setDetectorModel(detectorModel) #Don't touch, this is how the system knows what files to get
if energy==500.: #here you chose the overlay parameters as this determines how many files you need
  #it does NOT affect the content of the marlin steering file whatsoever, you need to make sure the values
  #there are correct. Only the file names are handled properly so that you don't need to care
  overlay.setBXOverlay(BXOverlay)
  overlay.setGGToHadInt(GGToHadInt500)
  overlay.setBkgEvtType("aa_lowpt2") ## lowpt2: correct number of events (500),
                                     ## not increased to 2500 to reduce number
                                     ## of downloaded files
elif energy == 1000.:
  overlay.setBXOverlay(BXOverlay)
  overlay.setGGToHadInt(GGToHadInt1000)
  overlay.setBkgEvtType("aa_lowpt")
elif energy == 350.:
  overlay.setBXOverlay(BXOverlay)
  overlay.setGGToHadInt(GGToHadInt350)
  overlay.setBkgEvtType("aa_lowpt")
elif energy == 250.:
  overlay.setBXOverlay(BXOverlay)
  overlay.setGGToHadInt(GGToHadInt250)
  overlay.setBkgEvtType("aa_lowpt")

else:
  print("Overlay ILD: No overlay parameters defined for this energy")

##Reconstruction ILD with overlay
mao = Marlin()
mao.setDebug()
mao.setVersion(MarlinVer) ##PUT HERE YOUR MARLIN VERSION
if ild_rec_ov:
  if energy in [250.0, 350.0, 500.0, 1000.0]:
    if UseDD4hepGeometry:
      mao.setSteeringFile("bbudsc_3evt_stdreco_dd4hep.xml")
      mao.setGearFile("GearOutput.xml")
      mao.setDetectorModel(detectorModel)
    else:
      mao.setSteeringFile("bbudsc_3evt_stdreco.xml")
      mao.setGearFile("GearOutput.xml")
  else:
    print("Marlin: No reconstruction suitable for this energy")


##Reconstruction ILD w/o overlay
ma = Marlin()
ma.setDebug()
ma.setVersion(MarlinVer)
ma.setEnergy(energy)
if ild_rec:
  if energy in [250.0, 350.0, 500.0, 1000.0]:
    if UseDD4hepGeometry:
      ma.setSteeringFile("bbudsc_3evt_stdreco_dd4hep.xml")
      ma.setGearFile("GearOutput.xml")
      ma.setDetectorModel(detectorModel)
    else:
      ma.setSteeringFile("bbudsc_3evt_stdreco.xml")
      ma.setGearFile("GearOutput.xml")
  else:
    print("Marlin: No reconstruction suitable for this energy %g" % (energy))


###################################################################################
### HERE WE DEFINE THE PRODUCTIONS
if activesplitstdhep and meta:
  print("################## Createing a production for stdhepsplit. Input meta is")
  pprint.pprint(meta)
  pstdhepsplit = ILDProductionJobOpt2017()
  pstdhepsplit.basepath = stdhepsplit_basepath # Sailer suggestion
  pstdhepsplit.matchToInput = matchToInput_stdhepsplit
  pstdhepsplit.setDryRun(dryrun)
  #pstdhepsplit.setILDConfig(ILDConfig) ## stdhepsplit does not need ILDConfig
  pstdhepsplit.setEvtClass(my_evtclass)
  pstdhepsplit.setEvtType(my_evttype)
  # pstdhepsplit.setUseSoftTagInPath(False)
  pstdhepsplit.setLogLevel("verbose")
  pstdhepsplit.setProdType('Split_ILD')
  pstdhepsplit.setBannedSites(banned_sites)
  pstdhepsplit.setProdPlugin('Limited') # exit with error: it seems i need
                                        # to set the Prod. plugin

  # generated files has not SoftwareTag: we exclude it from inputdataquery, but
  # reinserted at the end of the module to be used in the next simulation module
  tmp_softwaretag_val = ''
  if 'SoftwareTag' in meta:
    tmp_softwaretag_val = meta.pop('SoftwareTag')
    print("'SoftwareTag' found in metadata for pstdhepsplit module: excluded from input data query")

  res = pstdhepsplit.setInputDataQuery(meta)
  if not res['OK']:
    print(res['Message'])
    exit(1)
  pstdhepsplit.setOutputSE(SE_stdhepsplit)
  wname = process+"_"+str(energy)+"_split"
  wname += additional_name
  pstdhepsplit.setWorkflowName(wname)
  pstdhepsplit.setProdGroup(analysis+"_"+str(energy))

  #Add the application
  res = pstdhepsplit.append(stdhepsplit)
  if not res['OK']:
    print(res['Message'])
    exit(1)
  pstdhepsplit.addFinalization(True, True, True, True)
  descrp = "Splitting stdhep files"
  if additional_name:
    descrp += ", %s"%additional_name
  pstdhepsplit.setDescription(descrp)

  res = pstdhepsplit.createProduction()
  if not res['OK']:
    print(res['Message'])

  res = pstdhepsplit.setProcessIDInFinalPath()
  if not res['OK']:
    print(res['Message'])

  res = pstdhepsplit.finalizeProd()
  if not res['OK']:
    print(res['Message'])
    exit(1)
  pstdhepsplit.setNbOfTasks(nbtasks_split)
  #As before: get the metadata for this production to input into the next
  meta = pstdhepsplit.getMetadata()
  if tmp_softwaretag_val: # reinsert SoftwareTag: used in path construction
    meta.update({'SoftwareTag' : tmp_softwaretag_val})

  print("### Following meta data were created for subsequent simulation production")
  pprint.pprint(meta)
  print("### Done With Stdhepsplit", "\n" * 5)

if ild_sim and meta:
  print("################## Createing a production for simulation. Input meta is")
  pprint.pprint(meta)
  ####################
  ##Define the second production (simulation). Notice the setInputDataQuery call
  pSim = ILDProductionJobOpt2017()
  pSim.basepath = my_basepath
  pSim.matchToInput = matchToInput_mokka
  pSim.setDryRun(dryrun)
  pSim.setProdPlugin('Standard')
  pSim.setILDConfig(ILDConfigSim)
  pSim.setEvtClass(my_evtclass)
  pSim.setUseSoftTagInPath(True)
  pSim.setEvtType(my_evttype)
  pSim.setLogLevel("verbose")
  pSim.setProdType('MCSimulation_ILD')
  pSim.setBannedSites(banned_sites)
  pSim.setInputSandbox(input_sand_box)
  # pSim.setDestination(LCG_SITE)
  pSim.setProdPlugin('Limited')

  res = pSim.setInputDataQuery(meta)
  if not res['OK']:
    print(res['Message'])
    exit(1)
  pSim.setOutputSE(SE_sim)
  wname = process+"_"+str(energy)+"_ild_sim"
  wname += additional_name
  pSim.setWorkflowName(wname)
  pSim.setProdGroup(analysis+"_"+str(energy))
  #Add the application
  if UseDD4hepGeometry:
    res = pSim.append(ddsim)
  else:
    res = pSim.append(mo)
  if not res['OK']:
    print(res['Message'])
    exit(1)
  pSim.addFinalization(True, True, True, True)
  descrp = "%s model" % detectorModel

  if additional_name:
    descrp += ", %s"%additional_name
  pSim.setDescription(descrp)
  res = pSim.createProduction()
  if not res['OK']:
    print(res['Message'])

  res = pSim.setProcessIDInFinalPath()
  if not res['OK']:
    print(res['Message'])

  res = pSim.finalizeProd()
  if not res['OK']:
    print(res['Message'])
    exit(1)
  pSim.setNbOfTasks(nbtasks_sim)
  #As before: get the metadata for this production to input into the next
  meta = pSim.getMetadata()

  print("### Done With a creation of ild_sim transformation", "\n" * 5)

##Split at slcio level (after sim)
if activesplit and meta:
  print("################## Creating a prduction for sim-slcio split: input meta is")
  pprint.pprint(meta)
  #######################
  ## Split the input files.
  psplit =  ILDProductionJobOpt2017()
  psplit.setDryRun(dryrun)
  psplit.setCPUTime(30000)
  psplit.setLogLevel("verbose")
  psplit.setProdType('Split')
  psplit.setInputSandbox(input_sand_box)
  res = psplit.setInputDataQuery(meta)
  if not res['OK']:
    print(res['Message'])
    exit(1)
  psplit.setOutputSE(SE_psplit)
  wname = process+"_"+str(energy)+"_split"
  wname += additional_name
  psplit.setWorkflowName(wname)
  psplit.setProdGroup(analysis+"_"+str(energy))

  #Add the application
  res = psplit.append(split)
  if not res['OK']:
    print(res['Message'])
    exit(1)
  psplit.addFinalization(True, True, True, True)
  descrp = "Splitting slcio files"
  if additional_name:
    descrp += ", %s"%additional_name
  psplit.setDescription(descrp)

  res = psplit.createProduction()
  if not res['OK']:
    print(res['Message'])

  res = psplit.finalizeProd()
  if not res['OK']:
    print(res['Message'])
    exit(1)
  #As before: get the metadata for this production to input into the next
  meta = psplit.getMetadata()

if ild_rec and meta:
  print("################## Creating a production for reconstruction WITHOUT overlay. Input meta is")
  pprint.pprint(meta)
  #######################
  #Define the reconstruction prod
  pma = ILDProductionJobOpt2017()
  pma.setDryRun(dryrun)
  pma.matchToInput = matchToInput_marlin
  pma.setILDConfig(ILDConfigRec)
  pma.setLogLevel("verbose")
  pma.setProdType('MCReconstruction_ILD')
  pma.setEvtType(my_evttype)
  pma.setReconstructionBasePaths(recPath, dstPath)
  pma.setProdPlugin('Limited')
#   pma.setInputSandbox(input_sand_box_marlin)
  pma.setOutputSandbox(["*.log", "*dd4hep.xml", "*.sh", "TaggingEfficiency.root", "PfoAnalysis.root"])

  res = pma.setInputDataQuery(meta)
  if not res['OK']:
    print(res['Message'])
    exit(1)

  pma.setOutputSE(SE_rec)
  wname = process+"_"+str(energy)+"_ild_rec"
  wname += additional_name
  pma.setWorkflowName(wname)
  pma.setProdGroup(analysis+"_"+str(energy))

  #Add the application
  res = pma.append(ma)
  if not res['OK']:
    print(res['Message'])
    exit(1)
  pma.addFinalization(True, True, True, True)
  descrp = "%s, No overlay" % detectorModel
  if additional_name:
    descrp += ", %s"%additional_name
  pma.setDescription(descrp)

  res = pma.createProduction()
  if not res['OK']:
    print(res['Message'])
  res = pma.finalizeProd()
  if not res['OK']:
    print(res['Message'])
    exit(1)
  pma.setNbOfTasks(nbtasks_rec)

if ild_rec_ov and meta:
  print("################## Creating a production for reconstruction WITH overlay. Input meta is")
  pprint.pprint(meta)
  #######################
  #Define the reconstruction prod
  pmao = ILDProductionJobOpt2017()
  pmao.matchToInput = matchToInput_marlin
  pmao.setDryRun(dryrun)
  pmao.setILDConfig(ILDConfigRec)
  pmao.setEvtClass(my_evtclass)
  # pmao.setUseSoftTagInPath(False)
  pmao.setEvtType(my_evttype)
  pmao.setLogLevel("verbose")
  pmao.setProdType('MCReconstruction_Overlay_ILD')
  pmao.setBannedSites(banned_sites)
  pmao.setOutputSandbox(["*.log", "*.sh", "*.xml"])
  pmao.setReconstructionBasePaths(recPath, dstPath)
  pmao.setProdPlugin('Limited')

  res = pmao.setInputDataQuery(meta)
  if not res['OK']:
    print(res['Message'])
    exit(1)
  pmao.setOutputSE(SE_rec)
  wname = process+"_"+str(energy)+"_ild_rec_overlay"
  wname += additional_name
  pmao.setWorkflowName(wname)
  pmao.setProdGroup(analysis+"_"+str(energy))

  #Add the application
  res = pmao.append(overlay)
  if not res['OK']:
    print(res['Message'])
    exit(1)
  #Add the application
  res = pmao.append(mao)
  if not res['OK']:
    print(res['Message'])
    exit(1)
  pmao.addFinalization(True, True, True, True)
  descrp = "%s, Overlay" % detectorModel

  if additional_name:
    descrp += ", %s"%additional_name
  pmao.setDescription(descrp)
  res = pmao.createProduction()
  if not res['OK']:
    print(res['Message'])

  res = pmao.setProcessIDInFinalPath()
  if not res['OK']:
    print(res['Message'])

  res = pmao.finalizeProd()
  if not res['OK']:
    print(res['Message'])
    exit(1)
  pmao.setNbOfTasks(nbtasks_rec_ov)
