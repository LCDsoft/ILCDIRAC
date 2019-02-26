'''
Created on Feb 8, 2012
Modified on Aug 17, 2012

:author: Stephane Poss, Christian Grefe
'''

from __future__ import print_function
from DIRAC.Core.Base import Script
import sys

# default values for the parameters
prodID = None
process = None
dataType = 'GEN'
machine = 'ilc'
detectorType = 'sid'
detectorModel = 'sidloi3'
polarisation = None # can only be 'p80m20' or 'm80p20' in the DBD context
nEvtsPerJob = 100
energy = 1000
nameAppendix = ''
groupPrefix = 'SiD_DBD'
slicVersion = 'v3r0p3' # has to exist in the DIRAC configuration
stdhepSplitVersion = 'V2' # has to exist in the DIRAC configuration
slicMacro = 'defaultIlcCrossingAngle.mac' # has to exist in the default steering files tarball available in DIRAC
outputSRM = 'RAL-SRM'
replicaSRMs = [ "SLAC-SRM", "FNAL-SRM" ]


# definitions for command line parsing
Script.registerSwitch( 'D:', 'detector=', 'detector model used for the simulation (default: %s)' % ( detectorModel ) )
Script.registerSwitch( 'e:', 'energy=', 'energy used to generate input files (default: %s)' % ( energy ) )
Script.registerSwitch( 'i:', 'prodid=', 'production id of the input files' )
Script.registerSwitch( 'm:', 'machine=', 'machine used to generate input files (default: %s)' % ( machine ) )
Script.registerSwitch( 'n:', 'nevents=', 'maximum number of events per job (default: %s)' % ( nEvtsPerJob ) )
Script.registerSwitch( 'N:', 'name=', 'appended to the default production name' )
Script.registerSwitch( 'p:', 'process=', 'event type of the input files' )
Script.registerSwitch( 'P:', 'polarisation=', 'polarisation used to generate input files (default: %s)' % ( polarisation ) )
Script.registerSwitch( 'S:', 'storage=', 'storage element used to save the output files (default: %s)' % ( outputSRM ) )
Script.registerSwitch( 'V:', 'slicversion=', 'slic version used in the simulation (default: %s)' % ( slicVersion ) )

Script.setUsageMessage( sys.argv[0] + '-i <prodID> -p <process> -P <polarisation>' )

Script.parseCommandLine()

# process the parsed switches
switches = Script.getUnprocessedSwitches()
for switch in switches:
  opt = switch[0]
  arg = switch[1]
  if opt in ( 'D', 'detector' ):
    detectorModel = arg
  if opt in ( 'e', 'energy' ):
    energy = arg
  if opt in ( 'i', 'prodid' ):
    prodID = arg
  if opt in ( 'm', 'machine' ):
    machine = arg
  if opt in ( 'n', 'nevents' ):
    nEvtsPerJob = arg
  if opt in ( 'N', 'name' ):
    nameAppendix = arg
  if opt in ( 'p', 'process' ):
    process = arg
  if opt in ( 'P', 'polarisation' ):
    polarisation = arg
  if opt in ( 'S', 'storage' ):
    storage = arg
  if opt in ( 'V', 'slicversion' ):
    slicversion = arg        

from ILCDIRAC.Interfaces.API.NewInterface.SIDProductionJob import SIDProductionJob
from ILCDIRAC.Interfaces.API.NewInterface.Applications     import SLIC, StdHepSplit

###As it's a full chain, we start at generation
##so we need to define the process and the energy
## The rest will be set later. We could also set the process 
##and the energy directly in the whizard def, but for clarity
## it's better to do it before, that way we know the very 
##essential

if not prodID or not process or not polarisation:
  print('\tNeed to define at least production ID, process name and polarisation name.')
  print('\tUse -h for help.')
  sys.exit(2)

# define a meta data dictionary for lookup in the file catalog
meta = {}
meta['ProdID'] = prodID
meta['EvtType'] = process
#meta['Energy'] = energy
meta['Datatype'] = dataType
meta['Machine'] = machine
meta['DetectorType'] = detectorType
meta['Polarisation'] = polarisation

# give feedback on the meta data
print('Creating production using meta data query:')
for key in meta:
    print('  %s:\t%s' % (key, meta[key]))

# confirm the meta data query
res = ''
while res not in [ 'y', 'Y' ]:
  res = raw_input('\nContinue? (y/n)\t')
  if res in [ 'n', 'N' ]:
    sys.exit(0)

# Do Split
activesplitstdhep = True

# Do Sim
sid_sim = True

# Do Replication
replicateFiles = True

## Split
stdhepsplit = StdHepSplit()
stdhepsplit.setVersion( stdhepSplitVersion )
stdhepsplit.setNumberOfEventsPerFile( nEvtsPerJob )

## Simulation 
slic = SLIC()
slic.setVersion( slicVersion )
slic.setSteeringFile( slicMacro )
slic.setDetectorModel( detectorModel )
slic.setNumberOfEvents( nEvtsPerJob )

############################################
#What is below WILL NEVER NEED TO BE TOUCHED 
#(I'm not kidding, if you touch and break, not my problem)
#
# Define production step splitting the stdhep files
if activesplitstdhep and meta:
  pstdhepsplit =  SIDProductionJob()
  pstdhepsplit.setLogLevel( 'verbose' )
  pstdhepsplit.setProdType( 'Split' )
  res = pstdhepsplit.setInputDataQuery(meta)
  if not res['OK']:
    print(res['Message'])
    exit(1)
  pstdhepsplit.setOutputSE( outputSRM )
  wname = '%s_%s_%s_split' % (process, energy, polarisation )
  if nameAppendix:
    wname += '_%s' % ( nameAppendix )  
  pstdhepsplit.setWorkflowName(wname)
  pstdhepsplit.setProdGroup( '%s_%s_%s' % ( groupPrefix, process, energy ) )
  
  #Add the application
  res = pstdhepsplit.append(stdhepsplit)
  if not res['OK']:
    print(res['Message'])
    exit(1)
  pstdhepsplit.addFinalization( True, True, True, True )
  descrp = 'Splitting stdhep files'
  if nameAppendix:  
    descrp += ', %s' % ( nameAppendix )
  pstdhepsplit.setDescription(descrp)  
  
  res = pstdhepsplit.createProduction()
  if not res['OK']:
    print(res['Message'])
  res = pstdhepsplit.finalizeProd()
  if not res['OK']:
    print(res['Message'])
    exit(1)
  # get the metadata for this production to define input for the next step
  meta = pstdhepsplit.getMetadata()
  

# Define a production step for the simulation
if sid_sim and meta:
  psl = SIDProductionJob()
  psl.setLogLevel( 'verbose' )
  psl.setProdType( 'MCSimulation' )
  res = psl.setInputDataQuery( meta )
  if not res['OK']:
    print(res['Message'])
    exit(1)
  psl.setOutputSE( outputSRM )
  wname = '%s_%s_%s_%s_sim' % ( process, energy, polarisation, detectorModel )
  if nameAppendix:
    wname += '_%s' % (nameAppendix)
  psl.setWorkflowName(wname)
  psl.setProdGroup( '%s_%s_%s' % ( groupPrefix, process, energy ) )
  #Add the application
  res = psl.append(slic)
  if not res['OK']:
    print(res['Message'])
    exit(1)
  psl.addFinalization( True, True, True, True )
  descrp = '%s model' % ( detectorModel )
  if nameAppendix:  
    descrp += ', %s' % ( nameAppendix )
  psl.setDescription(descrp)

  res = psl.createProduction()
  if not res['OK']:
    print(res['Message'])
  res = psl.finalizeProd()
  if not res['OK']:
    print(res['Message'])
    exit(1)
  # get the metadata for this production to define input for the next step
  meta = psl.getMetadata()


from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

# Define transformation steps for the replication of the output data
if replicateFiles and meta:
  Trans = Transformation()
  Trans.setTransformationName( 'replicate_%s_%s_%s_%s' % ( process, energy, polarisation, meta['Datatype'] ) )
  description = 'Replicate %s %s %s %s to' % ( process, energy, polarisation, meta['Datatype'] )
  for replicaSRM in replicaSRMs:
    description += ' %s,' % ( replicaSRM )
  description.rstrip( ',' )
  Trans.setDescription( description )
  Trans.setLongDescription( description )
  Trans.setType( 'Replication' )
  Trans.setPlugin( 'Broadcast' )
  Trans.setSourceSE( outputSRM )
  Trans.setTargetSE( replicaSRMs )

  res = Trans.addTransformation()
  if not res['OK']:
    print(res)
    sys.exit(0)
  print(res)
  Trans.setStatus( 'Active' )
  Trans.setAgentType( 'Automatic' )
  currtrans = Trans.getTransformationID()['Value']
  client = TransformationClient()
  res = client.createTransformationInputDataQuery( currtrans, meta )
  print(res['OK'])

