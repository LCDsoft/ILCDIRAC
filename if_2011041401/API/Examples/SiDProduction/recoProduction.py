##
#  Convenience script for submitting lcsim jobs to the dirac production system
#  christian.grefe@cern.ch
##

from subprocess import call

print 'Getting production proxy ...'
#call( [ 'proxy-init' , '-g', 'ilc_prod' ] )


from DIRAC.Core.Base import Script
import sys

Script.registerSwitch( 'D:', 'description=', 'Short description of the workflow (default set by metadata)' )
Script.registerSwitch( 'g:', 'group=', 'Name of the production group (default set by metadata)' )
Script.registerSwitch( 'l:', 'lcsim=', 'LCSIM version to use (default 1.15-SNAPHOT)' )
Script.registerSwitch( 'm:', 'model=', 'Name of detector model to use (default clic_sid_cdr)' )
Script.registerSwitch( 'n', 'nocheck', 'Switches off additional check before submission' )
Script.registerSwitch( 'p:', 'prodid=', 'Production ID of input' )
Script.registerSwitch( 'P:', 'pandora=', 'SlicPandora version to use (default CDR0)' )
Script.registerSwitch( 't:', 'time=', 'CPU time limit per job in seconds (default 300000)' )
Script.registerSwitch( 'w:', 'workflow=', 'Name of the workflow (default set by metadata)' )

Script.setUsageMessage( sys.argv[0]+'-p <prodID> (<additional options>)' )

Script.parseCommandLine()
switches = Script.getUnprocessedSwitches()

detectorName = "clic_sid_cdr"
prodID = None
prodGroup = None
cpuLimit = 300000
lcsimVers = '1.15-SNAPSHOT'
pandoraVers = 'CDR0'
workflowName = None
workflowDescription = None
checkMeta = True

for switch in switches:
	opt = switch[0]
	arg = switch[1]
	if opt in ('D', 'description'):
		workflowDescription = arg
	elif opt in ('g', 'group'):
		prodGroup = arg
	elif opt in ('l', 'lcsim'):
		lcsimVers = arg
	elif opt in ('m', 'model'):
		detectorName = arg
	elif opt in ('p', 'prodid'):
		prodID = arg
	elif opt in ('P', 'pandora'):
		pandoraVers = arg
	elif opt in ('t', 'time'):
		cpuLimit = arg
	elif opt in ('v', 'version'):
		slicVersion = arg
	elif opt in ('w', 'workflow'):
		workflowName = arg
	elif opt in ('n', 'nocheck'):
		checkMeta = False
		
if prodID == None:
	Script.showHelp()
	sys.exit(2)	

from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
from ILCDIRAC.Interfaces.API.Production import Production
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager

# define the meta data just by production id
meta = {}
meta[ 'ProdID' ] = prodID

client = FileCatalogClient()
res = client.getCompatibleMetadata( meta )
if not res['OK']:
	self._reportError("Error looking up the catalog for metadata")
	sys.exit(2)

metaValues = res['Value']
if 'EvtType' not in metaValues:
	print 'Error in meta data for production', prodID, ': no event type defined.'
	sys.exit(2)
eventType = metaValues['EvtType'][0]
if 'Energy' not in metaValues:
	print 'Error in meta data for production', prodID, ': no energy defined.'
	sys.exit(2)
energy = metaValues['Energy'][0]
print 'Meta data for production', prodID, ':'
for key, value in metaValues.iteritems():
	if len(value) == 1:
		print '\t', key, ':', value[0]

if not prodGroup:
	prodGroup = eventType+'_'+energy+'_cdr'	
print 'Production group:', prodGroup

if not workflowName:
	workflowName = eventType+'_'+energy+'_rec_sid_cdr'	
print 'Workflow name:', workflowName

if not workflowDescription:
	workflowDescription = 'Reconstructing '+eventType+' at '+energy+', lcsim '+lcsimVers+', slicPandora '+pandoraVers+', '+detectorName
print 'Workflow description:', workflowDescription

print 'LCSim version:', lcsimVers
print 'SLICPandora version:', pandoraVers
print 'CPU limit:', cpuLimit, 'sec'

if checkMeta:
	answer = raw_input('Submit production? (Y/N): ')
	if not answer.lower() in ('y', 'yes'):
		sys.exit(2)


# additional files needed to run lcsim and pandora
lcsimPath = "/ilc/prod/software/lcsim/"
pandoraPath = "/ilc/prod/software/slicpandora/"
strategiesPath = "/ilc/prod/software/lcsim/trackingstrategies/"+detectorName+"/"

lcsimSteering1 = "defaultPrePandoraLcsim.xml"
lcsimSteering2 = "defaultPostPandoraLcsim.xml"
pandoraSettings = "PandoraSettingsSlic.xml"
strategies = "defaultStrategies.xml"

inputSandbox = [
	lcsimPath + lcsimSteering1,					# first lcsim step steering file
	lcsimPath + lcsimSteering1,					# second lcsim step steering file
	pandoraPath + pandoraSettings,			# pandora settings file, use default atm. because of dirac problem
	strategiesPath + strategies					# strategies file needed for track reconstruction
]

# check if input sandbox files exist
rm = ReplicaManager()
res = rm.getActiveReplicas( inputSandbox )

if not res['OK']:
	print "ERROR:", res['Message']
	sys.exit(2)
replicas = res['Value']['Successful']
noreplicas = res['Value']['Failed']

if len( noreplicas ):
	print 'Error: no replicas found for %s'%(noreplicas)
	sys.exit(2)

# make proper LFN path for job submission
inputSandbox = map(lambda x: "LFN:"+x, inputSandbox)

print "No errors found"
sys.exit(2)

# actual submission part
p = Production()
p.defineInputData( meta )

# First LCSim step - digitization and track reconstruction
p.addLCSIMStep( appVers=lcsimVers,
	steeringXML=lcsimSteering1,
	outputfile='prePandora.slcio',
	outputSE='CERN-SRM' )

# SLICPandora step - Particle Flow
p.addSLICPandoraStep( appVers=pandoraVers,
	detector=detectorName,
	pandorasettings=pandoraSettings,
	outputfile='pandora.slcio' )

# Second LCSim step - finalizing PID and creating REC and DST files
p.addLCSIMStep( appVers=lcsimVers,
	steeringXML=lcsimSteering2,
	outputRECfile='default',
	outputDSTfile='default',
	outputSE='CERN-SRM' )

p.addFinalizationStep( True, True , True , True )
p.setInputSandbox( inputSandbox )
p.setCPUTime( cpuLimit )
p.setOutputSandbox( [ "*.log" ] )
p.setWorkflowName( workflowName )
p.setWorkflowDescription( workflowDescription )
p.setProdType( "MCReconstruction" )
p.setProdGroup( prodGroup )
res = p.create()
if not res['OK']:
  print res['Message']
  sys.exit(1)
p.setInputDataQuery()
p.finalizeProdSubmission()
