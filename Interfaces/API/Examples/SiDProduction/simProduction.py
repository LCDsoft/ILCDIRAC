##
#  Convenience script for submitting slic jobs to the dirac production system
#  christian.grefe@cern.ch
##

from DIRAC.Core.Base import Script
import sys, types
import os.path

Script.registerSwitch( 'D:', 'description=', 'Short description of the workflow (default set by metadata)' )
Script.registerSwitch( 'e:', 'evttype=', 'Name of the production event type (optional in addition to production ID)' )
Script.registerSwitch( 'E:', 'energy=', 'Energy of the production events (default 3tev)' )
Script.registerSwitch( 'm:', 'model=', 'Name of detector model to use' )
Script.registerSwitch( 'n', 'nocheck', 'Switches off additional check before submission' )
Script.registerSwitch( 'g:', 'group=', 'Name of the production group (default set by metadata)' )
Script.registerSwitch( 'p:', 'prodid=', 'Production ID of input' )
Script.registerSwitch( 't:', 'time=', 'CPU time limit per job in seconds (default 300000)' )
Script.registerSwitch( 'v:', 'version=', 'SLIC version to use (default v2r8p4)' )
Script.registerSwitch( 'w:', 'workflow=', 'Name of the workflow (default set by metadata)' )

Script.setUsageMessage( sys.argv[0]+' -m <detectorModel> -p <prodID> (<additional options>)' )

Script.parseCommandLine()
switches = Script.getUnprocessedSwitches()

detectorName = None
prodID = None
eventType = None
dataType = ''
energy = ''
prodGroup = None
cpuLimit = 300000
slicVersion = 'v2r8p4'
workflowName = None
workflowDescription = None
checkMeta = True
slicMacro = 'defaultClicCrossingAngle.mac'

for switch in switches:
	opt = switch[0]
	arg = switch[1]
	if opt in ('D', 'description'):
		workflowDescription = arg
	elif opt in ('e', 'evttype'):
		eventType = arg
	elif opt in ('m', 'model'):
		detectorName = arg
	elif opt in ('g', 'group'):
		prodGroup = arg
	elif opt in ('p', 'prodid'):
		prodID = arg
	elif opt in ('t', 'time'):
		cpuLimit = arg
	elif opt in ('v', 'version'):
		slicVersion = arg
	elif opt in ('w', 'workflow'):
		workflowName = arg
	elif opt in ('n', 'nocheck'):
		checkMeta = False
	
if (detectorName == None) or (prodID == None):
	Script.showHelp()
	sys.exit(2)	

from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
from ILCDIRAC.Interfaces.API.Production import Production
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from subprocess import call

#print 'Getting production proxy ...'
#call( [ 'proxy-init' , '-g', 'ilc_prod' ] )

# define the meta data querry
meta = {}
meta[ 'ProdID' ] = prodID
if eventType:
	meta[ 'EvtType' ] = eventType
if energy:
	meta[ 'Energy'] = energy
if dataType:
	meta[ 'Datatype' ] = dataType

client = FileCatalogClient()
res = client.getCompatibleMetadata( meta )
if not res['OK']:
	self._reportError("Error looking up the catalog for metadata")
	sys.exit(2)

metaValues = res['Value']
#print metaValues

print 'Meta data for production', prodID, ':'
for key, value in metaValues.iteritems():
	if not len(value) == 0:
		print '\t', key, ':', value[0]

if dataType:
	print '\t', 'Datatype :', dataType

if not eventType:
	eventType = metaValues['EvtType'][0]
else:
	print '\t', 'EvtType :', eventType
	
if not energy:
	energy = metaValues['Energy'][0]
else:
	print '\t', 'Energy :', energy

if not prodGroup:
	prodGroup = eventType+'_'+energy+'_cdr'	
print 'Production group:', prodGroup

if not workflowName:
	workflowName = eventType+'_'+energy+'_sim_sid_cdr_2'	
print 'Workflow name:', workflowName

if not workflowDescription:
	workflowDescription = 'Simulating '+eventType+' at '+energy+', SLIC '+slicVersion+', '+detectorName
print 'Workflow description:', workflowDescription

print 'SLIC version:', slicVersion
print 'CPU limit:', cpuLimit, 'sec'

if checkMeta:
	answer = raw_input('Submit production? (Y/N): ')
	if not answer.lower() in ('y', 'yes'):
		sys.exit(2)

# actual submission part
p = Production()
p.defineInputData( meta )
p.addSLICStep( appVers=slicVersion,
	inputmac=slicMacro,
	detectormodel=detectorName,
	outputSE="CERN-SRM" )
p.addFinalizationStep( True, True , True , True )
p.setInputSandbox( [ "LFN:/ilc/prod/software/slic/"+slicMacro ] )		#need to pass somehow the input steering files
p.setCPUTime( cpuLimit )
p.setOutputSandbox( [ "*.log" ] )
p.setWorkflowName( workflowName )
p.setWorkflowDescription( workflowDescription )
p.setProdType( "MCSimulation" )
p.setProdGroup( prodGroup )
res = p.create()
if not res['OK']:
  print res['Message']
  sys.exit(1)
p.setInputDataQuery()
p.finalizeProdSubmission()

		
