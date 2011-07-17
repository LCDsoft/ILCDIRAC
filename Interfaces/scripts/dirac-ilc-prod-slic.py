##
#  Convenience script for submitting slic jobs to the dirac production system
#  christian.grefe@cern.ch
##

from DIRAC.Core.Base import Script
import sys

Script.registerSwitch( 'd:', 'description=', 'Short description of the workflow (default set by metadata)' )
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
prodGroup = None
cpuLimit = 300000
slicVersion = 'v2r8p4'
workflowName = None
workflowDescription = None
checkMeta = True

for switch in switches:
	opt = switch[0]
	arg = switch[1]
	if opt in ('d', 'description'):
		workflowDescription = arg
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

from ILCDIRAC.Core.Utilities.CheckAndGetProdProxy import CheckAndGetProdProxy
res = CheckAndGetProdProxy()
if not res['OK']:
  sys.exit(2)

from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
from ILCDIRAC.Interfaces.API.Production import Production
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC import S_ERROR, S_OK

# define the meta data just by production id
meta = {}
meta[ 'ProdID' ] = prodID

client = FileCatalogClient()
res = client.getCompatibleMetadata( meta )
if not res['OK']:
	print "Error looking up the catalog for metadata"
	sys.exit(2)

metaValues = res['Value']
print 'Meta data for production', prodID, ':'
for key, value in metaValues.iteritems():
	if len(value) == 1:
		print '\t', key, ':', value[0]

eventType = metaValues['EvtType'][0]
energy = metaValues['Energy'][0]

if not prodGroup:
	prodGroup = eventType+'_'+energy+'_cdr'	
print 'Production group:', prodGroup

if not workflowName:
	workflowName = eventType+'_'+energy+'_sim_sid_cdr'	
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
p.addSLICStep( appVers=slicVersion, inputmac="default.mac", detectormodel=detectorName, outputSE="CERN-SRM" )
p.addFinalizationStep( True, True , True , True )
p.setInputSandbox( [ "LFN:/ilc/prod/software/slic/default.mac" ] )		#need to pass somehow the input steering files
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

		
