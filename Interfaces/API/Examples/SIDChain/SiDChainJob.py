###
#  Convenience script for submitting jobs with full reconstruction chain (or parts of it) to DIRAC
#  christian.grefe@cern.ch
##

from DIRAC.Core.Base import Script
import sys

# default parameters
macroFile = 'slicMacros/defaultClicCrossingAngle.mac'
slicPandoraVer = 'CLIC_CDR'
lcsimVer = 'CLIC_CDR'
slicVer = 'v2r9p8'
detector = 'clic_sid_cdr'
jobTitle = 'fullReco'
inputFileList = None
nEvts = -1
nJobs = 1
settingsFile = ''
cpuLimit = 100000
mergeSlcioFiles = 1
slicPandoraPath = 'LFN:/ilc/prod/software/slicpandora/'
slicPandoraDetector = detector
lcsimPath = 'LFN:/ilc/prod/software/lcsim/'
systemConfig = 'x86_64-slc5-gcc43-opt'
storageElement = 'CERN-SRM'
aliasFile = 'alias.properties'
xmlFile = lcsimPath+'clic_cdr_prePandora.lcsim'
xmlPostPandora = lcsimPath+'clic_cdr_postPandoraOverlay.lcsim' # always use the overlay version to create the selected PFO files
lcsimTemplate = ''
strategyFile = 'defaultStrategies.xml'
banlistFile = 'bannedSites.py'
overlayBX = 0
overlayWeight = 3.2
maxFiles = -1
lfnlist = None
prodID = None
process = None
debug = True
agentMode = False

Script.registerSwitch( 'a:', 'alias=', 'name of the alias.properties file to use (default %s)'%(aliasFile) )
Script.registerSwitch( 'A', 'agent', 'Submits the job in agent mode, which will run the job on the local machine' )
Script.registerSwitch( 'b:', 'banlist=', 'file with list of banned sites (default %s)'%(banlistFile) )
Script.registerSwitch( 'D:', 'detector=', 'name of the detector model (default %s)'%(detector) )
Script.registerSwitch( 'i:', 'input=', 'input python script holding the lfnlist to process' )
Script.registerSwitch( 'I:', 'prodid=', 'use a production id to define the input lfnlist' )
Script.registerSwitch( 'l:', 'lcsimxml=', 'lcsim steering xml template (default %s)'%(lcsimPath+xmlFile) )
Script.registerSwitch( 'L:', 'lcsim=', 'lcsim version to use (default %s)'%(lcsimVer) )
Script.registerSwitch( 'j:', 'jobs=', 'number of jobs that each input file gets split into (default %s)'%(nJobs) )
Script.registerSwitch( 'm:', 'macro=', 'name of the macro file used for SLIC (default %s)'%(macroFile) )
Script.registerSwitch( 'M:', 'merge=', 'number of slcio input files used per job, only used if no slic step (default %s)'%(mergeSlcioFiles) )
Script.registerSwitch( 'n:', 'events=', 'number of events per job, -1 for all in file (default %s)'%(nEvts) )
Script.registerSwitch( 'O:', 'overlay=', 'number of bunch crossings overlaid over each event (default %s)'%(overlayBX) )
Script.registerSwitch( 'w:', 'overlayweight=', 'number of gg->hadron interactions per bunch crossing (default %s)'%(overlayWeight) )
Script.registerSwitch( 'p:', 'process=', 'process name to be used for naming of path etc.' )
Script.registerSwitch( 'P:', 'pandora=', 'slicPandora version to use (default %s)'%(slicPandoraVer) )
Script.registerSwitch( 'S:', 'slic=', 'slic version (default %s)'%(slicVer) )
Script.registerSwitch( 't:', 'time=', 'CPU time limit per job in seconds (default %s)'%(cpuLimit) )
Script.registerSwitch( 'T:', 'title=', 'job title (default %s)'%(jobTitle) )
Script.registerSwitch( 'v', 'verbose', 'switches off the verbose mode' )
Script.registerSwitch( 'x:', 'settings=', 'name of pandora settings file (default taken from grid installation)' )
Script.registerSwitch( 'y:', 'strategy=', 'name of tracking strategy file to use (default %s)'%(strategyFile) )
Script.registerSwitch( 'z:', 'maxfiles=', 'maximum number of files to process (default %s)'%(maxFiles) )
Script.registerSwitch( 'g:', 'pandoradetector=', 'name of pandora geometry xml file or just detector name (default %s)'%(maxFiles) )

Script.setUsageMessage( sys.argv[0]+'-n <nEvts> (-i <inputList> OR -I <prodID> AND/OR -m <macro>) (<additional options>)' )

Script.parseCommandLine()
switches = Script.getUnprocessedSwitches()

for switch in switches:
	opt = switch[0]
	arg = switch[1]
	if opt in ('a','alias'):
		aliasFile = arg
	if opt in ('A','agent'):
		agentMode = True
	if opt in ('b','banlist'):
		banlistFile = arg
	if opt in ('D','detector'):
		detector = arg
	if opt in ('i','input'):
		inputFileList = arg
	if opt in ('I','prodid'):
		prodID = arg
	if opt in ('l','lcsimxml'):
		lcsimTemplate = arg
	if opt in ('L','lcsim'):
		lcsimVer = arg
	if opt in ('j', 'jobs'):
		nJobs = int(arg)
	if opt in ('m','macro'):
		macroFile = arg
	if opt in ('M','merge'):
		mergeSlcioFiles = int(arg)
	if opt in ('n','events'):
		nEvts = int(arg)
	if opt in ('O','overlay'):
		overlayBX = int(arg)
	if opt in ('w','overlayweight'):
		overlayWeight = float(arg)
	if opt in ('p','process'):
		process = arg
	if opt in ('P','pandora'):
		slicPandoraVer = arg
	if opt in ('x','settings'):
		settingsFile = arg
	if opt in ('S','slic'):
		slicVer = arg
	if opt in ('t','time'):
		cpuLimit = arg
	if opt in ('T','title'):
		jobTitle = arg
	if opt in ('y','strategy'):
		strategyFile = arg
	if opt in ('z','maxfiles'):
		maxFiles = int(arg)
	if opt in ('v','verbose'):
		debug = False
	if opt in ('g','pandoradetector'):
		slicPandoraDetector = arg

if debug:
	print ''
	print '################################'
	print ' SiD job submission to DIRAC'
	print '        christian.grefe@cern.ch'
	print '################################'
	print ''

if not inputFileList and not prodID:
	if macroFile == 'slicMacros/default.mac':
		Script.showHelp()
		sys.exit(2)
		
if overlayBX > 0:
	if not lcsimTemplate:
		xmlFile = lcsimPath+'clic_cdr_prePandoraOverlay.lcsim'
	xmlPostPandora = lcsimPath+'clic_cdr_postPandoraOverlay.lcsim'


# helper function to replace strings in a textfile
def prepareFile( fileNameTemplate, fileNameOut, replacements ):
	f = open(fileNameTemplate)
	txt = f.read()
	counter = []
	for repkey,reprep in replacements:
		counter.append( ( repkey, reprep, txt.count(repkey) ) )
		txt = txt.replace( repkey, reprep )
	fOut = open( fileNameOut, "w" )
	fOut.write( txt )
	return counter

from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
from ILCDIRAC.Interfaces.API.ILCJob import ILCJob
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

fileCatalog = FileCatalogClient()

# list of files used as input to the job
inputSandbox = []

# tracking strategies
inputSandbox.append( 'LFN:/ilc/prod/software/lcsim/trackingStrategies/'+detector+'/'+strategyFile )

# JNI bindings for root writer
inputSandbox.append( 'lib' )

outputSandbox = [ "*.log", "*.xml", "*.lcsim" ]

# read file with list of banned sites
if banlistFile:
	f = open( banlistFile, 'r')
	exec(f.read())
if not bannedSites:
	bannedSites = ['']

if lcsimVer and overlayBX > 0:
	jobTitle += '_%sBXgghad'%(overlayBX)
repositoryFile = 'repositoryFiles/'+detector+'.'+jobTitle+'.'

# create a repository file and generate list of lfns to process
if prodID:      # use a production id to define the lfnlist
	meta = {}
	meta[ 'ProdID' ] = prodID
	print 'Using production ID to define LFN list:'
	result = fileCatalog.getCompatibleMetadata( meta )
	if not result['OK']:
		print "Error looking up the file catalog for metadata"
		sys.exit(2)
	metaData = result['Value']
	print '  Found production ID %s. Associated meta data:'%( prodID )
	for key, value in metaData.iteritems():
		if not len(value) == 0:
			print '    %s: %s'%( key, value[0] )
	if metaData['Datatype'][0] == 'SIM':
		slicVer = ''		# data has been simulated, can skip SLIC step
	if metaData['EvtType'] and not process:
		process = metaData['EvtType'][0]
	if slicVer and metaData['NumberOfEvents']:
		if nEvts < 1:
			nEvts = int(metaData['NumberOfEvents'][0])
	result = fileCatalog.findFilesByMetadata( meta )
	if not result['OK']:
		print "Error looking up the file catalog for metadata"
		sys.exit(2)
	lfnlist = map(lambda x: "LFN:"+x, result['Value'])	# need to add "LFN:" to each entry
	print '  Found %s files associated with meta data'%( len(lfnlist) )
	print ''
	repositoryFile += 'prod'+prodID+'.cfg'
elif inputFileList:     # read a file containing an lfnlist
	repositoryFile += inputFileList.split('/')[-1].replace('.py','.cfg')
	f = open( inputFileList, 'r')
	exec(f.read())
	if not lfnlist:
		print "Error no lfnlist in %s"%(inputFileList)
		sys.exit(2)
else:           # no input files, GEANT4 particle source should be defined in slic macro
	repositoryFile += macroFile.split('/')[-1].replace('.mac','.cfg')
	lfnlist = [ '' ]

dirac = DiracILC ( True , repositoryFile )

inputFiles = []
filesProcessed = 0
for inputFile in lfnlist:
	if filesProcessed == maxFiles:
		break
		
	filesProcessed += 1
	
	# processing multiple input files in a single job starting with lcsim
	if lcsimVer and not slicVer:
		inputFiles.append(inputFile)
		if len(inputFiles) < mergeSlcioFiles and filesProcessed != len(lfnlist):
			continue
		inputSlcios = inputFiles
		inputFiles = []
	
	if inputFile:
	
		if slicVer:
			outputFileBase = jobTitle+'_'+inputFile.split('/')[-1].replace( '.stdhep', '' )
			if not process:
				process = splitPath[ splitPath.index( 'gen' ) - 1]
		else:
			if not lcsimVer:
				inputSlcios = inputFile
				outputFileBase = jobTitle+'_'+inputSlcios.split('/')[-1].replace( '.slcio', '' )
			else:
				outputFileBase = jobTitle+'_'+inputSlcios[0].split('/')[-1].replace( '.slcio', '' )
			if not process:
				process = splitPath[-3]
	else:
		outputFileBase = jobTitle+'_'+macroFile.split('/')[-1].replace('.mac','_%s'%(nEvts))
		if not process:
			print 'ERROR: no process defined. Use -p <processName> to define the storage path'
			sys.exit(2)

	for job in xrange( nJobs ):
		
		outputFile = outputFileBase
		
		if not nJobs == 1:
			outputFile += '_%s'%(job)
		
		if lcsimVer:
			slicOutput='slic.slcio'
		else:
			slicOutput=outputFile+'.slcio'
		
		if slicPandoraVer:
			lcsimOutput='prePandora.slcio'
		else:
			lcsimOutput=outputFile+'.slcio'
		
		storagePath = detector+'/'+process+'/'+jobTitle

		outputData = []
		
		replacements = [
			( '__outputSlcio__', outputFile+'.slcio' ),
			( '__outputRoot__', outputFile+'.root' ),
			( '__outputAida__', outputFile+'.aida' ),
			( '__strategyFile__', strategyFile )
		]
		
		if lcsimTemplate:
			xmlFile = 'lcsimSteeringFiles/'+outputFile + '.xml'
			counter = prepareFile( lcsimTemplate, xmlFile, replacements )
			for key, rep, count in counter:
				if key.count('output') and count:
					outputData.append( rep )		# add to list of output files if key denotes some output and is present in template
		else:
			if slicPandoraVer and lcsimVer:
				outputData.append( outputFile+'_REC.slcio' )
				outputData.append( outputFile+'_DST.slcio' )
			else:
				outputData.append( outputFile+'.slcio' )		# default reco only creates one output file
		
		
		startEvt = job*nEvts
		
		job = ILCJob ( )
		
		if slicVer:
			if nEvts < 0:
				print 'ERROR: need to set number of events per job for SLIC. Use -n <nEvts> to set number of events.'
				sys.exit(2)
			res = job.setSLIC ( appVersion=slicVer ,
				detectorModel=detector ,
				macFile=macroFile ,
				inputGenfile=inputFile ,
				nbOfEvents=nEvts ,
				startFrom=startEvt ,
				outputFile=slicOutput
				)
			inputSlcios = '' # no inputSlcio for lcsim so it picks up its input from lcsim output
			if not res['OK']:
				print res['Message']
				sys.exit(2)
			
		if lcsimVer:
			if overlayBX > 0:
				if nEvts < 0:
					print 'ERROR: need to set number of events per job for overlay. Use -n <nEvts> to set number of events.'
					sys.exit(2)
				res = job.addOverlay( detector='SID',
					energy = '3tev',
					BXOverlay = overlayBX,
					NbGGtoHadInts = overlayWeight,
					NSigEventsPerJob = nEvts)
				if not res['OK']:
					print res['Message']
					sys.exit(2)
			res = job.setLCSIM ( lcsimVer ,
				xmlfile=xmlFile ,
				aliasproperties=aliasFile ,
				evtstoprocess=nEvts,
				inputslcio=inputSlcios,
				outputFile=lcsimOutput
				)
			inputSlcios = '' # no inputSlcio for slicPandora so it picks up its input from lcsim output
			if not res['OK']:
				print res['Message']
				sys.exit(2)
		
		if slicPandoraVer:
			res = job.setSLICPandora ( appVersion=slicPandoraVer ,
				detectorgeo=slicPandoraDetector ,
				inputslcio=inputSlcios ,
				pandorasettings=settingsFile ,
				nbevts=nEvts ,
				outputFile=outputFile+'.slcio'
				)
			if not res['OK']:
				print res['Message']
				sys.exit(2)
			if lcsimVer:
				res = job.setLCSIM ( lcsimVer ,
					xmlfile=xmlPostPandora ,
					aliasproperties=aliasFile ,
					evtstoprocess=nEvts,
					outputRECFile=outputFile+'_REC.slcio',
					outputDSTFile=outputFile+'_DST.slcio'
					)
				if not res['OK']:
					print res['Message']
					sys.exit(2)
		
		job.setOutputSandbox ( outputSandbox )
		job.setInputSandbox ( inputSandbox )
		job.setOutputData ( outputData, storageElement, storagePath )	
		job.setCPUTime( cpuLimit )
		job.setSystemConfig ( systemConfig )
		job.setName ( detector+"_"+process+"_"+jobTitle )
		job.setBannedSites( bannedSites )
		#job.setDestination('LCG.CERN.ch')
		
		if debug:
			print ''
			print 'Jobs to submit:'
			nTotal = len(lfnlist)*nJobs/mergeSlcioFiles
			if inputFile:
				print '  Number of input files:', len(lfnlist)
				if maxFiles > 0:
					print '  Maximum input files to use:', maxFiles
					nTotal = maxFiles*nJobs/mergeSlcioFiles
				if mergeSlcioFiles > 1:
					print '  Merged input files per job:', mergeSlcioFiles
			if nJobs != 1:
				print '  Jobs per input file:', nJobs
			if nEvts < 0:
				print '  Events per job: all'
			else :
				print '  Events per job:', nEvts
			print '  Total number of jobs:', nTotal
			print '  Maximum CPU time per job:', cpuLimit, 'sec'
			print ''
			
			print 'General parameters:'
			
			print '  Detector model:', detector
			print '  Process name:', process
			print '  Job title:', jobTitle
			print '  Banned sites:', bannedSites
			print '  Repository file:', repositoryFile
			print ''
			
			print 'Files:'
			print '  Input sand box:', inputSandbox
			print '  Output sand box:', outputSandbox
			print '  Output data:', outputData
			print '  Output storage path:', storagePath
			print '  Output storage element:', storageElement
			print ''
			
			print 'Steps executed:'
			step = 0
			if slicVer:
				step += 1
				print '  %s) Slic step:'%(step)
				print '    Slic version:', slicVer
				print '    Macro file:', macroFile
				print ''
			
			if lcsimVer:
				step += 1
				print '  %s) LCSim step:'%(step)
				print '    LCSim version:', lcsimVer
				print '    LCSim file:', xmlFile
				print '    Tracking strategies:', strategyFile
				print '    Detector alias file:', aliasFile
				if overlayBX > 0:
					print '    Number of bunch crossings overlayed:', overlayBX
					print '    gg->hadron events per bunch crossing:', overlayWeight
				print ''
			
			if slicPandoraVer:
				step += 1
				print '  %s) SlicPandora step:'%(step)
				print '    SlicPandora version:', slicPandoraVer
				mySettingsFile = settingsFile
				if not settingsFile:
					mySettingsFile = 'default'
				print '    Pandora settings file:', mySettingsFile
				print '    Pandora detector file:', slicPandoraDetector
				print ''
				if lcsimVer:
					step += 1
					print '  %s) LCSim step:'%(step)
					print '    LCSim version:', lcsimVer
					print '    LCSim file:', xmlPostPandora
					print '    Detector alias file:', aliasFile
					print ''

			answer = raw_input('Proceed and submit job(s)? (Y/N): ')
			if not answer.lower() in ('y', 'yes'):
				sys.exit(2)
			else:
				# no more debug output in further loops
				debug = False
		if not agentMode:
			#print "submitting"
			dirac.submit ( job )
		else:
			dirac.submit ( job, mode="Agent" )

