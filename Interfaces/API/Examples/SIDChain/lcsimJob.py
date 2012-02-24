###
#  Convenience script for submitting lcsim jobs to DIRAC
#  christian.grefe@cern.ch
###

from DIRAC.Core.Base 				import Script
from DIRAC.Core.Security.Misc		import getProxyInfo

import sys, os, types

# default parameters
lcsimVer = 'CLIC_CDR'
detector = 'clic_sid_cdr'
inputFileList = None
nEvts = -1
cpuLimit = 100000
mergeSlcioFiles = 1
slicPandoraDetector = detector
systemConfig = 'x86_64-slc5-gcc43-opt'
storageElement = 'CERN-SRM'
aliasFile = 'alias.properties'
lcsimTemplate = ''
strategyFile = 'defaultStrategies.xml'
banlistFile = 'bannedSites.py'
maxFiles = -1
jobTitle = ''
jarFile = ''
outputPath = ''
replaceFiles = False
recFiles = False
lfnlist = None
prodID = None
eventType = ''
debug = True
agentMode = False

# register parameters with dirac
Script.registerSwitch( 'a:', 'alias=', 'name of the alias.properties file to use (default %s)'%(aliasFile) )
Script.registerSwitch( 'A', 'agent', 'Submits the job in agent mode, which will run the job on the local machine' )
Script.registerSwitch( 'b:', 'banlist=', 'file with list of banned sites (default %s)'%(banlistFile) )
Script.registerSwitch( 'D:', 'detector=', 'name of the detector model (default %s)'%(detector) )
Script.registerSwitch( 'e:', 'eventType=', 'the name of the event type (taken from meta data if production ID is given)' )
Script.registerSwitch( 'f:', 'files=', 'maximum number of files to process (default %s)'%(maxFiles) )
Script.registerSwitch( 'i:', 'input=', 'input python script holding the lfnlist to process' )
Script.registerSwitch( 'J:', 'jar=', 'jar file which will be added to the input sand box' )
Script.registerSwitch( 'l:', 'lcsimxml=', 'lcsim steering xml template' )
Script.registerSwitch( 'L:', 'lcsim=', 'lcsim version to use (default %s)'%(lcsimVer) )
Script.registerSwitch( 'M:', 'merge=', 'number of slcio input files processed per job (default %s)'%(mergeSlcioFiles) )
Script.registerSwitch( 'n:', 'events=', 'number of events per job, -1 for all in file (default %s)'%(nEvts) )
Script.registerSwitch( 'O:', 'outputpath=', 'storage path in the file catalog (default is "/detectorName/eventType/jobTitle")' )
Script.registerSwitch( 'p:', 'prodid=', 'use a production id to define the input lfnlist' )
Script.registerSwitch( 'r', 'recfiles', 'use REC files instead of DST in case of production input.' )
Script.registerSwitch( 'R', 'replace', 'remove output files that already exist before job submission (default %s)'%(replaceFiles) )
Script.registerSwitch( 'S:', 'storageelement=', 'storage element used for output data (default %s)'%(storageElement) )
Script.registerSwitch( 't:', 'time=', 'CPU time limit per job in seconds (default %s)'%(cpuLimit) )
Script.registerSwitch( 'T:', 'title=', 'job title (default steering file name)' )
Script.registerSwitch( 'v', 'verbose', 'switches off the verbose mode' )
Script.registerSwitch( 'y:', 'strategy=', 'name of tracking strategy file to use (default %s)'%(strategyFile) )

Script.setUsageMessage( 'Required parameters:\n' +
					    '  %s -i <inputFileList> -l <lcsimSteeringFile> (-e <evtType> OR -O <outputPath>)\n'%(sys.argv[0]) +
					    '  %s -p <prodID> -l <lcsimSteeringFile>\n'%(sys.argv[0]) )

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
	if opt in ('e','eventtype'):
		eventType = arg
	if opt in ('D','detector'):
		detector = arg
	if opt in ('f','files'):
		maxFiles = int(arg)
	if opt in ('i','input'):
		inputFileList = arg
	if opt in ('J','jar'):
		jarFile = arg
	if opt in ('l','lcsimxml'):
		lcsimTemplate = arg
	if opt in ('L','lcsim'):
		lcsimVer = arg
	if opt in ('M','merge'):
		mergeSlcioFiles = int(arg)
	if opt in ('n','events'):
		nEvts = int(arg)
	if opt in ('O','outputpath'):
		outputPath = arg
	if opt in ('p','prodid'):
		prodID = arg
	if opt in ('r','recfiles'):
		recFiles = True
	if opt in ('R','override'):
		replaceFiles = True
	if opt in ('S','storageelement'):
		storageElement = arg
	if opt in ('t','time'):
		cpuLimit = int(arg)
	if opt in ('T','title'):
		jobTitle = arg
	if opt in ('v','verbose'):
		debug = False
	if opt in ('y','strategy'):
		strategyFile = arg
		
if debug:
	print ''
	print '################################'
	print ' LCSim job submission to DIRAC'
	print '        christian.grefe@cern.ch'
	print '################################'
	print ''
	
if not inputFileList and not prodID:
	Script.showHelp()
	sys.exit(2)

if not lcsimTemplate:
	Script.showHelp()
	sys.exit(2)
	
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

# get base user path from proxy information
result = getProxyInfo()
if not result[ 'OK' ]:
	print 'ERROR: unable to retrieve proxy information: %s'%(result['Message'])
	sys.exit(2)
userName = result[ 'Value' ]['username']
vomsList = result[ 'Value' ]['VOMS']
vo = vomsList[0]
if not len(vomsList) == 1:
	print 'Available VOs:'
	counter = 0
	for voms in vomsList:
		counter += 1
		print '%s : %s'%(counter, voms)
	answer = -1
	while (answer < 0 and answer > counter):
		answer = int(raw_input('Please select VO to use (1 - %d): '%(counter)))
	vo = vomsList[answer-1]
baseUserPath = '%s/user/%s/%s/'%(vo, userName[0], userName)

from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
from ILCDIRAC.Interfaces.API.ILCJob import ILCJob
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

fileCatalog = FileCatalogClient()

# prepare some directories
repoDir = 'repositoryFiles/'
if not os.path.isdir( repoDir ):
	os.mkdir( repoDir )
steeringDir = 'lcsimSteeringFiles/'
if not os.path.isdir( steeringDir ):
	os.mkdir( steeringDir )

# create a job title from the steering file name
if not jobTitle:
	jobTitle = lcsimTemplate.split( '/' )[ -1 ]
	jobTitle = jobTitle.replace( '.xml', '' )
	jobTitle = jobTitle.replace( '.lcsim', '' )
	jobTitle = jobTitle.replace( '_template', '' )

# repository file to store the submitted job IDs
repositoryFile = repoDir + '%s.%s.'%(detector, jobTitle)

# use a production ID to define the input data
if prodID:
	meta = {}
	meta[ 'ProdID' ] = prodID
	if recFiles:
		meta[ 'Datatype' ] = 'REC'
	else:
		meta[ 'Datatype' ] = 'DST'
	
	# find the files belonging to the production ID
	result = fileCatalog.findFilesByMetadata( meta )
	if not result['OK']:
		print 'Error looking up the file catalog for metadata: %s'%(result['Message'])
		sys.exit( 2 )
	lfnlist = result[ 'Value' ]
	print 'Found %s files with production ID %s and associated meta data:'%( len(lfnlist), prodID )
	# find other meta data associated with the production ID
	result = fileCatalog.getDirectoryMetadata( lfnlist[0] )
	if not result['OK']:
		print 'Error looking up the file catalog for metadata: %s'%(result['Message'])
		sys.exit( 2 )
	foundMeta = result[ 'Value' ]
	for key, value in foundMeta.iteritems():
		print '  %s: %s'%(key, value)
	if foundMeta[ 'EvtType' ] and not eventType:
		eventType = foundMeta[ 'EvtType' ]
	if foundMeta['NumberOfEvents'] and nEvts < 1:
		nEvts = int(foundMeta['NumberOfEvents'])
	print ''
	repositoryFile += 'prod'+prodID+'.cfg'

# use a list of LFNs as input data
elif inputFileList:
	repositoryFile += inputFileList.split('/')[-1].replace('.py','.cfg')
	f = open( inputFileList, 'r')
	exec(f.read())
	if not lfnlist or not type(lfnlist) == types.ListType:
		print "ERROR: no lfnlist in %s."%(inputFileList)
		sys.exit(2)
	if not eventType and not outputPath:
		print "ERROR: no event type or output path defined. Use -e <eventType> or -O <outputPath>."
		sys.exit(2)
	print 'Found %s files in %s.'%(len(lfnlist), inputFileList)
	
# define storage path
if not outputPath:
	outputPath = '%s/%s/%s/'%(detector, eventType, jobTitle)
else:
	correctedPath = ''
	for item in outputPath.split('/'):
		correctedPath += item+'/'
	outputPath = correctedPath

# sandboxes
inputSandbox = []
outputSandbox = [ "*.log", "*.xml", "*.lcsim" ]
# tracking strategies
if os.path.isfile( strategyFile ):
	inputSandbox.append( strategyFile )
else:
	inputSandbox.append( 'LFN:/ilc/prod/software/lcsim/trackingStrategies/'+detector+'/'+strategyFile )
# JNI bindings for root writer
inputSandbox.append( 'lib' )
# jar file
if jarFile:
	inputSandbox.appen( jarFile )

# read file with list of banned sites
if banlistFile:
	f = open( banlistFile, 'r')
	exec(f.read())
if not bannedSites:
	bannedSites = ['']
	
dirac = DiracILC ( True , repositoryFile )

jobs = []
inputFiles = []

allInputFiles = []
allOutputFiles = []

# create a list of jobs: group input files if applicable and determine corresponding output file names
filesProcessed = 0
for file in lfnlist:
	if filesProcessed == maxFiles:
		break
	filesProcessed += 1
	
	# remove LFN prefix, will be added again later
	file = file.replace( 'LFN:', '' )
	
	# merge multiple input files in a single job
	inputFiles.append( file )
	if len(inputFiles) < mergeSlcioFiles and filesProcessed != len(lfnlist):
		continue
	inputData = inputFiles
	inputFiles = []
	
	outputFileBase = jobTitle + '_' + inputData[0].split('/')[-1].replace( '.slcio', '' )
	outputData = []
	
	# strings that are automatically replaced within the lcsim steering file
	replacements = [
				( '__outputSlcio__', outputFileBase+'.slcio' ),
				( '__outputRoot__', outputFileBase+'.root' ),
				( '__outputAida__', outputFileBase+'.aida' ),
				( '__outputDat__', outputFileBase+'.dat' ),
				( '__outputTxt__', outputFileBase+'.txt' ),
				( '__strategyFile__', strategyFile )
	]
	
	xmlFile = steeringDir + outputFileBase + '.xml'
	counter = prepareFile( lcsimTemplate, xmlFile, replacements )
	for key, replacement, count in counter:
		# add to list of output files if key denotes some output and is present in template
		if key.count('output') and count:
			outputData.append( replacement )
	
	allInputFiles.extend( inputData )
	allOutputFiles.extend( outputData )
	jobs.append( {'xmlFile' : xmlFile, 'inputData' : inputData, 'outputData' : outputData, 'submit' : True } )

# check existence of input files
result = fileCatalog.isFile( allInputFiles )
if not result['OK']:
	print 'Error looking up the file catalog for metadata: %s'%(result['Message'])
	sys.exit( 2 )
checkedInputFiles = result[ 'Value' ][ 'Successful' ]

# check existence of output files
allOutputFiles = map(lambda x: baseUserPath+outputPath+x, allOutputFiles)
result = fileCatalog.isFile( allOutputFiles )
if not result['OK']:
	print 'Error looking up the file catalog for metadata: %s'%(result['Message'])
	sys.exit( 2 )
checkedOutputFiles = result[ 'Value' ][ 'Successful' ]
# append those files that did not make it through the query as non existing
for file in allOutputFiles:
	if not checkedOutputFiles.has_key( file ):
		checkedOutputFiles[ file ] = False

# check job sanity based on existance of input and output files
removeFiles = []
missingInputFiles = 0
existingOutputFiles = 0
skippedJobs = 0
for job in jobs:
	xmlFile = job['xmlFile']
	inputData = job['inputData']
	outputData = job['outputData']
	
	for file in inputData:
		if not checkedInputFiles[ file ]:
			# input file does not exist: skip job
			job['submit'] = False
			missingInputFiles += 1
	
	if job['submit']:
		for file in outputData:
			file = baseUserPath+outputPath+file
			if checkedOutputFiles[ file ]:
				# output file does exist
				if replaceFiles:
					removeFiles.append[ file ]
				else:
					job['submit'] = False
					existingOutputFiles += 1
	if not job['submit']:
		skippedJobs += 1
	# prepend 'LFN:' in order to properly use it as input data
	job['inputData'] = map(lambda x: "LFN:"+x, job['inputData'])

nTotalJobs = len(jobs) - skippedJobs
	
# give some feedback to the user before job submission
if debug:
	print 'Jobs to submit:'
	print '  Number of input files:', len(lfnlist)
	if maxFiles > 0:
		print '  Maximum input files to use:', maxFiles
	if mergeSlcioFiles > 1:
		print '  Merged input files per job:', mergeSlcioFiles
	if nEvts < 0:
		print '  Events per job: all'
	else :
		if mergeSlcioFiles > 1:
			print '  Events per job:', nEvts*mergeSlcioFiles
		else:
			print '  Events per job:', nEvts
	if missingInputFiles > 0:
		print '  Jobs skipped because of missing input files:', missingInputFiles
	if replaceFiles:
		print '  Number of existing files to be deleted:', len(removeFiles)
	else:
		if existingOutputFiles > 0:
			print '  Jobs skipped because of existing output files:', existingOutputFiles
	print '  Total number of jobs:', nTotalJobs
	print '  Maximum CPU time per job:', cpuLimit, 'sec'
	print ''
	
	print 'General parameters:'
	print '  Detector model:', detector
	if eventType:
		print '  Event type:', eventType
	print '  Job title:', jobTitle
	print '  Banned sites:', bannedSites
	print '  Repository file:', repositoryFile
	print ''			

	print 'Files:'
	print '  Input sand box:', inputSandbox
	print '  Input data:', jobs[0]['inputData']
	print '  Output sand box:', outputSandbox
	print '  Output data:', jobs[0]['outputData']
	print '  Output storage path:', baseUserPath+outputPath
	print '  Output storage element:', storageElement
	print ''
	
	print 'LCSim step:'
	print '  LCSim version:', lcsimVer
	print '  LCSim file:', jobs[0]['xmlFile']
	print '  Tracking strategies:', strategyFile
	print '  Detector alias file:', aliasFile
	print ''
	
	if agentMode:
		print 'WARNING: jobs are submitted in agent mode and will be executed locally.'
		print ''
	
	if nTotalJobs < 1:
		print 'No jobs to submit, please check your input!'
		sys.exit(2)
	
	answer = raw_input('Proceed and submit job(s)? (Y/N): ')
	if not answer.lower() in ('y', 'yes'):
		sys.exit(2)

if nTotalJobs < 1:
	print 'No jobs to submit, please check your input!'
	sys.exit(2)

# delete files
if len(removeFiles) > 0:
	result = fileCatalog.removeFile( removeFiles )
	print 'Removed files (still need debugging):'
	print result

# create and submit jobs
dirac = DiracILC ( True , repositoryFile )

counter = 0
for job in jobs:
	if not job['submit']:
		continue
	counter += 1
	xmlFile = job['xmlFile']
	inputData = job['inputData']
	outputData = job['outputData']
	outputSlcio = ''
	for fileName in outputData:
		if fileName.count('.slcio'):
			outputSlcio = fileName
			break

	ilcjob = ILCJob()
	res = ilcjob.setLCSIM ( appVersion = lcsimVer ,
						 xmlfile = xmlFile ,
						 aliasproperties = aliasFile ,
						 evtstoprocess = nEvts,
						 inputslcio = inputData,
						 outputFile = outputSlcio
					   )
	if not res['OK']:
		print res['Message']
		continue
	
	ilcjob.setOutputSandbox ( outputSandbox )
	ilcjob.setInputSandbox ( inputSandbox )
	# remove trailing "/" from output path
	if outputPath[-1:] == '/':
		outputPath = outputPath[:-1]
	ilcjob.setOutputData ( outputData, storageElement, outputPath )	
	ilcjob.setCPUTime( cpuLimit )
	ilcjob.setSystemConfig ( systemConfig )
	ilcjob.setName ( detector+"_"+eventType+"_"+jobTitle )
	ilcjob.setJobGroup( jobTitle )
	ilcjob.setBannedSites( bannedSites )
	
	print 'Submitting job %s / %s'%(counter, nTotalJobs)
	if agentMode:
		dirac.submit ( ilcjob, mode="Agent" )
	else:
		dirac.submit ( ilcjob )
