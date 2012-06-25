'''
Created on Feb 7, 2011

@author: cgrefe
'''
##
#  Convenience script for submitting jobs with full reconstruction chain (or parts of it) to dirac
#  christian.grefe@cern.ch
##

from DIRAC.Core.Base import Script
import sys

Script.registerSwitch( 'a:', 'alias=', 'name of the alias.properties file to use (default alias.properties)' )
Script.registerSwitch( 'A', 'agent', 'Submits the job in agent mode, which will run the job on the local machine' )
Script.registerSwitch( 'b:', 'banlist=', 'file with list of banned sites (default bannedSites.py)' )
Script.registerSwitch( 'd:', 'detector=', 'name of the detector model (default clic_sid_cdr)' )
Script.registerSwitch( 'D', 'debug', 'switches on the debug mode' )
Script.registerSwitch( 'i:', 'input=', 'input python script holding the lfnlist to process' )
Script.registerSwitch( 'l:', 'lcsimxml=', 'lcsim steering xml template (optional)' )
Script.registerSwitch( 'L:', 'lcsim=', 'lcsim version to use (1.15-SNAPSHOT)' )
Script.registerSwitch( 'j:', 'jobs=', 'number of jobs that each input file gets split into (default 1)' )
Script.registerSwitch( 'm:', 'macro=', 'name of the macro file used for SLIC (default default.mac)' )
Script.registerSwitch( 'n:', 'events=', 'number of events per job, -1 for all in file (default -1)' )
Script.registerSwitch( 'p:', 'process=', 'process name to be used for naming of path etc.' )
Script.registerSwitch( 'P:', 'pandora=', 'slicPandora version to use (default CDR0)' )
Script.registerSwitch( 'S:', 'slic=', 'slic version (default v2r8p4)' )
Script.registerSwitch( 't:', 'time=', 'CPU time limit per job in seconds (default 300000)' )
Script.registerSwitch( 'T:', 'title=', 'job title (default fullReco)' )
Script.registerSwitch( 'x:', 'settings=', 'name of pandora settings file (default taken from grid installation)' )

Script.setUsageMessage( sys.argv[0]+'-n <nEvts> (-i <inputList> AND/OR -m <macro>) (<additional options>)' )

Script.parseCommandLine()
switches = Script.getUnprocessedSwitches()

# default parameters
macroFile = 'slicMacros/default.mac'
slicPandoraVer = 'CDR0'
lcsimVer = '1.15-SNAPSHOT'
slicVer = 'v2r8p4'
detector = 'clic_sid_cdr'
jobTitle = 'fullReco'
inputFileList = None
inputSlcio = ''
nEvts = -1
nJobs = 1
settingsFile = ''
cpuLimit = 300000
slicPandoraPath = 'LFN:/ilc/prod/software/slicpandora/'
lcsimPath = 'LFN:/ilc/prod/software/lcsim/'
systemConfig = 'x86_64-slc5-gcc43-opt'
storageElement = 'CERN-SRM'
aliasFile = 'alias.properties'
xmlFile = 'defaultPrePandoraLcsim.xml'
lcsimTemplate = ''
strategyFile = 'defaultStrategies.xml'
banlistFile = 'bannedSites.py'
lfnlist = None
process = None
debug = False
agentMode = False

for switch in switches:
  opt = switch[0]
  arg = switch[1]
  if opt in ('a','alias'):
    aliasFile = arg
  if opt in ('A','agent'):
    agentMode = True
  if opt in ('b','banlist'):
    banlistFile = arg
  if opt in ('d','detector'):
    detector = arg
  if opt in ('D','debug'):
    debug = True
  if opt in ('i','input'):
    inputFileList = arg
  if opt in ('l','lcsimxml'):
    lcsimTemplate = arg
  if opt in ('L','lcsim'):
    lcsimVer = arg
  if opt in ('j', 'jobs'):
    nJobs = int(arg)
  if opt in ('m','macro'):
    macroFile = arg
  if opt in ('n','events'):
    nEvts = int(arg)
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

if not inputFileList:
  if macroFile == 'slicMacros/default.mac':
    Script.showHelp()
    sys.exit(2)
# helper function to replace strings in a textfile
def prepareFile( fileNameTemplate, fileNameOut, replacements ):
  f = open(fileNameTemplate)
  txt = f.read()
  counter = []
  for repkey, reprep in replacements:
    counter.append( ( repkey, reprep, txt.count(repkey) ) )
    txt = txt.replace( repkey, reprep )
  fOut = open( fileNameOut, "w" )
  fOut.write( txt )
  return counter

from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
from ILCDIRAC.Interfaces.API.ILCJob import ILCJob

inputSandbox = []

# tracking strategies
inputSandbox.append( 'LFN:/ilc/prod/software/lcsim/trackingStrategies/'+detector+'/'+strategyFile )

# JNI bindings for root writer
inputSandbox.append( 'lib' )
bannedSites = []
# read file with list of banned sites
if banlistFile:
  f = open( banlistFile, 'r')
  exec(f.read())
if not bannedSites:
  bannedSites = ['']

repositoryFile = 'repositoryFiles/'+detector+'.'+jobTitle+'.'

# read file with list of input files
if inputFileList:
  repositoryFile += inputFileList.split('/')[-1].replace('.py', '.cfg')
  f = open( inputFileList, 'r')
  exec(f.read())
else:
  repositoryFile += macroFile.split('/')[-1].replace('.mac', '.cfg')
if not lfnlist:
  lfnlist = [ '' ]

if debug:
  print 'Writing repository file to', repositoryFile, '...'

dirac = DiracILC ( True , repositoryFile )

for inputFile in lfnlist:

  for job in xrange( nJobs ):

    if inputFile:
      splitPath = inputFile.split('/')
      if slicVer:
        outputFile = jobTitle+'_'+splitPath[-1].replace( '.stdhep', '' )
        if not process:
          process = splitPath[ splitPath.index( 'gen' ) - 1]
      else:
        outputFile = jobTitle+'_'+splitPath[-1].replace( '.slcio', '' )
        inputSlcio = inputFile
        if not process:
          process = splitPath[-3]
    else:
      outputFile = jobTitle+'_'+macroFile.split('/')[-1].replace('.mac', '_%s' % (nEvts))
      if not process:
        print 'ERROR: no process defined. Use -p <processName> to define the storage path.'
        sys.exit(2)
    
    if not nJobs == 1:
      outputFile += '_%s'%(job)
    
    if slicPandoraVer:
      lcsimOutput = 'prePandora.slcio'
    else:
      lcsimOutput = outputFile+'.slcio'
    
    storagePath = detector+'/'+process+'/'+jobTitle

    outputData = []
    
    replacements = [
      ( '__outputSlcio__', outputFile+'.slcio' ),
      ( '__outputRoot__', outputFile+'.root' ),
      ( '__outputAida__', outputFile+'.aida' ),
      ( '__strategyFile__', strategyFile )
    ]
    
    if lcsimTemplate:
      xmlFile = outputFile + '.xml'
      counter = prepareFile( lcsimTemplate, xmlFile, replacements )
      for key, rep, count in counter:
        if key.count('output') and count:
          outputData.append( rep )# add to list of output files if key denotes some output and is present in template
    else:
      outputData.append( outputFile+'.slcio' )    # default reco only creates one output file
    
    
    if debug:
      print 'Parameters:'
      print '\tProcess:', process
      print '\tOutputFile:', outputFile
      print '\tStorage path:', storagePath
      print '\tDetector:', detector
      print '\tSlic version:', slicVer
      print '\tMacro file:', macroFile
      print '\tJobs per input file:', nJobs
      print '\tEvents per job:', nEvts
      print '\tLCSim version:', lcsimVer
      print '\tLCSim file:', xmlFile
      print '\tAlias file:', aliasFile
      print '\tSlicPandora version:', slicPandoraVer
      print '\tPandora settings:', settingsFile
      print '\tInput Sandbox:', inputSandbox
      print '\tOutput data:', outputData
      print '\tBanned sites:', bannedSites
    
    startEvt = job * nEvts
    
    job = ILCJob ( )
    
    if slicVer:
      if nEvts < 0:
        print 'ERROR: need to set number of events for SLIC. Use -n <nEvts> to set number of events.'
        sys.exit(2)
      res = job.setSLIC ( appVersion = slicVer ,
        detectorModel = detector ,
        macFile = macroFile ,
        inputGenfile = inputFile ,
        nbOfEvents = nEvts ,
        startFrom = startEvt ,
        outputFile = "slic.slcio"
        )
      if not res['OK']:
        print res['Message']
        sys.exit(2)
  
    if lcsimVer:
      res = job.setLCSIM ( lcsimVer ,
        xmlfile = xmlFile ,
        aliasproperties = aliasFile ,
        evtstoprocess = nEvts,
        inputslcio = inputSlcio,
        outputFile = lcsimOutput
        )
      inputSlcio = '' # no inputSlcio for slicPandora so it picks up its input from lcsim output
      if not res['OK']:
        print res['Message']
        sys.exit(2)
    
    if slicPandoraVer:
      res = job.setSLICPandora ( appVersion = slicPandoraVer ,
        detectorgeo = detector ,
        inputslcio = inputSlcio ,
        pandorasettings = settingsFile ,
        nbevts = nEvts ,
        outputFile = outputFile+'.slcio'
        )
      if not res['OK']:
        print res['Message']
        sys.exit(2)
    
    job.setOutputSandbox ( [ "*.log", "*.xml", "*.slcio" ] )
    job.setInputSandbox ( inputSandbox )
    #job.setOutputData ( outputData, storageElement, storagePath )  
    job.setCPUTime( cpuLimit )
    job.setSystemConfig ( systemConfig )
    job.setName ( detector+"_"+process+"_"+jobTitle )
    job.setBannedSites( bannedSites )
    #job.setDestination('LCG.CERN.ch')
    if debug:
      answer = raw_input('Submit job(s)? (Y/N): ')
      if not answer.lower() in ('y', 'yes'):
        sys.exit(2)
      else:
        # no more debug output in further loops
        debug = False
    if not agentMode:
      dirac.submit ( job )
    else:
      dirac.submit ( job, mode = "Agent" )

