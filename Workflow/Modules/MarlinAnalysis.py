'''
Run Marlin

ILCDIRAC.Workflow.Modules.MarlinAnalysis Called by Job Agent. 

:since: Feb 9, 2010

:author: Stephane Poss
:author: Przemyslaw Majewski
'''

import glob
import os
import shutil
 
from DIRAC.Core.Utilities.Subprocess                      import shellCall
from DIRAC                                                import S_OK, S_ERROR, gLogger

from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import getSoftwareFolder, getEnvironmentScript
from ILCDIRAC.Core.Utilities.PrepareOptionFiles           import prepareXMLFile, getNewLDLibs
from ILCDIRAC.Core.Utilities.resolvePathsAndNames         import resolveIFpaths, getProdFilename
from ILCDIRAC.Core.Utilities.PrepareLibs                  import removeLibc
from ILCDIRAC.Core.Utilities.FindSteeringFileDir          import getSteeringFileDirName
from ILCDIRAC.Workflow.Utilities.DD4hepMixin              import DD4hepMixin



__RCSID__ = "$Id$"

class MarlinAnalysis(DD4hepMixin, ModuleBase):
  """Define the Marlin analysis part of the workflow
  """
  def __init__(self):
    super(MarlinAnalysis, self).__init__( )
    self.enable = True
    self.STEP_NUMBER = ''
    self.log = gLogger.getSubLogger( "MarlinAnalysis" )
    self.result = S_ERROR()
    self.inputGEAR = ''
    self.outputREC = ''
    self.outputDST = ''
    self.applicationName = "Marlin"
    self.eventstring = ['ProgressHandler','event', 'EventNumber', 'StatusMonitor']
    self.envdict = {}
    self.ProcessorListToUse = []
    self.ProcessorListToExclude = []
    self.detectorModel = None

  def applicationSpecificInputs(self):
    """ Resolve all input variables for the module here.

    :return: S_OK()
    """

    if 'ParametricInputSandbox' in self.workflow_commons:
      paramsb = self.workflow_commons['ParametricInputSandbox']
      if not isinstance( paramsb, list ):
        if paramsb:
          paramsb = paramsb.split(";")
        else:
          paramsb = []
        
      self.InputFile += paramsb
    
    ##Backward compat needed, cannot remove yet.  
    self.outputREC = self.step_commons.get('outputREC', self.outputREC)
    self.outputDST = self.step_commons.get('outputDST', self.outputDST)
      
    if 'IS_PROD' in self.workflow_commons:
      if self.workflow_commons["IS_PROD"] and not self.OutputFile:
        #self.outputREC = getProdFilename(self.outputREC,int(self.workflow_commons["PRODUCTION_ID"]),
        #                                 int(self.workflow_commons["JOB_ID"]))
        #self.outputDST = getProdFilename(self.outputDST,int(self.workflow_commons["PRODUCTION_ID"]),
        #                                 int(self.workflow_commons["JOB_ID"]))
        #if 'MokkaOutput' in self.workflow_commons:
        #  self.InputFile = getProdFilename(self.workflow_commons["MokkaOutput"],int(self.workflow_commons["PRODUCTION_ID"]),
        #                                    int(self.workflow_commons["JOB_ID"]))
        if 'ProductionOutputData' in self.workflow_commons:
          outputlist = self.workflow_commons['ProductionOutputData'].split(";")
          for obj in outputlist:
            if obj.lower().count("_rec_"):
              self.outputREC = os.path.basename(obj)
            elif obj.lower().count("_dst_"):
              self.outputDST = os.path.basename(obj)
            elif obj.lower().count("_sim_"):
              self.InputFile = [os.path.basename(obj)]
        else:
          self.outputREC = getProdFilename(self.outputREC, int(self.workflow_commons["PRODUCTION_ID"]),
                                           int(self.workflow_commons["JOB_ID"]))
          self.outputDST = getProdFilename(self.outputDST, int(self.workflow_commons["PRODUCTION_ID"]),
                                           int(self.workflow_commons["JOB_ID"]))
          #if 'MokkaOutput' in self.workflow_commons:
          #  self.InputFile = getProdFilename(self.workflow_commons["MokkaOutput"],int(self.workflow_commons["PRODUCTION_ID"]),
          #                                    int(self.workflow_commons["JOB_ID"]))
          self.InputFile = [getProdFilename(self.InputFile, int(self.workflow_commons["PRODUCTION_ID"]),
                                            int(self.workflow_commons["JOB_ID"]))]
          
        
    if not self.InputFile and self.InputData:
      for files in self.InputData:
        if files.lower().find(".slcio") > -1:
          self.InputFile.append(files)
            
    return S_OK('Parameters resolved')
      
  def runIt(self):
    """
    Called by Agent
    
    Execute the following:
      - resolve where the soft was installed
      - prepare the list of file to feed Marlin with
      - create the XML file on which Marlin has to run, done by :any:`prepareXMLFile`
      - run Marlin and catch the exit code

    :return: S_OK(), S_ERROR()
    """
    self.result = S_OK()
    if not self.platform:
      self.result = S_ERROR( 'No ILC platform selected' )
    elif not self.applicationLog:
      self.result = S_ERROR( 'No Log file provided' )
    if not self.result['OK']:
      self.log.error("Failed to resolve input parameters:", self.result["Message"])
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('%s should not proceed as previous step did not end properly' % self.applicationName)

    #get the path to the detector model, either local or from the software
    compactFile = None
    if self.detectorModel:
      resXML = self._getDetectorXML()
      if not resXML['OK']:
        self.log.error("Could not obtain the detector XML file: ", resXML["Message"])
        return resXML
      compactFile = resXML['Value']

    res = getEnvironmentScript(self.platform, "marlin", self.applicationVersion, self.getEnvScript)
    if not res['OK']:
      self.log.error("Failed to get the env script")
      return res
    env_script_path = res["Value"]

    res = self._getInputFiles()
    if not res['OK']:
      self.log.error("Failed getting input files:", res['Message'])
      return res
    listofslcio = res['Value']

    
    finalXML = "marlinxml_" + self.STEP_NUMBER + ".xml"

    steeringfiledirname = ''
    res = getSteeringFileDirName(self.platform, "marlin", self.applicationVersion)
    if res['OK']:
      steeringfiledirname = res['Value']
    else:
      self.log.warn('Could not find the steering file directory', res['Message'])
      
    ##Handle PandoraSettings.xml
    pandorasettings = 'PandoraSettings.xml'
    if not os.path.exists(pandorasettings):
      if steeringfiledirname and os.path.exists(os.path.join(steeringfiledirname, pandorasettings)):
        try:
          shutil.copy(os.path.join(steeringfiledirname, pandorasettings), 
                      os.path.join(os.getcwd(), pandorasettings))
        except EnvironmentError, x:
          self.log.warn('Could not copy PandoraSettings.xml, exception: %s' % x)

    if self.inputGEAR:
      self.inputGEAR = os.path.basename(self.inputGEAR)
      if self.inputGEAR and not os.path.exists(self.inputGEAR) and steeringfiledirname \
         and os.path.exists(os.path.join(steeringfiledirname, self.inputGEAR)):
        self.inputGEAR = os.path.join(steeringfiledirname, self.inputGEAR)

    self.SteeringFile = os.path.basename(self.SteeringFile)
    if not os.path.exists(self.SteeringFile):
      if steeringfiledirname:
        if os.path.exists(os.path.join(steeringfiledirname, self.SteeringFile)):
          self.SteeringFile = os.path.join(steeringfiledirname, self.SteeringFile)
    if not self.SteeringFile:
      self.log.error("Steering file not defined, shouldn't happen!")
      return S_ERROR("Could not find steering file")



    resOver = self._checkRunOverlay()
    if not resOver['OK']:
      return resOver
    overlayParam = resOver['Value']

    res = prepareXMLFile(finalXML, self.SteeringFile, self.inputGEAR, listofslcio,
                         self.NumberOfEvents, self.OutputFile, self.outputREC, self.outputDST, 
                         self.debug,
                         dd4hepGeoFile=compactFile,
                         overlayParam=overlayParam,
                        )
    if not res['OK']:
      self.log.error('Something went wrong with XML generation because %s' % res['Message'])
      self.setApplicationStatus('Marlin: something went wrong with XML generation')
      return res

    res = self.prepareMARLIN_DLL(env_script_path)
    if not res['OK']:
      self.log.error('Failed building MARLIN_DLL:', res['Message'])
      self.setApplicationStatus('Failed to setup MARLIN_DLL')
      return S_ERROR('Something wrong with software installation')
    marlin_dll = res["Value"]
    
    self.result = self.runMarlin(finalXML, env_script_path, marlin_dll)
    if not self.result['OK']:
      self.log.error('Something wrong during running:', self.result['Message'])
      self.setApplicationStatus('Error during running %s' % self.applicationName)
      return S_ERROR('Failed to run %s' % self.applicationName)

    #self.result = {'OK':True,'Value':(0,'Disabled Execution','')}
    resultTuple = self.result['Value']
    if not os.path.exists(self.applicationLog):
      self.log.error("Something went terribly wrong, the log file is not present")
      self.setApplicationStatus('%s failed terribly, you are doomed!' % (self.applicationName))
      if not self.ignoreapperrors:
        return S_ERROR('%s did not produce the expected log' % (self.applicationName))

    status = resultTuple[0]
    # stdOutput = resultTuple[1]
    # stdError = resultTuple[2]
    self.log.info( "Status after the application execution is:", str( status ) )

    return self.finalStatusReport(status) 

  def prepareMARLIN_DLL(self, env_script_path):
    """ Prepare the run time environment: MARLIN_DLL in particular.
    """
    #to fix the MARLIN_DLL, we need to get it first
    with open("temp.sh",'w') as script:
      script.write("#!/bin/bash\n")
      lines = []
      lines.append('source %s > /dev/null' % env_script_path)
      lines.append('echo $MARLIN_DLL')
      script.write("\n".join(lines))
    os.chmod("temp.sh", 0755)
    res = shellCall(0, "./temp.sh")
    if not res['OK']:
      self.log.error("Could not get the MARLIN_DLL env")
      return S_ERROR("Failed getting the MARLIN_DLL")
    marlindll = res["Value"][1].rstrip()
    marlindll = marlindll.rstrip(":")
    try:
      os.remove('temp.sh')
    except EnvironmentError, e:
      self.log.warn("Failed to delete the temp file", str(e))

    if not marlindll:
      return S_ERROR("Empty MARLIN_DLL env variable!")
    #user libs
    userlib = ""
    
    if os.path.exists("./lib/marlin_dll"):
      for library in glob.glob("./lib/marlin_dll/*.so"):
        userlib = userlib + library + ":"
        
    userlib = userlib.rstrip(":")
    
    temp = marlindll.split(':')
    temp2 = userlib.split(":")
    lib1d = {}
    libuser = {}
    for lib in temp:
      lib1d[os.path.basename(lib)] = lib
    for lib in temp2:
      libuser[os.path.basename(lib)] = lib

    for lib1, path1 in lib1d.items():
      if lib1 in libuser:
        self.log.verbose("Duplicated lib found, removing %s" % path1)
        try:
          temp.remove(path1)
        except ValueError:
          pass
      
    marlindll = "%s:%s" % (":".join(temp), userlib) #Here we concatenate the default MarlinDLL with the user's stuff
    finallist = []
    items = marlindll.split(":")
    #Care for user defined list of processors, useful when someone does not want to run the full reco
    if self.ProcessorListToUse:
      for processor in self.ProcessorListToUse:
        for item in items:
          if item.count(processor):
            finallist.append(item)
    else:
      finallist = items
    items = finallist
    #Care for user defined excluded list of processors, useful when someone does not want to run the full reco
    if self.ProcessorListToExclude:
      for item in items:
        for processor in self.ProcessorListToExclude:
          if item.count(processor):
            finallist.remove(item)
    else:
      finallist = items


    ## LCFIPlus links with LCFIVertex, LCFIVertex needs to go first in the MARLIN_DLL
    plusPos = 0
    lcfiPos = 0
    for position, lib in enumerate(finallist):
      if 'libLCFIPlus' in lib:
        plusPos = position
      if 'libLCFIVertex' in lib:
        lcfiPos = position
    if plusPos < lcfiPos: # if lcfiplus is before lcfivertex
      #swap the two entries
      finallist[plusPos], finallist[lcfiPos] = finallist[lcfiPos], finallist[plusPos]

    marlindll = ":".join(finallist)
    self.log.verbose("Final MARLIN_DLL is:", marlindll)
    
    return S_OK(marlindll)

  def runMarlin(self, inputxml, env_script_path, marlin_dll):
    """ Actual bit of code running Marlin. Tomato calls this function.
    """
    scriptName = '%s_%s_Run_%s.sh' % (self.applicationName, self.applicationVersion, self.STEP_NUMBER)
    if os.path.exists(scriptName): 
      os.remove(scriptName)
    script = open(scriptName,'w')
    script.write('#!/bin/bash \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')
    script.write("source %s\n" % env_script_path)
    script.write("declare -x MARLIN_DLL=%s\n" % marlin_dll)
    ## needed for lcgeo detector components for example
    if os.path.exists("./lib/"):
      script.write('declare -x LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH\n')
    if os.path.exists("./lib/lddlib"):
      script.write('declare -x LD_LIBRARY_PATH=./lib/lddlib:$LD_LIBRARY_PATH\n') 
    script.write('declare -x PATH=$ROOTSYS/bin:$PATH\n')
    script.write('declare -x MARLIN_DEBUG=1\n')##Needed for recent version of marlin (from 03 april 2013)
    #We need to make sure the PandoraSettings is in the current directory
    script.write("""
if [ -e "${PANDORASETTINGS}" ]
then
   cp $PANDORASETTINGS .
fi    
""")
    script.write('echo =============================\n')
    script.write('echo LD_LIBRARY_PATH is\n')
    script.write('echo $LD_LIBRARY_PATH | tr ":" "\n"\n')
    script.write('echo =============================\n')
    script.write('echo PATH is\n')
    script.write('echo $PATH | tr ":" "\n"\n')
    script.write('echo =============================\n')
    script.write('echo MARLIN_DLL is\n')
    script.write('echo $MARLIN_DLL | tr ":" "\n"\n')
    script.write('echo =============================\n')
    if self.debug:
      script.write('echo ldd of executable is\n')
      script.write('ldd `which Marlin` \n' )
      script.write('echo =============================\n')
      if os.path.exists('./lib/marlin_dll'):
        script.write('ldd ./lib/marlin_dll/*.so \n')
      if os.path.exists('./lib/lddlib'):
        script.write('ldd ./lib/lddlib/*.so \n')
      script.write('echo =============================\n')
    script.write('env | sort >> localEnv.log\n')      

    if os.path.exists(inputxml):
      #check
      script.write('Marlin -c %s %s\n' % (inputxml, self.extraCLIarguments))
      #real run
      script.write('Marlin %s %s\n' % (inputxml, self.extraCLIarguments))
    else:
      script.close()
      self.log.error("Steering file missing")
      return S_ERROR("SteeringFile is missing")
    script.write('declare -x appstatus=$?\n')
    script.write('exit $appstatus\n')

    script.close()
    if os.path.exists(self.applicationLog): 
      os.remove(self.applicationLog)

    os.chmod(scriptName, 0755)
    comm = 'sh -c "./%s"' % (scriptName)
    self.setApplicationStatus('%s %s step %s' % (self.applicationName, self.applicationVersion, self.STEP_NUMBER))
    self.stdError = ''
    res = shellCall(0, comm, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)    
    return res
  
  def _getInputFiles(self):
    """ Resolve the input files. But not if in the application definition it was decided
    that it should forget about the input.
    """
    if self.ignoremissingInput:
      return S_OK("")
    res = resolveIFpaths(self.InputFile)
    if not res['OK']:
      self.setApplicationStatus('%s: missing slcio file' % self.applicationName)
      return S_ERROR('Missing slcio file!')
    runonslcio = res['Value']

    listofslcio = " ".join(runonslcio)
    
    return S_OK(listofslcio)

  def _checkRunOverlay( self ):
    """ checks if overlay should be run and which overlay parameters are there, allows running multiple overlay input steps """
    overlayParam = []
    for parameter in sorted( self.workflow_commons ):
      if parameter.startswith('OI_') and parameter.endswith('_eventType'):
        stepNumber = int( parameter.split('_')[1] )
        eventsPerBackgroundFile = self.workflow_commons.get("OI_%i_eventsPerBackgroundFile" % stepNumber)
        backgroundType = self.workflow_commons.get("OI_%i_eventType" % stepNumber )
        processorName = self.workflow_commons.get("OI_%i_processorName" % stepNumber, None )
        self.log.info( "Number of Events per BackgroundFile: %d " % eventsPerBackgroundFile )
        self.log.info( "Background type: %s " % backgroundType )
        overlayParam.append( (backgroundType, eventsPerBackgroundFile, processorName) )

    return S_OK( overlayParam )


  def getEnvScript(self, sysconfig, appname, appversion):
    """ Called if CVMFS is not available
    """
    res = getSoftwareFolder(sysconfig, appname, appversion)
    if not res['OK']:
      self.setApplicationStatus('Marlin: Could not find neither local area not shared area install')
      return res
    
    myMarlinDir = res['Value']

    ##Remove libc
    removeLibc(myMarlinDir + "/LDLibs")

    ##Need to fetch the new LD_LIBRARY_PATH
    new_ld_lib_path = getNewLDLibs(sysconfig, "marlin", appversion)

    marlindll = ""
    if os.path.exists("%s/MARLIN_DLL" % myMarlinDir):
      for library in os.listdir("%s/MARLIN_DLL" % myMarlinDir):
        marlindll = marlindll + "%s/MARLIN_DLL/%s" % (myMarlinDir, library) + ":"
      marlindll = "%s" % (marlindll)
    else:
      self.log.error('MARLIN_DLL folder not found, cannot proceed')
      return S_ERROR('MARLIN_DLL folder not found in %s' % myMarlinDir)

    env_script_name = "MarlinEnv.sh"
    script = open(env_script_name, "w")
    script.write("#!/bin/sh\n")
    script.write('##########################################################\n')
    script.write('# Dynamically generated script to create env for Marlin. #\n')
    script.write('##########################################################\n')
    script.write("declare -x PATH=%s/Executable:$PATH\n" % myMarlinDir)
    script.write('declare -x ROOTSYS=%s/ROOT\n' % (myMarlinDir))
    script.write('declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:%s/LDLibs:%s\n' % (myMarlinDir, new_ld_lib_path))
    script.write("declare -x MARLIN_DLL=%s\n" % marlindll)
    script.write("declare -x PANDORASETTINGS=%s/Settings/PandoraSettings.xml" % myMarlinDir)
    script.close()
    #os.chmod(env_script_name, 0755)
    return S_OK(os.path.abspath(env_script_name))
