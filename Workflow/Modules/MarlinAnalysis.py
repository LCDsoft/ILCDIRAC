##############################################################
# $HeadURL$
##############################################################

'''
Run Marlin

ILCDIRAC.Workflow.Modules.MarlinAnalysis Called by Job Agent. 

@since: Feb 9, 2010

@author: Stephane Poss 
@author: Przemyslaw Majewski
'''

__RCSID__ = "$Id$"

import os, string, shutil, glob, types
 
from DIRAC.Core.Utilities.Subprocess                      import shellCall
#from DIRAC.Core.DISET.RPCClient                           import RPCClient
from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import getSoftwareFolder
from ILCDIRAC.Core.Utilities.PrepareOptionFiles           import PrepareXMLFile, GetNewLDLibs
from ILCDIRAC.Core.Utilities.resolvePathsAndNames         import resolveIFpaths, getProdFilename
from ILCDIRAC.Core.Utilities.PrepareLibs                  import removeLibc
from ILCDIRAC.Core.Utilities.FindSteeringFileDir          import getSteeringFileDirName


from DIRAC                                                import S_OK, S_ERROR, gLogger


class MarlinAnalysis(ModuleBase):
  """Define the Marlin analysis part of the workflow
  """
  def __init__(self):
    super(MarlinAnalysis, self).__init__( )
    self.enable = True
    self.STEP_NUMBER = ''
    self.log = gLogger.getSubLogger( "MarlinAnalysis" )
    self.result = S_ERROR()
    self.InputFile = []
    self.SteeringFile = ''
    self.inputGEAR = ''
    self.outputREC = ''
    self.outputDST = ''
    self.applicationName = "Marlin"
    self.NumberOfEvents = -1
    self.eventstring = ['ProgressHandler','event']
    self.envdict = {}
    self.ProcessorListToUse = []
    self.ProcessorListToExclude = []
    
  def applicationSpecificInputs(self):
    """ Resolve all input variables for the module here.
    @return: S_OK()
    """
    ##TODO: Need to keep for old interface. Move to ModuleBase
    if self.step_commons.has_key('inputSlcio'):
      inputf = self.step_commons["inputSlcio"]
      if not type(inputf) == types.ListType:
        if len(inputf):
          inputf = inputf.split(";")
        else:
          inputf = [] 
      self.InputFile = inputf
      
    if self.workflow_commons.has_key('ParametricInputSandbox'):
      paramsb = self.workflow_commons['ParametricInputSandbox']
      if not type(paramsb) == types.ListType:
        if len(paramsb):
          paramsb = paramsb.split(";")
        else:
          paramsb = []
        
      self.InputFile += paramsb
      
    if self.step_commons.has_key('inputXML'):
      self.SteeringFile = self.step_commons['inputXML']
      
    if self.step_commons.has_key('inputGEAR'):
      self.inputGEAR = self.step_commons['inputGEAR']
      
    if self.step_commons.has_key('EvtsToProcess'):
      self.NumberOfEvents = self.step_commons['EvtsToProcess']
    
    ##Backward compat needed, cannot remove yet.  
    if self.step_commons.has_key('outputREC'):
      self.outputREC = self.step_commons['outputREC']
      
    if self.step_commons.has_key('outputDST'):
      self.outputDST = self.step_commons['outputDST']
      
    if self.workflow_commons.has_key("IS_PROD"):
      if self.workflow_commons["IS_PROD"] and len(self.OutputFile)==0:
        #self.outputREC = getProdFilename(self.outputREC,int(self.workflow_commons["PRODUCTION_ID"]),
        #                                 int(self.workflow_commons["JOB_ID"]))
        #self.outputDST = getProdFilename(self.outputDST,int(self.workflow_commons["PRODUCTION_ID"]),
        #                                 int(self.workflow_commons["JOB_ID"]))
        #if self.workflow_commons.has_key("MokkaOutput"):
        #  self.InputFile = getProdFilename(self.workflow_commons["MokkaOutput"],int(self.workflow_commons["PRODUCTION_ID"]),
        #                                    int(self.workflow_commons["JOB_ID"]))
        if self.workflow_commons.has_key('ProductionOutputData'):
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
          #if self.workflow_commons.has_key("MokkaOutput"):
          #  self.InputFile = getProdFilename(self.workflow_commons["MokkaOutput"],int(self.workflow_commons["PRODUCTION_ID"]),
          #                                    int(self.workflow_commons["JOB_ID"]))
          self.InputFile = [getProdFilename(self.InputFile, int(self.workflow_commons["PRODUCTION_ID"]),
                                            int(self.workflow_commons["JOB_ID"]))]
          
        
    if not len(self.InputFile) and len(self.InputData):
      for files in self.InputData:
        if files.lower().find(".slcio") > -1:
          self.InputFile.append(files)
            
    return S_OK('Parameters resolved')
      
  def execute(self):
    """
    Called by Agent
    
    Execute the following:
      - resolve where the soft was installed
      - prepare the list of file to feed Marlin with
      - create the XML file on which Marlin has to run, done by L{PrepareXMLFile}
      - run Marlin and catch the exit code
    @return: S_OK(), S_ERROR()
    """
    self.result = self.resolveInputVariables()
    if not self.systemConfig:
      self.result = S_ERROR( 'No ILC platform selected' )
    elif not self.applicationLog:
      self.result = S_ERROR( 'No Log file provided' )
    if not self.result['OK']:
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('%s should not proceed as previous step did not end properly' % self.applicationName)

    
    marlinDir = self.ops.getValue('/AvailableTarBalls/%s/%s/%s/TarBall' % (self.systemConfig, 
                                                                           "marlin", 
                                                                           self.applicationVersion), '')
    marlinDir = marlinDir.replace(".tgz", "").replace(".tar.gz", "")
    res = getSoftwareFolder(marlinDir)
    if not res['OK']:
      self.setApplicationStatus('Marlin: Could not find neither local area not shared area install')
      return res
    
    myMarlinDir = res['Value']

    ##Remove libc
    removeLibc(myMarlinDir + "/LDLibs")

    ##Need to fetch the new LD_LIBRARY_PATH
    new_ld_lib_path = GetNewLDLibs(self.systemConfig, "marlin", self.applicationVersion)

    res = self.GetInputFiles()
    if not res['OK']:
      self.log.error(res['Message'])
      return res
    listofslcio = res['Value']

    
    finalXML = "marlinxml_" + self.STEP_NUMBER + ".xml"

    steeringfiledirname = ''
    res = getSteeringFileDirName(self.systemConfig, "marlin", self.applicationVersion)     
    if res['OK']:
      steeringfiledirname = res['Value']
    else:
      self.log.warn('Could not find the steering file directory')
      
    ##Handle PandoraSettings.xml
    pandorasettings = 'PandoraSettings.xml'
    if not os.path.exists(pandorasettings):
      if os.path.exists(os.path.join(myMarlinDir, 'Settings', pandorasettings)):
        try:
          shutil.copy(os.path.join(myMarlinDir, 'Settings', pandorasettings), 
                      os.path.join(os.getcwd(), pandorasettings))
        except Exception, x:
          self.log.warn('Could not copy PandoraSettings.xml, exception: %s' % x)
      elif steeringfiledirname and os.path.exists(os.path.join(steeringfiledirname,pandorasettings)):
        try:
          shutil.copy(os.path.join(steeringfiledirname, pandorasettings), 
                      os.path.join(os.getcwd(), pandorasettings))
        except Exception, x:
          self.log.warn('Could not copy PandoraSettings.xml, exception: %s' % x)
           
    self.inputGEAR = os.path.basename(self.inputGEAR)
    if self.inputGEAR and not os.path.exists(self.inputGEAR):
      if steeringfiledirname:
        if os.path.exists(os.path.join(steeringfiledirname, self.inputGEAR)):
          self.inputGEAR = os.path.join(steeringfiledirname, self.inputGEAR)
        
    
    self.SteeringFile = os.path.basename(self.SteeringFile)
    if not os.path.exists(self.SteeringFile):
      if steeringfiledirname:
        if os.path.exists(os.path.join(steeringfiledirname, self.SteeringFile)):
          self.SteeringFile = os.path.join(steeringfiledirname, self.SteeringFile)
    if not self.SteeringFile:
      return S_ERROR("Could not find steering file")
    
    res = PrepareXMLFile(finalXML, self.SteeringFile, self.inputGEAR, listofslcio, 
                         self.NumberOfEvents, self.OutputFile, self.outputREC, self.outputDST, 
                         self.debug)
    if not res['OK']:
      self.log.error('Something went wrong with XML generation because %s' % res['Message'])
      self.setApplicationStatus('Marlin: something went wrong with XML generation')
      return res

    res = self.prepareMARLIN_DLL(myMarlinDir)
    if not res['OK']:
      self.log.error('Failed building MARLIN_DLL: %s' % res['Message'])
      self.setApplicationStatus('Failed to setup MARLIN_DLL')
      return S_ERROR('Something wrong with software installation')
    
    self.envdict['MARLIN_DLL'] = res['Value']
    self.envdict['MarlinDIR'] = myMarlinDir
    self.envdict['LD_LIB_PATH'] = new_ld_lib_path
    self.result = self.runMarlin(finalXML, self.envdict)
    if not self.result['OK']:
      self.log.error('Something wrong during running: %s' % self.result['Message'])
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
    self.log.info( "Status after the application execution is %s" % str( status ) )

    return self.finalStatusReport(status) 

  def prepareMARLIN_DLL(self, marlinDir):
    """ Prepare the run time environment: MARLIN_DLL in particular.
    """
    marlindll = ""
    if(os.path.exists("%s/MARLIN_DLL" % marlinDir)):
      for d in os.listdir("%s/MARLIN_DLL" % marlinDir):
        marlindll = marlindll + "%s/MARLIN_DLL/%s" % (marlinDir, d) + ":" 
      marlindll = "%s" % (marlindll)
    else:
      self.log.error('MARLIN_DLL folder not found, cannot proceed')
      return S_ERROR('MARLIN_DLL folder not found in %s' % marlinDir)
    #user libs
    userlib = ""
    if(os.path.exists("./lib")):
      if os.path.exists("./lib/marlin_dll"):
        for d in glob.glob("./lib/marlin_dll/*.so"):
          userlib = userlib + d + ":" 
      
    temp = marlindll.split(":")
    temp2 = userlib.split(":")
    for x in temp2:
      doublelib = "%s/MARLIN_DLL/" % (marlinDir) + os.path.basename(x)
      if doublelib in temp:
        self.log.verbose("Duplicated lib found, removing %s" % doublelib)
        try:
          temp.remove(doublelib)
        except:
          pass
      
    marlindll = "%s%s" % (string.join(temp, ":"), userlib)
    finallist = []
    items = marlindll.split(":")
    #Care for user defined list of processors, useful when someone does not want to run the full reco
    if len(self.ProcessorListToUse):
      for processor in self.ProcessorListToUse:
        for item in items:
          if item.count(processor):
            finallist.append(item)
    else:
      finallist = items
    items = finallist
    #Care for user defined excluded list of processors, useful when someone does not want to run the full reco
    if len(self.ProcessorListToExclude):
      for item in items:
        for processor in self.ProcessorListToExclude:
          if item.count(processor):
            finallist.remove(item)
    else:
      finallist = items
    marlindll = string.join(finallist, ":")
    
    return S_OK(marlindll)

  def runMarlin(self, inputxml, envdict):
    """ Actual bit of code running Marlin. Tomato calls this function.
    """
    scriptName = '%s_%s_Run_%s.sh' % (self.applicationName, self.applicationVersion, self.STEP_NUMBER)
    if os.path.exists(scriptName): 
      os.remove(scriptName)
    script = open(scriptName,'w')
    script.write('#!/bin/sh \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')
    myMarlinDir = envdict['MarlinDIR']

    if envdict.has_key('MARLIN_DLL'):
      script.write('declare -x MARLIN_DLL=%s\n' % envdict['MARLIN_DLL'])
    else:
      return S_ERROR('MARLIN_DLL not found.')  
          
    script.write('declare -x ROOTSYS=%s/ROOT\n' % (myMarlinDir))
    if envdict.has_key('LD_LIB_PATH'):
      script.write('declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:%s/LDLibs:%s\n' % (myMarlinDir, envdict['LD_LIB_PATH']))
    else:
      script.write('declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:%s/LDLibs\n' % (myMarlinDir))
    if os.path.exists("./lib/lddlib"):
      script.write('declare -x LD_LIBRARY_PATH=./lib/lddlib:$LD_LIBRARY_PATH\n')
      
    script.write('declare -x PATH=$ROOTSYS/bin:$PATH\n')
    script.write('declare -x MARLIN_DEBUG=1\n')##Needed for recent version of marlin (from 03 april 2013)
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
      script.write('ldd %s/Executable/* \n' % myMarlinDir)
      script.write('echo =============================\n')
      script.write('echo ldd of Marlin_DLL objects is\n')
      script.write('ldd %s/MARLIN_DLL/* \n' % myMarlinDir)
      if os.path.exists('./lib/marlin_dll'):
        script.write('ldd ./lib/marlin_dll/*.so \n')
      script.write('echo =============================\n')
      script.write('echo ldd of LDLIBS objects is\n')
      script.write('ldd %s/LDLibs/* \n' % myMarlinDir)  
      if os.path.exists('./lib/lddlib'):
        script.write('ldd ./lib/lddlib/*.so \n')
      script.write('echo =============================\n')
    script.write('env | sort >> localEnv.log\n')      

    if (os.path.exists("%s/Executable/Marlin" % myMarlinDir)):
      if (os.path.exists(inputxml)):
        #check
        script.write('%s/Executable/Marlin -c %s %s\n' % (myMarlinDir, inputxml, self.extraCLIarguments))
        #real run
        script.write('%s/Executable/Marlin %s %s\n' % (myMarlinDir, inputxml, self.extraCLIarguments))
    else:
      script.close()
      self.log.error("Marlin executable is missing, something is wrong with the installation!")
      return S_ERROR("Marlin executable is missing")
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
  
  def GetInputFiles(self):
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

    listofslcio = string.join(runonslcio, " ")
    
    return S_OK(listofslcio)
  
