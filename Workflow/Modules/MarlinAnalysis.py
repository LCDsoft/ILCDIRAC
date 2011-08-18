##############################################################
# $HeadURL$
##############################################################

'''
ILCDIRAC.Workflow.Modules.MarlinAnalysis Called by Job Agent. 

Define the Marlin analysis part of the workflow

@since: Feb 9, 2010

@author: Stephane Poss and Przemyslaw Majewski
'''

__RCSID__ = "$Id$"

import os,sys,re,string, shutil
 
from DIRAC.Core.Utilities.Subprocess                      import shellCall
#from DIRAC.Core.DISET.RPCClient                           import RPCClient
from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import LocalArea,SharedArea
from ILCDIRAC.Core.Utilities.PrepareOptionFiles           import PrepareXMLFile,GetNewLDLibs
from ILCDIRAC.Core.Utilities.ResolveDependencies          import resolveDepsTar
from ILCDIRAC.Core.Utilities.resolveIFpaths import resolveIFpaths
from ILCDIRAC.Core.Utilities.resolveOFnames import getProdFilename
from ILCDIRAC.Core.Utilities.InputFilesUtilities import getNumberOfevents
from ILCDIRAC.Core.Utilities.PrepareLibs import removeLibc


from DIRAC                                                import S_OK, S_ERROR, gLogger, gConfig


class MarlinAnalysis(ModuleBase):
  """Define the Marlin analysis part of the workflow
  """
  def __init__(self):
    ModuleBase.__init__(self)
    self.enable = True
    self.STEP_NUMBER = ''
    self.log = gLogger.getSubLogger( "MarlinAnalysis" )
    self.result = S_ERROR()
    self.inputSLCIO = ''
    self.SteeringFile =''
    self.inputGEAR =''
    self.outputREC = ''
    self.outputDST = ''
    self.applicationName = "Marlin"
    self.evtstoprocess = ''
    self.eventstring = ''
    self.envdict = {}
    
  def applicationSpecificInputs(self):
    """ Resolve all input variables for the module here.
    @return: S_OK()
    """
    ##TODO: Need to keep for old interface. Move to ModuleBase
    if self.step_commons.has_key('inputSlcio'):
      self.inputSLCIO =self.step_commons['inputSlcio']
      
    if self.workflow_commons.has_key('ParametricInputSandbox'):
      self.inputSLCIO += ";" + self.workflow_commons['ParametricInputSandbox']
            
    if self.step_commons.has_key('inputXML'):
      self.SteeringFile=self.step_commons['inputXML']
      
    if self.step_commons.has_key('inputGEAR'):
      self.inputGEAR=self.step_commons['inputGEAR']
      
    if self.step_commons.has_key('EvtsToProcess'):
      self.evtstoprocess = str(self.step_commons['EvtsToProcess'])
    
    ##Backward compat needed, cannot remove yet.  
    if self.step_commons.has_key('outputREC'):
      self.outputREC = self.step_commons['outputREC']
      
    if self.step_commons.has_key('outputDST'):
      self.outputDST = self.step_commons['outputDST']
      
    if self.workflow_commons.has_key("IS_PROD"):
      if self.workflow_commons["IS_PROD"]:
        #self.outputREC = getProdFilename(self.outputREC,int(self.workflow_commons["PRODUCTION_ID"]),
        #                                 int(self.workflow_commons["JOB_ID"]))
        #self.outputDST = getProdFilename(self.outputDST,int(self.workflow_commons["PRODUCTION_ID"]),
        #                                 int(self.workflow_commons["JOB_ID"]))
        #if self.workflow_commons.has_key("MokkaOutput"):
        #  self.inputSLCIO = getProdFilename(self.workflow_commons["MokkaOutput"],int(self.workflow_commons["PRODUCTION_ID"]),
        #                                    int(self.workflow_commons["JOB_ID"]))
        if self.workflow_commons.has_key('ProductionOutputData'):
          outputlist = self.workflow_commons['ProductionOutputData'].split(";")
          for obj in outputlist:
            if obj.lower().count("_rec_"):
              self.outputREC = os.path.basename(obj)
            elif obj.lower().count("_dst_"):
              self.outputDST = os.path.basename(obj)
            elif obj.lower().count("_sim_"):
              self.inputSLCIO = os.path.basename(obj)
        else:
          self.outputREC = getProdFilename(self.outputREC,int(self.workflow_commons["PRODUCTION_ID"]),
                                           int(self.workflow_commons["JOB_ID"]))
          self.outputDST = getProdFilename(self.outputDST,int(self.workflow_commons["PRODUCTION_ID"]),
                                           int(self.workflow_commons["JOB_ID"]))
          if self.workflow_commons.has_key("MokkaOutput"):
            self.inputSLCIO = getProdFilename(self.workflow_commons["MokkaOutput"],int(self.workflow_commons["PRODUCTION_ID"]),
                                              int(self.workflow_commons["JOB_ID"]))
          
    if self.InputData:
      if not self.workflow_commons.has_key("Luminosity") or not self.workflow_commons.has_key("NbOfEvents"):
        res = getNumberOfevents(self.InputData)
        if res.has_key("nbevts") and not self.workflow_commons.has_key("Luminosity") :
          self.workflow_commons["NbOfEvents"]=res["nbevts"]
          self.evtstoprocess = res["nbevts"]
        if res.has_key("lumi") and not self.workflow_commons.has_key("NbOfEvents"):
          self.workflow_commons["Luminosity"]=res["lumi"]
        
    if len(self.inputSLCIO)==0 and not len(self.InputData)==0:
      inputfiles = self.InputData.split(";")
      for files in inputfiles:
        if files.lower().find(".slcio")>-1:
          self.inputSLCIO += files+";"
      self.inputSLCIO = self.inputSLCIO.rstrip(";")
            
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
    self.result =self.resolveInputVariables()
    if not self.systemConfig:
      self.result = S_ERROR( 'No ILC platform selected' )
    elif not self.applicationLog:
      self.result = S_ERROR( 'No Log file provided' )
    if not self.result['OK']:
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' %(self.workflowStatus['OK'],self.stepStatus['OK']))
      return S_OK('%s should not proceed as previous step did not end properly'%self.applicationName)

    
    marlinDir = gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s/TarBall'%(self.systemConfig,"marlin",self.applicationVersion),'')
    marlinDir = marlinDir.replace(".tgz","").replace(".tar.gz","")
    mySoftwareRoot = ''
    localArea = LocalArea()
    sharedArea = SharedArea()
    if os.path.exists('%s%s%s' %(localArea,os.sep,marlinDir)):
      mySoftwareRoot = localArea
    elif os.path.exists('%s%s%s' %(sharedArea,os.sep,marlinDir)):
      mySoftwareRoot = sharedArea
    else:
      self.setApplicationStatus('Marlin: Could not find neither local area not shared area install')
      return S_ERROR('Missing installation of Marlin!')
    myMarlinDir = os.path.join(mySoftwareRoot,marlinDir)

    ##Remove libc
    removeLibc(myMarlinDir+"/LDLibs")

    ##Need to fetch the new LD_LIBRARY_PATH
    new_ld_lib_path= GetNewLDLibs(self.systemConfig,"marlin",self.applicationVersion,mySoftwareRoot)

    res = self.GetInputFiles()
    if not res['OK']:
      self.log.error(res['Message'])
      return res
    listofslcio = res['Value']

    ##Handle PandoraSettings.xml
    pandorasettings = 'PandoraSettings.xml'
    if not os.path.exists(pandorasettings):
      if os.path.exists(os.path.join(mySoftwareRoot,marlinDir,'Settings',pandorasettings)):
        try:
          shutil.copy(os.path.join(mySoftwareRoot,marlinDir,'Settings',pandorasettings),os.path.join(os.getcwd(),pandorasettings))
        except Exception,x:
          self.log.error('Could not copy PandoraSettings.xml, exception: %s'%x)
    
    finalXML = "marlinxml.xml"
    self.inputGEAR = os.path.basename(self.inputGEAR)
    if not os.path.exists(self.inputGEAR):
      if os.path.exists(os.path.join(mySoftwareRoot,"steeringfilesV1",self.inputGEAR)):
        self.inputGEAR = os.path.join(mySoftwareRoot,"steeringfilesV1",self.inputGEAR)
      
    self.SteeringFile = os.path.basename(self.SteeringFile)
    if not os.path.exists(self.SteeringFile):
      if os.path.exists(os.path.join(mySoftwareRoot,"steeringfilesV1",self.SteeringFile)):
        self.SteeringFile = os.path.join(mySoftwareRoot,"steeringfilesV1",self.SteeringFile)
    if not self.SteeringFile:
      return S_ERROR("Could not find steering file")
    
    res = PrepareXMLFile(finalXML,self.SteeringFile,self.inputGEAR,listofslcio,self.evtstoprocess,self.outputREC,self.outputDST,self.debug)
    if not res['OK']:
      self.log.error('Something went wrong with XML generation because %s'%res['Message'])
      self.setApplicationStatus('Marlin: something went wrong with XML generation')
      return res

    res = self.prepareMARLIN_DLL(myMarlinDir)
    if not res['OK']:
      self.log.error('Failed building MARLIN_DLL: %s'%res['Message'])
      self.setApplicationStatus('Failed to setup MARLIN_DLL')
      return S_ERROR('Something wrong with software installation')
    
    self.envdict['MARLIN_DLL'] = res['Value']
    self.envdict['MarlinDIR'] = myMarlinDir
    self.envdict['LD_LIB_PATH'] = new_ld_lib_path
    self.result = self.runMarlin(finalXML, self.envdict)
    if not self.result['OK']:
      self.log.error('Something wrong during running: %s'%self.result['Message'])
      self.setApplicationStatus('Error during running %s'%self.applicationName)
      return S_ERROR('Failed to run %s'%self.applicationName)

    #self.result = {'OK':True,'Value':(0,'Disabled Execution','')}
    resultTuple = self.result['Value']
    if not os.path.exists(self.applicationLog):
      self.log.error("Something went terribly wrong, the log file is not present")
      self.setApplicationStatus('%s failed terribly, you are doomed!' %(self.applicationName))
      if not self.ignoreapperrors:
        return S_ERROR('%s did not produce the expected log' %(self.applicationName))

    status = resultTuple[0]
    # stdOutput = resultTuple[1]
    # stdError = resultTuple[2]
    self.log.info( "Status after the application execution is %s" % str( status ) )

    return self.finalStatusReport(status) 

  def prepareMARLIN_DLL(self,marlinDir):
    marlindll = ""
    if(os.path.exists("%s/MARLIN_DLL"%marlinDir)):
      for d in os.listdir("%s/MARLIN_DLL"%marlinDir):
        marlindll = marlindll + "%s/MARLIN_DLL/%s"%(marlinDir,d) + ":" 
      marlindll="%s"%(marlindll)
    else:
      self.log.error('MARLIN_DLL folder not found, cannot proceed')
      return S_ERROR('MARLIN_DLL folder not found in %s'%marlinDir)
    #user libs
    userlib = ""
    if(os.path.exists("./lib")):
      if os.path.exists("./lib/marlin_dll"):
        for d in os.listdir("lib/marlin_dll"):
          userlib = userlib + "./lib/marlin_dll/%s"%d + ":" 
      
    temp=marlindll.split(":")
    temp2=userlib.split(":")
    for x in temp2:
      doublelib = "%s/MARLIN_DLL/"%(marlinDir)+os.path.basename(x)
      if doublelib in temp:
        self.log.verbose("Duplicated lib found, removing %s"%doublelib)
        try:
          temp.remove(doublelib)
        except:
          pass
      
    marlindll = "%s%s"%(string.join(temp,":"),userlib)    
    return S_OK(marlindll)

  def runMarlin(self,inputxml,envdict):
    scriptName = '%s_%s_Run_%s.sh' %(self.applicationName,self.applicationVersion,self.STEP_NUMBER)
    if os.path.exists(scriptName): os.remove(scriptName)
    script = open(scriptName,'w')
    script.write('#!/bin/sh \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')
    myMarlinDir=envdict['MarlinDIR']

    if envdict.has_key('MARLIN_DLL'):
      script.write('declare -x MARLIN_DLL=%s\n'%envdict['MARLIN_DLL'])
    else:
      return S_ERROR('MARLIN_DLL not found.')  
          
    script.write('declare -x ROOTSYS=%s/ROOT\n'%(myMarlinDir))
    if envdict.has_key('LD_LIB_PATH'):
      script.write('declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:%s/LDLibs:%s\n'%(myMarlinDir,envdict['LD_LIB_PATH']))
    else:
      script.write('declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:%s/LDLibs\n'%(myMarlinDir))
    if os.path.exists("./lib/lddlib"):
      script.write('declare -x LD_LIBRARY_PATH=./lib/lddlib:$LD_LIBRARY_PATH\n')
      
    script.write('declare -x PATH=$ROOTSYS/bin:$PATH\n')
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
    script.write('echo ldd of executable is\n')
    script.write('ldd %s/Executable/* \n'%myMarlinDir)
    script.write('echo =============================\n')
    script.write('echo ldd of Marlin_DLL objects is\n')
    script.write('ldd %s/MARLIN_DLL/* \n'%myMarlinDir)
    if os.path.exists('./lib/marlin_dll'):
      script.write('ldd ./lib/marlin_dll/* \n')
    script.write('echo =============================\n')
    script.write('echo ldd of LDLIBS objects is\n')
    script.write('ldd %s/LDLibs/* \n'%myMarlinDir)  
    if os.path.exists('./lib/lddlib'):
      script.write('ldd ./lib/lddlib/* \n')
    script.write('echo =============================\n')
    script.write('env | sort >> localEnv.log\n')      
    script.write('echo =============================\n')

    if (os.path.exists("%s/Executable/Marlin"%myMarlinDir)):
      if (os.path.exists(inputxml)):
        #check
        script.write('%s/Executable/Marlin -c %s\n'%(myMarlinDir,inputxml))
        #real run
        script.write('%s/Executable/Marlin %s\n'%(myMarlinDir,inputxml))
    else:
      script.close()
      self.log.error("Marlin executable is missing, something is wrong with the installation!")
      return S_ERROR("Marlin executable is missing")
    script.write('declare -x appstatus=$?\n')
    script.write('exit $appstatus\n')

    script.close()
    if os.path.exists(self.applicationLog): os.remove(self.applicationLog)

    os.chmod(scriptName,0755)
    comm = 'sh -c "./%s"' %(scriptName)
    self.setApplicationStatus('%s %s step %s' %(self.applicationName,self.applicationVersion,self.STEP_NUMBER))
    self.stdError = ''
    res = shellCall(0,comm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)    
    return res
  
  def GetInputFiles(self):
    inputfilelist = self.inputSLCIO.split(";")
    res = resolveIFpaths(inputfilelist)
    if not res['OK']:
      self.setApplicationStatus('%s: missing slcio file'%self.applicationName)
      return S_ERROR('Missing slcio file!')
    runonslcio = res['Value']

    listofslcio = string.join(runonslcio," ")
    
    return S_OK(listofslcio)
  
