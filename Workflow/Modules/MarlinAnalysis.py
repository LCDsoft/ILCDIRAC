# $HeadURL$
# $Id$
'''
ILCDIRAC.Workflow.Modules.MarlinAnalysis Called by Job Agent. 

Define the Marlin analysis part of the workflow

@since: Feb 9, 2010

@author: Stephane Poss and Przemyslaw Majewski
'''
import os,sys,re,string
 
from DIRAC.Core.Utilities.Subprocess                      import shellCall
#from DIRAC.Core.DISET.RPCClient                           import RPCClient
from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import LocalArea,SharedArea
from ILCDIRAC.Core.Utilities.PrepareOptionFiles           import PrepareXMLFile
from ILCDIRAC.Core.Utilities.ResolveDependencies          import resolveDepsTar

from DIRAC                                                import S_OK, S_ERROR, gLogger, gConfig
import DIRAC


class MarlinAnalysis(ModuleBase):
  """Define the Marlin analysis part of the workflow
  """
  def __init__(self):
    ModuleBase.__init__(self)
    self.enable = True
    self.STEP_NUMBER = ''
    self.debug = True
    self.log = gLogger.getSubLogger( "MarlinAnalysis" )
    self.result = S_ERROR()
    self.inputSLCIO = ''
    self.inputXML=''
    self.inputGEAR =''
    self.jobID = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']
    self.systemConfig = ''
    self.applicationLog = ''
    self.applicationVersion=''
    self.jobType = ''
    self.evtstoprocess = ''
    self.debug = False
    
  def resolveInputVariables(self):
    """ Resolve all input variables for the module here.
    @return: S_OK()
    """
    if self.workflow_commons.has_key('SystemConfig'):
      self.systemConfig = self.workflow_commons['SystemConfig']
      
    if self.step_commons.has_key('applicationVersion'):
      self.applicationVersion = self.step_commons['applicationVersion']
      self.applicationLog = self.step_commons['applicationLog']
      
    if self.workflow_commons.has_key('JobType'):
      self.jobType = self.workflow_commons['JobType']
      
    if self.step_commons.has_key('inputSlcio'):
      self.inputSLCIO =self.step_commons['inputSlcio']
      
    if self.step_commons.has_key('inputXML'):
      self.inputXML=self.step_commons['inputXML']
      
    if self.step_commons.has_key('inputGEAR'):
      self.inputGEAR=self.step_commons['inputGEAR']
      
    if self.workflow_commons.has_key('JobType'):
      self.jobType = self.workflow_commons['JobType']
      
    if self.step_commons.has_key('EvtsToProcess'):
      if(self.step_commons['EvtsToProcess']>0):
        self.evtstoprocess = str(self.step_commons['EvtsToProcess'])
    if self.step_commons.has_key('debug'):
      self.debug =  self.step_commons['debug']
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
    
    marlinDir = gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s/TarBall'%(self.systemConfig,"marlin",self.applicationVersion),'')
    marlinDir = marlinDir.rstrip(".tgz").rstrip(".tar.gz")
    mySoftwareRoot = ''
    localArea = LocalArea()
    sharedArea = SharedArea()
    if os.path.exists('%s%s%s' %(localArea,os.sep,marlinDir)):
      mySoftwareRoot = localArea
    if os.path.exists('%s%s%s' %(sharedArea,os.sep,marlinDir)):
      mySoftwareRoot = sharedArea
    myMarlinDir = os.path.join(mySoftwareRoot,marlinDir)
    ### Resolve dependencies
    deps = resolveDepsTar(self.systemConfig,"marlin",self.applicationVersion)
    for dep in deps:
      if os.path.exists(os.path.join(mySoftwareRoot,dep.rstrip(".tgz").rstrip(".tar.gz"))):
        depfolder = dep.rstrip(".tgz").rstrip(".tar.gz")
        if os.path.exists(os.path.join(mySoftwareRoot,depfolder,"lib")):
          self.log.verbose("Found lib folder in %s"%(depfolder))
          if os.environ.has_key("LD_LIBRARY_PATH"):
            os.environ["LD_LIBRARY_PATH"] = os.path.join(mySoftwareRoot,depfolder,"lib")+":%s"%os.environ["LD_LIBRARY_PATH"]
          else:
            os.environ["LD_LIBRARY_PATH"] = os.path.join(mySoftwareRoot,depfolder,"lib")

    runonslcio = []
    inputfilelist = self.inputSLCIO.split(";")
    for inputfile in inputfilelist:
      runonslcio.append(os.path.basename(inputfile))
    #print "input file list ",inputfilelist
    #listofslcio = self.inputSLCIO.replace(";", " ")
    listofslcio = string.join(runonslcio," ")
    #for inputfile in self.inputSLCIO:
    #  listofslcio += listofslcio+" "+inputfile
    #listofslcio = string.join(self.inputSLCIO," ")#string.join(runonslcio, ' ')
    
    finalXML = "marlinxml.xml"
    self.inputGEAR = os.path.basename(self.inputGEAR)
    self.inputXML = os.path.basename(self.inputXML)
    res = PrepareXMLFile(finalXML,self.inputXML,self.inputGEAR,listofslcio,self.evtstoprocess,self.debug)
    if not res:
      self.log.error('Something went wrong with XML generation')
      return S_ERROR('Something went wrong with XML generation')
    
    scriptName = 'Marlin_%s_Run_%s.sh' %(self.applicationVersion,self.STEP_NUMBER)
    if os.path.exists(scriptName): os.remove(scriptName)
    script = open(scriptName,'w')
    script.write('#!/bin/sh \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')
    marlindll = ""
    if(os.path.exists("%s/MARLIN_DLL"%myMarlinDir)):
      if os.environ.has_key('MARLIN_DLL'):
        for d in os.listdir("%s/MARLIN_DLL"%myMarlinDir):
          if d!="Marlin":
            marlindll = marlindll + "%s/MARLIN_DLL/%s"%(myMarlinDir,d) + ":" 
        #script.write('export MARLIN_DLL=%s:%s'%(marlindll,os.environ['MARLIN_DLL']))
        marlindll="%s:%s"%(marlindll,os.environ['MARLIN_DLL'])
      else:
        for d in os.listdir("%s/MARLIN_DLL"%myMarlinDir):
          marlindll = marlindll + "%s//MARLIN_DLL/%s"%(myMarlinDir,d) + ":" 
        #script.write('export MARLIN_DLL=%s:'%marlindll)
        marlindll="%s"%(marlindll)

    #user libs
    userlib = ""
    if(os.path.exists("./lib")):      
      for d in os.listdir("lib"):
          userlib = userlib + "./lib/%s"%d + ":" 
      
    temp=marlindll.split(":")
    temp2=userlib.split(":")
    
    for x in temp2:
      try:
        temp.remove(x)
      except:
        pass
    userlib=""
    for x in temp2:
      userlib=userlib + x + ":"
      
    marlindll = "%s%s"%(marlindll,userlib)
    
    if (len(marlindll) != 0):
      script.write('declare -x MARLIN_DLL=%s\n'%marlindll)
          
    script.write('declare -x ROOTSYS=%s/ROOT\n'%(myMarlinDir))
    if os.environ.has_key('LD_LIBRARY_PATH'):
      script.write('declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:%s/LDLibs:%s\n'%(myMarlinDir,os.environ['LD_LIBRARY_PATH']))
    else:
      script.write('declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:%s/LDLibs\n'%(myMarlinDir))
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
    #script.write('echo ldd is\n')
    #script.write('ldd %s/MarlinLibs/*/* \n'%mySoftwareRoot)
    #script.write('echo =============================\n')
    #script.write('echo uname is \n')
    #script.write('uname -a\n')
    #script.write('echo =============================\n')
    #script.write('echo gcc is \n')
    #script.write('gcc --version\n')
    #script.write('echo =============================\n')
    #script.write('echo ld is \n')
    #script.write('ld --version\n')
    script.write('env | sort >> localEnv.log\n')      
    script.write('echo =============================\n')

    if (os.path.exists("%s/Executable/Marlin"%myMarlinDir)):
      if (os.path.exists(finalXML)):
        #check
        script.write('%s/Executable/Marlin -c %s\n'%(myMarlinDir,finalXML))
        #real run
        script.write('%s/Executable/Marlin %s\n'%(myMarlinDir,finalXML))
            
    script.write('declare -x appstatus=$?\n')
    #script.write('where\n')
    #script.write('quit\n')
    #script.write('EOF\n')
    script.write('exit $appstatus\n')

    script.close()
    if os.path.exists(self.applicationLog): os.remove(self.applicationLog)

    os.chmod(scriptName,0755)
    comm = 'sh -c "./%s %s"' %(scriptName,finalXML)
    self.setApplicationStatus('Marlin %s step %s' %(self.applicationVersion,self.STEP_NUMBER))
    self.stdError = ''
    self.result = shellCall(0,comm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    #self.result = {'OK':True,'Value':(0,'Disabled Execution','')}
    resultTuple = self.result['Value']

    status = resultTuple[0]
    # stdOutput = resultTuple[1]
    # stdError = resultTuple[2]
    self.log.info( "Status after the application execution is %s" % str( status ) )

    failed = False
    if status != 0:
      self.log.error( "Marlin execution completed with errors:" )
      failed = True
    else:
      self.log.info( "Marlin execution completed successfully")

    if failed==True:
      self.log.error( "==================================\n StdError:\n" )
      self.log.error( self.stdError )
      #self.setApplicationStatus('%s Exited With Status %s' %(self.applicationName,status))
      self.log.error('Marlin Exited With Status %s' %(status))
      return S_ERROR('Marlin Exited With Status %s' %(status))
    self.setApplicationStatus('Marlin %s Successful' %(self.applicationVersion))
    return S_OK('Marlin %s Successful' %(self.applicationVersion))

  def redirectLogOutput(self, fd, message):
    """Catch the output from the application
    """
    sys.stdout.flush()
    if message:
      if re.search('INFO Evt',message): print message
    if self.applicationLog:
      log = open(self.applicationLog,'a')
      log.write(message+'\n')
      log.close()
    else:
      self.log.error("Application Log file not defined")
    if fd == 1:
      self.stdError += message