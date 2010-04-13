'''
ILCDIRAC.Workflow.Modules.LcsimAnalysis Called by Job Agent. 

Created on Apr 7, 2010

@author: sposs
'''
import os, sys, re, string
from DIRAC.Core.Utilities.Subprocess                      import shellCall
#from DIRAC.Core.DISET.RPCClient                           import RPCClient
from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import LocalArea,SharedArea
from ILCDIRAC.Core.Utilities.PrepareOptionFiles           import PrepareLCSIMFile
from DIRAC                                                import S_OK, S_ERROR, gLogger, gConfig
import DIRAC

class LCSIMAnalysis(ModuleBase):
  def __init__(self):
    ModuleBase.__init__(self)
    self.enable = True
    self.STEP_NUMBER = ''
    self.log = gLogger.getSubLogger( "LCSIMAnalysis" )
    self.result = S_ERROR()
    self.systemConfig = ''
    self.applicationLog = ''
    self.applicationVersion=''
    self.sourcedir = ''
    self.xmlfile = ''
    self.self.inputSLCIO = ''
    self.jobID = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']
     
  def resolveInputVariables(self):
    if self.workflow_commons.has_key('SystemConfig'):
      self.systemConfig = self.workflow_commons['SystemConfig']

    if self.step_commons.has_key('applicationVersion'):
      self.applicationVersion = self.step_commons['applicationVersion']
      self.applicationLog = self.step_commons['applicationLog']

    if self.step_commons.has_key('sourceDir'):
      self.sourcedir = self.step_commons['sourceDir']
    if self.step_commons.has_key('lcsimFile'):
      self.xmlfile = self.step_commons['lcsimFile']
    

    return S_OK('Parameters resolved')

  def execute(self):
    """
    Called by Agent
    """
    self.result =self.resolveInputVariables()
    if not self.systemConfig:
      self.result = S_ERROR( 'No LCD platform selected' )
    elif not self.applicationLog:
      self.result = S_ERROR( 'No Log file provided' )
    if not self.result['OK']:
      return self.result
    lcsimDir = 'lcsim'
    mySoftwareRoot = ''
    localArea = LocalArea()
    sharedArea = SharedArea()
    if os.path.exists('%s%s%s' %(localArea,os.sep,lcsimDir)):
      mySoftwareRoot = localArea
    if os.path.exists('%s%s%s' %(sharedArea,os.sep,lcsimDir)):
      mySoftwareRoot = sharedArea
    if not mySoftwareRoot:
      self.log.error('Directory %s was not found in either the local area %s or shared area %s' %(lcsimDir,localArea,sharedArea))
      return S_ERROR('Failed to discover software')

    #if tarfile.is_tarfile(self.sourcedir) :
    #  untarred_sourcedir = tarfile.open(self.sourcedir,'r')
    #  sourcedir = untarred_sourcedir.getmembers()[0].split("/")[0]
    #  untarred_sourcedir.close()
    #else :
    #  sourcedir = self.sourcedir
    runonslcio = []
    inputfilelist = self.inputSLCIO.split(";")
    for inputfile in inputfilelist:
      runonslcio.append(os.path.basename(inputfile))

    #look for lcsim filename
    lcsim_name = gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s/TarBall'%(self.systemConfig,"LCSIM",self.applicationVersion),'')
    if not lcsim_name:
      self.log.error("Could not find lcsim file name from CS")
      return S_ERROR("Could not find lcsim file name from CS")
    
    lcsimfile = "job.lcsim"
    xmlfileok = PrepareLCSIMFile(self.xmlfile,lcsimfile,runonslcio)
    if not xmlfileok:
      self.log.error("Could not treat input lcsim file")
      return S_ERROR("Error parsing input lcsim file")
    
    scriptName = 'LCSIM_%s_Run_%s.sh' %(self.applicationVersion,self.STEP_NUMBER)
    if os.path.exists(scriptName): os.remove(scriptName)
    script = open(scriptName,'w')
    script.write('#!/bin/sh \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')
    #for lib in os.path("%s/GeomConverter/target/lib"%(mySoftwareRoot)):
    #  script.write("declare -x CLASSPATH=$CLASSPATH:%s\n"%lib)
    #script.write("declare -x CLASSPATH=$CLASSPATH:%s/lcsim/target/lcsim-%s.jar\n"%(mySoftwareRoot,self.applicationVersion))
    #script.write("declare -x BINPATH=%s/bin\n"%(sourcedir))
    #script.write("declare -x SOURCEPATH=%s/src\n"%(sourcedir))
    #script.write("declare -x JAVALIBPATH=$SOURCEPATH/util\n")
    
    comm = "java -server -jar %s/%s %s\n"%(mySoftwareRoot,lcsim_name,self.xmlfile)
    print comm
    script.write(comm)
    script.write('declare -x appstatus=$?\n')
    script.write('exit $appstatus\n')    
    script.close()
    if os.path.exists(self.applicationLog): os.remove(self.applicationLog)

    os.chmod(scriptName,0755)
    comm = 'sh -c "./%s"' %scriptName
    self.setApplicationStatus('LCSIM %s step %s' %(self.applicationVersion,self.STEP_NUMBER))
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
      self.log.error( "LCSIM execution completed with errors:" )
      failed = True
    else:
      self.log.info( "LCSIM execution completed successfully")

    if failed:
      self.log.error( "==================================\n StdError:\n" )
      self.log.error( self.stdError) 
      #self.setApplicationStatus('%s Exited With Status %s' %(self.applicationName,status))
      self.log.error('LCSIM Exited With Status %s' %(status))
      return S_ERROR('LCSIM Exited With Status %s' %(status))    
    return S_OK('LCSIM %s Successful' %(self.applicationVersion))

    #############################################################################
    def redirectLogOutput(self, fd, message):
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
    #############################################################################
    