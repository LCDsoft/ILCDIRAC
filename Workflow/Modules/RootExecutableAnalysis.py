'''
ILCDIRAC.Workflow.Modules.RootMacroAnalysis

Module to run root macros

@since:  Apr 29, 2010

@author: Stephane Poss
'''
import os,sys
from DIRAC.Core.Utilities.Subprocess                      import shellCall
from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import LocalArea,SharedArea
from DIRAC                                                import S_OK, S_ERROR, gLogger

import DIRAC
class RootExecutableAnalysis(ModuleBase):
  """Run Root executable
  
  """
  def __init__(self):
    ModuleBase.__init__(self)
    self.enable = True
    self.STEP_NUMBER = ''
    self.log = gLogger.getSubLogger( "RootExecutableAnalysis" )
    
    self.appli = ''
    self.args = ''
    self.result = S_ERROR()
    
    self.jobID = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']
      
  def resolveInputVariables(self):
    """ Resolve all input variables for the module here.
    @return: S_OK()
    """
    if self.workflow_commons.has_key('SystemConfig'):
      self.systemConfig = self.workflow_commons['SystemConfig']
      
    if self.step_commons.has_key('applicationVersion'):
      self.applicationVersion = self.step_commons['applicationVersion']
      self.applicationLog = self.step_commons['applicationLog']
    if self.step_commons.has_key("script"):
      self.appli = self.step_commons["script"]
    if self.step_commons.has_key("args"):
      self.args = self.step_commons["args"]

    return S_OK('Parameters resolved') 
  
  def execute(self):
    """
    Called by Agent
    
    Execute the following:
      - define the platform
      - check for presence of ROOTSYS variable
      
    """
    self.result =self.resolveInputVariables()
    if not self.systemConfig:
      self.result = S_ERROR( 'No ILC platform selected' )
    elif not self.applicationLog:
      self.result = S_ERROR( 'No Log file provided' )
    if not self.result['OK']:
      return self.result

    if not os.environ.has_key("ROOTSYS"):
      self.log.error("Environment variable ROOTSYS was not defined, cannot do anything")
      return S_ERROR("Environment variable ROOTSYS was not defined, cannot do anything")

    #rootDir = 'root'
    #mySoftwareRoot = ''
    #localArea = LocalArea()
    #sharedArea = SharedArea()
    #if os.path.exists('%s%s%s' %(localArea,os.sep,rootDir)):
    #  mySoftwareRoot = localArea
    #if os.path.exists('%s%s%s' %(sharedArea,os.sep,rootDir)):
    #  mySoftwareRoot = sharedArea
    if len(self.appli)<1:
      return S_ERROR("Executable file not defined")
     
    self.appli = os.path.basename(self.appli)

    
    scriptName = 'Root_%s_Run_%s.sh' %(self.applicationVersion,self.STEP_NUMBER)
    if os.path.exists(scriptName): os.remove(scriptName)
    script = open(scriptName,'w')
    script.write('#!/bin/sh \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')

    if os.environ.has_key('LD_LIBRARY_PATH'):
      script.write('declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:%s\n'%(os.environ['LD_LIBRARY_PATH']))
    else:
      script.write('declare -x LD_LIBRARY_PATH=$ROOTSYS/lib\n')
      
    if(os.path.exists("./lib")):
      if os.environ.has_key('LD_LIBRARY_PATH'):
        script.write('declare -x LD_LIBRARY_PATH=./lib:%s\n'%(os.environ['LD_LIBRARY_PATH']))
      else:
        script.write('declare -x LD_LIBRARY_PATH=./lib\n')
        
    script.write('declare -x PATH=$ROOTSYS/bin:$PATH\n')
    script.write('echo =============================\n')
    script.write('echo LD_LIBRARY_PATH is\n')
    script.write('echo $LD_LIBRARY_PATH | tr ":" "\n"\n')
    script.write('echo =============================\n')
    script.write('echo PATH is\n')
    script.write('echo $PATH | tr ":" "\n"\n')
    script.write('echo =============================\n')
    script.write('env | sort >> localEnv.log\n')      
    script.write('echo =============================\n')
    script.write("chown u+x %s\n"%self.appli)
    comm = "./%s %s \n"%(self.appli,self.args)
    self.log.info("Will run %s"%(comm))
    script.write(comm)
    
    script.write('declare -x appstatus=$?\n')
    #script.write('where\n')
    #script.write('quit\n')
    #script.write('EOF\n')
    script.write('exit $appstatus\n')

    script.close()
    if os.path.exists(self.applicationLog): os.remove(self.applicationLog)

    os.chmod(scriptName,0755)
    comm = 'sh -c "./%s"' %(scriptName)
    self.setApplicationStatus('ROOT %s step %s' %(self.applicationVersion,self.STEP_NUMBER))
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
      self.log.error( "ROOT execution completed with errors:" )
      failed = True
    else:
      self.log.info( "ROOT execution completed successfully")

    if failed==True:
      self.log.error( "==================================\n StdError:\n" )
      self.log.error( self.stdError )
      #self.setApplicationStatus('%s Exited With Status %s' %(self.applicationName,status))
      self.log.error('ROOT Exited With Status %s' %(status))
      return S_ERROR('ROOT Exited With Status %s' %(status))
    self.setApplicationStatus('ROOT %s Successful' %(self.applicationVersion))
    return S_OK('ROOT %s Successful' %(self.applicationVersion))
    
  def redirectLogOutput(self, fd, message):
    """Catch the output from the application
    """
    sys.stdout.flush()
    if self.applicationLog:
      log = open(self.applicationLog,'a')
      log.write(message+'\n')
      log.close()
    else:
      self.log.error("Application Log file not defined")
    if fd == 1:
      self.stdError += message  