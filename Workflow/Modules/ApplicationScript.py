'''
Created on Jul 12, 2010

@author: sposs
'''
import os,sys,re,string
from DIRAC.Core.Utilities.Subprocess                      import shellCall
from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from DIRAC                                                import S_OK, S_ERROR, gLogger, gConfig

class ApplicationScript(ModuleBase):
  def __init__(self):
    ModuleBase.__init__(self)
    self.enable = True 
    self.log = gLogger.getSubLogger( "ScriptAnalysis" )
    self.script = None
    self.arguments = ''

  def applicationSpecificInputs(self):

    if self.step_commons.has_key('script'):
      self.script = self.step_commons['script']
      print self.script
    else:
      self.log.warn('No script defined')
    if self.step_commons.has_key('arguments'):
      self.arguments = self.step_commons['arguments']
      
    return S_OK()

  def execute(self):
    self.result =self.resolveInputVariables()
    if not self.applicationName or not self.applicationVersion:
      self.result = S_ERROR( 'No Application defined' )
    if not self.applicationLog:
      self.applicationLog = '%s.log' %(os.path.basename(self.script))    
    if not self.result['OK']:
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' %(self.workflowStatus['OK'],self.stepStatus['OK']))
      return S_OK('ApplicationScript should not proceed as previous step did not end properly')

    
    Cmd = []
    if re.search('.py$',self.script):
      Cmd.append('python')
      Cmd.append(os.path.basename(self.script))
      Cmd.append(self.arguments)
    else:
      Cmd.append("./"+os.path.basename(self.script))
      Cmd.append(self.arguments)

    command = ' '.join(Cmd)
    print 'Command = %s' %(command)  #Really print here as this is useful to see
    
    com = []
    cmdSep = 'echo "%s"' %('='*50)
    com.append(cmdSep)
    com.append('echo "Log file from execution of: %s"' %(command))
    com.append(cmdSep)
    com.append('env | sort >> localEnv.log')
    com.append(cmdSep)
    if os.path.exists("./lib"):
      com.append('declare -x LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH')
    com.append(command)
    com.append('declare -x appstatus=$?')
    com.append('exit $appstatus')
    finalCommand = string.join(com,';')
    
    self.stdError = ''    
    result = shellCall(0,finalCommand,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    if not result['OK']:
      self.log.error(result)
      return S_ERROR('Problem Executing Application')

    resultTuple = result['Value']

    status = resultTuple[0]
    # stdOutput = resultTuple[1]
    # stdError = resultTuple[2]
    self.log.info( "Status after %s execution is %s" %(os.path.basename(self.script),str(status)) )
    failed = False
    if status != 0:
      self.log.info( "%s execution completed with non-zero status:" % os.path.basename(self.script) )
      failed = True
    elif len(self.stdError) > 0:
      self.log.info( "%s execution completed with application warning:" % os.path.basename(self.script) )
      self.log.info(self.stdError)
    else:
      self.log.info( "%s execution completed successfully:" % os.path.basename(self.script) )

    if failed==True:
      self.log.error( "==================================\n StdError:\n" )
      self.log.error( self.stdError )
      return S_ERROR('%s Exited With Status %s' %(os.path.basename(self.script),status))

    #Above can't be removed as it is the last notification for user jobs
    self.setApplicationStatus('%s (%s %s) Successful' %(os.path.basename(self.script),self.applicationName,self.applicationVersion))
    return S_OK('%s (%s %s) Successful' %(os.path.basename(self.script),self.applicationName,self.applicationVersion))
          
  
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