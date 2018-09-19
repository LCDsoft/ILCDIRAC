'''
Module to run root macros

:since:  Apr 29, 2010
:author: Stephane Poss
'''

import os

from DIRAC.Core.Utilities.Subprocess                      import shellCall
from DIRAC import S_OK, S_ERROR, gLogger

from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import getEnvironmentScript
from ILCDIRAC.Workflow.Utilities.RootMixin                import RootMixin

__RCSID__ = '$Id$'
LOG = gLogger.getSubLogger(__name__)


class RootMacroAnalysis(RootMixin, ModuleBase):
  """Run Root macro
  """
  def __init__(self):
    super(RootMacroAnalysis, self).__init__()
    self.enable = True
    self.STEP_NUMBER = ''
    self.applicationName = 'ROOT'
    # from the interface
    self.script = ''
    self.arguments = ''

  def applicationSpecificInputs(self):
    """ Resolve all input variables for the module here.

    :return: S_OK()
    """
    return S_OK('Parameters resolved') 
  
  def runIt(self):
    """
    Called by Agent
    
    Execute the following:
      - define the platform
      - check for presence of ROOTSYS variable
      
    """
    self.result = S_OK()
    if not self.platform:
      self.result = S_ERROR( 'No ILC platform selected' )
    elif not self.applicationLog:
      self.result = S_ERROR( 'No Log file provided' )
    if not self.result['OK']:
      LOG.error("Failed to resolve input parameters:", self.result['Message'])
      return self.result

    res = getEnvironmentScript(self.platform, "root", self.applicationVersion, self.getRootEnvScript)
    LOG.notice("Got the environment script: %s" % res)
    if not res['OK']:
      LOG.error("Error getting the env script: ", res['Message'])
      return res
    envScriptPath = res['Value']

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      LOG.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('ROOT should not proceed as previous step did not end properly')

    if len(self.script) < 1:
      LOG.error('Macro file not defined, should not happen!')
      return S_ERROR("Macro file not defined")
     
    self.script = os.path.basename(self.script)
    
    scriptName = 'Root_%s_Run_%s.sh' % (self.applicationVersion, self.STEP_NUMBER)
    if os.path.exists(scriptName): 
      os.remove(scriptName)
    script = open(scriptName,'w')
    script.write('#!/bin/sh \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')

    script.write('source %s\n' % envScriptPath )

    if os.path.exists("./lib"):
      script.write('declare -x LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH\n')

    script.write('echo =============================\n')
    script.write('echo LD_LIBRARY_PATH is\n')
    script.write('echo $LD_LIBRARY_PATH | tr ":" "\n"\n')
    script.write('echo =============================\n')
    script.write('echo PATH is\n')
    script.write('echo $PATH | tr ":" "\n"\n')
    script.write('echo =============================\n')
    script.write('env | sort >> localEnv.log\n')      
    script.write('echo =============================\n')
    comm = "root -b -q %s" % self.script
    if self.arguments:
      ## need rawstring for arguments so we don't lose escaped quotation marks for string arguments
      comm = comm + r'\(%s\)' % self.arguments
    comm = comm + "\n"
    LOG.info("Will run %s" % (comm))
    script.write(comm)
    
    script.write('declare -x appstatus=$?\n')
    #script.write('where\n')
    #script.write('quit\n')
    #script.write('EOF\n')
    script.write('exit $appstatus\n')

    script.close()
    if os.path.exists(self.applicationLog): 
      os.remove(self.applicationLog)

    os.chmod(scriptName, 0755)
    comm = 'sh -c "./%s"' % (scriptName)
    self.setApplicationStatus('ROOT %s step %s' % (self.applicationVersion, self.STEP_NUMBER))
    self.stdError = ''
    self.result = shellCall(0, comm, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)
    #self.result = {'OK':True,'Value':(0,'Disabled Execution','')}
    resultTuple = self.result['Value']
    if not os.path.exists(self.applicationLog):
      LOG.error("Something went terribly wrong, the log file is not present")
      self.setApplicationStatus('root failed terribly, you are doomed!')
      return S_ERROR('root did not produce the expected log' )
    status = resultTuple[0]
    # stdOutput = resultTuple[1]
    # stdError = resultTuple[2]
    LOG.info("Status after the application execution is %s" % str(status))

    return self.finalStatusReport(status)
