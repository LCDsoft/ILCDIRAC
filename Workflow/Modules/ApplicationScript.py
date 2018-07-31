'''
Run any application provided by the user. Is used when a specific environment is needed (e.g. ROOT).

:since: Jul 12, 2010
:author: sposs
'''

import os
import re

from DIRAC.Core.Utilities.Subprocess                      import shellCall
from DIRAC                                                import S_OK, S_ERROR, gLogger

from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase

__RCSID__ = '$Id$'
LOG = gLogger.getSubLogger(__name__)

class ApplicationScript(ModuleBase):
  """ Default application environment. Called GenericApplication in the Interface.
  """
  def __init__(self):
    super(ApplicationScript, self).__init__()
    self.enable = True 
    self.script = None
    self.arguments = ''
    self.applicationName = 'Application script'
    self.applicationVersion = ''
    
  def applicationSpecificInputs(self):
    LOG.info("The arguments are %s" % self.arguments)
    if 'ParametricParameters' in self.workflow_commons:
      parametric = ' '
      if isinstance( self.workflow_commons['ParametricParameters'], list ):
        parametric = " ".join(self.workflow_commons['ParametricParameters'])
      else:
        parametric = self.workflow_commons['ParametricParameters']
      self.arguments += parametric

    return S_OK()

  def runIt(self):
    """ Run the application in a controlled environment
    """
    self.result = S_OK()
    if not self.script:
      self.result = S_ERROR('Script undefined.')
    if not self.applicationLog:
      self.applicationLog = '%s.log' % (os.path.basename(self.script))    
    if not self.result['OK']:
      LOG.error("Failed with :", self.result['Message'])
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      LOG.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('ApplicationScript should not proceed as previous step did not end properly')

    
    cmd = []
    if re.search('.py$', self.script):
      cmd.append('python')
      cmd.append(os.path.basename(self.script))
    else:
      cmd.append("./" + os.path.basename(self.script))
    cmd.append(self.arguments)
    cmd.append(self.extraCLIarguments)

    command = ' '.join(cmd)
    LOG.info('Command = %s' % (command))  # Really print here as this is useful to see
    
    com = []
    cmdSep = 'echo "%s"' % ('=' * 50)
    com.append(cmdSep)
    com.append('env | grep OMP')
    com.append( "export OMP_NUM_THREADS=1" )
    com.append('echo "Log file from execution of: %s"' % (command))
    com.append(cmdSep)
    com.append('env | sort >> localEnv.log')
    com.append(cmdSep)
    if os.path.exists("./lib"):
      com.append('declare -x LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH')
    com.append(command)
    com.append('declare -x appstatus=$?')
    com.append('env | grep OMP')
    com.append('exit $appstatus')
    finalCommand = ';'.join(com)
    
    self.stdError = ''    
    result = shellCall(0, finalCommand, callbackFunction = self.redirectLogOutput , bufferLimit = 20971520)
    if not result['OK']:
      LOG.error("Application failed :", result["Message"])
      return S_ERROR('Problem Executing Application')

    resultTuple = result['Value']

    status = resultTuple[0]
    # stdOutput = resultTuple[1]
    # stdError = resultTuple[2]
    LOG.info("Status after %s execution is %s" % (os.path.basename(self.script), str(status)))
    failed = False
    if status != 0:
      LOG.info("%s execution completed with non-zero status:" % os.path.basename(self.script))
      failed = True
    elif len(self.stdError) > 0:
      LOG.info("%s execution completed with application warning:" % os.path.basename(self.script))
      LOG.info(self.stdError)
    else:
      LOG.info("%s execution completed successfully:" % os.path.basename(self.script))

    if failed is True:
      LOG.error("==================================\n StdError:\n")
      LOG.error(self.stdError)
      return S_ERROR('%s Exited With Status %s' % (os.path.basename(self.script), status))

    #Above can't be removed as it is the last notification for user jobs
    self.setApplicationStatus('%s (%s %s) Successful' %(os.path.basename(self.script), self.applicationName, self.applicationVersion))
    return S_OK('%s (%s %s) Successful' % (os.path.basename(self.script), self.applicationName, self.applicationVersion))
