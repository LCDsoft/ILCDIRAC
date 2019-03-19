'''
Run Whizard2

:author: Marko Petric
:since:  June 29, 2015
'''

import os

from DIRAC.Core.Utilities.Subprocess                      import shellCall
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC                                                import S_OK, S_ERROR, gLogger
from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import getEnvironmentScript, extractTarball
from ILCDIRAC.Core.Utilities.resolvePathsAndNames         import getProdFilename

__RCSID__ = '$Id$'
LOG = gLogger.getSubLogger(__name__)


class Whizard2Analysis(ModuleBase):
  """
  Specific Module to run a Whizard2 job.
  """
  def __init__(self):
    super(Whizard2Analysis, self).__init__()
    self.enable = True
    self.STEP_NUMBER = ''
    self.result = S_ERROR()
    self.applicationName = 'whizard2'
    self.startFrom = 0
    self.randomSeed = -1
    self.whizard2SinFile = ''
    self.eventstring = ['+++ Generating event']
    self.decayProc = ['decay_proc']
    self.integratedProcess = ''
    self.datMan = DataManager()

  def applicationSpecificInputs(self):
    """ Resolve all input variables for the module here.

    :return: S_OK()
    """
    self.randomSeed = self._determineRandomSeed()

    if "IS_PROD" in self.workflow_commons and self.workflow_commons["IS_PROD"]:
      self.OutputFile = getProdFilename(self.OutputFile,
                                        int(self.workflow_commons["PRODUCTION_ID"]),
                                        int(self.workflow_commons["JOB_ID"]),
                                        self.workflow_commons,
                                       )

    return S_OK('Parameters resolved')

  def resolveIntegratedProcess(self):
    """Check if integrated process is set and act accordingly.

    If the integrated process was given as a tarball it should already be available in the working directory and we do
    nothing.
    """
    if not self.integratedProcess:
      return S_OK()

    # integratedProcess is set, check CVMFS or filecatalog
    processes = self.ops.getOptionsDict('/AvailableTarBalls/%s/whizard2/%s/integrated_processes/processes' %
                                        ('x86_64-slc5-gcc43-opt', self.applicationVersion))

    if not processes['OK']:
      LOG.error('Could not resolve known integrated processes', processes['Message'])
      return processes

    options = self.ops.getOptionsDict('/AvailableTarBalls/%s/whizard2/%s/integrated_processes' %
                                      ('x86_64-slc5-gcc43-opt', self.applicationVersion))
    if not options['OK']:
      LOG.error('Failed to get integrated processes options', options['Message'])
      return options

    cvmfsPath = options['Value'].get('CVMFSPath', '')
    tarballURL = options['Value'].get('TarBallURL', '')
    processTarball = processes['Value'].get(self.integratedProcess, '')

    localTarball = os.path.join(cvmfsPath, processTarball)
    if os.path.exists(localTarball):
      LOG.info('Tarball found on cvmfs: %r' % localTarball)
      return extractTarball(localTarball, os.getcwd())

    tarballLFN = os.path.join(tarballURL, processTarball)
    LOG.info('Trying to download tarball', tarballLFN)
    getFile = self.datMan.getFile(tarballLFN)
    if not getFile['OK']:
      LOG.error('Failed to download tarball', getFile['Message'])
      return getFile
    return extractTarball(os.path.split(tarballLFN)[1], os.getcwd())

  def runIt(self):
    """
    Called by JobAgent

    Execute the following:
      - get the environment variables that should have been set during installation
      - prepare the steering file and command line parameters
      - run Whizard2 on this steering file and catch the exit status

    :rtype: :func:`~DIRAC.Core.Utilities.ReturnValues.S_OK`, :func:`~DIRAC.Core.Utilities.ReturnValues.S_ERROR`
    """
    self.result = S_OK()
    if not self.platform:
      self.result = S_ERROR( 'No ILC platform selected' )
    elif not self.applicationLog:
      self.result = S_ERROR( 'No Log file provided' )
    if not self.result['OK']:
      LOG.error("Failed to resolve input parameters:", self.result['Message'])
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      LOG.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('Whizard2 should not proceed as previous step did not end properly')

    resIntProc = self.resolveIntegratedProcess()
    if not resIntProc['OK']:
      return resIntProc

    # get the enviroment script
    res = getEnvironmentScript(self.platform, self.applicationName, self.applicationVersion, S_ERROR("No init script provided in CVMFS!"))
    if not res['OK']:
      LOG.error("Could not obtain the environment script: ", res["Message"])
      return res
    envScriptPath = res["Value"]

    whizard2SteerName = 'Whizard2_%s_Steer_%s.sin'  % (self.applicationVersion, self.STEP_NUMBER)
    if os.path.exists(whizard2SteerName):
      os.remove(whizard2SteerName)

    whizard2Steer = []
    whizard2Steer.append('!Seed set via API')
    whizard2Steer.append('seed = %s' % self.randomSeed)
    whizard2Steer.append('')
    whizard2Steer.append('!Parameters set via whizard2SinFile')
    whizard2Steer.append('')
    whizard2Steer.append(self.whizard2SinFile)
    whizard2Steer.append('')
    whizard2Steer.append('!Number of events set via API')
    whizard2Steer.append('')
    whizard2Steer.append('n_events = %s' % self.NumberOfEvents)
    whizard2Steer.append('')
    whizard2Steer.append('simulate (%s) {' % ",".join(self.decayProc))
    whizard2Steer.append('        $sample = "%s"' % self.OutputFile.rsplit('.',1)[0] )
    if self.OutputFile.rsplit('.',1)[-1] == 'slcio':
      whizard2Steer.append('        sample_format = lcio')
      whizard2Steer.append('        $extension_lcio = "slcio"')
    else:
      whizard2Steer.append('        sample_format = %s' % self.OutputFile.rsplit('.',1)[-1] )
      whizard2Steer.append('        $extension_{st} = "{st}"'.format(st=self.OutputFile.rsplit('.',1)[-1]))
    whizard2Steer.append('}')
    
    with open(whizard2SteerName, 'w') as steerFile:
      steerFile.write( "\n".join(whizard2Steer) )

    scriptName = 'Whizard2_%s_Run_%s.sh' % (self.applicationVersion, self.STEP_NUMBER)
    if os.path.exists(scriptName):
      os.remove(scriptName)
    script = []
    script.append('#!/bin/bash')
    script.append('#####################################################################')
    script.append('# Dynamically generated script to run a production or analysis job. #')
    script.append('#####################################################################')
    script.append('source %s' % envScriptPath)
    script.append('echo =========')
    script.append('env | sort >> localEnv.log')
    script.append('echo whizard:`which whizard`')
    script.append('echo =========')
    script.append('whizard %s' % whizard2SteerName )
    script.append('declare -x appstatus=$?')
    script.append('exit $appstatus')

    with open(scriptName, 'w') as scriptFile:
      scriptFile.write( "\n".join(script) )

    if os.path.exists(self.applicationLog):
      os.remove(self.applicationLog)

    os.chmod(scriptName, 0o755)
    comm = 'bash "./%s"' % scriptName
    self.setApplicationStatus('Whizard2 %s step %s' % (self.applicationVersion, self.STEP_NUMBER))
    self.stdError = ''
    self.result = shellCall(0, comm, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)
    resultTuple = self.result['Value']
    if not os.path.exists(self.applicationLog):
      LOG.error("Something went terribly wrong, the log file is not present")
      self.setApplicationStatus('%s failed to produce log file' % (self.applicationName))
      if not self.ignoreapperrors:
        return S_ERROR('%s did not produce the expected log %s' % (self.applicationName, self.applicationLog))
    status = resultTuple[0]

    LOG.info("Status after the application execution is %s" % status)

    return self.finalStatusReport(status)

  def _determineRandomSeed(self):
    """determine what the randomSeed should be, depends on production or not

    .. Note::
      Whizard2 we use *randomSeed* and not *RandomSeed* as in the other workflow modules

    """
    if self.randomSeed == -1:
      self.randomSeed = self.jobID
    if "IS_PROD" in self.workflow_commons:
      self.randomSeed = int(str(int(self.workflow_commons["PRODUCTION_ID"])) + str(int(self.workflow_commons["JOB_ID"])))
    return self.randomSeed
