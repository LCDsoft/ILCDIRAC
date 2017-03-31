"""
The calibration client offers the interface for calibration worker nodes to talk to the calibration service.
The worker nodes use this interface to ask for new parameters, their event slices and inform the service
about the results of their reconstruction
"""

import subprocess
import sys
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC import S_ERROR, gLogger
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from ILCDIRAC.CalibrationSystem.Service.CalibrationHandler import CalibrationPhase

__RCSID__ = "$Id$"


class CalibrationClient(object):
  """ Handles the workflow of the worker nodes. Fetches the necessary data from the service,
  calls the calibration software to be run and reports the results back.
  """

  def __init__(self, calibrationID, workerID):
    """ Initializes the client

    :param workerID: ID of this worker
    :param calibrationID: ID of the calibration run this worker belongs to
    :returns: None
    """
    self.calibrationID = calibrationID
    self.workerID = workerID
    self.currentPhase = CalibrationPhase.ECalDigi
    self.currentStep = -1
    self.calibrationService = RPCClient('Calibration/Calibration')
    self.parameterSet = None

  def requestNewParameters(self):
    """ Fetches the new parameter set from the service and updates the step counter in this object with the new value. Throws a ValueError if the calibration ended already.

    :returns: A string if the calibration is finished and this job should stop, else the parameter set for the new step, or None if no new parameters are available yet
    :rtype: list
    """
    res = self.calibrationService.getNewParameters(self.calibrationID, self.currentStep)
    if res['OK']:
      if isinstance(res['Value'], basestring):
        return res
      self.currentPhase = res['current_phase']
      self.currentStep = res['current_step']
      return res['Value']
    else:
      return None  # No new parameters computed yet. Wait a bit and try again.

  def jumpToStep(self, phaseID, stepID):
    """ Jumps to the passed step and phase.

    :param int phaseID: ID of the phase to jump to, see CalibrationPhase
    :param int stepID: ID of the step to jump to
    :returns: nothing
    :rtype: None
    """
    self.currentPhase = phaseID
    self.currentStep = stepID

  MAXIMUM_REPORT_TRIES = 10

  def reportResult(self, result):
    """ Sends the computed histogram back to the service

    :param result: The histogram as computed by the calibration step run
    :returns: None
    """
    attempt = 0
    while attempt < CalibrationClient.MAXIMUM_REPORT_TRIES:
      res = self.calibrationService.submitResult(self.calibrationID, self.currentPhase,
                                                 self.currentStep, self.workerID, result)
      if res['OK']:
        return
      attempt = attempt + 1
    # FIXME: Decide if this is the correct way to handle this failure
    raise IOError('Could not report result back to CalibrationService.')

#FIXME: UNUSED, DELETE


def runCalibration(calibrationID, workerID, command):
  res = subprocess.check_output(['echo', 'Hello World!'])
  gLogger.warn('printed: %s', res)

#FIXME: UNUSED, DELETE
def runCalibration2( calibrationID, workerID, command ):
  """ Executes the calibration on this worker.

  :param string command: The command to start the calibration, when one appends the current histogram to it.
  :returns: exit code of the calibration, 0 in case of success
  :rtype: int
  """
  calibration_client = CalibrationClient(calibrationID, workerID)
  current_step = 1
  while True:  # FIXME: Find stoppig criterion
    try:
      current_params = calibration_client.requestNewParameters(current_step)
    except ValueError:
      gLogger.warn('Ending calibration run on this worker.')
      break
    subprocess.check_output([command, current_params])  # FIXME: Ensure this is how we can pass the new parameter
    #FIXME: This currently lacks the information at which offset the worker performs the calibration.
    #Suggested fix: Write class WorkerInfo as a wrapper for the offset+the histogram+anything else this might need
    current_step = calibration_client.currentStep


def createCalibration(steeringFile, softwareVersion, inputFiles, numberOfJobs):
  """ Starts a calibration.

  :param basestring steeringFile: Steering file used in the calibration
  :param basestring softwareVersion: Version of the software
  :param inputFiles: Input files for the calibration
  :type inputFiles: `python:list`
  :param int numberOfJobs: Number of jobs this service will run (actual number will be slightly lower)
  :returns: S_OK containing ID of the calibration, or S_ERROR if something went wrong
  :rtype: dict
  """
  res = getProxyInfo()
  if not res['OK'] or 'group' not in res['Value'] or 'username' not in res['Value']:
    err = S_ERROR('Problem with the proxy, need to know user and group: %s' % res)
    return err
  calibrationService = RPCClient('Calibration/Calibration')
  return calibrationService.createCalibration(steeringFile, softwareVersion, inputFiles, numberOfJobs,
                                              res['Value']['username'], res['Value']['group'])

if __name__ == '__main__':
  if len(sys.argv) < 4:
    print 'Not enogh arguments!\nUsage: calibrationID workerID command\nPassed parameters were %s' % sys.argv
    exit(2)
  # Assume argv[1] is calibrationID, argv[2] is workerID, rest is histogram
  exit(runCalibration(sys.argv[1], sys.argv[2], sys.argv[3:]))
