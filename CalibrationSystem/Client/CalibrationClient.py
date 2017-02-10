"""
The calibration client offers the interface for calibration worker nodes to talk to the calibration service.
The worker nodes use this interface to ask for new parameters, their event slices and inform the service
about the results of their reconstruction
"""

import subprocess
import sys
#from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.DISET.RPCClient import RPCClient


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
    self.currentStep = -1
    self.calibrationService = RPCClient('Calibration/Calibration')
    self.parameterSet = None

  def requestNewParameters(self, stepID):
    """ Fetches the new parameter set from the service and updates the step counter in this object with the new value.

    :param int stepID: ID of the step the worker finished last.
    :returns: A string if the calibration is finished and this job should stop, else the parameter set for the new step, or None if no new parameters are available yet
    :rtype: list
    """
    res = self.calibrationService.getNewParameters(self.calibrationID, stepID)
    if res['OK']:
      self.currentStep = res['current_step']
      return res['Value']
    else:
      return None  # No new parameters computed yet. Wait a bit and try again.

  MAXIMUM_REPORT_TRIES = 10

  def reportResult(self, stepID, result):
    """ Sends the computed histogram back to the service

    :param int stepID: ID of the step the worker was working on up to now.
    :param result: The histogram as computed by the calibration step run
    :returns: None
    """
    attempt = 0
    while attempt < CalibrationClient.MAXIMUM_REPORT_TRIES:
      res = self.calibrationService.submitResult(self.calibrationID, stepID, self.workerID, result)
      if res['OK']:
        return
      attempt = attempt + 1
    print ''  # FIXME: Error handling? ignore?


def runCalibration(calibrationID, workerID, command):
  """ Executes the calibration on this worker.

  :param string command: The command to start the calibration, when one appends the current histogram to it.
  :returns: exit code of the calibration, 0 in case of success
  :rtype: int
  """
  calibration_client = CalibrationClient(calibrationID, workerID)
  current_step = 1
  while True:  # FIXME: Find stoppig criterion
    current_params = calibration_client.requestNewParameters(current_step)
    subprocess.check_output([command, current_params])  # FIXME: Ensure this is how we can pass the new parameter
    #FIXME: This currently lacks the information at which offset the worker performs the calibration.
    #Suggested fix: Write class WorkerInfo as a wrapper for the offset+the histogram+anything else this might need
    current_step = calibration_client.currentStep


if __name__ == '__main__':
  # Assume argv[1] is calibrationID, argv[2] is workerID, rest is histogram
  exit(runCalibration(sys.argv[1], sys.argv[2], sys.argv[3:]))
