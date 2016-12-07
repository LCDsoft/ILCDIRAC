"""
The calibration client offers the interface for calibration worker nodes to talk to the calibration service.
The worker nodes use this interface to ask for new parameters, their event slices and inform the service
about the results of their reconstruction
"""

#from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.DISET.RPCClient import RPCClient


__RCSID__ = "$Id$"


class CalibrationClient(object):
  """ Handles the workflow of the worker nodes. Fetches the necessary data from the service,
  calls the calibration software to be run and reports the results back.
  """

  def __init__(self, workerID, calibrationID):
    """ Initializes the client

    :param workerID: ID of this worker
    :param calibrationID: ID of the calibration run this worker belongs to
    :returns: None
    """
    self.workerID = workerID
    self.calibrationID = calibrationID
    self.calibrationService = RPCClient('Calibration/Calibration')
    self.parameterSet = None

  def requestNewParameters(self):
    """ Fetches the new parameter set from the service

    :returns: parameter set for the new step
    :rtype: #FIXME
    """
    return self.calibrationService.getNewParameters()

  def reportResult(self, result):
    """ Sends the computed histogram back to the service

    :param result: The histogram as computed by the calibration step run
    :returns: None
    """
    stepID = 1  # FIXME:find way to gather this info
    res = self.calibrationService.submitResult(self.calibrationID, stepID, self.workerID, result)
    if not res['OK']:
      pass  # FIXME: Error handling? ignore?
