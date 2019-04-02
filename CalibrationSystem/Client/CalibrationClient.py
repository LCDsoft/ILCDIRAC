"""
The calibration client offers the interface for calibration worker nodes to talk to the calibration service.
The worker nodes use this interface to ask for new parameters, their event slices and inform the service
about the results of their reconstruction
"""

import subprocess
import sys
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from ILCDIRAC.CalibrationSystem.Utilities.fileutils import binaryFileToString

__RCSID__ = "$Id$"


class CalibrationPhase(object):
  """ Represents the different phases a calibration can be in.
  Since Python 2 does not have enums, this is hardcoded for the moment.
  Should this solution not be sufficient any more, one can make a better enum implementation by hand or install
  a backport of the python3 implementation from PyPi."""
  ECalDigi, HCalDigi, MuonAndHCalOtherDigi, ElectroMagEnergy, HadronicEnergy, PhotonTraining = range(6)

  @staticmethod
  def phaseIDFromString(phase_name):
    """ Returns the ID of the given CalibrationPhase, passed as a string.

    :param basestring phase_name: Name of the CalibrationPhase. Allowed are: ECalDigi, HCalDigi, MuonAndHCalOtherDigi,
    ElectroMagEnergy, HadronicEnergy, PhotonTraining
    :returns: ID of this phase
    :rtype: int
    """
    if phase_name == 'ECalDigi':
      return 0
    elif phase_name == 'HCalDigi':
      return 1
    elif phase_name == 'MuonAndHCalOtherDigi':
      return 2
    elif phase_name == 'ElectroMagEnergy':
      return 3
    elif phase_name == 'HadronicEnergy':
      return 4
    elif phase_name == 'PhotonTraining':
      return 5
    else:
      raise ValueError('There is no CalibrationPhase with the name %s' % phase_name)

  @staticmethod
  def fileKeyFromPhase(phaseID):
    """ Returns the ID of the given CalibrationPhase, passed as a string.

    :param basestring phase_name: Name of the CalibrationPhase. Allowed are: ECalDigi, HCalDigi, HCalOtherDigi,
    MuonDigi, ElectroMagEnergy, HadronicEnergy, PhotonTraining
    :returns: file key for this phase
    :rtype: str
    """
    if phaseID == CalibrationPhase.ECalDigi:
      return "GAMMA"
    elif phaseID == CalibrationPhase.HCalDigi:
      return "KAON"
    elif phaseID == CalibrationPhase.MuonAndHCalOtherDigi:
      return "MUON"
    elif phaseID == CalibrationPhase.ElectroMagEnergy:
      return "GAMMA"
    elif phaseID == CalibrationPhase.HadronicEnergy:
      return "KAON"
    elif phaseID == CalibrationPhase.PhotonTraining:
      return "ZUDS"
    else:
      raise ValueError('There is no CalibrationPhase with the ID %s' % phaseID)

  # TODO read these energies from CS or from users input
  @staticmethod
  def sampleEnergyFromPhase(phaseID):
    """ Returns energy of provided sample of the given CalibrationPhase, passed as a float.

    :param basestring phase_name: Name of the CalibrationPhase. Allowed are: ECalDigi, HCalDigi, HCalOtherDigi,
    MuonDigi, ElectroMagEnergy, HadronicEnergy, PhotonTraining
    :returns: file key for this phase
    :rtype: str
    """
    if phaseID == CalibrationPhase.ECalDigi:
      return 10.0
    elif phaseID == CalibrationPhase.HCalDigi:
      return 50.0
    elif phaseID == CalibrationPhase.MuonAndHCalOtherDigi:
      return 10.0
    elif phaseID == CalibrationPhase.ElectroMagEnergy:
      return 10.0
    elif phaseID == CalibrationPhase.HadronicEnergy:
      return 50.0
    elif phaseID == CalibrationPhase.PhotonTraining:
      return 200.0
    else:
      raise ValueError('There is no CalibrationPhase with the ID %s' % phaseID)

  @staticmethod
  def phaseNameFromID(phaseID):
    """ Returns the name of the CalibrationPhase with the given ID, as a string

    :param int phaseID: ID of the enquired CalibrationPhase
    :returns: The name of the CalibrationPhase
    :rtype: basestring
    """
    if phaseID == 0:
      return 'ECalDigi'
    elif phaseID == 1:
      return 'HCalDigi'
    elif phaseID == 2:
      return 'MuonAndHCalOtherDigi'
    elif phaseID == 3:
      return 'ElectroMagEnergy'
    elif phaseID == 4:
      return 'HadronicEnergy'
    elif phaseID == 5:
      return 'PhotonTraining'
    else:
      raise ValueError('There is no CalibrationPhase with the name %d' % phaseID)


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
    self.currentStep = -1  # counter of how much Marlin have been run on worker nodes
    self.currentPhase = CalibrationPhase.ECalDigi
    self.currentStage = 1
    self.calibrationService = RPCClient('Calibration/Calibration')
    self.parameterSet = None
    self.log = gLogger.getSubLogger("CalibrationClient")

  def requestNewParameters(self):
    """ Fetches the new parameter set from the service and updates the step counter in this object with the new value.
    Throws a ValueError if the calibration ended already.

    :returns: dict with 4 keys: calibrationIsFinished (bool), parameters (dict), currentPhase (int), currentStage (int)
    or None if no new parameters are available yet
    :rtype: list
    """
    gLogger.info('execute requestNewParameters')
    res = self.calibrationService.getNewParameters(self.calibrationID, self.currentStep)
    gLogger.info('requestNewParameters: res: %s' % res)
    if res['OK']:
      returnValue = res['Value']
      if not set(['calibrationIsFinished', 'parameters', 'currentPhase', 'currentStage']).issubset(returnValue.keys()):
        self.log.error('Corrupted structure of outcome data after request to Calibration service for new parameters')
        # FIXME should we return None in this case?
        return None
      self.currentPhase = returnValue['currentPhase']
      self.currentStage = returnValue['currentStage']
      return returnValue
    else:
      return None  # No new parameters computed yet. Wait a bit and try again.

  def requestNewPhotonLikelihood(self):
   res = self.calibrationService.getNewPhotonLikelihood(self.calibrationID)
   if res['OK']:
     return res['Value']
   else:
     return None

  #TODO do we need this functionality?
  def jumpToStep(self, stageID, phaseID, stepID):
    """ Jumps to the passed step and phase.

    :param int stageID: ID of the stage to jump to
    :param int phaseID: ID of the phase to jump to, see CalibrationPhase
    :param int stepID: ID of the step to jump to
    :returns: nothing
    :rtype: None
    """
    self.currentStage = stageID
    self.currentPhase = phaseID
    self.currentStep = stepID

  MAXIMUM_REPORT_TRIES = 10

  def reportResult(self, outFileName):
    """ Sends the root file from PfoAnalysis or PandoraLikelihoodDataPhotonTraining.xml from photon training back to the service procedure

    :param outFileName: Output file from one calibration iteration
    :returns: None
    """
    attempt = 0

    resultString = binaryFileToString(outFileName)
    self.currentStep = self.currentStep + 1

    while attempt < CalibrationClient.MAXIMUM_REPORT_TRIES:
      res = self.calibrationService.submitResult(self.calibrationID, self.currentStage, self.currentPhase,
                                                 self.currentStep, self.workerID, resultString)
      if res['OK']:
        return S_OK()
      self.log.warn("Failed to submit result, try %s: %s " % (attempt, res['Message']))
      attempt += 1

    return S_ERROR('Could not report result back to CalibrationService.')


def createCalibration(ilcsoftPath, inputFiles, numberOfJobs, marlinVersion, steeringFile, detectorModel):
  """ Starts a calibration.

  :param basestring steeringFile: Steering file used in the calibration
  :param basestring softwareVersion: Version of the software
  :param inputFiles: Input files for the calibration: dictionary of keys GAMMA, KAON, and MUON to list of lfns for each particle type
  :type inputFiles: `python:dict`
  :param int numberOfJobs: Number of jobs this service will run (actual number will be slightly lower)
  :returns: S_OK containing ID of the calibration, or S_ERROR if something went wrong
  :rtype: dict
  """

  calibrationService = RPCClient('Calibration/Calibration')
  if not isinstance(inputFiles, dict):
    gLogger.error("inputFiles is not a dictionary")
    return S_ERROR("badParameter")

  return calibrationService.createCalibration(ilcsoftPath, inputFiles, numberOfJobs, marlinVersion, steeringFile, detectorModel)


#FIXME is this for testing?
if __name__ == '__main__':
  if len(sys.argv) < 4:
    print 'Not enogh arguments!\nUsage: calibrationID workerID command\nPassed parameters were %s' % sys.argv
    exit(2)
  # Assume argv[1] is calibrationID, argv[2] is workerID, rest is histogram
  exit(runCalibration(sys.argv[1], sys.argv[2], sys.argv[3:]))
