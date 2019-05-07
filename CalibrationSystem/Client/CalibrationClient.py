"""
The calibration client offers the interface for calibration worker nodes to talk to the calibration service.
The worker nodes use this interface to ask for new parameters, their event slices and inform the service
about the results of their reconstruction
"""

from DIRAC.Core.Base.Client import Client
from DIRAC import S_OK, S_ERROR, gLogger
from ILCDIRAC.CalibrationSystem.Utilities.fileutils import binaryFileToString
from ILCDIRAC.CalibrationSystem.Client.DetectorSettings import CalibrationSettings

__RCSID__ = "$Id$"
LOG = gLogger.getSubLogger(__name__)


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
    if phaseID == CalibrationPhase.ECalDigi:
      return 'ECalDigi'
    elif phaseID == CalibrationPhase.HCalDigi:
      return 'HCalDigi'
    elif phaseID == CalibrationPhase.MuonAndHCalOtherDigi:
      return 'MuonAndHCalOtherDigi'
    elif phaseID == CalibrationPhase.ElectroMagEnergy:
      return 'ElectroMagEnergy'
    elif phaseID == CalibrationPhase.HadronicEnergy:
      return 'HadronicEnergy'
    elif phaseID == CalibrationPhase.PhotonTraining:
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
    self.currentStep = -1  # initial parameter request
    self.currentPhase = CalibrationPhase.ECalDigi
    self.currentStage = 1
    self.calibrationService = Client()
    self.calibrationService.setServer('Calibration/Calibration')
    self.parameterSet = None
    self.log = LOG

  def getInputDataDict(self):
    return self.calibrationService.getInputDataDict(self.calibrationID, self.workerID)

  def requestNewParameters(self):
    """ Fetches the new parameter set from the service and updates the step counter in this object with the new value.
    Throws a ValueError if the calibration ended already.

    :returns: dict with 4 keys: calibrationIsFinished (bool), parameters (dict), currentPhase (int), currentStage (int)
    or None if no new parameters are available yet
    :rtype: list
    """
    res = self.calibrationService.getNewParameters(self.calibrationID, self.currentStep)
    if res['OK']:
      returnValue = res['Value']
      if returnValue is not None:
        self.currentPhase = returnValue['currentPhase']
        self.currentStage = returnValue['currentStage']
        self.currentStep = returnValue['currentStep']
    return res

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

  # TODO read this constant from CS
  MAXIMUM_REPORT_TRIES = 10
  def reportResult(self, outFileName):
    """ Sends the root file from PfoAnalysis or PandoraLikelihoodDataPhotonTraining.xml from photon training
    back to the service procedure

    :param outFileName: Output file from one calibration iteration
    :returns: None
    """
    attempt = 0

    resultString = binaryFileToString(outFileName)

    while attempt < CalibrationClient.MAXIMUM_REPORT_TRIES:
      res = self.calibrationService.submitResult(self.calibrationID, self.currentStage, self.currentPhase,
                                                 self.currentStep, self.workerID, resultString)
      if res['OK']:
        return S_OK()
      self.log.warn("Failed to submit result, try %s: %s " % (attempt, res['Message']))
      attempt += 1

    return S_ERROR('Could not report result back to CalibrationService.')


def createCalibration(inputFiles, calibSettings):
  """ Starts a calibration.

  :param inputFiles: Input files for the calibration: dictionary of keys GAMMA, KAON, and MUON to list of lfns
                     for each particle type
  :param calibSettings: ???
  :returns: S_OK containing ID of the calibration, or S_ERROR if something went wrong
  :rtype: dict
  """

  if not isinstance(inputFiles, dict) or not isinstance(calibSettings, CalibrationSettings):
    errMsg = ("Wrong types of input arguments. Types should be (dict, CalibrationSettings)."
              "Types of provided arguments: (%s, %s)" % (type(inputFiles), type(calibSettings)))
    LOG.error(errMsg)
    return S_ERROR(errMsg)

  inputFileTypes = ['gamma', 'kaon', 'muon', 'zuds']
  if not set(inputFileTypes).issubset([iEl.lower() for iEl in inputFiles.keys()]):
    errMsg = ('Wrong input data. Dict inputFiles should have following keys: %s; provided dictionary has keys: %s'
              % (inputFileTypes, inputFiles.keys()))
    LOG.error(errMsg)
    return S_ERROR(errMsg)

  requiredSettingFields = CalibrationSettings().settingsDict.keys()
  providedSettingFields = calibSettings.settingsDict.keys()

  if (len(requiredSettingFields) != len(providedSettingFields)
          or not set(requiredSettingFields).issubset(providedSettingFields)):
    errMsg = ('calibSettings should contain %s fields. Numer of provided fields: %s.\nRequired fields: %s\nProvided fields: %s'
              % (len(requiredSettingFields), len(providedSettingFields), requiredSettingFields, providedSettingFields))
    LOG.error(errMsg)
    return S_ERROR("calibration setting doesn't contain")

  calibrationService = Client()
  calibrationService.setServer('Calibration/Calibration')
  return calibrationService.createCalibration(inputFiles, dict(calibSettings.settingsDict))


def killCalibration(calibId):
  calibrationService = Client()
  calibrationService.setServer('Calibration/Calibration')
  return calibrationService.killCalibration(calibId)


def getCalibrationStatuses():
  calibrationService = Client()
  calibrationService.setServer('Calibration/Calibration')
  return calibrationService.getUserCalibrationStatuses()
