"""The calibration client offers the interface for calibration worker nodes to talk to the calibration service.

The worker nodes use this interface to ask for new parameters, their event slices and inform the service
about the results of their reconstruction.
"""

from DIRAC.Core.Base.Client import Client
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from ILCDIRAC.CalibrationSystem.Utilities.fileutils import binaryFileToString

__RCSID__ = "$Id$"
LOG = gLogger.getSubLogger(__name__)


class CalibrationPhase(object):
  """Represents the different phases a calibration can be in.

  Since Python 2 does not have enums, this is hardcoded for the moment.
  Should this solution not be sufficient any more, one can make a better enum implementation by hand or install
  a backport of the python3 implementation from PyPi.
  """

  ECalDigi, HCalDigi, MuonAndHCalOtherDigi, ElectroMagEnergy, HadronicEnergy, PhotonTraining = range(6)

  @staticmethod
  def phaseIDFromString(phase_name):
    """Return the ID of the given CalibrationPhase, passed as a string.

    :param basestring phase_name: Name of the CalibrationPhase. Allowed are:
                                  ECalDigi, HCalDigi, MuonAndHCalOtherDigi,
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
    """Return the ID of the given CalibrationPhase, passed as a string.

    :param basestring phase_name: Name of the CalibrationPhase. Allowed are:
                                  ECalDigi, HCalDigi, MuonAndHCalOtherDigi,
                                  ElectroMagEnergy, HadronicEnergy, PhotonTraining
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
    """Return energy of provided sample of the given CalibrationPhase, passed as a float.

    :param basestring phase_name: Name of the CalibrationPhase. Allowed are:
                                  ECalDigi, HCalDigi, MuonAndHCalOtherDigi,
                                  ElectroMagEnergy, HadronicEnergy, PhotonTraining
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
    """Return the name of the CalibrationPhase with the given ID, as a string.

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


#  @createClient('Calibration/Calibration')
class CalibrationClient(Client):
  """Handles the workflow of the worker nodes.

  Fetches the necessary data from the service, calls the calibration software to be run and reports the results back.
  """

  def __init__(self, calibrationID=None, workerID=None, **kwargs):
    """Initialize the client.

    :param workerID: ID of this worker
    :param calibrationID: ID of the calibration run this worker belongs to
    :returns: None
    """
    super(CalibrationClient, self).__init__(**kwargs)
    self.setServer('Calibration/Calibration')
    self.calibrationID = calibrationID
    self.workerID = workerID
    self.currentStep = -1  # initial parameter request
    self.currentPhase = CalibrationPhase.ECalDigi
    self.currentStage = 1
    self.parameterSet = None
    self.log = LOG
    self.ops = Operations()
    self.maximumReportTries = self.ops.getValue('Calibration/MaximumReportTries', 10)  # TODO add this to CS

  def getInputDataDict(self, calibrationID=None, workerID=None):
    """Get input data dict."""
    if calibrationID is None and self.calibrationID is None:
      return S_ERROR("Specify calibrationID")
    if workerID is None and self.workerID is None:
      return S_ERROR("Specify workerID")

    calibIDToUse = calibrationID if calibrationID is not None else self.calibrationID
    workerIDToUse = workerID if workerID is not None else self.workerID

    return self._getRPC().getInputDataDict(calibIDToUse, workerIDToUse)

  def requestNewParameters(self):
    """Fetch new parameter set from the service and updates the step counter in this object with the new value.

    Throws a ValueError if the calibration ended already.
    :returns: dict with 4 keys: calibrationIsFinished (bool), parameters (dict), currentPhase (int), currentStage (int)
    or None if no new parameters are available yet
    :rtype: list
    """
    res = self.getNewParameters(self.calibrationID, self.currentStep)
    if res['OK']:
      returnValue = res['Value']
      # FIXME calibrationRun state will be updated number of worker times while only one time is enough
      if returnValue is not None:
        self.currentPhase = returnValue['currentPhase']
        self.currentStage = returnValue['currentStage']
        self.currentStep = returnValue['currentStep']
    return res

  def requestNewPhotonLikelihood(self):
    """Get new photon likelihood file."""
    res = self.getNewPhotonLikelihood(self.calibrationID)
    if res['OK']:
      return res['Value']
    else:
      return None

  # TODO do we need this functionality?
  def jumpToStep(self, stageID, phaseID, stepID):
    """Jump to the passed step and phase.

    :param int stageID: ID of the stage to jump to
    :param int phaseID: ID of the phase to jump to, see CalibrationPhase
    :param int stepID: ID of the step to jump to
    :returns: nothing
    :rtype: None
    """
    self.currentStage = stageID
    self.currentPhase = phaseID
    self.currentStep = stepID

  def reportResult(self, outFileName):
    """Send result of calibration step at the node (.root or .xml file) to the service.

    Send the root file from PfoAnalysis or PandoraLikelihoodDataPhotonTraining.xml from photon training
    back to the service procedure

    :param outFileName: Output file from one calibration iteration
    :returns: None
    """
    attempt = 0
    resultString = binaryFileToString(outFileName)

    while attempt < self.maximumReportTries:
      res = self.submitResult(self.calibrationID, self.currentStage, self.currentPhase,
                                                 self.currentStep, self.workerID, resultString)
      if res['OK']:
        return S_OK()
      self.log.warn("Failed to submit result, try", "%s: %s " % (attempt, res['Message']))
      attempt += 1

    return S_ERROR('Could not report result back to CalibrationService.')
