"""
The CalibrationHandler controls all calibration runs.

The CalibrationHandler collates (calibration results) and distributes (input parameters) information from
the calibration worker nodes and allows the creation of calibration runs. It will (re-)submit jobs and
distribute reconstruction workloads among them.
"""

import os
import re
import math
import shutil
import glob
from datetime import datetime
from datetime import timedelta

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import DErrno
from ILCDIRAC.CalibrationSystem.Service.CalibrationRun import CalibrationRun
from ILCDIRAC.CalibrationSystem.Utilities.fileutils import stringToBinaryFile
from ILCDIRAC.CalibrationSystem.Utilities.functions import loadCalibrationRun
from ILCDIRAC.CalibrationSystem.Utilities.functions import printSet
from ILCDIRAC.CalibrationSystem.Utilities.functions import saveCalibrationRun
from ILCDIRAC.CalibrationSystem.Utilities.functions import splitFilesAcrossJobs
from ILCDIRAC.CalibrationSystem.Service.DetectorSettings import CalibrationSettings
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from ILCDIRAC.CalibrationSystem.Utilities.functions import calibration_creation_failed

__RCSID__ = "$Id$"
LOG = gLogger.getSubLogger(__name__)


class WorkerInfo(object):
  """Wrapper class to store information needed by workers to compute their result."""

  def __init__(self, parameterSet, offset):
    """Create a new WorkerInfo object, passed to the worker nodes to enable them to compute their results.

    :param object parameterSet: The histogram to use for the calibration.
    :param int offset: The offset in the event file, used to determine starting point of this computation.
    """
    self.parameterSet = parameterSet
    self.offset = offset

  def getInfo(self):
    """Return worker info."""
    return (self.parameterSet, self.offset)


class CalibrationHandler(RequestHandler):
  """Handles the information exchange between worker nodes and the service."""

  @classmethod
  def initializeHandler(cls, _):
    """Initialize the handler, setting required variables. Called once in the beginning of the service."""
    cls.activeCalibrations = {}
    cls.idsOfCalibsToBeKilled = []
    cls.calibrationCounter = CalibrationHandler.loadStatus()
    cls.log = LOG
    cls.ops = Operations()
    # FIXME this parameter should be read from CS
    cls.timeToKeepCalibrationResultsInMinutes = cls.ops.getValue('Calibration/timeToKeepCalibrationResultsInMinutes',
                                                                 7 * 24 * 60)  # TODO add this to CS

    # try to find not finished calibrations
    notFinishedCalibIDs = [int(re.findall(r'\d+', x)[0]) for x in glob.glob('calib*')]
    if len(notFinishedCalibIDs) > 0:
      cls.log.info('Recovering calibrations after restart of CalibrationSystem service...')

    for iCalibID in notFinishedCalibIDs:
      tmpCalibRun = loadCalibrationRun(iCalibID)
      if tmpCalibRun is None:
        errMsg = ("Can't recover calibration #%s. No dump file is found in the calibration working directory."
                  " Deteting the directory." % iCalibID)
        cls.log.error(errMsg)
        shutil.rmtree('calib%s' % iCalibID)
      else:
        CalibrationHandler.activeCalibrations[iCalibID] = tmpCalibRun

    if len(notFinishedCalibIDs) > 0:
      cls.log.always('Recovering is finished. Managed to recover following calibrations: %s. Detailed info:' %
                     CalibrationHandler.activeCalibrations.keys())
      for _, iCalib in CalibrationHandler.activeCalibrations.iteritems():
        cls.log.always('Calib #%s: isFinished: %s; stage: %s; phase: %s; step: %s' % (iCalib.calibrationID,
                                                                                      iCalib.calibrationFinished,
                                                                                      iCalib.currentStage,
                                                                                      iCalib.currentPhase,
                                                                                      iCalib.currentStep))

    # TODO ask Andre if we want to stop service or just delete calibratino with ID greate than calibrationCounter?
    if max(notFinishedCalibIDs or [0]) > cls.calibrationCounter:
      errMsg = ('Something went wrong during an attempt to pickup unfinished calibrations during CalibrationHandler'
                ' initialization. calibrationCounter: %s is behind one of the picked up calibration IDs: %s\n Stop the'
                ' service!'
                % (cls.calibrationCounter, notFinishedCalibIDs))
      cls.log.error(errMsg)
      return S_ERROR(errMsg)

    return S_OK()

  def saveStatus(self):
    """Save id of the last calibration to file."""
    fileName = "status"
    with open(fileName, 'w') as f:
      f.write("%s" % self.calibrationCounter)

  @staticmethod
  def loadStatus():
    """Read id of the last calibration from the file."""
    fileName = "status"
    if os.path.exists(fileName):
      with open(fileName, 'r') as f:
        return int(f.readlines()[0])
    else:
      return 0

  def _getUsernameAndGroup(self):
    """Return name of the group and name of the user of the proxy the user is currently having.

    :returns: S_OK with value being dict with 'group' and 'username' entries or S_ERROR
    :rtype: `python:dict`
    """
    credDict = self.getRemoteCredentials()
    if 'DN' not in credDict or 'username' not in credDict:
      return S_ERROR("You must be authenticated!")
    if not credDict['isProxy']:
      return S_ERROR(DErrno.EX509, "chain does not contain a proxy")
    usernameAndGroupDict = {'group': credDict['group'], 'username': credDict['username']}
    return S_OK(usernameAndGroupDict)

  def _checkClientRequest(self, calibId):
    """Check if calibration with input ID exists and if it belongs to the user who made the request.

    :returns: S_OK w/o any message if calibration exists and user credentials are correct
              S_OK with message if calibration with such ID doesn't exist
              S_ERROR if impossible to retrieve user proxy or if user credentials are not correct
    :rtype: `python:dict`
    """
    activeCalibrations = list(CalibrationHandler.activeCalibrations.keys())
    if calibId not in activeCalibrations:
      return S_OK('No calibration with ID: %s was found. Active calibrations: %s' % (calibId, activeCalibrations))

    res = self._getUsernameAndGroup()
    if not res['OK']:
      return S_ERROR('Error while retrieving proxy user name or group.')
    usernameAndGroup = res['Value']

    calibration = CalibrationHandler.activeCalibrations[calibId]
    if calibration.proxyUserName == usernameAndGroup['username'] \
       and calibration.proxyUserGroup == usernameAndGroup['group']:
      return S_OK()
    else:
      return S_ERROR('Permission denied. Calibration with ID %s has been created by other user.' % calibId)

  def __checkForRequiredFields(self, requiredFields, providedFields, errMsg=""):
    if set(requiredFields) != set(providedFields):
      errMsg = ("%s Missing fields: %s; unused extra fields: %s"
                % (errMsg,
                   printSet(set(requiredFields) - set(providedFields)),
                   printSet(set(providedFields) - set(requiredFields))))
      self.log.error(errMsg)
      return S_ERROR(errMsg)
    return S_OK()

  # TODO split this function to two: first one just create CalibrationRun instance and returns it (to allow user to
  # setup different settings); second one - submits jobs
  auth_createCalibration = ['authenticated']
  types_createCalibration = [dict, dict, dict]

  def export_createCalibration(self, inputFiles, numberOfEventsPerFile, calibSettingsDict):
    """Create calibration run (series of calibration iterations).

    :param basestring marlinVersion: Version of the Marlin application to be used for reconstruction
    :param dict inputFiles: Input files for the calibration. Dictionary.
    :type inputFiles: `python:dict`
    :param dict numberOfEventsPerFile: Number of events per type of input file. Dictionary.
    :type  numberOfEventsPerFile: `python:dict`
    :param int numberOfJobs: Number of jobs this service will run (actual number will be slightly lower)
    :param basestring steeringFile: Steering file used in the calibration, LFN
    :returns: S_OK containing ID of the calibration, used to retrieve results etc
    :rtype: dict
    """
    inputFiles = {iKey.lower(): iVal for iKey, iVal in inputFiles.iteritems()}
    numberOfEventsPerFile = {iKey.lower(): iVal for iKey, iVal in numberOfEventsPerFile.iteritems()}

    for iKey, iVal in calibSettingsDict.items():
      keysWithNoneValues = []
      if iVal is None:
        keysWithNoneValues.append(iKey)
      if keysWithNoneValues:
        errMsg = "Following settings have None values: %s. All settings have to be set up." % keysWithNoneValues
        return S_ERROR(errMsg)

    requiredFields = ['gamma', 'kaon', 'muon', 'zuds']
    providedFields = inputFiles.keys()
    res = self.__checkForRequiredFields(requiredFields, providedFields,
                                        "Dict of input files doesn't contains all required fields.")
    if not res['OK']:
      return res

    providedFields = numberOfEventsPerFile.keys()
    res = self.__checkForRequiredFields(requiredFields, providedFields,
                                                    "Dict of number of events per type of input file doesn't contains"
                                                    " all required fields.")
    if not res['OK']:
      return res

    for iKey, iVal in numberOfEventsPerFile.iteritems():
      if not isinstance(iVal, int) or iVal <= 0:
        errMsg = "Wrong values for numberOfEventsPerFile dict for input file type: %s" % iKey
        self.log.error(errMsg)
        return S_ERROR(errMsg)

    requiredFields = set(CalibrationSettings().settingsDict.keys())
    providedFields = set(calibSettingsDict.keys())
    res = self.__checkForRequiredFields(requiredFields, providedFields,
                                        "Dict of calibration settings doesn't contains all required fields.")
    if not res['OK']:
      return res

    for iKey, iVal in numberOfEventsPerFile.iteritems():
      if iVal * len(inputFiles[iKey]) < calibSettingsDict['numberOfJobs']:
        errMsg = ("number of jobs (%s jobs) is larger than total number of provided events (%s events) for file type:"
                  " %s" % (calibSettingsDict['numberOfJobs'], iVal * len(inputFiles[iKey]), iKey))
        self.log.error(errMsg)
        return S_ERROR(errMsg)

    groupedInputFiles = splitFilesAcrossJobs(inputFiles, numberOfEventsPerFile, calibSettingsDict['numberOfJobs'])

    res = self.__inputVariablesSanityCheck(calibSettingsDict)
    if not res['OK']:
      return res

    CalibrationHandler.calibrationCounter += 1
    self.saveStatus()
    calibrationID = CalibrationHandler.calibrationCounter
    newRun = CalibrationRun(calibrationID, groupedInputFiles, calibSettingsDict)
    CalibrationHandler.activeCalibrations[calibrationID] = newRun

    res = self._getUsernameAndGroup()
    if not res['OK']:
      return S_ERROR('Error while retrieving proxy user name or group. CalibrationID = %s'
                     % (calibrationID))
    usernameAndGroup = res['Value']
    self.log.info('Retrieve user proxy - userName = %s, userGroup = %s'
                  % (usernameAndGroup['username'], usernameAndGroup['group']))

    newRun.proxyUserName = usernameAndGroup['username']
    newRun.proxyUserGroup = usernameAndGroup['group']

    res = newRun.submitJobs(proxyUserName=newRun.proxyUserName, proxyUserGroup=newRun.proxyUserGroup)
    if isinstance(res, dict):
      self.log.error('Error while submitting jobs. Res: %s' % res)
      return res
    self.log.info('results from submitJobs: %s' % res)
    if calibration_creation_failed(res):
      ret_val = S_ERROR('Submitting at least one of the jobs failed')  # FIXME: This should be treated, since the
      # successfully submitted jobs will still run
      ret_val['calibrations'] = res
      return ret_val
    return S_OK((calibrationID, res))

  auth_killCalibrations = ['authenticated']
  types_killCalibrations = [list]

  def export_killCalibrations(self, inList):
    """Kill set of calibrations.

    :returns: S_ERROR if wrong input,
              S_OK with 'Value' parameter being a dict of format: {calibID: <output of killCalibration() call>}
    :rtype: `python:dict`
    """
    outDict = {}
    for iEl in inList:
      if not isinstance(iEl, int):
        errMsg = ('All elements of input list has to be of integer type.'
                  ' You have provided elements of following types: %s' % [type(iEl) for iEl in inList])
        return S_ERROR(errMsg)
    for iEl in inList:
      #  res = self._checkClientRequest(iEl)
      #  if not res['OK'] or res['Value']:
      #    return res
      outDict[iEl] = self.export_killCalibration(iEl, 'killed by user request')
    return S_OK(outDict)

  auth_changeEosDirectoryToCopyTo = ['authenticated']
  types_changeEosDirectoryToCopyTo = [int, str]

  def export_changeEosDirectoryToCopyTo(self, calibId, newPath):
    """Update "outputPath" settings of the target calibration."""
    if not (isinstance(newPath, str) and isinstance(calibId, int)):
      return S_ERROR('Wrong types of input argumetns. Required types: [int, str]. Provided types: [%s, %s]'
                     % (type(calibId), type(newPath)))

    res = self._checkClientRequest(calibId)
    if not res['OK'] or res['Value']:
      return res

    calibration = CalibrationHandler.activeCalibrations[calibId]
    self.log.info('Calibration #%s: "outputPath" setting is changed from "%s" to "%s"'
                  % (calibId, calibration.settings['outputPath'], newPath))
    calibration.settings['outputPath'] = newPath
    calibration.resultsSuccessfullyCopiedToEos = False
    return S_OK()

  auth_killCalibration = ['authenticated']
  types_killCalibration = [int, str]

  def export_killCalibration(self, calibIdToKill, errMsg):
    """Kill calibration run.

    Send kill signal to all jobs associated with the calibration; mark calibration as finished; keeps all
    intermediate results on the server in case user want to inspect some of them
    """
    res = self._checkClientRequest(calibIdToKill)
    if not res['OK'] or res['Value']:
      return res

    calibration = CalibrationHandler.activeCalibrations[calibIdToKill]
    if not calibration.calibrationFinished:
      calibration.calibrationFinished = True
      calibration.calibrationEndTime = datetime.now()
    calibration.resultsSuccessfullyCopiedToEos = True  # we don't want files to be copied to user EOS
    # if users want logs they can request it with getResults command
    calibration.calibrationRunStatus = errMsg
    saveCalibrationRun(calibration)
    CalibrationHandler.idsOfCalibsToBeKilled += [calibIdToKill]
    return S_OK()

  auth_cleanCalibrations = ['authenticated']
  types_cleanCalibrations = [list]

  def export_cleanCalibrations(self, inList):
    """Remove calibration run from list of active calibrations and delete all associated files."""
    outDict = {}
    for iEl in inList:
      if not isinstance(iEl, int):
        errMsg = ('All elements of input list has to be of integer type.'
                  ' You have provided elements of following types: %s' % [type(iEl) for iEl in inList])
        self.log.error(errMsg)
        return S_ERROR(errMsg)
    for iEl in inList:
      res = self._checkClientRequest(iEl)
      if not res['OK'] or res['Value']:
        return res
      outDict[iEl] = self.export_cleanCalibration(iEl)
    return S_OK(outDict)

  auth_cleanCalibration = ['authenticated']
  types_cleanCalibration = [int]

  def export_cleanCalibration(self, calibIdToClean):
    """Clean temporary calibration results and remove calibration from the list of active calibrations.

    If calibration is still not finished - do nothing (if user want to stop it he has kill it first).
    """
    res = self._checkClientRequest(calibIdToClean)
    if not res['OK'] or res['Value']:
      return res
    activeCalibrations = list(CalibrationHandler.activeCalibrations.keys())
    if calibIdToClean not in activeCalibrations:
      return S_OK('No calibration with ID: %s was found. Active calibrations: %s' % (calibIdToClean,
                                                                                     activeCalibrations))
    calibration = CalibrationHandler.activeCalibrations[calibIdToClean]
    if not calibration.calibrationFinished:
      return S_OK('Cannot clean calibration with ID %s. It is still not finished/killed.' % calibIdToClean)
    else:
      CalibrationHandler.activeCalibrations.pop(calibIdToClean, None)
      shutil.rmtree('calib%s' % calibIdToClean)
      self.log.info('Calibration #%s was cleaned by user.' % calibIdToClean)
      return S_OK('Calibration with ID %s was cleaned.' % calibIdToClean)

  auth_getUserCalibrationStatuses = ['authenticated']
  types_getUserCalibrationStatuses = []

  def export_getUserCalibrationStatuses(self):
    """Get status of all active calibration runs.

    Called by user to check status of calibrations.
    """
    res = self._getUsernameAndGroup()
    if not res['OK']:
      return S_ERROR('Error while retrieving proxy user name or group.')
    usernameAndGroup = res['Value']

    statuses = []

    calibList = sorted(CalibrationHandler.activeCalibrations.keys())
    for calibrationID in calibList:
      calibration = CalibrationHandler.activeCalibrations[calibrationID]
      calibBelongsToUser = calibration.proxyUserName == usernameAndGroup['username'] \
                           and calibration.proxyUserGroup == usernameAndGroup['group']
      if not calibBelongsToUser:
        continue
      calibStatus = calibration.getCurrentStatus()
      if 'calibrationEndTime' in calibStatus.keys():
        calibStatus['timeLeftBeforeOutputWillBeDeleted'] = (
            '%s' % calibration.calibrationEndTime + timedelta(minutes=self.timeToKeepCalibrationResultsInMinutes)
            - datetime.now())
      calibStatus['totalNumberOfJobs'] = int(calibration.settings['numberOfJobs'])
      calibStatus['percentageOfFinishedJobs'] = int(
          100.0 * calibration.stepResults[calibration.currentStep].getNumberOfResults()
          / calibration.settings['numberOfJobs'])
      calibStatus['fractionOfFinishedJobsNeededToStartNextStep'] = int(
          100.0 * calibration.settings['fractionOfFinishedJobsNeededToStartNextStep'])
      statuses.append(calibStatus)
    return(S_OK(statuses))

  auth_submitResult = ['authenticated']
  types_submitResult = [int, int, int, int, int, basestring]

  def export_submitResult(self, calibrationID, stageID, phaseID, stepID, workerID, rootFileContent):
    """Report result of the calibration to the service.

    :param int calibrationID: ID of the current calibration run
    :param int phaseID: ID of the stage the calibration is currently in
    :param int phaseID: ID of the phase the calibration is currently in
    :param int stepID: ID of the step in this calibration
    :param int workerID: ID of the reporting worker
    :param rootFileContent: The binary string content of the root file containing the result of the reconstruction run
    :type rootFile: binary data string
    :returns: S_OK in case of success or if the submission was ignored (since it belongs to an older step),
              S_ERROR if the requested calibration can not be found.
    :rtype: dict
    """
    res = self._checkClientRequest(calibrationID)
    if not res['OK'] or res['Value']:
      return res
    calibration = CalibrationHandler.activeCalibrations.get(calibrationID, None)
    if not calibration:
      return S_ERROR('Calibration with ID %d not found.' % calibrationID)
    if stepID is calibration.currentStep:  # Only add result if it belongs to current step. Else ignore (it's ok)
      # FIXME: use mkdir -p like implementation
      dirName = "calib%s/stage%s/phase%s/step%s" % (calibrationID, stageID, phaseID, stepID)
      if not os.path.exists(dirName):
        try:
          os.makedirs(dirName)
        except OSError as e:
          errMsg = 'Cannot create directories. Current working directory: %s. Error message: %s' % (os.getcwd(), e)
          self.log.error(errMsg)
          return S_ERROR(errMsg)

      newFilename = "calib%s/stage%s/phase%s/step%s/pfoanalysis_w%s.root" % (calibrationID, stageID, phaseID, stepID,
                                                                             workerID)
      if stageID == 2:
        newFilename = "calib%s/stage%s/phase%s/step%s/PandoraLikelihoodDataPhotonTraining_w%s.xml" % (calibrationID,
                                                                                                      stageID, phaseID,
                                                                                                      stepID, workerID)
      stringToBinaryFile(rootFileContent, newFilename)
      calibration.addResult(stepID, workerID, newFilename)
    return S_OK()

  auth_checkForStepIncrement = ['TrustedHost']
  types_checkForStepIncrement = []

  def export_checkForStepIncrement(self):
    """Check whether there are any running Calibrations that received enough results to start the next step.

    Should only be called by the agent.
    :returns: S_OK when the check has been ended.
    :rtype: dict
    """
    self.log.info('Executing checkForStepIncrement. activeCalibrations: %s'
                  % CalibrationHandler.activeCalibrations.keys())
    for calibrationID in list(CalibrationHandler.activeCalibrations.keys()):
      # FIXME this still can lead to that some jobs will finish with error status because they didn't finish in time
      calibration = CalibrationHandler.activeCalibrations[calibrationID]
      if calibration.calibrationFinished:
        if not calibration.resultsSuccessfullyCopiedToEos:
          res = calibration.copyResultsToEos(proxyUserName=calibration.proxyUserName,
                                             proxyUserGroup=calibration.proxyUserGroup)
          if not res['OK']:
            return res
          else:
            calibration.resultsSuccessfullyCopiedToEos = True
        if ((datetime.now() - calibration.calibrationEndTime).seconds
              / 60.0 >= self.timeToKeepCalibrationResultsInMinutes):
          if not calibration.resultsSuccessfullyCopiedToEos:
            self.log.error('Calibration results have not been copied properly...')
          self.log.info('Removing calibration %s from the active calibration list and clean up local directory')
          CalibrationHandler.activeCalibrations.pop(calibrationID, None)
          shutil.rmtree('calib%s' % calibrationID)
      elif self.finalInterimResultReceived(calibration, calibration.currentStep):
        res = calibration.endCurrentStep()
        if not res['OK']:
          return res
    return S_OK()

  def finalInterimResultReceived(self, calibration, stepID):
    """Check number of finished jobs and start next step if number os larger than a certain threshold.

    Called periodically.
    :param CalibrationRun calibration: The calibration to check
    :param int stepID: The ID of the current step of that calibration
    :returns: True if enough results have been submitted, False otherwise
    :rtype: bool
    """
    # FIXME: Find out of this is susceptible to race condition
    numberOfResults = calibration.stepResults[stepID].getNumberOfResults()
    maxNumberOfJobs = calibration.settings['numberOfJobs']
    startNextStep = numberOfResults >= math.ceil(
        calibration.settings['fractionOfFinishedJobsNeededToStartNextStep'] * maxNumberOfJobs)
    self.log.info('Check status of jobs. CalibID: %d; numberOfResults: %d, totalNumbersOfJobs: %d, startNextStep: %s'
                  % (calibration.calibrationID, numberOfResults, maxNumberOfJobs, startNextStep))
    return startNextStep

  auth_getNewParameters = ['authenticated']
  types_getNewParameters = [int, int]

  def export_getNewParameters(self, calibrationID, stepIDOnWorker):
    """Retrieve parameters for the next step of the calibration.

    Called by the worker node.
    :param int calibrationID: ID of the calibration being run on the worker
    :param int stepIDOnWorker: current step ID on the worker node
    :returns: S_ERROR in case of error (e.g. inactive calibration asking for params),
              S_OK with the parameter set and the id of the current step
    :rtype: dict
    """
    cal = CalibrationHandler.activeCalibrations.get(calibrationID, None)
    if not cal:
      self.log.error("CalibrationID is not in active calibrations:",
                     "Active Calibrations:%s , asked for %s" % (self.activeCalibrations,
                                                                calibrationID))
      res = S_ERROR(
          "calibrationID is not in active calibrations: %s\nThis should mean that the calibration has finished"
          % calibrationID)
      return res
    res = self._checkClientRequest(calibrationID)
    if not res['OK'] or res['Value']:
      return res

    res = cal.getNewParameters(stepIDOnWorker)
    return res

  auth_getNewPhotonLikelihood = ['authenticated']
  types_getNewPhotonLikelihood = [int]

  def export_getNewPhotonLikelihood(self, calibrationID):
    """Retrieve new photon likelihood file for new step of the calibration.

    Called by the worker node.
    :param int calibrationID: ID of the calibration being run on the worker
    :returns: S_ERROR in case of error (e.g. inactive calibration asking for params),
              S_OK with the parameter set and the id of the current step
    :rtype: dict
    """
    res = self._checkClientRequest(calibrationID)
    if not res['OK'] or res['Value']:
      return res
    cal = CalibrationHandler.activeCalibrations.get(calibrationID, None)
    if not cal:
      self.log.error("CalibrationID is not in active calibrations:",
                     "Active Calibrations:%s , asked for %s" % (self.activeCalibrations,
                                                                calibrationID))
      result = S_ERROR(
          "calibrationID is not in active calibrations: %s\nThis should mean that the calibration has finished"
          % calibrationID)
    else:
      result = cal.getNewPhotonLikelihood()
    return result

  auth_getInputDataDict = ['authenticated']
  types_getInputDataDict = [int, int]

  def export_getInputDataDict(self, calibrationID, workerID):
    """Retrieve dict with input files and number of events to run and to skip.

    Called by the worker node.
    :param int calibrationID: ID of the calibration being run on the worker
    :returns: S_ERROR in case of error (e.g. inactive calibration asking for params),
              S_OK with the parameter set and the id of the current step
    :rtype: dict
    """
    res = self._checkClientRequest(calibrationID)
    if not res['OK'] or res['Value']:
      return res
    cal = CalibrationHandler.activeCalibrations.get(calibrationID, None)
    if not cal:
      self.log.error("CalibrationID is not in active calibrations:",
                     "Active Calibrations:%s , asked for %s" % (self.activeCalibrations,
                                                                calibrationID))
      result = S_ERROR(
          "calibrationID is not in active calibrations: %s\nThis should mean that the calibration has finished"
          % calibrationID)
    else:
      if workerID >= cal.settings['numberOfJobs']:
        errMsg = ('Value of workerID is larger than number of job in this calibration: '
                  'calibID: %s, nJobs: %s, workerID: %s' % (calibrationID, cal.settings['numberOfJobs'], workerID))
        self.log.error(errMsg)
        result = S_ERROR(errMsg)
      else:
        result = S_OK(cal.inputFiles[workerID])

    return result

  auth_resubmitJobs = ['TrustedHost']
  types_resubmitJobs = [list]

  def export_resubmitJobs(self, failedJobs):
    """Resubmit failed jobs.

    Called by the Agent.
    :param failedJobs: List of pairs of the form (calibrationID, workerID)
    :type failedJobs: `python:list`
    :returns: S_OK if successful, else a S_ERROR with a pair (errorstring, list_of_failed_id_pairs)
    :rtype: dict
    """
    for iCalib in CalibrationHandler.activeCalibrations:
      jobsToResubmit = []
      for calibrationID, workerID in failedJobs:
        if calibrationID == iCalib:
          jobsToResubmit.append(workerID)
      if jobsToResubmit:
        calibRun = CalibrationHandler.activeCalibrations[iCalib]
        calibRun.nFailedJobs += len(jobsToResubmit)
        if calibRun.nFailedJobs < calibRun.settings['numberOfJobs']:
          calibRun.submitJobs(jobsToResubmit, proxyUserName=calibRun.proxyUserName,
                              proxyUserGroup=calibRun.proxyUserGroup)  # pylint: disable=unexpected-keyword-arg
        else:
          errMsg = ('Number of failed jobs are larger than total number of jobs. Something wrong with this calibration'
                    ' run. Kill it!')
          # TODO FIXME test it! initial test failed...
          self.log.error(errMsg)
          self.export_killCalibration(iCalib, errMsg)
    return S_OK()

  auth_getNumberOfJobsPerCalibration = ['TrustedHost']
  types_getNumberOfJobsPerCalibration = []

  def export_getNumberOfJobsPerCalibration(self):
    """Return a dictionary that maps active calibration IDs to the number of initial jobs they submitted.

    Used by the agent to determine when to resubmit jobs.
    :returns: S_OK containing the dictionary with mapping calibrationID -> numberOfJobs
    :rtype: dict
    """
    result = {}
    for calibrationID in CalibrationHandler.activeCalibrations:
      result[calibrationID] = CalibrationHandler.activeCalibrations[calibrationID].settings['numberOfJobs']
    self.log.debug("Number of jobs per calibration: %s" % result)
    return S_OK(result)

  auth_getRunningCalibrations = ['TrustedHost']
  types_getRunningCalibrations = []

  def export_getRunningCalibrations(self):
    """Return a list of unfinished calibrations.

    :returns: S_OK containing the the list of calibrationIDs
    :rtype: list
    """
    result = []
    for calibrationID in CalibrationHandler.activeCalibrations:
      if not CalibrationHandler.activeCalibrations[calibrationID].calibrationFinished:
        result.append(calibrationID)
    self.log.debug("List of running calibrations: %s" % result)
    return S_OK(result)

  auth_getActiveCalibrations = ['TrustedHost']
  types_getActiveCalibrations = []

  def export_getActiveCalibrations(self):
    """Return a list of all active calibrations.

    :returns: S_OK containing the the list of calibrationIDs
    :rtype: list
    """
    self.log.debug("List of active calibrations: %s" % CalibrationHandler.activeCalibrations)
    return S_OK(CalibrationHandler.activeCalibrations)

  auth_getCalibrationsToBeKilled = ['TrustedHost']
  types_getCalibrationsToBeKilled = []

  def export_getCalibrationsToBeKilled(self):
    """Return list of calibrations to be killed."""
    listToReturn = CalibrationHandler.idsOfCalibsToBeKilled
    CalibrationHandler.idsOfCalibsToBeKilled = []
    self.log.debug("List of calibrations to be killed: %s" % listToReturn)
    return S_OK(listToReturn)

# TODO: Add stopping criterion to calibration loop. This should be checked when new parameter sets are calculated
# In that case, the calibration should be removed from activeCalibrations and the result stored.
# Should we then kill all jobs of that calibration?

  def __inputVariablesSanityCheck(self, inputSettings):

    settingsDictCopy = dict(inputSettings)

    def checkType(inType, containerType=None):
      errMsg = 'Invalid type of input settings for argument: %s'
      vals = {key: settingsDictCopy[key] for key in keys}
      for key, val in vals.iteritems():
        if containerType is None:
          if not isinstance(val, inType):
            return errMsg % key
        else:
          for containerElement in val:
            if not isinstance(containerElement, inType):
              return errMsg % key
      return None

    keys = ['disableSoftwareCompensation', 'startCalibrationFinished']
    msg = checkType(bool)
    if msg:
      return S_ERROR(msg)

    keys = ['startStage', 'startPhase', 'stopStage', 'stopPhase', 'nHcalLayers']
    msg = checkType(int)
    if msg:
      return S_ERROR(msg)

    keys = ['digitisationAccuracy', 'pandoraPFAAccuracy', 'fractionOfFinishedJobsNeededToStartNextStep']
    msg = checkType(float)
    if msg:
      return S_ERROR(msg)

    keys = ['platform', 'marlinVersion', 'marlinVersion_CS', 'DDPandoraPFANewProcessorName', 'DDCaloDigiName',
            'detectorModel', 'steeringFile', 'DDPandoraPFANewProcessorName', 'DDCaloDigiName']
    msg = checkType(str)
    if msg:
      return S_ERROR(msg)

    keys = ['ecalBarrelCosThetaRange', 'ecalEndcapCosThetaRange', 'hcalBarrelCosThetaRange', 'hcalEndcapCosThetaRange']
    msg = checkType(float, list)
    if msg:
      return S_ERROR(msg)

    def checkVal(validRange, isList=False):
      errMsg = 'Invalid value of input settings for argument: %s;'
      errMsg += ' Valid value range: %s;' % validRange
      errMsg += ' Provided value: %s'
      vals = {key: settingsDictCopy[key] for key in keys}
      for key, val in vals.iteritems():
        if not isList:
          if val < validRange[0] or val > validRange[1]:
            return errMsg % (key, val)
        else:
          for containerElement in val:
            if containerElement < validRange[0] or containerElement > validRange[1]:
              return errMsg % (key, containerElement)
      return None

    keys = ['startStage']
    msg = checkVal([1, 3])
    if msg:
      return S_ERROR(msg)

    keys = ['startPhase']
    msg = checkVal([0, 5])
    if msg:
      return S_ERROR(msg)

    keys = ['digitisationAccuracy', 'pandoraPFAAccuracy', 'fractionOfFinishedJobsNeededToStartNextStep']
    msg = checkVal([0.0, 1.0])
    if msg:
      return S_ERROR(msg)

    keys = ['ecalBarrelCosThetaRange', 'ecalEndcapCosThetaRange', 'hcalBarrelCosThetaRange', 'hcalEndcapCosThetaRange']
    msg = checkVal([0.0, 1.0], isList=True)
    if msg:
      return S_ERROR(msg)

    self.log.debug('All input variables are valid.')
    return S_OK()
