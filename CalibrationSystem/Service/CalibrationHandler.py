"""
The CalibrationHandler collates (calibration results) and distributes (input parameters) information from
the calibration worker nodes and allows the creation of calibration runs. It will (re-)submit jobs and
distribute reconstruction workloads among them
"""

import os
import copy
import math
import pickle
import shutil

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import DErrno
from ILCDIRAC.CalibrationSystem.Service.CalibrationRun import CalibrationRun
from ILCDIRAC.CalibrationSystem.Utilities.fileutils import stringToBinaryFile

__RCSID__ = "$Id$"
LOG = gLogger.getSubLogger(__name__)


class WorkerInfo(object):
  """ Wrapper class to store information needed by workers to compute their result. """
  def __init__(self, parameterSet, offset):
    """ Creates a new WorkerInfo object, passed to the worker nodes to enable them to compute their results.

    :param object parameterSet: The histogram to use for the calibration.
    :param int offset: The offset in the event file, used to determine starting point of this computation.
    """
    self.parameterSet = parameterSet
    self.offset = offset

  def getInfo(self):
    return (self.parameterSet, self.offset)


class CalibrationHandler(RequestHandler):
  """ Handles the information exchange between worker nodes and the service """
  @classmethod
  def initializeHandler(cls, _):
    """ Initializes the handler, setting required variables. Called once in the beginning of the service """
    cls.activeCalibrations = {}
    cls.calibrationCounter = 0
    cls.log = LOG
    return S_OK()

  def _getUsernameAndGroup(self):
    """ Returns name of the group and name of the user of the proxy the user is currently having

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

  def initialize(self):
    """ Initializes a single response, setting required variables. Called once /per request/.
    """
    pass

  # TODO split this function to two: first one just create CalibrationRun instance and returns it (to allow user to
  # setup different settings); second one - submits jobs
  auth_createCalibration = ['authenticated']
  #  types_createCalibration = [dict, int, basestring, basestring, basestring]
  #  def export_createCalibration(self, inputFiles, numberOfJobs, marlinVersion, steeringFile, detectorModel):
  types_createCalibration = [dict, dict]

  def export_createCalibration(self, inputFiles, calibSettingsDict):
    """ Called by users to create a calibration run (series of calibration iterations)

    :param basestring marlinVersion: Version of the Marlin application to be used for reconstruction
    :param dict inputFiles: Input files for the calibration. Dictionary.
    :type inputFiles: `python:dict`
    :param int numberOfJobs: Number of jobs this service will run (actual number will be slightly lower)
    :param basestring steeringFile: Steering file used in the calibration, LFN
    :returns: S_OK containing ID of the calibration, used to retrieve results etc
    :rtype: dict
    """

    inputFileDictLoweredKeys = {}
    for iKey, iList in inputFiles.iteritems():
      inputFileDictLoweredKeys[iKey.lower()] = iList

    res = self.__regroupInputFile(inputFileDictLoweredKeys, calibSettingsDict['numberOfJobs'])
    if not res['OK']:
      return res
    groupedInputFiles = res['Value']

    CalibrationHandler.calibrationCounter += 1
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
    if self._calibration_creation_failed(res):
      ret_val = S_ERROR('Submitting at least one of the jobs failed')  # FIXME: This should be treated, since the
      # successfully submitted jobs will still run
      ret_val['calibrations'] = res
      return ret_val
    return S_OK((calibrationID, res))

  auth_submitResult = ['authenticated']
  types_submitResult = [int, int, int, int, int, basestring]
  def export_submitResult(self, calibrationID, stageID, phaseID, stepID, workerID, rootFileContent):
    """ Called from the worker node to report the result of the calibration to the service

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
    calibration = CalibrationHandler.activeCalibrations.get(calibrationID, None)
    if not calibration:
      return S_ERROR('Calibration with ID %d not found.' % calibrationID)
    if stepID is calibration.currentStep:  # Only add result if it belongs to current step. Else ignore (it's ok)
      ## FIXME: use mkdir -p like implementation
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

  auth_checkForStepIncrement = ['authenticated']
  types_checkForStepIncrement = []
  def export_checkForStepIncrement(self):
    """ Should only be called by the agent. Periodically checks whether there are any running
    Calibrations that received enough results to start the next step.

    :returns: S_OK when the check has been ended.
    :rtype: dict
    """
    self.log.info('Executing checkForStepIncrement. activeCalibrations: %s'
                  % CalibrationHandler.activeCalibrations.keys())
    for calibrationID in CalibrationHandler.activeCalibrations.keys():
      # FIXME this still can lead to that some jobs will finish with error status because they didn't finish in time
      calibration = CalibrationHandler.activeCalibrations[calibrationID]
      if calibration.calibrationFinished:
        res = calibration.copyResultsToEos(proxyUserName=calibration.proxyUserName,
                                           proxyUserGroup=calibration.proxyUserGroup)
        self.log.info('Removing calibration %s from the active calibration list and clean up local directory')
        del CalibrationHandler.activeCalibrations[calibrationID]
        shutil.rmtree('calib%s' % calibrationID)
        # TODO should I do this? if only one calibration is not finished - all other will stop as well...
        if not res['OK']:
          return res
      elif self.finalInterimResultReceived(calibration, calibration.currentStep):
        calibration.endCurrentStep()
    return S_OK()

  def finalInterimResultReceived(self, calibration, stepID):
    """ Called periodically. Checks for the given calibration if we now have enough results to compute
    a new ParameterSet.

    :param CalibrationRun calibration: The calibration to check
    :param int stepID: The ID of the current step of that calibration
    :returns: True if enough results have been submitted, False otherwise
    :rtype: bool
    """
    #FIXME: Find out of this is susceptible to race condition
    numberOfResults = calibration.stepResults[stepID].getNumberOfResults()
    maxNumberOfJobs = calibration.settings['numberOfJobs']
    self.log.info('Executing finalInterimResultReceived. numberOfResults: %d, maxNumberOfJobs: %d'
                  % (numberOfResults, maxNumberOfJobs))
    return numberOfResults >= math.ceil(calibration.settings['fractionOfFinishedJobsNeededToStartNextStep'] * maxNumberOfJobs)

  auth_getNewParameters = ['authenticated']
  types_getNewParameters = [int, int]
  def export_getNewParameters(self, calibrationID, stepIDOnWorker):
    """ Called by the worker node to retrieve the parameters for the next iteration of the calibration

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

    res = cal.getNewParameters(stepIDOnWorker)
    return res

  auth_getNewPhotonLikelihood = ['authenticated']
  types_getNewPhotonLikelihood = [int]

  def export_getNewPhotonLikelihood(self, calibrationID):
    """ Called by the worker node to retrieve the parameters for the next iteration of the calibration

    :param int calibrationID: ID of the calibration being run on the worker
    :returns: S_ERROR in case of error (e.g. inactive calibration asking for params),
              S_OK with the parameter set and the id of the current step
    :rtype: dict
    """
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
    """ Called by the worker node to retrieve the parameters for the next iteration of the calibration

    :param int calibrationID: ID of the calibration being run on the worker
    :returns: S_ERROR in case of error (e.g. inactive calibration asking for params),
              S_OK with the parameter set and the id of the current step
    :rtype: dict
    """
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

  auth_resubmitJobs = ['authenticated']
  types_resubmitJobs = [list]
  def export_resubmitJobs(self, failedJobs):
    """ Takes a list of workerIDs and resubmits a job with the current parameterset there

    :param failedJobs: List of pairs of the form (calibrationID, workerID)
    :type failedJobs: `python:list`
    :returns: S_OK if successful, else a S_ERROR with a pair (errorstring, list_of_failed_id_pairs)
    :rtype: dict
    """

    failedPairs = []
    for calibrationID, workerID in failedJobs:
      if calibrationID not in CalibrationHandler.activeCalibrations:
        failedPairs.append((calibrationID, workerID))
        continue

    for iCalib in CalibrationHandler.activeCalibrations:
      jobsToResubmit = []
      for calibrationID, workerID in failedJobs:
        if calibrationID == iCalib.getCalibrationID():
          jobsToResubmit.append(workerID)
      if jobsToResubmit:
        iCalib.submitJobs(jobsToResubmit)  # pylint: disable=unexpected-keyword-arg

    if failedPairs:
      result = S_ERROR('Could not resubmit all jobs. Failed calibration/worker pairs are: %s' % failedPairs)
      result['failed_pairs'] = failedPairs
      return result
    else:
      return S_OK()

  auth_getNumberOfJobsPerCalibration = ['authenticated']
  types_getNumberOfJobsPerCalibration = []
  def export_getNumberOfJobsPerCalibration(self):
    """ Returns a dictionary that maps active calibration IDs to the number of initial jobs they submitted.
    Used by the agent to determine when to resubmit jobs.

    :returns: S_OK containing the dictionary with mapping calibrationID -> numberOfJobs
    :rtype: dict
    """
    result = {}
    for calibrationID in CalibrationHandler.activeCalibrations:
      result[calibrationID] = CalibrationHandler.activeCalibrations[calibrationID].settings['numberOfJobs']
    return S_OK(result)

#TODO: Add stopping criterion to calibration loop. This should be checked when new parameter sets are calculated
#In that case, the calibration should be removed from activeCalibrations and the result stored.
#Should we then kill all jobs of that calibration?

  def _calibration_creation_failed(self, results):
    """ Returns whether or not the creation of all calibration jobs was successful.

    :param results: List of S_OK/S_ERROR dicts that were returned by the submission call
    :returns: True if everything was successful, False otherwise
    :rtype: bool
    """
    success = True
    for job_result in results:
      success = success and job_result['OK']
    return not success

  def __regroupInputFile(self, inputFiles, numberOfJobs):
    """ Function to regroup inputFiles dict according to numberOfJobs. Output dict will have a format:
    list of files = outDict[iJob][fileType]

    :param inputFiles: Input list of files for the calibration. Dictionary.
    :type inputFiles: `python:dict`
    :param int numberOfJobs: Number of jobs to run
    :returns: S_OK with 'Value' element being a new regroupped dict or S_ERROR
    :rtype: dict
    """
    tmpDict = {}
    for iKey, iList in inputFiles.iteritems():
      if len(iList) < numberOfJobs:
        return S_ERROR('Too many jobs for provided input data. numberOfJobs==%s which is larger than number of '
                       'availables files for key %s: nFiles==%s' % (numberOfJobs, iKey, len(iList)))
      nFilesPerJob = int(len(iList) / numberOfJobs)
      nLeftoverFiles = len(iList) - nFilesPerJob * numberOfJobs
      newDict = {}
      for i in range(0, numberOfJobs):
        newDict[i] = []
        for j in range(0, nFilesPerJob):
          j = nFilesPerJob * i + j
          newDict[i].append(iList[j])
        if i < nLeftoverFiles:
          newDict[i].append(iList[nFilesPerJob * numberOfJobs + i])
      tmpDict[iKey] = newDict

    outDict = {}
    for iJob in range(0, numberOfJobs):
      newDict = {}
      for iType in [x.lower() for x in inputFiles.keys()]:
        newDict[iType] = tmpDict[iType][iJob]
      outDict[iJob] = newDict

    return S_OK(outDict)

####################################################################
#                                                                  #
#         Testcode, not to be used by production code              #
#                                                                  #
####################################################################
  auth_testGetInitVals = ['all']  # FIXME: Restrict to test usage only
  types_testGetInitVals = []

  def export_testGetInitVals(self):
    """ Called only by test methods! Resets the service so it can be tested.

    :returns: S_OK on success. (Should always succeed)
    :rtype: dict
    """
    return S_OK((self.activeCalibrations, self.calibrationCounter))

  auth_testGetVals = ['all']  # FIXME: Restrict to test usage only
  types_testGetVals = []

  def export_testGetVals(self):
    """ Called only by test methods! Resets the service so it can be tested.

    :returns: S_OK on success. (Should always succeed)
    :rtype: dict
    """
    pandoraAnalysisCalibExecutablePath = self.ops.getValue("/AvailableTarBalls/%s/pandora_calibration_scripts/%s/%s" %
                                                           (self.platform, self.appversion, "PandoraAnalysis"), None)
    ilcSoftInitScript = self.ops.getValue("/AvailableTarBalls/%s/pandora_calibration_scripts/%s/%s" % (self.platform,
                                                                                                       self.appversion, "CVMFSEnvScript"), None)
    marlinPandoraScriptsPath = self.ops.getValue("/AvailableTarBalls/%s/pandora_calibration_scripts/%s/%s"
                                                 % (self.platform, self.appversion, "MarlinPandora"), None)

    return S_OK((ilcSoftInitScript, marlinPandoraScriptsPath, pandoraAnalysisCalibExecutablePath))

  auth_testCreateRun = ['all']  # FIXME: Restrict to test usage only
  types_testCreateRun = []

  def export_testCreateRun(self):
    """ Called only by test methods! Resets the service so it can be tested.

    :returns: S_OK on success. (Should always succeed)
    :rtype: dict
    """
    newRun = CalibrationRun(1, 'dummy.txt', {0: "dummyInFile.slcio"}, 1, 'dummyMarlinVersion', 'dummyDetectorModel')

    return S_OK(newRun.getCalibrationID())

  auth_resetService = ['all']  # FIXME: Restrict to test usage only
  types_resetService = []
  def export_resetService(self):
    """ Called only by test methods! Resets the service so it can be tested.

    :returns: S_OK on success. (Should always succeed)
    :rtype: dict
    """
    CalibrationHandler.activeCalibrations = {}
    CalibrationHandler.calibrationCounter = 0
    return S_OK()

  auth_getInternals = ['all']  # FIXME: Restrict to test usage only
  types_getInternals = []
  def export_getInternals(self):
    """ Called only by test methods! Returns the class variables of this service,
    exposing its internals and making it testable.
    The activeCalibration dictionary is serialized using the dumps method from the pickle module.
    This is done since for an unknown reason one cannot return objects of custom (i.e. non-default python)
    classes through a service (else a socket timeout occurs).

    :returns: S_OK containing a tuple with the active calibrations dict (serialized with the pickle module) and
              the calibrationCounter
    :rtype: dict
    """
    return S_OK((pickle.dumps(CalibrationHandler.activeCalibrations),
                 copy.deepcopy(CalibrationHandler.calibrationCounter)))

  auth_setRunValues = ['all']
  types_setRunValues = [int, int, object, bool]
  def export_setRunValues(self, calibrationID, currentStep, parameterSet, calFinished):
    """ Sets the values of the calibration with ID calibrationID. It is put to step currentStep,
    gets the parameterSet as current parameter set and the stepFinished status.

    :param int calibrationID: ID of the calibration whose values are to be changed.
    :param int currentStpe: step the calibration is set to.
    :param int parameterSet: New parameterSet for the CalibrationRun
    :param bool calFinished: New calibrationFinished status for the CalibrationRun
    :returns: S_OK after it has finished
    :rtype: dict
    """
    calibration = CalibrationHandler.activeCalibrations.get(calibrationID, None)
    if not calibration:
      return S_ERROR('Calibration with ID %s not in active calibrations.' % calibrationID)
    calibration.currentStep = currentStep
    calibration.currentParameterSet = parameterSet
    calibration.calibrationFinished = calFinished
    return S_OK()

  auth_getopts = ['all']
  types_getopts = [basestring]
  def export_getopts(self, option):
    """ Returns the value of the option stored in the gConfig that this service accesses.

    :param basestring option: name of the option to be queried
    :returns: S_OK containing the value of the option
    :rtype: dict
    """
    return S_OK(gConfig.getValue(option))




