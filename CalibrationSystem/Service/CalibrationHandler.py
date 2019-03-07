"""
The CalibrationHandler collates (calibration results) and distributes (input parameters) information from
the calibration worker nodes and allows the creation of calibration runs. It will (re-)submit jobs and
distribute reconstruction workloads among them
"""

import os
import glob

from collections import defaultdict
import xml.etree.ElementTree as ET
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.Proxy import executeWithUserProxy
from ILCDIRAC.CalibrationSystem.Utilities.fileutils import stringToBinaryFile
from ILCDIRAC.Workflow.Modules.Calibration import Calibration
from DIRAC.Core.Utilities.Subprocess import shellCall

__RCSID__ = "$Id$"


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


#pylint: disable=no-self-use
class CalibrationResult(object):
  """ Wrapper class to store information about calibration computation interim results. Stores results
  from all worker nodes from a single step. """
  def __init__(self):
    self.results = dict()

  def addResult(self, workerID, result):
    """ Adds a result from a given worker to the object

    :param int workerID: ID of the worker providing the result
    :param result: list of floats representing the returned histogram
    :type result: `python:list`
    :returns: None
    """
    self.results[workerID] = result

  def getNumberOfResults(self):
    """ Return number of interim results stored in this wrapper

    :returns: Number of histograms stored in this object
    :rtype: int
    """
    return len(self.results)


class CalibrationRun(object):
  """ Object that stores information about a single run of the calibration software.
  Includes files, current parameter set, software version, the workers running as well as
  the results of each step.
  """
  def __init__(self, calibrationID, steeringFile, ilcsoftPath, inputFiles, numberOfJobs):
    self.calibrationID = calibrationID
    self.steeringFile = steeringFile
    self.ilcsoftPath = ilcsoftPath
    self.inputFiles = inputFiles
    #TODO ask user to provide these... or read it from CS
    self.ecalBarrelCosThetaRange = [0.0, 1.0]
    self.ecalEndcapCosThetaRange = [0.0, 1.0]
    self.hcalBarrelCosThetaRange = [0.0, 1.0]
    self.hcalEndcapCosThetaRange = [0.0, 1.0]
    #TODO make these three value below configurable
    self.nHcalLayers = 44
    self.digitisationAccuracy = 0.05
    self.pandoraPFAAccuracy = 0.005
    self.stepResults = defaultdict(CalibrationResult)
    self.currentStage = 1
    self.currentPhase = CalibrationPhase.ECalDigi
    self.currentStep = 0
    self.currentParameterSet = None
    self.numberOfJobs = numberOfJobs
    self.calibrationFinished = False
    #  /cvmfs/clicdp.cern.ch/iLCSoft/builds/2019-02-07/x86_64-slc6-gcc7-opt
    self.platform = ''
    self.appversion = ''
    self.calibrationConstantsDict = defaultdict()
    if len(ilcsoftPath.split('/')) >= 7:
      self.platform = ilcsoftPath.split('/')[6]
      self.appversion = ilcsoftPath.split('/')[5]
    #TODO use either line below or take it from configuration service
    #  self.calibrationBinariesDir = os.path.join(ilcsoftPath, "PandoraAnalysis/HEAD/bin/")

    #self.workerJobs = [] ##FIXME: Disabled because not used? Maybe in submit initial jobs
    #self.activeWorkers = dict() ## dict between calibration and worker node? ##FIXME:Disabled because not used?
    #FIXME: Probably need to store a mapping workerID -> part of calibration that worker is working on. This then needs
    #to be accessed by the agent in the case of resubmission

  @executeWithUserProxy
  def submitJobs(self, calibrationID, idsOfWorkerNodesToSubmitTo=None):
    """ Submit the calibration jobs to the workers for the first time.
    Use a specially crafted application that runs repeated Marlin reconstruction steps

    :param int calibrationID: ID of this calibration. Needed for the jobName parameter
    :param idsOfWorkerNodesToSubmitTo: list of integers representing IDs of worker nodes to submit jobs to;
                                       if None submit to all allocated nodes
    :returns: S_OK or S_ERROR
    :rtype: dict
    """
    usernameAndGroup = _getUsernameAndGroup()
    if not username['OK']:
        return S_ERROR('Error while retrieving proxy user name or group. CalibrationID = %s; WorkerID = %s'
                       % (calibrationID, curWorkerID))
    proxyUsername = usernameAndGroup['username']
    proxyUserGroup = usernameAndGroup['group']

    from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob
    from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
    dirac = DiracILC(True, 'some_job_repository.rep')
    results = []
    self.init_files()
    listOfNodesToSubmitTo = xrange(0, self.numberOfJobs)
    if idsOfWorkerNodesToSubmitTo is not None:
      listOfNodesToSubmitTo = idsOfWorkerNodesToSubmitTo
    for curWorkerID in listOfNodesToSubmitTo:
      curJob = UserJob()
      curJob.check = False  # Necessary to turn off user confirmation
      curJob.setName('CalibrationService_calid_%s_workerid_%s' % (calibrationID, curWorkerID))
      curJob.setJobGroup('CalibrationService_calib_job')

      # TODO check if the name of module (Calibration) is correct
      calib = Calibration(calibrationID, curWorkerID)
      # FIXME provide marlinVersion and detectorModel
      calib.setVersion(marlinVersion)
      calib.setDetectorModel(detectorModel)
      #  calib.setNbEvts(nEvts+1)
      #  calib.setProcessorsToUse([])
      calib.setSteeringFile(recoSteeringFileName)
      #  calib.setExtraCLIArguments(" --Config.Overlay="+overlayParameterValue+"  --Config.Tracking="+trackingType+"
      #                             --Output_DST.LCIOOutputFile="+outputFile+"
      #                             --constant.CalorimeterIntegrationTimeWindow="+str(calorimeterIntegrationTimeWindow))
      res = curJob.append(calib)
      if not res['OK']:
        print res['Message']
        return S_ERROR('Failed to setup Calibration worklow module. CalibrationID = %s; WorkerID = %s'
                       % (calibrationID, curWorkerID))

      # FIXME should we set any time limit at all?
      curJob.setCPUTime(60 * 60 * 24)
      # FIXME allow user to specify xml-files. CLIC detector have different name of PandoraLikelihhod file than CLD
      inputSB = ['GearOutput.xml', 'PandoraSettingsDefault.xml', 'PandoraLikelihoodData9EBin.xml']
      # FIXME implement distribution of slcio files of each category (MUON, KAON, PHOTON) among the jobs
      # FIXME treat case when number of input files in one of category is less than number of jobs...
      lcioFile = None
      #  key = CalibrationPhase.fileKeyFromPhase(self.currentPhase)
      #  lcioFile = _getLCIOInputFiles(self.inputFiles[key][i])

      curJob.setInputData(lcioFile)
      curJob.setInputSandbox(inputSB)
      curJob.setOutputSandbox(['*.log'])
      res = curJob.submit(dirac)
      results.append(res)

    return results

  def init_files(self):
    """ Initializes the necessary files etc.

    :returns: nothing
    :rtype: None
    """
    import tempfile
    tmp_path = tempfile.mkdtemp()
    #FIXME: Fix these to copy the actual directory
    # Copy GearOutput.xml, PandoraSettingsDefault.xml, PandoraLikelihoodData9EBin.xml to tmp dir
    GAMMA_FILES = execute_and_return_output(['python', 'Xml_Generation/countMatches.py',
                                             GAMMA_PATH, SLCIO_FORMAT]).split(' ')
    MUON_FILES = execute_and_return_output(['python', 'Xml_Generation/countMatches.py',
                                            MUON_PATH, SLCIO_FORMAT]).split(' ')
    KAON_FILES = execute_and_return_output(['python', 'Xml_Generation/countMatches.py',
                                              KAON_PATH, SLCIO_FORMAT]).split(' ')

  def addResult(self, stepID, workerID, result):
    """ Add a reconstruction result to the list of other results

    :param int stepID: ID of the step
    :param int workerID: ID of the worker providing the result
    :param result: reconstruction histogram from the worker node
    :type result: `python:list`
    :returns: None
    """
    self.stepResults[stepID].addResult(workerID, result)
    #FIXME: Do we add old step results? Current status is no, ensured in CalibrationHandler
    #FIXME: Do we delete old interim results?

  def getNewParameters(self, stepIDOnWorker):
    """ Returns the current parameters

    :param int stepIDOnWorker: The ID of the step the worker just completed.
    :returns: If the computation is finished, returns S_OK containing a success message string. If there is a new
              parameter set, a S_OK dict containing the updated parameter set. Else a S_ERROR
    :rtype: dict
    """
    if self.calibrationFinished:
      return S_ERROR('Calibration finished! End job now.')
    if self.currentStep > stepIDOnWorker:
      return S_OK(self.currentParameterSet)
    else:
      return S_ERROR('No new parameter set available yet. Current step in service: %s, step on worker: %s'
                     % (self.currentStep, stepIDOnWorker))

  def endCurrentStep(self):
    """ Calculates the new parameter set based on the results from the computations and prepares the object
    for the next step. (StepCounter increased, ...)

    :returns: None
    """
    self.__calculateNewParamsRoot(self.currentStep)
    self.currentStep += 1
    #if self.currentStep > 15: #FIXME: Implement real stopping criterion
    #TODO Implement real stopping criterion
    if self.currentStep > 1:  # FIXME: replace with line above after testing
      self.calibrationFinished = True
      #FIXME: Decide how a job finishing should be handled - set this flag to True and have user poll, do something
      #       actively here, etc...
    #self.activeWorkers = dict()
    #FIXME: implement following logic:
    # Check if error small enough. If yes, set step to 0 and phase to $nextphase
    # Else increase step by one
    # If $nextphase too high, calibrationFinished = True

  def __addLists(self, list1, list2):
    """ Adds two lists together by adding the first element, second element, and so on. Throws an exception
    if the lists have a different number of elements.

    :param list1: List that should be added element-wise to another
    :type list1: `python:list`
    :param list2: Other list that should be added element-wise
    :type list2: `python:list`
    :returns: The list [list1[0]+list2[0], list1[1]+list2[1], ...]
    :rtype: list
    """
    if len(list1) != len(list2):
      raise ValueError('The two lists do not have the same number of elements. \n List 1: %s \n List 2: %s'
                       % (list1, list2))
    result = []
    for first_elem, second_elem in zip(list1, list2):
      result.append(first_elem + second_elem)
    return result

  def __calculateNewParams(self, stepID):
    """ Calculates the new parameter set from the returned histograms. Only call if enough
    results have been reported back!

    :param int stepID: ID of the current step
    :returns: None
    """
    histograms = [self.stepResults[stepID].results[key] for key in self.stepResults[stepID].results.keys()]
    if not histograms:
      raise ValueError('No step results provided!')
    length = len(histograms)
    # Sum over elements of histograms
    result = histograms[0]
    for i in xrange(1, length):
      result = self.__addLists(result, histograms[i])
    # Divide by number of elements to calculate arithmetic mean
    number_of_elements = len(result)
    for i in xrange(0, number_of_elements):
      result[i] = result[i] / float(number_of_elements)
    return result

  def __mergePandoraLikelihoodXmlFiles(self):
    folder = "calib%s/stage%s/phase%s/" % (self.calibrationID, self.currentStage, self.currentPhase)
    filesToMerge = glob.glob(folder + "**/*.xml")
    outFileName = "newPandoraLikelihoodData.xml"

    #TODO how to get platform (e.g. x86_64-slc5-gcc43-opt) and appversion (e.g. ILCSoft-2019-02-20_gcc62)?
    #FIXME maybe one can use here ilcsoftpath? or is it better to extract path from Configuration Service?
    likelihoodMergeScriptPath = self.ops.getValue("/AvailableTarBalls/%s/%s/%s/CVMFSPath" % (platform,
                                                                                             'pandora_calibration_scripts', appversion), None)
    likelihoodMergeScript = os.path.join(mergeScriptPath, 'MergePandoraLikelihoodData.py')

    comm = 'python %s "main([%s],\'%s\')"' % (likelihoodMergeScript, ', '.join(("'%s'" % (ifile))
                                                                               for iFile in filesToMergeString), outFileName)
    res = shellCall(timeout=0, comm)

    return binaryFileToString(folder + '/' + outFileName)

  def __calculateNewCalibConstants(self, stepID):
    fileNamePattern = 'pfoanalysis_w*.root'
    fileDir = "calib%s/stage%s/phase%s/step%s/" % (self.calibrationID, self.currentStage, self.currentPhase, stepID)
    inputFilesPattern = os.path.join(fileDir + fileNamePattern)

    # TODO ask Andre to add separate entry for the directory with binaries from $ILCSOFT/PandoraAnalysis/HEAD/bin
    scriptPath = self.ops.getValue("/AvailableTarBalls/%s/%s/%s/CVMFSPath" % (platform,
                                                                              'pandora_calibration_scripts', appversion), None)

    truthEnergy = CalibrationPhase.sampleEnergyFromPhase(self.currentPhase)

    if self.currentPhase == CalibrationPhase.ECalDigi:
      binary = os.path.join(mergeScriptPath, 'ECalDigitisation_ContainedEvents')

      res = convert_and_execute([binary, '-a', inputFilesPattern, '-b', truthEnergy,
                                 '-c', digitisationAccuracy, '-d', fileDir, '-e', '90', '-g', 'Barrel',
                                 '-i', ecalBarrelCosThetaRange[0], '-j', ecalBarrelCosThetaRange[1]])

      res = convert_and_execute([binary, '-a', inputFilesPattern, '-b', truthEnergy,
                                 '-c', digitisationAccuracy, '-d', fileDir, '-e', '90', '-g', 'EndCap',
                                 '-i', ecalEndcapCosThetaRange[0], '-j', ecalEndcapCosThetaRange[1]])

      calibConstBarrel = float(execute_and_convert(['python', 'ECal_Digi_Extract.py', CALIBRATION_FILE,
                                                    truthEnergy, CALIBR_ECAL, 'Calibration_Constant', 'Barrel']))
      calibConstEndcap = float(execute_and_convert(['python', 'ECal_Digi_Extract.py', CALIBRATION_FILE,
                                                    truthEnergy, CALIBR_ECAL, 'Calibration_Constant', 'Endcap']))
      meanBarrel = float(execute_and_convert(['python', 'ECal_Digi_Extract.py', CALIBRATION_FILE,
                                              truthEnergy, CALIBR_ECAL, 'Mean', 'Barrel']))
      meanEndcap = float(execute_and_convert(['python', 'ECal_Digi_Extract.py', CALIBRATION_FILE,
                                              truthEnergy, CALIBR_ECAL, 'Mean', 'Endcap']))

      fractionalError = max(abs(meanBarrel - truthEnergy), abs(meanEndcap - truthEnergy)) / truthEnergy

      calibrationConstantsDict["processor[@name='MyDDCaloDigi']/parameter[@name='CalibrECAL']"]
      calibrationConstantsDict["processor[@name='MyDDCaloDigi']/parameter[@name='ECALEndcapCorrectionFactor']"]



class CalibrationHandler(RequestHandler):
  """ Handles the information exchange between worker nodes and the service """
  @classmethod
  def initializeHandler(cls, _):
    """ Initializes the handler, setting required variables. Called once in the beginning of the service """
    cls.activeCalibrations = {}
    cls.calibrationCounter = 0
    return S_OK()

  def initialize(self):
    """ Initializes a single response, setting required variables. Called once /per request/.
    """
    pass

  auth_createCalibration = ['authenticated']
  types_createCalibration = [basestring, basestring, dict, int, basestring, basestring]
  def export_createCalibration(self, steeringFile, ilcsoftPath, inputFiles, numberOfJobs):
    """ Called by users to create a calibration run (series of calibration iterations)

    :param basestring steeringFile: Steering file used in the calibration, LFN
    :param basestring marlinversion: Version of the Marlin application to be used for reconstruction
    :param inputFiles: Input files for the calibration. Dictionary
    :type inputFiles: `python:dict`
    :param int numberOfJobs: Number of jobs this service will run (actual number will be slightly lower)
    :returns: S_OK containing ID of the calibration, used to retrieve results etc
    :rtype: dict
    """
    CalibrationHandler.calibrationCounter += 1
    calibrationID = CalibrationHandler.calibrationCounter
    newRun = CalibrationRun(calibrationID, steeringFile, ilcsoftPath, inputFiles, numberOfJobs)
    CalibrationHandler.activeCalibrations[calibrationID] = newRun
    #newRun.submitJobs(calibrationID)
    #return S_OK(calibrationID)
    #FIXME: Check if lock is necessary.(Race condition?)
    res = newRun.submitJobs(calibrationID)  # , executionLock = False) #pylint: disable=unexpected-keyword-arg
    if _calibration_creation_failed(res):
      # FIXME: This should be treated, since the successfully submitted jobs will still run
      ret_val = S_ERROR('Submitting at least one of the jobs failed')
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
    #TODO: Fix race condition(if it exists)
    #TODO: different calibrations will use the same directory?
    calibration = CalibrationHandler.activeCalibrations.get(calibrationID, None)
    if not calibration:
      return S_ERROR('Calibration with ID %d not found.' % calibrationID)
    if stepID is calibration.currentStep:  # Only add result if it belongs to current step. Else ignore (it's ok)
      ## FIXME: use mkdir -p like implementation
      try:
        os.makedirs("calib%s/stage%s/phase%s/step%s" % (calibrationID, stageID, phaseID, stepID))
      except OSError:
        pass
      #FIXME filename depends from step (pfoanalysis either PandoraPhotonLikelihood)
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
    for calibrationID in CalibrationHandler.activeCalibrations:
      calibration = CalibrationHandler.activeCalibrations.get(calibrationID, None)
      if self.finalInterimResultReceived(calibration, calibration.currentStep):
        calibration.endCurrentStep()
    return S_OK()

  finishedJobsForNextStep = 0.9  # X% of all jobs must have finished in order for the next step to begin.

  def finalInterimResultReceived(self, calibration, stepID):
    """ Called periodically. Checks for the given calibration if we now have enough results to compute
    a new ParameterSet.

    :param CalibrationRun calibration: The calibration to check
    :param int stepID: The ID of the current step of that calibration
    :returns: True if enough results have been submitted, False otherwise
    :rtype: bool
    """
    #FIXME: Find out of this is susceptible to race condition
    import math
    numberOfResults = calibration.stepResults[stepID].getNumberOfResults()
    maxNumberOfJobs = calibration.numberOfJobs
    return numberOfResults >= math.ceil(CalibrationHandler.finishedJobsForNextStep * maxNumberOfJobs)

  auth_getNewParameters = ['authenticated']
  types_getNewParameters = [int, int]
  def export_getNewParameters(self, calibrationID, stepIDOnWorker):
    """ Called by the worker node to retrieve the parameters for the next iteration of the calibration

    :param int calibrationID: ID of the calibration being run on the worker
    :returns: S_ERROR in case of error (e.g. inactive calibration asking for params), S_OK with the parameter set and the id of the current step
    :rtype: dict
    """
    cal = CalibrationHandler.activeCalibrations.get(calibrationID, None)
    if not cal:
      gLogger.error("CalibrationID is not in active calibrations:",
                    "Active Calibrations:%s , asked for %s" % (self.activeCalibrations,
                                                               calibrationID))
      result = S_ERROR("calibrationID is not in active calibrations: %s\nThis should mean that the calibration has finished"
                       % calibrationID)
      return result
    res = cal.getNewParameters(stepIDOnWorker)
    res['currentPhase'] = cal.currentPhase
    res['currentStage'] = cal.currentStage
    return res

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
        if calibrationID == iCalib:
          jobsToResubmit.append(workerID)
      if jobsToResubmit:
        CalibrationHandler.submitJobs(iCalib, jobsToResubmit)  # pylint: disable=unexpected-keyword-arg

    if failedPairs:
      result = S_ERROR('Could not resubmit all jobs. Failed calibration/worker pairs are: %s' % failedPairs)
      result['failed_pairs'] = failedPairs
      return result
    else:
      return S_OK()

  #  auth_getNumberOfJobsPerCalibration = ['authenticated']
  auth_getNumberOfJobsPerCalibration = ['all']
  types_getNumberOfJobsPerCalibration = []
  def export_getNumberOfJobsPerCalibration(self):
    """ Returns a dictionary that maps active calibration IDs to the number of initial jobs they submitted.
    Used by the agent to determine when to resubmit jobs.

    :returns: S_OK containing the dictionary with mapping calibrationID -> numberOfJobs
    :rtype: dict
    """
    result = {}
    for calibrationID in CalibrationHandler.activeCalibrations:
      result[calibrationID] = CalibrationHandler.activeCalibrations[calibrationID].numberOfJobs
    return S_OK(result)

  def _getUsernameAndGroup(self):
    """ Returns name of the group and name of the user of the proxy the user is currently having
    Implementation is taken from DIRAC/Core/Security/BaseSecurity.py

    :returns: S_OK with value being dict with 'group' and 'username' entries or S_ERROR
    :rtype: `python:dict`
    """
    retVal = self.getCredentials()
    if not retVal['OK']:
      return retVal
    credDict = retVal['Value']
    if not credDict['isProxy']:
      return S_ERROR(DErrno.EX509, "chain does not contain a proxy")
    if not credDict['validDN']:
      return S_ERROR(DErrno.EDISET, "DN %s is not known in dirac" % credDict['subject'])
    if not credDict['validGroup']:
      return S_ERROR(DErrno.EDISET, "Group %s is invalid for DN %s" % (credDict['group'], credDict['subject']))
    usernameAndGroupDict = {'group': credDict['group'], 'username': credDict['username']}
    return S_OK(usernameAndGroupDict)

#TODO: Add stopping criterion to calibration loop. This should be checked when new parameter sets are calculated
#In that case, the calibration should be removed from activeCalibrations and the result stored.
#Should we then kill all jobs of that calibration?

####################################################################
#                                                                  #
#         Testcode, not to be used by production code              #
#                                                                  #
####################################################################

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

    :returns: S_OK containing a tuple with the active calibrations dict (serialized with the pickle module) and the calibrationCounter
    :rtype: dict
    """
    import copy
    import pickle
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
    from DIRAC import gConfig
    return S_OK(gConfig.getValue(option))

  auth_getproxy_info = ['all']
  types_getproxy_info = []
  def export_getproxy_info(self):
    """ Returns the info of the proxy this service is using.

    :returns: S_OK containing the proxy info
    :rtype: dict
    """
    from DIRAC.Core.Security.ProxyInfo import getProxyInfo
    return S_OK(getProxyInfo())


def _calibration_creation_failed(results):
  """ Returns whether or not the creation of all calibration jobs was successful.

  :param results: List of S_OK/S_ERROR dicts that were returned by the submission call
  :returns: True if everything was successful, False otherwise
  :rtype: bool
  """
  success = True
  for job_result in results:
    success = success and job_result['OK']
  return not success

def _getLCIOInputFiles(xml_file):
  """ Extracts the LCIO input file from the given marlin input xml

  :param string xml_file: Path to the Marlin input xml
  :returns: List with all LCIOInput files
  :rtype: list
  """
  result = []
  tree = ET.parse(xml_file)
  root = tree.getroot()
  elements = root.findall(".//parameter")
  for element in elements:
    if element.get('name', '') == 'LCIOInputFiles' and not element.text.count('afs'):
      result.append(element.text.strip())
  return result
