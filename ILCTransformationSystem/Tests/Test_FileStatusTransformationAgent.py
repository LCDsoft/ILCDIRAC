""" Test FileStatusTransformationAgent """

import unittest

import ILCDIRAC.ILCTransformationSystem.Agent.FileStatusTransformationAgent as FST
import DIRAC.Resources.Storage.StorageElement as SeModule
from ILCDIRAC.ILCTransformationSystem.Agent.FileStatusTransformationAgent import FileStatusTransformationAgent

from mock import MagicMock

from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger
import DIRAC

__RCSID__ = "$Id$"


class TestFSTAgent(unittest.TestCase):
  """ Test_fstAgent class """

  def setUp(self):
    self.agent = FST
    self.agent.AgentModule = MagicMock()
    self.fstAgent = FileStatusTransformationAgent()

    self.fstAgent.log = gLogger
    self.fstAgent.tClient = MagicMock(
        name="transMock", spec=DIRAC.TransformationSystem.Client.TransformationClient.TransformationClient)
    self.fstAgent.fcClient = MagicMock(name="fcMock", spec=DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient)
    self.fstAgent.reqClient = MagicMock(name="reqMock", spec=DIRAC.RequestManagementSystem.Client.ReqClient)
    self.fstAgent.nClient = MagicMock(name="nMock", spec=DIRAC.FrameworkSystem.Client.NotificationClient)

    self.fstAgent.tClient.setTaskStatus = MagicMock()
    self.fstAgent.enabled = True

    self.fakeTransID = 1L

    self.failedTask = {'TargetSE': 'DESY-SRM',
                       'TransformationID': 400103L,
                       'ExternalStatus': 'Failed',
                       'ExternalID': 0,
                       'TaskID': 0}

    self.doneTask = {'TargetSE': 'DESY-SRM',
                     'TransformationID': 400103L,
                     'ExternalID': 1,
                     'ExternalStatus': 'Done',
                     'TaskID': 1}

    self.notAvailableOnSrc = '/ilc/file_not_available_on_src'
    self.notAvailableOnDst = '/ilc/file_not_available_on_target'
    self.available = '/ilc/file_on_src_and_target'
    self.notAvailable = '/ilc/file_not_on_src_and_target'

    self.sourceSE = ['CERN-SRM']
    self.targetSE = ['DESY-SRM']

  def tearDown(self):
    pass

  def test_init(self):
    self.assertIsInstance(self.fstAgent, FileStatusTransformationAgent)
    self.assertIsInstance(self.fstAgent.tClient, MagicMock)
    self.assertIsInstance(self.fstAgent.fcClient, MagicMock)
    self.assertIsInstance(self.fstAgent.reqClient, MagicMock)
    self.assertTrue(self.fstAgent.enabled)
    self.assertEquals(self.fstAgent.transformationTypes, ['Replication'])
    self.assertEquals(self.fstAgent.transformationStatuses, ['Active'])

  def test_get_transformations_failure(self):
    """ fstAgent should stop execution cycle if tClient getTransformations returns an error """
    self.fstAgent.processTransformation = MagicMock()
    self.fstAgent.tClient.getTransformations.return_value = S_ERROR()

    res = self.fstAgent.execute()
    self.fstAgent.processTransformation.assert_not_called()
    self.assertFalse(res['OK'])

  def test_no_transformations_found(self):
    """ fstAgent should stop execution cycle if no transformations are found """
    self.fstAgent.processTransformation = MagicMock()
    self.fstAgent.tClient.getTransformations.return_value = S_OK([])

    res = self.fstAgent.execute()
    self.fstAgent.processTransformation.assert_not_called()
    self.assertTrue(res['OK'])

  def test_process_transformation_failure(self):
    """ fstAgent should not exit if processing of some transformation returns an Error,
        all transformations should be processed independently """
    self.fstAgent.processTransformation = MagicMock()
    self.fstAgent.getTransformations = MagicMock()

    transformations = [{'TransformationID': 1, 'SourceSE': ['CERN'], 'TargetSE': ['DESY'], 'DataTransType': 'x'},
                       {'TransformationID': 2, 'SourceSE': ['CERN'], 'TargetSE': ['DESY'], 'DataTransType': 'x'}]
    self.fstAgent.getTransformations.return_value = S_OK(transformations)
    self.fstAgent.processTransformation.return_value = S_ERROR()

    self.fstAgent.execute()
    self.assertEquals(len(self.fstAgent.processTransformation.mock_calls), 2)

  def _getOption(self, option, defaultVal):
    if option != "TransformationFileStatuses":
      return defaultVal
    defaultVal.append("UnknownStatus")
    return defaultVal

  def test_unknown_file_status(self):
    """ fstAgent should not process file statuses for whom check files function is not implemented """
    allowedFileStatuses = ["Assigned", "Problematic", "Processed", "Unused"]
    self.fstAgent.am_getOption = MagicMock(side_effect=self._getOption)
    self.fstAgent.am_setOption = MagicMock(side_effect=self._getOption)
    self.fstAgent.beginExecution()
    self.assertItemsEqual(self.fstAgent.transformationFileStatuses, allowedFileStatuses)

  def test_get_transformations(self):
    """ Test for getTransformations function """
    self.fstAgent.tClient.getTransformationParameters = MagicMock()
    self.fstAgent.getDataTransformationType = MagicMock()
    self.fstAgent.processTransformation = MagicMock()
    self.fstAgent.sendNotification = MagicMock()

    self.fstAgent.tClient.getTransformations.return_value = S_ERROR()
    res = self.fstAgent.getTransformations()
    self.assertFalse(res['OK'])

    res = self.fstAgent.getTransformations(transID=self.fakeTransID)
    self.fstAgent.tClient.getTransformation.called_once_with(condDict={'TransformationID': self.fakeTransID,
                                                                       'Status': self.fstAgent.transformationStatuses,
                                                                       'Type': self.fstAgent.transformationTypes})

    self.fstAgent.tClient.getTransformations.return_value = S_OK([{'Status': 'Active',
                                                                   'TransformationID': self.fakeTransID,
                                                                   'Type': 'Replication'}])
    self.fstAgent.tClient.getTransformationParameters.return_value = S_ERROR()
    self.fstAgent.execute()
    self.fstAgent.processTransformation.assert_not_called()
    self.fstAgent.sendNotification.assert_called()

    self.fstAgent.tClient.getTransformationParameters.return_value = S_OK({'TargetSE': "['CERN-DST-EOS']",
                                                                           'SourceSE': "['CERN-SRM']"})
    self.fstAgent.processTransformation.reset_mock()
    self.fstAgent.getDataTransformationType.return_value = S_ERROR()
    self.fstAgent.execute()
    self.fstAgent.processTransformation.assert_not_called()
    self.fstAgent.sendNotification.assert_called()

    self.fstAgent.getDataTransformationType.return_value = S_OK(FST.REPLICATION_TRANS)
    self.fstAgent.processTransformation.reset_mock()
    self.fstAgent.execute()
    self.fstAgent.processTransformation.assert_called_once_with(self.fakeTransID, ['CERN-SRM'], ['CERN-DST-EOS'],
                                                                FST.REPLICATION_TRANS)

  def test_send_notification(self):
    """ Test for sendNotification function """
    dataTransType = FST.REPLICATION_TRANS
    sourceSE = ['CERN-SRM']
    targetSE = ['DESY-SRM']
    errList = ["some error occured", "some other error"]
    accDict = {FST.SET_PROCESSED: [{'LFN': '/ilc/fake/lfn',
                                    'Status': 'Problematic',
                                    'AvailableOnSource': True,
                                    'AvailableOnTarget': True}]}
    self.fstAgent.nClient.sendMail = MagicMock()

    # email should not be sent if accounting dict and errors list is empty
    self.fstAgent.accounting = {}
    self.fstAgent.errors = []
    self.fstAgent.sendNotification(self.fakeTransID, dataTransType, sourceSE, targetSE)
    self.fstAgent.nClient.sendMail.assert_not_called()

    # email should be sent if accounting dict is empty but errors list contains some error strings
    self.fstAgent.errors = errList
    self.fstAgent.sendNotification(self.fakeTransID, dataTransType, sourceSE, targetSE)
    self.fstAgent.nClient.sendMail.assert_called()

    # email should be sent if errors list is empty but accounting dict contains some values
    self.fstAgent.nClient.sendMail.reset_mock()
    self.fstAgent.accounting = accDict
    self.fstAgent.errors = []
    self.fstAgent.sendNotification(self.fakeTransID, dataTransType, sourceSE, targetSE)
    self.fstAgent.nClient.sendMail.assert_called()

    # try sending email to all addresses even if we get error for sending email to some address
    self.fstAgent.nClient.sendMail.reset_mock()
    self.fstAgent.errors = errList
    self.fstAgent.accounting = accDict
    self.fstAgent.addressTo = ["name1@cern.ch", "name2@cern.ch"]
    self.fstAgent.nClient.sendMail.return_value = S_ERROR()
    res = self.fstAgent.sendNotification(self.fakeTransID, dataTransType, sourceSE, targetSE)
    self.assertEquals(len(self.fstAgent.nClient.sendMail.mock_calls), len(self.fstAgent.addressTo))
    self.assertTrue(res['OK'])

    # accounting dict and errors list should be cleared after notification is sent
    self.assertEquals(self.fstAgent.accounting, {})
    self.assertEquals(self.fstAgent.errors, [])

  def test_get_data_transformation_type(self):
    """ Test if getDataTransformationType function correctly returns the
        Data Transformation Type (Replication / Moving) """
    self.fstAgent.tClient.getTransformationParameters = MagicMock()

    self.fstAgent.tClient.getTransformationParameters.return_value = S_ERROR()
    res = self.fstAgent.getDataTransformationType(self.fakeTransID)
    self.assertFalse(res['OK'])

    self.fstAgent.tClient.getTransformationParameters.return_value = S_OK("")
    res = self.fstAgent.getDataTransformationType(self.fakeTransID)['Value']
    self.assertEquals(res, FST.REPLICATION_TRANS)

    self.fstAgent.tClient.getTransformationParameters.return_value = S_OK("sdfdsfdsfs")
    res = self.fstAgent.getDataTransformationType(self.fakeTransID)
    self.assertFalse(res['OK'])

    self.fstAgent.tClient.getTransformationParameters.return_value = S_OK(
        '[["ReplicateAndRegister", {"TargetSE": ["CERN-SRM"], "SourceSE": "CERN-DST-EOS"}],'
        '["RemoveReplica", {"TargetSE": "CERN-DST-EOS"}]]')
    res = self.fstAgent.getDataTransformationType(self.fakeTransID)['Value']
    self.assertEquals(res, FST.MOVING_TRANS)

    self.fstAgent.tClient.getTransformationParameters.return_value = S_OK(
        '[["RemoveReplica", {"TargetSE": "CERN-DST-EOS"}]]')
    res = self.fstAgent.getDataTransformationType(self.fakeTransID)
    self.assertFalse(res['OK'])

    self.fstAgent.tClient.getTransformationParameters.return_value = S_OK(
        'RemoveReplica:CERN-DST-EOS;ReplicateAndRegister')
    res = self.fstAgent.getDataTransformationType(self.fakeTransID)['Value']
    self.assertEquals(res, FST.MOVING_TRANS)

    self.fstAgent.tClient.getTransformationParameters.return_value = S_OK('RemoveReplica:CERN-DST-EOS')
    res = self.fstAgent.getDataTransformationType(self.fakeTransID)
    self.assertFalse(res['OK'])

    self.fstAgent.tClient.getTransformationParameters.return_value = S_OK(
        '[["ReplicateAndRegister", {"TargetSE": ["CERN-SRM"], "SourceSE": "CERN-DST-EOS"}]]')
    res = self.fstAgent.getDataTransformationType(self.fakeTransID)['Value']
    self.assertEquals(res, FST.REPLICATION_TRANS)

    self.fstAgent.tClient.getTransformationParameters.return_value = S_OK('ReplicateAndRegister')
    res = self.fstAgent.getDataTransformationType(self.fakeTransID)['Value']
    self.assertEquals(res, FST.REPLICATION_TRANS)

  def test_get_request_status(self):
    """ Test getRequestStatus function """
    taskID = 1
    taskIDs = [taskID]
    self.fstAgent.tClient.getTransformationTasks.return_value = S_ERROR()
    res = self.fstAgent.getRequestStatus(self.fakeTransID, taskIDs)
    self.assertFalse(res['OK'])

    self.fstAgent.tClient.getTransformationTasks.return_value = S_OK([{'TaskID': taskID,
                                                                       'ExternalStatus': 'Failed',
                                                                       'ExternalID': 123}])
    res = self.fstAgent.getRequestStatus(self.fakeTransID, taskIDs)
    result = res['Value']
    self.assertEquals(result[taskID]['RequestStatus'], 'Failed')
    self.assertEquals(result[taskID]['RequestID'], 123)

  def test_set_file_status(self):
    """ Test for setFileStatus function """
    transFiles = []
    newStatus = "processed"
    self.fstAgent.setFileStatus(self.fakeTransID, transFiles, newStatus)
    self.fstAgent.tClient.setFileStatusForTransformation.assert_not_called()

    lfn1 = '/ilc/fake/lfn1'
    lfn2 = '/ilc/fake/lfn2'
    transFiles = [{'LFN': lfn1, 'Status': 'Problematic', 'AvailableOnSource': True, 'AvailableOnTarget': True},
                  {'LFN': lfn2, 'Status': 'Problematic', 'AvailableOnSource': True, 'AvailableOnTarget': True}]

    self.fstAgent.tClient.setFileStatusForTransformation.return_value = S_ERROR()
    res = self.fstAgent.setFileStatus(self.fakeTransID, transFiles, newStatus)
    self.assertFalse(res['OK'])

    self.fstAgent.tClient.setFileStatusForTransformation.reset_mock()
    self.fstAgent.tClient.setFileStatusForTransformation.return_value = S_OK()
    self.fstAgent.setFileStatus(self.fakeTransID, transFiles, newStatus)
    self.fstAgent.tClient.setFileStatusForTransformation.assert_called_once_with(self.fakeTransID, newLFNsStatus={
                                                                                 lfn1: newStatus, lfn2: newStatus},
                                                                                 force=True)
    self.assertTrue(newStatus in self.fstAgent.accounting)
    self.assertEquals(len(self.fstAgent.accounting[newStatus]), 2)

  def test_exists_in_FC(self):
    """ Test if the existsInFC function correctly determines if all replicas of files are registered in FC """
    se1 = 'CERN-SRM'
    se2 = 'DESY-SRM'
    storageElements = [se1, se2]

    # file exists in FC and all replicas are registered
    fileExists = '/ilc/file/file1'

    # file exists in FC, but one replica is missing in FC
    fileOneRepLost = '/ilc/file/file2'

    # file exists in FC, but no replicas are registered in FC
    fileAllRepLost = '/ilc/file/file3'

    # file does not exist in FC
    fileRemoved = '/ilc/file/file4'

    files = [fileExists, fileOneRepLost, fileAllRepLost, fileRemoved]

    self.fstAgent.fcClient.getReplicas.return_value = S_ERROR()
    res = self.fstAgent.existsInFC(storageElements, files)
    self.assertFalse(res['OK'])

    self.fstAgent.fcClient.getReplicas.return_value = S_OK({'Successful': {fileExists: {se1: fileExists,
                                                                                        se2: fileExists},
                                                                           fileOneRepLost: {se1: fileOneRepLost},
                                                                           fileAllRepLost: {}},
                                                            'Failed': {fileRemoved: 'No such file or directory'}})
    res = self.fstAgent.existsInFC(storageElements, files)['Value']
    self.assertTrue(res['Successful'][fileExists])
    self.assertFalse(res['Successful'][fileOneRepLost])
    self.assertFalse(res['Successful'][fileAllRepLost])
    self.assertFalse(res['Successful'][fileRemoved])

  def test_exists_on_storage_element(self):
    """ Test if the existsOnSE function correctly determines if a file
        exists on all provided Storage Elements or not """

    se1 = 'CERN-SRM'
    se2 = 'DESY-SRM'
    storageElements = [se1, se2]

    # file exists on all SEs
    fileExists = '/ilc/file/file1'

    # file is lost on one SE
    fileOneRepLost = '/ilc/file/file2'

    # file is lost on all SEs
    fileAllRepLost = '/ilc/file/file3'

    # some error to get file status on SE
    fileFailed = '/ilc/file/file4'

    files = [fileExists, fileOneRepLost, fileAllRepLost, fileFailed]

    se1Result = S_OK({'Successful': {fileExists: True, fileOneRepLost: True, fileAllRepLost: False, fileFailed: True},
                      'Failed': {}})

    se2Result = S_OK({'Successful': {fileExists: True, fileOneRepLost: False, fileAllRepLost: False},
                      'Failed': {fileFailed: 'permission denied'}})

    SeModule.StorageElementItem.exists = MagicMock()
    SeModule.StorageElementItem.exists.return_value = S_ERROR()
    res = self.fstAgent.existsOnSE(storageElements, files)
    self.assertFalse(res['OK'])

    res = self.fstAgent.existsOnSE(storageElements, [])
    self.assertTrue(res['OK'])
    self.assertEquals(res['Value']['Successful'], {})
    self.assertEquals(res['Value']['Failed'], {})

    SeModule.StorageElementItem.exists.side_effect = [se1Result, se2Result]
    res = self.fstAgent.existsOnSE(storageElements, files)['Value']

    self.assertTrue(res['Successful'][fileExists])
    self.assertFalse(res['Successful'][fileAllRepLost])
    self.assertFalse(res['Successful'][fileOneRepLost])
    self.assertTrue(len(res['Failed']), 1)

  def test_exists(self):
    """ Tests if the exists function correctly determines if a file exists in File Catalog and Storage Elements """

    se1 = 'CERN-SRM'
    se2 = 'DESY-SRM'
    storageElements = ['CERN-SRM', 'DESY-SRM']

    # file exists in fc and all SEs
    fileExists = '/ilc/file/file1'

    # file exists in fc but one replica is lost on SE
    fileOneRepLostOnSE = '/ilc/file/file2'

    # file exists in fc but all replicas are lost on SEs
    fileAllRepLostOnSEs = '/ilc/file/file3'

    # file does not exists in FC
    fileRemoved = '/ilc/file/file4'

    files = [fileExists, fileOneRepLostOnSE, fileAllRepLostOnSEs, fileRemoved]

    self.fstAgent.existsInFC = MagicMock()
    self.fstAgent.existsOnSE = MagicMock()

    self.fstAgent.existsInFC.return_value = S_ERROR()
    res = self.fstAgent.exists(storageElements, files)
    self.assertFalse(res['OK'])

    # if no files were found in FC, no need to check the lfns on storage
    self.fstAgent.existsInFC.return_value = S_OK({'Successful': {fileRemoved: False},
                                                  'Failed': {}})
    self.fstAgent.exists(storageElements, [fileRemoved])
    self.fstAgent.existsOnSE.not_called()

    self.fstAgent.existsInFC.return_value = S_OK({'Successful': {fileExists: True, fileOneRepLostOnSE: True,
                                                                 fileAllRepLostOnSEs: True, fileRemoved: False},
                                                  'Failed': {}})
    self.fstAgent.existsOnSE.return_value = S_ERROR()
    res = self.fstAgent.exists(storageElements, files)
    self.assertFalse(res['OK'])

    self.fstAgent.existsOnSE.return_value = S_OK({'Successful': {},
                                                  'Failed': {se1: {fileExists: "permission denied",
                                                                   fileOneRepLostOnSE: "permission denied",
                                                                   fileAllRepLostOnSEs: "permission denied"},
                                                             se2: {}}})
    res = self.fstAgent.exists(storageElements, files)
    self.assertFalse(res['OK'])

    self.fstAgent.existsOnSE.return_value = S_OK({'Successful': {fileExists: True, fileOneRepLostOnSE: False,
                                                                 fileAllRepLostOnSEs: False},
                                                  'Failed': {se1: {}, se2: {}}})
    res = self.fstAgent.exists(storageElements, files)['Value']['Successful']
    self.assertTrue(res[fileExists])
    self.assertFalse(res[fileOneRepLostOnSE])
    self.assertFalse(res[fileAllRepLostOnSEs])
    self.assertFalse(res[fileRemoved])

  def test_select_failed_requests(self):
    """ Test if selectFailedRequests function returns True if transfile has a failed request """

    transFileWithFailedReq = {'TransformationID': 400103, 'TaskID': 0, 'LFN': '/ilc/file1'}
    transFileWithDoneReq = {'TransformationID': 400103, 'TaskID': 1, 'LFN': '/ilc/file2'}

    self.fstAgent.tClient.getTransformationTasks.return_value = S_ERROR()
    res = self.fstAgent.selectFailedRequests(transFileWithFailedReq)
    self.assertFalse(res)

    self.fstAgent.tClient.getTransformationTasks.return_value = S_OK([self.failedTask])
    res = self.fstAgent.selectFailedRequests(transFileWithFailedReq)
    self.assertTrue(res)

    self.fstAgent.tClient.getTransformationTasks.return_value = S_OK([self.doneTask])
    res = self.fstAgent.selectFailedRequests(transFileWithDoneReq)
    self.assertFalse(res)

  def test_retry_strategy_for_files(self):
    """ Test if the request exists then retry strategy is resetting the request otherwise set the file to unused """

    taskIDfile1 = 1
    taskIDfile2 = 2
    transFiles = [{'TransformationID': self.fakeTransID, 'TaskID': taskIDfile1, 'LFN': '/ilc/file1'},
                  {'TransformationID': self.fakeTransID, 'TaskID': taskIDfile2, 'LFN': '/ilc/file2'}]

    self.fstAgent.getRequestStatus = MagicMock()
    self.fstAgent.getRequestStatus.return_value = S_ERROR()
    res = self.fstAgent.retryStrategyForFiles(self.fakeTransID, transFiles)
    self.assertFalse(res['OK'])

    self.fstAgent.getRequestStatus.return_value = S_OK({taskIDfile1: {'RequestStatus': 'Problematic',
                                                                      'RequestID': 1},
                                                        taskIDfile2: {'RequestStatus': 'Problematic',
                                                                      'RequestID': 2}})

    # no request exists for first trans file and one request exists for second trans file
    self.fstAgent.reqClient.getRequest = MagicMock()
    self.fstAgent.reqClient.getRequest.side_effect = [S_ERROR('Request does not exist'), S_OK('Request exists')]

    res = self.fstAgent.retryStrategyForFiles(self.fakeTransID, transFiles)['Value']

    self.assertEquals(res[taskIDfile1]['Strategy'], FST.SET_UNUSED)
    self.assertEquals(res[taskIDfile2]['Strategy'], FST.RESET_REQUEST)

  def test_retry_files(self):
    """ Test for retryFiles function """
    transFiles = [{'LFN': '/ilc/file1', 'Status': 'Problematic', 'AvailableOnSource': True,
                   'AvailableOnTarget': True, 'TaskID': 1},
                  {'LFN': '/ilc/file2', 'Status': 'Problematic', 'AvailableOnSource': True,
                   'AvailableOnTarget': True, 'TaskID': 2}]

    self.fstAgent.retryStrategyForFiles = MagicMock()
    self.fstAgent.setFileStatus = MagicMock()
    self.fstAgent.reqClient.resetFailedRequest = MagicMock()

    self.fstAgent.retryStrategyForFiles.return_value = S_ERROR()
    res = self.fstAgent.retryFiles(self.fakeTransID, transFiles)
    self.assertFalse(res['OK'])

    self.fstAgent.retryStrategyForFiles.return_value = S_OK({1: {'Strategy': FST.RESET_REQUEST,
                                                                 'RequestID': 1},
                                                             2: {'Strategy': FST.SET_UNUSED}})
    self.fstAgent.reqClient.resetFailedRequest.return_value = S_ERROR()
    self.fstAgent.retryFiles(self.fakeTransID, transFiles)
    self.fstAgent.tClient.setTaskStatus.assert_not_called()

    self.fstAgent.reqClient.resetFailedRequest.return_value = S_OK("Not reset")
    self.fstAgent.retryFiles(self.fakeTransID, transFiles)
    self.fstAgent.tClient.setTaskStatus.assert_not_called()

    self.fstAgent.tClient.setTaskStatus.reset_mock()
    self.fstAgent.reqClient.resetFailedRequest.reset_mock()
    self.fstAgent.setFileStatus.reset_mock()
    self.fstAgent.reqClient.resetFailedRequest.return_value = S_OK()
    self.fstAgent.tClient.setTaskStatus.return_value = S_OK()
    self.fstAgent.retryFiles(self.fakeTransID, transFiles)
    self.fstAgent.reqClient.resetFailedRequest.assert_called_once_with(1, allR=True)
    self.fstAgent.tClient.setTaskStatus.assert_called_once_with(self.fakeTransID, 1, 'Waiting')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [transFiles[1]], 'Unused')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [transFiles[0]], 'Assigned')

    # set file statues to unused should not be called if all files have reset request strategy
    self.fstAgent.retryStrategyForFiles.return_value = S_OK({1: {'Strategy': FST.RESET_REQUEST,
                                                                 'RequestID': 1},
                                                             2: {'Strategy': FST.RESET_REQUEST,
                                                                 'RequestID': 2}})
    self.fstAgent.setFileStatus.reset_mock()
    self.fstAgent.reqClient.resetFailedRequest.reset_mock()
    self.fstAgent.retryFiles(self.fakeTransID, transFiles)
    self.assertEquals(len(self.fstAgent.reqClient.resetFailedRequest.mock_calls), 2)
    self.fstAgent.setFileStatus.not_called()
    self.fstAgent.setFileStatus.assert_called_once_with(self.fakeTransID, transFiles, 'Assigned')

  def _exists(self, se, lfns):
    """ returns lfns availability information """
    result = {}
    for lfn in lfns:
      if (lfn == self.notAvailableOnSrc or lfn == self.notAvailable) and se == self.sourceSE:
        result[lfn] = False
      elif (lfn == self.notAvailableOnDst or lfn == self.notAvailable) and se == self.targetSE:
        result[lfn] = False
      else:
        result[lfn] = True

    return S_OK({'Successful': result})

  def test_trans_files_treatment(self):
    """ test transformation files are treated properly (set new status / reset request)
        for replication and moving transformations """
    self.fstAgent.setFileStatus = MagicMock()
    self.fstAgent.sendNotification = MagicMock()

    fileNotAvailableOnSrc = {'TransformationID': self.fakeTransID, 'TaskID': 1, 'LFN': self.notAvailableOnSrc}
    fileNotAvailableOnDst = {'TransformationID': self.fakeTransID, 'TaskID': 2, 'LFN': self.notAvailableOnDst}
    fileAvailable = {'TransformationID': self.fakeTransID, 'TaskID': 3, 'LFN': self.available}
    fileNotAvailable = {'TransformationID': self.fakeTransID, 'TaskID': 4, 'LFN': self.notAvailable}

    transFiles = [fileNotAvailableOnSrc, fileNotAvailableOnDst, fileAvailable, fileNotAvailable]

    # all trans files have failed requests
    self.fstAgent.selectFailedRequests = MagicMock(return_value={tFile['TaskID']: True for tFile in transFiles})

    # no assosiated request in rms
    self.fstAgent.retryStrategyForFiles = MagicMock(return_value=S_OK(
        {tFile['TaskID']: {'Strategy': FST.SET_UNUSED} for tFile in transFiles}))

    self.fstAgent.exists = MagicMock()

    # all file statuses should be processed even if getTransformationFiles returns an error for some status
    self.fstAgent.transformationFileStatuses = ['Assigned', 'Problematic']
    self.fstAgent.tClient.getTransformationFiles.return_value = S_ERROR()
    self.fstAgent.processTransformation(self.fakeTransID, self.sourceSE, self.targetSE, FST.REPLICATION_TRANS)
    self.assertEquals(len(self.fstAgent.tClient.getTransformationFiles.mock_calls), 2)

    # all file statuses should be processed even if no transformation files are found for some status
    self.fstAgent.tClient.getTransformationFiles.reset_mock()
    self.fstAgent.tClient.getTransformationFiles.return_value = S_OK([])
    self.fstAgent.processTransformation(self.fakeTransID, self.sourceSE, self.targetSE, FST.REPLICATION_TRANS)
    self.assertEquals(len(self.fstAgent.tClient.getTransformationFiles.mock_calls), 2)

    # all file statuses should be processed even if we get a failure to determine if transformation files
    # with some status exists in FileCatalog and StorageElements
    self.fstAgent.tClient.getTransformationFiles.reset_mock()
    self.fstAgent.tClient.getTransformationFiles.return_value = S_OK(transFiles)
    self.fstAgent.exists.return_value = S_ERROR()
    self.fstAgent.processTransformation(self.fakeTransID, self.sourceSE, self.targetSE, FST.REPLICATION_TRANS)
    self.assertEquals(len(self.fstAgent.tClient.getTransformationFiles.mock_calls), 2)

    self.fstAgent.tClient.getTransformationFiles.return_value = S_OK(transFiles)
    self.fstAgent.exists.side_effect = self._exists
    self.fstAgent.transformationFileStatuses = ['Assigned']

    # check replication transformation treatment for assigned files
    self.fstAgent.setFileStatus.reset_mock()
    self.fstAgent.processTransformation(self.fakeTransID, self.sourceSE, self.targetSE, FST.REPLICATION_TRANS)
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [fileNotAvailableOnSrc, fileAvailable], 'Processed')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [fileNotAvailableOnDst], 'Unused')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [fileNotAvailable], 'Deleted')

    # check moving transformation treatment for assigned files
    self.fstAgent.setFileStatus.reset_mock()
    self.fstAgent.processTransformation(self.fakeTransID, self.sourceSE, self.targetSE, FST.MOVING_TRANS)
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [fileNotAvailableOnSrc], 'Processed')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [fileNotAvailableOnDst, fileAvailable], 'Unused')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [fileNotAvailable], 'Deleted')

    self.fstAgent.transformationFileStatuses = ['Processed']

    # check replication transformation treatment for processed files
    self.fstAgent.setFileStatus.reset_mock()
    self.fstAgent.processTransformation(self.fakeTransID, self.sourceSE, self.targetSE, FST.REPLICATION_TRANS)
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [fileNotAvailableOnDst], 'Unused')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [fileNotAvailable], 'Deleted')

    # check moving transformation treatment for processed files
    self.fstAgent.setFileStatus.reset_mock()
    self.fstAgent.processTransformation(self.fakeTransID, self.sourceSE, self.targetSE, FST.MOVING_TRANS)
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [fileNotAvailableOnDst, fileAvailable], 'Unused')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [fileNotAvailable], 'Deleted')

    self.fstAgent.transformationFileStatuses = ['Problematic']

    # check replication transformation treatment for problematic files
    self.fstAgent.setFileStatus.reset_mock()
    self.fstAgent.processTransformation(self.fakeTransID, self.sourceSE, self.targetSE, FST.REPLICATION_TRANS)
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [fileNotAvailableOnSrc, fileAvailable], 'Processed')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [fileNotAvailableOnDst], 'Unused')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [fileNotAvailable], 'Deleted')

    # check moving transformation treatment for problematic files
    self.fstAgent.setFileStatus.reset_mock()
    self.fstAgent.processTransformation(self.fakeTransID, self.sourceSE, self.targetSE, FST.MOVING_TRANS)
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [fileNotAvailableOnSrc], 'Processed')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [fileNotAvailableOnDst, fileAvailable], 'Unused')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [fileNotAvailable], 'Deleted')

    self.fstAgent.transformationFileStatuses = ['Unused']

    # check replication transformation treatment for unused files
    self.fstAgent.setFileStatus.reset_mock()
    self.fstAgent.processTransformation(self.fakeTransID, self.sourceSE, self.targetSE, FST.REPLICATION_TRANS)
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [fileNotAvailableOnSrc], 'Processed')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [fileNotAvailable], 'Deleted')

    # check moving transformation treatment for unused files
    self.fstAgent.setFileStatus.reset_mock()
    self.fstAgent.processTransformation(self.fakeTransID, self.sourceSE, self.targetSE, FST.MOVING_TRANS)
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [fileNotAvailableOnSrc], 'Processed')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [fileNotAvailable], 'Deleted')


if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase(TestFSTAgent)
  TESTRESULT = unittest.TextTestRunner(verbosity=3).run(SUITE)
