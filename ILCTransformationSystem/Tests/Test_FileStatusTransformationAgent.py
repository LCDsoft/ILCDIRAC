""" Test FileStatusTransformationAgent """

import unittest
import importlib

import ILCDIRAC.ILCTransformationSystem.Agent.FileStatusTransformationAgent as FST
from ILCDIRAC.ILCTransformationSystem.Agent.FileStatusTransformationAgent import FileStatusTransformationAgent

from mock import MagicMock

from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger
import DIRAC

__RCSID__ = "$Id$"

class TestFSTAgent( unittest.TestCase ):
  """ Test_fstAgent class """

  def setUp(self):
    self.agent = importlib.import_module('ILCDIRAC.ILCTransformationSystem.Agent.FileStatusTransformationAgent')
    self.agent.AgentModule = MagicMock()
    self.fstAgent = FileStatusTransformationAgent()

    self.fstAgent.log = gLogger
    self.fstAgent.tClient = MagicMock( name = "transMock",spec=DIRAC.TransformationSystem.Client.TransformationClient.TransformationClient)
    self.fstAgent.fcClient = MagicMock( name = "fcMock" , spec=DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient )
    self.fstAgent.reqClient = MagicMock( name = "reqMock" , spec=DIRAC.RequestManagementSystem.Client.ReqClient )

    self.fstAgent.reqClient.resetFailedRequest = MagicMock()
    self.fstAgent.tClient.setTaskStatus = MagicMock()
    self.fstAgent.setFileStatus = MagicMock()
    self.fstAgent.enabled = True

    self.transformations = [{'Status':'Active',
                             'TransformationID': 1L,
                             'Description':'Replicate files for prodID 777 to CERN-DST-EOS',
                             'Type':'Replication',
                             'TransformationName':'replicate_777_CERN-DST-EOS'}]
    self.fakeTransID = 1L

    self.failedTask = {'TargetSE': 'DESY-SRM',
                       'TransformationID': 400103L,
                       'ExternalStatus': 'Failed',
                       'ExternalID' :0,
                       'TaskID': 0}

    self.doneTask = {'TargetSE': 'DESY-SRM',
                     'TransformationID': 400103L,
                     'ExternalID' :1,
                     'ExternalStatus': 'Done',
                     'TaskID': 1}

    self.notAvailableOnSrc = '/ilc/file_not_available_on_src'
    self.notAvailableOnDst = '/ilc/file_not available_on_target'
    self.available = '/ilc/file_available_on_src_and_target'
    self.notAvailable = '/ilc/file_not_available_on_src_and_target'

    self.tFile1 = { 'TransformationID': self.fakeTransID, 'TaskID': 1, 'LFN': self.notAvailableOnSrc }
    self.tFile2 = { 'TransformationID': self.fakeTransID, 'TaskID': 2, 'LFN': self.notAvailableOnDst }
    self.tFile3 = { 'TransformationID': self.fakeTransID, 'TaskID': 3, 'LFN': self.available }
    self.tFile4 = { 'TransformationID': self.fakeTransID, 'TaskID': 4, 'LFN': self.notAvailable }

    self.transFiles = [self.tFile1, self.tFile2, self.tFile3, self.tFile4]
    self.sourceSE = ['CERN-SRM']
    self.targetSE = ['DESY-SRM']

  def tearDown(self):
    pass


  def test_init(self):
    self.assertIsInstance( self.fstAgent, FileStatusTransformationAgent )
    self.assertIsInstance( self.fstAgent.tClient, MagicMock )
    self.assertIsInstance( self.fstAgent.fcClient, MagicMock )
    self.assertIsInstance( self.fstAgent.reqClient, MagicMock )
    self.assertTrue( self.fstAgent.enabled )
    self.assertEquals( self.fstAgent.transformationTypes, ['Replication'])
    self.assertEquals( self.fstAgent.transformationStatuses, ['Active'])


  def test_get_transformations_failure(self):
    """ fstAgent should stop execution cycle if getTransformations returns an error """
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


  def test_unknown_file_status(self):
    """ fstAgent should not process file statuses other than Assigned, Problematic, Processed, Unused """
    allowedFileStatuses = ["Assigned", "Problematic", "Processed", "Unused"]
    self.fstAgent.am_getOption = MagicMock()
    self.fstAgent.am_getOption.side_effect = [False, ["Replication"], ["Active"], ["UnknownStatus", "Failed", "Done"]+allowedFileStatuses ]

    self.fstAgent.beginExecution()
    self.assertItemsEqual(self.fstAgent.transformationFileStatuses, allowedFileStatuses)


  def test_get_data_transformation_type(self):
    """ Test if getDataTransformationType function correctly returns the Data Transformation Type (Replication / Moving) """
    self.fstAgent.tClient.getTransformationParameters = MagicMock()
    replicationTrans = "Replication"
    movingTrans = "Moving"

    #empty body of transformation
    self.fstAgent.tClient.getTransformationParameters.return_value = S_OK("")
    res = self.fstAgent.getDataTransformationType(self.fakeTransID)['Value']
    self.assertEquals(res, replicationTrans)

    self.fstAgent.tClient.getTransformationParameters.return_value = S_OK('[["ReplicateAndRegister", {"TargetSE": ["CERN-SRM"], "SourceSE": "CERN-DST-EOS"}], ["RemoveReplica", {"TargetSE": "CERN-DST-EOS"}]]')
    res = self.fstAgent.getDataTransformationType(self.fakeTransID)['Value']
    self.assertEquals(res, movingTrans)

    self.fstAgent.tClient.getTransformationParameters.return_value = S_OK('RemoveReplica:CERN-DST-EOS;ReplicateAndRegister')
    res = self.fstAgent.getDataTransformationType(self.fakeTransID)['Value']
    self.assertEquals(res, movingTrans)

    self.fstAgent.tClient.getTransformationParameters.return_value = S_OK('[["ReplicateAndRegister", {"TargetSE": ["CERN-SRM"], "SourceSE": "CERN-DST-EOS"}]]')
    res = self.fstAgent.getDataTransformationType(self.fakeTransID)['Value']
    self.assertEquals(res, replicationTrans)

    self.fstAgent.tClient.getTransformationParameters.return_value = S_OK('ReplicateAndRegister')
    res = self.fstAgent.getDataTransformationType(self.fakeTransID)['Value']
    self.assertEquals(res, replicationTrans)

  def test_exists(self):
    """ Test if the exists function correctly determines if a file exists in both FileCatalog and StorageElement or not """

    se1 = 'CERN-SRM'
    se2 = 'DESY-SRM'
    SEs = [se1, se2]

    # file exists in FC and all SEs
    fileExists = '/ilc/file/file1'

    # file exists in FC, but one replica is lost on SE
    fileOneRepLost = '/ilc/file/file2'

    # file exists in FC, but all replicas are lost on SEs
    fileAllRepLost = '/ilc/file/file3'

    #file does not exist in FC
    fileRemoved = '/ilc/file/file4'

    files = [fileExists, fileOneRepLost, fileAllRepLost, fileRemoved]

    self.fstAgent.seObjDict[se1] = MagicMock()
    self.fstAgent.seObjDict[se2] = MagicMock()

    #file does not exist in FC
    self.fstAgent.fcClient.getReplicas.return_value = S_OK({'Successful': {fileExists: {se1: fileExists, se2: fileExists},
                                                                           fileOneRepLost: {se1: fileOneRepLost, se2: fileOneRepLost},
                                                                           fileAllRepLost: {se1: fileAllRepLost, se2: fileAllRepLost}},
                                                            'Failed': {fileRemoved: 'No such file or directory'}})

    self.fstAgent.seObjDict[se1].exists.return_value = S_OK({'Successful': {fileExists: True, fileOneRepLost: True, fileAllRepLost: False}, 'Failed': {}})
    self.fstAgent.seObjDict[se2].exists.return_value = S_OK({'Successful': {fileExists: True, fileOneRepLost: False, fileAllRepLost: False}, 'Failed': {}})

    res = self.fstAgent.exists(SEs, files )['Value']

    self.assertFalse(res[fileRemoved])
    self.assertFalse(res[fileOneRepLost])
    self.assertFalse(res[fileAllRepLost])
    self.assertTrue(res[fileExists])


  def test_select_failed_requests(self):
    """ Test if selectFailedRequests function returns True if transfile has a failed request """

    transFileWithFailedReq = {'TransformationID': 400103, 'TaskID': 0, 'LFN': '/ilc/file1'}
    transFileWithDoneReq = {'TransformationID': 400103, 'TaskID': 1, 'LFN': '/ilc/file2'}

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

    self.fstAgent.getRequestStatus = MagicMock(return_value = S_OK({taskIDfile1: {'RequestStatus': 'Problematic', 'RequestID': 1},
                                                                    taskIDfile2: {'RequestStatus': 'Problematic', 'RequestID': 2}}))

    #no request exists for first trans file and one request exists for second trans file
    self.fstAgent.reqClient.getRequest = MagicMock()
    self.fstAgent.reqClient.getRequest.side_effect = [S_ERROR('Request does not exist'), S_OK('Request exists') ]

    res = self.fstAgent.retryStrategyForFiles(self.fakeTransID, transFiles)['Value']

    self.assertEquals(res[taskIDfile1], FST.SET_UNUSED)
    self.assertEquals(res[taskIDfile2], FST.RESET_REQUEST)

  def exists(self, se, lfns):
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
    """ test transformation files are treated properly (set new status / reset request) for replication and moving transformations """

    self.fstAgent.tClient.getTransformationFiles.return_value = S_OK(self.transFiles)
    #all trans files have failed requests
    self.fstAgent.selectFailedRequests = MagicMock(return_value = {tFile['TaskID']: True for tFile in self.transFiles})

    #no assosiated request in rms
    self.fstAgent.retryStrategyForFiles = MagicMock( return_value = S_OK({tFile['TaskID']: FST.SET_UNUSED for tFile in self.transFiles}))

    self.fstAgent.exists = MagicMock( side_effect = self.exists)


    self.fstAgent.transformationFileStatuses = ['Assigned']

    #check replication transformation treatment for assigned files
    self.fstAgent.processTransformation(self.fakeTransID, self.sourceSE, self.targetSE, FST.REPLICATION_TRANS)
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [self.notAvailableOnSrc, self.available], 'Processed')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [self.notAvailableOnDst], 'Unused')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [self.notAvailable], 'Deleted')

    #check moving transformation treatment for assigned files
    self.fstAgent.setFileStatus.reset_mock()
    self.fstAgent.processTransformation(self.fakeTransID, self.sourceSE, self.targetSE, FST.MOVING_TRANS)
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [self.notAvailableOnSrc], 'Processed')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [self.notAvailableOnDst, self.available], 'Unused')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [self.notAvailable], 'Deleted')

    self.fstAgent.transformationFileStatuses = ['Processed']

    #check replication transformation treatment for processed files
    self.fstAgent.setFileStatus.reset_mock()
    self.fstAgent.processTransformation(self.fakeTransID, self.sourceSE, self.targetSE, FST.REPLICATION_TRANS)
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [self.notAvailableOnDst], 'Unused')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [self.notAvailable], 'Deleted')

    #check moving transformation treatment for processed files
    self.fstAgent.setFileStatus.reset_mock()
    self.fstAgent.processTransformation(self.fakeTransID, self.sourceSE, self.targetSE, FST.MOVING_TRANS)
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [self.notAvailableOnDst, self.available], 'Unused')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [self.notAvailable], 'Deleted')

    self.fstAgent.transformationFileStatuses = ['Problematic']

    #check replication transformation treatment for problematic files
    self.fstAgent.setFileStatus.reset_mock()
    self.fstAgent.processTransformation(self.fakeTransID, self.sourceSE, self.targetSE, FST.REPLICATION_TRANS)
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [self.notAvailableOnSrc, self.available], 'Processed')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [self.notAvailableOnDst], 'Unused')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [self.notAvailable], 'Deleted')

    #check moving transformation treatment for problematic files
    self.fstAgent.setFileStatus.reset_mock()
    self.fstAgent.processTransformation(self.fakeTransID, self.sourceSE, self.targetSE, FST.MOVING_TRANS)
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [self.notAvailableOnSrc], 'Processed')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [self.notAvailableOnDst, self.available], 'Unused')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [self.notAvailable], 'Deleted')

    self.fstAgent.transformationFileStatuses = ['Unused']

    #check replication transformation treatment for unused files
    self.fstAgent.setFileStatus.reset_mock()
    self.fstAgent.processTransformation(self.fakeTransID, self.sourceSE, self.targetSE, FST.REPLICATION_TRANS)
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [self.notAvailableOnSrc], 'Processed')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [self.notAvailable], 'Deleted')

    #check moving transformation treatment for unused files
    self.fstAgent.setFileStatus.reset_mock()
    self.fstAgent.processTransformation(self.fakeTransID, self.sourceSE, self.targetSE, FST.MOVING_TRANS)
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [self.notAvailableOnSrc], 'Processed')
    self.fstAgent.setFileStatus.assert_any_call(self.fakeTransID, [self.notAvailable], 'Deleted')

if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( TestFSTAgent )
  TESTRESULT = unittest.TextTestRunner( verbosity = 3 ).run( SUITE )
