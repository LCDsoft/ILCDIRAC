""" Test FileStatusTransformationAgent """

import unittest
import importlib

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
    self.fstAgent.enabled = True

    self.transformations = [{'Status':'Active',
                             'TransformationID': 1L,
                             'Description':'Replicate files for prodID 777 to CERN-DST-EOS',
                             'Type':'Replication',
                             'TransformationName':'replicate_777_CERN-DST-EOS'}]
    self.fakeTransID = 1L


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
    self.fstAgent.getTransformationParameters = MagicMock()
    replicationTrans = "Replication"
    movingTrans = "Moving"

    #empty body of transformation
    self.fstAgent.getTransformationParameters.return_value = S_OK("")
    res = self.fstAgent.getDataTransformationType(self.fakeTransID)['Value']
    self.assertEquals(res, replicationTrans)

    self.fstAgent.getTransformationParameters.return_value = S_OK('[["ReplicateAndRegister", {"TargetSE": ["CERN-SRM"], "SourceSE": "CERN-DST-EOS"}], ["RemoveReplica", {"TargetSE": "CERN-DST-EOS"}]]')
    res = self.fstAgent.getDataTransformationType(self.fakeTransID)['Value']
    self.assertEquals(res, movingTrans)

    self.fstAgent.getTransformationParameters.return_value = S_OK('RemoveReplica:CERN-DST-EOS;ReplicateAndRegister')
    res = self.fstAgent.getDataTransformationType(self.fakeTransID)['Value']
    self.assertEquals(res, movingTrans)

    self.fstAgent.getTransformationParameters.return_value = S_OK('[["ReplicateAndRegister", {"TargetSE": ["CERN-SRM"], "SourceSE": "CERN-DST-EOS"}]]')
    res = self.fstAgent.getDataTransformationType(self.fakeTransID)['Value']
    self.assertEquals(res, replicationTrans)

    self.fstAgent.getTransformationParameters.return_value = S_OK('ReplicateAndRegister')
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

if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( TestFSTAgent )
  TESTRESULT = unittest.TextTestRunner( verbosity = 3 ).run( SUITE )
