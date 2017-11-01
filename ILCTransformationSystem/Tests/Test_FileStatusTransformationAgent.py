
import unittest

from DIRAC import S_OK, S_ERROR

from ILCDIRAC.ILCTransformationSystem.Agent.FileStatusTransformationAgent import FileStatusTransformationAgent

__RCSID__ = "$Id$"

class TestFileStatusTransformationAgent( unittest.TestCase ):

  def setUp(self):
    self.agent = importlib.import_module('ILCDIRAC.ILCTransformationSystem.Agent.FileStatusTransformationAgent')
    self.agent.AgentModule = MagicMock()
    self.FSTAgent = FileStatusTransformationAgent()

    self.FSTAgent.log = gLogger
    self.FSTAgent.tClient = MagicMock( name = "transMock",spec=DIRAC.TransformationSystem.Client.TransformationClient.TransformationClient)
    self.FSTAgent.fcClient = MagicMock( name = "fcMock" , spec=DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient )
    self.FSTAgent.reqClient = MagicMock( name = "reqMock" , spec=DIRAC.RequestManagementSystem.Client.ReqClient )

    self.transformations = [{'Status':'Active',
                             'TransformationID': 1L,
                             'Description':'Replicate files for prodID 777 to CERN-DST-EOS',
                             'Type':'Replication',
                             'TransformationName':'replicate_777_CERN-DST-EOS'}]
    self.fakeTransID = 1L


  def tearDown(self):
    pass

  def test_init(self):
    self.assertIsInstance( self.FSTAgent, FileStatusTransformationAgent )
    self.assertIsInstance( self.FSTAgent.tClient, MagicMock )
    self.assertIsInstance( self.FSTAgent.fcClient, MagicMock )
    self.assertIsInstance( self.FSTAgent.reqClient, MagicMock )
    self.assertFalse( self.FSTAgent.enabled )
    self.assertEquals( self.FSTAgent.transformationTypes, ['Replication'])
    self.assertEquals( self.FSTAgent.transformationStatuses, ['Active'])


  def _createRequestWithStatus(self, status):
    request = MagicMock()
    request.Status=status
    return request

  def test_getRequestsForTasks(self):
    """ test if getRequestsForTasks fucntion returns only Done or Failed requests """
    requestStatuses = ["Waiting", "Failed", "Done", "Scheduled", "Assigned", "Canceled"]
    validStatuses = ['Done','Failed']
    self.FSTAgent.reqClient.peekRequest = MagicMock()
    self.FSTAgent.reqClient.peekRequest.side_effect = [S_OK(self._createRequestWithStatus(status)) for status in requestStatuses]
    fakeTasks = [{'ExternalID':reqID} for reqID in range(len(requestStatuses))]

    res = self.FSTAgent.getRequestsForTasks(fakeTasks)
    reqObjList = res['Value']

    self.assertEquals(2, len(reqObjList))
    for obj in reqObjList:
      self.assertIn(obj.Status, validStatuses)

  def test_file_not_found_in_FC(self):
    """ If file is not found in FC then file status should be changed to Deleted """
    lfns = {'/ilc/prod/file1': 'No such file or directory',
            '/ilc/prod/file2': 'No such file or directory'}

    statusDict = {'/ilc/prod/file1': 'Deleted',
                  '/ilc/prod/file2': 'Deleted'}

    self.FSTAgent.treatFilesNotInFileCatalog(self.fakeTransID, lfns)
    self.FSTAgent.tClient.setFileStatusForTransformation.called_once_with(self.fakeTransID, statusDict)


  def test_all_replicas_lost(self):
    """ If all replicas are lost then File should be removed from File Catalog """

    replicas_before = {'/ilc/prod/file1':  {'CERN-SRM': '/ilc/prod/file1',
                                            'DESY-SRM': '/ilc/prod/file1'},
                       '/ilc/prod/file2':  {'CERN-SRM': '/ilc/prod/file2',
                                            'DESY-SRM': '/ilc/prod/file2'}}

    replicas_after = {'/ilc/prod/file1':  {},
                      '/ilc/prod/file2':  {'CERN-SRM': '/ilc/prod/file2',
                                           'DESY-SRM': '/ilc/prod/file2'}}

    #all replicas lost for /ilc/prod/file1
    self.FSTAgent._getDanglingLFNs = MagicMock(return_value = S_OK(['/ilc/prod/file1']))
    self.FSTAgent.getReplicasForLFNs = MagicMock(return_value = S_OK({'Successful': replicas_after}))

    self.FSTAgent.treatFilesFoundInFileCatalog(self.fakeTransID, replicas_before)

    self.FSTAgent._getDanglingLFNs.assert_any_call('CERN-SRM', ['/ilc/prod/file1', '/ilc/prod/file2'])
    self.FSTAgent._getDanglingLFNs.assert_any_call('DESY-SRM', ['/ilc/prod/file1', '/ilc/prod/file2'])

    self.FSTAgent.fcClient.removeReplica.assert_any_call({ '/ilc/prod/file1': {'SE': 'CERN-SRM'} })
    self.FSTAgent.fcClient.removeReplica.assert_any_call({ '/ilc/prod/file1': {'SE': 'DESY-SRM'} })
    assert not self.FSTAgent.fcClient.removeReplica.assert_any_call({ '/ilc/prod/file2': {'SE': 'CERN-SRM'} })
    assert not self.FSTAgent.fcClient.removeReplica.assert_any_call({ '/ilc/prod/file2': {'SE': 'DESY-SRM'} })

    self.FSTAgent.getReplicasForLFNs.assert_called_once_with(['/ilc/prod/file1', '/ilc/prod/file2'])
    self.FSTAgent.fcClient.removeFile.assert_called_once_with(['/ilc/prod/file1'])

  def test_one_replica_lost(self):
    """ If one of the replicas is lost, replica entry from File Catalog should be removed"""

    replicas_before = {'/ilc/prod/file1':  {'CERN-SRM': '/ilc/prod/file1',
                                            'DESY-SRM': '/ilc/prod/file1'},
                       '/ilc/prod/file2':  {'CERN-SRM': '/ilc/prod/file2',
                                            'DESY-SRM': '/ilc/prod/file2'}}

    replicas_after = {'/ilc/prod/file1':  {'CERN-SRM': '/ilc/prod/file1'},
                      '/ilc/prod/file2':  {'CERN-SRM': '/ilc/prod/file2'}}

    # mock all files lost on DESY-SRM
    self.FSTAgent._getDanglingLFNs = MagicMock(side_effect = lambda se,lfns: S_OK(lfns) if se == 'DESY-SRM' else S_OK([]))
    self.FSTAgent.getReplicasForLFNs = MagicMock(return_value = S_OK({'Successful':replicas_after}))

    self.FSTAgent.treatFilesFoundInFileCatalog(self.fakeTransID, replicas_before)

    self.FSTAgent._getDanglingLFNs.assert_any_call('CERN-SRM', ['/ilc/prod/file1', '/ilc/prod/file2'])
    self.FSTAgent._getDanglingLFNs.assert_any_call('DESY-SRM', ['/ilc/prod/file1', '/ilc/prod/file2'])

    self.FSTAgent.fcClient.removeReplica.assert_any_call({ '/ilc/prod/file1': {'SE': 'DESY-SRM'} })
    self.FSTAgent.fcClient.removeReplica.assert_any_call({ '/ilc/prod/file2': {'SE': 'DESY-SRM'} })
    assert not self.FSTAgent.fcClient.removeReplica.assert_any_call({ '/ilc/prod/file1': {'SE': 'CERN-SRM'} })
    assert not self.FSTAgent.fcClient.removeReplica.assert_any_call({ '/ilc/prod/file2': {'SE': 'CERN-SRM'} })

    self.FSTAgent.getReplicasForLFNs.assert_called_once_with(['/ilc/prod/file1', '/ilc/prod/file2'])
    self.FSTAgent.fcClient.removeFile.assert_not_called()

  def test_no_replica_lost(self):
    """If no replica is lost then nothing should be removed from the File Catalog"""

    replicas = {'/ilc/prod/file1':  {'CERN-SRM': '/ilc/prod/file1',
                                     'DESY-SRM': '/ilc/prod/file1'},
                '/ilc/prod/file2':  {'CERN-SRM': '/ilc/prod/file2',
                                     'DESY-SRM': '/ilc/prod/file2'}}

    # mock no files lost on any SE
    self.FSTAgent._getDanglingLFNs = MagicMock(return_value = S_OK([]))
    self.FSTAgent.getReplicasForLFNs = MagicMock(return_value = S_OK({'Successful': replicas}))

    self.FSTAgent.treatFilesFoundInFileCatalog(self.fakeTransID, replicas)

    self.FSTAgent._getDanglingLFNs.assert_any_call('CERN-SRM', ['/ilc/prod/file1', '/ilc/prod/file2'])
    self.FSTAgent._getDanglingLFNs.assert_any_call('DESY-SRM', ['/ilc/prod/file1', '/ilc/prod/file2'])

    self.FSTAgent.fcClient.removeReplica.assert_not_called()

    self.FSTAgent.getReplicasForLFNs.assert_called_once_with(['/ilc/prod/file1', '/ilc/prod/file2'])
    self.FSTAgent.fcClient.removeFile.assert_not_called()


  def test_get_transformations_failure(self):
    """ FSTAgent should stop execution cycle if getTransformations returns an error """
    self.FSTAgent.getTransformationTasks = MagicMock()
    self.FSTAgent.tClient.getTransformations.return_value = S_ERROR()

    res = self.FSTAgent.execute()
    self.FSTAgent.getTransformationTasks.assert_not_called()
    self.assertFalse(res['OK'])


  def test_no_transformations_found(self):
    """ FSTAgent should stop execution cycle if no transformations are found """
    self.FSTAgent.getTransformationTasks = MagicMock()
    self.FSTAgent.tClient.getTransformations.return_value = S_OK([])

    res = self.FSTAgent.execute()
    self.FSTAgent.getTransformationTasks.assert_not_called()
    self.assertTrue(res['OK'])


  def test_get_trans_tasks_failure(self):
    """ FSTAgent should not fetch request IDs if getTransformationTasks returns an error """
    self.FSTAgent.tClient.getTransformations.return_value = S_OK(self.transformations)
    self.FSTAgent.tClient.getTransformationTasks.return_value = S_ERROR()
    self.FSTAgent.getRequestIDsForTasks = MagicMock()

    self.FSTAgent.execute()
    self.FSTAgent.getRequestIDsForTasks.assert_not_called()


  def test_no_tasks_found(self):
    """ FSTAgent should not fetch request IDs for transformations that have no tasks """
    self.FSTAgent.tClient.getTransformations.return_value = S_OK(self.transformations)
    self.FSTAgent.tClient.getTransformationTasks.return_value = S_OK([])
    self.FSTAgent.getRequestIDsForTasks = MagicMock()

    self.FSTAgent.execute()
    self.FSTAgent.getRequestIDsForTasks.assert_not_called()


  def test_no_done_failed_requests_found(self):
    """ FSTAgent should not fetch replicas if there are no Done or Failed requests  """
    taskIDs = [{'TaskID':1},{'TaskID':2}]
    self.FSTAgent.tClient.getTransformations.return_value = S_OK(self.transformations)
    self.FSTAgent.tClient.getTransformationTasks.return_value = S_OK(taskIDs)
    self.FSTAgent.getRequestsForTasks = MagicMock(return_value = S_OK([]))
    self.FSTAgent.getReplicasForLFNs = MagicMock()

    self.FSTAgent.execute()
    self.FSTAgent.getReplicasForLFNs.assert_not_called()


if __name__ == "__main__":
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestFileStatusTransformationAgent )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )
