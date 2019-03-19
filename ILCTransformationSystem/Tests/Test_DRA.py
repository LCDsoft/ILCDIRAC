"""Test the DataRecoveryAgent"""

import unittest
from collections import defaultdict

from mock import MagicMock as Mock, patch, ANY

import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger

from ILCDIRAC.ILCTransformationSystem.Agent.DataRecoveryAgent import DataRecoveryAgent
from ILCDIRAC.ILCTransformationSystem.Utilities.JobInfo import TaskInfoException

from ILCDIRAC.Tests.Utilities.GeneralUtils import MatchStringWith

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.ILCTransformationSystem.Agent.DataRecoveryAgent'

class TestDRA( unittest.TestCase ):
  """Test the DataRecoveryAgent"""
  dra = None

  @patch("DIRAC.Core.Base.AgentModule.PathFinder", new=Mock())
  @patch("DIRAC.ConfigurationSystem.Client.PathFinder.getSystemInstance", new=Mock() )
  @patch("%s.ReqClient" % MODULE_NAME, new=Mock() )
  def setUp ( self ):
    self.dra = DataRecoveryAgent( agentName="ILCTransformationSystem/DataRecoveryAgent", loadName="TestDRA" )
    self.dra.reqClient=Mock( name="reqMock", spec=DIRAC.RequestManagementSystem.Client.ReqClient.ReqClient )
    self.dra.tClient=Mock( name="transMock", spec=DIRAC.TransformationSystem.Client.TransformationClient.TransformationClient )
    self.dra.fcClient=Mock( name="fcMock", spec=DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient )
    self.dra.jobMon=Mock( name="jobMonMock", spec=DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient.JobMonitoringClient)
    self.dra.printEveryNJobs = 10
    self.dra.log = Mock( name="LogMock" )

  def tearDown ( self ):
    pass

  def getTestMock(self, nameID=0, jobID=1234567):
    """create a JobInfo object with mocks"""
    from ILCDIRAC.ILCTransformationSystem.Utilities.JobInfo import JobInfo
    testJob = Mock ( name = "jobInfoMock_%s" % nameID, spec=JobInfo )
    testJob.jobID = jobID
    testJob.tType = "testType"
    testJob.otherTasks = None
    testJob.inputFileExists = True
    testJob.status = "Done"
    testJob.fileStatus = "Assigned"
    testJob.outputFiles = ["/my/stupid/file.lfn", "/my/stupid/file2.lfn"]
    testJob.outputFileStatus = ["Exists", "Exists"]
    testJob.inputFile = "inputfile.lfn"
    testJob.pendingRequest = False
    testJob.getTaskInfo = Mock()
    return testJob

  @patch("DIRAC.Core.Base.AgentModule.PathFinder", new=Mock())
  @patch("DIRAC.ConfigurationSystem.Client.PathFinder.getSystemInstance", new=Mock() )
  @patch("%s.ReqClient" % MODULE_NAME, new=Mock() )
  def test_init( self ):
    """test for DataRecoveryAgent initialisation...................................................."""
    res = DataRecoveryAgent( agentName="ILCTransformationSystem/DataRecoveryAgent", loadName="TestDRA" )
    self.assertIsInstance( res , DataRecoveryAgent )

  def test_beginExecution( self ):
    """test for DataRecoveryAgent beginExecution...................................................."""
    res = self.dra.beginExecution()
    self.assertIn( "MCReconstruction", self.dra.transformationTypes )
    self.assertFalse( self.dra.enabled )
    self.assertTrue( res['OK'] )

  def test_getEligibleTransformations_success( self ):
    """test for DataRecoveryAgent getEligibleTransformations success................................"""
    transInfoDict = dict(TransformationID=1234, TransformationName="TestProd12", Type="TestProd",
                         AuthorDN='/some/cert/owner', AuthorGroup='Test_Prod')
    self.dra.tClient.getTransformations = Mock(return_value=S_OK([transInfoDict]))
    res = self.dra.getEligibleTransformations( status="Active", typeList=['TestProds'] )
    self.assertTrue( res['OK'] )
    self.assertIsInstance( res['Value'], dict )
    vals = res['Value']
    self.assertIn( "1234", vals )
    self.assertIsInstance(vals['1234'], dict)
    self.assertEqual(transInfoDict, vals["1234"])

  def test_getEligibleTransformations_failed( self ):
    """test for DataRecoveryAgent getEligibleTransformations failure................................"""
    self.dra.tClient.getTransformations = Mock( return_value=S_ERROR( "No can Do" ) )
    res = self.dra.getEligibleTransformations( status="Active", typeList=['TestProds'] )
    self.assertFalse( res['OK'] )
    self.assertEqual( "No can Do", res['Message'] )

  def test_treatProduction1( self ):
    """test for DataRecoveryAgent treatProduction success1.........................................."""
    getJobMock = Mock( name = "getJobMOck" )
    getJobMock.getJobs.return_value = ( Mock( name = "jobsMOck" ), 50, 50 )
    tinfoMock = Mock( name = "infoMock", return_value = getJobMock )
    self.dra.checkAllJobs = Mock()
    #catch the printout to check path taken
    transInfoDict = dict(TransformationID=1234, TransformationName="TestProd12", Type="TestProd",
                         AuthorDN='/some/cert/owner', AuthorGroup='Test_Prod')
    with patch("%s.TransformationInfo" % MODULE_NAME, new=tinfoMock ):
      self.dra.treatProduction(1234, transInfoDict)  # returns None
    ## check we start with the summary right away
    for _name, args, _kwargs in self.dra.log.notice.mock_calls:
      self.assertNotIn( 'Getting Tasks:', str(args) )

  def test_treatProduction2( self ):
    """test for DataRecoveryAgent treatProduction success2.........................................."""
    getJobMock = Mock( name = "getJobMOck" )
    getJobMock.getJobs.return_value = ( Mock( name = "jobsMock" ), 50, 50 )
    tinfoMock = Mock( name = "infoMock", return_value = getJobMock )
    self.dra.checkAllJobs = Mock()
    #catch the printout to check path taken
    transInfoDict = dict(TransformationID=1234, TransformationName="TestProd12", Type="MCSimulation",
                         AuthorDN='/some/cert/owner', AuthorGroup='Test_Prod')
    with patch("%s.TransformationInfo" % MODULE_NAME, new=tinfoMock ):
      self.dra.treatProduction(1234, transInfoDict)  # returns None
    self.dra.log.notice.assert_any_call(MatchStringWith("Getting tasks..."))

  def test_treatProduction3( self ):
    """test for DataRecoveryAgent treatProduction skip.............................................."""
    getJobMock = Mock( name = "getJobMOck" )
    getJobMock.getJobs.return_value = ( Mock( name = "jobsMock" ), 50, 50 )
    self.dra.checkAllJobs = Mock()
    self.dra.jobCache[1234] = (50, 50)
    #catch the printout to check path taken
    transInfoDict = dict(TransformationID=1234, TransformationName="TestProd12", Type="TestProd",
                         AuthorDN='/some/cert/owner', AuthorGroup='Test_Prod')

    with patch("%s.TransformationInfo" % MODULE_NAME,
               autospec=True,
               return_value=getJobMock ):
      self.dra.treatProduction(prodID=1234, transInfoDict=transInfoDict)  # returns None
      #self.assertIn( "Skipping production 1234", out.getvalue().strip().splitlines()[0] )
    self.dra.log.notice.assert_called_with( MatchStringWith("Skipping production 1234") )


  def test_checkJob( self ):
    """test for DataRecoveryAgent checkJob MCGeneration............................................."""

    from ILCDIRAC.ILCTransformationSystem.Utilities.TransformationInfo import TransformationInfo
    tInfoMock = Mock( name = "tInfoMock", spec=TransformationInfo )
    
    from ILCDIRAC.ILCTransformationSystem.Utilities.JobInfo import JobInfo

    ### Test First option for MCGeneration
    tInfoMock.reset_mock()
    testJob = JobInfo( jobID=1234567, status = "Failed", tID=123, tType="MCGeneration" )
    testJob.outputFiles = ["/my/stupid/file.lfn"]
    testJob.outputFileStatus = ["Exists"]

    self.dra.checkJob( testJob, tInfoMock )
    self.assertIn( "setJobDone", tInfoMock.method_calls[0] )
    self.assertEqual( self.dra.todo["MCGeneration"][0]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["MCGeneration"][1]["Counter"] , 0 )


    ### Test Second option for MCGeneration
    tInfoMock.reset_mock()
    testJob.status = "Done"
    testJob.outputFileStatus = ["Missing"]
    self.dra.checkJob( testJob, tInfoMock )
    self.assertIn( "setJobFailed", tInfoMock.method_calls[0] )
    self.assertEqual( self.dra.todo["MCGeneration"][0]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["MCGeneration"][1]["Counter"] , 1 )

    ### Test Second option for MCGeneration
    tInfoMock.reset_mock()
    testJob.status = "Done"
    testJob.outputFileStatus = ["Exists"]
    self.dra.checkJob( testJob, tInfoMock )
    self.assertEqual( tInfoMock.method_calls, [] )
    self.assertEqual( self.dra.todo["MCGeneration"][0]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["MCGeneration"][1]["Counter"] , 1 )


  def test_checkJob_others( self ):
    """test for DataRecoveryAgent checkJob other ProductionTypes ..................................."""

    from ILCDIRAC.ILCTransformationSystem.Utilities.TransformationInfo import TransformationInfo
    tInfoMock = Mock( name = "tInfoMock", spec=TransformationInfo )
    
    from ILCDIRAC.ILCTransformationSystem.Utilities.JobInfo import JobInfo

    ### Test First option for OtherProductions
    tInfoMock.reset_mock()
    testJob = JobInfo( jobID=1234567, status = "Failed", tID=123, tType="MCSimulation" )
    testJob.outputFiles = ["/my/stupid/file.lfn"]
    testJob.inputFile = "/my/input/file.lfn"
    testJob.outputFileStatus = ["Exists"]
    testJob.otherTasks = True
    self.dra.inputFilesProcessed = set()
    self.dra.checkJob( testJob, tInfoMock )
    self.assertIn( testJob.inputFile , self.dra.inputFilesProcessed )
    self.assertIn( "setJobDone", tInfoMock.method_calls[0] )
    self.assertIn( "setInputProcessed", tInfoMock.method_calls[1] )
    self.assertEqual( self.dra.todo["OtherProductions"][0]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][1]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][2]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][3]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][4]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][5]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][6]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][7]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][8]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][9]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][10]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][11]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][12]["Counter"] , 0 )

    ### Test Second option for OtherProductions
    tInfoMock.reset_mock()
    testJob = JobInfo( jobID=1234567, status = "Done", tID=123, tType="MCSimulation" )
    testJob.outputFiles = ["/my/stupid/file.lfn"]
    testJob.outputFileStatus = ["Missing"]
    testJob.otherTasks = True
    testJob.inputFile = "/my/inputfile.lfn"
    self.dra.inputFilesProcessed = set( [testJob.inputFile] )
    self.dra.checkJob( testJob, tInfoMock )
    self.assertIn( testJob.inputFile , self.dra.inputFilesProcessed )
    self.assertIn( "setJobFailed", tInfoMock.method_calls[0] )
    self.assertEqual( self.dra.todo["OtherProductions"][0]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][1]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][2]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][3]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][4]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][5]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][6]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][7]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][8]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][9]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][10]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][11]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][12]["Counter"] , 0 )

    ### Test Third option for OtherProductions
    tInfoMock.reset_mock()
    testJob = JobInfo( jobID=1234567, status = "Done", tID=123, tType="MCSimulation" )
    testJob.outputFiles = ["/my/stupid/file.lfn"]
    testJob.outputFileStatus = ["Exists"]
    testJob.otherTasks = True
    testJob.inputFile = "/my/inputfile.lfn"
    self.dra.inputFilesProcessed = set( [testJob.inputFile] )
    self.dra.checkJob( testJob, tInfoMock )
    self.assertIn( testJob.inputFile , self.dra.inputFilesProcessed )
    self.assertIn( "setJobFailed", tInfoMock.method_calls[0] )
    self.assertIn( "cleanOutputs", tInfoMock.method_calls[1] )
    self.assertEqual( self.dra.todo["OtherProductions"][0]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][1]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][2]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][3]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][4]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][5]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][6]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][7]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][8]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][9]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][10]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][11]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][12]["Counter"] , 0 )

    ### Test Fourth option for OtherProductions
    tInfoMock.reset_mock()
    testJob = JobInfo( jobID=1234567, status = "Done", tID=123, tType="MCSimulation" )
    testJob.outputFiles = ["/my/stupid/file.lfn"]
    testJob.outputFileStatus = ["Exists"]
    testJob.otherTasks = False
    testJob.inputFile = "/my/inputfile.lfn"
    testJob.inputFileExists = False
    testJob.fileStatus = "Exists"
    self.dra.inputFilesProcessed = set( )
    self.dra.checkJob( testJob, tInfoMock )
    self.assertIn( "cleanOutputs",    tInfoMock.method_calls[0] )
    self.assertIn( "setJobFailed",    tInfoMock.method_calls[1] )
    self.assertIn( "setInputDeleted", tInfoMock.method_calls[2] )
    self.assertEqual( self.dra.todo["OtherProductions"][0]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][1]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][2]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][3]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][4]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][5]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][6]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][7]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][8]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][9]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][10]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][11]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][12]["Counter"] , 0 )

    ### Test Fifth option for OtherProductions
    tInfoMock.reset_mock()
    testJob = JobInfo( jobID=1234567, status = "Done", tID=123, tType="MCSimulation" )
    testJob.outputFiles = ["/my/stupid/file.lfn"]
    testJob.outputFileStatus = ["Exists"]
    testJob.otherTasks = False
    testJob.inputFile = "/my/inputfile.lfn"
    testJob.inputFileExists = False
    testJob.fileStatus = "Deleted"
    self.dra.inputFilesProcessed = set( )
    self.dra.checkJob( testJob, tInfoMock )
    self.assertIn( "cleanOutputs",    tInfoMock.method_calls[0] )
    self.assertIn( "setJobFailed",    tInfoMock.method_calls[1] )
    self.assertEqual( self.dra.todo["OtherProductions"][0]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][1]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][2]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][3]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][4]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][5]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][6]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][7]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][8]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][9]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][10]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][11]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][12]["Counter"] , 0 )

    ### Test sixth option for OtherProductions
    tInfoMock.reset_mock()
    testJob = JobInfo( jobID=1234567, status = "Failed", tID=123, tType="MCSimulation" )
    testJob.outputFiles = ["/my/stupid/file.lfn"]
    testJob.outputFileStatus = ["Exists"]
    testJob.otherTasks = False
    testJob.inputFile = "/my/inputfile.lfn"
    testJob.inputFileExists = True
    testJob.fileStatus = "Assigned"
    self.dra.inputFilesProcessed = set()
    self.dra.checkJob( testJob, tInfoMock )
    self.assertIn( "setJobDone",        tInfoMock.method_calls[0] )
    self.assertIn( "setInputProcessed", tInfoMock.method_calls[1] )
    self.assertEqual( self.dra.todo["OtherProductions"][0]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][1]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][2]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][3]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][4]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][5]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][6]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][7]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][8]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][9]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][10]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][11]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][12]["Counter"] , 0 )

    ### Test seventh option for OtherProductions
    tInfoMock.reset_mock()
    testJob = JobInfo( jobID=1234567, status = "Failed", tID=123, tType="MCSimulation" )
    testJob.outputFiles = ["/my/stupid/file.lfn"]
    testJob.outputFileStatus = ["Exists"]
    testJob.otherTasks = False
    testJob.inputFile = "/my/inputfile.lfn"
    testJob.inputFileExists = True
    testJob.fileStatus = "Processed"
    self.dra.inputFilesProcessed = set()
    self.dra.checkJob( testJob, tInfoMock )
    self.assertIn( "setJobDone",        tInfoMock.method_calls[0] )
    self.assertEqual( self.dra.todo["OtherProductions"][0]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][1]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][2]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][3]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][4]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][5]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][6]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][7]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][8]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][9]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][10]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][11]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][12]["Counter"] , 0 )

    ### Test eighth option for OtherProductions
    tInfoMock.reset_mock()
    testJob = JobInfo( jobID=1234567, status = "Done", tID=123, tType="MCSimulation" )
    testJob.outputFiles = ["/my/stupid/file.lfn"]
    testJob.outputFileStatus = ["Exists"]
    testJob.otherTasks = False
    testJob.inputFile = "/my/inputfile.lfn"
    testJob.inputFileExists = True
    testJob.fileStatus = "Assigned"
    self.dra.inputFilesProcessed = set()
    self.dra.checkJob( testJob, tInfoMock )
    self.assertIn( "setInputProcessed", tInfoMock.method_calls[0] )
    self.assertEqual( self.dra.todo["OtherProductions"][0]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][1]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][2]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][3]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][4]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][5]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][6]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][7]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][8]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][9]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][10]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][11]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][12]["Counter"] , 0 )

    ### Test ninth option for OtherProductions
    tInfoMock.reset_mock()
    testJob = JobInfo( jobID=1234567, status = "Failed", tID=123, tType="MCSimulation" )
    testJob.outputFiles = ["/my/stupid/file.lfn"]
    testJob.outputFileStatus = ["Missing"]
    testJob.otherTasks = False
    testJob.inputFile = "/my/inputfile.lfn"
    testJob.inputFileExists = True
    testJob.fileStatus = "Assigned"
    testJob.errorCount = 14
    self.dra.inputFilesProcessed = set()
    self.dra.checkJob( testJob, tInfoMock )
    self.assertIn( "setInputMaxReset", tInfoMock.method_calls[0] )
    self.assertEqual( self.dra.todo["OtherProductions"][0]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][1]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][2]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][3]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][4]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][5]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][6]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][7]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][8]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][9]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][10]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][11]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][12]["Counter"] , 0 )

    ### Test MaxReset option for OtherProductions
    tInfoMock.reset_mock()
    testJob = JobInfo( jobID=1234567, status = "Failed", tID=123, tType="MCSimulation" )
    testJob.outputFiles = ["/my/stupid/file.lfn"]
    testJob.outputFileStatus = ["Missing"]
    testJob.otherTasks = False
    testJob.inputFile = "/my/inputfile.lfn"
    testJob.inputFileExists = True
    testJob.fileStatus = "Assigned"
    self.dra.inputFilesProcessed = set()
    self.dra.checkJob( testJob, tInfoMock )
    self.assertIn( "setInputUnused", tInfoMock.method_calls[0] )
    self.assertEqual( self.dra.todo["OtherProductions"][0]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][1]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][2]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][3]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][4]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][5]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][6]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][7]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][8]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][9]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][10]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][11]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][12]["Counter"] , 0 )

    ### Test tenth option for OtherProductions
    tInfoMock.reset_mock()
    testJob = JobInfo( jobID=1234567, status = "Done", tID=123, tType="MCSimulation" )
    testJob.outputFiles = ["/my/stupid/file.lfn"]
    testJob.outputFileStatus = ["Missing"]
    testJob.otherTasks = False
    testJob.inputFile = "/my/inputfile.lfn"
    testJob.inputFileExists = True
    testJob.fileStatus = "Assigned"
    self.dra.inputFilesProcessed = set()
    self.dra.checkJob( testJob, tInfoMock )
    self.assertIn( "setInputUnused", tInfoMock.method_calls[0] )
    self.assertIn( "setJobFailed",   tInfoMock.method_calls[1] )
    self.assertEqual( self.dra.todo["OtherProductions"][0]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][1]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][2]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][3]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][4]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][5]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][6]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][7]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][8]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][9]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][10]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][11]["Counter"] , 0 )
    self.assertEqual( self.dra.todo["OtherProductions"][12]["Counter"] , 0 )

    ### Test eleventh option for OtherProductions
    tInfoMock.reset_mock()
    testJob = JobInfo( jobID=1234567, status = "Failed", tID=123, tType="MCSimulation" )
    testJob.outputFiles = ["/my/stupid/file.lfn", "/my/stupid/file2.lfn"]
    testJob.outputFileStatus = ["Missing", "Exists"]
    testJob.otherTasks = False
    testJob.inputFile = "/my/inputfile.lfn"
    testJob.inputFileExists = True
    testJob.fileStatus = "Assigned"
    self.dra.inputFilesProcessed = set()
    self.dra.checkJob( testJob, tInfoMock )
    self.assertIn( "cleanOutputs",   tInfoMock.method_calls[0] )
    self.assertIn( "setInputUnused", tInfoMock.method_calls[1] )
    self.assertEqual( self.dra.todo["OtherProductions"][0]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][1]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][2]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][3]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][4]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][5]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][6]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][7]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][8]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][9]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][10]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][11]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][12]["Counter"] , 0 )

    ### Test twelfth option for OtherProductions
    tInfoMock.reset_mock()
    testJob = JobInfo( jobID=1234567, status = "Done", tID=123, tType="MCSimulation" )
    testJob.outputFiles = ["/my/stupid/file.lfn", "/my/stupid/file2.lfn"]
    testJob.outputFileStatus = ["Missing", "Exists"]
    testJob.otherTasks = False
    testJob.inputFile = "/my/inputfile.lfn"
    testJob.inputFileExists = True
    testJob.fileStatus = "Assigned"
    self.dra.inputFilesProcessed = set()
    self.dra.checkJob( testJob, tInfoMock )
    self.assertIn( "cleanOutputs",   tInfoMock.method_calls[0] )
    self.assertIn( "setInputUnused", tInfoMock.method_calls[1] )
    self.assertIn( "setJobFailed", tInfoMock.method_calls[2] )
    self.assertEqual( self.dra.todo["OtherProductions"][0]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][1]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][2]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][3]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][4]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][5]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][6]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][7]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][8]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][9]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][10]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][11]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][12]["Counter"] , 1 )

    ### Test thirteenth option for OtherProductions
    tInfoMock.reset_mock()
    testJob = JobInfo( jobID=1234567, status = "Done", tID=123, tType="MCSimulation" )
    testJob.outputFiles = ["/my/stupid/file.lfn", "/my/stupid/file2.lfn"]
    testJob.outputFileStatus = ["Missing", "Exists"]
    testJob.otherTasks = False
    testJob.inputFile = "/my/inputfile.lfn"
    testJob.inputFileExists = True
    testJob.fileStatus = "Unused"
    self.dra.inputFilesProcessed = set()
    self.dra.checkJob( testJob, tInfoMock )
    self.assertIn( "setJobFailed", tInfoMock.method_calls[0] )
    self.assertEqual( self.dra.todo["OtherProductions"][0]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][1]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][2]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][3]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][4]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][5]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][6]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][7]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][8]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][9]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][10]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][11]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][12]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][13]["Counter"] , 1 )

    ### Test fourteenth option for OtherProductions
    tInfoMock.reset_mock()
    testJob = JobInfo( jobID=1234567, status = "Strange", tID=123, tType="MCSimulation" )
    testJob.outputFiles = ["/my/stupid/file.lfn", "/my/stupid/file2.lfn"]
    testJob.outputFileStatus = ["Missing", "Exists"]
    testJob.otherTasks = False
    testJob.inputFile = "/my/inputfile.lfn"
    testJob.inputFileExists = True
    testJob.fileStatus = "Processed"
    self.dra.inputFilesProcessed = set()
    self.dra.checkJob( testJob, tInfoMock )
    self.assertEqual( [], tInfoMock.method_calls )
    self.assertEqual( self.dra.todo["OtherProductions"][0]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][1]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][2]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][3]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][4]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][5]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][6]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][7]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][8]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][9]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][10]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][11]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][12]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][13]["Counter"] , 1 )

    ### Test nothing triggers
    tInfoMock.reset_mock()
    testJob = JobInfo( jobID=1234567, status = "Failed", tID=123, tType="MCSimulation" )
    testJob.outputFiles = ["/my/stupid/file.lfn", "/my/stupid/file2.lfn"]
    testJob.outputFileStatus = ["Missing", "Missing"]
    testJob.otherTasks = True
    testJob.inputFile = "/my/inputfile.lfn"
    testJob.inputFileExists = True
    testJob.fileStatus = "Processed"
    self.dra.inputFilesProcessed = set()
    self.dra.checkJob( testJob, tInfoMock )
    self.assertEqual( [], tInfoMock.method_calls )
    self.assertEqual( self.dra.todo["OtherProductions"][0]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][1]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][2]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][3]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][4]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][5]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][6]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][7]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][8]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][9]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][10]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][11]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][12]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][13]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][14]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][-1]["Counter"] , 0 )

    ### Test failHard
    tInfoMock.reset_mock()
    testJob = JobInfo( jobID=666, status = "Done", tID=123, tType="MCSimulation" )
    testJob.outputFiles = ["/my/stupid/file.lfn"]
    testJob.outputFileStatus = ["Exists"]
    testJob.otherTasks = True
    testJob.inputFile = None
    testJob.inputFileExists = True
    testJob.fileStatus = "Processed"
    self.dra.inputFilesProcessed = set()
    self.dra._DataRecoveryAgent__failJobHard( testJob, tInfoMock ) #pylint: disable=protected-access, no-member
    self.assertIn( "cleanOutputs", tInfoMock.method_calls[0] )
    self.assertIn( "setJobFailed", tInfoMock.method_calls[1] )
    self.assertEqual( self.dra.todo["OtherProductions"][0]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][1]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][2]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][3]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][4]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][5]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][6]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][7]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][8]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][9]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][10]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][11]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][12]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][13]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][14]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][-1]["Counter"] , 1 )

    ### Test failHard, do nothing because already cleaned
    tInfoMock.reset_mock()
    testJob = JobInfo( jobID=667, status = "Failed", tID=123, tType="MCSimulation" )
    testJob.outputFiles = ["/my/stupid/file.lfn"]
    testJob.outputFileStatus = ["Missing"]
    testJob.otherTasks = True
    testJob.inputFile = None
    testJob.inputFileExists = False
    testJob.fileStatus = "Processed"
    self.dra.inputFilesProcessed = set()
    self.dra._DataRecoveryAgent__failJobHard( testJob, tInfoMock ) #pylint: disable=protected-access, no-member
    self.assertEqual( [], tInfoMock.method_calls )
    self.assertEqual( self.dra.todo["OtherProductions"][0]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][1]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][2]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][3]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][4]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][5]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][6]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][7]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][8]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][9]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][10]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][11]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][12]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][13]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][14]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][-1]["Counter"] , 1 )

    self.assertNotIn( "Failing job 667", self.dra.notesToSend )

    ### Test failHard3, do nothing because inputFile not None
    tInfoMock.reset_mock()
    testJob = JobInfo( jobID=668, status = "Failed", tID=123, tType="MCSimulation" )
    testJob.outputFiles = ["/my/stupid/file.lfn"]
    testJob.outputFileStatus = ["Missing"]
    testJob.otherTasks = True
    testJob.inputFile = "NotNone"
    testJob.inputFileExists = False
    testJob.fileStatus = "Processed"
    self.dra.inputFilesProcessed = set()
    self.dra._DataRecoveryAgent__failJobHard( testJob, tInfoMock ) # pylint: disable=protected-access, no-member
    self.assertEqual( [], tInfoMock.method_calls )
    self.assertEqual( self.dra.todo["OtherProductions"][0]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][1]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][2]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][3]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][4]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][5]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][6]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][7]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][8]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][9]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][10]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][11]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][12]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][13]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][14]["Counter"] , 1 )
    self.assertEqual( self.dra.todo["OtherProductions"][-1]["Counter"] , 1 )

    self.assertNotIn( "Failing job 668", self.dra.notesToSend )


  def test_notOnlyKeepers( self ):
    """ test for __notOnlyKeepers function """

    funcToTest = self.dra._DataRecoveryAgent__notOnlyKeepers #pylint: disable=protected-access, no-member
    self.assertTrue( funcToTest( "MCGeneration_ILD" ) )

    self.dra.todo['OtherProductions'][0]["Counter"]=3 ## keepers
    self.dra.todo['OtherProductions'][3]["Counter"]=0
    self.assertFalse( funcToTest( "MCSimulation" ) )

    self.dra.todo['OtherProductions'][0]["Counter"]=3 ## keepers
    self.dra.todo['OtherProductions'][3]["Counter"]=3
    self.assertTrue( funcToTest( "MCSimulation" ) )

  def test_checkAllJob( self ):
    """test for DataRecoveryAgent checkAllJobs ....................................................."""
    from ILCDIRAC.ILCTransformationSystem.Utilities.JobInfo import JobInfo

    ### test with additional task dicts
    from ILCDIRAC.ILCTransformationSystem.Utilities.TransformationInfo import TransformationInfo
    tInfoMock = Mock( name = "tInfoMock", spec=TransformationInfo )
    mockJobs = dict([ (i, self.getTestMock() ) for i in xrange(11) ] )
    mockJobs[2].pendingRequest = True
    mockJobs[3].getJobInformation = Mock(side_effect=(RuntimeError('ARGJob1'), None))
    mockJobs[4].getTaskInfo = Mock(side_effect=(TaskInfoException('ARG1'), None))
    taskDict = True
    lfnTaskDict = True
    self.dra.checkAllJobs( mockJobs, tInfoMock, taskDict, lfnTaskDict )
    self.dra.log.error.assert_any_call( MatchStringWith('+++++ Exception'), 'ARGJob1')
    self.dra.log.error.assert_any_call( MatchStringWith("Skip Task, due to TaskInfoException: ARG1") )
    self.dra.log.reset_mock()

    ### test inputFile None
    mockJobs = dict([ (i, self.getTestMock(nameID=i) ) for i in xrange(5) ] )
    mockJobs[1].inputFile = None
    mockJobs[1].getTaskInfo = Mock( side_effect = ( TaskInfoException("NoInputFile"), None ) )
    mockJobs[1].tType = "MCSimulation"
    tInfoMock.reset_mock()
    self.dra.checkAllJobs( mockJobs, tInfoMock, taskDict, lfnTaskDict = True )
    self.dra.log.notice.assert_any_call( MatchStringWith( "Failing job hard" ) )

  def test_checkAllJob_2(self):
    """Test where failJobHard fails (via cleanOutputs)."""
    from ILCDIRAC.ILCTransformationSystem.Utilities.TransformationInfo import TransformationInfo
    tInfoMock = Mock(name='tInfoMock', spec=TransformationInfo)
    mockJobs = dict([(i, self.getTestMock()) for i in xrange(5)])
    mockJobs[2].pendingRequest = True
    mockJobs[3].getTaskInfo = Mock(side_effect=(TaskInfoException('ARGJob3'), None))
    mockJobs[3].inputFile = None
    self.dra._DataRecoveryAgent__failJobHard = Mock(side_effect=(RuntimeError('ARGJob4'), None), name='FJH')
    self.dra.checkAllJobs(mockJobs, tInfoMock, tasksDict=True, lfnTaskDict=True)
    mockJobs[3].getTaskInfo.assert_called()
    self.dra._DataRecoveryAgent__failJobHard.assert_called()
    self.dra.log.error.assert_any_call(MatchStringWith('+++++ Exception'), 'ARGJob4')
    self.dra.log.reset_mock()

  def test_execute( self ):
    """test for DataRecoveryAgent execute .........................................................."""
    self.dra.treatProduction = Mock()

    self.dra.productionsToIgnore = [ 123, 456, 789 ]
    self.dra.jobCache = defaultdict( lambda: (0, 0) )
    self.dra.jobCache[ 123 ] = ( 10, 10 )
    self.dra.jobCache[ 124 ] = ( 10, 10 )
    self.dra.jobCache[ 125 ] = ( 10, 10 )

    ## Eligible fails
    self.dra.log.reset_mock()
    self.dra.getEligibleTransformations = Mock( return_value = S_ERROR( "outcast" ) )
    res = self.dra.execute()
    self.assertFalse( res["OK"] )
    self.dra.log.error.assert_any_call( ANY, MatchStringWith("outcast") )
    self.assertEqual( "Failure to get transformations", res['Message'] )

    d123 = dict(TransformationID=123, TransformationName="TestProd123", Type="MCGeneration",
                AuthorDN='/some/cert/owner', AuthorGroup='Test_Prod')
    d124 = dict(TransformationID=125, TransformationName="TestProd124", Type="MCGeneration",
                AuthorDN='/some/cert/owner', AuthorGroup='Test_Prod')
    d125 = dict(TransformationID=124, TransformationName="TestProd125", Type="MCGeneration",
                AuthorDN='/some/cert/owner', AuthorGroup='Test_Prod')

    ## Eligible succeeds
    self.dra.log.reset_mock()
    self.dra.getEligibleTransformations = Mock(return_value=S_OK({123: d123, 124: d124, 125: d125}))
    res = self.dra.execute()
    self.assertTrue( res["OK"] )
    self.dra.log.notice.assert_any_call( MatchStringWith("Will ignore the following productions: [123, 456, 789]") )
    self.dra.log.notice.assert_any_call( MatchStringWith("Ignoring Production: 123") )
    self.dra.log.notice.assert_any_call( MatchStringWith("Running over Production: 124" ) )


    ## Notes To Send
    self.dra.log.reset_mock()
    self.dra.getEligibleTransformations = Mock(return_value=S_OK({123: d123, 124: d124, 125: d125}))
    self.dra.notesToSend = "Da hast du deine Karte"
    sendmailMock = Mock()
    sendmailMock.sendMail.return_value = S_OK("Nice Card")
    notificationMock = Mock( return_value = sendmailMock )
    with patch("%s.NotificationClient" % MODULE_NAME, new=notificationMock ):
      res = self.dra.execute()
    self.assertTrue( res["OK"] )
    self.dra.log.notice.assert_any_call( MatchStringWith("Will ignore the following productions: [123, 456, 789]"))
    self.dra.log.notice.assert_any_call( MatchStringWith("Ignoring Production: 123" ))
    self.dra.log.notice.assert_any_call( MatchStringWith("Running over Production: 124" ))
    self.assertNotIn( 124, self.dra.jobCache ) ## was popped
    self.assertIn( 125, self.dra.jobCache )## was not popped
    gLogger.notice( "JobCache: %s" % self.dra.jobCache )

    ## sending notes fails
    self.dra.log.reset_mock()
    self.dra.notesToSend = "Da hast du deine Karte"
    sendmailMock = Mock()
    sendmailMock.sendMail.return_value = S_ERROR("No stamp")
    notificationMock = Mock( return_value = sendmailMock )
    with patch("%s.NotificationClient" % MODULE_NAME, new=notificationMock ):
      res = self.dra.execute()
    self.assertTrue( res["OK"] )
    self.assertNotIn( 124, self.dra.jobCache ) ## was popped
    self.assertIn( 125, self.dra.jobCache )## was not popped
    self.dra.log.error.assert_any_call( MatchStringWith("Cannot send notification mail"), ANY )

    self.assertEqual( "", self.dra.notesToSend )


  def test_printSummary( self ):
    """test DataRecoveryAgent printSummary.........................................................."""
    self.dra.notesToSend = ""
    self.dra.printSummary()
    self.assertNotIn( " Other Tasks --> Keep                                    :     0", self.dra.notesToSend )


    self.dra.notesToSend = "Note This"
    self.dra.printSummary()

  def test_setPendingRequests_1(self):
    """Check the setPendingRequests function."""
    mockJobs = dict((i, self.getTestMock(jobID=i)) for i in xrange(11))
    reqMock = Mock()
    reqMock.Status = "Done"
    reqClient = Mock(name="reqMock", spec=DIRAC.RequestManagementSystem.Client.ReqClient.ReqClient)
    reqClient.readRequestsForJobs.return_value = S_OK({"Successful": {}})
    self.dra.reqClient = reqClient
    self.dra.setPendingRequests(mockJobs)
    for _index, mj in mockJobs.items():
      self.assertFalse(mj.pendingRequest)

  def test_setPendingRequests_2(self):
    """Check the setPendingRequests function."""
    mockJobs = dict((i, self.getTestMock(jobID=i)) for i in xrange(11))
    reqMock = Mock()
    reqMock.RequestID = 666
    reqClient = Mock(name="reqMock", spec=DIRAC.RequestManagementSystem.Client.ReqClient.ReqClient)
    reqClient.readRequestsForJobs.return_value = S_OK({"Successful": {6: reqMock}})
    reqClient.getRequestStatus.return_value = {'Value': 'Done'}
    self.dra.reqClient = reqClient
    self.dra.setPendingRequests(mockJobs)
    for _index, mj in mockJobs.items():
      self.assertFalse(mj.pendingRequest)
    reqClient.getRequestStatus.assert_called_once_with(666)

  def test_setPendingRequests_3(self):
    """Check the setPendingRequests function."""
    mockJobs = dict((i, self.getTestMock(jobID=i)) for i in xrange(11))
    reqMock = Mock()
    reqMock.RequestID = 555
    reqClient = Mock(name="reqMock", spec=DIRAC.RequestManagementSystem.Client.ReqClient.ReqClient)
    reqClient.readRequestsForJobs.return_value = S_OK({'Successful': {5: reqMock}})
    reqClient.getRequestStatus.return_value = {'Value': 'Pending'}
    self.dra.reqClient = reqClient
    self.dra.setPendingRequests(mockJobs)
    for index, mj in mockJobs.items():
      if index == 5:
        self.assertTrue(mj.pendingRequest)
      else:
        self.assertFalse(mj.pendingRequest)
    reqClient.getRequestStatus.assert_called_once_with(555)

  def test_setPendingRequests_Fail(self):
    """Check the setPendingRequests function."""
    mockJobs = dict((i, self.getTestMock(jobID=i)) for i in xrange(11))
    reqMock = Mock()
    reqMock.Status = "Done"
    reqClient = Mock(name="reqMock", spec=DIRAC.RequestManagementSystem.Client.ReqClient.ReqClient)
    reqClient.readRequestsForJobs.side_effect = (S_ERROR('Failure'), S_OK({'Successful': {}}))
    self.dra.reqClient = reqClient
    self.dra.setPendingRequests(mockJobs)
    for _index, mj in mockJobs.items():
      self.assertFalse(mj.pendingRequest)

  def test_getLFNStatus(self):
    """Check the getLFNStatus function."""
    mockJobs = dict((i, self.getTestMock(jobID=i)) for i in xrange(11))
    self.dra.fcClient.exists.return_value = S_OK({'Successful':
                                                  {'/my/stupid/file.lfn': True,
                                                   '/my/stupid/file2.lfn': True}})
    lfnExistence = self.dra.getLFNStatus(mockJobs)
    self.assertEqual(lfnExistence, {'/my/stupid/file.lfn': True,
                                    '/my/stupid/file2.lfn': True})

    self.dra.fcClient.exists.side_effect = (S_ERROR('args'),
                                            S_OK({'Successful':
                                                  {'/my/stupid/file.lfn': True,
                                                   '/my/stupid/file2.lfn': True}}))
    lfnExistence = self.dra.getLFNStatus(mockJobs)
    self.assertEqual(lfnExistence, {'/my/stupid/file.lfn': True,
                                    '/my/stupid/file2.lfn': True})
