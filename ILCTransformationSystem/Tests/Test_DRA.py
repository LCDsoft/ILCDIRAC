"""Test the DataRecoveryAgent"""

import unittest
import sys
from StringIO import StringIO
from collections import defaultdict

from mock import MagicMock as Mock, patch

import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger

from ILCDIRAC.ILCTransformationSystem.Agent.DataRecoveryAgent import DataRecoveryAgent
from ILCDIRAC.ILCTransformationSystem.Utilities.JobInfo import TaskInfoException

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

  def tearDown ( self ):
    pass

  def getTestMock( self, nameID=0):
    """create a JobInfo object with mocks"""
    from ILCDIRAC.ILCTransformationSystem.Utilities.JobInfo import JobInfo
    testJob = Mock ( name = "jobInfoMock_%s" % nameID, spec=JobInfo )
    testJob.jobID = 1234567
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
    self.dra.tClient.getTransformations = Mock(
      return_value=S_OK( [dict(TransformationID=1234, TransformationName="TestProd12", Type="TestProd")] ) )

    res = self.dra.getEligibleTransformations( status="Active", typeList=['TestProds'] )
    self.assertTrue( res['OK'] )
    self.assertIsInstance( res['Value'], dict )
    vals = res['Value']
    self.assertIn( "1234", vals )
    self.assertIsInstance( vals['1234'], tuple )
    self.assertEqual( ("TestProd", "TestProd12"), vals["1234"] )

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
    out = StringIO()
    sys.stdout = out
    with patch("%s.TransformationInfo" % MODULE_NAME, new=tinfoMock ):
      self.dra.treatProduction( prodID=1234, transName="TestProd12", transType="MCGeneration" ) ##returns None
    self.assertNotIn( "Getting tasks...", out.getvalue().strip().splitlines()[0] )

  def test_treatProduction2( self ):
    """test for DataRecoveryAgent treatProduction success2.........................................."""
    getJobMock = Mock( name = "getJobMOck" )
    getJobMock.getJobs.return_value = ( Mock( name = "jobsMock" ), 50, 50 )
    tinfoMock = Mock( name = "infoMock", return_value = getJobMock )
    self.dra.checkAllJobs = Mock()
    #catch the printout to check path taken
    out = StringIO()
    sys.stdout = out
    with patch("%s.TransformationInfo" % MODULE_NAME, new=tinfoMock ):
      self.dra.treatProduction( prodID=1234, transName="TestProd12", transType="MCReconstruction" ) ##returns None
    self.assertIn( "Getting tasks...", out.getvalue().strip().splitlines()[0] )

  def test_treatProduction3( self ):
    """test for DataRecoveryAgent treatProduction skip.............................................."""
    getJobMock = Mock( name = "getJobMOck" )
    getJobMock.getJobs.return_value = ( Mock( name = "jobsMock" ), 50, 50 )
    self.dra.checkAllJobs = Mock()
    self.dra.jobCache[1234] = (50, 50)
    #catch the printout to check path taken
    out = StringIO()
    sys.stdout = out
    with patch("%s.TransformationInfo" % MODULE_NAME,
               autospec=True,
               return_value=getJobMock ):
      self.dra.treatProduction( prodID=1234, transName="TestProd12", transType="MCReconstruction" ) ##returns None
    self.assertIn( "Skipping production 1234", out.getvalue().strip().splitlines()[0] )


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
    out = StringIO()
    sys.stdout = out
    from ILCDIRAC.ILCTransformationSystem.Utilities.TransformationInfo import TransformationInfo
    tInfoMock = Mock( name = "tInfoMock", spec=TransformationInfo )
    mockJobs = dict([ (i, self.getTestMock() ) for i in xrange(11) ] )
    mockJobs[2].pendingRequest = True
    mockJobs[3].getJobInformation = Mock( side_effect = ( RuntimeError("ARGJob1"), None ) )
    mockJobs[4].getTaskInfo = Mock( side_effect = ( TaskInfoException("ARG1"), None ) )
    taskDict = True
    lfnTaskDict = True
    self.dra.checkAllJobs( mockJobs, tInfoMock, taskDict, lfnTaskDict )
    self.assertIn( "ERROR: +++++ Exception:  ARGJob1", out.getvalue().strip() )
    self.assertIn( "Skip Task, due to TaskInfoException: ARG1", out.getvalue().strip() )

    ### test without additional task dicts
    out = StringIO()
    sys.stdout = out
    mockJobs = dict([ (i, self.getTestMock() ) for i in xrange(5) ] )
    mockJobs[2].pendingRequest = True
    mockJobs[3].getJobInformation = Mock( side_effect = ( RuntimeError("ARGJob2"), None ) )
    tInfoMock.reset_mock()
    self.dra.checkAllJobs( mockJobs, tInfoMock )
    self.assertIn( "ERROR: +++++ Exception:  ARGJob2", out.getvalue().strip() )

    ### test inputFile None
    out = StringIO()
    sys.stdout = out
    mockJobs = dict([ (i, self.getTestMock(nameID=i) ) for i in xrange(5) ] )
    mockJobs[1].inputFile = None
    mockJobs[1].getTaskInfo = Mock( side_effect = ( TaskInfoException("NoInputFile"), None ) )
    mockJobs[1].tType = "MCSimulation"
    tInfoMock.reset_mock()
    self.dra.checkAllJobs( mockJobs, tInfoMock, taskDict, lfnTaskDict = True )
    self.assertIn( "Failing job hard", out.getvalue().strip() )

  def test_execute( self ):
    """test for DataRecoveryAgent execute .........................................................."""
    self.dra.treatProduction = Mock()

    out = StringIO()
    sys.stdout = out
    self.dra.productionsToIgnore = [ 123, 456, 789 ]
    self.dra.jobCache = defaultdict( lambda: (0, 0) )
    self.dra.jobCache[ 123 ] = ( 10, 10 )
    self.dra.jobCache[ 124 ] = ( 10, 10 )
    self.dra.jobCache[ 125 ] = ( 10, 10 )

    ## Eligible fails
    self.dra.getEligibleTransformations = Mock( return_value = S_ERROR( "outcast" ) )
    res = self.dra.execute()
    self.assertFalse( res["OK"] )
    self.assertIn( "outcast", out.getvalue() )
    self.assertEqual( "Failure to get transformations", res['Message'] )

    ## Eligible succeeds
    self.dra.getEligibleTransformations = Mock( return_value = S_OK( { 123: ("MCGeneration", "Trafo123"),
                                                                       124: ("MCGeneration", "Trafo124"),
                                                                       125: ("MCGeneration", "Trafo125")}
                                                                   ) )
    res = self.dra.execute()
    self.assertTrue( res["OK"] )
    self.assertIn( "Will ignore the following productions: [123, 456, 789]", out.getvalue() )
    self.assertIn( "Ignoring Production: 123", out.getvalue() )
    self.assertIn( "Running over Production: 124", out.getvalue() )


    ## Notes To Send
    self.dra.getEligibleTransformations = Mock( return_value = S_OK( { 123: ("MCGeneration", "Trafo123"),
                                                                       124: ("MCGeneration", "Trafo124"),
                                                                       125: ("MCGeneration", "Trafo125")}
                                                                   ) )
    self.dra.notesToSend = "Da hast du deine Karte"
    sendmailMock = Mock()
    sendmailMock.sendMail.return_value = S_OK("Nice Card")
    notificationMock = Mock( return_value = sendmailMock )
    with patch("%s.NotificationClient" % MODULE_NAME, new=notificationMock ):
      res = self.dra.execute()
    self.assertTrue( res["OK"] )
    self.assertIn( "Will ignore the following productions: [123, 456, 789]", out.getvalue() )
    self.assertIn( "Ignoring Production: 123", out.getvalue() )
    self.assertIn( "Running over Production: 124", out.getvalue() )
    self.assertNotIn( 124, self.dra.jobCache ) ## was popped
    self.assertIn( 125, self.dra.jobCache )## was not popped
    gLogger.notice( "JobCache: %s" % self.dra.jobCache )

    ## sending notes fails
    self.dra.notesToSend = "Da hast du deine Karte"
    sendmailMock = Mock()
    sendmailMock.sendMail.return_value = S_ERROR("No stamp")
    notificationMock = Mock( return_value = sendmailMock )
    with patch("%s.NotificationClient" % MODULE_NAME, new=notificationMock ):
      res = self.dra.execute()
    self.assertTrue( res["OK"] )
    self.assertNotIn( 124, self.dra.jobCache ) ## was popped
    self.assertIn( 125, self.dra.jobCache )## was not popped
    self.assertIn( "Cannot send notification mail", out.getvalue() )

    self.assertEqual( "", self.dra.notesToSend )


  def test_printSummary( self ):
    """test DataRecoveryAgent printSummary.........................................................."""
    out = StringIO()
    sys.stdout = out

    self.dra.notesToSend = ""
    self.dra.printSummary()
    self.assertNotIn( " Other Tasks --> Keep                                    :     0", self.dra.notesToSend )


    self.dra.notesToSend = "Note This"
    self.dra.printSummary()


if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( TestDRA )
  TESTRESULT = unittest.TextTestRunner( verbosity = 3 ).run( SUITE )
