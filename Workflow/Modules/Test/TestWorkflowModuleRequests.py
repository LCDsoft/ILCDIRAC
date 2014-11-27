#!/usr/env python

"""
Test generateFailoverFile
"""
__RCSID__ = "$Id$"
#pylint: disable=W0212,R0904
import unittest, copy, os
from mock import MagicMock as Mock

from DIRAC import gLogger, S_ERROR, S_OK

from DIRAC.Core.Base import Script
Script.parseCommandLine()
from ILCDIRAC.Workflow.Modules.ModuleBase import ModuleBase
from DIRAC.RequestManagementSystem.Client.Request import Request

from ILCDIRAC.Core.Utilities.ProductionData import getLogPath
from ILCDIRAC.Workflow.Modules.FailoverRequest import FailoverRequest
from ILCDIRAC.Workflow.Modules.UploadOutputData import UploadOutputData
from ILCDIRAC.Workflow.Modules.UploadLogFile import UploadLogFile
from ILCDIRAC.Workflow.Modules.UserJobFinalization import UserJobFinalization


from DIRAC.Workflow.Modules.test.Test_Modules import ModulesTestCase as DiracModulesTestCase
#import DIRAC.Workflow.Modules.test.Test_Modules as Test_Modules
gLogger.setLevel("Notice")
gLogger.showHeaders(True)
class ModulesTestCase ( DiracModulesTestCase ):
  """ ILCDirac version of Workflow module tests"""
  def setUp( self ):
    """Set up the objects"""
    super(ModulesTestCase, self).setUp()
    self.log = gLogger.getSubLogger("MODULEBASE")
    self.mb = ModuleBase()
    self.mb.rm = self.rm_mock
    self.mb.request = self.rc_mock
    self.mb.jobReport = self.jr_mock
    self.mb.fileReport = self.fr_mock
    self.mb.workflow_commons = self.wf_commons[0]
    self.mb.workflow_commons['LogFilePath'] = "/ilc/user/s/sailer/test/dummy/folder"
    self.mb.log = gLogger.getSubLogger("ModuleBaseTest")
    self.mb.log.showHeaders(True)
    self.mb.ignoreapperrors = False
    self.uod = UploadOutputData()
    self.uod.workflow_commons = self.mb.workflow_commons

    self.fr = FailoverRequest()
    self.fr.workflow_commons = self.mb.workflow_commons

    self.ulf = UploadLogFile()

    self.rc_mock = Mock(name='RequestContainer')
    self.rc_mock.update.return_value = {'OK': True, 'Value': ''}
    self.rc_mock.setDISETRequest.return_value = {'OK': True, 'Value': ''}
    self.rc_mock.isEmpty.return_value = {'OK': True, 'Value': ''}
    self.rc_mock.toXML.return_value = {'OK': True, 'Value': 'Ex Em El'}
    self.rc_mock.getDigest.return_value = {'OK': True, 'Value': 'Indigestion'}
    self.rc_mock.__len__.return_value = 1

class TestModuleBase( ModulesTestCase ):
  """ Tests for ModuleBase functions"""

    
  def test_generateFailoverFile( self ):
    """run the generateFailoverFile function and see what happens"""
    dummy_res = self.mb.generateFailoverFile()
    #print res

  def test_CreateRemoveRequest( self ):
    """ModuleBase: Create a removal request for some LFN.................................."""
    gLogger.setLevel("ERROR")
    lfnList = ['/ilc/user/s/sailer/2014_11/12/12345/testsim.slcio','/ilc/user/s/sailer/2014_11/12/12345/testsim.slcio']
    mob = ModuleBase()
    mob.workflow_commons = dict()
    mob.jobID = 444444
    mob.addRemovalRequests(lfnList)
    request = mob.workflow_commons['Request']
    mob.log.notice(request)
    self.assertTrue( len(request) == 1 )


  def test_MB_getCandidateFiles( self ):
    """ModuleBase: getCandidateFiles: files exist.........................................."""
    gLogger.setLevel("ERROR")
    outputList = {'h_nunu_gen_4191_0007': {'outputPath': '/ilc/prod/clic/1.4tev/h_nunu/gen', 'outputFile': 'h_nunu_gen_4191_0007.stdhep', 'outputDataSE': 'CERN-SRM'}, 'h_nunu_gen_4191_0006': {'outputPath': '/ilc/prod/clic/1.4tev/h_nunu/gen', 'outputFile': 'h_nunu_gen_4191_0006.stdhep', 'outputDataSE': 'CERN-SRM'}, 'h_nunu_gen_4191_0005': {'outputPath': '/ilc/prod/clic/1.4tev/h_nunu/gen', 'outputFile': 'h_nunu_gen_4191_0005.stdhep', 'outputDataSE': 'CERN-SRM'}, 'h_nunu_gen_4191_0004': {'outputPath': '/ilc/prod/clic/1.4tev/h_nunu/gen', 'outputFile': 'h_nunu_gen_4191_0004.stdhep', 'outputDataSE': 'CERN-SRM'}, 'h_nunu_gen_4191_0003': {'outputPath': '/ilc/prod/clic/1.4tev/h_nunu/gen', 'outputFile': 'h_nunu_gen_4191_0003.stdhep', 'outputDataSE': 'CERN-SRM'}, 'h_nunu_gen_4191_0002': {'outputPath': '/ilc/prod/clic/1.4tev/h_nunu/gen', 'outputFile': 'h_nunu_gen_4191_0002.stdhep', 'outputDataSE': 'CERN-SRM'}, 'h_nunu_gen_4191_0001': {'outputPath': '/ilc/prod/clic/1.4tev/h_nunu/gen', 'outputFile': 'h_nunu_gen_4191_0001.stdhep', 'outputDataSE': 'CERN-SRM'}, 'h_nunu_gen_4191_0000': {'outputPath': '/ilc/prod/clic/1.4tev/h_nunu/gen', 'outputFile': 'h_nunu_gen_4191_0000.stdhep', 'outputDataSE': 'CERN-SRM'}}.values()
    outputLFNs = ['/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0000.stdhep',
                  '/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0001.stdhep',
                  '/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0002.stdhep',
                  '/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0003.stdhep',
                  '/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0004.stdhep',
                  '/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0005.stdhep',
                  '/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0006.stdhep',
                  '/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0007.stdhep']
    dummy_fileMask = None
    result = self.mb.getCandidateFiles(outputList, outputLFNs, dummy_fileMask)
    resDict = [ os.path.basename(lfn) in result['Value'] for lfn in outputLFNs ]
    gLogger.debug("Result: %s" % result)
    gLogger.debug("ResDict: %s" % resDict)
    self.assertTrue( all(resDict) )

  def test_MB_getCandidateFiles_FileNotFound( self ):
    """ModuleBase: getCandidateFiles: No Such File........................................."""
    gLogger.setLevel("ERROR")
    outputList = {'h_nunu_gen_4191_NSF': {'outputPath': '/ilc/prod/clic/1.4tev/h_nunu/gen', 'outputFile': 'h_nunu_gen_4191_NSF.stdhep', 'outputDataSE': 'CERN-SRM'}}.values()
    outputLFNs = ['/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_NSF.stdhep']
    dummy_fileMask = None
    result = self.mb.getCandidateFiles(outputList, outputLFNs, dummy_fileMask)
    self.assertTrue( "Output Data Not Found" in result['Message'] )

  def test_MB_getCandidateFiles_FileTooLong( self ):
    """ModuleBase: getCandidateFiles: File Too Long........................................"""
    gLogger.setLevel("ERROR")
    outputList = {'a'*128: {'outputPath': '/ilc/prod/clic/1.4tev/h_nunu/gen', 'outputFile': 'a'*128, 'outputDataSE': 'CERN-SRM'}}.values()
    outputLFNs = ['/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/'+'a'*128]
    dummy_fileMask = None
    result = self.mb.getCandidateFiles(outputList, outputLFNs, dummy_fileMask)
    self.assertTrue( "Filename too long" in result['Message'] )

  def test_MB_getCandidateFiles_PathTooLong( self ):
    """ModuleBase: getCandidateFiles: Path Too Long........................................"""
    gLogger.setLevel("ERROR")
    outputList = {'a'*127: {'outputPath': '/bbbbbbbbbb'*26, 'outputFile': 'a'*127, 'outputDataSE': 'CERN-SRM'}}.values()
    outputLFNs = ['/bbbbbbbbbb'*26+'/'+'a'*127]
    dummy_fileMask = None
    result = self.mb.getCandidateFiles(outputList, outputLFNs, dummy_fileMask)
    self.assertTrue( "LFN too long" in result['Message'] )


#############################################################################
# UploadLogFile.py
#############################################################################

class TestUploadLogFile( ModulesTestCase ):
  """ test UploadLogFile """

  def test_ULF_ASI_NoLogFiles( self ):
    """ULF.applicationSpecificInputs: no log files present.............................."""
    self.ulf = UploadLogFile()
    self.ulf.workflow_commons = copy.deepcopy(self.mb.workflow_commons)
    self.ulf.log = gLogger.getSubLogger("ULF-NoLogFiles")
    self.ulf.log.setLevel("INFO")

    self.ulf.resolveInputVariables = Mock(return_value=S_OK())
    self.ulf._determineRelevantFiles = Mock(return_value=S_OK([]))
    self.ulf.applicationSpecificInputs()
    res = self.ulf.execute()
    self.assertTrue( res['OK'] )

  def test_ULF_ASI_OneLogFile( self ):
    """ULF.applicationSpecificInputs: one log files present............................."""
    self.ulf = UploadLogFile()
    self.ulf.log = gLogger.getSubLogger("ULF-OneLogFile")
    self.ulf.workflow_commons = copy.deepcopy(self.wf_commons[0])
    self.ulf._determineRelevantFiles = Mock(return_value=S_OK(['MyLogFile.log']))
    self.ulf.logSE.putDirectory = Mock(return_value=S_OK(dict(Failed=['MyLogFile.log'],Message="Ekke Ekke Ekke Ekke")))
    self.ulf.logLFNPath = getLogPath(self.ulf.workflow_commons)['Value']['LogTargetPath'][0]
    self.ulf._tryFailoverTransfer = Mock(return_value = S_OK({'Request': self.rc_mock,
                                                              'uploadedSE': 'CERN-SRM'}))
    self.ulf.applicationSpecificInputs()
    res = self.ulf.execute()
    self.assertTrue( res['OK'] )


  def test_ULF_ASI_FailedFailover( self ):
    """ULF.applicationSpecificInputs: Failovertransfer failes..........................."""
    self.ulf = UploadLogFile()
    self.ulf.log = gLogger.getSubLogger("ULF-OneLogFile")
    self.ulf.workflow_commons = copy.deepcopy(self.wf_commons[0])
    self.ulf._determineRelevantFiles = Mock(return_value=S_OK(['MyLogFile.log']))
    self.ulf.logSE.putDirectory = Mock(return_value=S_OK(dict(Failed=['MyLogFile.log'],Message="Ekke Ekke Ekke Ekke")))
    self.ulf.logLFNPath = getLogPath(self.ulf.workflow_commons)['Value']['LogTargetPath'][0]
    self.ulf._tryFailoverTransfer = Mock(return_value = S_OK())
    self.ulf.applicationSpecificInputs()
    res = self.ulf.execute()
    self.assertTrue( res['OK'] )

  def test_ULF_ASI_LogFileGone( self ):
    """ULF.applicationSpecificInputs: log file disappeared, IOError....................."""
    self.ulf = UploadLogFile()
    self.ulf.workflow_commons = copy.deepcopy(self.wf_commons[0])
    self.ulf.log = gLogger.getSubLogger("ULF-LogFileGone")

    self.ulf.logLFNPath = getLogPath(self.ulf.workflow_commons)['Value']['LogTargetPath'][0]
    self.ulf._determineRelevantFiles = Mock(return_value=S_OK(['std.out']))
    self.assertRaises( IOError, self.ulf.execute )

  def test_ULF_ASI_execute( self ):
    """ULF.ASI,Exe: run through and get request.........................................."""
    self.ulf.workflow_commons = copy.deepcopy(self.wf_commons[0])
    self.ulf.log = gLogger.getSubLogger("ULF-RequestTest")
    self.ulf._determineRelevantFiles = Mock(return_value=S_OK(['MyLogFile.log','MyOtherLogFile.log']))
    self.ulf.logSE.putDirectory = Mock(return_value=S_OK(dict(Failed=['MyLogFile.log', 'MyOtherLogFile.log'],
                                                              Message="Ekke Ekke Ekke Ekke")))
    self.mb.workflow_commons['Request']  = Request()
    self.ulf._tryFailoverTransfer = Mock(return_value = S_OK({'Request': self.mb.workflow_commons['Request'],
                                                              'uploadedSE': 'CERN-SRM'}))
    self.ulf.logLFNPath = getLogPath(self.ulf.workflow_commons)['Value']['LogTargetPath'][0]
    self.ulf.applicationSpecificInputs()
    res = self.ulf.execute()
    self.assertTrue( res['OK'] )

#############################################################################
# UploadOutputData.py
#############################################################################

    
class UploadOutputDataSuccess( ModulesTestCase ):
  """ test UploadLogFile """
  def test_execute( self ):
    """ tests execute function"""
    pass

class UploadOutputDataFailure( ModulesTestCase ):
  """ test UploadLogFile """
  def test_execute( self ):
    """ tests execute function"""
    pass

#############################################################################
# FailoverRequest.py
#############################################################################

class TestFailoverRequest( ModulesTestCase ):
  """ test UploadLogFile """
  def setUp( self ):
    super(TestFailoverRequest, self).setUp()
    self.frq = None

  def test_ASI_Enabled( self ):
    """applicationSpecificInputs: control flag is enabled......................................."""
    self.frq = FailoverRequest()
    self.frq.workflow_commons = dict( )
    self.frq.log = gLogger.getSubLogger("testASI")
    os.environ['JOBID']="12345"
    self.frq.applicationSpecificInputs()
    del os.environ['JOBID']
    self.assertTrue ( self.frq.enable )

  def test_ASI_Disable( self ):
    """applicationSpecificInputs: control flag is enabled with non boolean......................"""
    self.frq = FailoverRequest()
    self.frq.workflow_commons = dict( )
    self.frq.log = gLogger.getSubLogger("testASI")
    os.environ['JOBID']="12345"
    self.frq.step_commons = dict( Enable = "arg")
    self.frq.applicationSpecificInputs()
    del os.environ['JOBID']
    self.assertFalse ( self.frq.enable )

  def test_ASI_Disabled( self ):
    """applicationSpecificInputs: control flag is disabled......................................"""
    self.frq = FailoverRequest()
    self.frq.workflow_commons = dict( )
    self.frq.log = gLogger.getSubLogger("testASI")

    self.frq.applicationSpecificInputs()
    self.assertTrue ( self.frq.enable == False )

  def test_ASI_AllVariables( self ):
    """applicationSpecificInputs: checks if all variables have been properly set after this call"""
    self.frq = FailoverRequest()
    self.frq.workflow_commons = dict( JobReport = self.jr_mock, FileReport = self.fr_mock, PRODUCTION_ID=43321, JOB_ID = 12345 )
    os.environ['JOBID']="12345"
    self.frq.applicationSpecificInputs()
    del os.environ['JOBID']
    self.assertTrue( self.frq.jobReport and self.frq.fileReport and
                     self.frq.productionID and self.frq.prodJobID and self.frq.enable )

  def test_ASI_NoVariables( self ):
    """applicationSpecificInputs: checks that no variables have been set after this call........"""
    self.frq = FailoverRequest()
    self.frq.workflow_commons = dict()
    os.environ['JOBID']="12345"
    self.frq.applicationSpecificInputs()
    del os.environ['JOBID']
    self.assertFalse( self.frq.jobReport or self.frq.fileReport or
                      self.frq.productionID or self.frq.prodJobID )

  def test_Exe_Disabled( self ):
    """execute: is disabled....................................................................."""
    self.frq = FailoverRequest()
    self.frq._getJobReporter = Mock(return_value=self.jr_mock)
    self.frq.log = gLogger.getSubLogger("Frq-Exe-Disabled")
    self.frq.workflow_commons = dict( JobReport = self.jr_mock, FileReport = self.fr_mock, PRODUCTION_ID=43321, JOB_ID = 12345 )
    self.frq.enable = False
    self.frq.workflow_commons = dict( )
    res = self.frq.execute()
    self.assertTrue( "Module is disabled" in res['Value'] )

  def test_Exe_WFFail( self ):
    """execute: WF Failed......................................................................."""
    gLogger.setLevel("ERROR")
    self.frq = FailoverRequest()
    self.frq.log = gLogger.getSubLogger("Frq-Exe-Fail")
    self.frq.applicationSpecificInputs = Mock(return_value = S_OK())
    self.jr_mock.generateForwardDISET = Mock(return_value = S_ERROR("EKKE"))
    self.frq.enable = True
    self.frq.jobID = 12345
    self.frq.workflow_commons = dict( JobReport = self.jr_mock, FileReport = self.fr_mock, PRODUCTION_ID=43321, JOB_ID = 12345 )
    self.frq.workflowStatus = S_ERROR()
    res = self.frq.execute()
    self.assertFalse( res['OK'] )

  def test_Exe_RIV_Failes( self ):
    """execute: WF Failed......................................................................."""
    gLogger.setLevel("ERROR")
    self.frq = FailoverRequest()
    self.frq.log = gLogger.getSubLogger("Frq-Exe-Fail")
    self.frq.resolveInputVariables = Mock(return_value = S_ERROR("EKKE: no input variables"))
    self.frq.applicationSpecificInputs = Mock(return_value = S_OK())
    self.jr_mock.generateForwardDISET = Mock(return_value = S_ERROR("EKKE"))
    self.frq.enable = True
    self.frq.jobID = 12345
    self.frq.workflow_commons = dict( JobReport = self.jr_mock, FileReport = self.fr_mock, PRODUCTION_ID=43321, JOB_ID = 12345 )
    self.frq.workflowStatus = S_ERROR()
    res = self.frq.execute()
    self.assertFalse( res['OK'] )

  def test_Exe_Success( self ):
    """execute: succeeds........................................................................"""
    gLogger.setLevel("ERROR")
    self.frq = FailoverRequest()
    self.frq.log = gLogger.getSubLogger("Frq-Exe-Succeed")
    self.frq.applicationSpecificInputs = Mock(return_value=S_OK())
    self.frq.jobID = 12345
    self.frq.workflow_commons = dict( JobReport = self.jr_mock, FileReport = self.fr_mock, PRODUCTION_ID=43321, JOB_ID = 12345 )
    self.frq.workflow_commons['Request'] = self.rc_mock
    res = self.frq.execute()
    self.assertTrue( res['OK'] )

  def test_Exe_genDisetRequest( self ):
    """execute: Generate Diset Request.........................................................."""
    gLogger.setLevel("ERROR")
    self.frq = FailoverRequest()
    self.frq.log = gLogger.getSubLogger("Frq-Exe-GenDiset")
    self.frq.applicationSpecificInputs = Mock(return_value=S_OK())
    self.frq.enable = True
    self.frq.jobID = 12345
    self.frq.fileReport = Mock(name="FailedFileReport")
    self.frq.fileReport.commit.return_value = S_ERROR("Nobody suspects the ")
    self.frq.fileReport.generateForwardDISET.return_value = S_OK("Spanish Inquisition")
    self.frq.workflow_commons = dict( JobReport = self.jr_mock, FileReport = self.frq.fileReport,
                                      Request = self.rc_mock, PRODUCTION_ID=43321, JOB_ID = 12345 )
    self.frq.execute()
    self.assertTrue( self.frq.workflow_commons['Request'] )

#############################################################################
# UploadOutputData.py
#############################################################################

class TestUploadOutputData( ModulesTestCase ):
  """ test UploadOutputData """
  def setUp( self ):
    super(TestUploadOutputData, self).setUp()
    self.uod = None

  def test_ASI_Enabled( self ):
    """UOD.applicationSpecificInputs: control flag is enabled......................................."""
    gLogger.setLevel("ERROR")
    self.uod = UploadOutputData()
    self.uod.workflow_commons = dict( )
    self.uod.log = gLogger.getSubLogger("testASI")
    os.environ['JOBID']="12345"
    self.uod.applicationSpecificInputs()
    del os.environ['JOBID']
    self.assertTrue( self.uod.enable )

  def test_ASI_Disable( self ):
    """UOD.applicationSpecificInputs: control flag is enabled with non boolean......................"""
    gLogger.setLevel("ERROR")
    self.uod = UploadOutputData()
    self.uod.workflow_commons = dict( )
    self.uod.log = gLogger.getSubLogger("testASI")
    os.environ['JOBID']="12345"
    self.uod.step_commons = dict( Enable = "arg")
    self.uod.applicationSpecificInputs()
    del os.environ['JOBID']
    self.assertFalse( self.uod.enable )

  def test_ASI_Disabled( self ):
    """UOD.applicationSpecificInputs: control flag is disabled......................................"""
    gLogger.setLevel("ERROR")
    self.uod = UploadOutputData()
    self.uod.workflow_commons = dict( )
    self.uod.log = gLogger.getSubLogger("testASI")
    self.uod.applicationSpecificInputs()
    self.assertTrue( self.uod.enable == False )

  def test_ASI_AllVariables( self ):
    """UOD.applicationSpecificInputs: checks if all variables have been properly set after this call"""
    gLogger.setLevel("ERROR")
    self.uod = UploadOutputData()
    self.uod.log = gLogger.getSubLogger("Uod-Asi-AllVars")
    self.uod.workflow_commons = dict( JobReport = self.jr_mock, FileReport = self.fr_mock, PRODUCTION_ID=43321,
                                      JOB_ID = 12345,
                                      outputList = [{'outputFile': 'myFile_gen.stdhep'},
                                                    {'outputFile': 'myFile_dst.slcio'},
                                                    {'outputFile': 'myFile_sim.slcio'},
                                                    {'outputFile': 'myFile_rec.slcio'},
                                                    {'outputFile': 'myFile_unk.slcio'}
                                                   ],
                                      ProductionOutputData = "/my/long/path/GEN/myFile_gen_12345_001.stdhep;/my/long/path/DST/myFile_dst_12345_001.slcio"
                                    )
    os.environ['JOBID']="12345"
    self.uod.applicationSpecificInputs()
    del os.environ['JOBID']
    self.assertTrue( type(self.uod.outputDataFileMask) == type([]) )
    self.assertTrue( self.uod.outputMode )
    self.assertTrue( self.uod.outputList )
    self.assertTrue( self.uod.productionID )
    self.assertTrue( self.uod.prodOutputLFNs )
    self.assertTrue( self.uod.experiment )

  def test_ASI_NoVariables( self ):
    """UOD.applicationSpecificInputs: checks that no variables have been set after this call........"""
    gLogger.setLevel("ERROR")
    self.uod = UploadOutputData()
    self.uod.workflow_commons = dict()
    os.environ['JOBID']="12345"
    self.uod.applicationSpecificInputs()
    del os.environ['JOBID']
    self.assertFalse( self.uod.jobReport or self.uod.productionID )


  def test_ASI_OutputListCorrect( self ):
    """UOD.applicationSpecificInputs: check outputfile list is treated properly....................."""
    gLogger.setLevel("ERROR")
    self.uod = UploadOutputData()
    self.uod.workflow_commons = {'outputList': [{'outputPath': '/ilc/prod/clic/1.4tev/h_nunu/gen',
                                                 'outputFile': 'h_nunu_gen.stdhep',
                                                 'outputDataSE': 'CERN-SRM'},
                                                {'outputPath': '/ilc/prod/clic/1.4tev/h_nunu/SID/SIM',
                                                 'outputFile': 'h_nunu_sim.slcio',
                                                 'outputDataSE': 'CERN-SRM'},
                                                {'outputPath': '/ilc/prod/clic/1.4tev/h_nunu/SID/REC',
                                                 'outputFile': 'h_nunu_rec.slcio',
                                                 'outputDataSE': 'CERN-SRM'},
                                                {'outputPath': '/ilc/prod/clic/1.4tev/h_nunu/SID/DST',
                                                 'outputFile': 'h_nunu_dst.slcio',
                                                 'outputDataSE': 'CERN-SRM'}],
                                 'ProductionOutputData':
                                 '/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0001.stdhep;/ilc/prod/clic/1.4tev/h_nunu/SID/SIM/00004192/001/h_nunu_sim_4192_1002.slcio;/ilc/prod/clic/1.4tev/h_nunu/SID/REC/00004193/002/h_nunu_rec_4193_2766.slcio;/ilc/prod/clic/1.4tev/h_nunu/SID/DST/00004193/002/h_nunu_dst_4193_2766.slcio'}
    os.environ['JOBID']="12345"
    self.uod.applicationSpecificInputs()
    del os.environ['JOBID']
    self.uod.log.debug([ o['outputFile'] for o in self.uod.outputList] )
    self.assertTrue( len(self.uod.outputList) == 4 )

  def test_GOL_Reco( self ):
    """outputList properly formated for reconstruction jobs........................................."""
    gLogger.setLevel("ERROR")
    wf_real = {'TaskID': '2766', 'TotalSteps': '4', 'JobName': '00004193_00002766', 'Priority': '1', 'SoftwarePackages': 'overlayinput.1;lcsim.CLIC_CDR;slicpandora.CLIC_CDR_photon_fix', 'DebugLFNs': '', 'Status': 'Created', 'JobReport': self.jr_mock, 'BannedSites': 'LCG.Bristol.uk;LCG.RAL-LCG2.uk', 'LogLevel': 'verbose', 'StdOutput': 'std.out', 'JobType': 'MCReconstruction_Overlay', 'SystemConfig': 'x86_64-slc5-gcc43-opt', 'TransformationID': '4193', 'JOB_ID': '00002766', 'productionVersion': '$Id: ', 'StdError': 'std.err', 'LogTargetPath': '/ilc/prod/clic/1.4tev/h_nunu/SID/REC/00004193/LOG/00004193_2766.tar', 'IS_PROD': 'True', 'Request': self.rc_mock, 'ParametricInputSandbox': '', 'emailAddress': 'stephane.poss@cern.ch', 'JobGroup': '00004193', 'NbOfEvts': 200, 'Origin': 'DIRAC', 'outputList': [{'outputPath': '/ilc/prod/clic/1.4tev/h_nunu/SID/REC', 'outputFile': 'h_nunu_rec.slcio', 'outputDataSE': 'CERN-SRM'}, {'outputPath': '/ilc/prod/clic/1.4tev/h_nunu/SID/DST', 'outputFile': 'h_nunu_dst.slcio', 'outputDataSE': 'CERN-SRM'}], 'Energy': 1400.0, 'ProductionOutputData': '/ilc/prod/clic/1.4tev/h_nunu/SID/REC/00004193/002/h_nunu_rec_4193_2766.slcio;/ilc/prod/clic/1.4tev/h_nunu/SID/DST/00004193/002/h_nunu_dst_4193_2766.slcio', 'Site': 'ANY', 'OwnerGroup': 'ilc_prod', 'PRODUCTION_ID': '00004193', 'Owner': 'sailer', 'MaxCPUTime': '300000', 'LogFilePath': '/ilc/prod/clic/1.4tev/h_nunu/SID/REC/00004193/LOG/002', 'InputData': '/ilc/prod/clic/1.4tev/h_nunu/SID/SIM/00004192/000/h_nunu_sim_4192_655.slcio'}
    self.uod = UploadOutputData()
    self.uod.workflow_commons = wf_real
    self.uod.outputList = wf_real['outputList']
    proddata = self.uod.workflow_commons['ProductionOutputData'].split(";")
    olist = {}
    for obj in self.uod.outputList:
      self.uod.getTreatedOutputlist(proddata, olist, obj)

    self.uod.log.debug ( "OList: %s " % olist )
    filesFound = [f in olist for f in ('h_nunu_dst_4193_2766', 'h_nunu_rec_4193_2766') ]
    self.uod.log.debug("%s" % filesFound )
    self.assertTrue( all( filesFound ) )

  def test_GOL_RecoNew( self ):
    """outputList properly formated for reconstruction jobs........................................."""
    gLogger.setLevel("ERROR")
    wf_real = {'TaskID': '2766', 'TotalSteps': '4', 'JobName': '00004193_00002766', 'Priority': '1', 'SoftwarePackages': 'overlayinput.1;lcsim.CLIC_CDR;slicpandora.CLIC_CDR_photon_fix', 'DebugLFNs': '', 'Status': 'Created', 'JobReport': self.jr_mock, 'BannedSites': 'LCG.Bristol.uk;LCG.RAL-LCG2.uk', 'LogLevel': 'verbose', 'StdOutput': 'std.out', 'JobType': 'MCReconstruction_Overlay', 'SystemConfig': 'x86_64-slc5-gcc43-opt', 'TransformationID': '4193', 'JOB_ID': '00002766', 'productionVersion': '$Id: ', 'StdError': 'std.err', 'LogTargetPath': '/ilc/prod/clic/1.4tev/h_nunu/SID/REC/00004193/LOG/00004193_2766.tar', 'IS_PROD': 'True', 'Request': self.rc_mock, 'ParametricInputSandbox': '', 'emailAddress': 'stephane.poss@cern.ch', 'JobGroup': '00004193', 'NbOfEvts': 200, 'Origin': 'DIRAC', 'outputList': [{'outputPath': '/ilc/prod/clic/1.4tev/h_nunu/SID/REC', 'outputFile': 'h_nunu_rec.slcio', 'outputDataSE': 'CERN-SRM'}, {'outputPath': '/ilc/prod/clic/1.4tev/h_nunu/SID/DST', 'outputFile': 'h_nunu_dst.slcio', 'outputDataSE': 'CERN-SRM'}], 'Energy': 1400.0, 'ProductionOutputData': '/ilc/prod/clic/1.4tev/h_nunu/SID/REC/00004193/002/h_nunu_rec_4193_2766.slcio;/ilc/prod/clic/1.4tev/h_nunu/SID/DST/00004193/002/h_nunu_dst_4193_2766.slcio', 'Site': 'ANY', 'OwnerGroup': 'ilc_prod', 'PRODUCTION_ID': '00004193', 'Owner': 'sailer', 'MaxCPUTime': '300000', 'LogFilePath': '/ilc/prod/clic/1.4tev/h_nunu/SID/REC/00004193/LOG/002', 'InputData': '/ilc/prod/clic/1.4tev/h_nunu/SID/SIM/00004192/000/h_nunu_sim_4192_655.slcio'}
    self.uod = UploadOutputData()
    self.uod.workflow_commons = wf_real
    self.uod.outputList = wf_real['outputList']
    proddata = self.uod.workflow_commons['ProductionOutputData'].split(";")
    olist = {}
    for obj in self.uod.outputList:
      self.uod.getTreatedOutputlistNew(proddata, olist, obj)

    self.uod.log.debug ( "OList: %s " % olist )
    filesFound = [f in olist for f in ('h_nunu_dst_4193_2766', 'h_nunu_rec_4193_2766') ]
    self.uod.log.debug("%s" % filesFound )
    self.assertTrue( all( filesFound ) )


  def test_GOL_gen( self ):
    """outputList properly formated for reconstruction jobs........................................."""
    gLogger.setLevel("ERROR")
    self.uod = UploadOutputData()
    self.uod.workflow_commons = {'outputList': [{'outputPath': '/ilc/prod/clic/1.4tev/h_nunu/gen',
                                                 'outputFile': 'h_nunu_gen.stdhep',
                                                 'outputDataSE': 'CERN-SRM'}],
                                 'ProductionOutputData':
                                 '/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0000.stdhep;/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0001.stdhep;/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0002.stdhep;/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0003.stdhep;/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0004.stdhep;/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0005.stdhep;/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0006.stdhep;/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0007.stdhep;'}

    self.uod.outputList = self.uod.workflow_commons['outputList']
    proddata = self.uod.workflow_commons['ProductionOutputData'].split(";")
    olist = {}
    for obj in self.uod.outputList:
      self.uod.getTreatedOutputlistNew(proddata, olist, obj)

    self.uod.log.debug ( "OList: %s " % olist )
    filesFound = [f in olist for f in ('h_nunu_gen_4191_0000',
                                       'h_nunu_gen_4191_0001',
                                       'h_nunu_gen_4191_0002',
                                       'h_nunu_gen_4191_0003',
                                       'h_nunu_gen_4191_0004',
                                       'h_nunu_gen_4191_0005',
                                       'h_nunu_gen_4191_0006',
                                       'h_nunu_gen_4191_0007') ]
    self.uod.log.debug("%s" % filesFound )
    self.assertTrue( all( filesFound ) )


#############################################################################
# UserJobFinalization.py
#############################################################################

class TestUserJobFinalization( ModulesTestCase ):
  """ test UserJobFinalization """
  def setUp( self ):
    super(TestUserJobFinalization, self).setUp()
    self.ujf = UserJobFinalization()


  def test_UJF_execute_isLastStep(self):
    """UJF.execute: is last step"""
    self.ujf.step_commons['STEP_NUMBER'] = 2
    self.ujf.workflow_commons['TotalSteps'] = 2
    resLS = self.ujf.isLastStep()
    self.assertTrue( resLS['OK'] )

  def test_UJF_execute_isLastStep_not(self):
    """UJF.execute: is Not the last step"""
    self.ujf.step_commons['STEP_NUMBER'] = 1
    self.ujf.workflow_commons['TotalSteps'] = 2
    resLS = self.ujf.isLastStep()
    self.assertFalse( resLS['OK'] )

  def test_UFJ_getOutputList(self):
    """UJF.execute: getOutputList"""
    gLogger.setLevel("DEBUG")
    self.ujf.userOutputSE = "CERN-SRM"
    self.ujf.userOutputData = ['gen.stdhep',
                               'sim.slcio',
                               'rec.slcio',
                               'dst.slcio']

    outputList = self.ujf.getOutputList()
    self.log.debug(outputList)

#############################################################################
# Run Tests
#############################################################################
def runTests():
  """Runs our tests"""
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ModulesTestCase )

  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestUploadLogFile ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestModuleBase ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestUploadOutputData ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestFailoverRequest ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestUserJobFinalization ) )
  
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  print testResult

  ## Test from Dirac Proper
  # suite = unittest.defaultTestLoader.loadTestsFromTestCase( DiracModulesTestCase )
  # suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( Test_Modules.ModuleBaseSuccess ) )
  # suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( Test_Modules.FailoverRequestSuccess ) )
  # testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

if __name__ == '__main__':
  runTests()
