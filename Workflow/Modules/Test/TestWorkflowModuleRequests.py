#!/usr/bin/env python
"""
Test WorkflowModules
"""
__RCSID__ = "$Id$"
#pylint: disable=W0212,R0904,C0302
import unittest, copy, os, shutil
import tempfile
import sys
from StringIO import StringIO

from mock import MagicMock as Mock, patch
from DIRAC import gLogger, S_ERROR, S_OK

from ILCDIRAC.Workflow.Modules.ModuleBase import ModuleBase
from DIRAC.RequestManagementSystem.Client.Request import Request

from ILCDIRAC.Core.Utilities.ProductionData import getLogPath
from ILCDIRAC.Workflow.Modules.FailoverRequest import FailoverRequest
from ILCDIRAC.Workflow.Modules.UploadOutputData import UploadOutputData
from ILCDIRAC.Workflow.Modules.UploadLogFile import UploadLogFile
from ILCDIRAC.Workflow.Modules.UserJobFinalization import UserJobFinalization


#from DIRAC.Workflow.Modules.test.Test_Modules import ModulesTestCase as DiracModulesTestCase
#import DIRAC.Workflow.Modules.test.Test_Modules as Test_Modules
gLogger.setLevel("DEBUG")
gLogger.showHeaders(True)

def cleanup(tempdir):
  """
  Remove files after run
  """
  try:
    shutil.rmtree(tempdir)
  except OSError:
    pass

class ModulesTestCase ( unittest.TestCase ):
  """ ILCDirac version of Workflow module tests"""

  @patch("ILCDIRAC.Workflow.Modules.ModuleBase.getProxyInfoAsString", new=Mock(return_value=S_OK()))
  @patch("DIRAC.Core.Security.ProxyInfo.getProxyInfoAsString", new=Mock(return_value=S_OK()))
  @patch("ILCDIRAC.Workflow.Modules.ModuleBase.Operations", new=Mock())
  @patch("ILCDIRAC.Workflow.Modules.UploadLogFile.StorageElement", new=Mock(return_value=S_OK()))
  @patch("DIRAC.Resources.Storage.StorageElement.StorageElementItem", new=Mock() )
  @patch('DIRAC.ConfigurationSystem.Client.Helpers.Operations.Operations', new=Mock())
  @patch("DIRAC.Resources.Storage.StorageElement.StorageElementItem", new=Mock() )
  @patch("DIRAC.Resources.Storage.StorageFactory.StorageFactory", new=Mock() )
  def setUp( self ): #pylint: disable=R0915
    """Set up the objects"""
    self.curdir = os.getcwd()
    self.tempdir = tempfile.mkdtemp("", dir = "./")
    os.chdir(self.tempdir)

    self.log = gLogger.getSubLogger("MODULEBASE")

    self.prod_id = 123
    self.prod_job_id = 456
    self.wms_job_id = 0
    self.workflowStatus = {'OK':True}
    self.stepStatus = {'OK':True}

    self.jr_mock = Mock()
    self.jr_mock.setApplicationStatus.return_value = {'OK': True, 'Value': ''}
    self.jr_mock.generateRequest.return_value = {'OK': True, 'Value': 'pippo'}
    self.jr_mock.setJobParameter.return_value = {'OK': True, 'Value': 'pippo'}
    self.jr_mock.generateForwardDISET.return_value = {'OK': True, 'Value': 'pippo'}
#    self.jr_mock.setJobApplicationStatus.return_value = {'OK': True, 'Value': 'pippo'}

    self.fr_mock = Mock()
    self.fr_mock.getFiles.return_value = {"/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0000.stdhep":"ApplicationCrash",
                                          "/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0001.stdhep":"ARG"}
    self.fr_mock.setFileStatus.return_value = {'OK': True, 'Value': ''}
    self.fr_mock.commit.return_value = {'OK': True, 'Value': ''}
    self.fr_mock.generateRequest.return_value = {'OK': True, 'Value': ''}

    self.rm_mock = Mock()
    self.rm_mock.getReplicas.return_value = {'OK': True, 'Value':{'Successful':{'pippo':'metadataPippo'},
                                                                  'Failed':None}}
    self.rm_mock.getCatalogFileMetadata.return_value = {'OK': True, 'Value':{'Successful':{'pippo':'metadataPippo'},
                                                                             'Failed':None}}
    self.rm_mock.removeFile.return_value = {'OK': True, 'Value': {'Failed':False}}
    self.rm_mock.putStorageDirectory.return_value = {'OK': True, 'Value': {'Failed':False}}
    self.rm_mock.addCatalogFile.return_value = {'OK': True, 'Value': {'Failed':False}}
    self.rm_mock.putAndRegister.return_value = {'OK': True, 'Value': {'Failed':False}}
    self.rm_mock.getFile.return_value = {'OK': True, 'Value': {'Failed':False}}

    self.rc_mock = Mock(name='RequestContainer')
    self.rc_mock.update.return_value = {'OK': True, 'Value': ''}
    self.rc_mock.setDISETRequest.return_value = {'OK': True, 'Value': ''}
    self.rc_mock.isEmpty.return_value = {'OK': True, 'Value': ''}
    self.rc_mock.toXML.return_value = {'OK': True, 'Value': 'Ex Em El'}
    self.rc_mock.getDigest.return_value = {'OK': True, 'Value': 'Indigestion'}
    self.rc_mock.toJSON.return_value = S_OK("JSON Bieber requests your presence")
    self.rc_mock.__len__.return_value = 1

    self.ar_mock = Mock()
    self.ar_mock.commit.return_value = {'OK': True, 'Value': ''}

    self.wf_commons = [ {'PRODUCTION_ID': str( self.prod_id ), 'JOB_ID': str( self.prod_job_id ), 'eventType': '123456789', 'jobType': 'merge',
                         'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'',
                         'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData', 'numberOfEvents':'100',
                         'JobReport':self.jr_mock, 'Request':self.rc_mock, 'AccountingReport': self.ar_mock, 'FileReport':self.fr_mock,
                         'SystemConfig':'sys_config', 'runNumber':'Unknown', 'appSteps': ['someApp_1']},
                        {'PRODUCTION_ID': str( self.prod_id ), 'JOB_ID': str( self.prod_job_id ),
                         'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'', 'jobType': 'merge',
                         'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData', 'numberOfEvents':'100',
                         'JobReport':self.jr_mock, 'Request':self.rc_mock, 'AccountingReport': self.ar_mock, 'FileReport':self.fr_mock,
                         'SystemConfig':'sys_config', 'LogFilePath':'someDir', 'runNumber':'Unknown',
                         'appSteps': ['someApp_1']},
                        {'PRODUCTION_ID': str( self.prod_id ), 'JOB_ID': str( self.prod_job_id ),
                         'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'', 'jobType': 'merge',
                         'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData', 'numberOfEvents':'100',
                         'JobReport':self.jr_mock, 'Request':self.rc_mock, 'AccountingReport': self.ar_mock, 'FileReport':self.fr_mock,
                         'SystemConfig':'sys_config', 'LogFilePath':'someDir', 'LogTargetPath':'someOtherDir',
                         'runNumber':'Unknown', 'appSteps': ['someApp_1']},
                        {'PRODUCTION_ID': str( self.prod_id ), 'JOB_ID': str( self.prod_job_id ),
                         'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'', 'jobType': 'merge',
                         'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData', 'numberOfEvents':'100',
                         'JobReport':self.jr_mock, 'Request':self.rc_mock, 'AccountingReport': self.ar_mock, 'FileReport':self.fr_mock,
                         'SystemConfig':'sys_config', 'LogFilePath':'someDir', 'LogTargetPath':'someOtherDir',
                         'runNumber':'Unknown', 'appSteps': ['someApp_1'] },
                        {'PRODUCTION_ID': str( self.prod_id ), 'JOB_ID': str( self.prod_job_id ),
                         'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'', 'jobType': 'reco',
                         'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData',
                         'JobReport':self.jr_mock, 'Request':self.rc_mock, 'AccountingReport': self.ar_mock, 'FileReport':self.fr_mock,
                         'SystemConfig':'sys_config', 'runNumber':'Unknown', 'appSteps': ['someApp_1']},
                        {'PRODUCTION_ID': str( self.prod_id ), 'JOB_ID': str( self.prod_job_id ),
                         'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'', 'jobType': 'reco',
                         'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData',
                         'JobReport':self.jr_mock, 'Request':self.rc_mock, 'AccountingReport': self.ar_mock, 'FileReport':self.fr_mock,
                         'SystemConfig':'sys_config', 'LogFilePath':'someDir', 'runNumber':'Unknown',
                         'appSteps': ['someApp_1']},
                        {'PRODUCTION_ID': str( self.prod_id ), 'JOB_ID': str( self.prod_job_id ),
                         'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'', 'jobType': 'reco',
                         'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData',
                         'JobReport':self.jr_mock, 'Request':self.rc_mock, 'AccountingReport': self.ar_mock, 'FileReport':self.fr_mock,
                         'SystemConfig':'sys_config', 'LogFilePath':'someDir', 'LogTargetPath':'someOtherDir',
                         'runNumber':'Unknown', 'appSteps': ['someApp_1']},
                        {'PRODUCTION_ID': str( self.prod_id ), 'JOB_ID': str( self.prod_job_id ),
                         'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'', 'jobType': 'reco',
                         'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData',
                         'JobReport':self.jr_mock, 'Request':self.rc_mock, 'AccountingReport': self.ar_mock, 'FileReport':self.fr_mock,
                         'SystemConfig':'sys_config', 'LogFilePath':'someDir', 'LogTargetPath':'someOtherDir',
                         'runNumber':'Unknown', 'appSteps': ['someApp_1']},
                        {'PRODUCTION_ID': str( self.prod_id ), 'JOB_ID': str( self.prod_job_id ),
                         'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'', 'jobType': 'reco',
                         'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData',
                         'JobReport':self.jr_mock, 'Request':self.rc_mock, 'AccountingReport': self.ar_mock, 'FileReport':self.fr_mock,
                         'SystemConfig':'sys_config', 'LogFilePath':'someDir', 'LogTargetPath':'someOtherDir',
                         'runNumber':'Unknown', 'InputData': '', 'appSteps': ['someApp_1'] },
                        {'PRODUCTION_ID': str( self.prod_id ), 'JOB_ID': str( self.prod_job_id ),
                         'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'', 'jobType': 'reco',
                         'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData',
                         'JobReport':self.jr_mock, 'Request':self.rc_mock, 'AccountingReport': self.ar_mock, 'FileReport':self.fr_mock,
                         'SystemConfig':'sys_config', 'LogFilePath':'someDir', 'LogTargetPath':'someOtherDir',
                         'runNumber':'Unknown', 'InputData': 'foo;bar', 'appSteps': ['someApp_1'] },
                        {'PRODUCTION_ID': str( self.prod_id ), 'JOB_ID': str( self.prod_job_id ),
                         'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'', 'jobType': 'reco',
                         'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData',
                         'JobReport':self.jr_mock, 'Request':self.rc_mock, 'AccountingReport': self.ar_mock, 'FileReport':self.fr_mock,
                         'SystemConfig':'sys_config', 'LogFilePath':'someDir', 'LogTargetPath':'someOtherDir',
                         'runNumber':'Unknown', 'InputData': 'foo;bar', 'ParametricInputData':'' ,
                         'appSteps': ['someApp_1']},
                        {'PRODUCTION_ID': str( self.prod_id ), 'JOB_ID': str( self.prod_job_id ),
                         'configName': 'aConfigName', 'configVersion': 'aConfigVersion', 'outputDataFileMask':'', 'jobType': 'reco',
                         'BookkeepingLFNs':'aa', 'ProductionOutputData':'ProductionOutputData',
                         'JobReport':self.jr_mock, 'Request':self.rc_mock, 'AccountingReport': self.ar_mock, 'FileReport':self.fr_mock,
                         'SystemConfig':'sys_config', 'LogFilePath':'someDir', 'LogTargetPath':'someOtherDir',
                         'runNumber':'Unknown', 'InputData': 'foo;bar', 'ParametricInputData':'pid1;pid2;pid3',
                         'appSteps': ['someApp_1']},
                      ]
    self.step_commons = [ {'applicationName':'someApp', 'applicationVersion':'v1r0', 'eventType': '123456789',
                           'applicationLog':'appLog', 'extraPackages':'', 'XMLSummary':'XMLSummaryFile',
                           'numberOfEvents':'100', 'BKStepID':'123', 'StepProcPass':'Sim123', 'outputFilePrefix':'pref_',
                           'STEP_INSTANCE_NAME':'someApp_1',
                           'listoutput':[{'outputDataName':str( self.prod_id ) + '_' + str( self.prod_job_id ) + '_', 'outputDataSE':'aaa',
                                          'outputDataType':'bbb'}]},
                          {'applicationName':'someApp', 'applicationVersion':'v1r0', 'eventType': '123456789',
                           'applicationLog':'appLog', 'extraPackages':'', 'XMLSummary':'XMLSummaryFile',
                           'numberOfEvents':'100', 'BKStepID':'123', 'StepProcPass':'Sim123', 'outputFilePrefix':'pref_',
                           'optionsLine': '',
                           'STEP_INSTANCE_NAME':'someApp_1',
                           'listoutput':[{'outputDataName':str( self.prod_id ) + '_' + str( self.prod_job_id ) + '_', 'outputDataSE':'aaa',
                                          'outputDataType':'bbb'}]},
                          {'applicationName':'someApp', 'applicationVersion':'v1r0', 'eventType': '123456789',
                           'applicationLog':'appLog', 'extraPackages':'', 'XMLSummary':'XMLSummaryFile',
                           'numberOfEvents':'100', 'BKStepID':'123', 'StepProcPass':'Sim123', 'outputFilePrefix':'pref_',
                           'extraOptionsLine': 'blaBla',
                           'STEP_INSTANCE_NAME':'someApp_1',
                           'listoutput':[{'outputDataName':str( self.prod_id ) + '_' + str( self.prod_job_id ) + '_', 'outputDataSE':'aaa',
                                          'outputDataType':'bbb'}]}
                        ]
    self.step_number = '321'
    self.step_id = '%s_%s_%s' % ( self.prod_id, self.prod_job_id, self.step_number )

    self.mbase = ModuleBase()
    self.mbase.rm = self.rm_mock
    self.mbase.request = self.rc_mock
    self.mbase.jobReport = self.jr_mock
    self.mbase.fileReport = self.fr_mock
    self.mbase.workflow_commons = self.wf_commons[0]
    self.mbase.workflow_commons['LogFilePath'] = "/ilc/user/s/sailer/test/dummy/folder"
    self.mbase.workflow_commons['Platform'] = "x86_64-slc5-gcc43-opt"
    self.mbase.log = gLogger.getSubLogger("ModuleBaseTest")
    self.mbase.log.showHeaders(True)
    self.mbase.ignoreapperrors = False
    self.uod = UploadOutputData()
    self.uod.workflow_commons = self.mbase.workflow_commons

    self.ulf = UploadLogFile()

    ### create some dummy files
    for i in xrange(0,8):
      path="h_nunu_gen_4191_000%s.stdhep" % str(i)
      with open(path, 'a'):
        pass
    path="test3.stdhep"
    with open(path, 'a'):
      pass
    try:
      os.makedirs("myILDConfig")
    except OSError:
      pass

  def tearDown( self ):
    os.chdir(self.curdir)
    cleanup(self.tempdir)

@patch("DIRAC.Core.Security.ProxyInfo.getProxyInfoAsString", new=Mock(return_value=S_OK()))
@patch("ILCDIRAC.Workflow.Modules.ModuleBase.getProxyInfoAsString", new=Mock(return_value=S_OK()))
class TestModuleBase( ModulesTestCase ):
  """ Tests for ModuleBase functions"""

  def test_generateFailoverFile( self ):
    """run the generateFailoverFile function and see what happens..................................."""
    with patch("ILCDIRAC.Workflow.Modules.ModuleBase.RequestValidator", Mock() ):
      dummy_res = self.mbase.generateFailoverFile()
    #print res

  def test_CreateRemoveRequest( self ):
    """ModuleBase: Create a removal request for some LFN............................................"""
    gLogger.setLevel("ERROR")
    lfnList = ['/ilc/user/s/sailer/2014_11/12/12345/testsim.slcio','/ilc/user/s/sailer/2014_11/12/12345/testsim.slcio']
    mob = ModuleBase()
    mob.workflow_commons = dict()
    mob.jobID = 444444
    mob.addRemovalRequests(lfnList)
    request = mob.workflow_commons['Request']
    mob.log.notice(request)
    self.assertEqual( len(request), 1 )

  def test_MB_getCandidateFiles( self ):
    """ModuleBase: getCandidateFiles: files exist..................................................."""
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
    result = self.mbase.getCandidateFiles(outputList, outputLFNs, dummy_fileMask)
    print result
    resDict = [ os.path.basename(lfn) in result['Value'] for lfn in outputLFNs ]
    gLogger.debug("Result: %s" % result)
    gLogger.debug("ResDict: %s" % resDict)
    self.assertTrue( all(resDict) )

  def test_MB_getCandidateFiles_FileNotFound( self ):
    """ModuleBase: getCandidateFiles: No Such File.................................................."""
    gLogger.setLevel("ERROR")
    outputList = {'h_nunu_gen_4191_NSF': {'outputPath': '/ilc/prod/clic/1.4tev/h_nunu/gen', 'outputFile': 'h_nunu_gen_4191_NSF.stdhep', 'outputDataSE': 'CERN-SRM'}}.values()
    outputLFNs = ['/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_NSF.stdhep']
    dummy_fileMask = None
    result = self.mbase.getCandidateFiles(outputList, outputLFNs, dummy_fileMask)
    self.assertIn( "Output Data Not Found", result['Message'] )

  def test_MB_getCandidateFiles_FileTooLong( self ):
    """ModuleBase: getCandidateFiles: File Too Long................................................."""
    gLogger.setLevel("ERROR")
    outputList = {'a'*128: {'outputPath': '/ilc/prod/clic/1.4tev/h_nunu/gen', 'outputFile': 'a'*128, 'outputDataSE': 'CERN-SRM'}}.values()
    outputLFNs = ['/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/'+'a'*128]
    dummy_fileMask = None
    result = self.mbase.getCandidateFiles(outputList, outputLFNs, dummy_fileMask)
    self.assertIn( "Filename too long", result['Message'] )

  def test_MB_getCandidateFiles_PathTooLong( self ):
    """ModuleBase: getCandidateFiles: Path Too Long................................................."""
    gLogger.setLevel("ERROR")
    outputList = {'a'*127: {'outputPath': '/bbbbbbbbbb'*26, 'outputFile': 'a'*127, 'outputDataSE': 'CERN-SRM'}}.values()
    outputLFNs = ['/bbbbbbbbbb'*26+'/'+'a'*127]
    dummy_fileMask = None
    result = self.mbase.getCandidateFiles(outputList, outputLFNs, dummy_fileMask)
    self.assertIn( "LFN too long", result['Message'] )

  def test_MB_logWorkingDirectory( self ):
    """ModuleBase: logWorkingDirectory.............................................................."""
    gLogger.setLevel("ERROR")
    self.mbase.logWorkingDirectory()

  def test_MB_redirectLogOutput_1( self ):
    """ModuleBase: redirectLogOutput 1.............................................................."""
    gLogger.setLevel("ERROR")
    self.mbase.eventstring = "+++ Event String"
    message = "+++ Event String 123"
    out = StringIO()
    sys.stdout = out
    with open("logFile", "w") as fd:
      self.mbase.redirectLogOutput(fd, message)
    self.assertEqual( message, out.getvalue().strip().splitlines()[0] )

  def test_MB_redirectLogOutput_2( self ):
    """ModuleBase: redirectLogOutput 2.............................................................."""
    gLogger.setLevel("ERROR")
    self.mbase.eventstring = "+++ Event String"
    self.mbase.applicationLog = "grailDiary.log"
    message = ["+++ Event String 123","andSomeOtherString"]
    out = StringIO()
    sys.stdout = out
    with open("logFile", "w") as fd:
      for mes in message:
        self.mbase.redirectLogOutput(fd, mes)
    with open(self.mbase.applicationLog, "r") as logF:
      self.assertEqual( logF.read().strip(), "\n".join(message) )
    self.assertEqual( message[0], out.getvalue().strip().splitlines()[0] )

  def test_MB_redirectLogOutput_3( self ):
    """ModuleBase: redirectLogOutput 3.............................................................."""
    gLogger.setLevel("ERROR")
    self.mbase.eventstring = "+++ Event String"
    self.mbase.applicationLog = "grailDiary.log"
    self.mbase.excludeAllButEventString = True
    message = ["+++ Event String 123","andSomeOtherString"]
    out = StringIO()
    sys.stdout = out
    with open("logFile", "w") as fd:
      for mes in message:
        self.mbase.redirectLogOutput(fd, mes)
    with open(self.mbase.applicationLog, "r") as logF:
      self.assertEqual( logF.read().strip(), message[0] )
    self.assertEqual( message[0], out.getvalue().strip().splitlines()[0] )

  def test_MB_redirectLogOutput_4( self ):
    """ModuleBase: redirectLogOutput 4.............................................................."""
    gLogger.setLevel("ERROR")
    self.mbase.eventstring = ""
    self.mbase.applicationLog = "grailDiary.log"
    self.mbase.excludeAllButEventString = True
    message = ["+++ Event String 123","andSomeOtherString"]
    out = StringIO()
    sys.stdout = out
    with open("logFile", "w") as fd:
      for mes in message:
        self.mbase.redirectLogOutput(fd, mes)
    with open(self.mbase.applicationLog, "r") as logF:
      self.assertEqual( logF.read().strip(), "" )
    self.assertEqual( "", out.getvalue().strip() )

  def test_MB_redirectLogOutput_noMes( self ):
    """ModuleBase: redirectLogOutput no message....................................................."""
    gLogger.setLevel("ERROR")
    self.mbase.eventstring = "+++ Event String"
    self.mbase.applicationLog = "grailDiary.log"
    message = ""
    out = StringIO()
    sys.stdout = out
    with open("logFile", "w") as fd:
      self.mbase.redirectLogOutput(fd, message)
    self.assertFalse( os.path.exists( self.mbase.applicationLog ) )
    self.assertEqual( "", out.getvalue().strip() )

  def test_MB_redirectLogOutput_noES( self ):
    """ModuleBase: redirectLogOutput no eventstring................................................."""
    gLogger.setLevel("ERROR")
    self.mbase.eventstring = []
    self.mbase.applicationLog = "grailDiary.log"
    message = "some message"
    out = StringIO()
    sys.stdout = out
    with open("logFile", "w") as fd:
      self.mbase.redirectLogOutput(fd, message)
    with open(self.mbase.applicationLog, "r") as logF:
      self.assertEqual( logF.read().strip(), message )
    self.assertEqual( "", out.getvalue().strip() )

  def test_MB_redirectLogOutput_noES_2( self ):
    """ModuleBase: redirectLogOutput no eventstring 2..............................................."""
    gLogger.setLevel("ERROR")
    self.mbase.eventstring = ''
    self.mbase.applicationLog = "grailDiary.log"
    message = "some message"
    out = StringIO()
    sys.stdout = out
    with open("logFile", "w") as fd:
      self.mbase.redirectLogOutput(fd, message)
    with open(self.mbase.applicationLog, "r") as logF:
      self.assertEqual( logF.read().strip(), message )
    self.assertEqual( "", out.getvalue().strip() )

  def test_MB_redirectLogOutput_ESNone( self ):
    """ModuleBase: redirectLogOutput eventstring is None............................................"""
    gLogger.setLevel("ERROR")
    self.mbase.eventstring = None
    self.mbase.applicationLog = "grailDiary.log"
    message = ["some message", "and some other message"]
    out = StringIO()
    sys.stdout = out
    with open("logFile", "w") as fd:
      for mes in message:
        self.mbase.redirectLogOutput(fd, mes)
    with open(self.mbase.applicationLog, "r") as logF:
      self.assertEqual( logF.read().strip().splitlines(), message )
    self.assertEqual( message, out.getvalue().strip().splitlines() )

  def test_MB_treatILDConfigPackage( self ):
    """ModuleBase: treatILDConfigPackage............................................................"""
    gLogger.setLevel("ERROR")
    self.mbase.platform = self.mbase.workflow_commons.get('Platform', self.mbase.platform)
    self.mbase.workflow_commons['ILDConfigPackage'] = "ILDConfigv01-16-p03"
    with patch( "ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation.checkCVMFS",
                Mock( return_value=S_OK(("myILDConfig", "init.sh"))) #needs tuple
              ):
      res = self.mbase.treatILDConfigPackage()
    self.assertTrue(res['OK'])

#############################################################################
# UploadLogFile.py
#############################################################################

@patch("DIRAC.Core.Security.ProxyInfo.getProxyInfoAsString", new=Mock(return_value=S_OK()))
@patch("DIRAC.Resources.Storage.StorageElement.StorageElementItem", new=Mock() )
@patch("DIRAC.Resources.Storage.StorageFactory.StorageFactory", new=Mock() )
@patch("ILCDIRAC.Workflow.Modules.ModuleBase.getProxyInfoAsString", new=Mock(return_value=S_OK()))
@patch("ILCDIRAC.Workflow.Modules.UploadLogFile.StorageElement", new=Mock(return_value=S_OK()))
@patch('DIRAC.ConfigurationSystem.Client.Helpers.Operations.Operations', new=Mock())
class TestUploadLogFile( ModulesTestCase ):
  """ test UploadLogFile """

  @patch("ILCDIRAC.Workflow.Modules.ModuleBase.getProxyInfoAsString", new=Mock(return_value=S_OK()))
  @patch("DIRAC.Core.Security.ProxyInfo.getProxyInfoAsString", new=Mock(return_value=S_OK()))
  def setUp( self ):
    """create logfile"""
    super(TestUploadLogFile, self).setUp()
    with open("MyLogFile.log", "w") as logFile:
      logFile.write("soemthing")
    with open("MyOtherLogFile.log", "w") as logFile:
      logFile.write("soemthing")
    try:
      os.makedirs( "./my/log/folder" )
    except OSError:
      pass
    with open("./my/log/folder/MyLogFile.log", "w") as logFile:
      logFile.write("something else")

  def test_ULF_ASI_NoLogFiles( self ):
    """ULF.applicationSpecificInputs: no log files present.........................................."""
    self.ulf = UploadLogFile()
    self.ulf.workflow_commons = copy.deepcopy(self.mbase.workflow_commons)
    self.ulf.log = gLogger.getSubLogger("ULF-NoLogFiles")
    self.ulf.log.setLevel("INFO")

    self.ulf.resolveInputVariables = Mock(return_value=S_OK())
    self.ulf._determineRelevantFiles = Mock(return_value=S_OK([]))
    self.ulf.applicationSpecificInputs()
    res = self.ulf.execute()
    self.assertTrue( res['OK'] )

  def test_ULF_ASI_enableNotBool( self ):
    """ULF.applicationSpecificInputs: enable is not boolean........................................."""
    self.ulf = UploadLogFile()
    self.ulf.workflow_commons = copy.deepcopy(self.mbase.workflow_commons)
    self.ulf.enable = "notboolean"
    self.ulf.failoverTest = "notboolean"
    self.ulf.jobID = 0
    self.ulf.log = gLogger.getSubLogger("ULF-NoLogFiles")
    self.ulf.log.setLevel("INFO")
    self.ulf.resolveInputVariables = Mock(return_value=S_OK())
    self.ulf._determineRelevantFiles = Mock(return_value=S_OK([]))
    self.ulf.applicationSpecificInputs()
    res = self.ulf.execute()
    self.assertTrue( res['OK'] )

  @patch("ILCDIRAC.Workflow.Modules.UploadLogFile.getLogPath", new=Mock(return_value=S_ERROR("not the path you are looking for") ))
  def test_ULF_ASI_noLogPath( self ):
    """ULF.applicationSpecificInputs: getLogPath fails.............................................."""
    self.ulf = UploadLogFile()
    self.ulf.workflow_commons = copy.deepcopy(self.mbase.workflow_commons)
    self.ulf.workflow_commons.pop("LogFilePath", None)
    self.ulf.workflow_commons.pop("LogTargetPath", None)
    self.ulf.log = gLogger.getSubLogger("ULF-NoLogFiles")
    self.ulf.log.setLevel("INFO")
    self.ulf.resolveInputVariables = Mock(return_value=S_OK())
    self.ulf._determineRelevantFiles = Mock(return_value=S_OK([]))
    res = self.ulf.applicationSpecificInputs()
    self.assertEqual( res['Message'], "not the path you are looking for" )

  def test_ULF_ASI_ListLogPath( self ):
    """ULF.applicationSpecificInputs: getLogPath does not return strings............................"""
    self.ulf = UploadLogFile()
    self.ulf.log = gLogger.getSubLogger("ULF-NoLogFiles")
    self.ulf.log.setLevel("INFO")
    self.ulf.logFilePath = None
    self.ulf.logTargetPath = None
    self.ulf.workflow_commons = copy.deepcopy(self.mbase.workflow_commons)
    self.ulf.workflow_commons["LogFilePath"] = ["/ilc/prod/ilc/sid", "path"]
    self.ulf.workflow_commons["LogTargetPath"] = ["/ilc/prod/clic/log.tar.gz", "path"]
    self.ulf.resolveInputVariables = Mock(return_value=S_OK())
    self.ulf._determineRelevantFiles = Mock(return_value=S_OK([]))
    _res = self.ulf.applicationSpecificInputs()
    self.assertEqual( self.ulf.experiment, "ILC_SID" )

  def test_ULF_ASI_expClic( self ):
    """ULF.applicationSpecificInputs: experiment CLIC..............................................."""
    self.ulf = UploadLogFile()
    self.ulf.workflow_commons = copy.deepcopy(self.mbase.workflow_commons)
    self.ulf.workflow_commons["LogFilePath"] = "/ilc/prod/clic"
    self.ulf.workflow_commons["LogTargetPath"] = "/ilc/prod/clic/log.tar.gz"
    self.ulf.resolveInputVariables = Mock(return_value=S_OK())
    self.ulf._determineRelevantFiles = Mock(return_value=S_OK([]))
    _res = self.ulf.applicationSpecificInputs()
    self.assertEqual(self.ulf.experiment, "CLIC" )

  def test_ULF_ASI_expSid( self ):
    """ULF.applicationSpecificInputs: experiment Sid................................................"""
    self.ulf = UploadLogFile()
    self.ulf.log = gLogger.getSubLogger("ULF-ExpChecks")
    self.ulf.log.setLevel("DEBUG")
    self.ulf.workflow_commons = copy.deepcopy(self.mbase.workflow_commons)
    self.ulf.workflow_commons["LogFilePath"] = "/ilc/prod/ilc/sid/prodID/gen"
    self.ulf.workflow_commons["LogTargetPath"] = "/ilc/prod/ilc/sid/log.tar.gz"
    self.ulf.resolveInputVariables = Mock(return_value=S_OK())
    self.ulf._determineRelevantFiles = Mock(return_value=S_OK([]))
    _res = self.ulf.applicationSpecificInputs()
    self.assertEqual(self.ulf.experiment, "ILC_SID" )

  def test_ULF_ASI_expILD( self ):
    """ULF.applicationSpecificInputs: experiment ILD................................................"""
    self.ulf = UploadLogFile()
    self.ulf.workflow_commons = copy.deepcopy(self.mbase.workflow_commons)
    self.ulf.log = gLogger.getSubLogger("ULF-NoLogFiles")
    self.ulf.log.setLevel("INFO")
    self.ulf.workflow_commons["LogFilePath"] = "/ilc/prod/ilc/mc-dbd"
    self.ulf.workflow_commons["LogTargetPath"] = "/ilc/prod/ilc/mc-dbd/log.tar.gz"
    self.ulf.resolveInputVariables = Mock(return_value=S_OK())
    self.ulf._determineRelevantFiles = Mock(return_value=S_OK([]))
    _res = self.ulf.applicationSpecificInputs()
    self.assertEqual(self.ulf.experiment, "ILC_ILD")

  def test_ULF_ASI_OneLogFile( self ):
    """ULF.applicationSpecificInputs: one log files present........................................."""
    self.ulf = UploadLogFile()
    self.ulf.log = gLogger.getSubLogger("ULF-OneLogFile")
    self.ulf.workflow_commons = copy.deepcopy(self.wf_commons[0])
    self.ulf._determineRelevantFiles = Mock(return_value=S_OK(['MyLogFile.log']))
    self.ulf.logSE = Mock()
    self.ulf.logSE.putFile = Mock(return_value=S_OK(dict(Failed=['MyLogFiles.tar.gz'],Message="Ekke Ekke Ekke Ekke")))
    self.ulf.logLFNPath = getLogPath(self.ulf.workflow_commons)['Value']['LogTargetPath'][0]
    self.ulf._tryFailoverTransfer = Mock(return_value = S_OK({'Request': self.rc_mock,
                                                              'uploadedSE': 'CERN-SRM'}))
    self.ulf.applicationSpecificInputs()
    res = self.ulf.execute()
    self.assertTrue( res['OK'] )

  def test_ULF_ASI_FailedFailover( self ):
    """ULF.applicationSpecificInputs: Failovertransfer fails........................................"""
    gLogger.setLevel("ERROR")
    self.ulf = UploadLogFile()
    self.ulf.log = gLogger.getSubLogger("ULF-OneLogFile")
    self.ulf.workflow_commons = copy.deepcopy(self.wf_commons[0])
    self.ulf._determineRelevantFiles = Mock(return_value=S_OK(['MyLogFile.log']))
    self.ulf.logSE = Mock()
    self.ulf.logSE.putFile = Mock(return_value=S_OK(dict(Failed=['MyLogFiles.tar.gz'],Message="Ekke Ekke Ekke Ekke")))
    self.ulf.logLFNPath = getLogPath(self.ulf.workflow_commons)['Value']['LogTargetPath'][0]
    self.ulf._tryFailoverTransfer = Mock(return_value = S_OK())
    self.ulf.applicationSpecificInputs()
    res = self.ulf.execute()
    self.assertTrue( res['OK'] )

  def test_ULF_ASI_LogFileGone( self ):
    """ULF.applicationSpecificInputs: log file disappeared, IOError................................."""
    self.ulf = UploadLogFile()
    self.ulf.workflow_commons = copy.deepcopy(self.wf_commons[0])
    self.ulf.log = gLogger.getSubLogger("ULF-LogFileGone")

    self.ulf.logLFNPath = getLogPath(self.ulf.workflow_commons)['Value']['LogTargetPath'][0]
    self.ulf._determineRelevantFiles = Mock(return_value=S_OK(['std.out']))
    self.assertRaises( IOError, self.ulf.execute )

  def test_ULF_ASI_execute( self ):
    """ULF.ASI,Exe: run through and get request....................................................."""
    self.ulf.workflow_commons = copy.deepcopy(self.wf_commons[0])
    self.ulf.log = gLogger.getSubLogger("ULF-RequestTest")
    self.ulf.jobID = 12345
    self.ulf._determineRelevantFiles = Mock(return_value=S_OK(['MyLogFile.log','MyOtherLogFile.log']))
    self.ulf.logSE = Mock()
    self.ulf.logSE.putFile = Mock(return_value=S_OK(dict(Failed=['MyLogFile.log', 'MyOtherLogFile.log'],
                                                         Message="Ekke Ekke Ekke Ekke")))
    self.mbase.workflow_commons['Request']  = Request()
    self.mbase.workflow_commons['Request'].RequestName = "MockingRequest"
    self.ulf._tryFailoverTransfer = Mock(return_value = S_OK({'Request': self.mbase.workflow_commons['Request'],
                                                              'uploadedSE': 'CERN-SRM'}))
    self.ulf._getRequestContainer = self.rc_mock
    self.ulf.logLFNPath = getLogPath(self.ulf.workflow_commons)['Value']['LogTargetPath'][0]
    self.ulf.applicationSpecificInputs()
    res = self.ulf.execute()
    self.assertTrue( res['OK'] )

  def test_ULF_finalize_move( self ):
    """ULF.Finalize: move to new folder............................................................."""
    gLogger.setLevel("ERROR")
    with patch("DIRAC.Resources.Storage.StorageElement.StorageElementItem", Mock() ):
      self.ulf = UploadLogFile()
    self.ulf.logSE = Mock()
    self.ulf.workflow_commons = copy.deepcopy(self.mbase.workflow_commons)
    self.ulf.log = gLogger.getSubLogger("ULF-FinalMove")
    self.ulf.jobID = 12345
    self.ulf._determineRelevantFiles = Mock(return_value=S_OK(['MyLogFile.log','MyOtherLogFile.log']))
    #self.ulf.logSE.putFile = Mock(return_value=S_OK(dict(Failed=['MyLogFiles.tar.gz'],
    #                                                     Message="Ekke Ekke Ekke Ekke")))
    self.mbase.workflow_commons['Request']  = self.rc_mock
    self.ulf._tryFailoverTransfer = Mock(return_value = S_OK({'Request': self.mbase.workflow_commons['Request'],
                                                              'uploadedSE': 'CERN-SRM'}))
    self.ulf.logLFNPath = getLogPath(self.ulf.workflow_commons)['Value']['LogTargetPath'][0]
    self.ulf.logFilePath = self.ulf.workflow_commons['LogFilePath']
    self.ulf.logdir = os.path.realpath("./my/log/folder")
    #self.ulf.execute()
    res = self.ulf.finalize()
    self.assertTrue( res['OK'] )

  def test_ULF__determineRelevantFiles_1( self ):
    """ULF._determineRelevantFiles 1................................................................"""
    self.ulf = UploadLogFile()
    res = self.ulf._determineRelevantFiles()
    self.assertTrue( res['OK'] )

  @patch('DIRAC.ConfigurationSystem.Client.Helpers.Operations.Operations.getValue', new=Mock(return_value=["*.root"]) )
  def test_ULF__determineRelevantFiles_2( self ):
    """ULF._determineRelevantFiles 2................................................................"""
    self.ulf = UploadLogFile()
    _res = self.ulf._determineRelevantFiles()
    self.assertEqual( self.ulf.logExtensions, ["*.root"] )

  @patch('DIRAC.ConfigurationSystem.Client.Helpers.Operations.Operations.getValue', new=Mock(return_value=["FAIL-SRM"]))
  @patch('ILCDIRAC.Workflow.Modules.UploadLogFile.FailoverTransfer', new=Mock() )
  def test_ULF__tryFailoverTransfer_1( self ):
    """ULF._tryFailoverTransfer 1..................................................................."""
    self.ulf = UploadLogFile()
    self.ulf.workflow_commons = copy.deepcopy(self.mbase.workflow_commons)
    self.ulf.workflow_commons['Request']  = self.rc_mock
    _res = self.ulf._tryFailoverTransfer("log.tar.gz","myLogDir")
    self.assertEqual( self.ulf.failoverSEs, ["FAIL-SRM"] )

  def test_ULF__tryFailoverTransfer_2( self ):
    """ULF._tryFailoverTransfer 2..................................................................."""

    from DIRAC.DataManagementSystem.Client.FailoverTransfer import FailoverTransfer
    FailoverTransfer.transferAndRegisterFile = Mock(return_value=S_ERROR("IT ACTUALLY WORKS!!!!!!1eleven!!"))
    self.ulf = UploadLogFile()
    self.ulf.workflow_commons = copy.deepcopy(self.mbase.workflow_commons)
    self.ulf.workflow_commons['Request']  = self.rc_mock
    res = self.ulf._tryFailoverTransfer("log.tar.gz","myLogDir")
    self.assertEqual( res['Value'], None )

#############################################################################
# UploadOutputData.py
#############################################################################

@patch("DIRAC.Core.Security.ProxyInfo.getProxyInfoAsString", new=Mock(return_value=S_OK()))
@patch("ILCDIRAC.Workflow.Modules.ModuleBase.getProxyInfoAsString", new=Mock(return_value=S_OK()))
class UploadOutputDataSuccess( ModulesTestCase ):
  """ test UploadLogFile """
  def test_execute( self ):
    """tests execute function......................................................................."""
    pass

@patch("DIRAC.Core.Security.ProxyInfo.getProxyInfoAsString", new=Mock(return_value=S_OK()))
@patch("ILCDIRAC.Workflow.Modules.ModuleBase.getProxyInfoAsString", new=Mock(return_value=S_OK()))
class UploadOutputDataFailure( ModulesTestCase ):
  """ test UploadLogFile """
  def test_execute( self ):
    """tests execute function......................................................................."""
    pass

#############################################################################
# FailoverRequest.py
#############################################################################
@patch("DIRAC.Core.Security.ProxyInfo.getProxyInfoAsString", new=Mock(return_value=S_OK()))
@patch("DIRAC.Resources.Storage.StorageElement.StorageElementItem", new=Mock() )
@patch("DIRAC.Resources.Storage.StorageFactory.StorageFactory", new=Mock() )
@patch("ILCDIRAC.Workflow.Modules.ModuleBase.getProxyInfoAsString", new=Mock(return_value=S_OK()))
@patch("ILCDIRAC.Workflow.Modules.UploadLogFile.StorageElement", new=Mock(return_value=S_OK()))
@patch('DIRAC.ConfigurationSystem.Client.Helpers.Operations.Operations', new=Mock())
class TestFailoverRequest( ModulesTestCase ):
  """ test UploadLogFile """

  @patch("ILCDIRAC.Workflow.Modules.ModuleBase.getProxyInfoAsString", new=Mock(return_value=S_OK()))
  @patch("DIRAC.Core.Security.ProxyInfo.getProxyInfoAsString", new=Mock(return_value=S_OK()))
  def setUp( self ):
    super(TestFailoverRequest, self).setUp()
    self.frq = None

  @patch("DIRAC.RequestManagementSystem.private.RequestValidator.RequestValidator", new=Mock(return_value=S_OK()) )
  @patch("DIRAC.RequestManagementSystem.private.RequestValidator.RequestValidator.validate", new=Mock(return_value=S_OK()) )
  def test_ASI_Enabled( self ):
    """applicationSpecificInputs: control flag is enabled..........................................."""
    gLogger.setLevel("ERROR")
    self.frq = FailoverRequest()
    self.frq.workflow_commons = dict( )
    self.frq.log = gLogger.getSubLogger("testASI")
    os.environ['JOBID']="12345"
    self.frq.applicationSpecificInputs()
    del os.environ['JOBID']
    self.assertTrue ( self.frq.enable )

  @patch("DIRAC.RequestManagementSystem.private.RequestValidator.RequestValidator", new=Mock(return_value=S_OK()) )
  @patch("DIRAC.RequestManagementSystem.private.RequestValidator.RequestValidator.validate", new=Mock(return_value=S_OK()) )
  def test_ASI_Disable( self ):
    """applicationSpecificInputs: control flag is enabled with non boolean.........................."""
    gLogger.setLevel("ERROR")
    self.frq = FailoverRequest()
    self.frq.workflow_commons = dict( )
    self.frq.log = gLogger.getSubLogger("testASI")
    os.environ['JOBID']="12345"
    self.frq.step_commons = dict( Enable = "arg")
    self.frq.applicationSpecificInputs()
    del os.environ['JOBID']
    self.assertFalse ( self.frq.enable )

  def test_ASI_Disabled( self ):
    """applicationSpecificInputs: control flag is disabled.........................................."""
    gLogger.setLevel("ERROR")
    self.frq = FailoverRequest()
    self.frq.workflow_commons = dict( )
    self.frq.log = gLogger.getSubLogger("testASI")

    self.frq.applicationSpecificInputs()
    self.assertTrue ( self.frq.enable == False )

  def test_ASI_AllVariables( self ):
    """applicationSpecificInputs: checks if all variables have been properly set after this call...."""
    gLogger.setLevel("ERROR")
    self.frq = FailoverRequest()
    self.frq.workflow_commons = dict( JobReport = self.jr_mock, FileReport = self.fr_mock, PRODUCTION_ID=43321, JOB_ID = 12345 )
    os.environ['JOBID']="12345"
    self.frq.applicationSpecificInputs()
    del os.environ['JOBID']
    self.assertTrue( self.frq.jobReport and self.frq.fileReport and
                     self.frq.productionID and self.frq.prodJobID and self.frq.enable )

  def test_ASI_NoVariables( self ):
    """applicationSpecificInputs: checks that no variables have been set after this call............"""
    gLogger.setLevel("ERROR")
    self.frq = FailoverRequest()
    self.frq.workflow_commons = dict()
    os.environ['JOBID']="12345"
    self.frq.applicationSpecificInputs()
    del os.environ['JOBID']
    self.assertFalse( self.frq.jobReport or self.frq.fileReport or
                      self.frq.productionID or self.frq.prodJobID )

  def test_Exe_Disabled( self ):
    """execute: is disabled........................................................................."""
    gLogger.setLevel("ERROR")
    self.frq = FailoverRequest()
    self.frq._getJobReporter = Mock(return_value=self.jr_mock)
    self.frq.log = gLogger.getSubLogger("Frq-Exe-Disabled")
    self.frq.workflow_commons = dict( JobReport = self.jr_mock, FileReport = self.fr_mock, PRODUCTION_ID=43321, JOB_ID = 12345 )
    self.frq.enable = False
    self.frq.workflow_commons = dict( )
    res = self.frq.execute()
    self.assertIn( "Module is disabled",  res['Value'] )

  def test_Exe_WFFail( self ):
    """execute: WF Failed..........................................................................."""
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
    """execute: WF Failed..........................................................................."""
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
    """execute: succeeds............................................................................"""
    gLogger.setLevel("ERROR")
    with patch("ILCDIRAC.Workflow.Modules.ModuleBase.RequestValidator", Mock() ):
      self.frq = FailoverRequest()
    self.frq.log = gLogger.getSubLogger("Frq-Exe-Succeed")
    self.frq.applicationSpecificInputs = Mock(return_value=S_OK())
    self.frq.jobID = 12345
    self.frq.workflow_commons = dict( JobReport = self.jr_mock, FileReport = self.fr_mock, PRODUCTION_ID=43321, JOB_ID = 12345 )
    self.frq.workflow_commons['Request'] = self.rc_mock
    with patch("ILCDIRAC.Workflow.Modules.ModuleBase.RequestValidator", Mock() ):
      res = self.frq.execute()
    self.assertTrue( res['OK'] )

  def test_Exe_Success_input( self ):
    """execute: succeeds inputdata.................................................................."""
    gLogger.setLevel("ERROR")
    with patch("ILCDIRAC.Workflow.Modules.ModuleBase.RequestValidator", Mock() ):
      self.frq = FailoverRequest()
    self.frq.fileReport = self.fr_mock
    self.frq.log = gLogger.getSubLogger("Frq-Exe-Succeed")
    self.frq.InputData = ["/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0000.stdhep"]
    self.frq.productionID = 43321
    self.frq.applicationSpecificInputs = Mock(return_value=S_OK())
    self.frq.jobID = 12345
    self.frq.workflow_commons = dict( JobReport = self.jr_mock, FileReport = self.fr_mock, PRODUCTION_ID=43321, JOB_ID = 12345 )
    self.frq.workflow_commons['Request'] = self.rc_mock
    with patch("ILCDIRAC.Workflow.Modules.ModuleBase.RequestValidator", Mock() ):
      res = self.frq.execute()
    self.assertTrue( res['OK'] )

  def test_Exe_Success_input_no( self ):
    """execute: succeeds inputdata.................................................................."""
    gLogger.setLevel("ERROR")
    with patch("ILCDIRAC.Workflow.Modules.ModuleBase.RequestValidator", Mock() ):
      self.frq = FailoverRequest()
    self.frq.fileReport = self.fr_mock
    self.frq.log = gLogger.getSubLogger("Frq-Exe-Succeed")
    self.frq.InputData = ["/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0000.stdhep",
                          "/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0001.stdhep",
                          "/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0002.stdhep",
                          "/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0003.stdhep",
                         ]
    self.frq.productionID = 43321
    self.frq.applicationSpecificInputs = Mock(return_value=S_OK())
    self.frq.jobID = 12345
    self.frq.workflow_commons = dict( JobReport = self.jr_mock, FileReport = self.fr_mock, PRODUCTION_ID=43321, JOB_ID = 12345 )
    self.frq.workflow_commons['Request'] = self.rc_mock
    with patch("ILCDIRAC.Workflow.Modules.ModuleBase.RequestValidator", Mock() ):
      res = self.frq.execute()
    self.assertTrue( res['OK'] )

  def test_Exe_statusFalse( self ):
    """execute: workflowStatus not OK..............................................................."""
    gLogger.setLevel("ERROR")
    with patch("ILCDIRAC.Workflow.Modules.ModuleBase.RequestValidator", Mock() ):
      self.frq = FailoverRequest()
    self.frq.fileReport = self.fr_mock
    self.frq.log = gLogger.getSubLogger("Frq-Exe-Succeed")
    self.frq.InputData = ["/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0000.stdhep",
                          "/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0001.stdhep",
                          "/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0002.stdhep",
                          "/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0003.stdhep",
                         ]
    self.frq.productionID = 43321
    self.frq.applicationSpecificInputs = Mock(return_value=S_OK())
    self.frq.jobID = 12345
    self.frq.workflowStatus['OK'] = False
    self.frq.workflow_commons = dict( JobReport = self.jr_mock, FileReport = self.fr_mock, PRODUCTION_ID=43321, JOB_ID = 12345 )
    self.frq.workflow_commons['Request'] = self.rc_mock
    with patch("ILCDIRAC.Workflow.Modules.ModuleBase.RequestValidator", Mock() ):
      res = self.frq.execute()
    self.assertTrue( not res['OK'] )

  def test_Exe_statusFalse_diset_err( self ):
    """execute: workflowStatus not OK diset error..................................................."""
    gLogger.setLevel("ERROR")
    with patch("ILCDIRAC.Workflow.Modules.ModuleBase.RequestValidator", Mock() ):
      self.frq = FailoverRequest()
    self.frq.log = gLogger.getSubLogger("Frq-Exe-Succeed")
    self.frq.InputData = ["/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0000.stdhep",
                          "/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0001.stdhep",
                          "/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0002.stdhep",
                          "/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0003.stdhep",
                         ]
    self.frq.productionID = 43321
    self.frq.applicationSpecificInputs = Mock(return_value=S_OK())
    self.frq.jobID = 12345
    self.frq.workflowStatus['OK'] = False
    self.frq.jobType = "NotSplit"
    fr_mock = Mock(self.fr_mock)
    fr_mock.commit.return_value = S_ERROR()
    fr_mock.generateForwardDISET = Mock( return_value=S_ERROR("EKKE") )
    self.frq.fileReport = fr_mock
    self.frq.workflow_commons = dict( JobReport = self.jr_mock, FileReport = fr_mock, PRODUCTION_ID=43321, JOB_ID = 12345 )
    self.frq.workflow_commons['Request'] = self.rc_mock
    with patch("ILCDIRAC.Workflow.Modules.ModuleBase.RequestValidator", Mock() ):
      res = self.frq.execute()
    self.assertTrue( not res['OK'] )

  def test_Exe_statusFalse_diset_ok( self ):
    """execute: workflowStatus not OK diset success................................................."""
    gLogger.setLevel("ERROR")
    with patch("ILCDIRAC.Workflow.Modules.ModuleBase.RequestValidator", Mock() ):
      self.frq = FailoverRequest()
    self.frq.log = gLogger.getSubLogger("Frq-Exe-Succeed")
    self.frq.InputData = ["/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0000.stdhep",
                          "/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0001.stdhep",
                          "/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0002.stdhep",
                          "/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0003.stdhep",
                         ]
    self.frq.productionID = 43321
    self.frq.applicationSpecificInputs = Mock(return_value=S_OK())
    self.frq.jobID = 12345
    self.frq.workflowStatus['OK'] = False
    fr_mock = Mock(self.fr_mock)
    fr_mock.commit.return_value = S_ERROR()
    fr_mock.generateForwardDISET = Mock( return_value=S_OK(None) )
    self.frq.fileReport = fr_mock
    self.frq.workflow_commons = dict( JobReport = self.jr_mock, FileReport = fr_mock, PRODUCTION_ID=43321, JOB_ID = 12345 )
    self.frq.workflow_commons['Request'] = self.rc_mock
    with patch("ILCDIRAC.Workflow.Modules.ModuleBase.RequestValidator", Mock() ):
      res = self.frq.execute()
    self.assertTrue( not res['OK'] )

  def test_Exe_statusFalse_failoverFail( self ):
    """execute: workflowStatus not OK failoverfailure..............................................."""
    gLogger.setLevel("ERROR")
    with patch("ILCDIRAC.Workflow.Modules.ModuleBase.RequestValidator", Mock() ):
      self.frq = FailoverRequest()
    self.frq.log = gLogger.getSubLogger("Frq-Exe-Succeed")
    self.frq.InputData = ["/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0000.stdhep",
                          "/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0001.stdhep",
                          "/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0002.stdhep",
                          "/ilc/prod/clic/1.4tev/h_nunu/GEN/00004191/000/h_nunu_gen_4191_0003.stdhep",
                         ]
    self.frq.productionID = 43321
    self.frq.applicationSpecificInputs = Mock(return_value=S_OK())
    self.frq.jobID = 12345
    self.frq.generateFailoverFile = Mock(return_value=S_ERROR("No can do"))
    self.frq.workflowStatus['OK'] = False
    fr_mock = Mock(self.fr_mock)
    fr_mock.commit.return_value = S_ERROR()
    fr_mock.generateForwardDISET = Mock( return_value=S_OK(None) )
    self.frq.fileReport = fr_mock
    self.frq.workflow_commons = dict( JobReport = self.jr_mock, FileReport = fr_mock, PRODUCTION_ID=43321, JOB_ID = 12345 )
    self.frq.workflow_commons['Request'] = self.rc_mock
    with patch("ILCDIRAC.Workflow.Modules.ModuleBase.RequestValidator", Mock() ):
      res = self.frq.execute()
    self.assertTrue( not res['OK'] )

  def test_Exe_genDisetRequest( self ):
    """execute: Generate Diset Request.............................................................."""
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
    with patch("ILCDIRAC.Workflow.Modules.ModuleBase.RequestValidator", Mock() ):
      self.frq.execute()
    self.assertTrue( self.frq.workflow_commons['Request'] )

  def test_set_registrationRequest( self ):
    """execute: test if setRegistrationRequest succeeds ............................................"""
    # setup the filedict from getFileMetaData and pass that to setRegistrationRequest
    gLogger.setLevel("ERROR")
    self.uod = UploadOutputData()
    self.uod.log = gLogger.getSubLogger("FRQ-Exe-RegReqs")
    self.uod.prodOutputLFNs = ['/ilc/user/s/sailer/test3.stdhep']
    self.uod.jobReport = Mock()
    self.uod.jobReport.generateForwardDISET.return_value = S_ERROR("No JobRep")
    self.uod.fileReport = self.fr_mock
    self.uod.workflow_commons = dict(Enable=True, PRODUCTION_ID=43321, JOB_ID = 12345 )

    candidateFiles = {'test3.stdhep': {'lfn':'/ilc/user/s/sailer/test3.stdhep', 'workflowSE':'CERN-DIP-4'}}
    self.uod.getCandidateFiles = Mock(return_value=S_OK())
    res = self.uod.getFileMetadata(candidateFiles)
    self.uod.log.debug("MetaData: %s" % res)
    fileDict = res['Value']
    self.uod.log.debug("MetaData fileDict: %s" % fileDict)
    self.uod.getFileMetadata = Mock(return_value=res)
    self.uod.getDestinationSEList = Mock(return_value=S_OK(["CERN-DIP-4"]))
    self.uod.enable = True
    self.uod.jobID = 12345
    from DIRAC.DataManagementSystem.Client.FailoverTransfer import FailoverTransfer
    lfn = '/ilc/user/s/sailer/test3.stdhep'
    targetSE = "CERN-DIP-4"
    catalog = ["FileCatalog"]
    fot = FailoverTransfer( self.uod._getRequestContainer() )
    fot._setRegistrationRequest(lfn, targetSE, fileDict['test3.stdhep']['filedict'], catalog)
    self.uod.log.info("RegReq: %s " % self.uod._getRequestContainer() )
    res = self.uod.generateFailoverFile( )
    self.uod.log.info("failOverFile: %s " % res)

#############################################################################
# UploadOutputData.py
#############################################################################

@patch("DIRAC.Core.Security.ProxyInfo.getProxyInfoAsString", new=Mock(return_value=S_OK()))
@patch("ILCDIRAC.Workflow.Modules.ModuleBase.getProxyInfoAsString", new=Mock(return_value=S_OK()))
class TestUploadOutputData( ModulesTestCase ):
  """ test UploadOutputData """

  @patch("ILCDIRAC.Workflow.Modules.ModuleBase.getProxyInfoAsString", new=Mock(return_value=S_OK()))
  @patch("DIRAC.Core.Security.ProxyInfo.getProxyInfoAsString", new=Mock(return_value=S_OK()))
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
    self.assertFalse( self.uod.enable )

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
    self.assertIsInstance( self.uod.outputDataFileMask, list )
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
    self.assertEqual( len(self.uod.outputList), 4 )

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

  @patch('DIRAC.ConfigurationSystem.Client.Helpers.Operations.Operations', new=Mock() )
  @patch("ILCDIRAC.Workflow.Modules.ModuleBase.RequestValidator", new=Mock() )
  def test_EXE_cleanUpRequests( self ):
    """execute: test when Requests are being cleaned up ............................................"""
    # we want that the upload output data fails to upload and do registration,
    # so we need to create a registration request, by failing at the right
    # place in the FailoverTransfer, which means we need to somehow control
    # the failovertransfer that is being called by the UploadOutputData...
    #
    gLogger.setLevel("ERROR")
    self.uod = UploadOutputData()
    self.uod.log = gLogger.getSubLogger("UOD-Exe-RegReqs")
    self.uod.prodOutputLFNs = ['/ilc/user/s/sailer/test3.stdhep']
    self.uod.workflow_commons = dict(Enable=True)
    candidateFiles = {'test3.stdhep': {'lfn':'/ilc/user/s/sailer/test3.stdhep', 'workflowSE':'CERN-DIP-4'}}
    self.uod.getCandidateFiles = Mock(return_value=S_OK())
    res = self.uod.getFileMetadata(candidateFiles)
    self.uod.getFileMetadata = Mock(return_value=res)
    self.uod.getDestinationSEList = Mock(return_value=S_OK(["CERN-DIP-4"]))
    self.uod.enable = True
    self.uod.jobID = 12345
    from DIRAC.DataManagementSystem.Client.FailoverTransfer import FailoverTransfer
    FailoverTransfer.transferAndRegisterFile = Mock(return_value=S_ERROR("IT ACTUALLY WORKS!!!!!!1eleven!!"))
    _resUodExe = self.uod.execute()
    res = self.uod.generateFailoverFile()
    self.uod.log.info("RequestValidation: %s " % res)

#############################################################################
# UserJobFinalization.py
#############################################################################

@patch("DIRAC.Core.Security.ProxyInfo.getProxyInfoAsString", new=Mock(return_value=S_OK()))
@patch("ILCDIRAC.Workflow.Modules.ModuleBase.getProxyInfoAsString", new=Mock(return_value=S_OK()))
class TestUserJobFinalization( ModulesTestCase ):
  """ test UserJobFinalization """

  @patch("ILCDIRAC.Workflow.Modules.ModuleBase.getProxyInfoAsString", new=Mock(return_value=S_OK()))
  @patch("DIRAC.Core.Security.ProxyInfo.getProxyInfoAsString", new=Mock(return_value=S_OK()))
  def setUp( self ):
    super(TestUserJobFinalization, self).setUp()
    with patch('DIRAC.ConfigurationSystem.Client.Helpers.Operations.Operations', Mock() ):
      self.ujf = UserJobFinalization()

  def test_UJF_execute_isLastStep(self):
    """UJF.execute: is last step...................................................................."""
    self.ujf.step_commons['STEP_NUMBER'] = 2
    self.ujf.workflow_commons['TotalSteps'] = 2
    resLS = self.ujf.isLastStep()
    self.assertTrue( resLS['OK'] )

  def test_UJF_execute_isLastStep_not(self):
    """UJF.execute: is Not the last step............................................................"""
    self.ujf.step_commons['STEP_NUMBER'] = 1
    self.ujf.workflow_commons['TotalSteps'] = 2
    resLS = self.ujf.isLastStep()
    self.assertFalse( resLS['OK'] )

  def test_UFJ_getOutputList(self):
    """UJF.execute: getOutputList..................................................................."""
    gLogger.setLevel("ERROR")
    self.ujf.userOutputSE = "CERN-SRM"
    self.ujf.userOutputData = ['gen.stdhep',
                               'sim.slcio',
                               'rec.slcio',
                               'dst.slcio']

    outputList = self.ujf.getOutputList()
    self.log.debug(outputList)

  def test_UJF_TRFF(self):
    """UJF.execute: transferAndRegisterFailoverFile................................................."""
    gLogger.setLevel("ERROR")
    ft_mock = Mock()
    ft_mock.transferAndRegisterFileFailover.return_value=S_OK()
    candidateFiles = {'test3.stdhep': {'lfn':'/ilc/user/s/sailer/test3.stdhep', 'workflowSE':'CERN-SRM'}}
    resMetadata = self.ujf.getFileMetadata( candidateFiles )
    filesToFailover = resMetadata['Value']
    filesToFailover['test3.stdhep']['resolvedSE'] = ['CERN-SRM', 'KEK-SRM', 'RAL-SRM']
    filesUploaded = []
    self.ujf.failoverSEs= ['CERN-SRM', 'RAL-SRM']
    res = self.ujf.transferRegisterAndFailoverFiles(ft_mock, filesToFailover, filesUploaded)
    self.log.debug(res)
    self.log.debug(filesUploaded)
    self.log.debug(res)
    self.assertFalse( res['Value']['cleanUp'] and filesUploaded )

  def test_UJF_TRFF_Failed(self):
    """UJF.execute: transferAndRegisterFailoverFile, no more SEs...................................."""
    gLogger.setLevel("ERROR")
    ft_mock = Mock()
    ft_mock.transferAndRegisterFileFailover.return_value=S_OK()
    filesToFailover = {'test.txt': { 'lfn': '/ilc/user/s/sailer/test/test.txt',
                                     'localpath': './test.txt',
                                     'resolvedSE': ['CERN-SRM', 'KEK-SRM', 'RAL-SRM'],
                                     'workflowSE': ['CERN-SRM'],
                                     'path': 'SLCIO',
                                     'guid': 'A331AE88-AD87-AF39-97E1-44257D8200C8'}}
    filesUploaded = []
    self.ujf.failoverSEs= ['CERN-SRM']
    res = self.ujf.transferRegisterAndFailoverFiles(ft_mock, filesToFailover, filesUploaded)
    self.log.debug(res)
    self.log.debug(filesUploaded)
    self.log.debug(res)
    self.assertTrue( res['Value']['cleanUp'] and not filesUploaded )

if __name__ == '__main__':
  unittest.main(verbosity=2)
