"""
Unit tests for the FccAnalysis.py file
"""

import unittest
import os
from mock import patch, MagicMock as Mock
from ILCDIRAC.Workflow.Modules.FccAnalysis import FccAnalysis
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, \
assertDiracFailsWith, assertDiracSucceedsWith

from DIRAC import S_OK, S_ERROR

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Workflow.Modules.FccAnalysis'


class TestFccAnalysis( unittest.TestCase ):
  """ Test the FccAnalysis module
  """

  def setUp( self ):
    """set up the objects"""    

    def replace_realpath( path ):
      return os.path.join("/test/realpath", path)

    self.log_mock = Mock()

    self.patches = [patch("%s.os.path.realpath" % MODULE_NAME, new=Mock(side_effect=replace_realpath)),
                    patch("%s.os.path.dirname" % MODULE_NAME, new=Mock(return_value="/test/dirname")),
                    patch("%s.FccAnalysis.redirectLogOutput" % MODULE_NAME, new=Mock()),
                    patch('%s.LOG' % MODULE_NAME, new=self.log_mock),
                    ]

    for patcher in self.patches:
      patcher.start()

    self.fccAna = FccAnalysis()

    self.fccAna.platform = "Testplatform123"
    self.fccAna.applicationLog = "testlog123"
    self.fccAna.SteeringFile = os.path.realpath("fccConfFile.cfg")
    self.fccAna.applicationName = "fccApp"
    self.fccAna.applicationVersion = "v1.0"
    self.fccAna.STEP_NUMBER = "1"
    self.fccAppIndex = "%s_%s_Step_%s" % (self.fccAna.applicationName, self.fccAna.applicationVersion,
                                          self.fccAna.STEP_NUMBER)
    self.applicationScript = os.path.realpath("%s.sh" % self.fccAppIndex)
    self.root_files = ["outputFile.root", "outputFile2.root"]
    self.exists_dict = {
      self.fccAna.SteeringFile : True,
      self.fccAna.applicationLog : True,
      "/test/realpath/outputFile.root" : True,
      '/test/realpath/outputFile.txt' : True
      }

  def replace_exists( self, path ):
    return self.exists_dict[path]

  def tearDown( self ):
    for patcher in self.patches:
      patcher.stop()
    del self.fccAna

  def test_runit_noplatform( self ):
    self.fccAna.platform = None
    assertDiracFailsWith( self.fccAna.runIt(), 'No ILC platform selected', self )

  def test_runit_noapplog( self ):
    self.fccAna.platform = "Testplatform123"
    self.fccAna.applicationLog = None
    assertDiracFailsWith( self.fccAna.runIt(), 'No Log file provided', self )

  def test_runit_workflowbad( self ):
    self.fccAna.applicationLog = "testlog123"
    self.fccAna.workflowStatus = { 'OK' : False }
    assertDiracSucceedsWith( self.fccAna.runIt(), 'should not proceed', self )

  def test_runit_stepbad( self ):
    self.fccAna.stepStatus = { 'OK' : False }
    assertDiracSucceedsWith( self.fccAna.runIt(), 'should not proceed', self )

  @patch("%s.getEnvironmentScript" % MODULE_NAME, new=Mock(return_value=S_OK()))
  @patch("%s.os.path.exists" % MODULE_NAME, new=Mock(return_value=True))
  def test_getenvironmentscript_cfg_lookup( self ):        
    self.assertTrue( self.fccAna.getEnvironmentScript() )

  @patch("%s.getEnvironmentScript" % MODULE_NAME, new=Mock(return_value=S_ERROR()))
  def test_getenvironmentscript_cfg_lookup_failed( self ):        
    self.assertFalse( self.fccAna.getEnvironmentScript() )
    self.log_mock.error.assert_called_once_with( "Environment : 'dirac.cfg' file look up failed" )

  @patch("%s.getEnvironmentScript" % MODULE_NAME, new=Mock(return_value=S_OK()))
  @patch("%s.os.path.exists" % MODULE_NAME, new=Mock(return_value=False))
  def test_getenvironmentscript_pathexists_failed( self ):        
    self.assertFalse( self.fccAna.getEnvironmentScript() )

  def test_writetofile( self ):
    data_to_write = 'LEAVE ME HERE' 
    with patch('__builtin__.open', create=True) as mock_write:
      self.assertTrue( self.fccAna.writeToFile("w", "/file/to/write", data_to_write) )   

      mock_write.assert_called_once_with( "/file/to/write", 'w' )
      manager = mock_write.return_value.__enter__.return_value
      manager.write.assert_called_with( data_to_write )

      debug_message = "Application : File write operation successfull"
      self.log_mock.debug.assert_called_once_with( debug_message )

  @patch('%s.open' % MODULE_NAME, new=Mock(side_effect=IOError("ioerror")), create=True )
  def test_writetofile_failed( self ):
    self.assertFalse( self.fccAna.writeToFile("w", "", "") )   

    error_message = "Application : File write operation failed\nioerror"
    self.log_mock.error.assert_called_once_with( error_message )

  def test_generatescriptonthefly( self ):
    error_message = (
      "Environment : Environment script not found\n"
      "for this configuration : conf, name, version\n"
      "Can not generate one dynamically"
    )
    assertDiracFailsWith( self.fccAna.generateScriptOnTheFly("conf", "name", "version"), error_message, self )
    # This is the only one error message put in debug level, check FccAnalysis
    self.log_mock.debug.assert_called_once_with( error_message )

  def test_generatebashscript( self ):      
    with patch('%s.FccAnalysis.writeToFile' % MODULE_NAME, new=Mock(return_value=True)) as mock_write, \
         patch('%s.os.chmod' % MODULE_NAME) as mock_chmod:

      self.assertTrue( self.fccAna.generateBashScript("command1") )
      mock_write.assert_called_once_with( 'w', self.fccAna.applicationScript, '#!/bin/bash\nsource \ncommand1\n' )
      mock_chmod.assert_called_once_with( self.fccAna.applicationScript, 0o755 )
      self.log_mock.debug.assert_any_call( "Application code : Bash script creation successfull" )
      self.log_mock.debug.assert_any_call( "Application file : Bash script rights setting successfull" )

  def test_generatebashscript_write_failed( self ):
    with patch('%s.FccAnalysis.writeToFile' % MODULE_NAME, new=Mock(return_value=False)) as mock_write:

      self.assertFalse( self.fccAna.generateBashScript("command1") )

      mock_write.assert_called_once_with( 'w', self.fccAna.applicationScript, '#!/bin/bash\nsource \ncommand1\n' )
      error_message = "Application code : Bash script creation failed"
      self.log_mock.error.assert_called_once_with( error_message )

  @patch("%s.FccAnalysis.writeToFile" % MODULE_NAME, new=Mock(return_value=True))
  def test_generategaudiconffile( self ):
    self.fccAna.logLevel = "DEBUG"
    self.assertTrue( self.fccAna.generateGaudiConfFile() )

  def test_generategaudiconffile_gaudioptions( self ):
    self.fccAna.logLevel = "DEBUG"
    self.fccAna.NumberOfEvents = 42
    self.fccAna.RandomSeed = 126
    self.fccAna.randomGenerator["Gaudi"] = True
    self.fccAna.read = True
    self.fccAna.InputData = ["/path/to/data"]
    self.fccAna.InputFile = []

    gaudiOptions = ["from Configurables import ApplicationMgr"]
    gaudiOptions += ["from Gaudi.Configuration import *"]

    eventSetting = ["# N) AUTOMATIC GENERATION OF CODE DONE BY FCC APPLICATION FOR EVENT NUMBER SETTING"]
    eventSetting += ["ApplicationMgr().EvtMax=%s" % self.fccAna.NumberOfEvents]
    gaudiOptions += eventSetting

    seedSetting = ["# N) AUTOMATIC GENERATION OF CODE DONE BY FCC APPLICATION FOR SEED NUMBER SETTING"]
    seedSetting += ["from Configurables import SimG4Svc, RndmGenSvc"]
    seedSetting += ['from GaudiSvc.GaudiSvcConf import HepRndm__Engine_CLHEP__RanluxEngine_']
    seedSetting += ["randomEngine = eval('HepRndm__Engine_CLHEP__RanluxEngine_')"]
    seedSetting += ["randomEngine = randomEngine('RndmGenSvc.Engine')"]
    seedSetting += ["randomEngine.Seeds = [%d]  " % self.fccAna.RandomSeed]

    gaudiOptions += seedSetting

    levelSetting = "ApplicationMgr().OutputLevel=%s" % self.fccAna.logLevel
    gaudiOptions += [levelSetting]

    fccswPodioOptions = ["# N) AUTOMATIC GENERATION OF CODE DONE BY FCC APPLICATION FOR INPUT FILE SETTING"]
    fccswPodioOptions += ["from Configurables import FCCDataSvc"]
    fccswPodioOptions += ["import os"]

    fccInputDataSubstitution = [ '%s' for data in self.fccAna.InputData]
    fccInputData = ["os.path.realpath(os.path.basename('%s'))" % data
                    for data in self.fccAna.InputData]
    # We can provide many input files to FCCDataSvc() like this :
    inputSetting = ["podioevent = FCCDataSvc('EventDataSvc', input='%s' %% (%s))" % (" ".join(fccInputDataSubstitution), ", ".join(fccInputData))]
    inputSetting += ["ApplicationMgr().ExtSvc += [podioevent]"]
    fccswPodioOptions += inputSetting

    gaudiOptions += fccswPodioOptions

    with patch("%s.FccAnalysis.writeToFile" % MODULE_NAME) as mock_write:
      mock_write.return_value = True
      self.assertTrue( self.fccAna.generateGaudiConfFile() )
      mock_write.assert_called_once_with( 'w', self.fccAna.gaudiOptionsFile, "\n".join(gaudiOptions) + '\n' )  
      
  def test_generategaudiconffile_levelfailed( self ):
    self.fccAna.logLevel = "GUBED"
    self.assertFalse( self.fccAna.generateGaudiConfFile() )
    message = (
      "FCCSW specific consistency : Invalid value for the log level\n"
      "Possible values for the log level are :\n%(log)s" % {'log' : " ".join(self.fccAna.logLevels)}
    )
    self.log_mock.error.assert_called_with( message )    

  @patch("%s.FccAnalysis.writeToFile" % MODULE_NAME, new=Mock(return_value=False))
  def test_generategaudiconffile_writefailed( self ):
    self.fccAna.logLevel = "DEBUG"
    self.assertFalse( self.fccAna.generateGaudiConfFile() )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=False))
  def test_runit_getenvironmentscript_failed( self ):
    error_message = (
      "Environment : Environment script look up failed\n"
      "Failed to get environment"
    )
    assertDiracFailsWith( self.fccAna.runIt(), error_message, self )
    self.log_mock.error.assert_called_once_with( error_message )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.os.path.exists" % MODULE_NAME, new=Mock(return_value=False))
  def test_runit_conffile_failed( self ):
    error_message = (
      "Environment : FCC configuration file '%(sfile)s' does not exist,"
      " can not run FCC application" % {'sfile' : self.fccAna.SteeringFile}
    )
    assertDiracFailsWith( self.fccAna.runIt(), error_message, self )
    self.log_mock.info.assert_any_call( "Environment : Environment script look up successfull" )
    self.log_mock.error.assert_called_once_with( error_message )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.FccAnalysis.generateGaudiConfFile' % MODULE_NAME, new=Mock(return_value=False))
  def test_runit_generategaudiconffile_failed( self ):
    self.fccAna.isGaudiOptionsFileNeeded = True

    with patch('os.path.exists') as  mock_exists:
 
      mock_exists.side_effect = self.replace_exists
      error_message = "Application code : generateGaudiConfFile() failed"
      assertDiracFailsWith( self.fccAna.runIt(), error_message, self )
      self.log_mock.error.assert_called_once_with( error_message )
      self.log_mock.info.assert_any_call( "Environment : Environment script look up successfull" )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=False))
  def test_runit_generatebashscript_failed( self ):
    with patch('os.path.exists') as  mock_exists:
 
      mock_exists.side_effect = self.replace_exists
      error_message = "Application code : Creation of the bash script failed"
      assertDiracFailsWith( self.fccAna.runIt(), error_message, self )
      self.log_mock.info.assert_any_call( "Environment : Environment script look up successfull" )
      self.log_mock.error.assert_called_once_with( error_message )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  def test_runit_shellcall_failed( self ):
    with patch('os.path.exists') as  mock_exists, \
         patch("%s.shellCall" % MODULE_NAME) as mock_shellcall:

      mock_exists.side_effect = self.replace_exists
      mock_shellcall.return_value=S_ERROR()
    
      error_message = "Application : Application execution failed"
      assertDiracFailsWith( self.fccAna.runIt(), error_message, self )
      self.log_mock.info.assert_any_call( "Environment : Environment script look up successfull" )
      self.log_mock.error.assert_called_once_with( error_message )
      mock_shellcall.assert_called_once_with( 0, self.fccAna.applicationScript, callbackFunction = self.fccAna.redirectLogOutput, bufferLimit = 20971520 )     
      self.log_mock.debug.assert_any_call( "Application code : Creation of the bash script successfull" )
      self.log_mock.debug.assert_any_call( "Application : Application execution and log file creation...")
      
  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.glob.glob' % MODULE_NAME, new=Mock(return_value=[]))
  def test_runit_log_failed_withoutapp( self ):
    self.fccAna.ignoreapperrors = True
    self.exists_dict[self.fccAna.applicationLog] = False

    with patch('os.makedirs'), \
         patch('os.path.exists') as  mock_exists, \
         patch("%s.shellCall" % MODULE_NAME, new=Mock(return_value={'OK' : True, 'Value' : ["", "stdout", "stderr"]})) as mock_shellcall:

      mock_exists.side_effect = self.replace_exists
      assertDiracSucceedsWith( self.fccAna.runIt(), "Execution of the FCC application successfull", self )
      self.log_mock.info.assert_any_call( "Environment : Environment script look up successfull" )
      self.log_mock.debug.assert_any_call( "Application code : Creation of the bash script successfull" )
      self.log_mock.debug.assert_any_call( "Application : Application execution and log file creation..." )
      self.log_mock.debug.assert_any_call( "Application : Application execution successfull" )
      
      self.log_mock.error.assert_called_once_with( "Application : Log file creation failed" )
      mock_shellcall.assert_called_once_with( 0, self.fccAna.applicationScript, callbackFunction = self.fccAna.redirectLogOutput, bufferLimit = 20971520 )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  def test_runit_log_failed_withapp( self ):
    self.exists_dict[self.fccAna.applicationLog] = False
    self.fccAna.ignoreapperrors = False

    with patch('os.makedirs'), \
         patch('os.path.exists') as  mock_exists, \
         patch("%s.shellCall" % MODULE_NAME, new=Mock(return_value={'OK' : True, 'Value' : ["", "stdout", "stderr"]})) as mock_shellcall:

      mock_exists.side_effect = self.replace_exists
      error_message = '%s did not produce the expected log %s' % (self.fccAna.applicationName, self.fccAna.applicationLog)
      assertDiracFailsWith( self.fccAna.runIt(), error_message, self )
      self.log_mock.error.assert_any_call(error_message)
      self.log_mock.info.assert_any_call( "Environment : Environment script look up successfull" )
      self.log_mock.debug.assert_any_call( "Application code : Creation of the bash script successfull" )
      self.log_mock.debug.assert_any_call( "Application : Application execution and log file creation..." )
      self.log_mock.debug.assert_any_call( "Application : Application execution successfull" )
      mock_shellcall.assert_called_once_with( 0, self.fccAna.applicationScript, callbackFunction = self.fccAna.redirectLogOutput, bufferLimit = 20971520 )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.FccAnalysis.writeToFile' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.glob.glob" % MODULE_NAME, new=Mock(return_value=[]))
  def test_runit( self ):
    with patch('os.makedirs'), \
         patch('os.path.exists') as  mock_exists, \
         patch("%s.shellCall" % MODULE_NAME, new=Mock(return_value={'OK' : True, 'Value' : ["", "stdout", "stderr"]})) as mock_shellcall:

      mock_exists.side_effect = self.replace_exists

      assertDiracSucceedsWith( self.fccAna.runIt(), "Execution of the FCC application successfull", self )
      self.log_mock.info.assert_any_call( "Environment : Environment script look up successfull" )
      self.log_mock.debug.assert_any_call( "Application code : Creation of the bash script successfull" )
      self.log_mock.debug.assert_any_call( "Application : Application execution and log file creation..." )
      self.log_mock.debug.assert_any_call( "Application : Application execution successfull" )
      self.log_mock.debug.assert_any_call( "Application : Log file creation successfull" )
      self.log_mock.warn.assert_called_once_with( "Application : no root files have been generated, was that intentional ?" )
      mock_shellcall.assert_called_once_with( 0, self.fccAna.applicationScript, callbackFunction = self.fccAna.redirectLogOutput, bufferLimit = 20971520 )
    
  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.FccAnalysis.writeToFile' % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.glob.glob' % MODULE_NAME, new=Mock(return_value=[]))
  def test_runit_with_inputfile( self ):
    get_input_from_app =  "output_%s.root" % (self.fccAna.fccAppIndex)
    self.fccAna.step_commons['InputFile'] = get_input_from_app

    input_file = os.path.realpath( get_input_from_app )
    
    self.exists_dict[input_file] = True
    
    with patch('os.makedirs'), \
         patch('os.path.exists') as  mock_exists :

      mock_exists.side_effect = self.replace_exists
      assertDiracSucceedsWith( self.fccAna.runIt(), "Execution of the FCC application successfull", self )

      assertEqualsImproved( self.fccAna.InputFile, input_file, self )
      assertEqualsImproved( self.fccAna.SteeringFile, self.fccAna.InputFile, self )
      self.log_mock.debug.assert_any_call( "Application : Configuration file taken from the input file(s) '%s'" % self.fccAna.InputFile )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.FccAnalysis.writeToFile' % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.glob.glob' % MODULE_NAME, new=Mock(return_value=[]))
  def test_runit_with_many_inputfiles( self ):
    get_input_from_app =  ["output1_%s.root" % self.fccAppIndex,"output2_%s.root" % self.fccAppIndex]
    self.fccAna.step_commons['InputFile'] = get_input_from_app

    input_file1 = os.path.realpath( get_input_from_app[0] )
    input_file2 = os.path.realpath( get_input_from_app[1] )

    self.exists_dict[input_file1] = True
    self.exists_dict[input_file2] = True

    with patch('os.makedirs'), \
         patch('os.path.exists') as  mock_exists :

      mock_exists.side_effect = self.replace_exists
      assertDiracSucceedsWith( self.fccAna.runIt(), "Execution of the FCC application successfull", self )

      assertEqualsImproved( self.fccAna.InputFile, [input_file1, input_file2], self )
      assertEqualsImproved( self.fccAna.SteeringFile, " ".join(self.fccAna.InputFile), self )
      self.log_mock.debug.assert_any_call( "Application : Configuration file taken from the input file(s) '%s'" % self.fccAna.SteeringFile )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.FccAnalysis.writeToFile' % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.glob.glob' % MODULE_NAME, new=Mock(return_value=[]))
  def test_runit_without_inputfile( self ):
    with patch('os.makedirs'), \
         patch('os.path.exists') as  mock_exists :

      mock_exists.side_effect = self.replace_exists
      assertDiracSucceedsWith( self.fccAna.runIt(), "Execution of the FCC application successfull", self )
      assertEqualsImproved( self.fccAna.InputFile, [], self )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.FccAnalysis.writeToFile' % MODULE_NAME, new=Mock(return_value=True))
  def test_runit_without_current_rootfiles( self ):
    getctime_dict = {
      os.path.realpath(self.root_files[1]) : 1501667507.9749944,
      os.path.realpath(self.root_files[0]) : 1501667510.7510207,
      os.path.realpath(self.applicationScript) : 1501667512.7510207
      }

    def replace_getctime( path ):
      return getctime_dict[os.path.realpath(path)]

    with patch('os.makedirs'), \
         patch("%s.glob.glob" % MODULE_NAME) as mock_glob, \
         patch("os.path.getctime") as mock_getctime, \
         patch('os.path.exists') as  mock_exists :

      mock_exists.side_effect = self.replace_exists
      mock_getctime.side_effect = replace_getctime
      mock_glob.return_value = self.root_files
      assertDiracSucceedsWith( self.fccAna.runIt(), "Execution of the FCC application successfull", self )

      self.log_mock.warn.assert_called_once_with( "Application : This application did not generate any root files, was that intentional ?" )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.glob.glob" % MODULE_NAME, new=Mock(return_value=[]))
  def test_runit_cardfile( self ):
    card_file = "/path/to/cardFile"
    self.fccAna.randomGenerator = {"Pythia" : [card_file]}
    self.fccAna.RandomSeed = 126
    self.fccAna.NumberOfEvents = 42

    
    with patch('%s.FccAnalysis.readFromFile'  % MODULE_NAME) as mock_read, \
         patch('%s.FccAnalysis.writeToFile'  % MODULE_NAME) as mock_write, \
         patch('os.makedirs'), \
         patch('os.path.exists') as  mock_exists, \
         patch("%s.shellCall" % MODULE_NAME, new=Mock(return_value={'OK' : True, 'Value' : ["", "stdout", "stderr"]})) as mock_shellcall:

      mock_exists.side_effect = self.replace_exists
      content = "! CONTENT OF THE CARD FILE"
      message = 'Application : Card file reading successfull'
      mock_read.return_value = (content, message)
      mock_write.return_value = True

      assertDiracSucceedsWith( self.fccAna.runIt(), "Execution of the FCC application successfull", self )
      self.log_mock.info.assert_any_call( "Environment : Environment script look up successfull" )
      self.log_mock.debug.assert_any_call( "Application code : Creation of the bash script successfull" )
      self.log_mock.debug.assert_any_call( "Application : Application execution and log file creation..." )
      self.log_mock.debug.assert_any_call( "Application : Application execution successfull" )
      self.log_mock.debug.assert_any_call( "Application : Log file creation successfull" )
      self.log_mock.warn.assert_called_once_with( "Application : no root files have been generated, was that intentional ?" )
      mock_shellcall.assert_called_once_with( 0, self.fccAna.applicationScript, callbackFunction = self.fccAna.redirectLogOutput, bufferLimit = 20971520 )

      eventSetting = ["! N) AUTOMATIC GENERATION OF CODE DONE BY FCC APPLICATION FOR EVENT NUMBER SETTING"]
      eventSetting += ["Main:numberOfEvents = %d         ! number of events to generate" % self.fccAna.NumberOfEvents]
      contentWithEventSet = "%s\n%s\n" % (content, "\n".join(eventSetting))

      seedSetting = ["! N) AUTOMATIC GENERATION OF CODE DONE BY FCC APPLICATION FOR SEED NUMBER SETTING"]
      seedSetting += ["Random:setSeed = on         ! apply user-set seed everytime the Pythia::init is called"]
      seedSetting += ["Random:seed = %d         ! -1=default seed, 0=seed based on time, >0 user seed number" % self.fccAna.RandomSeed]
      contentWithEventSeedSet = "%s\n%s\n" % (contentWithEventSet, "\n".join(seedSetting))
        
      self.log_mock.debug.assert_any_call( message )
      mock_read.assert_called_once_with( card_file )
      mock_write.assert_any_call( 'w', card_file, contentWithEventSeedSet )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.glob.glob" % MODULE_NAME, new=Mock(return_value=[]))
  def test_runit_cardfile_readfailed( self ):
    card_file = "/path/to/cardFile"
    self.fccAna.randomGenerator = {"Pythia" : [card_file]}
    self.fccAna.RandomSeed = 126
    self.fccAna.NumberOfEvents = 42
    
    with patch('%s.FccAnalysis.readFromFile'  % MODULE_NAME) as mock_read, \
         patch('%s.FccAnalysis.writeToFile'  % MODULE_NAME) as mock_write, \
         patch('os.path.exists') as  mock_exists :
      
      mock_exists.side_effect = self.replace_exists
      message = 'Application : Card file reading failed'
      mock_read.return_value = (None, message)
      mock_write.return_value = True

      assertDiracFailsWith( self.fccAna.runIt(), message, self )
      
      self.log_mock.error.assert_called_once_with( message )
      mock_read.assert_called_once_with( card_file )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.glob.glob" % MODULE_NAME, new=Mock(return_value=[]))
  @patch("%s.FccAnalysis.writeToFile" % MODULE_NAME, new=Mock(return_value=False))
  def test_runit_cardfile_writefailed( self ):
    card_file = "/path/to/cardFile"
    self.fccAna.randomGenerator = {"Pythia" : [card_file]}  
    self.fccAna.RandomSeed = 126
    self.fccAna.NumberOfEvents = 42
    
    with patch('%s.FccAnalysis.readFromFile'  % MODULE_NAME) as mock_read, \
         patch('os.path.exists') as  mock_exists :
      
      mock_exists.side_effect = self.replace_exists
      content = "******************"
      message = 'Application : Card file reading successfull'
      mock_read.return_value = (content, message)

      error_message = "Application : Card file overwitting failed"
      assertDiracFailsWith( self.fccAna.runIt(), error_message, self )
      
      self.log_mock.error.assert_called_once_with( error_message )
      mock_read.assert_called_once_with( card_file )

  def test_readfromfile( self ):
    with patch('__builtin__.open') as mock_open:
      manager = mock_open.return_value.__enter__.return_value
      manager.read.return_value = 'some data'
      content, message  = self.fccAna.readFromFile("/my/file/to/read")
      assertEqualsImproved( content, 'some data', self )   
      mock_open.assert_called_with( "/my/file/to/read", 'r' )
      assertEqualsImproved( message, 'Application : Card file reading successfull', self )

  @patch('__builtin__.open', new=Mock(side_effect=IOError("ioerror")) )
  def test_readfromfile_failed( self ):
    content, message  = self.fccAna.readFromFile("/my/file/to/read")    
    assertEqualsImproved( None, content, self )   
    assertEqualsImproved( 'Application : Card file reading failed\nioerror', message, self )
