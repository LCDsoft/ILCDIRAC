"""
Unit tests for the FccAnalysis.py file
"""

import unittest
import os
from mock import patch, mock_open, MagicMock as Mock
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
        
    patches = [ 
                patch("%s.os.path.realpath" % MODULE_NAME, new=Mock(side_effect=replace_realpath)),
                patch("%s.os.path.dirname" % MODULE_NAME, new=Mock(return_value="/test/dirname")),
                patch("%s.FccAnalysis.redirectLogOutput" % MODULE_NAME, new=Mock())
              ]

    for patcher in patches:
      patcher.start()

    self.fccAna = FccAnalysis()
    self.log_mock = Mock()
    self.fccAna.log = self.log_mock

    self.fccAna.platform = os.path.realpath("Testplatform123")
    self.fccAna.applicationLog = os.path.realpath("testlog123")
    self.fccAna.fccConfFile = os.path.realpath("fccConfFile.cfg")
    self.fccAna.applicationName = "fccApp"
    self.fccAna.applicationVersion = "v1.0"
    self.fccAna.STEP_NUMBER = "1"
    self.fccAna.fccAppIndex = "%s_%s_Step_%s" % (self.fccAna.applicationName, self.fccAna.applicationVersion, self.fccAna.STEP_NUMBER)
    self.fccAna.applicationFolder = os.path.realpath(self.fccAna.fccAppIndex)
    self.fccAna.applicationScript = os.path.join(self.fccAna.applicationFolder, "%s.sh" % self.fccAna.fccAppIndex)

    self.exists_dict = { self.fccAna.fccConfFile : True, self.fccAna.applicationFolder : False, self.fccAna.applicationLog : True}
    
  def replace_exists( self, path ):
    return self.exists_dict[path]

  def tearDown( self ):
    del self.fccAna
    patch.stopall()

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

  @patch('%s.open' % MODULE_NAME, new=Mock(side_effect=IOError()), create=True )
  def test_writetofile_failed( self ):
    self.assertFalse( self.fccAna.writeToFile("w", "", "") )   

    error_message = "Application : File write operation failed"
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
         patch('%s.FccAnalysis.chmod' % MODULE_NAME, new=Mock(return_value=True)) as mock_chmod:

      self.assertTrue( self.fccAna.generateBashScript(["command1", "command2"]) )
      mock_write.assert_called_once_with( 'w', self.fccAna.applicationScript, '#!/bin/bash\nsource \ncommand1\ncommand2\n' )   
      mock_chmod.assert_any_call( self.fccAna.applicationScript, 'X' )
      mock_chmod.assert_any_call( self.fccAna.applicationScript , 'R' )   
      self.log_mock.debug.assert_any_call( "Application code : Bash script creation successfull" )
      self.log_mock.debug.assert_any_call( "Application file : Bash script rights setting successfull" )

  def test_generatebashscript_write_failed( self ):
    with patch('%s.FccAnalysis.writeToFile' % MODULE_NAME, new=Mock(return_value=False)) as mock_write:

      self.assertFalse( self.fccAna.generateBashScript(["command1", "command2"]) )

      mock_write.assert_called_once_with( 'w', self.fccAna.applicationScript, '#!/bin/bash\nsource \ncommand1\ncommand2\n' )   
      error_message = "Application code : Bash script creation failed"
      self.log_mock.error.assert_called_once_with( error_message )

  def test_generatebashscript_chmod_failed( self ):
    with patch('%s.FccAnalysis.writeToFile' % MODULE_NAME, new=Mock(return_value=True)) as mock_write, \
         patch('%s.FccAnalysis.chmod' % MODULE_NAME, new=Mock(return_value=False)) as mock_chmod:    

      
      self.assertFalse( self.fccAna.generateBashScript(["command1", "command2"]) )   

      mock_write.assert_called_once_with( 'w', self.fccAna.applicationScript, '#!/bin/bash\nsource \ncommand1\ncommand2\n' )
      # The 'False' returned by the left part of the condition make second part non executed
      mock_chmod.assert_called_once_with( self.fccAna.applicationScript, 'R' )   
      self.log_mock.error.assert_called_once_with( "Application file : Bash script rights setting failed" )
    
  def test_chmod( self ):
    with patch('%s.eval' % MODULE_NAME) as mock_eval,\
         patch('%s.os.stat' % MODULE_NAME) as mock_stat,\
         patch('%s.os.chmod' % MODULE_NAME) as mock_chmod:

      mock_eval.return_value =  33152
      mock_stat.return_value.st_mode =  64
      self.assertTrue( self.fccAna.chmod("file2chmod", "X") )
 
      mock_eval.assert_any_call( 'stat.S_IXUSR' ) 
      mock_eval.assert_any_call( 'stat.S_IXGRP' )
      mock_eval.assert_any_call( 'stat.S_IXOTH' )

      mock_stat.assert_called_with( "file2chmod" )
      # 33152|33152|33152|64 = 33216
      mock_chmod.assert_called_with( "file2chmod", 33216 )

  def test_chmod_osstat_failed( self ):
    with patch('%s.eval' % MODULE_NAME) as mock_eval,\
      patch('%s.os.stat' % MODULE_NAME) as mock_stat,\
      patch('%s.os.chmod' % MODULE_NAME) as mock_chmod:

      mock_eval.return_value =  33152
      mock_stat.side_effect = OSError()

      self.assertFalse( self.fccAna.chmod("file2chmod", "X") )

      mock_eval.assert_any_call( 'stat.S_IXUSR' ) 
      mock_eval.assert_any_call( 'stat.S_IXGRP' )
      mock_eval.assert_any_call( 'stat.S_IXOTH' )

      mock_stat.assert_called_with( "file2chmod" ) 

  def test_chmod_builtin_failed( self ):
    with patch('%s.eval' % MODULE_NAME) as mock_eval,\
      patch('%s.os.stat' % MODULE_NAME) as mock_stat,\
      patch('%s.os.chmod' % MODULE_NAME) as mock_chmod:

      mock_eval.return_value =  33152
      mock_stat.return_value.st_mode =  64
      mock_chmod.side_effect = OSError()

      self.assertFalse( self.fccAna.chmod("file2chmod", "X") )

      mock_eval.assert_any_call( 'stat.S_IXUSR' ) 
      mock_eval.assert_any_call( 'stat.S_IXGRP' )
      mock_eval.assert_any_call( 'stat.S_IXOTH' )

      mock_stat.assert_called_with( "file2chmod" )
      # 33152|33152|33152|64 = 33216
      mock_chmod.assert_called_with( "file2chmod", 33216 )

  @patch("%s.FccAnalysis.writeToFile" % MODULE_NAME, new=Mock(return_value=True))
  def test_generategaudiconffile( self ):
    self.fccAna.logLevel = "DEBUG"
    self.assertTrue( self.fccAna.generateGaudiConfFile() )

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
      "Environment : FCC configuration file does not exist,"
      " can not run FCC application"
    )
    assertDiracFailsWith( self.fccAna.runIt(), error_message, self )
    self.log_mock.info.assert_any_call( "Environment : Environment script look up successfull" )
    self.log_mock.error.assert_called_once_with( error_message )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.os.path.exists" % MODULE_NAME, new=Mock(return_value=True))
  def test_runit_applicationfolder_exists( self ):
    self.fccAna.fccConfFile = os.path.realpath("fccConfFile.cfg")
    error_message = "Application : Application folder '%s' already exists !" % self.fccAna.applicationFolder
    assertDiracFailsWith( self.fccAna.runIt(), error_message, self )
    self.log_mock.error.assert_called_once_with( error_message )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.glob.glob" % MODULE_NAME, new=Mock(return_value=[]))
  def test_runit_mkdirapplicationfolder( self ):
    with patch('os.path.exists') as  mock_exists, \
         patch('os.makedirs') as mock_makedirs, \
         patch("%s.shellCall" % MODULE_NAME) as mock_shellcall:

      mock_shellcall.return_value=S_OK()
      mock_exists.side_effect = self.replace_exists
      assertDiracSucceedsWith( self.fccAna.runIt(), "Execution of the FCC application successfull", self )
      self.log_mock.info.assert_any_call( "Environment : Environment script look up successfull" )
      self.log_mock.debug.assert_any_call( "Application : Creation of the application folder '%s'..." % self.fccAna.applicationFolder )
      mock_makedirs.assert_called_once_with( self.fccAna.applicationFolder )
      self.log_mock.debug.assert_any_call( "Application : Creation of the application folder '%s' successfull" % self.fccAna.applicationFolder )
      self.log_mock.warn.assert_any_call( "Application : no root file has been generated, is that normal ?" )
      mock_shellcall.assert_called_once_with( 0, self.fccAna.applicationScript, callbackFunction = self.fccAna.redirectLogOutput, bufferLimit = 20971520 )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.FccAnalysis.generateGaudiConfFile' % MODULE_NAME, new=Mock(return_value=False))
  def test_runit_generategaudiconffile_failed( self ):
    self.fccAna.gaudiOptionsFile = True

    with patch('os.path.exists') as  mock_exists, \
         patch('os.makedirs') as mock_makedirs:
 
      mock_exists.side_effect = self.replace_exists
      error_message = "ApplicationgGaudi options : generateGaudiConfFile() failed"
      assertDiracFailsWith( self.fccAna.runIt(), error_message, self )
      self.log_mock.error.assert_called_once_with( error_message )
      self.log_mock.info.assert_any_call( "Environment : Environment script look up successfull" )
      self.log_mock.debug.assert_any_call( "Application : Creation of the application folder '%s'..." % self.fccAna.applicationFolder )
      mock_makedirs.assert_called_once_with( self.fccAna.applicationFolder )
      self.log_mock.debug.assert_any_call( "Application : Creation of the application folder '%s' successfull" % self.fccAna.applicationFolder )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  def test_runit_mkdirapplicationfolder_failed( self ):
    with patch('os.path.exists') as  mock_exists, \
         patch('os.makedirs') as mock_makedirs:
 
      mock_exists.side_effect = self.replace_exists
      mock_makedirs.side_effect = OSError()
      error_message = "Application : Creation of the application folder '%s' failed" % self.fccAna.applicationFolder
      assertDiracFailsWith( self.fccAna.runIt(), error_message, self )
      self.log_mock.error.assert_called_once_with( error_message )
      self.log_mock.info.assert_any_call( "Environment : Environment script look up successfull" )
      self.log_mock.debug.assert_any_call( "Application : Creation of the application folder '%s'..." % self.fccAna.applicationFolder )
      mock_makedirs.assert_called_once_with( self.fccAna.applicationFolder )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=False))
  def test_runit_generatebashscript_failed( self ):
    with patch('os.path.exists') as  mock_exists, \
         patch('os.makedirs') as mock_makedirs:
 
      mock_exists.side_effect = self.replace_exists
      error_message = "Application code : Creation of the bash script failed"
      assertDiracFailsWith( self.fccAna.runIt(), error_message, self )
      self.log_mock.info.assert_any_call( "Environment : Environment script look up successfull" )
      self.log_mock.error.assert_called_once_with( error_message )
      self.log_mock.debug.assert_any_call( "Application : Creation of the application folder '%s'..." % self.fccAna.applicationFolder )
      mock_makedirs.assert_called_once_with( self.fccAna.applicationFolder )
      self.log_mock.debug.assert_any_call( "Application : Creation of the application folder '%s' successfull" % self.fccAna.applicationFolder )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  def test_runit_shellcall_failed( self ):
    with patch('os.path.exists') as  mock_exists, \
         patch('os.makedirs') as mock_makedirs, \
         patch("%s.shellCall" % MODULE_NAME) as mock_shellcall:

      mock_exists.side_effect = self.replace_exists
      mock_shellcall.return_value=S_ERROR()
    
      error_message = "Application : Application execution failed"
      assertDiracFailsWith( self.fccAna.runIt(), error_message, self )
      self.log_mock.info.assert_any_call( "Environment : Environment script look up successfull" )
      self.log_mock.error.assert_called_once_with( error_message )
      mock_shellcall.assert_called_once_with( 0, self.fccAna.applicationScript, callbackFunction = self.fccAna.redirectLogOutput, bufferLimit = 20971520 )     
      self.log_mock.debug.assert_any_call( "Application : Creation of the application folder '%s'..." % self.fccAna.applicationFolder )
      mock_makedirs.assert_called_once_with( self.fccAna.applicationFolder )
      self.log_mock.debug.assert_any_call( "Application : Creation of the application folder '%s' successfull" % self.fccAna.applicationFolder )
      self.log_mock.debug.assert_any_call( "Application code : Creation of the bash script successfull" )
      self.log_mock.debug.assert_any_call( "Application : Application execution and log file creation...")
      
  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.glob.glob' % MODULE_NAME, new=Mock(return_value=[]))
  def test_runit_log_failed_withoutapp( self ):
    self.fccAna.ignoreapperrors = True
    self.exists_dict[self.fccAna.applicationLog] = False

    with patch('os.makedirs') as mock_makedirs, \
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

    with patch('os.makedirs') as mock_makedirs, \
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
    with patch('os.makedirs') as mock_makedirs, \
         patch('os.path.exists') as  mock_exists, \
         patch("%s.shellCall" % MODULE_NAME, new=Mock(return_value={'OK' : True, 'Value' : ["", "stdout", "stderr"]})) as mock_shellcall:

      mock_exists.side_effect = self.replace_exists

      assertDiracSucceedsWith( self.fccAna.runIt(), "Execution of the FCC application successfull", self )
      self.log_mock.info.assert_any_call( "Environment : Environment script look up successfull" )
      self.log_mock.debug.assert_any_call( "Application code : Creation of the bash script successfull" )
      self.log_mock.debug.assert_any_call( "Application : Application execution and log file creation..." )
      self.log_mock.debug.assert_any_call( "Application : Application execution successfull" )
      self.log_mock.debug.assert_any_call( "Application : Log file creation successfull" )
      self.log_mock.warn.assert_any_call( "Application : no root file has been generated, is that normal ?" )
      mock_shellcall.assert_called_once_with( 0, self.fccAna.applicationScript, callbackFunction = self.fccAna.redirectLogOutput, bufferLimit = 20971520 )
    
  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.FccAnalysis.writeToFile' % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.glob.glob' % MODULE_NAME, new=Mock(return_value=[]))
  def test_runit_with_inputdata( self ):
    input_data = "/ilc/user/u/username/jobID/data1"    
    self.fccAna.workflow_commons['InputData'] = input_data
    with patch('os.makedirs') as mock_makedirs, \
         patch('os.path.exists') as  mock_exists :

      mock_exists.side_effect = self.replace_exists
      assertDiracSucceedsWith( self.fccAna.runIt(), "Execution of the FCC application successfull", self )
      assertEqualsImproved( self.fccAna.InputData, input_data, self )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.FccAnalysis.writeToFile' % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.glob.glob' % MODULE_NAME, new=Mock(return_value=[]))
  def test_runit_without_inputdata( self ):
    with patch('os.makedirs') as mock_makedirs, \
         patch('os.path.exists') as  mock_exists :

      mock_exists.side_effect = self.replace_exists
      assertDiracSucceedsWith( self.fccAna.runIt(), "Execution of the FCC application successfull", self )
      assertEqualsImproved( self.fccAna.InputData, [], self )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.FccAnalysis.writeToFile' % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.glob.glob' % MODULE_NAME, new=Mock(return_value=[]))
  def test_runit_with_numberofevents( self ):
    number_of_events = "3"    
    self.fccAna.workflow_commons['NumberOfEvents'] = number_of_events

    with patch('os.makedirs') as mock_makedirs, \
         patch('os.path.exists') as  mock_exists :

      mock_exists.side_effect = self.replace_exists
      assertDiracSucceedsWith( self.fccAna.runIt(), "Execution of the FCC application successfull", self )
      assertEqualsImproved( self.fccAna.NumberOfEvents, number_of_events, self )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.FccAnalysis.writeToFile' % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.glob.glob' % MODULE_NAME, new=Mock(return_value=[]))
  def test_runit_without_numberofevents( self ):
    with patch('os.makedirs') as mock_makedirs, \
         patch('os.path.exists') as  mock_exists :

      mock_exists.side_effect = self.replace_exists
      assertDiracSucceedsWith( self.fccAna.runIt(), "Execution of the FCC application successfull", self )
      assertEqualsImproved( self.fccAna.NumberOfEvents, 0, self )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.FccAnalysis.writeToFile' % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.glob.glob' % MODULE_NAME, new=Mock(return_value=[]))
  def test_runit_with_inputfile( self ):
    get_input_file = os.path.realpath("inputFile1")
    self.fccAna.step_commons['InputFile'] = get_input_file

    input_file = "JobID_%s_%s" % (self.fccAna.jobID, os.path.basename(get_input_file))
    
    input_file = os.path.join(os.path.dirname(get_input_file), input_file)

    self.exists_dict[input_file] = True
    
    with patch('os.makedirs') as mock_makedirs, \
         patch('os.path.exists') as  mock_exists :

      mock_exists.side_effect = self.replace_exists
      assertDiracSucceedsWith( self.fccAna.runIt(), "Execution of the FCC application successfull", self )

      assertEqualsImproved( self.fccAna.InputFile, input_file, self )
      assertEqualsImproved( self.fccAna.fccConfFile, self.fccAna.InputFile, self )
      self.log_mock.debug.assert_any_call( "Application : Configuration file taken from the input file '%s'" % self.fccAna.InputFile )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.FccAnalysis.writeToFile' % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.glob.glob' % MODULE_NAME, new=Mock(return_value=[]))
  def test_runit_without_inputfile( self ):
    input_file = os.path.realpath("inputFile1")

    with patch('os.makedirs') as mock_makedirs, \
         patch('os.path.exists') as  mock_exists :

      mock_exists.side_effect = self.replace_exists
      assertDiracSucceedsWith( self.fccAna.runIt(), "Execution of the FCC application successfull", self )
      assertEqualsImproved( self.fccAna.InputFile, [], self )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.FccAnalysis.writeToFile' % MODULE_NAME, new=Mock(return_value=True))
  def test_runit_with_rootfiles( self ):
    root_files = ["/path/to/rootfile1", "/path/to/rootfile2"]    
    getctime_dict = { root_files[1] : 1501667507.9749944, root_files[0] : 1501667510.7510207}
    self.fccAna.workflow_commons['UserOutputData'] = "/vo/user/initial/username/outputFile.root;/vo/user/initial/username/outputFile.txt"
    
    def replace_getctime( path ):
      return getctime_dict[path]

    with patch('os.makedirs') as mock_makedirs, \
         patch("%s.glob.glob" % MODULE_NAME) as mock_glob, \
         patch("os.path.getctime") as mock_getctime, \
         patch("shutil.move") as mock_move, \
         patch("shutil.copy") as mock_copy, \
         patch('os.path.exists') as  mock_exists :

      mock_exists.side_effect = self.replace_exists
      mock_getctime.side_effect = replace_getctime
      mock_glob.return_value = root_files
      assertDiracSucceedsWith( self.fccAna.runIt(), "Execution of the FCC application successfull", self )

      old = os.path.realpath(root_files[0])
      self.log_mock.debug.assert_any_call( "Application : Root file '%s' renaming..." % old )

      outputFile = "JobID_%s_%s" % (self.fccAna.jobID, os.path.basename(self.fccAna.outputFile))
      renamedRootFile = os.path.realpath(outputFile)
            
      mock_move.assert_called_once_with( old, renamedRootFile )
      self.log_mock.debug.assert_any_call( "Application : Root file '%s' renamed successfully to '%s'" % (old, renamedRootFile) )

      self.log_mock.debug.assert_any_call( "Application : Root file '%s' copy..." % renamedRootFile )

      copiedRootFile = os.path.join(os.path.dirname(self.fccAna.outputFile), outputFile)

      mock_copy.assert_called_once_with( renamedRootFile, copiedRootFile )
      self.log_mock.debug.assert_any_call( "Application : Root file '%s' copied successfully to '%s'" % (renamedRootFile, copiedRootFile) )

      lfnTree = os.path.dirname("/vo/user/initial/username/outputFile.root")
      indexedOutput = os.path.join(lfnTree, outputFile)

      assertEqualsImproved( self.fccAna.workflow_commons['UserOutputData'], "%s;/vo/user/initial/username/outputFile.txt" % indexedOutput, self )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.FccAnalysis.writeToFile' % MODULE_NAME, new=Mock(return_value=True))
  def test_runit_with_rootfiles_shutil_renaming_failed( self ):
    root_files = ["/path/to/rootfile1", "/path/to/rootfile2"]    
    getctime_dict = { root_files[1] : 1501667507.9749944, root_files[0] : 1501667510.7510207}
    
    def replace_getctime( path ):
      return getctime_dict[path]

    with patch('os.makedirs') as mock_makedirs, \
         patch("%s.glob.glob" % MODULE_NAME) as mock_glob, \
         patch("os.path.getctime") as mock_getctime, \
         patch("shutil.move") as mock_move, \
         patch('os.path.exists') as  mock_exists :

      mock_exists.side_effect = self.replace_exists
      mock_getctime.side_effect = replace_getctime
      mock_move.side_effect = IOError()
      mock_glob.return_value = root_files

      old = os.path.realpath(root_files[0])
      error_message = "Application : Root file '%s' renaming failed" % old
      assertDiracFailsWith( self.fccAna.runIt(), error_message, self )

      self.log_mock.error.assert_called_once_with( error_message )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  @patch('%s.FccAnalysis.writeToFile' % MODULE_NAME, new=Mock(return_value=True))
  def test_runit_with_rootfiles_shutil_copy_failed( self ):
    root_files = ["/path/to/rootfile1", "/path/to/rootfile2"]    
    getctime_dict = { root_files[1] : 1501667507.9749944, root_files[0] : 1501667510.7510207}
    
    def replace_getctime( path ):
      return getctime_dict[path]

    with patch('os.makedirs') as mock_makedirs, \
         patch("%s.glob.glob" % MODULE_NAME) as mock_glob, \
         patch("os.path.getctime") as mock_getctime, \
         patch("shutil.move") as mock_move, \
         patch("shutil.copy") as mock_copy, \
         patch('os.path.exists') as  mock_exists :

      mock_exists.side_effect = self.replace_exists
      mock_getctime.side_effect = replace_getctime
      mock_copy.side_effect = IOError()
      mock_glob.return_value = root_files

      old = os.path.realpath(root_files[0])

      outputFile = "JobID_%s_%s" % (self.fccAna.jobID, os.path.basename(self.fccAna.outputFile))
      renamedRootFile = os.path.realpath(outputFile)
            
      copiedRootFile = os.path.join(os.path.dirname(self.fccAna.outputFile), outputFile)


      error_message = "Application : Root file '%s' copy failed" % renamedRootFile

      assertDiracFailsWith( self.fccAna.runIt(), error_message, self )

      mock_move.assert_called_once_with( old, renamedRootFile )
      mock_copy.assert_called_once_with( renamedRootFile, copiedRootFile )

      self.log_mock.debug.assert_any_call( "Application : Root file '%s' renaming..." % old )
      self.log_mock.debug.assert_any_call( "Application : Root file '%s' renamed successfully to '%s'" % (old, renamedRootFile) )
      self.log_mock.debug.assert_any_call( "Application : Root file '%s' copy..." % renamedRootFile )
      self.log_mock.error.assert_called_once_with( error_message )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.glob.glob" % MODULE_NAME, new=Mock(return_value=[]))
  def test_runit_cardfile( self ):
    self.fccAna.cardFiles = ["/path/to/cardFile"]
    card_file = os.path.realpath(self.fccAna.cardFiles[0])
    self.fccAna.RandomSeed = 1234
    self.fccAna.NumberOfEvents = 42
    
    with patch('%s.FccAnalysis.readFromFile'  % MODULE_NAME) as mock_read, \
         patch('%s.FccAnalysis.writeToFile'  % MODULE_NAME) as mock_write, \
         patch('os.makedirs') as mock_makedirs, \
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
      self.log_mock.warn.assert_any_call( "Application : no root file has been generated, is that normal ?" )
      mock_shellcall.assert_called_once_with( 0, self.fccAna.applicationScript, callbackFunction = self.fccAna.redirectLogOutput, bufferLimit = 20971520 )

      eventSetting = ["! N) AUTOMATIC GENERATION OF CODE DONE BY FCC APPLICATION FOR EVENT NUMBER SETTING"]
      eventSetting += ["Main:numberOfEvents = 42         ! number of events to generate"]
      contentWithEventSet = "%s\n%s\n" % (content, "\n".join(eventSetting))

      seedSetting = ["! N) AUTOMATIC GENERATION OF CODE DONE BY FCC APPLICATION FOR SEED SETTING"]
      seedSetting += ["Random:setSeed = on         ! apply user-set seed everytime the Pythia::init is called"]
      seedSetting += ["Random:seed = 1234         ! -1=default seed, 0=seed based on time, >0 user seed number"]
      contentWithEventSeedSet = "%s\n%s\n" % (contentWithEventSet, "\n".join(seedSetting))
        
      contentWithEventSet = "%s\n%s\n" % (contentWithEventSet, "\n".join(seedSetting))

      self.log_mock.debug.assert_any_call( message )
      mock_read.assert_called_once_with( card_file )
      mock_write.assert_any_call( 'w', card_file, contentWithEventSet )

  @patch('%s.FccAnalysis.getEnvironmentScript' % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.FccAnalysis.generateBashScript" % MODULE_NAME, new=Mock(return_value=True))
  @patch("%s.glob.glob" % MODULE_NAME, new=Mock(return_value=[]))
  def test_runit_cardfile_readfailed( self ):
    self.fccAna.cardFiles = ["/path/to/cardFile"]
    card_file = os.path.realpath(self.fccAna.cardFiles[0])
    self.fccAna.RandomSeed = 1234
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
    self.fccAna.cardFiles = ["/path/to/cardFile"]
    card_file = os.path.realpath(self.fccAna.cardFiles[0])
    self.fccAna.RandomSeed = 1234
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

  @patch('__builtin__.open', new=Mock(side_effect=IOError()) )
  def test_readfromfile_failed( self ):
    content, message  = self.fccAna.readFromFile("/my/file/to/read")    
    assertEqualsImproved( None, content, self )   
    error_message = 'Application : Card file reading failed'
    assertEqualsImproved( error_message, message, self )   

  def test_readfromfile( self ):
    with patch('__builtin__.open') as mock_open:
      manager = mock_open.return_value.__enter__.return_value
      manager.read.return_value = 'some data'
      content, message  = self.fccAna.readFromFile("/my/file/to/read")
      assertEqualsImproved( content, 'some data', self )   
      mock_open.assert_called_with( "/my/file/to/read", 'r' )
      debug_message = 'Application : Card file reading successfull'
      assertEqualsImproved( message, debug_message, self )   
