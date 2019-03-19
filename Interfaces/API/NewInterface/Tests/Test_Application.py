#!/usr/local/env python
""" Test the ILCDIRAC Application module
"""

from __future__ import print_function
import inspect
import unittest
from mock import MagicMock as Mock, patch

from DIRAC import gLogger
from ILCDIRAC.Interfaces.API.NewInterface.Application import Application
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertDiracSucceeds, assertDiracSucceedsWith_equals

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.Application'

gLogger.setLevel("DEBUG")
gLogger.showHeaders(True)

#pylint: disable=protected-access
class TestApplication( unittest.TestCase ):
  """tests for the Application base interface"""

  def setUp( self ):
    self.app = Application()


  def tearDown( self ):
    pass

  def test_setParam_1( self ):
    """ test _setParam function """
    params = { 'Version': '01-00' }
    ret = self.app._setparams( params )
    self.assertTrue( ret['OK'], ret.get('Message','') )
    self.assertEqual( self.app.version, params['Version'] )

  def test_setParam_Fail_1( self ):
    """ test _setParam function, make sure getattr throws correct exception """
    params = { 'NotVersion': '01-00' }
    ret = self.app._setparams( params )
    self.assertTrue( ret['OK'], ret.get('Message','') )


#pylint: disable=protected-access
class AppTestCase( unittest.TestCase ):
  """ Test the ILCDIRAC Application class
  """

  def setUp(self):
    """set up the objects"""
    self.app = Application( {} )

  def test_repr( self ):
    app1 = Application( {} )
    app1.appname = 'MyTestApp'
    app1.version = 'v1'
    assertEqualsImproved( app1.__repr__(), 'MyTestApp v1', self )
    app2 = Application( {} )
    app2.appname = 'secondTestApp'
    app2.version = ''
    assertEqualsImproved( app2.__repr__(), 'secondTestApp', self )

  def test_setparams( self ):
    self.app._setparams( { 'Name' : 'mytestapp', 'Version' : 'tv1', 'SteeringFile' : 'test.steer',
                           'LogFile' : 'mylog.log', 'OutputFile' : 'test.out', 'OutputSE' : 'CERN-TEST',
                           'InputFile' : 'test.in' } )
    app = self.app
    assertEqualsImproved( ( app.appname, app.version, app.steeringFile, app.logFile, app.outputFile,
                            app.outputSE, app.inputFile ), ( 'mytestapp', 'tv1', 'test.steer', 'mylog.log',
                                                             'test.out', 'CERN-TEST', 'test.in' ), self )
    self.assertFalse( app._errorDict )

  def test_setparams_empty( self ):
    assertDiracSucceeds( self.app._setparams( {} ), self )

  def test_setparams_wrongtype( self ):
    assertDiracSucceeds( self.app._setparams( { 'Name' : 134, 'Version' : { False : True },
                                                'OutputSE' : True } ), self )
    assertEqualsImproved( len( self.app._errorDict['_checkArgs'] ), 3, self )

  def test_setparams_nonexistant_attribute( self ):
    log_mock = Mock()
    with patch.object(inspect.getmodule(Application), 'LOG', new=log_mock):
      assertDiracSucceeds(self.app._setparams({'ThisAttributeIsntPartOfApplication': 'someStuff'}), self)
    self.assertTrue( log_mock.error.called )

  def test_getparamsdict( self ):
    self.app = Application( { 'Name' : '', 'Version' : 'v18', 'SteeringFile' : 'testSteer',
                              'LogFile' : 'log.test', 'OutputFile' : 'out.test' } )
    result = self.app._getParamsDict()
    assertDiracSucceedsWith_equals( result, { 'version' : 'v18', 'steeringFile' : 'testSteer',
                                              'logFile' : 'log.test', 'outputFile' : 'out.test' }, self )

# Most setters already tested via _setparams tests which call setX
  def test_getinputfromapp( self ):
    app_mock = Mock()
    app_mock.appname = 'myCoolApp'
    self.assertFalse( self.app._inputapp )
    assertDiracSucceeds( self.app.getInputFromApp( app_mock ), self )
    assertEqualsImproved( self.app._inputapp, [ app_mock ], self )

  def test_listattributes( self ):
    log_mock = Mock()
    with patch.object(inspect.getmodule(Application), 'LOG', new=log_mock):
      self.app.setDebug(False)
      self.app.setExtraCLIArguments('my_custom_option other_option cl_argument')
      self.app.listAttributes()
    log_mock.notice.assert_any_call( '  debug: Not defined')
    log_mock.notice.assert_any_call( '  extraCLIArguments: my_custom_option other_option cl_argument' )

  def test_setters( self ):
    self.app.setSteeringFile( '/nonexistant/dir/mysteerfile')
    self.assertFalse( self.app.inputSB )
    self.app.setSteeringFile( 'lfn:/mydir/mysteerfile')
    assertEqualsImproved( self.app.inputSB, [ 'lfn:/mydir/mysteerfile' ], self )
    self.app.proparameters = {}
    self.app.detectortype = 'mytestdetector'
    self.app.datatype = 'mytestdatatype'
    assertDiracSucceeds( self.app.setOutputFile( 'mytestoutputfile', 'mytestpath' ), self )
    assertEqualsImproved( ( self.app.prodparameters, self.app.outputFile, self.app.outputPath ),
                          ( { 'mytestoutputfile' : { 'detectortype' : 'mytestdetector',
                                                     'datatype' : 'mytestdatatype' } }, 'mytestoutputfile',
                            'mytestpath' ), self )
    self.app.inputSB = []
    assertDiracFailsWith( self.app.setInputFile( True ),
                          'Problem with ILCDIRAC.Interfaces.API.NewInterface.Application.setInputFile', self )
    self.app.setInputFile( 'lfn:/mydir/input.file' )
    assertEqualsImproved( self.app.inputSB, [ 'lfn:/mydir/input.file' ], self )
    self.app.inputSB = []
    self.app.setInputFile( [ '/invalid/dir/myfile.txt', 'lfn:/valid/dir/myfile.txt' ] )
    assertEqualsImproved( self.app.inputSB, [ 'lfn:/valid/dir/myfile.txt' ], self )

  def test_private_methods( self ):
    with patch.object(self.app, '_applicationModule', new=Mock(return_value=Mock())) as module_mock:
      self.app._setApplicationModuleAndParameters( Mock() )
      self.assertTrue( module_mock.called )
    self.app._setOutputComputeDataList( Mock() )
    self.app._getComputeOutputDataListModule()
    self.app._applicationModule()
    self.app._applicationModuleValues( None )
    assertDiracFailsWith( self.app._userjobmodules( None ), 'not implemented', self )
    assertDiracFailsWith( self.app._prodjobmodules( None ), 'not implemented', self )
    assertDiracSucceeds( self.app._checkConsistency(), self )
    self.app._inputapp = [ 'myapp', 'failonthis' ]
    self.app._jobapps = [ 'myapp', 'irrelevant_app', 'other_app' ]
    assertDiracFailsWith( self.app._checkRequiredApp(), 'job order not correct', self )

def runTests():
  """Runs our tests"""
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestApplication )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  print(testResult)


if __name__ == '__main__':
  runTests()
