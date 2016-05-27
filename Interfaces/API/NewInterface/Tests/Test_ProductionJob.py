#!/usr/local/env python

"""
Test user jobfinalization

"""
__RCSID__ = "$Id$"

from mock import patch, mock_open, MagicMock as Mock
import unittest
from decimal import Decimal
from DIRAC import gLogger, S_OK, S_ERROR
from ILCDIRAC.Interfaces.API.NewInterface.ProductionJob import ProductionJob
from ILCDIRAC.Interfaces.API.NewInterface.Applications.DDSim import DDSim
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith
from ILCDIRAC.Tests.Utilities.FileUtils import FileUtil

gLogger.setLevel("DEBUG")
gLogger.showHeaders(True)

class ProductionJobTestCase( unittest.TestCase ):
  """ Base class for the ProductionJob test cases
  """
  def setUp(self):
    """set up the objects"""
    super(ProductionJobTestCase, self).setUp()
    self.prodJob = ProductionJob()
    self.prodJob.energy=250.0

  def test_Energy250( self ):
    """ProductionJob getEnergyPath 250gev..........................................................."""
    self.prodJob.energy = Decimal('250.0')
    res = self.prodJob.getEnergyPath()
    self.assertEqual( "250gev/", res )

  def test_Energy350( self ):
    """ProductionJob getEnergyPath 350gev..........................................................."""
    self.prodJob.energy = 350.0
    res = self.prodJob.getEnergyPath()
    self.assertEqual( "350gev/", res )

  def test_Energy3000( self ):
    """ProductionJob getEnergyPatt 3tev............................................................."""
    self.prodJob.energy = 3000.0
    res = self.prodJob.getEnergyPath()
    self.assertEqual( "3tev/", res )

  def test_Energy1400( self ):
    """ProductionJob getEnergyPath 1.4tev .........................................................."""
    self.prodJob.energy = 1400.0
    res = self.prodJob.getEnergyPath()
    self.assertEqual( "1.4tev/", res )

class ProductionJobCompleteTestCase( unittest.TestCase ): # pylint: disable=R0904
  """ Tests the rest of the ProductionJob TestCases """

  def setUp( self ):
    super( ProductionJobCompleteTestCase, self ).setUp()
    self.prodJob = ProductionJob()

  def test_setconfig( self ):
    # TODO add more checks on the result, espc. if the addParameter call was successful
    ver = '1481.30'
    res = self.prodJob.setConfig(ver)
    self.assertTrue(res['OK'])
    assertEqualsImproved(self.prodJob.prodparameters['ILDConfigVersion'], ver, self)

  def test_setJobFileGroupSize_normal( self ):
    # Basic setter method
    num = 4871
    self.prodJob.setJobFileGroupSize(num)
    assertEqualsImproved(self.prodJob.jobFileGroupSize, num, self)
    assertEqualsImproved(self.prodJob.prodparameters['NbInputFiles'], num, self)

  def test_setJobFileGroupSize_fails( self ):
    # Append before changing jobFileGroupSize, causing it to fail
    # Application can be arbitrary
    ddsim = DDSim()
    ddsim.setVersion('ILCSoft-01-17-09')
    ddsim.setDetectorModel('CLIC_o2_v03')
    ddsim.setNumberOfEvents(1)
    ddsim.setInputFile('Muon_50GeV_Fixed_cosTheta0.7.stdhep')
    # Set necessary parameters to call append() - job is never run
    self.prodJob.energy = 250.0
    self.prodJob.evttype = 'electron party'
    self.prodJob.outputStorage = 'CERN-EOS-DST'
    with patch('ILCDIRAC.Interfaces.API.NewInterface.Applications.DDSim._analyseJob', new=Mock(return_value=S_OK())), patch('ILCDIRAC.Interfaces.API.NewInterface.Applications.DDSim._checkConsistency', new=Mock(return_value=S_OK())), patch('ILCDIRAC.Interfaces.API.NewInterface.Applications.DDSim._checkFinalConsistency', new=Mock(return_value=S_OK())):
      res = self.prodJob.append(ddsim)
      self.assertTrue(res['OK'])
    res = self.prodJob.setJobFileGroupSize(1389)
    self.assertFalse(res['OK'])
    self.assertIn('input is needed at the beginning', res['Message'].lower())

  def test_setInputDataQuery( self ):
    with patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getMetadataFields', new=Mock(return_value=S_OK({'DirectoryMetaFields' : { 'ProdID' : 19872456 }}))), patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.findDirectoriesByMetadata', new=Mock(side_effect=[S_OK(['dir1','dir2']), S_OK({'abc' : 'testdir123'})])),  patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getDirectoryUserMetadata', new=Mock(return_value=S_OK({ 'EvtType' : 'electron party'}))):
      self.prodJob.energycat='7'
      res = self.prodJob.setInputDataQuery({'ProdID' : 19872456})
      msg = ''
      if 'Message' in res:
        msg = res['Message']
      self.assertTrue(res['OK'], msg)
      #TODO check output of method
      assertEqualsImproved( self.prodJob.energy, Decimal('7'), self )

  def test_setInputDataQuery_finddirfails( self ):
    with patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.findDirectoriesByMetadata', new=Mock(return_value=S_ERROR('failed getting metadata fields'))):
      res = self.prodJob.setInputDataQuery({'ProdID' : 19872456})
      self.assertFalse(res['OK'])
      assertEqualsImproved(res['Message'], 'failed getting metadata fields', self)

  def test_setInputDataQuery_finddirempty( self ):
    with patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.findDirectoriesByMetadata', new=Mock(return_value=S_OK([]))):
      res = self.prodJob.setInputDataQuery({'ProdID' : 19872456})
      self.assertFalse(res['OK'])
      self.assertIn( 'no directories found', res['Message'].lower() )

  def test_setInputDataQuery_getmetadatafails( self ):
    with patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getMetadataFields', new=Mock(return_value=S_ERROR('some_error'))), patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.findDirectoriesByMetadata', new=Mock(side_effect=[S_OK(['dir1','dir2']), S_OK({'abc' : 'testdir123'})])):
      res = self.prodJob.setInputDataQuery({'ProdID' : 19872456})
      self.assertFalse(res['OK'])
      self.assertIn('could not contact file catalog', res['Message'].lower() )

  def test_setInputDataQuery_filecatalogWrongCase( self ):
    with patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getMetadataFields', new=Mock(return_value=S_OK({'DirectoryMetaFields' : { 'prodid' : 19872456 }}))), patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.findDirectoriesByMetadata', new=Mock(side_effect=[S_OK(['dir1','dir2']), S_OK({'abc' : 'testdir123'})])):
      res = self.prodJob.setInputDataQuery({'ProdID' : 19872456})
      self.assertFalse(res['OK'])
      self.assertIn('key syntax error', res['Message'].lower() )

  def test_setInputDataQuery_filecatalogMissingKey( self ):
    with patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getMetadataFields', new=Mock(return_value=S_OK({'DirectoryMetaFields' : { 'ProdID' : 19872456 }}))), patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.findDirectoriesByMetadata', new=Mock(side_effect=[S_OK(['dir1','dir2']), S_OK({'abc' : 'testdir123'})])),  patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getDirectoryUserMetadata', new=Mock(return_value=S_OK({ 'EvtType' : 'electron party'}))):
      # Key not present in getMetadataFields return value
      randomkey = '09428ituj42itufgm'
      res = self.prodJob.setInputDataQuery( {'ProdID' : 19872456, randomkey : 'testvalue'} )
      self.assertFalse( res['OK'] )
      self.assertIn( 'key %s not found in metadata keys' % randomkey, res['Message'].lower() )

  def test_setInputDataQuery_noprodid( self ):
    with patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getMetadataFields', new=Mock(return_value=S_OK({'DirectoryMetaFields' : { 'ProdID' : 19872456 }}))), patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.findDirectoriesByMetadata', new=Mock(side_effect=[S_OK(['dir1','dir2']), S_OK({'abc' : 'testdir123'})])),  patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getDirectoryUserMetadata', new=Mock(return_value=S_OK({ 'EvtType' : 'electron party'}))):
      res = self.prodJob.setInputDataQuery({})
      self.assertFalse( res['OK'] )
      self.assertIn( "input metadata dictionary must contain at least a key 'prodid' as reference", res['Message'].lower() )

  def test_setInputDataQuery_second_finddir_fails( self ):
    with patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getMetadataFields', new=Mock(return_value=S_OK({'DirectoryMetaFields' : { 'ProdID' : 19872456 }}))), patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.findDirectoriesByMetadata', new=Mock(side_effect=[S_OK(['dir1','dir2']), S_ERROR('some_error')])):
      res = self.prodJob.setInputDataQuery({'ProdID' : 19872456})
      self.assertFalse( res['OK'] )
      self.assertIn( 'error looking up the catalog', res['Message'].lower() )

  def test_setInputDataQuery_second_finddir_invalid( self ):
    with patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getMetadataFields', new=Mock(return_value=S_OK({'DirectoryMetaFields' : { 'ProdID' : 19872456 }}))), patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.findDirectoriesByMetadata', new=Mock(side_effect=[S_OK(['dir1','dir2']), S_OK({})])):
      res = self.prodJob.setInputDataQuery({'ProdID' : 19872456})
      self.assertFalse( res['OK'] )
      self.assertIn( 'could not find any directories', res['Message'].lower() )

  def test_setInputDataQuery_getdirusermetadata_fails( self ):
    with patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getMetadataFields', new=Mock(return_value=S_OK({'DirectoryMetaFields' : { 'ProdID' : 19872456 }}))), patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.findDirectoriesByMetadata', new=Mock(side_effect=[S_OK(['dir1','dir2']), S_OK({'abc' : 'testdir123'})])),  patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getDirectoryUserMetadata', new=Mock(return_value=S_ERROR('some_error'))):
      res = self.prodJob.setInputDataQuery({'ProdID' : 19872456})
      self.assertFalse( res['OK'] )
      self.assertIn( 'error looking up the catalog for directory metadata', res['Message'].lower() )

  def test_setInputDataQuery_getenergyfromcompatmeta_1( self ):
    with patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getMetadataFields', new=Mock(return_value=S_OK({'DirectoryMetaFields' : { 'ProdID' : 19872456 }}))), patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.findDirectoriesByMetadata', new=Mock(side_effect=[S_OK(['dir1','dir2']), S_OK({'abc' : 'testdir123'})])),  patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getDirectoryUserMetadata', new=Mock(return_value=S_OK({ 'EvtType' : ['electron party'], 'Energy' : '13gev' }))):
      res = self.prodJob.setInputDataQuery({'ProdID' : 19872456})
      self.assertTrue(res['OK'])
      assertEqualsImproved( self.prodJob.energy, Decimal('13'), self )

  def test_setInputDataQuery_getenergyfromcompatmeta_2( self ):
    with patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getMetadataFields', new=Mock(return_value=S_OK({'DirectoryMetaFields' : { 'ProdID' : 19872456 }}))), patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.findDirectoriesByMetadata', new=Mock(side_effect=[S_OK(['dir1','dir2']), S_OK({'abc' : 'testdir123'})])),  patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getDirectoryUserMetadata', new=Mock(return_value=S_OK({ 'EvtType' : 'electron party', 'Energy' : ['13tev'] }))):
      res = self.prodJob.setInputDataQuery({'ProdID' : 19872456})
      self.assertTrue(res['OK'])
      assertEqualsImproved( self.prodJob.energy, Decimal('13000'), self )

  def test_setInputDataQuery_getenergyfromcompatmeta_3( self ):
    with patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getMetadataFields', new=Mock(return_value=S_OK({'DirectoryMetaFields' : { 'ProdID' : 19872456 }}))), patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.findDirectoriesByMetadata', new=Mock(side_effect=[S_OK(['dir1','dir2']), S_OK({'abc' : 'testdir123'})])),  patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getDirectoryUserMetadata', new=Mock(return_value=S_OK({ 'EvtType' : 'electron party', 'Energy' : 13 }))):
      res = self.prodJob.setInputDataQuery({'ProdID' : 19872456})
      self.assertTrue(res['OK'])
      assertEqualsImproved( self.prodJob.energy, Decimal('13'), self )

  def test_setInputDataQuery_noevttype( self ):
    with patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getMetadataFields', new=Mock(return_value=S_OK({'DirectoryMetaFields' : { 'ProdID' : 19872456 }}))), patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.findDirectoriesByMetadata', new=Mock(side_effect=[S_OK(['dir1','dir2']), S_OK({'abc' : 'testdir123'})])),  patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getDirectoryUserMetadata', new=Mock(return_value=S_OK({}))):
      res = self.prodJob.setInputDataQuery({'ProdID' : 19872456})
      self.assertFalse(res['OK'])
      self.assertIn('evttype is not in the metadata', res['Message'].lower())

  def test_setInputDataQuery_numofevts_1( self ):
    with patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getMetadataFields', new=Mock(return_value=S_OK({'DirectoryMetaFields' : { 'ProdID' : 19872456, 'NumberOfEvents' : 'testsuihe123' }}))), patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.findDirectoriesByMetadata', new=Mock(side_effect=[S_OK(['dir1','dir2']), S_OK({'abc' : 'testdir123'})])),  patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getDirectoryUserMetadata', new=Mock(return_value=S_OK({ 'EvtType' : 'electron party', 'Energy' : 13 }))):
      res = self.prodJob.setInputDataQuery({'ProdID' : 19872456, 'NumberOfEvents' : ['42985']})
      self.assertTrue(res['OK'])
      assertEqualsImproved(self.prodJob.nbevts, 42985, self)

  def test_setInputDataQuery_numofevts_2( self ):
    with patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getMetadataFields', new=Mock(return_value=S_OK({'DirectoryMetaFields' : { 'ProdID' : 19872456, 'NumberOfEvents' : 'testabc' }}))), patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.findDirectoriesByMetadata', new=Mock(side_effect=[S_OK(['dir1','dir2']), S_OK({'abc' : 'testdir123'})])),  patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getDirectoryUserMetadata', new=Mock(return_value=S_OK({ 'EvtType' : 'electron party', 'Energy' : 13 }))):
      res = self.prodJob.setInputDataQuery({'ProdID' : 19872456, 'NumberOfEvents' : '968541'})
      self.assertTrue(res['OK'])
      assertEqualsImproved(self.prodJob.nbevts, 968541, self)

  def test_setInputDataQuery_datatype_1( self ):
    with patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getMetadataFields', new=Mock(return_value=S_OK({'DirectoryMetaFields' : { 'ProdID' : 19872456, 'Datatype' : 'test123', 'DetectorType' : 'testdetector' }}))), patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.findDirectoriesByMetadata', new=Mock(side_effect=[S_OK(['dir1','dir2']), S_OK({'abc' : 'testdir123'})])),  patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getDirectoryUserMetadata', new=Mock(return_value=S_OK({ 'EvtType' : 'electron party'}))):
      self.prodJob.energycat='7'
      res = self.prodJob.setInputDataQuery({'ProdID' : 19872456, 'Datatype' : 'mytype', 'DetectorType' : 'GoodDetector874'})
      self.assertTrue(res['OK'])
      assertEqualsImproved( self.prodJob.datatype, 'mytype', self )
      assertEqualsImproved( self.prodJob.detector, 'GoodDetector874', self )

  def test_setInputDataQuery_datatype_2( self ):
    with patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getMetadataFields', new=Mock(return_value=S_OK({'DirectoryMetaFields' : { 'ProdID' : 19872456, 'Datatype' : 'test123', 'DetectorType' : 'testdetector' }}))), patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.findDirectoriesByMetadata', new=Mock(side_effect=[S_OK(['dir1','dir2']), S_OK({'abc' : 'testdir123'})])),  patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getDirectoryUserMetadata', new=Mock(return_value=S_OK({ 'EvtType' : 'electron party'}))):
      self.prodJob.energycat='7'
      res = self.prodJob.setInputDataQuery({'ProdID' : 19872456, 'Datatype' : 'gen', 'DetectorType' : 'abc'})
      self.assertTrue(res['OK'])
      assertEqualsImproved( self.prodJob.datatype, 'gen', self )
      assertEqualsImproved( self.prodJob.detector, '', self )

  def test_setInputDataQuery_datatype_list1( self ):
    with patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getMetadataFields', new=Mock(return_value=S_OK({'DirectoryMetaFields' : { 'ProdID' : 19872456, 'Datatype' : 'test123', 'DetectorType' : 'testdetector' }}))), patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.findDirectoriesByMetadata', new=Mock(side_effect=[S_OK(['dir1','dir2']), S_OK({'abc' : 'testdir123'})])),  patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getDirectoryUserMetadata', new=Mock(return_value=S_OK({ 'EvtType' : 'electron party'}))):
      self.prodJob.energycat='7'
      res = self.prodJob.setInputDataQuery({'ProdID' : 19872456, 'Datatype' : ['mytype'], 'DetectorType' : ['MyDetector3000']})
      self.assertTrue(res['OK'])
      assertEqualsImproved( self.prodJob.datatype, 'mytype', self )
      assertEqualsImproved( self.prodJob.detector, 'MyDetector3000', self )

  def test_setInputDataQuery_datatype_list2( self ):
    with patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getMetadataFields', new=Mock(return_value=S_OK({'DirectoryMetaFields' : { 'ProdID' : 19872456, 'Datatype' : 'test123', 'DetectorType' : 'testdetector' }}))), patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.findDirectoriesByMetadata', new=Mock(side_effect=[S_OK(['dir1','dir2']), S_OK({'abc' : 'testdir123'})])),  patch('DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient.getDirectoryUserMetadata', new=Mock(return_value=S_OK({ 'EvtType' : 'electron party'}))):
      self.prodJob.energycat='7'
      res = self.prodJob.setInputDataQuery({'ProdID' : 19872456, 'Datatype' : ['gen'], 'DetectorType' : '904215fadf'})
      self.assertTrue(res['OK'])
      assertEqualsImproved( self.prodJob.datatype, 'gen', self )
      assertEqualsImproved( self.prodJob.detector, '', self )

  def test_createproduction( self ):
    job = self.prodJob
    job.proxyinfo = { 'OK' : 'yes, trust me', 'Value' : {'group' : 'ilc_prod'} }
    job.created = False
    job.inputdataquery = True
    job.slicesize = 10
    job.inputBKSelection = True
    job.call_finalization = True
    job.prodparameters['ILDConfigVersion'] = 'goodversion1.215'
    job.nbevts = 89134
    job.finalpaths = [ 'test/path/my' ]
    job.workflow.setName('mytestworkflow')
    job.finalsdict = { 'uploadData' : 'myuploaddata', 'registerData' : 'myregisterdata', 'uploadLog' : 'myuploadlog', 'sendFailover' : 'mysendfailover' }
    file_contents = [[], ["I'm an XML file"]]
    handles = FileUtil.getMultipleReadHandles(file_contents)
    moduleName = 'ILCDIRAC.Interfaces.API.NewInterface.ProductionJob'
    with patch('__builtin__.open', mock_open(), create=True) as mo, patch('%s.Transformation.addTransformation' % moduleName, new=Mock(return_value=S_OK())):
      mo.side_effect = (h for h in handles)
      job.description = 'MyTestDescription'
      res = job.createProduction( 'goodtestname' )
      self.assertTrue( res['OK'] )
      mo.assert_any_call( 'mytestworkflow.xml', 'r' )
      expected = [[EXPECTED_XML], []]
      self.assertEquals(len(file_contents), len(expected))
      for (index, handle) in enumerate(handles):
        cur_handle = handle.__enter__()
        self.assertEquals(len(expected[index]), handle.__enter__.return_value.write.call_count)
        for entry in expected[index]:
          cur_handle.write.assert_any_call(entry)

  def test_createproduction_2( self ):
    job = self.prodJob
    job.proxyinfo = { 'OK' : 'yes, trust me', 'Value' : {'group' : 'ilc_prod'} }
    job.created = False
    job.inputdataquery = True
    job.slicesize = 10
    job.inputBKSelection = True
    job.call_finalization = True
    job.finalpaths = [ 'test/path/my' ]
    job.workflow.setName('mytestworkflow')
    job.finalsdict = { 'uploadData' : 'myuploaddata', 'registerData' : 'myregisterdata', 'uploadLog' : 'myuploadlog', 'sendFailover' : 'mysendfailover' }
    file_contents = [[], ["I'm an XML file"]]
    handles = FileUtil.getMultipleReadHandles(file_contents)
    moduleName = 'ILCDIRAC.Interfaces.API.NewInterface.ProductionJob'
    with patch('__builtin__.open', mock_open(), create=True) as mo, patch('%s.Transformation.addTransformation' % moduleName, new=Mock(return_value=S_OK())):
      mo.side_effect = (h for h in handles)
      job.description = 'MyTestDescription'
      res = job.createProduction( 'goodtestname' )
      self.assertTrue( res['OK'] )
      mo.assert_any_call( 'mytestworkflow.xml', 'r' )
      expected = [[EXPECTED_XML], []]
      self.assertEquals(len(file_contents), len(expected))
      for (index, handle) in enumerate(handles):
        cur_handle = handle.__enter__()
        self.assertEquals(len(expected[index]), handle.__enter__.return_value.write.call_count)
        for entry in expected[index]:
          cur_handle.write.assert_any_call(entry)

  def test_createproduction_nofinalization( self ):
    job = self.prodJob
    job.proxyinfo = { 'OK' : 'yes, trust me', 'Value' : {'group' : 'ilc_prod'} }
    job.created = False
    job.call_finalization = False
    job.workflow.setName('mytestworkflow')
    job.finalsdict = { 'uploadData' : 'myuploaddata', 'registerData' : 'myregisterdata', 'uploadLog' : 'myuploadlog', 'sendFailover' : 'mysendfailover' }
    file_contents = [[], ["I'm an XML file"]]
    handles = FileUtil.getMultipleReadHandles(file_contents)
    moduleName = 'ILCDIRAC.Interfaces.API.NewInterface.ProductionJob'
    with patch('__builtin__.open', mock_open(), create=True) as mo, patch('%s.Transformation.addTransformation' % moduleName, new=Mock(return_value=S_OK())):
      mo.side_effect = (h for h in handles)
      job.description = 'MyTestDescription'
      res = job.createProduction()
      self.assertTrue(res['OK'])
      mo.assert_any_call('mytestworkflow.xml', 'r')
      expected = [[EXPECTED_XML_NOFINAL], []]
      self.assertEquals(len(file_contents), len(expected))
      for (index, handle) in enumerate(handles):
        cur_handle = handle.__enter__()
        print cur_handle.write.mock_calls
        self.assertEquals(len(expected[index]), handle.__enter__.return_value.write.call_count)
        for entry in expected[index]:
          cur_handle.write.assert_any_call(entry)

  @patch('__builtin__.open', mock_open(), create=True)
  def test_createproduction_basic_checks( self ):
    job = self.prodJob
    job.proxyinfo = { 'OK' : False, 'Value' : {'group' : 'ilc_prod'}, 'Message' : 'not ok' }
    res = job.createProduction()
    assertDiracFailsWith( res, 'you need a ilc_prod proxy', self )
    job.proxyinfo = { 'OK' : True, 'Value' : {} }
    res = job.createProduction()
    assertDiracFailsWith( res, 'could not determine group', self )
    job.proxyinfo = { 'OK' : True, 'Value' : { 'group' : 'LHCz'} }
    res = job.createProduction()
    assertDiracFailsWith( res, 'not allowed to create production', self )
    job.created = True
    job.proxyinfo = { 'OK' : True, 'Value' : {'group' : 'ilc_prod'} }
    res = job.createProduction()
    assertDiracFailsWith( res, 'already created', self )
    job.created = False
    with patch('ILCDIRAC.Interfaces.API.NewInterface.ProductionJob.ProductionJob._addToWorkflow', new=Mock(return_value=S_ERROR('some_error'))):
      assertDiracFailsWith( job.createProduction(), 'some_error', self )
    with patch('ILCDIRAC.Interfaces.API.NewInterface.ProductionJob.ProductionJob.createWorkflow', new=Mock(side_effect=OSError('some_os_error'))):
      assertDiracFailsWith( job.createProduction(), 'could not create workflow', self )
    with patch('ILCDIRAC.Interfaces.API.NewInterface.ProductionJob.Transformation.addTransformation', new=Mock(return_value=S_ERROR('myerror123'))), patch('ILCDIRAC.Interfaces.API.NewInterface.ProductionJob.open', mock_open(), create=True):
      assertDiracFailsWith( job.createProduction(), 'myerror123', self )
    job.trc = Mock()
    job.trc.getTransformationStats.return_value = S_OK('fail this') #S_OK because it means it found a transformation by that name, so the new one cannot be created
    with patch('ILCDIRAC.Interfaces.API.NewInterface.ProductionJob.open', mock_open(), create=True):
      assertDiracFailsWith( job.createProduction(), 'already exists', self )

  @patch('__builtin__.open', mock_open(), create=True)
  def test_createproduction_dryrun( self ):
    job = self.prodJob
    job.proxyinfo = { 'OK' : 'yes, trust me', 'Value' : {'group' : 'ilc_prod'} }
    job.created = False
    job.dryrun = True
    job.call_finalization = True
    job.workflow.setName('mytestworkflow')
    job.finalsdict = { 'uploadData' : 'myuploaddata', 'registerData' : 'myregisterdata', 'uploadLog' : 'myuploadlog', 'sendFailover' : 'mysendfailover' }
    file_contents = [["I'm an XML file"]]
    handles = FileUtil.getMultipleReadHandles(file_contents)
    moduleName = 'ILCDIRAC.Interfaces.API.NewInterface.ProductionJob'
    with patch('%s.open' % moduleName, mock_open(), create=True) as mo, patch('%s.Transformation.addTransformation' % moduleName, new=Mock(return_value=S_OK())):
      mo.side_effect = (h for h in handles)
      job.description = 'MyTestDescription'
      res = job.createProduction( 'goodtestname' )
      self.assertTrue( res['OK'] )
      mo.assert_any_call( 'mytestworkflow.xml', 'r' )
      expected = [[]]
      self.assertEquals(len(file_contents), len(expected))
      for (index, handle) in enumerate(handles):
        cur_handle = handle.__enter__()
        self.assertEquals(len(expected[index]), handle.__enter__.return_value.write.call_count)
        for entry in expected[index]:
          cur_handle.write.assert_any_call(entry)

  def test_setNbOfTasks( self ):
    assertDiracFailsWith( self.prodJob.setNbOfTasks(5), 'no transformation defined', self )
    self.prodJob.currtrans = Mock()
    self.prodJob.inputBKSelection = True
    assertDiracFailsWith( self.prodJob.setNbOfTasks(2), '', self ) #Returns empty S_ERROR, probably should add at least error message
    self.prodJob.inputBKSelection = False
    testNbTasks = 1375
    res = self.prodJob.setNbOfTasks( testNbTasks )
    self.assertTrue( res['OK'] )
    assertEqualsImproved( self.prodJob.nbtasks, testNbTasks, self )
    self.prodJob.currtrans.setMaxNumberOfTasks.assert_called_with( testNbTasks )

  # Methods to test
  # setNbOfTasks
  # applyInputDataQuery
  # addMetadataToFinalFiles
  # finalizeProd
  # getMetadata
  # getEnergyPath


  # Setters to test in bulk
  # setDryRun
  # setProdGroup
  # setProdPlugin
  # setNbEvtsPerSlice
  # setProdType
  # setWorkflowName
  # setWorkflowDescription
  # createWorkflow
  # setOutputSE

EXPECTED_XML = '<Workflow>\n<origin></origin>\n<description><![CDATA[]]></description>\n<descr_short></descr_short>\n<version>0.0</version>\n<type></type>\n<name>mytestworkflow</name>\n<Parameter name="JobType" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Job Type"><value><![CDATA[User]]></value></Parameter>\n<Parameter name="Priority" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Priority"><value><![CDATA[1]]></value></Parameter>\n<Parameter name="JobGroup" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified job group"><value><![CDATA[@{PRODUCTION_ID}]]></value></Parameter>\n<Parameter name="JobName" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Name of Job"><value><![CDATA[Name]]></value></Parameter>\n<Parameter name="Site" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Site Requirement"><value><![CDATA[ANY]]></value></Parameter>\n<Parameter name="Origin" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Origin of client"><value><![CDATA[DIRAC]]></value></Parameter>\n<Parameter name="StdOutput" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Standard output file"><value><![CDATA[std.out]]></value></Parameter>\n<Parameter name="StdError" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Standard error file"><value><![CDATA[std.err]]></value></Parameter>\n<Parameter name="InputData" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Default null input data value"><value><![CDATA[]]></value></Parameter>\n<Parameter name="LogLevel" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified logging level"><value><![CDATA[verbose]]></value></Parameter>\n<Parameter name="arguments" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Arguments to executable Step"><value><![CDATA[]]></value></Parameter>\n<Parameter name="ParametricInputData" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Default null parametric input data value"><value><![CDATA[]]></value></Parameter>\n<Parameter name="ParametricInputSandbox" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Default null parametric input sandbox value"><value><![CDATA[]]></value></Parameter>\n<Parameter name="IS_PROD" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="This job is a production job"><value><![CDATA[True]]></value></Parameter>\n<Parameter name="MaxCPUTime" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="CPU time in secs"><value><![CDATA[300000]]></value></Parameter>\n<Parameter name="CPUTime" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="CPU time in secs"><value><![CDATA[300000]]></value></Parameter>\n<Parameter name="productionVersion" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProdAPIVersion"><value><![CDATA[$Id$]]></value></Parameter>\n<Parameter name="PRODUCTION_ID" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProductionID"><value><![CDATA[00012345]]></value></Parameter>\n<Parameter name="JOB_ID" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProductionJobID"><value><![CDATA[00012345]]></value></Parameter>\n<Parameter name="emailAddress" type="string" linked_module="" linked_parameter="" in="True" out="False" description="CrashEmailAddress"><value><![CDATA[ilcdirac-support@cern.ch]]></value></Parameter>\n<ModuleDefinition>\n<body><![CDATA[from ILCDIRAC.Workflow.Modules.UploadOutputData import UploadOutputData]]></body>\n<origin></origin>\n<description><![CDATA[Uploads the output data]]></description>\n<descr_short></descr_short>\n<required></required>\n<version>0.0</version>\n<type>UploadOutputData</type>\n<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[False]]></value></Parameter>\n</ModuleDefinition>\n<ModuleDefinition>\n<body><![CDATA[from ILCDIRAC.Workflow.Modules.RegisterOutputData import RegisterOutputData]]></body>\n<origin></origin>\n<description><![CDATA[Module to add in the metadata catalog the relevant info about the files]]></description>\n<descr_short></descr_short>\n<required></required>\n<version>0.0</version>\n<type>RegisterOutputData</type>\n<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[False]]></value></Parameter>\n</ModuleDefinition>\n<ModuleDefinition>\n<body><![CDATA[from ILCDIRAC.Workflow.Modules.UploadLogFile import UploadLogFile]]></body>\n<origin></origin>\n<description><![CDATA[Uploads the output log files]]></description>\n<descr_short></descr_short>\n<required></required>\n<version>0.0</version>\n<type>UploadLogFile</type>\n<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[False]]></value></Parameter>\n</ModuleDefinition>\n<ModuleDefinition>\n<body><![CDATA[from ILCDIRAC.Workflow.Modules.FailoverRequest import FailoverRequest]]></body>\n<origin></origin>\n<description><![CDATA[Sends any failover requests]]></description>\n<descr_short></descr_short>\n<required></required>\n<version>0.0</version>\n<type>FailoverRequest</type>\n<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[False]]></value></Parameter>\n</ModuleDefinition>\n<StepDefinition>\n<origin></origin>\n<version>0.0</version>\n<type>Job_Finalization</type>\n<description><![CDATA[]]></description>\n<descr_short></descr_short>\n<ModuleInstance>\n<type>UploadOutputData</type>\n<name>dataUpload</name>\n<descr_short></descr_short>\n<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[True]]></value></Parameter>\n</ModuleInstance>\n<ModuleInstance>\n<type>RegisterOutputData</type>\n<name>RegisterOutputData</name>\n<descr_short></descr_short>\n<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[True]]></value></Parameter>\n</ModuleInstance>\n<ModuleInstance>\n<type>UploadLogFile</type>\n<name>logUpload</name>\n<descr_short></descr_short>\n<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[True]]></value></Parameter>\n</ModuleInstance>\n<ModuleInstance>\n<type>FailoverRequest</type>\n<name>failoverRequest</name>\n<descr_short></descr_short>\n<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[True]]></value></Parameter>\n</ModuleInstance>\n</StepDefinition>\n<StepInstance>\n<type>Job_Finalization</type>\n<name>finalization</name>\n<descr_short></descr_short>\n</StepInstance>\n</Workflow>\n'

EXPECTED_XML_NOFINAL = '<Workflow>\n<origin></origin>\n<description><![CDATA[]]></description>\n<descr_short></descr_short>\n<version>0.0</version>\n<type></type>\n<name>mytestworkflow</name>\n<Parameter name="JobType" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Job Type"><value><![CDATA[User]]></value></Parameter>\n<Parameter name="Priority" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Priority"><value><![CDATA[1]]></value></Parameter>\n<Parameter name="JobGroup" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified job group"><value><![CDATA[@{PRODUCTION_ID}]]></value></Parameter>\n<Parameter name="JobName" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Name of Job"><value><![CDATA[Name]]></value></Parameter>\n<Parameter name="Site" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Site Requirement"><value><![CDATA[ANY]]></value></Parameter>\n<Parameter name="Origin" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Origin of client"><value><![CDATA[DIRAC]]></value></Parameter>\n<Parameter name="StdOutput" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Standard output file"><value><![CDATA[std.out]]></value></Parameter>\n<Parameter name="StdError" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Standard error file"><value><![CDATA[std.err]]></value></Parameter>\n<Parameter name="InputData" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Default null input data value"><value><![CDATA[]]></value></Parameter>\n<Parameter name="LogLevel" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified logging level"><value><![CDATA[verbose]]></value></Parameter>\n<Parameter name="arguments" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Arguments to executable Step"><value><![CDATA[]]></value></Parameter>\n<Parameter name="ParametricInputData" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Default null parametric input data value"><value><![CDATA[]]></value></Parameter>\n<Parameter name="ParametricInputSandbox" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Default null parametric input sandbox value"><value><![CDATA[]]></value></Parameter>\n<Parameter name="IS_PROD" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="This job is a production job"><value><![CDATA[True]]></value></Parameter>\n<Parameter name="MaxCPUTime" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="CPU time in secs"><value><![CDATA[300000]]></value></Parameter>\n<Parameter name="CPUTime" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="CPU time in secs"><value><![CDATA[300000]]></value></Parameter>\n<Parameter name="productionVersion" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProdAPIVersion"><value><![CDATA[$Id$]]></value></Parameter>\n<Parameter name="PRODUCTION_ID" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProductionID"><value><![CDATA[00012345]]></value></Parameter>\n<Parameter name="JOB_ID" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProductionJobID"><value><![CDATA[00012345]]></value></Parameter>\n<Parameter name="emailAddress" type="string" linked_module="" linked_parameter="" in="True" out="False" description="CrashEmailAddress"><value><![CDATA[ilcdirac-support@cern.ch]]></value></Parameter>\n</Workflow>\n'

def runTests():
  """Runs our tests"""
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ProductionJobTestCase )
  
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  print testResult


if __name__ == '__main__':
  runTests()
