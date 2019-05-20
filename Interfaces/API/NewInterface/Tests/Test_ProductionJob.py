"""Test ProductionJob."""

from __future__ import print_function
import unittest
from decimal import Decimal
from mock import patch, mock_open, MagicMock as Mock

from parameterized import parameterized

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from ILCDIRAC.Interfaces.API.NewInterface.ProductionJob import ProductionJob
from ILCDIRAC.Interfaces.API.NewInterface.Applications.DDSim import DDSim
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertDiracSucceeds
from ILCDIRAC.Tests.Utilities.FileUtils import FileUtil

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.ProductionJob'
DDSIM_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.Applications.DDSim'

gLogger.setLevel("DEBUG")
gLogger.showHeaders(True)

class ProductionJobTestCase( unittest.TestCase ):
  """ Base class for the ProductionJob test cases
  """
  def setUp(self):
    """set up the objects"""
    super(ProductionJobTestCase, self).setUp()
    with patch.object(ProductionJob, 'setPlatform', new=Mock(S_OK())):
      self.prodJob = ProductionJob()
    self.prodJob.trc = Mock(name="TRC")
    self.prodJob.trc.getTransformationStats.return_value = S_ERROR('Production does not exist yet')
    self.prodJob.energy=250.0
    self.prodJob.fc = Mock('FileCatalogMock')
    self.prodJob.fc.isDirectory = Mock(name='isDir')
    self.prodJob.fc.isDirectory.side_effect = isdir_sideeffect
    self.prodJob.fc.getMetadataFields = Mock(name='getMeta')
    self.prodJob.fc.createDirectory = Mock(name='mkdir')
    self.prodJob.fc.changePathMode = Mock(name='chmod')
    self.prodJob.fc.setMetadata = Mock(name='SMD')
    self.prodJob.fc.getDirectoryUserMetadata = Mock(name='GDUMD')

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

  def test_Energy43222(self):
    """Test ProductionJob getEnergyPath 432.22gev."""
    self.prodJob.energy = 432.22
    res = self.prodJob.getEnergyPath()
    self.assertEqual("432.22gev/", res)


#FIXME Add checks that the important lines were really executed
class ProductionJobCompleteTestCase( unittest.TestCase ):
  """ Tests the rest of the ProductionJob TestCases """

  def setUp( self ):
    super( ProductionJobCompleteTestCase, self ).setUp()
    with patch.object(ProductionJob, 'setPlatform', new=Mock(S_OK())):
      self.prodJob = ProductionJob()

  def test_setconfig( self ):
    # TODO add more checks on the result, espc. if the addParameter call was successful
    ver = '1481.30'
    res = self.prodJob.setConfig(ver)
    assertDiracSucceeds( res, self )
    assertEqualsImproved(self.prodJob.prodparameters['ILDConfigVersion'], ver, self)


  def test_setclicconfig( self ):
    ver = '2017-99-99'
    res = self.prodJob.setCLICConfig(ver)
    assertDiracSucceeds( res, self )
    assertEqualsImproved(self.prodJob.prodparameters['ClicConfigVersion'], ver, self)


class ProductionJobSetJobFileGroupSizeTest( ProductionJobTestCase ):
  """ Tests the setJobFileGroupSize method
  """

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
    with patch('%s._analyseJob' % DDSIM_NAME, new=Mock(return_value=S_OK())), \
         patch('%s._checkConsistency' % DDSIM_NAME, new=Mock(return_value=S_OK())), \
         patch('%s._checkFinalConsistency' % DDSIM_NAME, new=Mock(return_value=S_OK())):
      res = self.prodJob.append(ddsim)
      assertDiracSucceeds( res, self )
    res = self.prodJob.setJobFileGroupSize(1389)
    assertDiracFailsWith( res, 'input is needed at the beginning', self )

class ProductionJobSetInputDataQuery( ProductionJobTestCase ):
  """ Tests the setInputDataQuery method
  """

  @classmethod
  def setUpClass(cls):
    ProductionJobSetInputDataQuery.HAS_PROXY = getProxyInfo()['OK']

  def test_setInputDataQuery(self):
    """Test the inputDataQuery method."""
    self.prodJob.fc.getMetadataFields = Mock(return_value=S_OK({'DirectoryMetaFields': {'ProdID': 19872456}}))
    self.prodJob.fc.findDirectoriesByMetadata = Mock(return_value=S_OK({'abc': 'testdir123'}))
    self.prodJob.fc.getDirectoryUserMetadata = Mock(return_value=S_OK({'EvtType': 'electron party'}))
    self.prodJob.energycat = '7'
    res = self.prodJob.setInputDataQuery({'ProdID': 19872456})
    assertDiracSucceeds(res, self)
    # TODO check output of method
    assertEqualsImproved(self.prodJob.energy, Decimal('7'), self)

  # TODO fix here
  def test_setInputDataQuery_getmetadatafails( self ):
    """Test the inputDataQuery when getmetadata fails."""
    self.prodJob.fc.getMetadataFields = Mock(return_value=S_ERROR('some_error'))
    self.prodJob.fc.findDirectoriesByMetadata = Mock(return_value=S_OK({'abc': 'testdir123'}))
    res = self.prodJob.setInputDataQuery({'ProdID': 19872456})
    assertDiracFailsWith(res, 'could not contact file catalog', self)

  def test_setInputDataQuery_filecatalogWrongCase( self ):
    self.prodJob.fc.getMetadataFields = Mock(return_value=S_OK({'DirectoryMetaFields': {'prodid': 19872456}}))
    self.prodJob.fc.findDirectoriesByMetadata = Mock(return_value=S_OK({'abc': 'testdir123'}))
    res = self.prodJob.setInputDataQuery({'ProdID': 19872456})
    assertDiracFailsWith(res, 'key syntax error', self)

  def test_setInputDataQuery_filecatalogMissingKey( self ):
    self.prodJob.fc.getMetadataFields = Mock(return_value=S_OK({'DirectoryMetaFields': {'ProdID': 19872456}}))
    self.prodJob.fc.findDirectoriesByMetadata = Mock(return_value=S_OK({'abc': 'testdir123'}))
    self.prodJob.fc.getDirectoryUserMetadata = Mock(return_value=S_OK({'EvtType': 'electron party'}))
    # Key not present in getMetadataFields return value
    randomkey = '09428ituj42itufgm'
    res = self.prodJob.setInputDataQuery({'ProdID': 19872456, randomkey: 'testvalue'})
    assertDiracFailsWith(res, 'key %r not found in metadata keys' % randomkey, self)

  def test_setInputDataQuery_noprodid( self ):
    self.prodJob.fc.getMetadataFields = Mock(return_value=S_OK({'DirectoryMetaFields': {'ProdID': 19872456}}))
    self.prodJob.fc.findDirectoriesByMetadata = Mock(side_effect=[S_OK(['dir1', 'dir2']), S_OK({'abc': 'testdir123'})])
    self.prodJob.fc.getDirectoryUserMetadata = Mock(return_value=S_OK({'EvtType': 'electron party'}))
    res = self.prodJob.setInputDataQuery({})
    assertDiracFailsWith(res, "input metadata dictionary must contain at least a key 'prodid' as reference",
                         self)

  def test_setInputDataQuery_finddir_fails( self ):
    self.prodJob.fc.getMetadataFields = Mock(return_value=S_OK({'DirectoryMetaFields': {'ProdID': 19872456}}))
    self.prodJob.fc.findDirectoriesByMetadata = Mock(return_value=S_ERROR('some_error'))
    res = self.prodJob.setInputDataQuery({'ProdID': 19872456})
    assertDiracFailsWith(res, 'error looking up the catalog', self)

  def test_setInputDataQuery_finddir_invalid( self ):
    self.prodJob.fc.getMetadataFields = Mock(return_value=S_OK({'DirectoryMetaFields': {'ProdID': 19872456}}))
    self.prodJob.fc.findDirectoriesByMetadata = Mock(return_value=S_OK({}))
    res = self.prodJob.setInputDataQuery({'ProdID': 19872456})
    assertDiracFailsWith(res, 'could not find any directories', self)

  def test_setInputDataQuery_getdirusermetadata_fails( self ):
    self.prodJob.fc.getMetadataFields = Mock(return_value=S_OK({'DirectoryMetaFields': {'ProdID': 19872456}}))
    self.prodJob.fc.findDirectoriesByMetadata = Mock(return_value=S_OK({'abc': 'testdir123'}))
    self.prodJob.fc.getDirectoryUserMetadata = Mock(return_value=S_ERROR('some_error'))
    res = self.prodJob.setInputDataQuery({'ProdID': 19872456})
    assertDiracFailsWith(res, 'error looking up the catalog for directory metadata', self)

  def test_setInputDataQuery_getenergyfromcompatmeta_1( self ):
    self.prodJob.fc.getMetadataFields = Mock(return_value=S_OK({'DirectoryMetaFields': {'ProdID': 19872456}}))
    self.prodJob.fc.findDirectoriesByMetadata = Mock(return_value=S_OK({'abc': 'testdir123'}))
    self.prodJob.fc.getDirectoryUserMetadata = Mock(
        return_value=S_OK({'EvtType': ['electron party'], 'Energy': '13gev'}))
    res = self.prodJob.setInputDataQuery({'ProdID': 19872456})
    assertDiracSucceeds(res, self)
    assertEqualsImproved(self.prodJob.energy, Decimal('13'), self)

  def test_setInputDataQuery_getenergyfromcompatmeta_2( self ):
    self.prodJob.fc.getMetadataFields = Mock(return_value=S_OK({'DirectoryMetaFields': {'ProdID': 19872456}}))
    self.prodJob.fc.findDirectoriesByMetadata = Mock(return_value=S_OK({'abc': 'testdir123'}))
    self.prodJob.fc.getDirectoryUserMetadata = Mock(
        return_value=S_OK({'EvtType': 'electron party', 'Energy': ['13tev']}))
    res = self.prodJob.setInputDataQuery({'ProdID': 19872456})
    assertDiracSucceeds(res, self)
    assertEqualsImproved(self.prodJob.energy, Decimal('13000'), self)

  def test_setInputDataQuery_getenergyfromcompatmeta_3( self ):
    self.prodJob.fc.getMetadataFields = Mock(return_value=S_OK({'DirectoryMetaFields': {'ProdID': 19872456}}))
    self.prodJob.fc.findDirectoriesByMetadata = Mock(return_value=S_OK({'abc': 'testdir123'}))
    self.prodJob.fc.getDirectoryUserMetadata = Mock(return_value=S_OK({'EvtType': 'electron party', 'Energy': 13}))
    res = self.prodJob.setInputDataQuery({'ProdID': 19872456})
    assertDiracSucceeds(res, self)
    assertEqualsImproved(self.prodJob.energy, Decimal('13'), self)

  def test_setInputDataQuery_noevttype( self ):
    self.prodJob.fc.getMetadataFields = Mock(return_value=S_OK({'DirectoryMetaFields': {'ProdID': 19872456}}))
    self.prodJob.fc.findDirectoriesByMetadata = Mock(return_value=S_OK({'abc': 'testdir123'}))
    self.prodJob.fc.getDirectoryUserMetadata = Mock(return_value=S_OK({}))
    res = self.prodJob.setInputDataQuery({'ProdID': 19872456})
    assertDiracFailsWith(res, 'evttype is not in the metadata', self)

  def test_setInputDataQuery_numofevts_1( self ):
    self.prodJob.fc.getMetadataFields = Mock(return_value=S_OK(
        {'DirectoryMetaFields': {'ProdID': 19872456, 'NumberOfEvents': 'testsuihe123'}}))
    self.prodJob.fc.findDirectoriesByMetadata = Mock(return_value=S_OK({'abc': 'testdir123'}))
    self.prodJob.fc.getDirectoryUserMetadata = Mock(return_value=S_OK({'EvtType': 'electron party', 'Energy': 13}))
    res = self.prodJob.setInputDataQuery({'ProdID': 19872456, 'NumberOfEvents': ['42985']})
    assertDiracSucceeds(res, self)
    assertEqualsImproved(self.prodJob.nbevts, 42985, self)

  def test_setInputDataQuery_numofevts_2( self ):
    self.prodJob.fc.getMetadataFields = Mock(return_value=S_OK(
        {'DirectoryMetaFields': {'ProdID': 19872456, 'NumberOfEvents': 'testabc'}}))
    self.prodJob.fc.findDirectoriesByMetadata = Mock(return_value=S_OK({'abc': 'testdir123'}))
    self.prodJob.fc.getDirectoryUserMetadata = Mock(return_value=S_OK({'EvtType': 'electron party', 'Energy': 13}))
    res = self.prodJob.setInputDataQuery({'ProdID': 19872456, 'NumberOfEvents': '968541'})
    assertDiracSucceeds(res, self)
    assertEqualsImproved(self.prodJob.nbevts, 968541, self)

  def test_setInputDataQuery_datatype_1( self ):
    self.prodJob.fc.getMetadataFields = Mock(return_value=S_OK(
        {'DirectoryMetaFields': {'ProdID': 19872456, 'Datatype': 'test123', 'DetectorType': 'testdetector'}}))
    self.prodJob.fc.findDirectoriesByMetadata = Mock(return_value=S_OK({'abc': 'testdir123'}))
    self.prodJob.fc.getDirectoryUserMetadata = Mock(return_value=S_OK({'EvtType': 'electron party'}))
    self.prodJob.energycat = '7'
    res = self.prodJob.setInputDataQuery({'ProdID': 19872456, 'Datatype': 'mytype',
                                          'DetectorType': 'GoodDetector874'})
    assertDiracSucceeds(res, self)
    assertEqualsImproved(self.prodJob.datatype, 'mytype', self)
    assertEqualsImproved(self.prodJob.detector, 'GoodDetector874', self)

  def test_setInputDataQuery_datatype_2( self ):
    self.prodJob.fc.getMetadataFields = Mock(return_value=S_OK(
        {'DirectoryMetaFields': {'ProdID': 19872456, 'Datatype': 'test123', 'DetectorType': 'testdetector'}}))
    self.prodJob.fc.findDirectoriesByMetadata = Mock(return_value=S_OK({'abc': 'testdir123'}))
    self.prodJob.fc.getDirectoryUserMetadata = Mock(return_value=S_OK({'EvtType': 'electron party'}))
    self.prodJob.energycat = '7'
    res = self.prodJob.setInputDataQuery({'ProdID': 19872456, 'Datatype': 'gen', 'DetectorType': 'abc'})
    assertDiracSucceeds(res, self)
    assertEqualsImproved(self.prodJob.datatype, 'gen', self)
    assertEqualsImproved(self.prodJob.detector, '', self)

  def test_setInputDataQuery_datatype_list1( self ):
    self.prodJob.fc.getMetadataFields = Mock(return_value=S_OK(
        {'DirectoryMetaFields': {'ProdID': 19872456, 'Datatype': 'test123', 'DetectorType': 'testdetector'}}))
    self.prodJob.fc.findDirectoriesByMetadata = Mock(return_value=S_OK({'abc': 'testdir123'}))
    self.prodJob.fc.getDirectoryUserMetadata = Mock(return_value=S_OK({'EvtType': 'electron party'}))
    self.prodJob.energycat = '7'
    res = self.prodJob.setInputDataQuery({'ProdID': 19872456, 'Datatype': ['mytype'],
                                          'DetectorType': ['MyDetector3000']})
    assertDiracSucceeds(res, self)
    assertEqualsImproved(self.prodJob.datatype, 'mytype', self)
    assertEqualsImproved(self.prodJob.detector, 'MyDetector3000', self)

  def test_setInputDataQuery_datatype_list2( self ):
    self.prodJob.fc.getMetadataFields = Mock(return_value=S_OK(
        {'DirectoryMetaFields': {'ProdID': 19872456, 'Datatype': 'test123', 'DetectorType': 'testdetector'}}))
    self.prodJob.fc.findDirectoriesByMetadata = Mock(return_value=S_OK({'abc': 'testdir123'}))
    self.prodJob.fc.getDirectoryUserMetadata = Mock(return_value=S_OK({'EvtType': 'electron party'}))
    self.prodJob.energycat = '7'
    res = self.prodJob.setInputDataQuery({'ProdID': 19872456, 'Datatype': ['gen'],
                                          'DetectorType': '904215fadf'})
    assertDiracSucceeds(res, self)
    assertEqualsImproved(self.prodJob.datatype, 'gen', self)
    assertEqualsImproved(self.prodJob.detector, '', self)

  def test_createproduction( self ):
    job = self.prodJob
    job.proxyinfo = {'OK': 'yes, trust me', 'Value': {'groupProperties': 'ProductionManagement'}}
    job.created = False
    job.inputdataquery = True
    job.slicesize = 10
    job.inputBKSelection = True
    job.call_finalization = True
    job.prodparameters['ILDConfigVersion'] = 'goodversion1.215'
    job.nbevts = 89134
    job.finalpaths = [ 'test/path/my' ]
    job.workflow.setName('mytestworkflow')
    job.finalsdict = { 'uploadData' : 'myuploaddata', 'registerData' : 'myregisterdata',
                       'uploadLog' : 'myuploadlog', 'sendFailover' : 'mysendfailover' }
    file_contents = [["I'm an XML file"]]
    handles = FileUtil.getMultipleReadHandles(file_contents)

    with patch('__builtin__.open', mock_open()) as all_mo, \
         patch('%s.open' % MODULE_NAME, mock_open(), create=True) as mo, \
         patch('%s.Transformation.addTransformation' % MODULE_NAME, new=Mock(return_value=S_OK())):
      mo.side_effect = (h for h in handles)
      job.description = 'MyTestDescription'
      res = job.createProduction( 'goodtestname' )
      assertDiracSucceeds( res, self )
      expected = [[]]
      FileUtil.checkFileInteractions( self, mo, [ ( 'mytestworkflow.xml', 'r' ) ], expected, handles )
      all_mo.assert_any_call('Name.xml', 'w')
      all_mo_handle = all_mo()
      assert all_mo_handle.write.call_args[0][0].splitlines() == (EXPECTED_XML_WITH_PROXY if ProductionJobSetInputDataQuery.HAS_PROXY else EXPECTED_XML).splitlines()

  def test_createproduction_2( self ):
    job = self.prodJob
    job.proxyinfo = {'OK': 'yes, trust me', 'Value': {'groupProperties': 'ProductionManagement'}}
    job.created = False
    job.inputdataquery = True
    job.slicesize = 10
    job.inputBKSelection = True
    job.call_finalization = True
    job.finalpaths = [ 'test/path/my' ]
    job.workflow.setName('mytestworkflow')
    job.finalsdict = { 'uploadData' : 'myuploaddata', 'registerData' : 'myregisterdata',
                       'uploadLog' : 'myuploadlog', 'sendFailover' : 'mysendfailover' }
    file_contents = [["I'm an XML file"]]
    handles = FileUtil.getMultipleReadHandles(file_contents)
    with patch('__builtin__.open', mock_open()) as all_mo, \
         patch('%s.open' % MODULE_NAME, mock_open(), create=True) as mo, \
         patch('%s.Transformation.addTransformation' % MODULE_NAME, new=Mock(return_value=S_OK())):
      mo.side_effect = (h for h in handles)
      job.description = 'MyTestDescription'
      res = job.createProduction( 'goodtestname' )
      assertDiracSucceeds( res, self )
      expected = [[]]
      expected_tuples = [( 'mytestworkflow.xml', 'r' )]
      FileUtil.checkFileInteractions( self, mo, expected_tuples, expected, handles )
      all_mo.assert_any_call('Name.xml', 'w')
      all_mo_handle = all_mo()
      assert all_mo_handle.write.call_args[0][0].splitlines() == (EXPECTED_XML_WITH_PROXY if ProductionJobSetInputDataQuery.HAS_PROXY else EXPECTED_XML).splitlines()

  def test_createproduction_nofinalization( self ):
    job = self.prodJob
    job.proxyinfo = {'OK': 'yes, trust me', 'Value': {'groupProperties': 'ProductionManagement'}}
    job.created = False
    job.call_finalization = False
    job.workflow.setName('mytestworkflow')
    job.finalsdict = { 'uploadData' : 'myuploaddata', 'registerData' : 'myregisterdata',
                       'uploadLog' : 'myuploadlog', 'sendFailover' : 'mysendfailover' }
    file_contents = [["I'm an XML file"]]
    handles = FileUtil.getMultipleReadHandles(file_contents)
    with patch('__builtin__.open', mock_open()) as all_mo, \
         patch('%s.open' % MODULE_NAME, mock_open(), create=True) as mo, \
         patch('%s.Transformation.addTransformation' % MODULE_NAME, new=Mock(return_value=S_OK())):
      mo.side_effect = (h for h in handles)
      job.description = 'MyTestDescription'
      res = job.createProduction()
      assertDiracSucceeds( res, self )
      expected = [[]]
      expected_tuples = [( 'mytestworkflow.xml', 'r' )]
      FileUtil.checkFileInteractions( self, mo, expected_tuples, expected, handles )
      all_mo.assert_any_call('Name.xml', 'w')
      all_mo_handle = all_mo()
      assert all_mo_handle.write.call_args[0][0].splitlines() == (EXPECTED_XML_NOFINAL_WITH_PROXY if ProductionJobSetInputDataQuery.HAS_PROXY
                                                                  else EXPECTED_XML_NOFINAL).splitlines()

  @patch('__builtin__.open', mock_open())
  def test_createproduction_basic_checks( self ):
    job = self.prodJob
    job.proxyinfo = {'OK': False, 'Message': 'not ok'}
    res = job.createProduction()
    assertDiracFailsWith(res, 'you need a production proxy', self)
    job.proxyinfo = { 'OK' : True, 'Value' : {} }
    res = job.createProduction()
    assertDiracFailsWith( res, 'could not determine group', self )
    job.proxyinfo = {'OK': True, 'Value': {'groupProperties': 'NormalUser'}}
    res = job.createProduction()
    assertDiracFailsWith( res, 'not allowed to create production', self )
    job.created = True
    job.proxyinfo = {'OK': True, 'Value': {'groupProperties': 'ProductionManagement'}}
    res = job.createProduction()
    assertDiracFailsWith( res, 'already created', self )
    job.created = False
    with patch('%s.ProductionJob._addToWorkflow' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_error'))):
      assertDiracFailsWith( job.createProduction(), 'some_error', self )
    with patch('%s.ProductionJob.createWorkflow' % MODULE_NAME, new=Mock(side_effect=OSError('some_os_error'))):
      assertDiracFailsWith( job.createProduction(), 'could not create workflow', self )
    with patch('%s.Transformation.addTransformation' % MODULE_NAME, new=Mock(return_value=S_ERROR('myerror123'))), \
         patch('%s.open' % MODULE_NAME, mock_open(), create=True):
      assertDiracFailsWith( job.createProduction(), 'myerror123', self )
    job.trc = Mock()
    job.trc.getTransformationStats.return_value = S_OK('fail this') #S_OK because it means it found a transformation by that name, so the new one cannot be created
    with patch('%s.open' % MODULE_NAME, mock_open(), create=True):
      assertDiracFailsWith( job.createProduction(), 'already exists', self )

  @patch('__builtin__.open', mock_open())
  def test_createproduction_dryrun( self ):
    job = self.prodJob
    job.proxyinfo = {'OK': 'yes, trust me', 'Value': {'groupProperties': 'ProductionManagement'}}
    job.created = False
    job.dryrun = True
    job.call_finalization = True
    job.workflow.setName('mytestworkflow')
    job.finalsdict = { 'uploadData' : 'myuploaddata', 'registerData' : 'myregisterdata',
                       'uploadLog' : 'myuploadlog', 'sendFailover' : 'mysendfailover' }
    file_contents = [["I'm an XML file"]]
    handles = FileUtil.getMultipleReadHandles(file_contents)
    with patch('%s.open' % MODULE_NAME, mock_open(), create=True) as mo, \
         patch('%s.Transformation.addTransformation' % MODULE_NAME, new=Mock(return_value=S_OK())):
      mo.side_effect = (h for h in handles)
      job.description = 'MyTestDescription'
      res = job.createProduction( 'goodtestname' )
      assertDiracSucceeds( res, self )
      expected = [[]]
      FileUtil.checkFileInteractions( self, mo, [( 'mytestworkflow.xml', 'r' )], expected, handles )

  def test_setNbOfTasks( self ):
    assertDiracFailsWith( self.prodJob.setNbOfTasks(5), 'no transformation defined', self )
    self.prodJob.currtrans = Mock()
    self.prodJob.inputBKSelection = True
    assertDiracFailsWith( self.prodJob.setNbOfTasks(2), '', self ) #Returns empty S_ERROR, probably should add at least error message
    self.prodJob.inputBKSelection = False
    testNbTasks = 1375
    res = self.prodJob.setNbOfTasks( testNbTasks )
    assertDiracSucceeds( res, self )
    assertEqualsImproved( self.prodJob.nbtasks, testNbTasks, self )
    self.prodJob.currtrans.setMaxNumberOfTasks.assert_called_with( testNbTasks ) #pylint: disable=no-member

  def test_applyInputDataQuery( self ):
    job = self.prodJob
    assertDiracFailsWith( job.applyInputDataQuery(), 'no transformation defined', self )
    self.assertFalse( job.transfid )
    job.dryrun = True
    res = job.applyInputDataQuery( prodid = 13 )
    assertDiracSucceeds( res, self )
    testid = 138
    job.dryrun = False

    job.trc.createTransformationInputDataQuery.return_value = S_OK('works')
    res = job.applyInputDataQuery(' my metadata ', testid)
    assertDiracSucceeds(res, self)
    assertEqualsImproved(job.transfid, testid, self)
    assertEqualsImproved(job.inputBKSelection, ' my metadata ', self)

    job.trc.createTransformationInputDataQuery.return_value = S_ERROR('mycoolerror')
    assertDiracFailsWith(job.applyInputDataQuery(' my metadata ', testid), 'mycoolerror', self)

  #TODO Fix runtime of these tests???
  def test_finalizeProd( self ):
    job = self.prodJob
    job.slicesize = 0
    job.dryrun = False
    testdict = { 'JobType' : 'mytest', 'Process' : 'mytestprocess', 'Energy' : 86451 }
    job.currtrans = Mock()
    job.currtrans.getTransformationID.return_value = { 'Value' : 651 }  #pylint: disable=no-member
    res = job.finalizeProd( 1387, testdict )
    assertDiracSucceeds( res, self )

  def test_finalizeProd_dryrun( self ):
    job = self.prodJob
    job.slicesize = 0
    testdict = { 'JobType' : 'mytest', 'Process' : 'mytestprocess', 'Energy' : 86451 }
    job.currtrans = 651
    job.dryrun = True
    res = job.finalizeProd( 1387, testdict )
    assertDiracSucceeds( res, self )

  def test_finalizeProd_withparams( self ):
    job = self.prodJob
    job.slicesize = 0
    job.prodparameters = { 'JobType' : [ 'mytest' ], 'lumi' : 12, 'NbInputFiles' : 1,
                           'FCInputQuery' : { 'sampleKey' : 'sampleValue' }, 'SWPackages' : 'mytestpackages',
                           'SoftwareTag' : 'Monday', 'ILDConfigVersion' : 'goodILDConfversion123.2' }
    job.prodparameters['ClicConfigVersion'] = 'better3.14'
    job.prodparameters['extraCLIArguments'] = '--Config.Tracking=Awesome'
    job.finalpaths = [ 'testpath123/a/b/c', 'othertestpath/many_dirs/file.txt' ]
    job.slicesize = 561
    job.metadict_external = { 'additional_entry' : 'swpackage_value' }
    job.finalMetaDict = {'testpath123/a/b/c': {},
                         'another_one/asd': {'key': 'val'},
                         'need/more/paths': {},
                         'nonsearchable/_exists_': {},
                         'othertestpath/many_dirs/file.txt': {}}
    job.finalMetaDictNonSearch = {'nonsearchable/path2': {},
                                  'test/path/more/needed': {}}

    job.fc.createDirectory.side_effect = createdir_sideeffect
    job.fc.changePathMode.side_effect = changepath_sideeffect
    job.fc.getDirectoryUserMetadata = Mock(return_value=S_OK({'key': 'val', 'k2': 'v2'}))

    job.trc.setTransformationParameter.return_value = S_OK(True)
    res = job.finalizeProd(1387)
    assertDiracSucceeds(res, self)
    # TODO Check result variables to be as expected

  def test_finalizeProd_metaFail(self):
    job = self.prodJob
    job.prodparameters = {'JobType': ['mytest'], 'lumi': 12, 'NbInputFiles': 1,
                          'FCInputQuery': {'sampleKey': 'sampleValue'},
                          'SWPackages': 'mytestpackages',
                          'SoftwareTag': 'Monday',
                          'ILDConfigVersion': 'goodILDConfversion123.2'}
    job.slicesize = 0
    job.finalMetaDict = {'another_one/asd': {'key': 'val'}}

    job.fc.createDirectory.side_effect = createdir_sideeffect
    job.fc.changePathMode.side_effect = changepath_sideeffect
    job.fc.getDirectoryUserMetadata = Mock(return_value=S_OK({'key': 'val2'}))

    job.trc.setTransformationParameter.return_value = S_OK(True)
    res = job.finalizeProd(1387)
    assertDiracFailsWith(res, 'meta', self)


  def test_finalizeProd_noswpackages_nometadictexternal( self ):
    job = self.prodJob
    job.slicesize = 0
    job.prodparameters = { 'JobType' : 'mytest', 'lumi' : 12, 'NbInputFiles' : 1,
                           'FCInputQuery' : { 'sampleKey' : 'sampleValue' }, 'SoftwareTag' : 'Monday',
                           'ILDConfigVersion' : 'goodILDConfversion123.2' }
    job.finalpaths = [ 'testpath123/a/b/c', 'othertestpath/many_dirs/file.txt' ]
    job.slicesize = 561
    job.finalMetaDict = { 'asd' : {'tag':'asd'} }
    job.finalMetaDictNonSearch = { 'testpath123/a/b/c' : {} }

    job.fc.createDirectory.side_effect = createdir_sideeffect
    job.fc.changePathMode.side_effect = changepath_sideeffect

    job.trc.setTransformationParameter.return_value = S_OK(True)
    res = job.finalizeProd(1387)
    assertDiracSucceeds(res, self)

  def test_finalizeProd_lumiZero( self ):
    job = self.prodJob
    job.prodparameters = { 'JobType' : 'mytest', 'lumi' : 0, 'NbInputFiles' : 1,
                           'FCInputQuery' : { 'sampleKey' : 'sampleValue' }, 'SWPackages' : 'mytestpackages',
                           'SoftwareTag' : 'Monday', 'ILDConfigVersion' : 'goodILDConfversion123.2' }
    res = job.finalizeProd( 1387 )
    assertDiracSucceeds( res, self )

  def test_finalizeProd_notrans( self ):
    job = self.prodJob
    assertDiracFailsWith( job.finalizeProd(), 'no transformation defined', self )

  def test_finalizeProd_setMetaFails( self ):
    job = self.prodJob
    job.fc.setMetadata = Mock(return_value=S_OK(True))
    job.slicesize = 0
    job.prodparameters = {'JobType': 'mytest', 'lumi': 12, 'NbInputFiles': 1,
                          'FCInputQuery': {'sampleKey': 'sampleValue'}, 'SWPackages': 'mytestpackages',
                          'SoftwareTag': 'Monday', 'ILDConfigVersion': 'goodILDConfversion123.2'}
    job.finalpaths = ['testpath123/a/b/c', 'othertestpath/many_dirs/file.txt']
    job.slicesize = 561
    job.metadict_external = {'additional_entry': 'swpackage_value'}
    job.finalMetaDict = {'testpath123/a/b/c': {}, 'another_path/file.txt': {}, 'another_one/asd': {},
                         'wrongpath': {}, 'something_invalid': {}, 'nonsearchable/path': {},
                         'other_unsearchable/path/f.txt': {}, 'need/more/paths': {},
                         'othertestpath/many_dirs/file.txt': {}}
    job.finalMetaDictNonSearch = {'nonsearchable/path2': {}, 'other_unsearchables/path/gh.txt': {},
                                  'test/path/more/needed': {}, 'my_file/hidden.txt': {}, 'tmp.txt': {},
                                  '/usr/bin/test': {}, '/myfile_f.txt': {}}

    job.fc.createDirectory = Mock(side_effect=createdir_sideeffect)
    job.fc.changePathMode = Mock(side_effect=changepath_sideeffect)
    res = job.finalizeProd(1387)
    assertDiracFailsWith(res, 'meta', self)

  def test_finalizeProd_setMetaFails_2(self):
    job = self.prodJob
    job.fc.setMetadata = Mock(return_value=S_ERROR('Failed to set meta'))
    job.slicesize = 0
    job.prodparameters = { 'JobType' : 'mytest', 'lumi' : 12, 'NbInputFiles' : 1,
                           'FCInputQuery' : { 'sampleKey' : 'sampleValue' }, 'SWPackages' : 'mytestpackages',
                           'SoftwareTag' : 'Monday', 'ILDConfigVersion' : 'goodILDConfversion123.2' }
    job.finalpaths = [ 'testpath123/a/b/c', 'othertestpath/many_dirs/file.txt' ]
    job.slicesize = 561
    job.finalMetaDict = {'testpath123/a/b/c': {'k': '1', 'k2': '2'}}

    job.fc.createDirectory = Mock(side_effect=createdir_sideeffect)
    job.fc.changePathMode = Mock(side_effect=changepath_sideeffect)
    res = job.finalizeProd(1387)
    assertDiracFailsWith(res, 'failed to set meta', self)


  def test_getMetadata( self ):
    job = self.prodJob
    reference_dict = { '1' : {'test1' : 1, '09ksrt' : '123tgvda'}, '2' : {'vdunivi' : -135, 21 : 'sdfg',
                                                                          job : 0, 'NumberOfEvents' : 1002 } }
    reference_dict_nonbofevts = { 'test1' : 1, '09ksrt' : '123tgvda', 'vdunivi' : -135, 21 : 'sdfg', job : 0 }
    job.finalMetaDict = reference_dict
    assertEqualsImproved( job.getMetadata(), reference_dict_nonbofevts, self )
    job.finalMetaDict = {}
    assertEqualsImproved( job.getMetadata(), {}, self )
    easy_dict = { 'key1' : 'value1', '1983jrtmfgik' : 1984137895, '198034' : job, '09842jtm' : '9k0femoqifu' }
    job.finalMetaDict = { '1' : easy_dict }
    assertEqualsImproved( job.getMetadata(), easy_dict, self )

class ProductionJobJobSpecificParamsTest( ProductionJobTestCase ):
  """ Tests the jobSpecificParams method by calling append() and mocking out the other parts
  """

  def setUp( self ):
    """set up the objects"""
    super(ProductionJobJobSpecificParamsTest, self).setUp()
    self.prodJob.outputStorage = 'CERN-DIP-4'
    self.myapp = create_application_mock()

  def tearDown( self ):
    self.prodJob = None
    self.myapp = None

  def test_jobSpecificParams(self):
    """Test jobSpecificParams inside the append call.

    Does nothing basically.
    """
    res = self.prodJob.append(self.myapp)
    assertDiracSucceeds( res, self )

  def test_jobSpecificParams_alreadyCreated( self ):
    self.prodJob.created = True
    assertDiracFailsWith( self.prodJob.append(self.myapp), 'production was created', self )

  def test_jobSpecificParams_loggingFails( self ):
    self.myapp.logFile = ''
    self.myapp.setLogFile.return_value = S_ERROR('some_log_error')
    res = self.prodJob.append(self.myapp)
    assertDiracFailsWith( res, 'some_log_error', self )

  def test_jobSpecificParams_checkNbEvts_1( self ):
    self.prodJob.nbevts = 0
    self.prodJob.slicesize = 0
    assertDiracSucceeds( self.prodJob.append(self.myapp), self )
    assertEqualsImproved( self.prodJob.nbevts, 3, self )

  def test_jobSpecificParams_checkNbEvts_2( self ):
    self.myapp.numberOfEvents = 0
    self.prodJob.nbevts = 0
    assertDiracFailsWith( self.prodJob.append(self.myapp), 'number of events to process is not defined', self )

  def test_jobSpecificParams_checkNbEvts_3( self ):
    self.prodJob.slicesize = 6
    self.prodJob.nbevts = 0
    self.myapp.numberOfEvents = 0
    assertDiracSucceeds( self.prodJob.append(self.myapp), self )
    self.myapp.setNumberOfEvents.assert_called_with( 6 )

  def test_jobSpecificParams_checkNbEvts_4( self ):
    self.prodJob.slicesize = 0
    self.prodJob.nbevts = 3
    self.prodJob.jobFileGroupSize = 7
    self.myapp.numberOfEvents = 0
    assertDiracSucceeds( self.prodJob.append(self.myapp), self )
    self.myapp.setNumberOfEvents.assert_called_with( 21 )

  def test_jobSpecificParams_checkNbEvts_5( self ):
    self.prodJob.nbevts = 0
    self.prodJob.slicesize = 1
    self.myapp.numberOfEvents = 11
    assertDiracSucceeds( self.prodJob.append(self.myapp), self )

  def test_jobSpecificParams_checkNbEvts_6( self ):
    self.prodJob.nbevts = 0
    self.prodJob.slicesize = 0
    self.myapp.numberOfEvents = 11
    assertDiracSucceeds( self.prodJob.append(self.myapp), self )

  def test_jobSpecificParams_checkNbEvts_7( self ):
    self.prodJob.nbevts = 0
    self.prodJob.slicesize = 2
    self.myapp.numberOfEvents = 0
    self.myapp.setNumberOfEvents.return_value = S_ERROR( 'some_append_error' )
    assertDiracFailsWith( self.prodJob.append(self.myapp), 'some_append_error', self )

  def test_jobSpecificParams_energytests( self ):
    self.prodJob.energy = 0
    self.myapp.energy = 0
    assertDiracFailsWith( self.prodJob.append(self.myapp), 'could not find the energy defined', self )
    self.prodJob.energy = 250
    self.myapp.setEnergy.return_value = S_ERROR('some_energy_error')
    assertDiracFailsWith( self.prodJob.append(self.myapp), 'some_energy_error', self )
    with patch('%s.hasattr' % MODULE_NAME, new=Mock(return_value=False), create=True):
      self.prodJob.evttype = ''
      self.myapp.setEnergy.return_value = S_OK(True)
      assertDiracFailsWith( self.prodJob.append(self.myapp), 'event type not found', self )
    self.prodJob.prodparameters['SWPackages'] = 'someProgramv1.0;anotherProgram'
    reference_string = 'someProgramv1.0;anotherProgram;coolphysicssimulation.v3p10'
    self.myapp.accountInProduction = False
    self.assertTrue( self.prodJob.append(self.myapp) )
    assertEqualsImproved( self.prodJob.prodparameters['SWPackages'], reference_string, self )
    with patch('%s.ProductionJob._updateProdParameters' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_update_error'))):
      assertDiracFailsWith( self.prodJob.append(self.myapp), 'some_update_error', self )
    self.prodJob.outputStorage = ''
    assertDiracFailsWith( self.prodJob.append(self.myapp), 'specify the output storage element', self )

  def test_jobSpecificParams_failures( self ):
    self.myapp.setOutputSE.return_value = S_ERROR('setOSE_error')
    assertDiracFailsWith( self.prodJob.append(self.myapp), 'setOSE_error', self )
    self.myapp.setOutputSE.return_value = S_OK(True)
    self.prodJob.evttype = 'mytype/'
    self.myapp.willBeCut = False
    with patch('%s.hasattr' % MODULE_NAME, new=Mock(return_value=True), create=True):
      self.assertTrue(self.prodJob.append(self.myapp))
    with patch('%s.hasattr' % MODULE_NAME, new=Mock(side_effect=[True, False, True, True, True]), create=True):
      self.myapp.outputFile = False
      self.myapp.willBeCut = False
      self.myapp.detectortype = 'mydetector'
      assertDiracSucceeds( self.prodJob.append(self.myapp), self )
    with patch('%s.hasattr' % MODULE_NAME, new=Mock(side_effect=[True, False, True, True, True]), create=True):
      my_dict = self.prodJob.prodparameters
      def getitem(name):
        return my_dict[name]
      def setitem(name, val):
        my_dict[name] = val
      mock_dict = Mock()
      mock_dict.__getitem__.side_effect = getitem
      mock_dict.__setitem__.side_effect = setitem
      mock_dict.update.side_effect = ValueError('some_prodparam_err')
      self.prodJob.prodparameters = mock_dict
      self.prodJob.evttype = ''
      self.myapp.outputFile = False
      self.myapp.willBeCut = False
      self.myapp.detectortype = ''
      self.myapp.datatype = ''
      self.prodJob.datatype = 'datatype'
      self.prodJob.detector = 'mycooldetector'
      assertDiracFailsWith( self.prodJob.append(self.myapp), 'some_prodparam_err', self )


  def hasAttrMocks( self, *args, **kwargs ): #pylint: disable=unused-argument
    """ mock the operations getConfig calls """
    print("arguments", args, kwargs)
    opsDict={
      'extraCLIArguments': False,
      'detectorModel': False,
      'detectortype': False,
      'eventType': True,
      'outputFile': True,
      'datatype': True,
      'setOutputRecFile': False,
      'setOutputDstFile': False,
    }
    self.assertIn( args[1], opsDict )
    return opsDict[ args[1] ]

  def hasAttrMocks_2( self, *args, **kwargs ): #pylint: disable=unused-argument
    """ mock the operations getConfig calls """
    print("arguments", args, kwargs)
    opsDict={
      'extraCLIArguments': False,
      'detectorModel': False,
      'detectortype': True,
      'eventType': True,
      'outputFile': True,
      'datatype': True,
      'setOutputRecFile': False,
      'setOutputDstFile': False,
    }
    self.assertIn( args[1], opsDict )
    return opsDict[ args[1] ]

  def test_jobSpecificParams_detector_and_datatype( self ):
    self.myapp.setOutputSE.return_value = S_OK(True)
    self.prodJob.evttype = ''
    self.myapp.willBeCut = False
    self.myapp.outputFile = False
    self.myapp.extraCLIArguments = ''
    hasAttrMock = Mock()
    hasAttrMock.side_effect = self.hasAttrMocks
    with patch('%s.hasattr' % MODULE_NAME, new=hasAttrMock, create=True), \
         patch('%s.ProductionJob._updateProdParameters' % MODULE_NAME, new=Mock(return_value=S_OK())):
      assertDiracSucceeds( self.prodJob.append(self.myapp), self )
    self.prodJob.evttype = ''

    hasAttrMock = Mock()
    hasAttrMock.side_effect = self.hasAttrMocks_2
    with patch('%s.hasattr' % MODULE_NAME, new=hasAttrMock, create=True):
      self.myapp.detectortype = 'application_detectortype'
      assertDiracSucceeds( self.prodJob.append(self.myapp), self )

  @parameterized.expand([(True, ['/vo/prod/250gev/fake/awesome/REC', '/vo/prod/250gev/fake/awesome/DST']),
                         (False, ['/vo/prod/250gev/fake/awesome/DST'])])
  def test_jobSpecificParams_rec_and_dst(self, keep, finalPaths):
    """Test if disabling rec file works."""
    self.myapp.setOutputSE.return_value = S_OK(True)
    self.prodJob.evttype = ''
    self.prodJob.basepath = '/vo/prod/'
    self.prodJob.basename = 'basename'
    self.myapp.eventType = 'fake'
    self.myapp.detectortype = 'awesome'
    self.myapp.willBeCut = False
    self.myapp.outputFile = False
    self.myapp.extraCLIArguments = ''
    self.myapp.keepRecFile = keep
    self.myapp.setOutputRecFile = Mock(name='RecFile')
    self.myapp.setOutputDstFile = Mock(name='DstFile')
    with patch('%s.ProductionJob._updateProdParameters' % MODULE_NAME, new=Mock(return_value=S_OK())):
      assertDiracSucceeds(self.prodJob.append(self.myapp), self)
    if keep:
      self.myapp.setOutputRecFile.assert_called_once()
    else:
      self.myapp.setOutputRecFile.assert_not_called()
    self.myapp.setOutputDstFile.assert_called_once()
    assert self.prodJob.finalpaths == finalPaths

  def test_setters_1( self ):
    import random
    self.prodJob.setDryRun( True )
    assertEqualsImproved( self.prodJob.dryrun, True, self )
    self.prodJob.setProdGroup( 'myprodgroup' )
    assertEqualsImproved( self.prodJob.prodGroup, 'myprodgroup', self )
    self.prodJob.setProdPlugin( 'Limited' )
    assertEqualsImproved( self.prodJob.plugin, 'Limited', self )
    self.prodJob.setNbEvtsPerSlice(13)
    assertEqualsImproved( self.prodJob.slicesize, 13, self )
    myprodtype = random.choice( ALLOWED_PRODTYPES )
    self.prodJob.setDescription("It's green")
    assert self.prodJob.description[-5:] == 'green'
    self.prodJob.setProdType( myprodtype )
    assertEqualsImproved( self.prodJob.type, myprodtype, self )
    self.prodJob.setWorkflowName( 'mysuperworkflow' )
    assertEqualsImproved( self.prodJob.workflow['name'], 'mysuperworkflow', self )
    assertEqualsImproved( self.prodJob.workflow['name'], self.prodJob.name, self )
    self.prodJob.setWorkflowDescription( 'this is a test workflow, pls dont execute' )
    assertEqualsImproved( self.prodJob.workflow['description'],
                          'this is a test workflow, pls dont execute', self )
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(side_effect=[True, False])), \
         patch('%s.shutil.move' % MODULE_NAME, new=Mock(return_value=True)) as move_mock, \
         patch('%s.os.remove' % MODULE_NAME, new=Mock(True)) as remove_mock, \
         patch('__builtin__.open', mock_open()) as open_mock:
      self.prodJob.createWorkflow()
      assertEqualsImproved( len(move_mock.mock_calls), 1, self )
      move_mock.assert_called_once_with( 'mysuperworkflow.xml', 'mysuperworkflow.xml.backup' )
      self.assertFalse( remove_mock.called )
      assertEqualsImproved( len(open_mock.mock_calls), 4, self ) #Open, __enter__, write, __exit__
    assertDiracSucceeds( self.prodJob.setOutputSE( 'myoutputstorage_for_testing' ), self )
    assertEqualsImproved( self.prodJob.outputStorage, 'myoutputstorage_for_testing', self )

  def test_setters_2( self ):
    self.prodJob.setDryRun( False )
    assertEqualsImproved( self.prodJob.dryrun, False, self )
    self.prodJob.setProdGroup( '' )
    assertEqualsImproved( self.prodJob.prodGroup, '', self )
    self.prodJob.setProdPlugin( '' )
    assertEqualsImproved( self.prodJob.plugin, '', self )
    self.prodJob.setNbEvtsPerSlice(0)
    assertEqualsImproved( self.prodJob.slicesize, 0, self )
    # Throws exception
    try:
      self.prodJob.setProdType( '9i8u1j4tn' )
      self.fail()
    except TypeError as myerror:
      self.assertNotEqual( self.prodJob.type, '9i8u1j4tn' )
      message = '%s' % myerror
      self.assertIn( 'prod must be one of', message.lower() )
    self.prodJob.setWorkflowName( '' )
    assertEqualsImproved( self.prodJob.workflow['name'], '', self )
    assertEqualsImproved( self.prodJob.workflow['name'], self.prodJob.name, self )
    self.prodJob.setWorkflowDescription( '' )
    assertEqualsImproved( self.prodJob.workflow['description'], '', self )
    # Skipping createWorkflow
    self.prodJob.setOutputSE( '' )
    assertEqualsImproved( self.prodJob.outputStorage, '', self )


ALLOWED_PRODTYPES = ['MCGeneration', 'MCSimulation', 'Test', 'MCReconstruction', 'MCReconstruction_Overlay',
                     'Merge', 'Split']
def create_application_mock():
  """ Returns a mock object containing all necessary values for being used as an application in ProductionJob
  """
  myapp = Mock()
  myapp._analyseJob.return_value = S_OK()
  myapp._checkConsistency.return_value = S_OK()
  myapp._checkFinalConsistency.return_value = S_OK()
  myapp.inputSB = [ [] ]
  myapp.energy = 350 #gev
  myapp.numberOfEvents = 3
  myapp.appname = 'coolphysicssimulation'
  myapp.version = 'v3p10'
  return myapp


#TODO Check if really 4 necessary
# Constants needed for the tests
EXPECTED_XML = '<Workflow>\n<origin></origin>\n<description><![CDATA[]]></description>\n<descr_short></descr_short>\n<version>0.0</version>\n<type></type>\n<name>mytestworkflow</name>\n<Parameter name="JobType" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Job Type"><value><![CDATA[User]]></value></Parameter>\n<Parameter name="Priority" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Priority"><value><![CDATA[1]]></value></Parameter>\n<Parameter name="JobGroup" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified job group"><value><![CDATA[@{PRODUCTION_ID}]]></value></Parameter>\n<Parameter name="JobName" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Name of Job"><value><![CDATA[Name]]></value></Parameter>\n<Parameter name="Origin" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Origin of client"><value><![CDATA[DIRAC]]></value></Parameter>\n<Parameter name="StdOutput" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Standard output file"><value><![CDATA[std.out]]></value></Parameter>\n<Parameter name="StdError" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Standard error file"><value><![CDATA[std.err]]></value></Parameter>\n<Parameter name="InputData" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Default null input data value"><value><![CDATA[]]></value></Parameter>\n<Parameter name="LogLevel" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified logging level"><value><![CDATA[verbose]]></value></Parameter>\n<Parameter name="arguments" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Arguments to executable Step"><value><![CDATA[]]></value></Parameter>\n<Parameter name="ParametricInputData" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Default null parametric input data value"><value><![CDATA[]]></value></Parameter>\n<Parameter name="ParametricInputSandbox" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Default null parametric input sandbox value"><value><![CDATA[]]></value></Parameter>\n<Parameter name="IS_PROD" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="This job is a production job"><value><![CDATA[True]]></value></Parameter>\n<Parameter name="MaxCPUTime" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="CPU time in secs"><value><![CDATA[300000]]></value></Parameter>\n<Parameter name="CPUTime" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="CPU time in secs"><value><![CDATA[300000]]></value></Parameter>\n<Parameter name="productionVersion" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProdAPIVersion"><value><![CDATA[$Id$]]></value></Parameter>\n<Parameter name="PRODUCTION_ID" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProductionID"><value><![CDATA[00012345]]></value></Parameter>\n<Parameter name="JOB_ID" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProductionJobID"><value><![CDATA[00012345]]></value></Parameter>\n<Parameter name="emailAddress" type="string" linked_module="" linked_parameter="" in="True" out="False" description="CrashEmailAddress"><value><![CDATA[ilcdirac-support@cern.ch]]></value></Parameter>\n<ModuleDefinition>\n<body><![CDATA[from ILCDIRAC.Workflow.Modules.UploadOutputData import UploadOutputData]]></body>\n<origin></origin>\n<description><![CDATA[Uploads the output data]]></description>\n<descr_short></descr_short>\n<required></required>\n<version>0.0</version>\n<type>UploadOutputData</type>\n<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[False]]></value></Parameter>\n</ModuleDefinition>\n<ModuleDefinition>\n<body><![CDATA[from ILCDIRAC.Workflow.Modules.RegisterOutputData import RegisterOutputData]]></body>\n<origin></origin>\n<description><![CDATA[Module to add in the metadata catalog the relevant info about the files]]></description>\n<descr_short></descr_short>\n<required></required>\n<version>0.0</version>\n<type>RegisterOutputData</type>\n<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[False]]></value></Parameter>\n</ModuleDefinition>\n<ModuleDefinition>\n<body><![CDATA[from ILCDIRAC.Workflow.Modules.ReportErrors import ReportErrors]]></body>\n<origin></origin>\n<description><![CDATA[Reports errors at the end]]></description>\n<descr_short></descr_short>\n<required></required>\n<version>0.0</version>\n<type>ReportErrors</type>\n</ModuleDefinition>\n<ModuleDefinition>\n<body><![CDATA[from ILCDIRAC.Workflow.Modules.UploadLogFile import UploadLogFile]]></body>\n<origin></origin>\n<description><![CDATA[Uploads the output log files]]></description>\n<descr_short></descr_short>\n<required></required>\n<version>0.0</version>\n<type>UploadLogFile</type>\n<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[False]]></value></Parameter>\n</ModuleDefinition>\n<ModuleDefinition>\n<body><![CDATA[from ILCDIRAC.Workflow.Modules.FailoverRequest import FailoverRequest]]></body>\n<origin></origin>\n<description><![CDATA[Sends any failover requests]]></description>\n<descr_short></descr_short>\n<required></required>\n<version>0.0</version>\n<type>FailoverRequest</type>\n<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[False]]></value></Parameter>\n</ModuleDefinition>\n<StepDefinition>\n<origin></origin>\n<version>0.0</version>\n<type>Job_Finalization</type>\n<description><![CDATA[]]></description>\n<descr_short></descr_short>\n<ModuleInstance>\n<type>UploadOutputData</type>\n<name>dataUpload</name>\n<descr_short></descr_short>\n<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[True]]></value></Parameter>\n</ModuleInstance>\n<ModuleInstance>\n<type>RegisterOutputData</type>\n<name>RegisterOutputData</name>\n<descr_short></descr_short>\n<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[True]]></value></Parameter>\n</ModuleInstance>\n<ModuleInstance>\n<type>UploadLogFile</type>\n<name>logUpload</name>\n<descr_short></descr_short>\n<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[True]]></value></Parameter>\n</ModuleInstance>\n<ModuleInstance>\n<type>FailoverRequest</type>\n<name>failoverRequest</name>\n<descr_short></descr_short>\n<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[True]]></value></Parameter>\n</ModuleInstance>\n<ModuleInstance>\n<type>ReportErrors</type>\n<name>reportErrors</name>\n<descr_short></descr_short>\n</ModuleInstance>\n</StepDefinition>\n<StepInstance>\n<type>Job_Finalization</type>\n<name>finalization</name>\n<descr_short></descr_short>\n</StepInstance>\n</Workflow>\n'

EXPECTED_XML_WITH_PROXY = '<Workflow>\n<origin></origin>\n<description><![CDATA[]]></description>\n<descr_short></descr_short>\n<version>0.0</version>\n<type></type>\n<name>mytestworkflow</name>\n<Parameter name="JobType" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Job Type"><value><![CDATA[User]]></value></Parameter>\n<Parameter name="Priority" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Priority"><value><![CDATA[1]]></value></Parameter>\n<Parameter name="JobGroup" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified job group"><value><![CDATA[@{PRODUCTION_ID}]]></value></Parameter>\n<Parameter name="JobName" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Name of Job"><value><![CDATA[Name]]></value></Parameter>\n<Parameter name="Origin" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Origin of client"><value><![CDATA[DIRAC]]></value></Parameter>\n<Parameter name="StdOutput" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Standard output file"><value><![CDATA[std.out]]></value></Parameter>\n<Parameter name="StdError" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Standard error file"><value><![CDATA[std.err]]></value></Parameter>\n<Parameter name="InputData" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Default null input data value"><value><![CDATA[]]></value></Parameter>\n<Parameter name="LogLevel" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified logging level"><value><![CDATA[verbose]]></value></Parameter>\n<Parameter name="arguments" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Arguments to executable Step"><value><![CDATA[]]></value></Parameter>\n<Parameter name="ParametricInputData" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Default null parametric input data value"><value><![CDATA[]]></value></Parameter>\n<Parameter name="ParametricInputSandbox" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Default null parametric input sandbox value"><value><![CDATA[]]></value></Parameter>\n<Parameter name="IS_PROD" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="This job is a production job"><value><![CDATA[True]]></value></Parameter>\n<Parameter name="MaxCPUTime" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="CPU time in secs"><value><![CDATA[300000]]></value></Parameter>\n<Parameter name="CPUTime" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="CPU time in secs"><value><![CDATA[300000]]></value></Parameter>\n<Parameter name="productionVersion" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProdAPIVersion"><value><![CDATA[$Id$]]></value></Parameter>\n<Parameter name="PRODUCTION_ID" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProductionID"><value><![CDATA[00012345]]></value></Parameter>\n<Parameter name="JOB_ID" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProductionJobID"><value><![CDATA[00012345]]></value></Parameter>\n<Parameter name="emailAddress" type="string" linked_module="" linked_parameter="" in="True" out="False" description="CrashEmailAddress"><value><![CDATA[ilcdirac-support@cern.ch]]></value></Parameter>\n<ModuleDefinition>\n<body><![CDATA[from ILCDIRAC.Workflow.Modules.UploadOutputData import UploadOutputData]]></body>\n<origin></origin>\n<description><![CDATA[Uploads the output data]]></description>\n<descr_short></descr_short>\n<required></required>\n<version>0.0</version>\n<type>UploadOutputData</type>\n<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[False]]></value></Parameter>\n</ModuleDefinition>\n<ModuleDefinition>\n<body><![CDATA[from ILCDIRAC.Workflow.Modules.RegisterOutputData import RegisterOutputData]]></body>\n<origin></origin>\n<description><![CDATA[Module to add in the metadata catalog the relevant info about the files]]></description>\n<descr_short></descr_short>\n<required></required>\n<version>0.0</version>\n<type>RegisterOutputData</type>\n<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[False]]></value></Parameter>\n</ModuleDefinition>\n<ModuleDefinition>\n<body><![CDATA[from ILCDIRAC.Workflow.Modules.ReportErrors import ReportErrors]]></body>\n<origin></origin>\n<description><![CDATA[Reports errors at the end]]></description>\n<descr_short></descr_short>\n<required></required>\n<version>0.0</version>\n<type>ReportErrors</type>\n</ModuleDefinition>\n<ModuleDefinition>\n<body><![CDATA[from ILCDIRAC.Workflow.Modules.UploadLogFile import UploadLogFile]]></body>\n<origin></origin>\n<description><![CDATA[Uploads the output log files]]></description>\n<descr_short></descr_short>\n<required></required>\n<version>0.0</version>\n<type>UploadLogFile</type>\n<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[False]]></value></Parameter>\n</ModuleDefinition>\n<ModuleDefinition>\n<body><![CDATA[from ILCDIRAC.Workflow.Modules.FailoverRequest import FailoverRequest]]></body>\n<origin></origin>\n<description><![CDATA[Sends any failover requests]]></description>\n<descr_short></descr_short>\n<required></required>\n<version>0.0</version>\n<type>FailoverRequest</type>\n<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[False]]></value></Parameter>\n</ModuleDefinition>\n<StepDefinition>\n<origin></origin>\n<version>0.0</version>\n<type>Job_Finalization</type>\n<description><![CDATA[]]></description>\n<descr_short></descr_short>\n<ModuleInstance>\n<type>UploadOutputData</type>\n<name>dataUpload</name>\n<descr_short></descr_short>\n<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[True]]></value></Parameter>\n</ModuleInstance>\n<ModuleInstance>\n<type>RegisterOutputData</type>\n<name>RegisterOutputData</name>\n<descr_short></descr_short>\n<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[True]]></value></Parameter>\n</ModuleInstance>\n<ModuleInstance>\n<type>UploadLogFile</type>\n<name>logUpload</name>\n<descr_short></descr_short>\n<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[True]]></value></Parameter>\n</ModuleInstance>\n<ModuleInstance>\n<type>FailoverRequest</type>\n<name>failoverRequest</name>\n<descr_short></descr_short>\n<Parameter name="enable" type="bool" linked_module="" linked_parameter="" in="True" out="False" description="EnableFlag"><value><![CDATA[True]]></value></Parameter>\n</ModuleInstance>\n<ModuleInstance>\n<type>ReportErrors</type>\n<name>reportErrors</name>\n<descr_short></descr_short>\n</ModuleInstance>\n</StepDefinition>\n<StepInstance>\n<type>Job_Finalization</type>\n<name>finalization</name>\n<descr_short></descr_short>\n</StepInstance>\n</Workflow>\n'

EXPECTED_XML_NOFINAL = '<Workflow>\n<origin></origin>\n<description><![CDATA[]]></description>\n<descr_short></descr_short>\n<version>0.0</version>\n<type></type>\n<name>mytestworkflow</name>\n<Parameter name="JobType" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Job Type"><value><![CDATA[User]]></value></Parameter>\n<Parameter name="Priority" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Priority"><value><![CDATA[1]]></value></Parameter>\n<Parameter name="JobGroup" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified job group"><value><![CDATA[@{PRODUCTION_ID}]]></value></Parameter>\n<Parameter name="JobName" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Name of Job"><value><![CDATA[Name]]></value></Parameter>\n<Parameter name="Origin" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Origin of client"><value><![CDATA[DIRAC]]></value></Parameter>\n<Parameter name="StdOutput" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Standard output file"><value><![CDATA[std.out]]></value></Parameter>\n<Parameter name="StdError" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Standard error file"><value><![CDATA[std.err]]></value></Parameter>\n<Parameter name="InputData" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Default null input data value"><value><![CDATA[]]></value></Parameter>\n<Parameter name="LogLevel" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified logging level"><value><![CDATA[verbose]]></value></Parameter>\n<Parameter name="arguments" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Arguments to executable Step"><value><![CDATA[]]></value></Parameter>\n<Parameter name="ParametricInputData" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Default null parametric input data value"><value><![CDATA[]]></value></Parameter>\n<Parameter name="ParametricInputSandbox" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Default null parametric input sandbox value"><value><![CDATA[]]></value></Parameter>\n<Parameter name="IS_PROD" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="This job is a production job"><value><![CDATA[True]]></value></Parameter>\n<Parameter name="MaxCPUTime" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="CPU time in secs"><value><![CDATA[300000]]></value></Parameter>\n<Parameter name="CPUTime" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="CPU time in secs"><value><![CDATA[300000]]></value></Parameter>\n<Parameter name="productionVersion" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProdAPIVersion"><value><![CDATA[$Id$]]></value></Parameter>\n<Parameter name="PRODUCTION_ID" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProductionID"><value><![CDATA[00012345]]></value></Parameter>\n<Parameter name="JOB_ID" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProductionJobID"><value><![CDATA[00012345]]></value></Parameter>\n<Parameter name="emailAddress" type="string" linked_module="" linked_parameter="" in="True" out="False" description="CrashEmailAddress"><value><![CDATA[ilcdirac-support@cern.ch]]></value></Parameter>\n</Workflow>\n'

EXPECTED_XML_NOFINAL_WITH_PROXY = '<Workflow>\n<origin></origin>\n<description><![CDATA[]]></description>\n<descr_short></descr_short>\n<version>0.0</version>\n<type></type>\n<name>mytestworkflow</name>\n<Parameter name="JobType" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Job Type"><value><![CDATA[User]]></value></Parameter>\n<Parameter name="Priority" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Priority"><value><![CDATA[1]]></value></Parameter>\n<Parameter name="JobGroup" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified job group"><value><![CDATA[@{PRODUCTION_ID}]]></value></Parameter>\n<Parameter name="JobName" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Name of Job"><value><![CDATA[Name]]></value></Parameter>\n<Parameter name="Origin" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Origin of client"><value><![CDATA[DIRAC]]></value></Parameter>\n<Parameter name="StdOutput" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Standard output file"><value><![CDATA[std.out]]></value></Parameter>\n<Parameter name="StdError" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Standard error file"><value><![CDATA[std.err]]></value></Parameter>\n<Parameter name="InputData" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="Default null input data value"><value><![CDATA[]]></value></Parameter>\n<Parameter name="LogLevel" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="User specified logging level"><value><![CDATA[verbose]]></value></Parameter>\n<Parameter name="arguments" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Arguments to executable Step"><value><![CDATA[]]></value></Parameter>\n<Parameter name="ParametricInputData" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Default null parametric input data value"><value><![CDATA[]]></value></Parameter>\n<Parameter name="ParametricInputSandbox" type="string" linked_module="" linked_parameter="" in="True" out="False" description="Default null parametric input sandbox value"><value><![CDATA[]]></value></Parameter>\n<Parameter name="IS_PROD" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="This job is a production job"><value><![CDATA[True]]></value></Parameter>\n<Parameter name="MaxCPUTime" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="CPU time in secs"><value><![CDATA[300000]]></value></Parameter>\n<Parameter name="CPUTime" type="JDL" linked_module="" linked_parameter="" in="True" out="False" description="CPU time in secs"><value><![CDATA[300000]]></value></Parameter>\n<Parameter name="productionVersion" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProdAPIVersion"><value><![CDATA[$Id$]]></value></Parameter>\n<Parameter name="PRODUCTION_ID" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProductionID"><value><![CDATA[00012345]]></value></Parameter>\n<Parameter name="JOB_ID" type="string" linked_module="" linked_parameter="" in="True" out="False" description="ProductionJobID"><value><![CDATA[00012345]]></value></Parameter>\n<Parameter name="emailAddress" type="string" linked_module="" linked_parameter="" in="True" out="False" description="CrashEmailAddress"><value><![CDATA[ilcdirac-support@cern.ch]]></value></Parameter>\n</Workflow>\n'


CREATEDIR_DICT = {'testpath123/a/b/c': S_OK({'Successful': {'testpath123/a/b/c': 'created'}}),
                  'another_path/file.txt': S_OK({'Successful': {}, 'Failed':
                                                 {'another_path/file.txt':
                                                  'could not create, OSError'}}),
                  'another_one/asd': S_OK({'Successful': {'another_one/asf': 'created'}}),
                  'wrongpath': S_ERROR('some_error'),
                  'something_invalid': S_OK({'Successful': {}, 'Failed': {}}),
                  'nonsearchable/path': S_OK({'Successful': {'nonsearchable/path': 'created'}}),
                  'other_unsearchable/path/f.txt': S_OK({'Successful': {},
                                                         'Failed': {'other_unsearchable/path/f.ppt':
                                                                    'could not create, OSError'}}),
                  'need/more/paths': S_OK({'Successful': {'need/more/paths': 'created'}}),
                  'othertestpath/many_dirs/file.txt': S_OK({'Successful':
                                                            {'othertestpath/many_dirs/file.txt':
                                                             'created'}}),
                  'nonsearchable/path2': S_OK({'Successful': {'nonsearchable/path2': 'created'}}),
                  'other_unsearchables/path/gh.txt': S_OK({'Successful': {}, 'Failed':
                                                           {'other_unsearchables/path/gh.txt':
                                                            'could not create, OSError'}}),
                  'test/path/more/needed': S_OK({'Successful': {'diff_string': 'created'}}),
                  'my_file/hidden.txt': S_ERROR('some_error'),
                  'tmp.txt': S_OK({'Successful': {}, 'Failed': {}}),
                  '/usr/bin/test': S_OK({'Successful': {'/usr/bin/test': 'created'}}),
                  '/myfile_f.txt': S_OK({'Successful': {}, 'Failed':
                                         {'/myfile_f.ppt': 'could not create, OSError'}}),
                  'asd': S_OK({'Successful': {'asd': 'created'}, 'Failed': {}})}

CHANGEPATH_DICT = { 'testpath123/a/b/c' : S_OK(),
                    'nonsearchable/path' : S_ERROR('this is a test. fail please.'),
                    'need/more/paths' : S_OK(),
                    'othertestpath/many_dirs/file.txt' : S_OK(),
                    'nonsearchable/path2' : S_OK(),
                    '/usr/bin/test' : S_ERROR(),
                    'another_one/asd': S_OK(),
                    'test/path/more/needed': S_OK(),
                    'asd' : S_OK() }#, S_OK(), S_ERROR('this is a test. fail please.')}


def isdir_sideeffect(path):
  """Return true or false."""
  if '_exists_' in path:
    return S_OK({'Successful': {path: True}, 'Failed': {}})
  return S_OK({'Successful': {}, 'Failed': {path: 'Missing'}})


def createdir_sideeffect( value ):
  """ Returns the appropriate return value of the createDir method for the given directory string

  :param str value: directory to be created
  :returns: S_OK/S_ERROR structure with the value in CREATEDIR_DICT
  :rtype: dict

  """
  return CREATEDIR_DICT[value]

def changepath_sideeffect( val, bool_flag ): #pylint: disable=unused-argument
  """ Returns the appropriate return value of the changePathMode method for the given directory string.
  bool_flag is not used in this test version, but in the original method.

  :param str val: path to be changed to
  :param bool bool_flag: ignored, used in the actual implementation
  :returns: S_OK/S_ERROR structure with the value in CHANGEPATH_DICT
  :rtype: dict

  """
  return CHANGEPATH_DICT[val.iterkeys().next()]

def runTests():
  """Runs our tests"""
  alltests = unittest.TestSuite()
  alltests.addTest( unittest.makeSuite( ProductionJobTestCase ) )
  alltests.addTest( unittest.makeSuite( ProductionJobCompleteTestCase ) )
  alltests.addTest( unittest.makeSuite( ProductionJobSetJobFileGroupSizeTest ) )
  alltests.addTest( unittest.makeSuite( ProductionJobSetInputDataQuery ) )
  alltests.addTest( unittest.makeSuite( ProductionJobJobSpecificParamsTest ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( alltests )
  print(testResult)

if __name__ == '__main__':
  runTests()
