"""
Tests for OverlayDB

"""
import unittest
from mock import patch, MagicMock as Mock

from ILCDIRAC.Tests.Utilities.GeneralUtils import assertDiracFailsWith, assertDiracSucceeds, \
  assertDiracSucceedsWith_equals, assertMockCalls
from DIRAC import S_OK, S_ERROR

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.OverlaySystem.DB.OverlayDB'

# pylint: disable=no-member
class TestOverlayDB( unittest.TestCase ):
  """Tests of OverlayDB"""
  def setUp( self ):
    """ Prepare OverlayDB object
    """
    from ILCDIRAC.OverlaySystem.DB.OverlayDB import OverlayDB
    from DIRAC.Core.Base.DB import DB
    value_dict = { '/Overlay/MaxConcurrentRunning' : 10, '/Overlay/Sites/testSite1/MaxConcurrentRunning' : 2,
                   '/Overlay/Sites/myOtherSite/MaxConcurrentRunning' : 2 }
    sections_dict = { '/Overlay/Sites/' : [ 'testSite1', 'myOtherSite' ] }
    self.ops_mock = Mock()
    self.ops_mock.getValue.side_effect = lambda x, _ : value_dict[x]
    self.ops_mock.getSections.side_effect = lambda x : S_OK( sections_dict[x] )
    with patch('%s.Operations' % MODULE_NAME, new=Mock(return_value=self.ops_mock)), \
         patch.object(OverlayDB, '_createTables', new=Mock()), \
         patch.object(DB, '__init__', new=Mock()):
      self.odb = OverlayDB()

  def test_getsites( self ):
    con_mock = Mock()
    with patch('%s.OverlayDB._getConnection' % MODULE_NAME, new=Mock(return_value=S_OK(con_mock))), \
         patch('%s.OverlayDB._query' % MODULE_NAME, new=Mock(return_value=S_OK([ ('some_row',), ('other_row', )]))) as query_mock:
      assertDiracSucceedsWith_equals( self.odb.getSites(), [ 'some_row', 'other_row' ], self )
      query_mock.assert_called_once_with( 'SELECT Site From OverlayData;', con_mock )

  def test_getsites_query_fails( self ):
    con_mock = Mock()
    with patch('%s.OverlayDB._getConnection' % MODULE_NAME, new=Mock(return_value=S_OK(con_mock))), \
         patch('%s.OverlayDB._query' % MODULE_NAME, new=Mock(return_value=S_ERROR('test_query_fails'))):
      assertDiracFailsWith( self.odb.getSites(), 'Could not get sites', self )

  def test_getsites_nosites_available( self ):
    con_mock = Mock()
    with patch('%s.OverlayDB._query' % MODULE_NAME, new=Mock(return_value=S_OK([]))):
      assertDiracSucceedsWith_equals( self.odb.getSites( con_mock ), [], self )

  def test_getsites_connection_fails( self ):
    with patch('%s.OverlayDB._getConnection' % MODULE_NAME, new=Mock(return_value=S_ERROR())), \
         patch('%s.OverlayDB._query' % MODULE_NAME, new=Mock(return_value=S_ERROR('noconnection'))):
      assertDiracFailsWith( self.odb.getSites(), 'Could not get sites', self )

  def test_setjobsatsites( self ):
    con_mock = Mock()
    with patch('%s.OverlayDB._update' % MODULE_NAME, new=Mock(return_value=S_OK())) as update_mock:
      assertDiracSucceeds( self.odb.setJobsAtSites( { 'MyTestSite1' : 1487, 'other_site' : '138', 'large_testsite' : 40913.2 }, con_mock ), self )
      assertMockCalls( update_mock, [
        ( "UPDATE OverlayData SET NumberOfJobs=1487 WHERE Site='MyTestSite1';", con_mock ),
        ( "UPDATE OverlayData SET NumberOfJobs=138 WHERE Site='other_site';", con_mock ),
        ( "UPDATE OverlayData SET NumberOfJobs=40913 WHERE Site='large_testsite';", con_mock ) ], self )

  def test_setjobsatsites_nothingtodo( self ):
    assertDiracSucceeds( self.odb.setJobsAtSites( {}, Mock() ), self )

  def test_setjobsatsites_update_fails( self ):
    def replace_update( request ):
      """ Mocks the return value of the _update method """
      if 'other_site' in request:
        return S_ERROR()
      return S_OK()
    with patch('%s.OverlayDB._update' % MODULE_NAME, new=Mock(side_effect=lambda request, _ : replace_update( request ))):
      assertDiracFailsWith( self.odb.setJobsAtSites( { 'MyTestSite1' : 1487, 'other_site' : '138', 'large_testsite' : 40913.2 }, Mock() ), 'could not set number of jobs at site', self )

  def test_getjobsatsite( self ):
    con_mock = Mock()
    with patch('%s.OverlayDB._query' % MODULE_NAME, new=Mock(return_value=S_OK([ [ 123 ] ]))) as query_mock:
      assertDiracSucceedsWith_equals( self.odb.getJobsAtSite( 'myTest_Site1', con_mock ), 123, self )
      query_mock.assert_called_once_with( "SELECT NumberOfJobs FROM OverlayData WHERE Site='myTest_Site1';", con_mock )

  def test_getjobsatsite_query_fails( self ):
    con_mock = Mock()
    with patch('%s.OverlayDB._query' % MODULE_NAME, new=Mock(return_value=S_ERROR())):
      assertDiracSucceedsWith_equals( self.odb.getJobsAtSite( 'myTest_Site1', con_mock ), 0, self )

  def test_getjobsatsite_no_site_found( self ):
    con_mock = Mock()
    with patch('%s.OverlayDB._query' % MODULE_NAME, new=Mock(return_value=S_OK([]))):
      assertDiracSucceedsWith_equals( self.odb.getJobsAtSite( 'nonexistent_site_testme', con_mock ), 0, self )

  def test_canrun_toomanyjobs( self ):
    con_mock = Mock()
    with patch('%s.OverlayDB._query' % MODULE_NAME, new=Mock(return_value=S_OK([[2]]))) as query_mock:
      assertDiracSucceedsWith_equals( self.odb.canRun( 'testSite1', con_mock ), False, self )
      query_mock.assert_called_once_with( "SELECT NumberOfJobs FROM OverlayData WHERE Site='testSite1';",
                                          con_mock )

  def test_canrun_add_to_new_site( self ):
    con_mock = Mock()
    with patch('%s.OverlayDB._query' % MODULE_NAME, new=Mock(return_value=S_ERROR())), \
         patch('%s.OverlayDB._update' % MODULE_NAME, new=Mock(return_value=S_OK())) as update_mock:
      assertDiracSucceedsWith_equals( self.odb.canRun( 'tenJobSite', con_mock ), True, self )
      assertMockCalls( update_mock,
                       [ ( "INSERT INTO OverlayData (Site,NumberOfJobs) VALUES ('tenJobSite',1);", con_mock ),
                         ( "UPDATE OverlayData SET NumberOfJobs=2 WHERE Site='tenJobSite';", con_mock ) ], self )

  def test_canrun_addsite_fails( self ):
    con_mock = Mock()
    with patch('%s.OverlayDB._query' % MODULE_NAME, new=Mock(return_value=S_ERROR())), \
         patch('%s.OverlayDB._update' % MODULE_NAME, new=Mock(side_effect=[S_ERROR('update_test_err'), S_OK()])) as update_mock:
      assertDiracSucceedsWith_equals( self.odb.canRun( 'tenJobSite', con_mock ), True, self )
      assertMockCalls( update_mock,
                       [ ( "INSERT INTO OverlayData (Site,NumberOfJobs) VALUES ('tenJobSite',1);", con_mock ),
                         ( "UPDATE OverlayData SET NumberOfJobs=2 WHERE Site='tenJobSite';", con_mock ) ], self )

  def test_jobdone( self ):
    con_mock = Mock()
    with patch('%s.OverlayDB._query' % MODULE_NAME, new=Mock(return_value=S_OK([[148]]))) as query_mock, \
         patch('%s.OverlayDB._update' % MODULE_NAME, new=Mock(return_value=S_OK())) as update_mock:
      assertDiracSucceeds( self.odb.jobDone( 'my_TestSite1', con_mock ), self )
      query_mock.assert_called_once_with( "SELECT NumberOfJobs FROM OverlayData WHERE Site='my_TestSite1';",
                                          con_mock )
      update_mock.assert_called_once_with( "UPDATE OverlayData SET NumberOfJobs=147 WHERE Site='my_TestSite1';",
                                           con_mock )

  def test_jobdone_checksite_fails( self ):
    con_mock = Mock()
    with patch('%s.OverlayDB._query' % MODULE_NAME, new=Mock(return_value=S_ERROR())):
      assertDiracFailsWith( self.odb.jobDone( 'my_TestSite1', con_mock ), 'Could not get site', self )

  def test_jobdone_nojobs( self ):
    con_mock = Mock()
    with patch('%s.OverlayDB._query' % MODULE_NAME, new=Mock(return_value=S_OK([[1]]))) as query_mock:
      assertDiracSucceeds( self.odb.jobDone( 'my_TestSite1', con_mock ), self )
      query_mock.assert_called_once_with( "SELECT NumberOfJobs FROM OverlayData WHERE Site='my_TestSite1';",
                                          con_mock )

  def test_jobdone_update_fails( self ):
    con_mock = Mock()
    with patch('%s.OverlayDB._query' % MODULE_NAME, new=Mock(return_value=S_OK([[148]]))), \
         patch('%s.OverlayDB._update' % MODULE_NAME, new=Mock(return_value=S_ERROR('update_test_err'))):
      assertDiracFailsWith( self.odb.jobDone( 'my_TestSite1', con_mock ), 'update_test_err', self )
