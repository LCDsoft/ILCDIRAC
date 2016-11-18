#!/usr/bin/env python
"""Test the FileUtils class"""

import sys
import unittest
from mock import mock_open, patch, MagicMock as Mock

from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, \
  assertDiracSucceeds, assertDiracSucceedsWith_equals, assertDiracFails, assertMockCalls

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Core.Utilities.FileUtils'

#pylint: disable=too-many-public-methods
class TestFileUtils( unittest.TestCase ):
  """ Test the different methods of the class
  """

  def setUp( self ):
    dm_mock = Mock()
    ops_mock = Mock()
    mocked_modules = { 'DIRAC.DataManagementSystem.Client.DataManager' : dm_mock,
                       'DIRAC.ConfigurationSystem.Client.Helpers.Operations' : ops_mock }
    self.module_patcher = patch.dict( sys.modules, mocked_modules )
    self.module_patcher.start()

  def tearDown( self ):
    self.module_patcher.stop()

  def test_upload_tarball_missing( self ):
    from ILCDIRAC.Core.Utilities.FileUtils import upload
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=False)) as exists_mock:
      assertDiracFails( upload( 'mypath', 'appTarTest' ), self )
      exists_mock.assert_called_once_with( 'appTarTest' )

  def test_upload_copy_fails( self ):
    from ILCDIRAC.Core.Utilities.FileUtils import upload
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)) as exists_mock, \
         patch('%s.shutil.copy' % MODULE_NAME, new=Mock(side_effect=EnvironmentError('copy_testfail'))) as copy_mock:
      assertDiracFailsWith( upload( 'http://www.cern.ch/lcd-data/mypath', 'appTarTest' ),
                            'could not copy because copy_testfail', self )
      exists_mock.assert_called_once_with( 'appTarTest' )
      copy_mock.assert_called_once_with( 'appTarTest', '/afs/cern.ch/eng/clic/data/software/appTarTest' )

  def test_upload_invalid_location( self ):
    from ILCDIRAC.Core.Utilities.FileUtils import upload
    with patch('%s.os.path.exists' % MODULE_NAME, new=Mock(return_value=True)):
      assertDiracFails( upload( 'http://www.mypath.com', 'appTarTest' ), self )
