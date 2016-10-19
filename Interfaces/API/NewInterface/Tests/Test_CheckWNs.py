#!/usr/local/env python
"""
Test CheckWNs module

"""

import unittest
from mock import patch, MagicMock as Mock

from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Interfaces.API.NewInterface.Applications import CheckWNs
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertDiracFailsWith, assertDiracSucceeds

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.Applications.CheckWNs'

#pylint: disable=protected-access
class CheckWNsTestCase( unittest.TestCase ):
  """ Base class for the Pythia test cases
  """
  def setUp(self):
    """set up the objects"""
    self.cwn = CheckWNs( {} )

  def test_userjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.cwn._userjobmodules( module_mock ), self )

  def test_prodjobmodules( self ):
    module_mock = Mock()
    assertDiracSucceeds( self.cwn._prodjobmodules( module_mock ), self )

  def test_userjobmodules_fails( self ):
    with patch('%s._setUserJobFinalization' % MODULE_NAME, new=Mock(return_value=S_OK('something'))),\
         patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_test_err'))):
      assertDiracFailsWith( self.cwn._userjobmodules( None ),
                            'userjobmodules failed', self )

  def test_prodjobmodules_fails( self ):
    with patch('%s._setApplicationModuleAndParameters' % MODULE_NAME, new=Mock(return_value=S_ERROR('some_test_err'))):
      assertDiracFailsWith( self.cwn._prodjobmodules( None ),
                            'prodjobmodules failed', self )

  def test_checkconsistency( self ):
    assertDiracSucceeds( self.cwn._checkConsistency(), self )

  def test_checkconsistency_nojob( self ):
    assertDiracSucceeds( self.cwn._checkConsistency( None ), self )

  def test_checkconsistency_mock_job( self ):
    assertDiracSucceeds( self.cwn._checkConsistency( Mock() ), self )
