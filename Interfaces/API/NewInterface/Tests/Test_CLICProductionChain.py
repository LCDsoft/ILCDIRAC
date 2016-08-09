#!/usr/local/env python
"""
Test user jobfinalization

"""

import unittest
from mock import patch, mock_open, MagicMock as Mock

from DIRAC import gLogger, S_OK, S_ERROR
# Currently no module
#from ILCDIRAC.Interfaces.API.NewInterface.Productions.CLICProductionChain import CLICProductionChain
from ILCDIRAC.Tests.Utilities.GeneralUtils import assertEqualsImproved, assertDiracFailsWith, assertDiracSucceeds
from ILCDIRAC.Tests.Utilities.FileUtils import FileUtil

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.ProductionJob'

class CLICProductionChainTestCase( unittest.TestCase ):
  """ Base class for the ProductionJob test cases
  """
  def setUp(self):
    """set up the objects"""
    pass

  def test_method( self ):
    pass
