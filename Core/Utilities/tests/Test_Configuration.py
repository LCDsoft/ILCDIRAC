"""Test the Core Confogiration"""

import unittest
from mock import MagicMock as Mock
from ILCDIRAC.Core.Utilities import Configuration

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Core.Utilities.Configuration'


class TestCheckConf(unittest.TestCase):
  """Test the Configuration Utilities Module """

  def setUp(self):
    pass

  def tearDown(self):
    pass

  def test_getOptionValue(self):
    ops = Mock()

    def getValueMock(path, defVal):
      return {"/base/myOp": "baseVal",
              "/base/foo/myOp": "fooVal",
              "/base/foo/bar/myOp": "barVal",
              "/base/foo/bar/baz/myOp": "bazVal",
             }.get(path, defVal)

    ops.getValue.side_effect = getValueMock

    value = Configuration.getOptionValue(ops, "/base", "myOp", "defVal", ['foo', 'bar', 'baz'])
    self.assertEqual(value, 'bazVal')

    value = Configuration.getOptionValue(ops, "/base", "myOp", "defVal", ['foo', 'bar', 'baz2'])
    self.assertEqual(value, 'barVal')

    value = Configuration.getOptionValue(ops, "/base", "myOp", "defVal", ['foo', 'bar2', 'baz'])
    self.assertEqual(value, 'fooVal')

    value = Configuration.getOptionValue(ops, "/base", "myOp", "defVal", ['args', '', 'kwargs'])
    self.assertEqual(value, 'baseVal')

    value = Configuration.getOptionValue(ops, "/base2", "myOp", "defVal", ['args', '', 'kwargs'])
    self.assertEqual(value, 'defVal')
