"""Tests for Utilities functions."""

import pytest

from ILCDIRAC.Core.Utilities.Utilities import toInt


@pytest.mark.parametrize("number, expected, cond",
                         [("1", 1, None),
                          ("1.2", False, None),
                          (1.2, 1, None),
                          ("-1", -1, None),
                          (None, None, None),
                          ("a", False, None),
                          ("-1", False, lambda x: x > 0),
                          ("12", 12, lambda x: x > 0),
                          ])
def test_toint(number, expected, cond):
  """Testing the to int function."""
  assert toInt(number, cond=cond) == expected
