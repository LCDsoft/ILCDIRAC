"""Tests for Utilities functions."""

import pytest

from ILCDIRAC.Core.Utilities.Utilities import toInt, listify, lowerFirst, canConvert


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


@pytest.mark.parametrize("string, cast, expected",
                         [("1", None, ['1']),
                          ("1,3", None, ['1', '3']),
                          ("1,3,,,", int, [1, 3]),
                          ("0, 1,3", int, [0, 1, 3]),
                          ("  foo  , bar  ", None, ['foo', 'bar']),
                          ([1, 3, 4], None, [1, 3, 4]),
                          ])
def test_listify(string, cast, expected):
  """Testing the to int function."""
  assert listify(string, cast) == expected


@pytest.mark.parametrize('string, expected',
                         [('1', '1'),
                          ('SOMETHING', 'sOMETHING'),
                          ('something', 'something'),
                          ('CamelCase', 'camelCase'),
                          ])
def test_lowerFirst(string, expected):
  """Testing the lowerFirst function."""
  assert lowerFirst(string) == expected


@pytest.mark.parametrize('string, cast, expected',
                         [('5', int, True),
                           ('Five', int, False),
                           ('5.', int, False),
                           ('5.0', float, True),
                           ('5.0e10', float, True),
                           ('fff5.0f', float, False),
                           ])
def test_canConvert(string, cast, expected):
  """Testing the canConvert function."""
  assert canConvert(string, cast) == expected
