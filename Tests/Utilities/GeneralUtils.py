"""Contains general utility methods for unit testing.
"""

__RCSID__ = "$Id$"

def assertEqualsImproved(val1, val2, assertobject):
  """Asserts that val1 and val2 are equal and automatically generates a meaningful error message. Only disadvantage is that you have to check 1 method up in the stacktrace.
  :param T val1: First value to be compared
  :param T val2: Second value, compared to val1
  :param TestCase assertobject: Testcase object, used to gain the base assertEquals method.
  """
  assertobject.assertEquals(val1, val2, "Test expected these values to be the same, but they were not! Value 1 = %s,\n Value 2 = %s" % (str(val1), str(val2)))

def assertInImproved(val1, val2, assertobject):
  """Asserts that val1 is contained in val2 and automatically generates a meaningful error message. Only disadvantage is that you have to check 1 method up in the stacktrace.
  :param T val1: First value
  :param S val2: Second value, can contain val1
  :param TestCase assertobject: Testcase object, used to gain the base assertEquals method.
  """
  assertobject.assertIn(val1, val2, "Test expected these values to be the same, but they were not! Value 1 = %s,\n Value 2 = %s" % (str(val1), str(val2)))
