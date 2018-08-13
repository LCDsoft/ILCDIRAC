"""Utility functions."""
from DIRAC import gLogger

LOG = gLogger.getSubLogger(__name__)


def toInt(number, cond=None):
  """Cast number parameter to an integer.

  >>> number = toInt("1000")

  :param number: the number to cast (number of events, number of jobs)
  :type number: str or int
  :param cond: function to check validity of number, range, etc.
  :type cond: unary function
  :return: The success or the failure of the casting
  :rtype: bool, int or None
  """
  if number is None:
    return number

  try:
    number = int(number)
  except ValueError:
    LOG.error("Argument is not an integer")
    return False
  if cond is not None and not cond(number):
    LOG.error("Argument does not pass condition: %r" % cond)
    return False
  return number
