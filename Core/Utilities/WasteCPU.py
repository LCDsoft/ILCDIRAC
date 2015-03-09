'''
This is the worst piece of code ever NEEDED: just run some CPU intensive code to waste CPU time.

@author: Stephane Poss
@since: Jul 26, 2011
'''
__RCSID__ = "$Id$"
import time
from math import log
from DIRAC import S_OK, S_ERROR

def wasteCPUCycles(timecut):
  """ Waste, waste, and waste more CPU.
  """
  number = 1e31
  first = time.clock()
  while time.clock()-first < timecut:
    try:
      number = log(number)
    except ValueError as x:
      return S_ERROR("Failed to waste %s CPU seconds:%s" % (timecut, str(x)))
    if number < 0:
      number = -number
    if number == 0:
      number = 4
  return S_OK("Successfully wasted %s seconds" % timecut)
