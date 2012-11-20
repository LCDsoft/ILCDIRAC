'''
This is the worst piece of code ever NEEDED: just run some CPU intensive code to waste CPU time.

@author: Stephane Poss
@since: Jul 26, 2011
'''
import time
from math import log
from DIRAC import S_OK, S_ERROR

def WasteCPUCycles(timecut):
  """ Waste, waste, and waste more CPU.
  """
  a = 1e31
  first = time.clock()
  while time.clock()-first < timecut:
    try:
      a = log(a)
    except Exception, x:
      return S_ERROR("Failed to waste %s CPU seconds:%s" % (timecut, str(x)))  
    if a < 0:
      a = -a
    if a == 0:
      a = 4  
  return S_OK("Successfully wasted %s seconds" % timecut)    
