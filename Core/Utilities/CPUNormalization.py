""" Utilities for CPU normalization

"""

import os
import random

from DIRAC import S_ERROR, S_OK
from DIRAC.WorkloadManagementSystem.Client.CPUNormalization import UNITS

def getCPUNormalization( reference = 'HS06', iterations = 1 ):
  """Get Normalized Power of the current CPU in [reference] units """
  if reference not in UNITS:
    return S_ERROR( 'Unknown Normalization unit %s' % str( reference ) )
  try:
    max( min( int( iterations ), 10 ), 1 )
  except ( TypeError, ValueError ), x :
    return S_ERROR( x )

  # This number of iterations corresponds to 1kHS2k.seconds, i.e. 250 HS06 seconds
  # 06.11.2015: fixed absolute normalization w.r.t. MJF at GRIDKA
  from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
  corr = Operations().getValue( 'JobScheduling/CPUNormalizationCorrection', 1. )
  nloop = int( 1000 * 1000 * 12.5 )
  calib = 250.0 / UNITS[reference] / corr

  m = long( 0 )
  m2 = long( 0 )
  p = 0
  p2 = 0
  # Do one iteration extra to allow CPUs with variable speed
  for i in xrange( iterations + 1 ):
    if i == 1:
      start = os.times()
    # Now the iterations
    for _j in xrange( nloop ):
      t = random.normalvariate( 10, 1 )
      m += t
      m2 += t * t
      p += t
      p2 += t * t
  end = os.times()
  cput = sum( end[:4] ) - sum( start[:4] )
  wall = end[4] - start[4]

  if not cput:
    return S_ERROR( 'Can not get used CPU' )

  return S_OK( {'CPU': cput, 'WALL':wall, 'NORM': calib * iterations / cput, 'UNIT': reference } )
