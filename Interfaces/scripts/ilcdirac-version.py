#!/usr/bin/env python
"""
    print version of current ILCDIRAC installation
"""

import DIRAC
import ILCDIRAC

__RCSID__ = "$Id$"


def printVersions():
  """prints the ILCDIRAC and DIRAC versions"""
  print "ILCDirac Version:", ILCDIRAC.version
  print "With DIRAC version:", DIRAC.version


if __name__ == "__main__":
  printVersions()