#!/usr/bin/env python
"""
    print version of current ILCDIRAC installation
"""

import DIRAC
import ILCDIRAC

__RCSID__ = "$Id$"

print "ILCDirac Version:", ILCDIRAC.version
print "With DIRAC version:", DIRAC.version
