#!/usr/bin/env python
"""
    print version of current ILCDIRAC installation
"""
__RCSID__ = "$Id$"
import DIRAC
import ILCDIRAC
print "ILCDirac Version:", ILCDIRAC.version, "\nWith DIRAC version:", DIRAC.version
