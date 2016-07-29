"""
|  Print version of current ILCDIRAC installation.
|  Mandatory when submitting support requests

.. versionadded:: 23.0

:author: A. Sailer
"""

import DIRAC
import ILCDIRAC

__RCSID__ = "$Id$"


def _printVersions():
  """prints the ILCDIRAC and DIRAC versions"""
  print "ILCDirac Version:", ILCDIRAC.version
  print "With DIRAC version:", DIRAC.version


if __name__ == "__main__":
  _printVersions()
