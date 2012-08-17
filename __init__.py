# $HeadURL: svn+ssh://sposs@svn.cern.ch/reps/dirac/ILCDIRAC/trunk/ILCDIRAC/__init__.py $
# $Id: __init__.py 55576 2012-08-17 07:31:38Z sposs $
"""
ILCDIRAC package, implements ILC/CLIC production and application specific stuff
"""
from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)

# Define Version

majorVersion = 12
minorVersion = 0
patchLevel = 10
preVersion = 0
    
version      = "v%sr%s" % ( majorVersion, minorVersion )
buildVersion = "v%dr%d" % ( majorVersion, minorVersion )
if patchLevel:
  version = "%sp%s" % ( version, patchLevel )
  buildVersion = "%s build %s" % ( buildVersion, patchLevel )
if preVersion:
  version = "%s-pre%s" % ( version, preVersion )
  buildVersion = "%s pre %s" % ( buildVersion, preVersion )

