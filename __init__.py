# $HeadURL: svn+ssh://sposs@svn.cern.ch/reps/dirac/ILCDIRAC/trunk/ILCDIRAC/__init__.py $
# $Id: __init__.py 49191 2012-03-20 10:48:42Z sposs $

from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)

# Define Version

majorVersion = 8
minorVersion = 2
patchLevel = 23
preVersion = 0
    
version      = "v%sr%s" % ( majorVersion, minorVersion )
buildVersion = "v%dr%d" % ( majorVersion, minorVersion )
if patchLevel:
  version = "%sp%s" % ( version, patchLevel )
  buildVersion = "%s build %s" % ( buildVersion, patchLevel )
if preVersion:
  version = "%s-pre%s" % ( version, preVersion )
  buildVersion = "%s pre %s" % ( buildVersion, preVersion )

