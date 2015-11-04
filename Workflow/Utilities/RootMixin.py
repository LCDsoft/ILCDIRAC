""" Shared functionality for :mod:`~ILCDIRAC.Workflow.Modules.RootMacroAnalysis` and :mod:`~ILCDIRAC.Workflow.Modules.RootExecutableAnalysis`"""

import os

from DIRAC import S_OK, S_ERROR

class RootMixin( object ):
  """ Mixin class for :mod:`~ILCDIRAC.Workflow.Modules.RootMacroAnalysis` and :mod:`~ILCDIRAC.Workflow.Modules.RootExecutableAnalysis`"""
  def getRootEnvScript( self, _platform, _appname, _appversion ):
    """create the environment script if it is not already available

    Need to set LD_LIBRARY_PATH and PATH based on ROOTSYS

    As this is only called when we are not CVMFS native the ROOTSYS must have
    been set by :func:`~ILCDIRAC.Core.Utilities.TARsoft.configureRoot`. Function
    signature must conform to
    :func:`~ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation.getEnvironmentScript`,
    but none of the arguments are used.

    :param string _platform: Unused, Software platform
    :param string _appname: Unused, application name
    :param string _appversion: Unused, application version
    :returns: S_OK( pathToScript )

    """
    if 'ROOTSYS' not in os.environ:
      self.log.error( "ROOTSYS is not set" )
      return S_ERROR( "ROOTSYS is not set" )
    self.log.info( "Creating RootEnv.sh with ROOTSYS: %s " % os.environ['ROOTSYS'] )

    scriptName = "rootEnv.sh"
    with open(scriptName, "w") as script:
      if 'LD_LIBRARY_PATH' in os.environ:
        script.write('declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:$LD_LIBRARY_PATH\n' )
      else:
        script.write('declare -x LD_LIBRARY_PATH=$ROOTSYS/lib\n')
        script.write('declare -x PATH=$ROOTSYS/bin:$PATH\n')
    return S_OK( os.path.join( os.getcwd(), scriptName ) )
