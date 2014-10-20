""" Module that converts stdhep files to slcio. Goes usually before Tomato

@author: Ching Bon Lam

"""
__RCSID__ = "$Id$"


from DIRAC.Core.Utilities.Subprocess                      import shellCall
from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from DIRAC                                                import S_ERROR, gLogger
from ILCDIRAC.Core.Utilities.PrepareLibs                  import removeLibc

import os

class StdHepConverter(ModuleBase):
  """ Convert to SLCIO some stdhep files: Useful to run Tomato after Whizard
  """
  def __init__(self):

    super(StdHepConverter, self).__init__()

    self.STEP_NUMBER = ''
    self.log         = gLogger.getSubLogger( "StdHepConverter" )
    # Step parameters
    self.applicationName = 'StdhepConverter'

  def execute(self):
    """ Called from Workflow 
    """
    # Get input variables

    self.result = self.resolveInputVariables()

    if not self.platform:
      self.result = S_ERROR( 'No ILC platform selected' )

    if not self.result['OK']:
      self.log.error('Failed to resolve input parameters:', self.result['Message'])
      return self.result


    if not os.environ.has_key("LCIO"):
      self.log.error("Environment variable LCIO was not defined, cannot do anything")
      return S_ERROR("Environment variable LCIO was not defined, cannot do anything")

    # removeLibc

    removeLibc( os.path.join( os.environ["LCIO"], "lib" ) )

    # Setting up script

    LD_LIBRARY_PATH = os.path.join( "$LCIO", "lib" )
    if 'LD_LIBRARY_PATH' in os.environ:
      LD_LIBRARY_PATH += ":" + os.environ['LD_LIBRARY_PATH']

    PATH = "$LCIO/bin"
    if 'PATH' in os.environ:
      PATH += ":" + os.environ['PATH']

    scriptContent = """
#!/bin/sh

################################################################################
# Dynamically generated script by StdHepConverter module                       #
################################################################################

declare -x LD_LIBRARY_PATH=%s
declare -x PATH=%s

for STDHEPFILE in *.stdhep; do
    stdhepjob $STDHEPFILE ${STDHEPFILE/.stdhep/.slcio} -1
done

exit $?

""" % ( LD_LIBRARY_PATH, PATH )

    # Write script to file

    scriptPath = 'StdHepConverter_%s_Run_%s.sh' % ( self.applicationVersion, self.STEP_NUMBER )

    if os.path.exists(scriptPath):
      os.remove(scriptPath)

    script = open( scriptPath, 'w' )
    script.write( scriptContent )
    script.close()

    # Setup log file for application stdout

    if os.path.exists(self.applicationLog):
      os.remove(self.applicationLog)

    # Run code

    os.chmod( scriptPath, 0755 )

    command = 'sh -c "./%s"' % ( scriptPath )

    self.setApplicationStatus( 'StdHepConverter %s step %s' % ( self.applicationVersion, self.STEP_NUMBER ) )
    self.stdError = ''

    self.result = shellCall( 0,
                             command,
                             callbackFunction = self.redirectLogOutput,
                             bufferLimit = 20971520
                           )

    # Check results

    resultTuple = self.result['Value']
    status      = resultTuple[0]

    return self.finalStatusReport(status)

