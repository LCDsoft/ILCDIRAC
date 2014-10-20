#####################################################
# $HeadURL$
#####################################################
"""
Module to check the file contents
@author: Ching Bon Lam
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.Subprocess                      import shellCall
from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from DIRAC                                                import S_OK, S_ERROR, gLogger
from ILCDIRAC.Core.Utilities.PrepareLibs                  import removeLibc

import os

from string import Template

class CheckCollections(ModuleBase):
  """ Check the collections in a given slcio file.
  """
  def __init__(self):

    super(CheckCollections, self).__init__()

    self.STEP_NUMBER = ''
    self.log         = gLogger.getSubLogger( "CheckCollections" )
    self.args        = ''
    #self.result      = S_ERROR()
    self.jobID       = None
    self.applicationName = 'CheckCollections'
    # Step parameters

    self.InputFile    = []
    self.collections  = None

  def execute(self):
    """ Run the thing
    """
    # Get input variables

    result = self.resolveInputVariables()

    # Checks

    if not self.platform:
      result = S_ERROR( 'No ILC platform selected' )

    if not os.environ.has_key("LCIO"):
      self.log.error("Environment variable LCIO was not defined, cannot do anything")
      result = S_ERROR("Environment variable LCIO was not defined, cannot do anything")

    if not result['OK']:
      self.log.error("Failed to resolve the input parameters:", self.result["Message"])
      return result

        # removeLibc

    removeLibc( os.path.join( os.environ["LCIO"], "lib" ) )

    # Setting up script

    LD_LIBRARY_PATH = os.path.join( "$LCIO", "lib" )
    if os.environ.has_key('LD_LIBRARY_PATH'):
      LD_LIBRARY_PATH += ":" + os.environ['LD_LIBRARY_PATH']

    PATH = "$LCIO/bin"
    if os.environ.has_key('PATH'):
      PATH += ":" + os.environ['PATH']

    scriptContent = Template('''
#!/bin/sh

#------------------------------------------------------------------------------#
# Dynamically generated script by CheckCollections module                      #
#------------------------------------------------------------------------------#

declare -x LD_LIBRARY_PATH=$LD_LIBRARY_PATH_
declare -x PATH=$PATH_

python <<PYTHONSCRIPT

import sys, subprocess

exitStatus = 0

for file in $files:

    cmdResult      = subprocess.Popen( ["lcio", "count", file], stdout=subprocess.PIPE ).communicate()[ 0 ]
    numberOfEvents = int( cmdResult.strip().split()[1] )

    cmdAnajobResult = subprocess.Popen( ["anajob", file], stdout=subprocess.PIPE ).communicate()[ 0 ]

    for collection in $collections:

        cmdResult           = subprocess.Popen( ["grep", "-c", collection], stdin=subprocess.PIPE, stdout=subprocess.PIPE ).communicate( cmdAnajobResult )[ 0 ]
        numberOfCollections = int( cmdResult.strip() )

        if numberOfEvents != numberOfCollections:

            print 'Inconsistency in %s: %i events vs %i collections (%s)' % ( file, numberOfEvents, numberOfCollections, collection )

            exitStatus = 1
            #sys.exit( exitStatus )

sys.exit( exitStatus )

PYTHONSCRIPT

declare -x appstatus=$$?
exit $$appstatus

''')

    scriptContent = scriptContent.substitute(
            LD_LIBRARY_PATH_ = LD_LIBRARY_PATH,
            PATH_            = PATH,
            files            = self.InputFile,
            collections      = self.collections
    )

    # Write script to file

    scriptPath = 'CheckCollections_%s_Run_%s' % ( self.applicationVersion, self.STEP_NUMBER )

    if os.path.exists(scriptPath):
      os.remove(scriptPath)

    script = open( scriptPath, 'w' )
    script.write( scriptContent )
    script.close()


    # Setup log file for application stdout

    if os.path.exists( self.applicationLog ):
      os.remove( self.applicationLog )

    # Run code

    os.chmod( scriptPath, 0755 )

    command = '"./%s"' % ( scriptPath )

    self.setApplicationStatus( 'CheckCollections %s step %s' % ( self.applicationVersion, self.STEP_NUMBER ) )
    self.stdError = ''

    self.result = shellCall(
                            0,
                            command,
                            callbackFunction = self.redirectLogOutput,
                            bufferLimit = 20971520
    )

    # Check results

    resultTuple = self.result['Value']
    status      = resultTuple[0]

    self.log.info( "Status after the application execution is %s" % str( status ) )

    return self.finalStatusReport(status)


  def applicationSpecificInputs(self):

    # Logfile

    if not self.applicationLog:
      self.applicationLog = 'CheckCollections_%s_Run_%s.log' % ( self.applicationVersion, self.STEP_NUMBER )

    #
    self.InputFile = [os.path.basename( myfile ) for myfile in self.InputFile]
    #

    if len( self.collections ) == 0:
      return S_ERROR( 'No list of collections defined to check for.' )

    #

    return S_OK('Parameters resolved')
