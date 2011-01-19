import os,sys

from DIRAC.Core.Utilities.Subprocess                      import shellCall
from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import LocalArea,SharedArea
from DIRAC                                                import S_OK, S_ERROR, gLogger

import DIRAC

from string import Template

class CheckCollections(ModuleBase):

    def __init__(self):

        ModuleBase.__init__(self)

        self.STEP_NUMBER = ''
        self.log         = gLogger.getSubLogger( "CheckCollections" )
        self.args        = ''
        #self.result      = S_ERROR()
        self.jobID       = None

        if os.environ.has_key('JOBID'):
            self.jobID = os.environ['JOBID']

        print "%s initialized" % ( self.__str__() )

    def execute(self):

        # Get input variables

        result = self._resolveInputVariables()

        # Checks

        if not self.systemConfig:
            result = S_ERROR( 'No ILC platform selected' )
            
        if not os.environ.has_key("LCIO"):
            self.log.error("Environment variable LCIO was not defined, cannot do anything")
            result = S_ERROR("Environment variable LCIO was not defined, cannot do anything")

        if not result['OK']:
            return result

        # Setting up script

        LD_LIBRARY_PATH = "$LCIO/lib"
        if os.environ.has_key('LD_LIBRARY_PATH'):
            LD_LIBRARY_PATH = LD_LIBRARY_PATH + ":" + os.environ['LD_LIBRARY_PATH']

        PATH = "$LCIO/bin"
        if os.environ.has_key('PATH'):
            PATH = PATH + ":" + os.environ['PATH']

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
    numberOfEvents = cmdResult.strip().split()[1]

    cmdAnajobResult = subprocess.Popen( ["anajob", file], stdout=subprocess.PIPE ).communicate()[ 0 ]

    for collection in $collections:

        cmdResult = subprocess.Popen( ["grep", "-c", collection], stdin=subprocess.PIPE, stdout=subprocess.PIPE ).communicate( cmdAnajobResult )[ 0 ]

        print "%s %s" %( numberOfEvents, cmdResult )

sys.exit( exitStatus )

PYTHONSCRIPT

''')

        scriptContent = scriptContent.substitute(
            LD_LIBRARY_PATH_ = LD_LIBRARY_PATH,
            PATH_            = PATH,
            files            = self.inputSLCIOFiles,
            collections      = self.collections
        )

        # Write script to file

        scriptPath = 'CheckCollections_%s_Run_%s' %( self.applicationVersion, self.STEP_NUMBER )

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

        command = '"./%s"' %( scriptPath )

        self.setApplicationStatus( 'CheckCollections %s step %s' %( self.applicationVersion, self.STEP_NUMBER ) )
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
        
        if status:
          self.setApplicationStatus("CheckCollections failed with status %s"%(status))
          return S_ERROR("Checking collections failed")

        # Return

        return S_OK('CheckCollections')

    def redirectLogOutput(self, fd, message):

        sys.stdout.flush()

        if self.applicationLog:

            log = open(self.applicationLog,'a')
            log.write(message+'\n')
            log.close()

        else:
            self.log.error("Application Log file not defined")

        if fd == 1:
            self.stdError += message

    def _resolveInputVariables(self):

        if self.workflow_commons.has_key('SystemConfig'):
            self.systemConfig = self.workflow_commons['SystemConfig']

        if self.step_commons.has_key('applicationVersion'):
            self.applicationVersion = self.step_commons['applicationVersion']

        if self.step_commons.has_key('applicationLog'):
            self.applicationLog = self.step_commons['applicationLog']

        if self.step_commons.has_key('inputSLCIOFiles'):
            self.inputSLCIOFiles = self.step_commons['inputSLCIOFiles'].split(";")

        if self.step_commons.has_key('collections'):
            self.collections = self.step_commons['collections'].split(";")

        return S_OK('Parameters resolved')
