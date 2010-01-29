'''
Mokka analysis module. Called by Job Agent. 

Created on Jan 29, 2010

@author: sposs
'''
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from LCDDIRAC.Workflow.Modules.ModuleBase                import ModuleBase
from LCDDIRAC.Core.Utilities.CombinedSoftwareInstallation  import MySiteRoot
from DIRAC                                               import S_OK, S_ERROR, gLogger, gConfig, List

import DIRAC
import shutil, re, string, os, sys, time, glob


class MokkaAnalysis(ModuleBase):
    '''
    Specific Module to run a Mokka job.
    '''
    def __init__(self):
        ModuleBase.__init__(self)
        self.enable = True
        self.STEP_NUMBER = ''
        self.debug = True
        self.log = gLogger.getSubLogger( "MokkaAnalysis" )
        self.result = S_ERROR()
        self.optfile = ''
        self.run_number = 0
        self.firstEventNumber = 1
        self.jobID = None
        if os.environ.has_key('JOBID'):
            self.jobID = os.environ['JOBID']

        self.systemConfig = ''
        self.applicationLog = ''
        self.numberOfEvents = 0
        self.inputData = '' # to be resolved
        self.InputData = '' # from the (JDL WMS approach)
        self.outputData = ''
        self.generator_name=''
        self.optfile_extra = ''
        self.optionsLinePrev = ''
        self.optionsLine = ''
        self.extraPackages = ''
        self.applicationType = ''
        self.jobType = ''

#############################################################################
    def resolveInputVariables(self):
        """ Resolve all input variables for the module here.
        """
        if self.workflow_commons.has_key('SystemConfig'):
            self.systemConfig = self.workflow_commons['SystemConfig']

        if self.step_commons.has_key('applicationVersion'):
            self.applicationVersion = self.step_commons['applicationVersion']
            self.applicationLog = self.step_commons['applicationLog']

        if self.step_commons.has_key('numberOfEvents'):
            self.numberOfEvents = self.step_commons['numberOfEvents']

        if self.step_commons.has_key('optionsFile'):
            self.optionsFile = self.step_commons['optionsFile']

        if self.step_commons.has_key('optionsLine'):
            self.optionsLine = self.step_commons['optionsLine']

        if self.step_commons.has_key('optionsLinePrev'):
            self.optionsLinePrev = self.step_commons['optionsLinePrev']

        if self.step_commons.has_key('generatorName'):
            self.generator_name = self.step_commons['generatorName']

        if self.step_commons.has_key('extraPackages'):
            self.extraPackages = self.step_commons['extraPackages']

        if self.workflow_commons.has_key('InputData'):
            self.InputData = self.workflow_commons['InputData']

        if self.step_commons.has_key('inputData'):
            self.inputData = self.step_commons['inputData']

        if self.workflow_commons.has_key('JobType'):
            self.jobType = self.workflow_commons['JobType']

    
    def execute(self):
        """
        Called by Agent
        """
        self.resolveInputVariables()
        if not self.systemConfig:
            self.result = S_ERROR( 'No LHCb platform selected' )
        elif not self.applicationLog:
            self.result = S_ERROR( 'No Log file provided' )

        if not self.result['OK']:
            return self.result
        if not self.optionsFile and not self.optionsLine:
            self.log.warn( 'No options File nor options Line provided' )
            self.result = S_OK()

        cwd = os.getcwd()
        self.root = gConfig.getValue('/LocalSite/Root',cwd)
        self.log.debug(self.version)
        self.log.info( "Executing Mokka %s"%(self.applicationVersion))
        self.log.info("Platform for job is %s" % ( self.systemConfig ) )
        self.log.info("Root directory for job is %s" % ( self.root ) )
        sharedArea = MySiteRoot()
        if sharedArea == '':
            self.log.error( 'MySiteRoot Not found' )
            return S_ERROR(' MySiteRoot Not Found')

        mySiteRoot=sharedArea
        self.log.info('MYSITEROOT is %s' %mySiteRoot)
        localArea = sharedArea
        if re.search(':',sharedArea):
            localArea = string.split(sharedArea,':')[0]
        self.log.info('Setting local software area to %s' %localArea)

        scriptName = 'Mokka_%s_Run_%s.sh' %(self.applicationVersion,self.STEP_NUMBER)

        if os.path.exists(scriptName): os.remove(scriptName)
        script = open(scriptName,'w')
        script.write('#!/bin/sh \n')
        script.write('#####################################################################\n')
        script.write('# Dynamically generated script to run a production or analysis job. #\n')
        script.write('#####################################################################\n')
        comm = "Mokka dosomethingto be done"
        print "Command : %s"%(comm)
        script.write(comm)
        script.write('declare -x appstatus=$?\n')
        script.write('where\n')
        script.write('quit\n')
        script.write('EOF\n')

        script.write('exit $appstatus\n')

        script.close()
        if os.path.exists(self.applicationLog): os.remove(self.applicationLog)

        os.chmod(scriptName,0755)
        comm = 'sh -c "./%s"' %scriptName
        self.setApplicationStatus('Mokka %s step %s' %(self.applicationVersion,self.STEP_NUMBER))
        self.stdError = ''
        self.result = shellCall(0,comm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
        #self.result = {'OK':True,'Value':(0,'Disabled Execution','')}
        resultTuple = self.result['Value']

        status = resultTuple[0]
        # stdOutput = resultTuple[1]
        # stdError = resultTuple[2]
        self.log.info( "Status after the application execution is %s" % str( status ) )

        failed = False
        if status != 0:
            self.log.error( "%s execution completed with errors:" % self.applicationName )
            failed = True
        else:
            self.log.info( "%s execution completed succesfully:" % self.applicationName )

        if failed==True:
            self.log.error( "==================================\n StdError:\n" )
            self.log.error( self.stdError )
            #self.setApplicationStatus('%s Exited With Status %s' %(self.applicationName,status))
            self.log.error('%s Exited With Status %s' %(self.applicationName,status))
            return S_ERROR('%s Exited With Status %s' %(self.applicationName,status))
        # Still have to set the application status e.g. user job case.
        self.setApplicationStatus('%s %s Successful' %(self.applicationName,self.applicationVersion))
        return S_OK('%s %s Successful' %(self.applicationName,self.applicationVersion))

    #############################################################################
    def redirectLogOutput(self, fd, message):
        sys.stdout.flush()
        if message:
            if re.search('INFO Evt',message): print message
        if self.applicationLog:
            log = open(self.applicationLog,'a')
            log.write(message+'\n')
            log.close()
        else:
            self.log.error("Application Log file not defined")
        if fd == 1:
            self.stdError += message
    #############################################################################

        