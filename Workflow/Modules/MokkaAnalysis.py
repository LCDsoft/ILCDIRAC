'''
Created on Jan 29, 2010

@author: sposs
'''
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from LCDDIRAC.Workflow.Modules.ModuleBase                  import ModuleBase
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
        self.log = gLogger.getSubLogger( "GaudiApplication" )
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
        
    