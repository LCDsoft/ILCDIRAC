'''
Mokka analysis module. Called by Job Agent. 

Created on Jan 29, 2010

@author: sposs
'''
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from LCDDIRAC.Workflow.Modules.ModuleBase                import ModuleBase
from LCDDIRAC.Core.Utilities.CombinedSoftwareInstallation  import MySiteRoot
from LCDDIRAC.Core.Utilities.PrepareSteeringFile         import PrepareSteeringFile
from LCDDIRAC.Core.Utilities.SQLWrapper                   import SQLWrapper
from DIRAC                                               import S_OK, S_ERROR, gLogger, gConfig, List

import DIRAC
import re, os, sys


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
        self.steeringFile = ''
        self.stdhepFile = ''
        self.run_number = 0
        self.firstEventNumber = 1
        self.jobID = None
        if os.environ.has_key('JOBID'):
            self.jobID = os.environ['JOBID']

        self.systemConfig = ''
        self.applicationLog = ''
        self.applicationVersion=''
        self.dbslice = ''
        self.numberOfEvents = 0
        self.startFrom = 1
        #self.inputData = '' # to be resolved
        #self.InputData = '' # from the (JDL WMS approach)
        #self.outputData = ''
        #self.generator_name=''
        #self.optionsLinePrev = ''
        #self.optionsLine = ''
        #self.extraPackages = ''
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
          
      if self.step_commons.has_key('startFrom'):
        self.startFrom = self.step_commons['startFrom']

      if self.step_commons.has_key("steeringFile"):
        self.steeringFile = self.step_commons['steeringFile']

      if self.step_commons.has_key('stdhepFile'):
        self.stdhepFile = self.step_commons['stdhepFile']

      if self.step_commons.has_key('detectorModel'):
        self.detectorModel = self.step_commons['detectorModel']

      #if self.step_commons.has_key('optionsLine'):
      #    self.optionsLine = self.step_commons['optionsLine']

      #if self.step_commons.has_key('optionsLinePrev'):
      #    self.optionsLinePrev = self.step_commons['optionsLinePrev']

      if self.step_commons.has_key('dbSlice'):
        self.dbslice = self.step_commons['dbSlice']
          
      #if self.step_commons.has_key('generatorName'):
      #    self.generator_name = self.step_commons['generatorName']

      #if self.step_commons.has_key('extraPackages'):
      #    self.extraPackages = self.step_commons['extraPackages']

      #if self.workflow_commons.has_key('InputData'):
      #    self.InputData = self.workflow_commons['InputData']

      #if self.step_commons.has_key('inputData'):
      #    self.inputData = self.step_commons['inputData']

      if self.workflow_commons.has_key('JobType'):
          self.jobType = self.workflow_commons['JobType']
      return S_OK('Parameters resolved')
    
    def execute(self):
      """
      Called by Agent
      """
      result = self.resolveInputVariables()
      if not result['OK']:
        return result
      #if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      #  self.log.info('Skip this module, failure detected in a previous step :')
      #  self.log.info('Workflow status : %s' %(self.workflowStatus))
      #  self.log.info('Step Status %s' %(self.stepStatus))
      #  return S_OK()

      self.result = S_OK()
       
      if not self.systemConfig:
        self.result = S_ERROR( 'No LCD platform selected' )
      elif not self.applicationLog:
        self.result = S_ERROR( 'No Log file provided' )

      if not self.result['OK']:
        return self.result

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

      #mySiteRoot=sharedArea
      #self.log.info('MYSITEROOT is %s' %mySiteRoot)
      #localArea = sharedArea
      #if re.search(':',sharedArea):
      #  localArea = string.split(sharedArea,':')[0]
      #self.log.info('Setting local software area to %s' %localArea)

      ####Setup MySQL instance
      sqlwrapper = SQLWrapper(self.dbslice)
      result =sqlwrapper.mysqlSetup()
      if not result['OK']:
        return result

      ###steering file that will be used to run
      mokkasteer = "mokka.steer"
      ###prepare steering file
      #first, I need to take the stdhep file, stripped of its path (possible LFN)
      self.stdhepFile = os.path.basename(self.stdhepFile)
      ##idem for steering file
      self.steeringFile = os.path.basename(self.steeringFile)
      steerok = PrepareSteeringFile(self.steeringFile,mokkasteer,self.detectorModel,self.stdhepFile,self.numberOfEvents,self.startFrom)
      if not steerok:
        self.log.error('Failed to create MOKKA steering file')
        return S_ERROR('Failed to create MOKKA steering file')

      scriptName = 'Mokka_%s_Run_%s.sh' %(self.applicationVersion,self.STEP_NUMBER)

      if os.path.exists(scriptName): os.remove(scriptName)
      script = open(scriptName,'w')
      script.write('#!/bin/sh \n')
      script.write('#####################################################################\n')
      script.write('# Dynamically generated script to run a production or analysis job. #\n')
      script.write('#####################################################################\n')
      #if(os.path.exists(sharedArea+"/initILCSOFT.sh")):
      #    script.write("%s/initILCSOFT.sh"%sharedArea)
      script.write("g4releases=%s/ilcsoft\n"%(sharedArea))
      script.write("G4SYSTEM=Linux-g++\n")
      script.write("G4INSTALL=$g4releases/share/$g4version\n")
      script.write("export G4SYSTEM G4INSTALL G4LIB CLHEP_BASE_DIR\n")
      script.write('G4LEDATA="$g4releases/sl4/g4data/g4dataEMLOW"\n')
      script.write('G4NEUTRONHPDATA="$g4releases/sl4/g4data/g4dataNDL"\n')
      script.write('G4LEVELGAMMADATA="$g4releases/sl4/g4data/g4dataPhotonEvap"\n')
      script.write('G4RADIOACTIVEDATA="$g4releases/sl4/g4data/g4dataRadiativeDecay"\n')
      ###No such data on the GRID (???)
      #script.write('G4ELASTICDATA="$g4releases/share/data/G4ELASTIC1.1"\n')
      script.write('G4ABLADATA="$g4releases/sl4/g4data/g4dataABLA"\n')
      script.write("export G4LEDATA G4NEUTRONHPDATA G4LEVELGAMMADATA G4RADIOACTIVEDATA G4ABLADATA\n")
      if(os.path.exists("lib")):
        if os.environ.has_key('LD_LIBRARY_PATH'):
          script.write('export LD_LIBRARY_PATH=lib:lddlib:%s'%os.environ['LD_LIBRARY_PATH'])
        else:
          script.write('export LD_LIBRARY_PATH=lib:lddlib')
      script.write("export PATH=lddlib:%s"%os.environ['PATH'])
      comm = "./mokkadbscripts/mokka-wrapper.sh %s"%mokkasteer
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
        self.log.error( "Mokka execution completed with errors:" )
        failed = True
      else:
        self.log.info( "Mokka execution completed successfully")

      if failed==True:
        self.log.error( "==================================\n StdError:\n" )
        self.log.error( self.stdError )
        #self.setApplicationStatus('%s Exited With Status %s' %(self.applicationName,status))
        self.log.error('Mokka Exited With Status %s' %(status))
        return S_ERROR('Mokka Exited With Status %s' %(status))

      ###cleanup after putting some dirt...
      result = sqlwrapper.mysqlCleanUp()
      if not result['OK']:
        return result
      # Still have to set the application status e.g. user job case.
      self.setApplicationStatus('Mokka %s Successful' %(self.applicationVersion))
      return S_OK('Mokka %s Successful' %(self.applicationVersion))

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

        