# $HeadURL$
# $Id$
'''
Mokka analysis module. Called by Job Agent. 

@since:  Jan 29, 2010

@author: Stephane Poss and Przemyslaw Majewski
'''
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from ILCDIRAC.Workflow.Modules.ModuleBase                import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation  import LocalArea,SharedArea
from ILCDIRAC.Core.Utilities.PrepareOptionFiles         import PrepareSteeringFile
from ILCDIRAC.Core.Utilities.SQLWrapper                   import SQLWrapper
from ILCDIRAC.Core.Utilities.ResolveDependencies          import resolveDepsTar
from ILCDIRAC.Core.Utilities.resolveIFpaths import resolveIFpaths
from ILCDIRAC.Core.Utilities.resolveOFnames import getProdFilename

from DIRAC                                               import S_OK, S_ERROR, gLogger, gConfig

import DIRAC

import re, os, sys

#random string generator 
import string
from random import choice



class MokkaAnalysis(ModuleBase):
    """
    Specific Module to run a Mokka job.
    """
    def __init__(self):
        ModuleBase.__init__(self)
        self.enable = True
        self.STEP_NUMBER = ''
        self.debug = True
        self.log = gLogger.getSubLogger( "MokkaAnalysis" )
        self.result = S_ERROR()
        self.steeringFile = ''
        self.stdhepFile = ''
        self.macFile = ''
        self.run_number = 0
        self.firstEventNumber = 1
        self.jobID = None
        if os.environ.has_key('JOBID'):
            self.jobID = os.environ['JOBID']

        self.systemConfig = ''
        self.applicationLog = ''
        self.applicationName = 'Mokka'
        self.applicationVersion=''
        self.dbslice = ''
        self.numberOfEvents = 0
        self.startFrom = 1
        self.inputData = '' # to be resolved
        self.InputData = '' # from the (JDL WMS approach)
        self.outputFile = ''
        #self.generator_name=''
        #self.optionsLinePrev = ''
        #self.optionsLine = ''
        #self.extraPackages = ''
        self.jobType = ''
        self.debug = False
#############################################################################
    def resolveInputVariables(self):
      """ Resolve all input variables for the module here.
      @return: S_OK()
      """
      if self.workflow_commons.has_key('SystemConfig'):
          self.systemConfig = self.workflow_commons['SystemConfig']

      if self.workflow_commons.has_key('JobType'):
        self.jobType = self.workflow_commons['JobType']

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
      
      if self.step_commons.has_key('macFile'):
        self.macFile = self.step_commons['macFile']

      if self.step_commons.has_key('detectorModel'):
        self.detectorModel = self.step_commons['detectorModel']

      if self.step_commons.has_key('dbSlice'):
        self.dbslice = self.step_commons['dbSlice']
      if self.step_commons.has_key('debug'):
        self.debug = self.step_commons['debug']
      if self.step_commons.has_key("outputFile"):
        self.outputFile = self.step_commons["outputFile"]
      
      if self.workflow_commons.has_key("IS_PROD"):
        if self.workflow_commons["IS_PROD"]:
          self.outputFile = getProdFilename(self.outputFile,int(self.workflow_commons["PRODUCTION_ID"]),
                                            int(self.workflow_commons["JOB_ID"]))
          
      if self.workflow_commons.has_key('InputData'):
          self.InputData = self.workflow_commons['InputData']

      #if self.step_commons.has_key('inputData'):
      #  self.inputData = self.step_commons['inputData']

      if len(self.stdhepFile)==0 and not len(self.InputData)==0:
        inputfiles = self.InputData.split(";")
        for files in inputfiles:
          if files.lower().find(".stdhep")>-1 or files.lower().find(".hepevt")>-1:
            self.stdhepFile = files
            break
        
      return S_OK('Parameters resolved')
    
    def execute(self):
      """ Called by Agent
      
      Executes the following:
        - read the application parameters that where defined in ILCJob, and stored in the job definition
        - setup the SQL server and run it in the background, via a call to L{SQLWrapper}
        - prepare the steering fie using L{PrepareSteeringFile}
        - run Mokka and catch its return status
      @return: S_OK(), S_ERROR()
      
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
        self.result = S_ERROR( 'No ILC platform selected' )
      elif not self.applicationLog:
        self.result = S_ERROR( 'No Log file provided' )

      if not self.result['OK']:
        return self.result

      cwd = os.getcwd()
      self.root = gConfig.getValue('/LocalSite/Root',cwd)
      self.log.info( "Executing Mokka %s"%(self.applicationVersion))
      self.log.info("Platform for job is %s" % ( self.systemConfig ) )
      self.log.info("Root directory for job is %s" % ( self.root ) )

      mokkaDir = gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s/TarBall'%(self.systemConfig,"mokka",self.applicationVersion),'')
      mokkaDir = mokkaDir.replace(".tgz","").replace(".tar.gz","")
      #mokkaDir = 'lddLib' ###Temporary while mokka tar ball are not redone.
      mySoftwareRoot = ''
      localArea = LocalArea()
      sharedArea = SharedArea()
      if os.path.exists('%s%s%s' %(localArea,os.sep,mokkaDir)):
        mySoftwareRoot = localArea
      elif os.path.exists('%s%s%s' %(sharedArea,os.sep,mokkaDir)):
        mySoftwareRoot = sharedArea
      else:
        self.log.error("Mokka: could not find installation directory!")
        return S_ERROR("Mokka installation could not be found")  
      myMokkaDir = os.path.join(mySoftwareRoot,mokkaDir)
      
      if not mySoftwareRoot:
        self.log.error('Directory %s was not found in either the local area %s or shared area %s' %(mokkaDir,localArea,sharedArea))
        return S_ERROR('Failed to discover software')

      ### Resolve dependencies
      deps = resolveDepsTar(self.systemConfig,"mokka",self.applicationVersion)
      for dep in deps:
        if os.path.exists(os.path.join(mySoftwareRoot,dep.rstrip(".tgz").rstrip(".tar.gz"))):
          depfolder = dep.rstrip(".tgz").rstrip(".tar.gz")
          if os.path.exists(os.path.join(mySoftwareRoot,depfolder,"lib")):
            self.log.verbose("Found lib folder in %s"%(depfolder))
            if os.environ.has_key("LD_LIBRARY_PATH"):
              os.environ["LD_LIBRARY_PATH"] = os.path.join(mySoftwareRoot,depfolder,"lib")+":%s"%os.environ["LD_LIBRARY_PATH"]
            else:
              os.environ["LD_LIBRARY_PATH"] = os.path.join(mySoftwareRoot,depfolder,"lib")

      ####Setup MySQL instance
      
      MokkaDBrandomName =  '/tmp/MokkaDBRoot-' + self.GenRandString(8);
      
      #sqlwrapper = SQLWrapper(self.dbslice,mySoftwareRoot,"/tmp/MokkaDBRoot")#mySoftwareRoot)
      sqlwrapper = SQLWrapper(self.dbslice,mySoftwareRoot,MokkaDBrandomName)#mySoftwareRoot)
      result = sqlwrapper.makedirs()
      if not result['OK']:
        self.setApplicationStatus('MySQL setup failed to create directories.')
        return result
      result =sqlwrapper.mysqlSetup()
      if not result['OK']:
        self.setApplicationStatus('MySQL setup failed.')
        return result

      ###steering file that will be used to run
      mokkasteer = "mokka.steer"
      ###prepare steering file
      #first, I need to take the stdhep file, find its path (possible LFN)
      
      if len(self.stdhepFile)>0:
        #self.stdhepFile = os.path.basename(self.stdhepFile)
        res = resolveIFpaths([self.stdhepFile])
        if not res['OK']:
          self.log.error("Generator file not found")
          return res
        self.stdhepFile = res['Value'][0]
      if len(self.macFile)>0:
        self.macFile = os.path.basename(self.macFile)
      ##idem for steering file
      self.steeringFile = os.path.basename(self.steeringFile)
      steerok = PrepareSteeringFile(self.steeringFile,mokkasteer,self.detectorModel,self.stdhepFile,
                                    self.macFile,self.numberOfEvents,self.startFrom,self.debug,
                                    self.outputFile)
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
      script.write("declare -x g4releases=%s\n" %(myMokkaDir))
      script.write("declare -x G4SYSTEM=Linux-g++\n")
      script.write("declare -x G4INSTALL=$g4releases/share/$g4version\n")
      #script.write("export G4SYSTEM G4INSTALL G4LIB CLHEP_BASE_DIR\n")
      script.write('declare -x G4LEDATA="$g4releases/sl4/g4data/g4dataEMLOW"\n')
      script.write('declare -x G4NEUTRONHPDATA="$g4releases/sl4/g4data/g4dataNDL"\n')
      script.write('declare -x G4LEVELGAMMADATA="$g4releases/sl4/g4data/g4dataPhotonEvap"\n')
      script.write('declare -x G4RADIOACTIVEDATA="$g4releases/sl4/g4data/g4dataRadiativeDecay"\n')
      ###No such data on the GRID (???)
      #script.write('G4ELASTICDATA="$g4releases/share/data/G4ELASTIC1.1"\n')
      script.write('declare -x G4ABLADATA="$g4releases/sl4/g4data/g4dataABLA"\n')
      #script.write("export G4LEDATA G4NEUTRONHPDATA G4LEVELGAMMADATA G4RADIOACTIVEDATA G4ABLADATA\n")
      
      #### Do something with the additional environment variables
      add_env = gConfig.getOptionsDict("/Operations/AvailableTarBalls/%s/%s/%s/AdditionalEnvVar"%(self.systemConfig,"mokka",self.applicationVersion))
      if add_env['OK']:
        for key in add_env['Value'].keys():
          script.write('declare -x %s=%s/%s\n'%(key,mySoftwareRoot,add_env['Value'][key]))
      else:
        self.log.verbose("No additional environment variables needed for this application")
      
      if(os.path.exists("./lib")):
        if os.environ.has_key('LD_LIBRARY_PATH'):
          script.write('declare -x LD_LIBRARY_PATH=./lib:%s:%s\n'%(myMokkaDir,os.environ['LD_LIBRARY_PATH']))
        else:
          script.write('declare -x LD_LIBRARY_PATH=./lib:%s\n' %(myMokkaDir))
      else:
        if os.environ.has_key('LD_LIBRARY_PATH'):
          script.write('declare -x LD_LIBRARY_PATH=%s:%s\n'%(myMokkaDir,os.environ['LD_LIBRARY_PATH']))
        else:
          script.write('declare -x LD_LIBRARY_PATH=%s\n'%(myMokkaDir))          
          
      script.write("declare -x PATH=%s:%s\n"%(myMokkaDir,os.environ['PATH']))
      
      script.write('echo =============================\n')
      script.write('echo LD_LIBRARY_PATH is\n')
      script.write('echo $LD_LIBRARY_PATH | tr ":" "\n"\n')
      script.write('echo =============================\n')
      script.write('echo PATH is\n')
      script.write('echo $PATH | tr ":" "\n"\n')
      script.write('env | sort >> localEnv.log\n')      
      script.write('echo =============================\n')
      
      ##Tear appart this mokka-wrapper
      comm = '%s/Mokka -hlocalhost:%s/mysql.sock %s\n'%(myMokkaDir,sqlwrapper.getMokkaTMPDIR(),mokkasteer)
      print "Command : %s"%(comm)
      script.write(comm)
      script.write('declare -x appstatus=$?\n')
      #script.write('where\n')
      #script.write('quit\n')
      #script.write('EOF\n')

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
      self.log.info( "Status after Mokka execution is %s" % str( status ) )
      result = sqlwrapper.mysqlCleanUp()

      ###Now change the name of Mokka output to the specified filename
      if os.path.exists("out.slcio"):
        if len(self.outputFile)>0:
          os.rename("out.slcio", self.outputFile)

      failed = False
      if not status == 0 and not status==106 :
        self.log.error( "Mokka execution completed with errors:" )
        failed = True
      elif status==106:
        self.log.info( "Mokka execution reached end of input generator file")
      else:
        self.log.info( "Mokka execution finished successfully")

      if failed==True:
        self.log.error( "==================================\n StdError:\n" )
        self.log.error( self.stdError) 
        #self.setApplicationStatus('%s Exited With Status %s' %(self.applicationName,status))
        self.log.error('Mokka Exited With Status %s' %(status))
        self.setApplicationStatus('Mokka Exited With Status %s' %(status))
        return S_ERROR('Mokka Exited With Status %s' %(status))

      ###cleanup after putting some dirt...
      #for now cleanup is executed by mokka-wrapper.sh on exit or kill
      
      #result = sqlwrapper.mysqlCleanUp()
      #if not result['OK']:
      #  return result
      # Still have to set the application status e.g. user job case.
      if status==106:
        self.setApplicationStatus('Mokka %s reached end of input generator file' %(self.applicationVersion))
        return S_OK('Mokka %s reached end of input generator file' %(self.applicationVersion))
      self.setApplicationStatus('Mokka %s Successful' %(self.applicationVersion))
      return S_OK('Mokka %s Successful' %(self.applicationVersion))

    #############################################################################
    def redirectLogOutput(self, fd, message):
      """Used to catch the application print outs
      """
      sys.stdout.flush()
      if message:
        if re.search('>>> Event',message): print message
      if self.applicationLog:
        log = open(self.applicationLog,'a')
        log.write(message+'\n')
        log.close()
      else:
        self.log.error("Application Log file not defined")
      if fd == 1:
        self.stdError += message
    #############################################################################

    def GenRandString(self, length=8, chars=string.letters + string.digits):
      """Return random string of 8 chars
      """
      return ''.join([choice(chars) for i in range(length)])
