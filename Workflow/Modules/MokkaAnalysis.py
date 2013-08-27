#####################################################
# $HeadURL$
#####################################################
'''
Mokka analysis module. 

Called by Job Agent. 

@since:  Jan 29, 2010

@author: Stephane Poss
@author: Przemyslaw Majewski
'''

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.Subprocess                      import shellCall
from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase, GenRandString
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation  import LocalArea, SharedArea, getSoftwareFolder
from ILCDIRAC.Core.Utilities.PrepareOptionFiles           import PrepareSteeringFile, GetNewLDLibs
from ILCDIRAC.Core.Utilities.SQLWrapper                   import SQLWrapper
from ILCDIRAC.Core.Utilities.PrepareLibs                  import removeLibc

from ILCDIRAC.Core.Utilities.resolvePathsAndNames         import resolveIFpaths, getProdFilename
from ILCDIRAC.Core.Utilities.FindSteeringFileDir          import getSteeringFileDirName

from DIRAC                                                import S_OK, S_ERROR, gLogger, gConfig

import  os, shutil, types

class MokkaAnalysis(ModuleBase):
  """
  Specific Module to run a Mokka job.
  """
  def __init__(self):
    super(MokkaAnalysis, self).__init__()
    self.enable = True
    self.STEP_NUMBER = ''
    self.log = gLogger.getSubLogger( "MokkaAnalysis" )
    self.SteeringFile = ''
    self.InputFile = [] 
    self.macFile = ''
    self.detectorModel = '' 
    self.run_number = 0
    self.firstEventNumber = 1
    self.applicationName = 'Mokka'
    self.dbSlice = ''
    self.NumberOfEvents = 0
    self.startFrom = 0
    self.eventstring = ['>>> Event']
    self.processID = ''
    self.RandomSeed = 0
    self.mcRunNumber = 0

#############################################################################
  def applicationSpecificInputs(self):
    """ Resolve all input variables for the module here.
    @return: S_OK()
    """

    if self.step_commons.has_key('numberOfEvents'):
      self.NumberOfEvents = self.step_commons['numberOfEvents']
          
    if self.step_commons.has_key('startFrom'):
      self.startFrom = self.step_commons['startFrom']
      
    if self.WorkflowStartFrom:
      self.startFrom = self.WorkflowStartFrom

      #Need to keep until old prods are archived.
    if self.step_commons.has_key("steeringFile"):
      self.SteeringFile = self.step_commons['steeringFile']

    if self.step_commons.has_key('stdhepFile'):
      inputf = self.step_commons["stdhepFile"]
      if not type(inputf) == types.ListType:
        inputf = inputf.split(";")
      self.InputFile = inputf
        
      
    if self.step_commons.has_key('macFile'):
      self.macFile = self.step_commons['macFile']

    if self.step_commons.has_key('detectorModel'):
      self.detectorModel = self.step_commons['detectorModel']
        
    if self.step_commons.has_key('ProcessID'):
      self.processID = self.step_commons['ProcessID']
      
    if not self.RandomSeed:
      if self.step_commons.has_key("RandomSeed"):
        self.RandomSeed = self.step_commons["RandomSeed"]
      elif self.jobID:
        self.RandomSeed = self.jobID  
    if self.workflow_commons.has_key("IS_PROD"):  
      self.RandomSeed = int(str(int(self.workflow_commons["PRODUCTION_ID"])) + str(int(self.workflow_commons["JOB_ID"])))
      
    if self.step_commons.has_key('dbSlice'):
      self.dbSlice = self.step_commons['dbSlice']
      
    if self.workflow_commons.has_key("IS_PROD"):
      if self.workflow_commons["IS_PROD"]:
        self.mcRunNumber = self.RandomSeed
        #self.OutputFile = getProdFilename(self.outputFile,int(self.workflow_commons["PRODUCTION_ID"]),
        #                                  int(self.workflow_commons["JOB_ID"]))
        if self.workflow_commons.has_key('ProductionOutputData'):
          outputlist = self.workflow_commons['ProductionOutputData'].split(";")
          for obj in outputlist:
            if obj.lower().count("_sim_"):
              self.OutputFile = os.path.basename(obj)
            elif obj.lower().count("_gen_"):
              self.InputFile = [os.path.basename(obj)]
        else:
          self.OutputFile = getProdFilename(self.OutputFile, int(self.workflow_commons["PRODUCTION_ID"]),
                                            int(self.workflow_commons["JOB_ID"]))
          #if self.workflow_commons.has_key("WhizardOutput"):
          #  self.InputFile = getProdFilename(self.workflow_commons["WhizardOutput"],
          #                                    int(self.workflow_commons["PRODUCTION_ID"]),
          #                                    int(self.workflow_commons["JOB_ID"]))
          self.InputFile = [getProdFilename(self.InputFile, int(self.workflow_commons["PRODUCTION_ID"]),
                                            int(self.workflow_commons["JOB_ID"]))]
      
    if len(self.InputData):      
      if 'EvtClass' in self.inputdataMeta and not self.processID:
        self.processID = self.inputdataMeta['EvtClass']
      if 'EvtType' in self.inputdataMeta and not self.processID:
        self.processID = self.inputdataMeta['EvtType']

    if not len(self.InputFile) and len(self.InputData):
      for files in self.InputData:
        if files.lower().find(".stdhep") > -1 or files.lower().find(".hepevt") > -1:
          self.InputFile.append(files)
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

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('Mokka should not proceed as previous step did not end properly')

    cwd = os.getcwd()
    root = gConfig.getValue('/LocalSite/Root', cwd)
    self.log.info( "Executing Mokka %s" % ( self.applicationVersion ))
    self.log.info("Platform for job is %s" % ( self.systemConfig ) )
    self.log.info("Root directory for job is %s" % ( root ) )

    mokkaDir = self.ops.getValue('/AvailableTarBalls/%s/%s/%s/TarBall' % (self.systemConfig, "mokka", 
                                                                          self.applicationVersion), '')
    if not mokkaDir:
      self.log.error('Could not get Tar ball name')
      return S_ERROR('Failed finding software directory')
    mokkaDir = mokkaDir.replace(".tgz", "").replace(".tar.gz", "")
    #mokkaDir = 'lddLib' ###Temporary while mokka tar ball are not redone.
    mySoftwareRoot = ''
    localArea = LocalArea()
    sharedArea = SharedArea()
    if os.path.exists('%s%s%s' % (localArea, os.sep, mokkaDir)):
      mySoftwareRoot = localArea
    elif os.path.exists('%s%s%s' % (sharedArea, os.sep, mokkaDir)):
      mySoftwareRoot = sharedArea
    else:
      self.log.error("Mokka: could not find installation directory!")
      return S_ERROR("Mokka installation could not be found")  
    ##This is a duplication of what is above, but the above cannot easily be removed...
    res = getSoftwareFolder(mokkaDir)
    if not res['OK']:
      self.log.error("Mokka: could not find installation directory!")
      return res
    myMokkaDir = res['Value']
      
    if not mySoftwareRoot:
      self.log.error('Directory %s was not found in either the local area %s or shared area %s' % (mokkaDir, localArea, 
                                                                                                   sharedArea))
      return S_ERROR('Failed to discover software')


    ####Setup MySQL instance      
    MokkaDBrandomName =  '/tmp/MokkaDBRoot-' + GenRandString(8)
      
    #sqlwrapper = SQLWrapper(self.dbslice,mySoftwareRoot,"/tmp/MokkaDBRoot")#mySoftwareRoot)
    sqlwrapper = SQLWrapper(mySoftwareRoot, MokkaDBrandomName)#mySoftwareRoot)
    res = sqlwrapper.setDBpath(myMokkaDir, self.dbSlice)
    if not res['OK']:
      self.log.error("Failed to find the DB slice")
      return res
    result = sqlwrapper.makedirs()
    if not result['OK']:
      self.setApplicationStatus('MySQL setup failed to create directories.')
      return result
    result = sqlwrapper.mysqlSetup()
    if not result['OK']:
      self.setApplicationStatus('MySQL setup failed.')
      return result

    ##Need to fetch the new LD_LIBRARY_PATH
    new_ld_lib_path = GetNewLDLibs(self.systemConfig, "mokka", self.applicationVersion)

    ##Remove libc
    removeLibc(myMokkaDir)

    ##Get the particle.tbl, if any
    path_to_particle_tbl = ''
    dir_to_particletbl = os.path.join(myMokkaDir, 'ConfigFiles')
    if os.path.exists(dir_to_particletbl):
      configdir = os.listdir(dir_to_particletbl)
      if 'particle.tbl' in configdir:
        path_to_particle_tbl = os.path.join(dir_to_particletbl, 'particle.tbl')
    if os.path.exists('./particle.tbl'):
      path_to_particle_tbl = "./particle.tbl"    
    ###steering file that will be used to run
    mokkasteer = "mokka.steer"
    if os.path.exists("mokka.steer"):
      try:
        shutil.move("mokka.steer", "mymokka.steer")
      except:
        self.log.error("Failed renaming the steering file")
      self.SteeringFile = "mymokka.steer"
        
    ###prepare steering file
    #first, I need to take the stdhep file, find its path (possible LFN)      
    if len(self.InputFile) > 0:
      #self.InputFile = os.path.basename(self.InputFile)
      res = resolveIFpaths(self.InputFile)
      if not res['OK']:
        self.log.error("Generator file not found")
        result = sqlwrapper.mysqlCleanUp()
        return res
      self.InputFile = res['Value']
    if len(self.macFile) > 0:
      self.macFile = os.path.basename(self.macFile)
    ##idem for steering file
      
    self.SteeringFile = os.path.basename(self.SteeringFile)
    if not os.path.exists(self.SteeringFile):
      res =  getSteeringFileDirName(self.systemConfig, "mokka", self.applicationVersion)
      if not res['OK']:
        result = sqlwrapper.mysqlCleanUp()
        return res
      steeringfiledirname = res['Value']
      if os.path.exists(os.path.join(steeringfiledirname, self.SteeringFile)):
        try:
          shutil.copy(os.path.join(steeringfiledirname, self.SteeringFile), "./" + self.SteeringFile )
        except Exception, x:
          result = sqlwrapper.mysqlCleanUp()
          return S_ERROR('Failed to access file %s: %s' % (self.SteeringFile, str(x)))  
          #self.steeringFile = os.path.join(mySoftwareRoot,"steeringfiles",self.steeringFile)
    if not os.path.exists(self.SteeringFile):
      result = sqlwrapper.mysqlCleanUp()
      return S_ERROR("Could not find steering file")
    
    ### The following is because if someone uses particle gun, there is no InputFile
    if not len(self.InputFile):
      self.InputFile = ['']
    steerok = PrepareSteeringFile(self.SteeringFile, mokkasteer, self.detectorModel, self.InputFile[0],
                                  self.macFile, self.NumberOfEvents, self.startFrom, self.RandomSeed,
                                  self.mcRunNumber,
                                  path_to_particle_tbl,
                                  self.processID,
                                  self.debug,
                                  self.OutputFile,
                                  self.inputdataMeta)
    if not steerok['OK']:
      self.log.error('Failed to create MOKKA steering file')
      return S_ERROR('Failed to create MOKKA steering file')

    ###Extra option depending on mokka version
    mokkaextraoption = ""
    if self.applicationVersion not in ["v07-02", "v07-02fw", "v07-02fwhp", "MokkaRevision42", "MokkaRevision43", 
                                       "MokkaRevision44", "Revision45"]:
      mokkaextraoption = "-U"

    scriptName = 'Mokka_%s_Run_%s.sh' % (self.applicationVersion, self.STEP_NUMBER)

    if os.path.exists(scriptName): 
      os.remove(scriptName)
    script = open(scriptName, 'w')
    script.write('#!/bin/sh \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')
    #if(os.path.exists(sharedArea+"/initILCSOFT.sh")):
    #    script.write("%s/initILCSOFT.sh"%sharedArea)
    script.write("declare -x g4releases=%s\n" % (myMokkaDir))
    script.write("declare -x G4SYSTEM=Linux-g++\n")
    script.write("declare -x G4INSTALL=$g4releases/share/$g4version\n")
    #script.write("export G4SYSTEM G4INSTALL G4LIB CLHEP_BASE_DIR\n")
    script.write('declare -x G4LEDATA="$g4releases/G4LEDATA"\n')
    script.write('declare -x G4NEUTRONHPDATA="$g4releases/sl4/g4data/g4dataNDL"\n')
    script.write('declare -x G4LEVELGAMMADATA="$g4releases/sl4/g4data/g4dataPhotonEvap"\n')
    script.write('declare -x G4RADIOACTIVEDATA="$g4releases/sl4/g4data/g4dataRadiativeDecay"\n')
    ###No such data on the GRID (???)
    #script.write('G4ELASTICDATA="$g4releases/share/data/G4ELASTIC1.1"\n')
    script.write('declare -x G4ABLADATA="$g4releases/sl4/g4data/g4dataABLA"\n')
    #script.write("export G4LEDATA G4NEUTRONHPDATA G4LEVELGAMMADATA G4RADIOACTIVEDATA G4ABLADATA\n")
    script.write('declare -x G4NEUTRONHP_NEGLECT_DOPPLER=1\n')
    #### Do something with the additional environment variables
    add_env = self.ops.getOptionsDict("/AvailableTarBalls/%s/%s/%s/AdditionalEnvVar" % (self.systemConfig, 
                                                                                        "mokka", 
                                                                                        self.applicationVersion))
    if add_env['OK']:
      for key in add_env['Value'].keys():
        script.write('declare -x %s=%s/%s\n' % (key, mySoftwareRoot, add_env['Value'][key]))
    else:
      self.log.verbose("No additional environment variables needed for this application")
      
    if(os.path.exists("./lib")):
      if new_ld_lib_path:
        script.write('declare -x LD_LIBRARY_PATH=./lib:%s:%s\n' % (myMokkaDir, new_ld_lib_path))
      else:
        script.write('declare -x LD_LIBRARY_PATH=./lib:%s\n' % (myMokkaDir))
    else:
      if new_ld_lib_path:
        script.write('declare -x LD_LIBRARY_PATH=%s:%s\n' % (myMokkaDir, new_ld_lib_path))
      else:
        script.write('declare -x LD_LIBRARY_PATH=%s\n' % (myMokkaDir))          
          
    script.write("declare -x PATH=%s:%s\n" % (myMokkaDir, os.environ['PATH']))
      
    script.write('echo =============================\n')
    script.write('echo Content of mokka.steer:\n')
    script.write('cat mokka.steer\n')
    script.write('echo =============================\n')
    script.write('echo Content of mokkamac.mac:\n')
    script.write('cat mokkamac.mac\n')
    script.write('echo =============================\n')
    script.write('echo LD_LIBRARY_PATH is\n')
    script.write('echo $LD_LIBRARY_PATH | tr ":" "\n"\n')
    script.write('echo =============================\n')
    script.write('echo PATH is\n')
    script.write('echo $PATH | tr ":" "\n"\n')
    script.write('env | sort >> localEnv.log\n')      
    script.write('echo =============================\n')
      
    ##Tear appart this mokka-wrapper
    comm = '%s/Mokka %s -hlocalhost:%s/mysql.sock %s %s\n' % (myMokkaDir, mokkaextraoption, sqlwrapper.getMokkaTMPDIR(), 
                                                           mokkasteer, self.extraCLIarguments)
    print "Command : %s" % (comm)
    script.write(comm)
    script.write('declare -x appstatus=$?\n')
    #script.write('where\n')
    #script.write('quit\n')
    #script.write('EOF\n')

    script.write('exit $appstatus\n')

    script.close()
    if os.path.exists(self.applicationLog): 
      os.remove(self.applicationLog)

    os.chmod(scriptName, 0755)
    comm = 'sh -c "./%s"' % scriptName
    self.setApplicationStatus('Mokka %s step %s' % (self.applicationVersion, self.STEP_NUMBER))
    self.stdError = ''
    self.result = shellCall(0, comm, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)
    #self.result = {'OK':True,'Value':(0,'Disabled Execution','')}
    resultTuple = self.result['Value']

    status = resultTuple[0]
    # stdOutput = resultTuple[1]
    # stdError = resultTuple[2]
    self.log.info( "Status after Mokka execution is %s" % str( status ) )
    result = sqlwrapper.mysqlCleanUp()
    if not os.path.exists(self.applicationLog):
      self.log.error("Something went terribly wrong, the log file is not present")
      self.setApplicationStatus('%s failed terribly, you are doomed!' % (self.applicationName))
      if not self.ignoreapperrors:
        return S_ERROR('%s did not produce the expected log' % (self.applicationName))
    ###Now change the name of Mokka output to the specified filename
    if os.path.exists("out.slcio"):
      if len(self.OutputFile) > 0:
        os.rename("out.slcio", self.OutputFile)

    failed = False
    if not status in [0, 106, 9]:
      self.log.error( "Mokka execution completed with errors:" )
      failed = True
    elif status in [106, 9]:
      self.log.info( "Mokka execution reached end of input generator file")
    else:
      self.log.info( "Mokka execution finished successfully")

    message = 'Mokka %s Successful' % (self.applicationVersion)
    if failed == True:
      self.log.error( "==================================\n StdError:\n" )
      self.log.error( self.stdError) 
      #self.setApplicationStatus('%s Exited With Status %s' %(self.applicationName,status))
      self.log.error('Mokka Exited With Status %s' % (status))
      message = 'Mokka Exited With Status %s' % (status)
      self.setApplicationStatus(message)
      if not self.ignoreapperrors:
        return S_ERROR(message)
    else:
      if status in [106, 9]:
        message = 'Mokka %s reached end of input generator file' % (self.applicationVersion)
      self.setApplicationStatus(message)
    return S_OK( { 'OutputFile' : self.OutputFile } )

  #############################################################################


