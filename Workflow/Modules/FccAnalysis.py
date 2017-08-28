"""FCC application is defined by 2 modules which are :

     - ILCDIRAC.Interfaces.API.NewInterface.Applications.Fcc
     - ILCDIRAC.Workflow.Modules.FccAnalysis (this module)

"""

# standard libraries
import os
import re
import glob
import shutil

# DIRAC libraries
from ILCDIRAC.Workflow.Modules.ModuleBase import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import getEnvironmentScript
from DIRAC.Core.Utilities import shellCall
from DIRAC import gLogger, S_ERROR, S_OK

__RCSID__ = "$Id$"


class FccAnalysis(ModuleBase):
  """FccAnalysis class generates a bash script that will run
  the FCC application considering FCC module parameters.

  It inherits from ModuleBase class.

  """

  def __init__(self):

    super(FccAnalysis, self).__init__()

    self.enable = True

    self.fccExecutable = ''
    self.isGaudiOptionsFileNeeded = False
    self.gaudiOptionsFile = ''
    self.fccAppIndex = ''
    self.STEP_NUMBER = ''
    self.RandomSeed = 0
    self.read = False
    self.randomGenerator = {}
    self.environmentScript = ''

    self.platform = ''
    self.debug = True
    self.log = gLogger.getSubLogger("FccAnalysis")

    self.applicationScript = ''

    # Gaudi log levels
    self.logLevels = ['DEBUG', 'INFO', 'ERROR', 'FATAL']

    # User log level chosen
    self.logLevel = None

  def runIt(self):
    """Main method called by the Agent.
       The Application's call must reside here.

       This module consists on creating a bash script calling
       an executable followed by arguments provided by the Fcc module.

    :return: The success or the failure of the execution
    :rtype: DIRAC.S_OK, DIRAC.S_ERROR

    """

    self.result = S_OK()
    if not self.platform:
      errorMessage = 'No ILC platform selected'
      self.log.error(errorMessage)
      self.result = S_ERROR( errorMessage )
    elif not self.applicationLog:
      errorMessage = 'No Log file provided'
      self.log.error(errorMessage)
      self.result = S_ERROR( errorMessage )
    if not self.result['OK']:
      self.log.error("Failed to resolve input parameters:", self.result["Message"])
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('%s should not proceed as previous step did not end properly' % self.applicationName)

    if 'IS_PROD' in self.workflow_commons:
      self.RandomSeed = int(str(int(self.workflow_commons["PRODUCTION_ID"])) + str(int(self.workflow_commons["JOB_ID"])))
    elif self.jobID:
      self.RandomSeed = self.jobID

    debugMessage = (
      "Splitting : Parameter 'RandomSeed' given successfully"
      " with this value '%(RandomSeed)s'" % {'RandomSeed':self.RandomSeed}
    )

    self.log.debug(debugMessage)
    
    self.log.info("Environment : Environment script look up...")

    # Try to locate environment script in 'dirac.cfg' file
    if not self.getEnvironmentScript():
      errorMessage = (
        "Environment : Environment script look up failed\n"
        "Failed to get environment"
      )
      self.log.error(errorMessage)
      return S_ERROR(errorMessage)

    debugMessage = "Environment : Environment script found at : %s" % self.environmentScript
    self.log.debug(debugMessage)

    self.log.info("Environment : Environment script look up successfull")

    # Each Fcc application has its own folder to store results etc...
    self.fccAppIndex = "%s_%s_Step_%s" % (self.applicationName, self.applicationVersion, self.STEP_NUMBER)
    self.applicationScript = os.path.realpath("%s.sh" % self.fccAppIndex)

    getOutputFileFromApp = self.step_commons.get('InputFile', self.InputFile)
    if getOutputFileFromApp:
      self.InputFile = os.path.realpath(getOutputFileFromApp)
    
    # SteeringFile is replaced by ouput of previous application
    # if it is FccAnalysis
    if self.InputFile and not self.isGaudiOptionsFileNeeded:
      self.SteeringFile = self.InputFile
      self.log.debug("Application : Configuration file taken from the input file '%s'" % self.InputFile)
    #elif self.SteeringFile and not self.SteeringFile.startswith('/cvmfs/'):
    # We uploaded even cvmfs file to sandbox in Fcc module
    elif self.SteeringFile:
      self.SteeringFile = os.path.realpath(os.path.basename(self.SteeringFile))

    if not os.path.exists(self.SteeringFile):
      errorMessage = (
        "Environment : FCC configuration file does not exist,"
        " can not run FCC application"
      )
      self.log.error(errorMessage)
      return S_ERROR(errorMessage)
    #self.RandomSeed = 1234
    # Set the seed of the application in overwritting Pythia card file
    if "Pythia" in self.randomGenerator:
      for cardFile in self.randomGenerator["Pythia"] :
        absoluteCardFile = os.path.realpath(cardFile)    
        content, message = self.readFromFile( absoluteCardFile  )

        if not content:
          self.log.error(message)
          return S_ERROR(message)

        self.log.debug(message)

        tempContent = content

        if 'Main:numberOfEvents' in tempContent and self.NumberOfEvents:
          eventSetting = "Main:numberOfEvents = %d" % self.NumberOfEvents
          tempContent = re.sub(r'Main:numberOfEvents *= *\d+', eventSetting, tempContent)
        elif self.NumberOfEvents:
          # add the card line  
          eventSetting = ["! N) AUTOMATIC GENERATION OF CODE DONE BY FCC APPLICATION FOR EVENT NUMBER SETTING"]
          eventSetting += ["Main:numberOfEvents = %d         ! number of events to generate" % self.NumberOfEvents]
          tempContent = "%s\n%s\n" % (tempContent, "\n".join(eventSetting))

        if "Random:setSeed" in tempContent and "Random:seed" in tempContent and self.RandomSeed:
          seedSetting = "Random:seed = %d" % self.RandomSeed
          tempContent = re.sub(r'Random:seed *= *\d+', seedSetting, tempContent)
        elif self.RandomSeed:
          seedSetting = ["! N) AUTOMATIC GENERATION OF CODE DONE BY FCC APPLICATION FOR SEED SETTING"]
          seedSetting += ["Random:setSeed = on         ! apply user-set seed everytime the Pythia::init is called"]
          seedSetting += ["Random:seed = %d         ! -1=default seed, 0=seed based on time, >0 user seed number" % self.RandomSeed]
          tempContent = "%s\n%s\n" % (tempContent, "\n".join(seedSetting))
      
        if tempContent != content and not self.writeToFile('w', absoluteCardFile, tempContent):
          errorMessage = "Application : Card file overwitting failed"
          self.log.error(errorMessage)
          return S_ERROR(errorMessage)

    debugMessage = (
      "Application code : Creation of the bash script"
      " to call the application with FCC module parameters..."
    )
    self.log.debug(debugMessage)

    # FCC PHYSICS does not need this file so do not resolve it if it is not given
    # else 'realpath of "" ' will result in cwd.
    if self.isGaudiOptionsFileNeeded:
      if not self.generateGaudiConfFile():
        errorMessage = "Application code : generateGaudiConfFile() failed"
        self.log.error(errorMessage)
        return S_ERROR(errorMessage)

    # Main command
    bashCommands = ['%s %s %s' %
                    (self.fccExecutable, self.SteeringFile, self.gaudiOptionsFile)]


    if not self.generateBashScript(bashCommands):
      errorMessage = "Application code : Creation of the bash script failed"
      self.log.error(errorMessage)
      return S_ERROR(errorMessage)

    self.log.debug("Application code : Creation of the bash script successfull")

    self.log.debug("Application : Application execution and log file creation...")
    # Call of the application
    call = shellCall(0, self.applicationScript, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)

    if 'OK' in call and not call['OK']:
      errorMessage = "Application : Application execution failed"
      self.log.error(errorMessage)
      return S_ERROR(errorMessage)

    self.log.debug("Application : Application execution successfull")

    if not os.path.exists(self.applicationLog):
      errorMessage = "Application : Log file creation failed"
      self.log.error(errorMessage)
      if not self.ignoreapperrors:
        errorMessage = '%s did not produce the expected log %s' % (self.applicationName, self.applicationLog)
        self.log.error(errorMessage)
        return S_ERROR(errorMessage)

    self.log.debug("Application : Log file creation successfull")

    # Check if root file has been generated
    rootFiles = glob.glob('*.root')

    if not rootFiles:
      self.log.warn("Application : no root files have been generated, is that normal ?")
    else:
      # Rename the last created root file
      # The last created root file is the one generated by the actual application and not a previous application
      rootFilesSorted = [(os.path.getctime(f), f) for f in rootFiles]
      rootFilesSorted.sort()
      lastCreatedRootFileTimeName = rootFilesSorted[-1]
      lastCreatedRootFileName = lastCreatedRootFileTimeName[1]
      old = os.path.realpath(lastCreatedRootFileName)

      # Rename last root file generated by the application if the application has generated a root file
      # This check avoid overwritting of the last root file generated by a previous application !
      # if the current application does not generate any root file

      applicationScriptTimeCreation = os.path.getctime(self.applicationScript)
      lastCreatedRootFileTimeCreation = os.path.getctime(old)

      renamedRootFile = os.path.realpath(self.OutputFile)

      if lastCreatedRootFileTimeCreation < applicationScriptTimeCreation:
        self.log.warn("Application : This application did not generate any root files, is that normal ?")
      else:
        debugMessage = "Application : Root file '%s' renaming..." % old
        self.log.debug(debugMessage)

        # Rename root file to make it unique at application level
        try:
          shutil.move(old, renamedRootFile)
        except (IOError, shutil.Error) as e:
          errorMessage = "Application : Application unique root file '%s' renaming failed\n%s" % (old, e)
          self.log.error(errorMessage)
          return S_ERROR(errorMessage)

        debugMessage = "Application : Application unique root file '%s' renamed successfully to '%s'" % (old, renamedRootFile)
        self.log.debug(debugMessage)

        # Update output data name else job will never find output data specified
        # at the user level through Job.setOutputData("myOutput.root") because we are renaming it
        if 'UserOutputData' in self.workflow_commons:
          outputData = self.workflow_commons['UserOutputData'].split(";")
          for idx, data in enumerate(outputData):
            if data.endswith(".root") and old == os.path.realpath(os.path.basename(data)):
              lfnTree = os.path.dirname(data)
              outputData[idx] = os.path.join(lfnTree, "JobID_%s_%s" % (self.jobID, os.path.basename(renamedRootFile)))

          self.workflow_commons['UserOutputData'] = ";".join(outputData)


    # If the last application has finished (then the job has finished)
    # Rename all root files to make them unique at job level

    if self.STEP_NUMBER == self.workflow_commons['TotalSteps']:

      rootFiles = glob.glob('*.root')

      for rootFile in rootFiles:
        uniqueRootFile = os.path.realpath("JobID_%s_%s" % (self.jobID, rootFile))

        rootFile = os.path.realpath(rootFile)

        debugMessage = "Application : Job unique root file '%s' renaming..." % rootFile
        self.log.debug(debugMessage)

        if rootFile != uniqueRootFile:
          # Rename root file to make it unique at job level
          try:
            shutil.move(rootFile, uniqueRootFile)
          except (IOError, shutil.Error) as e:
            errorMessage = "Application : Job unique root file '%s' renaming failed\n%s" % (rootFile, e)
            self.log.error(errorMessage)
            return S_ERROR(errorMessage)

          debugMessage = "Application : Job unique root file '%s' renamed successfully to '%s'" % (rootFile, uniqueRootFile)
          self.log.debug(debugMessage)

    return S_OK("Execution of the FCC application successfull")

###############################  FccAnalysis METHODS #############################################
  def generateBashScript(self, commands):
    """This function generates a bash script containing the environment setup
    and the command related to the FCC application.

    :param commands: The commands for calling the application
    :type commands: list

    :return: The success or the failure of the bash script creation
    :rtype: bool

    """

    # Set environment and execute the application
    shebang = "#!/bin/bash"

    setEnvironmentScript = 'source %s' % self.environmentScript
    bashScriptText = [shebang, setEnvironmentScript] + commands

    self.log.debug("Application command : %s" % '\n'.join(commands))

    # Write the temporary application's script
    self.log.debug("Application code : Bash script creation...")

    if not self.writeToFile('w', self.applicationScript, '\n'.join(bashScriptText) + '\n'):
      self.log.error("Application code : Bash script creation failed")
      return False

    self.log.debug("Application code : Bash script creation successfull")

    self.log.debug("Application file : Bash script rights setting...")

    os.chmod( self.applicationScript, 0o755 )
    
    self.log.debug("Application file : Bash script rights setting successfull")

    return True

  def generateGaudiConfFile(self):
    """Generation of the Gaudi configuration file
    with the setting of :

      - The number of event
      - The seed 
      - The gaudi log level
      - The input file for FCCDataSvc

    There are 2 ways for configuring gaudirun.py :
    1) By using gaudirun.py options :
    e.g. ./gaudirun.py --option "ApplicationMgr().EvtMax=10"
    
    2) By creating a new python script containing the gaudi configuration :
    This script has to be given as a second argument to gaudirun.py
    e.g. ./gaudirun.py geant_pgun_fullsim.py gaudi_options.py
    It then contains the event setting etc...
    We decided to choose the second one.

    :return: The success or the failure of the gaudi option file creation
    :rtype: bool

    """

    gaudiOptions = ["# N) AUTOMATIC GENERATION OF CODE DONE BY FCC APPLICATION FOR EVENT NUMBER AND SEED SETTING"]
    gaudiOptions += ["from Configurables import ApplicationMgr, RndmGenSvc"]
    gaudiOptions += ["from Gaudi.Configuration import *"]

    # In putting -1, gaudi read all event of the file given to FCCDataSvc
    if self.NumberOfEvents:
      eventSetting = "ApplicationMgr().EvtMax=%s" % self.NumberOfEvents
      gaudiOptions += [eventSetting]

    if "Gaudi" in self.randomGenerator and self.RandomSeed:

      randomEngineName = "HepRndm::Engine<CLHEP::RanluxEngine>"

      charsToReplace = [':', '<', '>']

      for char in charsToReplace:
        randomEngineName = randomEngineName.replace(char, "_")

      seedSetting = ['from GaudiSvc.GaudiSvcConf import %s' % randomEngineName]
      seedSetting += ["randomEngine = eval('%s')" % randomEngineName]
      seedSetting += ["randomEngine = randomEngine('RndmGenSvc.Engine')"]
      seedSetting += ["randomEngine.Seeds = [%d]  " % self.RandomSeed]

      gaudiOptions += seedSetting

    if self.logLevel:
      if self.logLevel.upper() in self.logLevels:
        levelSetting = "ApplicationMgr().OutputLevel=%s" % self.logLevel
        gaudiOptions += [levelSetting]
      else:
        message = (
          "FCCSW specific consistency : Invalid value for the log level\n"
          "Possible values for the log level are :\n%(log)s" % {'log' : " ".join(self.logLevels)}
        )
        self.log.error(message)
        return False


    # If it is an application that reads events or there are input data
    if self.read and (self.InputFile or self.InputData):
          
      fccswPodioOptions = ["from Configurables import FCCDataSvc, PodioOutput"]
      fccswPodioOptions += ["import os"]

      dataToRead = self.InputData if self.InputData else self.InputFile

      dataToList = [dataToRead] if isinstance(dataToRead, str) else dataToRead

      fccInputDataSubstitution = [ '%s' for data in dataToList]

      if self.InputData:
        # convert /ilc/vo/user/u/username/data1 to /cwd/data1
        fccInputData = ["os.path.realpath(os.path.basename('%s'))" % data
                        for data in dataToList]
      else:
        fccInputData = ["'%s'" % data for data in dataToList]

      # We can provide many input files to FCCDataSvc() like this :
      inputSetting = ["podioevent = FCCDataSvc('EventDataSvc', input='%s' %% (%s))" % (" ".join(fccInputDataSubstitution), ", ".join(fccInputData))]
      inputSetting += ["ApplicationMgr().ExtSvc += [podioevent]"]
      fccswPodioOptions += inputSetting

      gaudiOptions += fccswPodioOptions

    self.gaudiOptionsFile = '%s_gaudiOptions.py' % self.fccAppIndex

    debugMessage = 'FCCSW configuration : Gaudi configuration file creation...'
    self.log.debug(debugMessage)

    return self.writeToFile('w', self.gaudiOptionsFile,
                            "\n".join(gaudiOptions) + '\n')

  def generateScriptOnTheFly(self, sysConfig="", appName="", appVersion=""):
    """Normally, this function has to generate dynamically the
    FCC environment script but nothing for the moment.

    Called if CVMFS is not available
    (CVMFS should be always available else FCC application can't run).

    :param sysConfig: The platform required by the application
    :type sysConfig: str

    :param appName: The name of the application
    :type appName: str

    :param appVersion: The version of the application
    :type appVersion: str

    """

    # We do not generate the environment script like in MarlinAnalysis etc...
    # Because if we do not have access to cvmfs, we can do nothing.

    #print('%s %s %s' % (sysConfig, appName, appVersion))
    errorMessage = (
      "Environment : Environment script not found\n"
      "for this configuration : %(conf)s, %(name)s, %(version)s\n"
      "Can not generate one dynamically" % {'conf':sysConfig, 'name':appName, 'version':appVersion}
    )

    # Put intentionally in debug level
    self.log.debug(errorMessage)
    return S_ERROR(errorMessage)

  def getEnvironmentScript(self):
    """This function gets environment script path from 'dirac.cfg' file
    according to the version, application name and platform.

    :return: The success or failure of the environment script creation
    :rtype: DIRAC.S_OK, DIRAC.S_ERROR

    """

    environmentScript = getEnvironmentScript(self.platform, self.applicationName.lower(),
                                             self.applicationVersion, self.generateScriptOnTheFly)

    if 'OK' in environmentScript and not environmentScript['OK']:
      self.log.error("Environment : 'dirac.cfg' file look up failed")
      return False

    self.environmentScript = environmentScript["Value"]

    return os.path.exists(self.environmentScript)

  def writeToFile(self, operation, fileName, fileText):
    """This function creates a new file and
    writes the given content into this file.

    :param operation: The operation('w' or 'a') of the writting operation
    :type operation: str

    :param fileName: The name of the file to create
    :type fileName: str

    :param fileText: The content of the file
    :type fileText: str

    :return: success or failure of the write operation
    :rtype: bool

    """

    try:
      # Create file with 'operation' permission
      with open(fileName, operation) as textFile:
        textFile.write(fileText)
    except IOError as  e:
      errorMessage = "Application : File write operation failed\n%s" % e
      self.log.error(errorMessage)
      return False

    debugMessage = "Application : File write operation successfull"
    self.log.debug(debugMessage)
    return True
    
  @staticmethod  
  def readFromFile(fileName):
    """This function reads a file and returns its content.

    :param fileName: The path of the file to read
    :type fileName: str

    :return: The content of the file
    :rtype: str, str

    """

    try:
      with open(fileName, 'r') as fileToRead:
        content = fileToRead.read()
    except IOError as e:
      errorMessage = 'Application : Card file reading failed\n%s' % e
      return None, errorMessage

    debugMessage = 'Application : Card file reading successfull'
    return content, debugMessage
   