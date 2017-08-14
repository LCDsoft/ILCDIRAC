"""FCC application is defined by 2 modules which are :

     - ILCDIRAC.Interfaces.API.NewInterface.Applications.Fcc
     - ILCDIRAC.Workflow.Modules.FccAnalysis (this module)

"""

# standard libraries
import os
import re
import stat
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
    self.fccConfFile = ''
    self.gaudiOptionsFile = ''
    self.fccAppIndex = ''
    self.STEP_NUMBER = ''
    self.RandomSeed = 0
    self.outputFile = ''
    self.read = False
    self.cardFile = ''
    self.environmentScript = ''

    self.platform = ''
    self.debug = True
    self.log = gLogger.getSubLogger("FccAnalysis")

    self.applicationScript = ''
    self.applicationFolder = ''

    # Gaudi log levels
    self.logLevels = ['DEBUG', 'INFO', 'ERROR', 'FATAL']

    # User log level chosen
    self.logLevel = None

  def runIt(self):
    """Main method called by the Agent.
       The Application's call must reside here.

       This module consists on creating a bash script calling
       an executable followed by arguments provided by the Fcc module.

    :return: The success or failure of the execution
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

    # Worflow parameters given on the fly by parametric job functions
    if 'InputData' in self.workflow_commons:
      self.InputData = self.workflow_commons['InputData']

      debugMessage = (
        "Parametric : Parameter 'InputData' given successfully"
        " with this value '%(InputData)s'" % {'InputData':self.InputData}
      )
      self.log.debug(debugMessage)

    if 'NumberOfEvents' in self.workflow_commons:
      self.NumberOfEvents = self.workflow_commons['NumberOfEvents']

      debugMessage = (
      "Parametric : Parameter 'NumberOfEvents' given successfully"
      " with this value '%(NumberOfEvents)s'" % {'NumberOfEvents':self.NumberOfEvents}
      )

      self.log.debug(debugMessage)

    if 'IS_PROD' in self.workflow_commons:
      self.RandomSeed = int(str(int(self.workflow_commons["PRODUCTION_ID"])) + str(int(self.workflow_commons["JOB_ID"])))
    elif self.jobID:
      self.RandomSeed = self.jobID

    debugMessage = (
    "Parametric : Parameter 'RandomSeed' given successfully"
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

    if 'InputFile' in self.step_commons:
      getInputFile = self.step_commons.get('InputFile', self.InputFile)
      if getInputFile:
        inputFile = "JobID_%s_%s" % (self.jobID, os.path.basename(getInputFile))
        self.InputFile = os.path.join(os.path.dirname(getInputFile), inputFile)
    
    if self.InputFile:
      self.fccConfFile = self.InputFile
      self.log.debug("Application : Configuration file taken from the input file '%s'" % self.InputFile)
    elif self.fccConfFile and not self.fccConfFile.startswith('/cvmfs/'):
      self.fccConfFile = os.path.realpath(os.path.basename(self.fccConfFile))

    if not os.path.exists(self.fccConfFile):
      errorMessage = (
        "Environment : FCC configuration file does not exist,"
        " can not run FCC application"
      )
      self.log.error(errorMessage)
      return S_ERROR(errorMessage)

    # Set the seed of the application in overwritting Pyhtia card file
    if self.cardFile:
      content, message = self.readFromFile(self.fccConfFile)

      if not content:
        self.log.error(message)
        return S_ERROR(message)

      self.fccConfFile = os.path.realpath(os.path.basename(self.fccConfFile))

      self.log.debug(message)

      if 'Main:numberOfEvents' in content and self.NumberOfEvents:
        eventSetting = "Main:numberOfEvents = %d" % self.NumberOfEvents
        contentWithEventSet = re.sub(r'Main:numberOfEvents *= *\d+', eventSetting, content)
      else:
        # add the card line  
        eventSetting = "Main:numberOfEvents = %d         ! number of events to generate" % self.NumberOfEvents
        contentWithEventSet = "%s\n%s\n" % (content, eventSetting)

      if self.RandomSeed:
        seedSetting = ["Random:setSeed = on         ! random flag"]
        seedSetting += ["Random:seed = %d         ! random mode" % self.RandomSeed]
      
        contentWithEventSet = "%s\n%s\n" % (contentWithEventSet, "\n".join(seedSetting))
    
      if not self.writeToFile('w', self.fccConfFile, contentWithEventSet):
        errorMessage = "Application : Card file overwitting failed"
        self.log.error(errorMessage)
        return S_ERROR(errorMessage)
            
    # Each Fcc application has its own folder to store results etc...
    self.fccAppIndex = "%s_%s_Step_%s" % (self.applicationName, self.applicationVersion, self.STEP_NUMBER)
    self.applicationFolder = os.path.realpath(self.fccAppIndex)

    debugMessage = "Application : Creation of the application folder '%s'..." % self.applicationFolder
    self.log.debug(debugMessage)

    if os.path.exists(self.applicationFolder):
      errorMessage = "Application : Application folder '%s' already exists !" % self.applicationFolder
      self.log.error(errorMessage)
      return S_ERROR(errorMessage)

    try:
      os.makedirs(self.applicationFolder)
    except OSError:
      errorMessage = "Application : Creation of the application folder '%s' failed" % self.applicationFolder
      self.log.error(errorMessage)
      return S_ERROR(errorMessage)

    debugMessage = "Application : Creation of the application folder '%s' successfull" % self.applicationFolder
    self.log.debug(debugMessage)

    self.applicationScript = os.path.join(self.applicationFolder, "%s.sh" % self.fccAppIndex)

    debugMessage = (
      "Application code : Creation of the bash script"
      " to call the application with FCC module parameters..."
    )
    self.log.debug(debugMessage)

    # FCC PHYSICS does not need this file so do not resolve it if it is not given
    # else 'realpath of "" ' will result in cwd.
    if self.gaudiOptionsFile:
      if not self.generateGaudiConfFile():
        errorMessage = "ApplicationgGaudi options : generateGaudiConfFile() failed"
        self.log.error(errorMessage)
        return S_ERROR(errorMessage)

    # Main command
    bashCommands = ['%s %s %s' %
                    (self.fccExecutable, self.fccConfFile, self.gaudiOptionsFile)]


    if not self.generateBashScript(bashCommands):
      errorMessage = "Application code : Creation of the bash script failed"
      self.log.error(errorMessage)
      return S_ERROR(errorMessage)

    self.log.debug("Application code : Creation of the bash script successfull")

    self.log.debug("Application : Application execution and log file creation...")
    # Call of the application
    # Redirect log file to application folder
    self.applicationLog = os.path.join( self.applicationFolder, self.applicationLog)
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
      self.log.warn("Application : no root file has been generated, is that normal ?")
    else:
      # Rename the last root file created
      rootFilesSorted = [(os.path.getctime(f), f) for f in rootFiles]
      rootFilesSorted.sort()
      lastCreatedRootFileTimeName = rootFilesSorted[-1]
      lastCreatedRootFileName = lastCreatedRootFileTimeName[1]
      old = os.path.realpath(lastCreatedRootFileName)

      outputFile = "JobID_%s_%s" % (self.jobID, os.path.basename(self.outputFile))

      if 'UserOutputData' in self.workflow_commons:
        outputData = self.workflow_commons['UserOutputData'].split(";")

        for idx, data in enumerate(outputData):
          if data.endswith(".root"):
            lfnTree = os.path.dirname(data)
            outputData[idx] = os.path.join(lfnTree, outputFile)

        self.workflow_commons['UserOutputData'] = ";".join(outputData)

      debugMessage = "Application : Root file '%s' renaming..." % old
      self.log.debug(debugMessage)

      renamedRootFile = os.path.realpath(outputFile)

      # Rename root file to make it unique
      try:
        shutil.move(old, renamedRootFile)
      except IOError, shutil.Error:
        errorMessage = "Application : Root file '%s' renaming failed" % old
        self.log.error(errorMessage)
        return S_ERROR(errorMessage)

      debugMessage = "Application : Root file '%s' renamed successfully to '%s'" % (old, renamedRootFile)
      self.log.debug(debugMessage)

      debugMessage = "Application : Root file '%s' copy..." % renamedRootFile
      self.log.debug(debugMessage)

      copiedRootFile = os.path.join(os.path.dirname(self.outputFile), outputFile)

      # Copy root file to the Application folder
      try:
        shutil.copy(renamedRootFile, copiedRootFile)
      except IOError, shutil.Error:
        errorMessage = "Application : Root file '%s' copy failed" % renamedRootFile
        self.log.error(errorMessage)
        return S_ERROR(errorMessage)

      debugMessage = "Application : Root file '%s' copied successfully to '%s'" % (renamedRootFile, copiedRootFile)
      self.log.debug(debugMessage)

    return S_OK("Execution of the FCC application successfull")

###############################  FccAnalysis FUNCTIONS #############################################
  def chmod(self, file, permission):
    """This function sets the permission of a file.
    We want to make the bash script executable.

    :param file: The file to set the permission
    :type file: str

    :param permisssion: The permission ('W', 'R' or 'X')
    :type permission: str

    :return: success or failure of setting the permission
    :rtype: bool

    """

    # Reflet chmod a+permission
    # Make the file x,r, or w for everyone
    userPermission = eval('stat.S_I%sUSR' % permission)
    groupPermission = eval('stat.S_I%sGRP' % permission)
    otherPermission = eval('stat.S_I%sOTH' % permission)

    permission = userPermission | groupPermission | otherPermission

    try:
      # Get actual mode of the file
      mode = os.stat(file).st_mode
      # Merge the new permission with the existing one
      os.chmod(file, mode | permission)
    except OSError:
      return False

    return True

  def generateBashScript(self, commands):
    """This function generates a bash script containing the environment setup
    and the command related to the FCC application.

    :param commands: The commands for calling the application
    :type commands: list

    :return: success or failure of the bash script creation
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

    # Make the script executable and readable for all
    if not (self.chmod(self.applicationScript, 'R') and self.chmod(self.applicationScript, 'X')):
      self.log.error("Application file : Bash script rights setting failed")
      return False

    self.log.debug("Application file : Bash script rights setting successfull")

    return True

  def generateGaudiConfFile(self):
    """Generation of the Gaudi configuration file
    with the setting of :

      -  The number of event
      -  The gaudi log level
      -  The input file for FCCDataSvc

    There is 2 ways for configuring gaudirun.py :
    1) By using gaudirun.py options :
    e.g. ./gaudirun.py --option "ApplicationMgr().EvtMax=10"
    
    2) By creating a new python script containing the gaudi configuration :
    This script has to be given as a second argument to gaudirun.py
    e.g. ./gaudirun.py geant_pgun_fullsim.py gaudi_options.py
    It then contains the event setting etc...
    We decided to choose the second one.

    :return: success or failure of the gaudi option file creation
    :rtype: bool

    """

    gaudiOptions = ["from Configurables import ApplicationMgr"]
    gaudiOptions += ["from Gaudi.Configuration import *"]

    # In putting -1, gaudi read all event of the file given to FCCDataSvc
    eventSetting = "ApplicationMgr().EvtMax=%s" % self.NumberOfEvents
    gaudiOptions += [eventSetting]

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

    fccswPodioOptions = ["from Gaudi.Configuration import *"]
    fccswPodioOptions += ["from Configurables import ApplicationMgr, FCCDataSvc, PodioOutput"]
    fccswPodioOptions += ["import os"]

    # If it is an application that read events and there are input data
    if self.read and self.InputData:
      fccInputDataSubstitution = [ '%s' for data in self.InputData]
      fccInputData = ["os.path.realpath(os.path.basename('%s'))" % data
                  for data in self._fccInputData]
      # We can provide many input files to FCCDataSvc() like this :
      inputSetting = "FCCDataSvc().input='%s' %% (%s)" % (" ".join(fccInputDataSubstitution), ", ".join(fccInputData))
      fccswPodioOptions += [inputSetting]
      gaudiOptions += fccswPodioOptions

    self.gaudiOptionsFile = os.path.join(self.applicationFolder,
                         '%s_gaudiOptions.py' % self.fccAppIndex)

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
    except IOError:
      errorMessage = "Application : File write operation failed"
      self.log.error(errorMessage)
      return False

    debugMessage = "Application : File write operation successfull"
    self.log.debug(debugMessage)
    return True
    
  def readFromFile(self, fileName):
    """This function reads a file and returns its content.

    :param fileName: The path of the file to read
    :type fileName: str

    :return: The content of the file
    :rtype: str, str

    """

    try:
      with open(fileName, 'r') as file:
        content = file.read()
    except IOError:
      errorMessage = 'Application : Card file reading failed'
      return None, errorMessage

    debugMessage = 'Application : Card file reading successfull'
    return content, debugMessage
   