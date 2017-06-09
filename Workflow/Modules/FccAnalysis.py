"""FCC application is defined by 2 modules which are :

     - ILCDIRAC.Interfaces.API.NewInterface.Applications.Fcc
     - ILCDIRAC.Workflow.Modules.FccAnalysis (this module)

   This module is called by 'DIRAC' that know it via the
   attribute '_modulename' of the Fcc module.

"""

# standard libraries
import os
import stat

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
    self.split = ''

    self.environmentScript = ''

    self.software = ''
    self.version = ''
    self.platform = ''
    self.debug = True
    self.log = gLogger.getSubLogger("FccAnalysis")

    self.applicationScript = os.path.join(os.getcwd(), 'user_temp_job.sh')

  def execute(self):
    """Main method called by the Agent.
       The Application's call must reside here.

       In fact, an FCC application consists on calling a bash script executing
       an executable followed by arguments provided by the Fcc module via module parameters.

    :return: The success or failure of the execution
    :rtype: DIRAC.S_OK, DIRAC.S_ERROR

    """

    # Worflow parameters given on the fly by parametric job functions
    if 'split' in self.workflow_commons:
      debugMessage = (
        "Environment : Parameter 'split' given successfully"
        " with this value '%(split)s'" % {'split':self.workflow_commons['split']}
      )
      self.log.debug(debugMessage)

    self.log.info("Environment : Environment script look up...")

    # Try to locate environment script in 'dirac.cfg' file
    if not self._getEnvironmentScript():
      errorMessage = (
        "Environment : Environment script look up failed\n"
        "Failed to get environment"
      )
      self.log.error(errorMessage)
      return S_ERROR(errorMessage)

    debugMessage = "Environment : Environment script found at : %s" % self.environmentScript
    self.log.debug(debugMessage)

    self.log.info("Environment : Environment script look up successfull")

    if not self.fccConfFile.startswith('/cvmfs/'):
      self.fccConfFile = os.path.abspath(os.path.basename(self.fccConfFile))

    if not os.path.exists(self.fccConfFile):
      errorMessage = (
        "Environment : FCC configuration file does not exist,"
        " can not run FCC application"
      )
      self.log.error(errorMessage)
      return S_ERROR(errorMessage)

    # FCC PHYSICS does not need this file so do not resolve it if it is not given
    # else 'abspath' will results in cwd.
    if self.gaudiOptionsFile:
      self.gaudiOptionsFile = os.path.abspath(os.path.basename(self.gaudiOptionsFile))


      if not os.path.exists(self.gaudiOptionsFile):
        errorMessage = (
          "Environment : Gaudi option file does not exist,"
          " can not run FCC application"
        )
        self.log.error(errorMessage)
        return S_ERROR(errorMessage)

    debugMessage = (
      "Application code : Creation of the bash script"
      " to call the application with FCC module parameters..."
    )
    self.log.debug(debugMessage)

    # Main command
    bashCommands = ['%s %s %s' %
             (self.fccExecutable, self.fccConfFile, self.gaudiOptionsFile)]


    if not self._generateBashScript(bashCommands):
      errorMessage = "Application code : Creation of the bash script failed"
      self.log.error(errorMessage)
      return S_ERROR(errorMessage)

    self.log.debug("Application code : Creation of the bash script successfull")

    # Call of the application
    call = shellCall(0, self.applicationScript)


    if 'OK' in call and not call['OK']:
      errorMessage = "Application code : Execution of application's script failed"
      self.log.error(errorMessage)
      return S_ERROR(errorMessage)

    infoMessage = (
      "Application code : Execution of application's script successfull\n"
      "standard output is written to '%(idx)s.out'\n"
      "standard error is written to '%(idx)s.err'" % {'idx':self.fccAppIndex}
    )
    self.log.info(infoMessage)

    self.log.debug("Application : Standard output creation...")

    # If error in writting standard output/error, let the application run successfully

    if not self._writeToFile('w', os.path.join(os.getcwd(),'%s.out' % self.fccAppIndex), str(call['Value'][1])):
      self.log.error("Application : Standard output creation failed")
    else:
      self.log.debug("Application : Standard output creation successfull")


    self.log.debug("Application : Standard error creation...")

    if not self._writeToFile('w', os.path.join(os.getcwd(),'%s.err' % self.fccAppIndex), str(call['Value'][2])):
      self.log.error("Application : Standard error creation failed")
    else:
      self.log.debug("Application : Standard error creation successfull")


    return S_OK("Execution of the FCC application successfull")

###############################  FccAnalysis FUNCTIONS #############################################

  def _chmod(self, file, permission):
    """This function sets the permission of a file.
    We want to make the bash script executable.

    :param file: The file to set the permission
    :type file: str

    :param permisssion: The permission ('W', 'R' or 'X')
    :type permission: str

    """

    # Reflet chmod a+permission
    # Make the file x,r, or w for everyone
    userPermission = eval('stat.S_I%sUSR' % permission)
    groupPermission = eval('stat.S_I%sGRP' % permission)
    otherPermission = eval('stat.S_I%sOTH' % permission)

    permission = userPermission | groupPermission | otherPermission

    # Get actual mode of the file
    mode = os.stat(file).st_mode

    # Merge the new permission with the existing one
    os.chmod(file, mode | permission)

  def _generateBashScript(self, commands):
    """This function generates a bash script containing the environment setup
    and the command related to the FCC application.

    :param commands: The commands to call the application
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

    if not self._writeToFile('w', self.applicationScript, '\n'.join(bashScriptText) + '\n'):
      self.log.error("Application code : Bash script creation failed")
      return False

    self.log.debug("Application code : Bash script creation successfull")

    # Make the script executable and readable for all
    self._chmod(self.applicationScript, 'R')
    self._chmod(self.applicationScript, 'X')

    return True

  def _generateScriptOnTheFly(self, sysConfig="", appName="", appVersion=""):
    """Normally, this function generates dynamically the
    FCC environment script but nothing for the moment.

    Called if CVMFS is not available
    (CVMFS should be always available else FCC software can't run).

    :param sysConfig: The platform required by the software
    :type sysConfig: str

    :param appName: The name of the software
    :type appName: str

    :param appVersion: The version of the software
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
    return S_ERROR(errorMessage)

  def _getEnvironmentScript(self):
    """This function gets environment script path from 'dirac.cfg' file
    according to the version, software and platform.

    """

    environmentScript = getEnvironmentScript(self.platform, self.software,
                          self.version, self._generateScriptOnTheFly)

    if 'OK' in environmentScript and not environmentScript['OK']:
      return False

    self.environmentScript = environmentScript["Value"]

    return os.path.exists(self.environmentScript)

  def _writeToFile(self, operation, fileName, fileText):
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
    