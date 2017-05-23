"""FCC application is defined by 2 modules which are :

     - ILCDIRAC.Interfaces.API.NewInterface.Applications.Fcc
     - ILCDIRAC.Workflow.Modules.FccAnalysis (this module)

   This module is called by 'DIRAC' that know it via the
   attribute '_modulename' of Fcc module.

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

    self.fcc_executable = ''
    self.fcc_conf_file = ''
    self.gaudi_options_file = ''
    self.fcc_app_index = ''
    self.split = ''

    self.environment_script = ''

    self.software = ''
    self.version = ''
    self.platform = ''
    self.debug = True
    self.log = gLogger.getSubLogger("FccAnalysis")

    self.application_script = os.path.join(os.getcwd(), 'user_temp_job.sh')

  def execute(self):
    """Main method called by the Agent.
       The Application's call must reside here.

       In fact, an FCC application consists on executing a bash script containing
       an executable following by arguments provided by the Fcc module via module parameters.

    :return: The success or failure of the execution
    :rtype: DIRAC.S_OK, DIRAC.S_ERROR

    """

    # Worflow parameters given on the fly by parametric job functions
    if 'split' in self.workflow_commons:
      debug_message = "Environment : Parameter 'split' given successfully"
      debug_message += " with this value '%s'" % self.workflow_commons['split']
      self.log.debug(debug_message)

    self.log.info("Environment : Environment script look up...")

    # Try to locate environment script in 'dirac.cfg' file
    if not self._get_environment_script():
      error_message = "Environment : Environment script look up failed"
      error_message += "\nFailed to get environment"
      self.log.error(error_message)
      return S_ERROR(error_message)

    debug_message = "Environment : Environment script found at : %s" % (self.environment_script)
    self.log.debug(debug_message)

    self.log.info("Environment : Environment script look up successfull")

    if not self.fcc_conf_file.startswith('/cvmfs/'):
      self.fcc_conf_file = os.path.abspath(os.path.basename(self.fcc_conf_file))

      if not os.path.exists(self.fcc_conf_file):
        error_message = "Environment : FCC configuration file does not exist,"
        error_message += " can not run FCC application"
        self.log.error(error_message)
        return S_ERROR(error_message)

    # FCC PHYSICS does not need this file so do not resolve it if it is not given
    # else 'abspath' will results in cwd.
    if self.gaudi_options_file:
      self.gaudi_options_file = os.path.abspath(os.path.basename(self.gaudi_options_file))


      if not os.path.exists(self.gaudi_options_file):
        error_message = "Environment : Gaudi option file does not exist,"
        error_message += " can not run FCC application"
        self.log.error(error_message)
        return S_ERROR(error_message)

    debug_message = "Application code : Creation of the bash script"
    debug_message += " to call the application with FCC module parameters..."
    self.log.debug(debug_message)

    # Main command
    bash_commands = ['%s %s %s' %
             (self.fcc_executable, self.fcc_conf_file, self.gaudi_options_file)]


    if not self._generate_bash_script(bash_commands):
      error_message = "Application code : Creation of the bash script failed"
      self.log.error(error_message)
      return S_ERROR(error_message)

    self.log.debug("Application code : Creation of the bash script successfull")

    # Call of the application
    call = shellCall(0, self.application_script)


    if 'OK' in call and not call['OK']:
      error_message = "Application code : Execution of application script failed"
      self.log.error(error_message)
      return S_ERROR(error_message)

    info_message = ["Application code : Execution of application script successfull"]
    info_message += ["standard output is written to '%s.out'" % self.fcc_app_index]
    info_message += ["standard error is written to '%s.err'" % self.fcc_app_index]

    self.log.info('\n'.join(info_message))

    self.log.debug("Application : Standard output creation...")

    # If error in writting standard output/error, let the application run successfully

    if not self._write2file('w', os.path.join(os.getcwd(),'%s.out' % self.fcc_app_index), str(call['Value'][1])):
      self.log.error("Application : Standard output creation failed")
    else:
      self.log.debug("Application : Standard output creation successfull")


    self.log.debug("Application : Standard error creation...")

    if not self._write2file('w', os.path.join(os.getcwd(),'%s.err' % self.fcc_app_index), str(call['Value'][2])):
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
    user_permission = eval('stat.S_I%sUSR' % permission)
    group_permission = eval('stat.S_I%sGRP' % permission)
    other_permission = eval('stat.S_I%sOTH' % permission)

    permission = user_permission | group_permission | other_permission

    # Get actual mode of the file
    mode = os.stat(file).st_mode

    # Merge the new permission with the existing one
    os.chmod(file, mode | permission)

  def _generate_bash_script(self, commands):
    """This function generates a bash script containing the environment setup
    and the command related to the FCC application.

    :param commands: The commands to call the application
    :type commands: list

    :return: success or failure of the bash script creation
    :rtype: bool

    """

    # Set environnement and execute the application
    shebang = "#!/bin/bash"

    set_environment_script = 'source %s' % self.environment_script
    bash_script_text = [shebang, set_environment_script] + commands

    self.log.debug("Application command : %s" % '\n'.join(commands))

    # Write the temporary job
    self.log.debug("Application code : Bash script creation...")

    if not self._write2file('w', self.application_script, '\n'.join(bash_script_text) + '\n'):
      self.log.error("Application code : Bash script creation failed")
      return False

    self.log.debug("Application code : Bash script creation successfull")

    # Make the script executable and readable for all
    self._chmod(self.application_script, 'R')
    self._chmod(self.application_script, 'X')

    return True

  def _generate_script_on_the_fly(self, sysconfig="", appname="", appversion=""):
    """Normally, this function generates dynamically the
    FCC environment script but nothing for the moment.

    Called if CVMFS is not available
    (CVMFS should be always available else FCC software can't run).

    :param sysconfig: The platform required by the software
    :type sysconfig: str

    :param appname: The name of the software
    :type appname: str

    :param appversion: The version of the software
    :type appversion: str

    """

    # We do not generate the environment script like in MarlinAnalysis etc...
    # Because if we do not have access to cvmfs, we can do nothing.

    #print('%s %s %s' % (sysconfig, appname, appversion))
    error_message = 'Environment : Environment script not found'
    error_message += ' for this configuration %s %s %s' % (sysconfig, appname, appversion)
    error_message += ' can not generate one dynamically'
    return S_ERROR(error_message)

  def _get_environment_script(self):
    """This function gets environment script path from 'dirac.cfg' file
    according to the version, software and platform.

    """

    environment_script = getEnvironmentScript(self.platform, self.software,
                          self.version, self._generate_script_on_the_fly)

    if 'OK' in environment_script and not environment_script['OK']:
      return False

    self.environment_script = environment_script["Value"]

    return os.path.exists(self.environment_script)

  def _write2file(self, operation, file_name, filetext):
    """This function creates a new file and
    writes the given content into this file.

    :param operation: The operation('w' or 'a') of the writting operation
    :type operation: str

    :param file_name: The name of the file to create
    :type file_name: str

    :param filetext: The content of the file
    :type filetext: str

    :return: success or failure of the write operation
    :rtype: bool

    """

    try:
      # Create file with 'operation' permission
      with open(file_name, operation) as text_file:
        text_file.write(filetext)
    except IOError:
      error_message = "Application : File write operation failed"
      self.log.error(error_message)
      return False

    debug_message = "Application : File write operation successfull"
    self.log.debug(debug_message)
    return True
    