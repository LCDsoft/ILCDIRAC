"""FCC application is defined by 2 modules which are :

     - ILCDIRAC.Interfaces.API.NewInterface.Applications.Fcc (this module)
     - ILCDIRAC.Workflow.Modules.FccAnalysis

   FCC applications usually run under a FccJob located here :

     - ILCDIRAC.Interfaces.API.NewInterface.FccJob

"""

# standard libraries
import os
import re
import shutil
import types

# DIRAC libraries
from ILCDIRAC.Interfaces.API.NewInterface.LCApplication import LCApplication
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Workflow.Parameter import Parameter

__RCSID__ = "$Id$"


class Fcc(LCApplication):
  """Fcc class defines the skeleton of an FCC application
  It inherits from the inheritance chain :
    -  LCApplication -> LCUtilityApplication -> Application

  There are 2 'types' of FCC applications (look at the bottom of this module) :
  -   FccSw
  -   FccAnalysis

  """

  def __init__(self, fcc_output_file,
         number_of_events, extraInputs, extraOutputs):
    """FCC application generic attributes :

    :param fcc_output_file: The output file of the application
    :type fcc_output_file: str

    :param number_of_events: The number of events
    :type number_of_events: int

    :param extraInputs: The local input files required by the application
    :type extraInputs: tuple

    :param extraOutputs: The local output files
    :type extraOutputs: tuple

    """

    # Now, more attributes specific for each application
    # (FCCSW or FCCPHYSICS)

    # Required
    self.fcc_executable = ''
    self.fcc_conf_file = ''

    self.gaudi_options_file = ''

    self._fcc_output_file = fcc_output_file
    self._fcc_input_data = []
    self.fcc_app_name = self.__class__.__name__
    self.fcc_app_index = ''

    self.NumberOfEvents = number_of_events

    self._extra_inputs = set([extraInputs]) if extraInputs else set()
    self._extra_outputs = set([extraOutputs]) if extraOutputs else set()

    # Path of FCCSW installation
    self.fccsw_path = ''

    # Final input sandbox
    self._input_sandbox = set()

    # Temporary input sandbox
    # contains user files/folders not yet checked
    self._temp_input_sandbox = set()

    # Temporary input sandbox
    # contains user files/folders not yet checked
    # and that need to be filtered (like 'Detector' folder of FCCSW installation)
    self._folders_to_filter = set()

    # Folder filters
    # which extension to filter
    self._filtered_extensions = []

    # What operation to do for each extension (include it or exclude it)
    self._excludes_or_includes = []

    # Local path for the temporary sandbox
    self._temp_cwd = os.path.join(os.getcwd(), 'fcc_temp_dirac')

    # Temporary output sandbox
    self._output_sandbox = set()

    # Gaudi log levels
    self._LOG_LEVELS = ['DEBUG', 'INFO', 'ERROR', 'FATAL']

    # User log level chosen
    self.log_level = None

    self.datatype = 'REC'
    self.detectortype = 'ILD'


    super(Fcc, self).__init__()
    # Those 5 need to come after default constructor
    self._modulename = 'FccAnalysis'
    #self._importLocation = 'ILCDIRAC.Workflow.Modules'
    self._moduledescription = "Module running FCC software"
    self.software = "fccsw"
    self.version = "v1.0"
    self.platform = "x86_64-slc6-gcc49-opt"

  def _applicationModule(self):
    """It transfers parameter names of the module
       FCC to the module FCCAnalysis.

    """

    md1 = self._createModuleDefinition()

    md1.addParameter(Parameter("fcc_executable", "", "string", "", "", False, False,
                   "The executable to run"))

    md1.addParameter(Parameter("fcc_conf_file", "", "string", "", "", False, False,
                   "FCC configuration file"))

    md1.addParameter(Parameter("fcc_app_index", "", "string", "", "", False, False,
                   "FCC application index"))

    md1.addParameter(Parameter("software", "", "string", "", "", False, False,
                   "The software to select"))

    md1.addParameter(Parameter("version", "", "string", "", "", False, False,
                   "The version of the software"))

    md1.addParameter(Parameter("platform", "", "string", "", "", False, False,
                   "The platform required by the software"))

    return md1

  def _applicationModuleValues(self, moduleinstance):
    """It transfers parameter values of the module
    FCC to the module FCCAnalysis.

    :param moduleinstance: The module we load (FCCAnalysis)

    """

    moduleinstance.setValue("fcc_executable", self.fcc_executable)
    moduleinstance.setValue("fcc_conf_file", self.fcc_conf_file)
    moduleinstance.setValue("fcc_app_index", self.fcc_app_index)
    moduleinstance.setValue("software", self.software)
    moduleinstance.setValue("version", self.version)
    moduleinstance.setValue("platform", self.platform)

  def _checkConsistency(self, job=None):
    """This function checks the minimum requirements of the application
    and updates the sandbox with files/folders required by the application.

    :param job: The job containing the application
    :type job: DIRAC.Interfaces.API.Job.Job

    :return: The success or failure of the consistency
    :rtype: DIRAC.S_OK, DIRAC.S_ERROR

    """

    info_message = "Application general consistency :"
    info_message += " _checkConsistency() on '%s'..." % self.fcc_app_name
    self._log.info(info_message)

    if not (self.version  and  self.platform):
      error_message = 'Consistency : Version, name and platform have to be set !'
      self._log.error(error_message)
      return S_ERROR(error_message)

    info_message = "Consistency : Version and platform of the application set to :"
    info_message += "\nversion : %s" % self.version
    info_message += "\nplatform : %s" % self.platform

    self._log.info(info_message)

    if not (self.fcc_executable and self.fcc_conf_file):
      error_message = "Consistency : Error in parsing '%s' application :" %  self.fcc_app_name
      error_message += "\nYou have to provide at least an executable"
      error_message += "and a configuration file for each application\n"
      self._log.error(error_message)
      return S_ERROR(error_message)

    debug_message = "Consistency : Executable and configuration of the application set to :"
    debug_message += "\nexecutable : %s" % self.fcc_executable
    debug_message += "\nconfiguration : %s" % self.fcc_conf_file

    self._log.debug(debug_message)

    # We add the log to the ouput sandbox
    # (I think it is already done by DIRAC)
    #self._output_sandbox.add(fcc_app_log)

    # All input files are put in the temporary sandbox for a
    # pre-checking before being added to the final sandbox
    self._temp_input_sandbox.add(self.fcc_conf_file)
    self._temp_input_sandbox = self._temp_input_sandbox.union(self._extra_inputs)

    # We add extra (in/out)puts
    self._output_sandbox = self._output_sandbox.union(self._extra_outputs)

    # These files are already added by default by DIRAC to the sandbox
    #if 'wms' == mode:
      #self._output_sandbox.add('std.out')
      #self._output_sandbox.add('localEnv.log')

    info_message = "Sandboxing : Sandboxing in progress..."
    self._log.info(info_message)

    # We update the sandbox with files/folders required by the application
    if not self._import_to_sandbox():
      error_message = "_import_to_sandbox() failed"
      self._log.error(error_message)
      return S_ERROR(error_message)

    info_message = "Sandboxing : Sandboxing successfull"
    self._log.info(info_message)

    """
    setOutputFile() method informs the job that this application has an output file
    This output can be used as input for another application.
    In this way, app2.getInputFromApp(app1) method knows the ouput file of the given application
    app1 thanks to its method setOutputFile().
    """

    if self._fcc_output_file:
      self.setOutputFile(self._fcc_output_file)

    #self.setLogFile(fcc_app_log)

    # Before submitting the job, we filter some folders required by FCCSW application
    # and we import the filtered folders to a temporary sandbox.
    if not self._set_filter_to_folders():
      error_message = "_set_filter_to_folders() failed"
      self._log.error(error_message)
      return S_ERROR(error_message)

    fcc_app_log = '%s.log' % self.fcc_app_index

    # self.inputSB is an attribute of the DIRAC Application and not of FCC.
    # The description file of the job (JDL file) contains a section for the input sandbox
    # This section is filled with a list of files (self.inputSB).
    # After user input files, application files, and application additionnal
    # files checked in the temporary sandbox, we 'merge' our 'final input sandbox'
    # to the DIRAC application input sandbox : self.inputSB
    self._input_sandbox = self._input_sandbox.union(self._folders_to_filter)
    self.inputSB = list(self._input_sandbox)

    """
    Sandbox can be set at the application level or at the job level.
    Whatever the level choosed, sandbox files are all put
    in the same final destination which is a list of paths
    in the JDL file (see Input Sandbox parameter of the JDL file).

    """

    info_message = '\n********************************FCC SUMMARY******************************'

    info_message += "\nYou plan to submit this application with its corresponding log :"
    info_message += '\n ' + self.fcc_app_name + ' --> ' + fcc_app_log

    self._log.info(info_message)


    if self._input_sandbox:
      info_message = '\nHere is the content of its input sandbox :\n'
      info_message += '\n'.join(self._input_sandbox)
      self._log.info(info_message)

    if self._output_sandbox:
      info_message = '\nHere is the content of its output sandbox :\n'
      info_message += '\n'.join(self._output_sandbox)
      self._log.info(info_message)


    info_message = '\n********************************FCC SUMMARY******************************'
    self._log.info(info_message)


    info_message = "Application general consistency : _checkConsistency()"
    info_message += " on '%s' successfull" % self.fcc_app_name
    self._log.info(info_message)

    # Flush application sandboxes

    #self._flush_sandboxes()

    return S_OK(info_message)

  def _checkWorkflowConsistency(self):
    """Summary of the application done after
    application _checkConsistency() method.
    """
    return self._checkRequiredApp()

  def _prodjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setOutputComputeDataList(stepdefinition)
    if not res1["OK"] or not res2["OK"]:
      return S_ERROR('prodjobmodules failed')
    return S_OK()

  def _resolveLinkedStepParameters(self, stepinstance):
    if type(self._linkedidx) == types.IntType:
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
    return S_OK()

  def _userjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setUserJobFinalization(stepdefinition)
    if not res1["OK"] or not res2["OK"]:
      return S_ERROR('userjobmodules failed')
    return S_OK()

###############################  Fcc FUNCTIONS #####################################################

  def _flush_sandboxes(self):
    """ Clear all sandboxes.
     Usefull when the same application is appended many times.
     But this possibility has been removed.
     """

    # FCC Application attributes
    self._temp_input_sandbox.clear()
    self._input_sandbox.clear()

  def _filter_folders(self, temp_folder, actual_folder,
            filtered_extension, exclude_or_include):
    """Knowing the filter for each folder, we copy the 'filtered'
    folder in the temporary folder 'fcc_temp_dirac'.

    After, we add the path of the 'filtered' folder to the sandbox instead of
    its original path.

    Like that, we do not import unnecessary files to the sandbox.

    :param temp_folder: The temporary working directory (destination) used for the sandboxing
    :type temp_folder: str

    :param actual_folder: The original (source) path of folder looked by the copy process
    :type actual_folder: str

    :param filtered_extension: The extension of file we do (not) want
    :type filtered_extension: str

    :param exclude_or_include: extension is excluded or included
    :type exclude_or_include: bool

    :return: success or failure of filtering
    :rtype: bool

    """

    # If folders have to be filtered then, we do not have to add them directly to the sandbox
    # because it will put all content of folders.
    # So we copy only the content we want in the filtered folder
    # inside the temporary folder 'fcc_temp_dirac'.
    # After, we add the filtered folders located in 'fcc_temp_dirac' to the sandbox.

    debug_message = "Sandboxing : Checking of the filtered folder '%s'..." % temp_folder
    self._log.debug(debug_message)

    if not os.path.exists(temp_folder):
      debug_message = "Sandboxing : Creation of the filtered folder '%s'..." % temp_folder
      self._log.debug(debug_message)

      try:
        os.makedirs(temp_folder)
      except OSError:
        error_message = "Sandboxing : Creation of the filtered folder"
        error_message += " '%s' failed" % temp_folder
        self._log.error(error_message)
        return False

      debug_message = "Sandboxing : Creation of the filtered folder"
      debug_message += " '%s' successfull" % temp_folder
      self._log.debug(debug_message)

    for path in os.listdir(actual_folder):
      source = os.path.realpath(os.path.join(actual_folder, path))
      destination = os.path.realpath(os.path.join(temp_folder, path))

      if not os.path.isfile(source):
        # Recursive call for folders
        if not self._filter_folders(destination, source, filtered_extension, exclude_or_include):
          error_message = "Sandboxing : _filter_folders() failed"
          self._log.error(error_message)
          return False
      else:
        # If file then copy it
        if not os.path.exists(source):
          error_message = "Sandboxing : The file '%s' does not exist" % source
          self._log.error(error_message)
          return False

        if os.path.exists(destination):
          debug_message = "Sandboxing : The file '%s' already exists" % source
          self._log.debug(debug_message)
          return True

        debug_message = "Sandboxing : File '%s' copy..." % source
        self._log.debug(debug_message)

        if ((exclude_or_include and not path.endswith(filtered_extension))
            or (not exclude_or_include and path.endswith(filtered_extension))
            or not filtered_extension):

          try:
            shutil.copyfile(source, destination)
          except IOError, shutil.Error:
            error_message = "Sandboxing : The copy of the file '%s' failed"% destination
            self._log.error(error_message)
            return False

          debug_message = "Sandboxing : Copy of the file"
          debug_message += " '%s' successfull to '%s'" % (source, destination)
          self._log.debug(debug_message)

    debug_message = "Sandboxing : Folder '%s' filtering successfull" % temp_folder
    self._log.debug(debug_message)
    return True

  def _find_path(self, path):
    """This function checks if file/folder exists.

    :param path: The path to look for
    :type path: str

    :return: The full path and its existence
    :rtype: str, bool

    """

    path = os.path.abspath(path)
    return (path, True) if os.path.exists(path) else (path, False)

  def _import_fccsw_files(self):
    """FCCSW application needs additional files specified in the configuration file
    It also needs folders like 'InstallArea' and 'Detector'.

    :return: The success or the failure of the importation
    :rtype: bool

    """

    debug_message = "Sandboxing : Creation of a temporary folder 'fcc_temp_dirac'"
    debug_message += " in the current working directory for the sandboxing..."
    self._log.debug(debug_message)

    # First, it creates a temporary local directory for folders whose
    # the content does not have to be sandboxed entirely
    # like filtered folders
    if not os.path.exists(self._temp_cwd):
      try:
        os.makedirs(self._temp_cwd)
      except OSError:
        error_message = "Sandboxing : Creation of 'fcc_temp_dirac' folder failed"
        self._log.error(error_message)
        return False

      debug_message = "Sandboxing : Creation of 'fcc_temp_dirac' folder successfull"
      self._log.debug(debug_message)
    else:
      debug_message = "Sandboxing : The temporary folder 'fcc_temp_dirac' already exists"
      self._log.debug(debug_message)

    #install_area_folder = os.path.join(self.fccsw_path, 'InstallArea')
    detector_folder = os.path.join(self.fccsw_path, 'Detector')
    #fccsw_folders = [install_area_folder, detector_folder]

    # We do not need all the content of these folders hence the filtering
    self._folders_to_filter.add(detector_folder)

    # Explanation
    # InstallArea_folder : all dbg files are excluded
    # Detector_folder : only xml files are included
    self._filtered_extensions += ['.dbg', '.xml']
    self._excludes_or_includes += [True, False]

    debug_message = "Sandboxing : FCC configuration file reading..."
    self._log.debug(debug_message)

    content, message = self._read_from_file(self.fcc_conf_file)

    # If configuration file is not valid then consistency fails
    if not content:
      self._log.error(message)
      return False

    self._log.debug(message)

    debug_message = "Sandboxing : FCC configuration file reading successfull"
    self._log.debug(debug_message)

    # Find all additional files specified in the fccsw configuration file
    #xml_files = re.findall(r'file:(.*.xml)',content)

    txt_files = re.findall(r'="(.*.txt)', content)
    cmd_files = re.findall(r'filename="(.*.cmd)', content)

    # From these paths we re-create the tree in the temporary sandbox
    # with only the desired file.
    # In the configuration file, these paths are relative to FCCSW installation.
    # e.g. Generation/data/foo.xml

    if not self._resolve_tree_of_files(txt_files, '.txt'):
      error_message = "Sandboxing : _resolve_tree_of_files() failed"
      self._log.error(error_message)
      return False
      # Do not continue remaining checks

    # We do the same now for '.cmd' files specified in the configuration file
    return self._resolve_tree_of_files(cmd_files, '.cmd')

  def _import_files(self):
    """This function adds folders/files specified by the user for an application
    to the sandbox.

    :return: The success or the failure of the importation
    :rtype: bool

    """

    upload_path_message = "does not exist\n"
    upload_path_message += "Please ensure that your path exists in an accessible file system "
    upload_path_message += "(AFS or CVMFS)"

    if not self._temp_input_sandbox:
      warn_message = "Sandboxing : Your application has an empty input sandbox"
      self._log.warn(warn_message)
      return True

    for path in self._temp_input_sandbox:
      # We made a pre-checking of files in reachable filesystems (e.g. AFS, CVMFS)
      path, is_exist = self._find_path(path)

      # If file does not exist then consistency fails
      if not is_exist:
        error_message = "Sandboxing : The path '%s' %s" % (path, upload_path_message)
        self._log.error(error_message)
        return False
      else:
        if path.startswith('/afs/'):
          warn_message = "Sandboxing : You plan to upload '%s'" % path
          warn_message += " which is stored on AFS"
          warn_message += "\nSTORING FILES ON AFS IS DEPRECATED"

          # We log the message in the warning level
          self._log.warn(warn_message)

        # cvmfs paths do not need to be uploaded, they can be accessed remotely.
        # but for the moment do not be smart about it
        #if not path.startswith('/cvmfs/'):
        debug_message = "Sandboxing : The path '%s' required by the application" % path
        debug_message += " has been added to te sandbox"
        self._log.debug(debug_message)

        # if path is already in the sandbox, set type will kill duplicates
        self._input_sandbox.add(path)

    debug_message = "Sandboxing : Files required by FCC application"
    debug_message += " verified and added successfully to the sandbox"

    self._log.debug(debug_message)
    return True

  def _import_to_sandbox(self):
    """This function checks all the files and folders
    of the temporary sandbox and add them to the 'final' sandbox.

    :return: The success or the failure of the importation
    :rtype: bool

    """

    debug_message = "Sandboxing : Importation of user files/folders..."
    self._log.debug(debug_message)

    # Import files required by the application
    # If import process fails for some reasons (see functions above for more details)
    # then consistency fails
    if not self._import_files():
      error_message = "Sandboxing : _import_files() failed"
      self._log.error(error_message)
      return False
      # Do not continue remaining checks

    debug_message = "Sandboxing : Importation of user files/folders successfull"
    self._log.debug(debug_message)

    return True

  def _set_filter_to_folders(self):
    """Some folders required by FCCSW do not need to be imported with
    all their content.

    Some files have to be excluded or only some files have to be included.
    Then for each folder, we have the include/exclude parameter and the linked extension.

    This function map the folders with their corresponding filters if there are.

    :return: The success or the failure of the filter setting
    :rtype: bool

    """

    if not self._folders_to_filter:
      debug_message = "Sandboxing : No filtering required"
      self._log.debug(debug_message)
      return True

    copied_folders = set()

    for idx, actual_folder in enumerate(self._folders_to_filter):

      if not os.path.exists(actual_folder):
        error_message = ["Sandboxing : _filter_folders() failed"]
        error_message += ["The folder '%s' does not exist" % actual_folder]
        error_message += ["Check if you're FCCSW installation is complete"]
        self._log.error('\n'.join(error_message))
        return False

      if idx < len(self._filtered_extensions):
        filtered_extension = self._filtered_extensions[idx]
        exclude_or_include = self._excludes_or_includes[idx]
      else:
        filtered_extension = False
        exclude_or_include = False

      temp_folder = os.path.join(self._temp_cwd, os.path.basename(actual_folder))

      # DIRAC already compress the sandbox before submitting the job
      # do not compress folders

      debug_message = "Sandboxing : Folders filtering..."
      self._log.debug(debug_message)

      if not self._filter_folders(temp_folder, actual_folder, filtered_extension, exclude_or_include):
        error_message = "Sandboxing : _filter_folders() failed"
        self._log.error(error_message)
        return False

      debug_message = "Sandboxing : Folders filtering successfull"
      self._log.debug(debug_message)

      copied_folders.add(temp_folder)

    self._folders_to_filter = copied_folders

    return True

  def _read_from_file(self, file_name):
    """This function reads a file and returns its content.

    :param file_name: The path of the file to read
    :type file_name: str

    :return: The content of the file
    :rtype: str

    """

    try:
      with open(file_name, 'r') as file:
        content = file.read()
    except IOError:
      error_message = 'Sandboxing : FCC configuration file reading failed'
      return None, error_message

    debug_message = 'Sandboxing : FCC configuration file reading successfull'
    return content, debug_message

  def _resolve_tree_of_files(self, files, extension):
    """FCC configuration file like 'geant_pgun_fullsim.py'
    needs files coming from FCCSW installation. The path of these files
    are hard-coded in the FCC configuration file with a relative path to FCCSW installation.

    This function aims to resolve the full path of each hard-coded files
    required by the configuration file.

    Once we have the full path of the file, we recreate the tree strucutre
    of the file in a temporary folder and copy the file to it.

    Because the original directory of the file may contain other files, in this
    way we copy only the desired file.

    The file has now a new location and this is this new tree which will be
    added to the sandbox.

    :param files: The files specified in the configuration file
    :type files: list

    :param extension: The extension of file to resolve
    :type extension: str

    :return: success or failure of checking file
    :rtype: bool

    """

    if not files:
      warn_message = "Sandboxing : FCCSW configuration file"
      warn_message += " does not seem to need any additional '%s' files" % extension
      self._log.warn(warn_message)
      return True

    for file in files:
      # We save the relative path of the file
      # e.g. Generation/data/
      tree = os.path.dirname(file)
      # We prepend the temporary sandbox to the tree
      # which will become the new location of the file.
      tree_full_path = os.path.join(self._temp_cwd, tree)

      debug_message = "Sandboxing : Tree '%s' of additionnal" % tree_full_path
      debug_message += " '%s' files creation..." % extension
      self._log.debug(debug_message)

      # We create the tree locally in the temporary folder
      if not os.path.exists(tree_full_path):
        try:
          os.makedirs(tree_full_path)
        except OSError:
          error_message = "Sandboxing : Tree '%s' of additionnal" % tree_full_path
          error_message += " '%s' files creation failed" % extension
          self._log.error(error_message)
          return False

        debug_message = "Sandboxing : Tree '%s' of additionnal" % tree_full_path
        debug_message += " '%s' files creation successfull" % extension
        self._log.debug(debug_message)

      else:
        debug_message = "Sandboxing : Tree '%s' already exists" % tree_full_path
        self._log.debug(debug_message)

      # We take the first directory of the tree
      # We add this root directory to the 'final' sandbox
      root_folder = tree.split(os.path.sep)[0]
      root_folder_full_path = os.path.join(self._temp_cwd, root_folder)

      self._input_sandbox.add(root_folder_full_path)

      source = os.path.realpath(os.path.join(self.fccsw_path, file))
      destination = os.path.realpath(os.path.join(self._temp_cwd, file))

      if not os.path.exists(source):
        error_message = "Sandboxing : The file '%s' does not exist" % source
        self._log.error(error_message)
        return False

      if os.path.exists(destination):
        debug_message = "Sandboxing : The file '%s' already exists" % destination
        self._log.debug(debug_message)
        return True

      debug_message = "Sandboxing : Additional file '%s' copy..." % source
      self._log.debug(debug_message)

      try:
        shutil.copyfile(source, destination)
      except IOError, shutil.Error:
        error_message = "Sandboxing : Additionnal files"
        error_message += " '%s' copy failed" % source
        self._log.error(error_message)
        return False

    debug_message = "Sandboxing : Additionnal files"
    debug_message += " '%s' copy successfull to '%s'" % (source, destination)
    self._log.debug(debug_message)

    return True

  def _write2file(self, operation, file_name, filetext):
    """This function creates a new file and
    writes the given content into this file.

    :param operation: The operation('w' or 'a') of the writting operation
    :type operation: str

    :param file_name: The name of the file to create
    :type file_name: str

    :param filetext: The content of the file
    :type filetext: str

    """

    try:
      # Create file with 'operation' permission
      with open(file_name, operation) as text_file:
        text_file.write(filetext)
    except IOError:
      error_message = 'FCCSW configuration : Gaudi configuration file creation failed'
      self._log.error(error_message)
      return False

    debug_message = 'FCCSW configuration : Gaudi configuration file creation successfull'
    self._log.debug(debug_message)
    return True

###############################  Fcc DAUGHTER CLASSES ##############################################


class FccSw(Fcc):
  """Definition of an FCCSW application.

  Usage:

  >>> FCC_SW = FccSw(
      fcc_conf_file='Examples/options/geant_pgun_fullsim.py',
      fccsw_path='/build/username/FCC/FCCSW'
    )

  >>> FCC_SW.number_of_events = 1000

  """

  def __init__(self, fcc_conf_file="", fcc_output_file="",
         fccsw_path="", number_of_events=None, extraInputs=(), extraOutputs=()):

    super(FccSw, self).__init__(fcc_output_file,
                  number_of_events, extraInputs, extraOutputs)

    self.fccsw_path = fccsw_path
    self.fcc_conf_file = fcc_conf_file

  def _checkConsistency(self, job=None):


    self._log.debug("FCCSW specific consistency : _checkConsistency()...")

    if not(self.fccsw_path and self.fcc_conf_file):
      error_message = "FCCSW specific consistency : Error in parsing FCCSW application :"
      error_message += "\nYou have to provide the path of FCCSW installation"
      error_message += " and a valid configuration file"
      self._log.error(error_message)
      return S_ERROR(error_message)

    self.fccsw_path = os.path.abspath(self.fccsw_path)
    #self.fcc_conf_file = os.path.join(self.fccsw_path, self.fcc_conf_file)

    if not os.path.exists(self.fccsw_path):
      error_message = "FCCSW specific consistency : You have to provide the valid path"
      error_message += " of the FCCSW installation"
      self._log.error(error_message)
      return S_ERROR(error_message)

    if not self._generate_gaudi_cfg_file():
      error_message = "FCCSW specific consistency : _generate_gaudi_cfg_file() failed"
      self._log.error(error_message)
      return S_ERROR(error_message)

    # We have to add the gaudi option file to the sandbox
    self._input_sandbox.add(self.gaudi_options_file)

    # Stuff to call gaudirun.py
    python = 'python'
    xenv = '`which xenv`'
    xenv = '/cvmfs/fcc.cern.ch/sw/0.8/gaudi/v28r2/x86_64-slc6-gcc49-opt/scripts/xenv'

    # If InstallArea folder is on cvmfs so nothing to do
    # else download it because 'FCCSW.xenv' needs libraries from this folder

    if not self.fccsw_path.startswith('/cvmfs/'):
      arg_xenv = 'InstallArea/FCCSW.xenv'
      install_area_folder = os.path.join(self.fccsw_path, 'InstallArea')
      # We do not need all the content of these folders hence the filtering
      self._folders_to_filter.add(install_area_folder)
    else:
      arg_xenv = '%s/FCCSW.xenv' % self.fccsw_path

    exe = 'gaudirun.py'

    self.fcc_executable = 'exec %s %s --xml %s %s' %(python, xenv, arg_xenv, exe)

    self._log.debug("FCCSW specific consistency : _checkConsistency() successfull")

    return super(FccSw, self)._checkConsistency()

  def _generate_gaudi_cfg_file(self):
    """Generation of the Gaudi configuration file
    with the setting of :

      -  The number of event
      -  The gaudi log level
      -  The input file for FCCDataSvc
    """

    # There is 2 ways for configuring gaudirun.py
    # 1) By using gaudirun.py options :
    # e.g. ./gaudirun.py --option "ApplicationMgr().EvtMax=10"
    # 2) By creating a new python script containing the gaudi configuration :
    # This script has to be given as a second argument to gaudirun.py
    # e.g. ./gaudirun.py geant_pgun_fullsim.py gaudi_options.py
    # It then contains the event setting.
    # We decided to choose the second one.

    gaudi_options = ["from Configurables import ApplicationMgr"]
    gaudi_options += ["from Gaudi.Configuration import *"]

    #in putting -1, gaudi read all event of the file given to FCCDataSvc
    event_setting = "ApplicationMgr().EvtMax=%s" % self.NumberOfEvents
    gaudi_options += [event_setting]

    if self.log_level:
      if self.log_level.upper() in self._LOG_LEVELS:
        level_setting = "ApplicationMgr().OutputLevel=%s" % self.log_level
        gaudi_options += [level_setting]
      else:
        message = ["FCCSW specific consistency : Invalid value for the log level"]
        message += ["Possible values for the log level are :"]
        message += [" ".join(self._LOG_LEVELS)]
        self._log.error("\n".join(message))
        return False

    fccsw_podio_options = ["from Gaudi.Configuration import *"]
    fccsw_podio_options += ["from Configurables import ApplicationMgr, FCCDataSvc, PodioOutput"]
    fccsw_podio_options += ["import os"]

    if self._fcc_input_data:
      self._fcc_input_data = ["os.path.abspath(os.path.basename('%s'))" % data
                  for data in self._fcc_input_data]

      input_setting = "FCCDataSvc().input=%s" % " ".join(self._fcc_input_data)
      fccsw_podio_options += [input_setting]
      gaudi_options += fccsw_podio_options

    self.gaudi_options_file = os.path.join(os.getcwd(),
                         '%s_gaudi_options.py' % self.fcc_app_index)

    debug_message = 'FCCSW configuration : Gaudi configuration file creation...'
    self._log.debug(debug_message)

    return self._write2file('w', self.gaudi_options_file,
                "\n".join(gaudi_options) + '\n')

  def _import_to_sandbox(self):
    """Redefinition of FCC._import_to_sandbox() method.
      FCCSW needs extra folders like 'InstallArea', 'Detector'
      and extra files specified in its configuration file.
    """

    if not super(FccSw, self)._import_to_sandbox():
      error_message = "Sandboxing : _import_to_sandbox() failed"
      self._log.error(error_message)
      return False

    return self._import_fccsw_files()


class FccAnalysis(Fcc):
  """Definition of an FCCAnalysis application.
  By default, it runs FCCPHYSICS.

  Usage:

  >>> FCC_PHYSICS = FccAnalysis(
      fcc_conf_file='/cvmfs/fcc.cern.ch/sw/0.7/fcc-physics/0.1/x86_64-slc6-gcc49-opt/share/ee_ZH_Zmumu_Hbb.txt',
      fcc_output_file="ee_ZH_Zmumu_Hbb.root"
    )
  >>> FCC_PHYSICS.number_of_events = 1000

  """

  def __init__(self, executable='fcc-pythia8-generate', fcc_conf_file="", fcc_output_file="",
         number_of_events=None, extraInputs=(), extraOutputs=()):

    super(FccAnalysis, self).__init__(fcc_output_file,
                      number_of_events, extraInputs, extraOutputs)

    self.fcc_conf_file = fcc_conf_file
    self.fcc_executable = executable
