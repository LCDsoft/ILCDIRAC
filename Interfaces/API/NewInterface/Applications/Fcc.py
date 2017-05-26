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

  def __init__(self, outputFile,
         NumberOfEvents, extraInputs, extraOutputs):
    """FCC application generic attributes :

    :param outputFile: The output file of the application
    :type outputFile: str

    :param NumberOfEvents: The number of events
    :type NumberOfEvents: int

    :param extraInputs: The local input files required by the application
    :type extraInputs: tuple

    :param extraOutputs: The local output files
    :type extraOutputs: tuple

    """

    # Now, more attributes specific for each application
    # (FCCSW or FCCPHYSICS)

    # Required
    self.fccExecutable = ''
    self.fccConfFile = ''

    self.gaudiOptionsFile = ''

    self._fccOutputFile = outputFile
    self._fccInputData = []
    self.fccAppName = self.__class__.__name__
    self.fccAppIndex = ''

    self.NumberOfEvents = NumberOfEvents

    self._extraInputs = set([extraInputs]) if extraInputs else set()
    self._extraOutputs = set([extraOutputs]) if extraOutputs else set()

    # Path of FCCSW installation
    self.fccswPath = ''

    # Final input sandbox
    self._inputSandbox = set()

    # Temporary input sandbox
    # contains user files/folders not yet checked
    self._tempInputSandbox = set()

    # Temporary input sandbox
    # contains user files/folders not yet checked
    # and that need to be filtered (like 'Detector' folder of FCCSW installation)
    self._foldersToFilter = set()

    # Folder filters
    # which extension to filter
    self._filteredExtensions = []

    # What operation to do for each extension (include it or exclude it)
    self._excludesOrIncludes = []

    # Local path for the temporary sandbox
    self._tempCwd = os.path.join(os.getcwd(), 'temp_fcc_dirac')

    # Temporary output sandbox
    self._outputSandbox = set()

    # Gaudi log levels
    self._logLevels = ['DEBUG', 'INFO', 'ERROR', 'FATAL']

    # User log level chosen
    self.logLevel = None

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

    md1.addParameter(Parameter("fccExecutable", "", "string", "", "", False, False,
                   "The executable to run"))

    md1.addParameter(Parameter("fccConfFile", "", "string", "", "", False, False,
                   "FCC configuration file"))

    md1.addParameter(Parameter("fccAppIndex", "", "string", "", "", False, False,
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

    moduleinstance.setValue("fccExecutable", self.fccExecutable)
    moduleinstance.setValue("fccConfFile", self.fccConfFile)
    moduleinstance.setValue("fccAppIndex", self.fccAppIndex)
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

    infoMessage = "Application general consistency :"
    infoMessage += " _checkConsistency() on '%s'..." % self.fccAppName
    self._log.info(infoMessage)

    if not (self.version  and  self.platform):
      errorMessage = 'Consistency : Version, name and platform have to be set !'
      self._log.error(errorMessage)
      return S_ERROR(errorMessage)

    infoMessage = "Consistency : Version and platform of the application set to :"
    infoMessage += "\nversion : %s" % self.version
    infoMessage += "\nplatform : %s" % self.platform

    self._log.info(infoMessage)

    if not (self.fccExecutable and self.fccConfFile):
      errorMessage = "Consistency : Error in parsing '%s' application :" %  self.fccAppName
      errorMessage += "\nYou have to provide at least an executable"
      errorMessage += "and a configuration file for each application\n"
      self._log.error(errorMessage)
      return S_ERROR(errorMessage)

    debugMessage = "Consistency : Executable and configuration of the application set to :"
    debugMessage += "\nexecutable : %s" % self.fccExecutable
    debugMessage += "\nconfiguration : %s" % self.fccConfFile

    self._log.debug(debugMessage)

    # We add the log to the ouput sandbox
    # (I think it is already done by DIRAC)
    #self._outputSandbox.add(fccAppLog)

    # All input files are put in the temporary sandbox for a
    # pre-checking before being added to the final sandbox
    self._tempInputSandbox.add(self.fccConfFile)
    self._tempInputSandbox = self._tempInputSandbox.union(self._extraInputs)

    # We add extra (in/out)puts
    self._outputSandbox = self._outputSandbox.union(self._extraOutputs)

    # These files are already added by default by DIRAC to the sandbox
    #if 'wms' == mode:
      #self._outputSandbox.add('std.out')
      #self._outputSandbox.add('localEnv.log')

    infoMessage = "Sandboxing : Sandboxing in progress..."
    self._log.info(infoMessage)

    # We update the sandbox with files/folders required by the application
    if not self._importToSandbox():
      errorMessage = "_importToSandbox() failed"
      self._log.error(errorMessage)
      return S_ERROR(errorMessage)

    infoMessage = "Sandboxing : Sandboxing successfull"
    self._log.info(infoMessage)

    """
    setOutputFile() method informs the job that this application has an output file
    This output can be used as input for another application.
    In this way, app2.getInputFromApp(app1) method knows the ouput file of the given application
    app1 thanks to its method setOutputFile().
    """

    if self._fccOutputFile:
      self.setOutputFile(self._fccOutputFile)

    # Before submitting the job, we filter some folders required by FCCSW application
    # and we import the filtered folders to a temporary sandbox.
    if not self._setFilterToFolders():
      errorMessage = "_setFilterToFolders() failed"
      self._log.error(errorMessage)
      return S_ERROR(errorMessage)

    fccAppLog = '%s.log' % self.fccAppIndex

    if not self.logFile:
      self.setLogFile(fccAppLog)

    # We add to the output sandbox default output files like logfile, standard output and standard error 
    self._outputSandbox.add(fccAppLog)
    self._outputSandbox.add("%s.out" % self.fccAppIndex)
    self._outputSandbox.add("%s.err" % self.fccAppIndex)
    
    # self.inputSB is an attribute of the DIRAC Application and not of FCC.
    # The description file of the job (JDL file) contains a section for the input sandbox
    # This section is filled with a list of files (self.inputSB).
    # After user input files, application files, and application additionnal
    # files checked in the temporary sandbox, we 'merge' our 'final input sandbox'
    # to the DIRAC application input sandbox : self.inputSB
    self._inputSandbox = self._inputSandbox.union(self._foldersToFilter)
    self.inputSB = list(self._inputSandbox)

    """
    Sandbox can be set at the application level or at the job level.
    Whatever the level choosed, sandbox files are all put
    in the same final destination which is a list of paths
    in the JDL file (see Input Sandbox parameter of the JDL file).

    """

    infoMessage = '\n********************************FCC SUMMARY******************************'

    infoMessage += "\nYou plan to submit this application with its corresponding log :"
    infoMessage += '\n ' + self.fccAppName + ' --> ' + fccAppLog

    self._log.info(infoMessage)


    if self._inputSandbox:
      infoMessage = '\nHere is the content of its input sandbox :\n'
      infoMessage += '\n'.join(self._inputSandbox)
      self._log.info(infoMessage)

    if self._outputSandbox:
      infoMessage = '\nHere is the content of its output sandbox :\n'
      infoMessage += '\n'.join(self._outputSandbox)
      self._log.info(infoMessage)


    infoMessage = '\n********************************FCC SUMMARY******************************'
    self._log.info(infoMessage)


    infoMessage = "Application general consistency : _checkConsistency()"
    infoMessage += " on '%s' successfull" % self.fccAppName
    self._log.info(infoMessage)

    # Flush application sandboxes

    #self._flushSandboxes()

    return S_OK(infoMessage)

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

  def _flushSandboxes(self):
    """ Clear all sandboxes.
     Usefull when the same application is appended many times.
     But this possibility has been removed.
     """

    # FCC Application attributes
    self._tempInputSandbox.clear()
    self._inputSandbox.clear()

  def _filterFolders(self, tempFolder, actualFolder,
            filteredExtension, excludeOrInclude):
    """Knowing the filter for each folder, we copy the 'filtered'
    folder in the temporary folder 'temp_fcc_dirac'.

    After, we add the path of the 'filtered' folder to the sandbox instead of
    its original path.

    Like that, we do not import unnecessary files to the sandbox.

    :param tempFolder: The temporary working directory (destination) used for the sandboxing
    :type tempFolder: str

    :param actualFolder: The original (source) path of folder looked by the copy process
    :type actualFolder: str

    :param filteredExtension: The extension of file we do (not) want
    :type filteredExtension: str

    :param excludeOrInclude: extension is excluded or included
    :type excludeOrInclude: bool

    :return: success or failure of filtering
    :rtype: bool

    """

    # If folders have to be filtered then, we do not have to add them directly to the sandbox
    # because it will put all content of folders.
    # So we copy only the content we want in the filtered folder
    # inside the temporary folder 'temp_fcc_dirac'.
    # After, we add the filtered folders located in 'temp_fcc_dirac' to the sandbox.

    debugMessage = "Sandboxing : Checking of the filtered folder '%s'..." % tempFolder
    self._log.debug(debugMessage)

    if not os.path.exists(tempFolder):
      debugMessage = "Sandboxing : Creation of the filtered folder '%s'..." % tempFolder
      self._log.debug(debugMessage)

      try:
        os.makedirs(tempFolder)
      except OSError:
        errorMessage = "Sandboxing : Creation of the filtered folder"
        errorMessage += " '%s' failed" % tempFolder
        self._log.error(errorMessage)
        return False

      debugMessage = "Sandboxing : Creation of the filtered folder"
      debugMessage += " '%s' successfull" % tempFolder
      self._log.debug(debugMessage)

    for path in os.listdir(actualFolder):
      source = os.path.realpath(os.path.join(actualFolder, path))
      destination = os.path.realpath(os.path.join(tempFolder, path))

      if not os.path.isfile(source):
        # Recursive call for folders
        if not self._filterFolders(destination, source, filteredExtension, excludeOrInclude):
          errorMessage = "Sandboxing : _filterFolders() failed"
          self._log.error(errorMessage)
          return False
      else:
        # If file then copy it
        if not os.path.exists(source):
          errorMessage = "Sandboxing : The file '%s' does not exist" % source
          self._log.error(errorMessage)
          return False

        if os.path.exists(destination):
          debugMessage = "Sandboxing : The file '%s' already exists" % source
          self._log.debug(debugMessage)
          return True

        debugMessage = "Sandboxing : File '%s' copy..." % source
        self._log.debug(debugMessage)

        if ((excludeOrInclude and not path.endswith(filteredExtension))
            or (not excludeOrInclude and path.endswith(filteredExtension))
            or not filteredExtension):

          try:
            shutil.copyfile(source, destination)
          except IOError, shutil.Error:
            errorMessage = "Sandboxing : The copy of the file '%s' failed"% destination
            self._log.error(errorMessage)
            return False

          debugMessage = "Sandboxing : Copy of the file"
          debugMessage += " '%s' successfull to '%s'" % (source, destination)
          self._log.debug(debugMessage)

    debugMessage = "Sandboxing : Folder '%s' filtering successfull" % tempFolder
    self._log.debug(debugMessage)
    return True

  def _findPath(self, path):
    """This function checks if file/folder exists.

    :param path: The path to look for
    :type path: str

    :return: The full path and its existence
    :rtype: str, bool

    """

    path = os.path.abspath(path)
    return (path, True) if os.path.exists(path) else (path, False)

  def _importFccswFiles(self):
    """FCCSW application needs additional files specified in the configuration file
    It also needs folders like 'InstallArea' and 'Detector'.

    :return: The success or the failure of the importation
    :rtype: bool

    """

    #installAreaFolder = os.path.join(self.fccswPath, 'InstallArea')
    detectorFolder = os.path.join(self.fccswPath, 'Detector')
    #fccsw_folders = [installAreaFolder, detectorFolder]

    # We do not need all the content of these folders hence the filtering
    self._foldersToFilter.add(detectorFolder)

    # Explanation
    # InstallArea_folder : all dbg files are excluded
    # detectorFolder : only xml files are included
    self._filteredExtensions += ['.dbg', '.xml']
    self._excludesOrIncludes += [True, False]

    debugMessage = "Sandboxing : FCC configuration file reading..."
    self._log.debug(debugMessage)

    content, message = self._readFromFile(self.fccConfFile)

    # If configuration file is not valid then consistency fails
    if not content:
      self._log.error(message)
      return False

    self._log.debug(message)

    debugMessage = "Sandboxing : FCC configuration file reading successfull"
    self._log.debug(debugMessage)

    # Find all additional files specified in the fccsw configuration file
    #xml_files = re.findall(r'file:(.*.xml)',content)

    txtFiles = re.findall(r'="(.*.txt)', content)
    cmdFiles = re.findall(r'filename="(.*.cmd)', content)

    # From these paths we re-create the tree in the temporary sandbox
    # with only the desired file.
    # In the configuration file, these paths are relative to FCCSW installation.
    # e.g. Generation/data/foo.xml

    if not self._resolveTreeOfFiles(txtFiles, '.txt'):
      errorMessage = "Sandboxing : _resolveTreeOfFiles() failed"
      self._log.error(errorMessage)
      return False
      # Do not continue remaining checks

    # We do the same now for '.cmd' files specified in the configuration file
    return self._resolveTreeOfFiles(cmdFiles, '.cmd')

  def _importFiles(self):
    """This function adds folders/files specified by the user for an application
    to the sandbox.

    :return: The success or the failure of the importation
    :rtype: bool

    """

    uploadPathMessage = "does not exist\n"
    uploadPathMessage += "Please ensure that your path exists in an accessible file system "
    uploadPathMessage += "(AFS or CVMFS)"

    if not self._tempInputSandbox:
      warnMessage = "Sandboxing : Your application has an empty input sandbox"
      self._log.warn(warnMessage)
      return True

    for path in self._tempInputSandbox:
      # We made a pre-checking of files in reachable filesystems (e.g. AFS, CVMFS)
      path, isExist = self._findPath(path)

      # If file does not exist then consistency fails
      if not isExist:
        errorMessage = "Sandboxing : The path '%s' %s" % (path, uploadPathMessage)
        self._log.error(errorMessage)
        return False
      else:
        if path.startswith('/afs/'):
          warnMessage = "Sandboxing : You plan to upload '%s'" % path
          warnMessage += " which is stored on AFS"
          warnMessage += "\nSTORING FILES ON AFS IS DEPRECATED"

          # We log the message in the warning level
          self._log.warn(warnMessage)

        # cvmfs paths do not need to be uploaded, they can be accessed remotely.
        # but for the moment do not be smart about it
        #if not path.startswith('/cvmfs/'):
        debugMessage = "Sandboxing : The path '%s' required by the application" % path
        debugMessage += " has been added to te sandbox"
        self._log.debug(debugMessage)

        # if path is already in the sandbox, set type will kill duplicates
        self._inputSandbox.add(path)

    debugMessage = "Sandboxing : Files required by FCC application"
    debugMessage += " verified and added successfully to the sandbox"

    self._log.debug(debugMessage)
    return True

  def _importToSandbox(self):
    """This function checks all the files and folders
    of the temporary sandbox and add them to the 'final' sandbox.

    :return: The success or the failure of the importation
    :rtype: bool

    """

    debugMessage = "Sandboxing : Importation of user files/folders..."
    self._log.debug(debugMessage)

    # Import files required by the application
    # If import process fails for some reasons (see functions above for more details)
    # then consistency fails
    if not self._importFiles():
      errorMessage = "Sandboxing : _importFiles() failed"
      self._log.error(errorMessage)
      return False
      # Do not continue remaining checks

    debugMessage = "Sandboxing : Importation of user files/folders successfull"
    self._log.debug(debugMessage)

    return True

  def _setFilterToFolders(self):
    """Some folders required by FCCSW do not need to be imported with
    all their content.

    Some files have to be excluded or only some files have to be included.
    Then for each folder, we have the include/exclude parameter and the linked extension.

    This function map the folders with their corresponding filters if there are.

    :return: The success or the failure of the filter setting
    :rtype: bool

    """

    if not self._foldersToFilter:
      debugMessage = "Sandboxing : No filtering required"
      self._log.debug(debugMessage)
      return True

    copiedFolders = set()

    for idx, actualFolder in enumerate(self._foldersToFilter):

      if not os.path.exists(actualFolder):
        errorMessage = ["Sandboxing : _filterFolders() failed"]
        errorMessage += ["The folder '%s' does not exist" % actualFolder]
        errorMessage += ["Check if you're FCCSW installation is complete"]
        self._log.error('\n'.join(errorMessage))
        return False

      if idx < len(self._filteredExtensions):
        filteredExtension = self._filteredExtensions[idx]
        excludeOrInclude = self._excludesOrIncludes[idx]
      else:
        filteredExtension = False
        excludeOrInclude = False

      tempFolder = os.path.join(self._tempCwd, os.path.basename(actualFolder))

      # DIRAC already compress the sandbox before submitting the job
      # do not compress folders

      debugMessage = "Sandboxing : Folders filtering..."
      self._log.debug(debugMessage)

      if not self._filterFolders(tempFolder, actualFolder, filteredExtension, excludeOrInclude):
        errorMessage = "Sandboxing : _filterFolders() failed"
        self._log.error(errorMessage)
        return False

      debugMessage = "Sandboxing : Folders filtering successfull"
      self._log.debug(debugMessage)

      copiedFolders.add(tempFolder)

    self._foldersToFilter = copiedFolders

    return True

  def _readFromFile(self, file_name):
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
      errorMessage = 'Sandboxing : FCC configuration file reading failed'
      return None, errorMessage

    debugMessage = 'Sandboxing : FCC configuration file reading successfull'
    return content, debugMessage

  def _resolveTreeOfFiles(self, files, extension):
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
      warnMessage = "Sandboxing : FCCSW configuration file"
      warnMessage += " does not seem to need any additional '%s' files" % extension
      self._log.warn(warnMessage)
      return True

    for file in files:
      # We save the relative path of the file
      # e.g. Generation/data/
      tree = os.path.dirname(file)
      # We prepend the temporary sandbox to the tree
      # which will become the new location of the file.
      treeFullPath = os.path.join(self._tempCwd, tree)

      debugMessage = "Sandboxing : Tree '%s' of additionnal" % treeFullPath
      debugMessage += " '%s' files creation..." % extension
      self._log.debug(debugMessage)

      # We create the tree locally in the temporary folder
      if not os.path.exists(treeFullPath):
        try:
          os.makedirs(treeFullPath)
        except OSError:
          errorMessage = "Sandboxing : Tree '%s' of additionnal" % treeFullPath
          errorMessage += " '%s' files creation failed" % extension
          self._log.error(errorMessage)
          return False

        debugMessage = "Sandboxing : Tree '%s' of additionnal" % treeFullPath
        debugMessage += " '%s' files creation successfull" % extension
        self._log.debug(debugMessage)

      else:
        debugMessage = "Sandboxing : Tree '%s' already exists" % treeFullPath
        self._log.debug(debugMessage)

      # We take the first directory of the tree
      # We add this root directory to the 'final' sandbox
      rootFolder = tree.split(os.path.sep)[0]
      rootFolderFullPath = os.path.join(self._tempCwd, rootFolder)

      self._inputSandbox.add(rootFolderFullPath)

      source = os.path.realpath(os.path.join(self.fccswPath, file))
      destination = os.path.realpath(os.path.join(self._tempCwd, file))

      if not os.path.exists(source):
        errorMessage = "Sandboxing : The file '%s' does not exist" % source
        self._log.error(errorMessage)
        return False

      if os.path.exists(destination):
        debugMessage = "Sandboxing : The file '%s' already exists" % destination
        self._log.debug(debugMessage)
        return True

      debugMessage = "Sandboxing : Additional file '%s' copy..." % source
      self._log.debug(debugMessage)

      try:
        shutil.copyfile(source, destination)
      except IOError, shutil.Error:
        errorMessage = "Sandboxing : Additionnal files"
        errorMessage += " '%s' copy failed" % source
        self._log.error(errorMessage)
        return False

    debugMessage = "Sandboxing : Additionnal files"
    debugMessage += " '%s' copy successfull to '%s'" % (source, destination)
    self._log.debug(debugMessage)

    return True

  def _writeToFile(self, operation, file_name, filetext):
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
      with open(file_name, operation) as textFile:
        textFile.write(filetext)
    except IOError:
      errorMessage = 'FCCSW configuration : Gaudi configuration file creation failed'
      self._log.error(errorMessage)
      return False

    debugMessage = 'FCCSW configuration : Gaudi configuration file creation successfull'
    self._log.debug(debugMessage)
    return True

###############################  Fcc DAUGHTER CLASSES ##############################################


class FccSw(Fcc):
  """Definition of an FCCSW application.

  Usage:

  >>> fccSw = FccSw(
      fccConfFile='Examples/options/geant_pgun_fullsim.py',
      fccswPath='/build/username/FCC/FCCSW'
    )

  >>> fccSw.NumberOfEvents = 1000

  """

  def __init__(self, fccConfFile="", outputFile="",
         fccswPath="", NumberOfEvents=None, extraInputs=(), extraOutputs=()):

    super(FccSw, self).__init__(outputFile,
                  NumberOfEvents, extraInputs, extraOutputs)

    self.fccswPath = fccswPath
    self.fccConfFile = fccConfFile

  def _checkConsistency(self, job=None):

    self._log.debug("FCCSW specific consistency : _checkConsistency()...")

    if not(self.fccswPath and self.fccConfFile):
      errorMessage = "FCCSW specific consistency : Error in parsing FCCSW application :"
      errorMessage += "\nYou have to provide the path of FCCSW installation"
      errorMessage += " and a valid configuration file"
      self._log.error(errorMessage)
      return S_ERROR(errorMessage)

    self.fccswPath = os.path.abspath(self.fccswPath)
    #self.fccConfFile = os.path.join(self.fccswPath, self.fccConfFile)

    if not os.path.exists(self.fccswPath):
      errorMessage = "FCCSW specific consistency : You have to provide the valid path"
      errorMessage += " of the FCCSW installation"
      self._log.error(errorMessage)
      return S_ERROR(errorMessage)

    debugMessage = "FCCSW specific consistency : Creation of a temporary folder 'temp_fcc_dirac'"
    debugMessage += " in the current working directory..."
    self._log.debug(debugMessage)

    # First, it creates a temporary local directory for folders whose
    # the content does not have to be sandboxed entirely
    # like filtered folders
    if not os.path.exists(self._tempCwd):
      try:
        os.makedirs(self._tempCwd)
      except OSError:
        errorMessage = "FCCSW specific consistency : Creation of 'temp_fcc_dirac' folder failed"
        self._log.error(errorMessage)
        return False

      debugMessage = "FCCSW specific consistency : Creation of 'temp_fcc_dirac' folder successfull"
      self._log.debug(debugMessage)
    else:
      debugMessage = "FCCSW specific consistency : The temporary folder 'temp_fcc_dirac' already exists"
      self._log.debug(debugMessage)

    if not self._generateGaudiConfFile():
      errorMessage = "FCCSW specific consistency : _generateGaudiConfFile() failed"
      self._log.error(errorMessage)
      return S_ERROR(errorMessage)

    # We have to add the gaudi option file to the sandbox
    self._inputSandbox.add(self.gaudiOptionsFile)

    # Stuff to call gaudirun.py
    python = 'python'
    xenv = '`which xenv`'
    xenv = '/cvmfs/fcc.cern.ch/sw/0.8/gaudi/v28r2/x86_64-slc6-gcc49-opt/scripts/xenv'

    # If InstallArea folder is on cvmfs so nothing to do
    # else download it because 'FCCSW.xenv' needs libraries from this folder

    if not self.fccswPath.startswith('/cvmfs/'):
      argXenv = 'InstallArea/FCCSW.xenv'
      installAreaFolder = os.path.join(self.fccswPath, 'InstallArea')
      # We do not need all the content of these folders hence the filtering
      self._foldersToFilter.add(installAreaFolder)
    else:
      argXenv = '%s/FCCSW.xenv' % self.fccswPath

    exe = 'gaudirun.py'

    self.fccExecutable = 'exec %s %s --xml %s %s' %(python, xenv, argXenv, exe)

    self._log.debug("FCCSW specific consistency : _checkConsistency() successfull")

    return super(FccSw, self)._checkConsistency()

  def _generateGaudiConfFile(self):
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

    gaudiOptions = ["from Configurables import ApplicationMgr"]
    gaudiOptions += ["from Gaudi.Configuration import *"]

    #in putting -1, gaudi read all event of the file given to FCCDataSvc
    eventSetting = "ApplicationMgr().EvtMax=%s" % self.NumberOfEvents
    gaudiOptions += [eventSetting]

    if self.logLevel:
      if self.logLevel.upper() in self._logLevels:
        levelSetting = "ApplicationMgr().OutputLevel=%s" % self.logLevel
        gaudiOptions += [levelSetting]
      else:
        message = ["FCCSW specific consistency : Invalid value for the log level"]
        message += ["Possible values for the log level are :"]
        message += [" ".join(self._logLevels)]
        self._log.error("\n".join(message))
        return False

    fccswPodioOptions = ["from Gaudi.Configuration import *"]
    fccswPodioOptions += ["from Configurables import ApplicationMgr, FCCDataSvc, PodioOutput"]
    fccswPodioOptions += ["import os"]

    if self._fccInputData:
      self._fccInputData = ["os.path.abspath(os.path.basename('%s'))" % data
                  for data in self._fccInputData]

      inputSetting = "FCCDataSvc().input=%s" % " ".join(self._fccInputData)
      fccswPodioOptions += [inputSetting]
      gaudiOptions += fccswPodioOptions

    self.gaudiOptionsFile = os.path.join(self._tempCwd,
                         '%s_gaudiOptions.py' % self.fccAppIndex)

    debugMessage = 'FCCSW configuration : Gaudi configuration file creation...'
    self._log.debug(debugMessage)

    return self._writeToFile('w', self.gaudiOptionsFile,
                "\n".join(gaudiOptions) + '\n')

  def _importToSandbox(self):
    """Redefinition of FCC._importToSandbox() method.
      FCCSW needs extra folders like 'InstallArea', 'Detector'
      and extra files specified in its configuration file.
    """

    if not super(FccSw, self)._importToSandbox():
      errorMessage = "Sandboxing : _importToSandbox() failed"
      self._log.error(errorMessage)
      return False

    return self._importFccswFiles()


class FccAnalysis(Fcc):
  """Definition of an FCCAnalysis application.
  By default, it runs FCCPHYSICS.

  Usage:

  >>> FCC_PHYSICS = FccAnalysis(
      fccConfFile='/cvmfs/fcc.cern.ch/sw/0.7/fcc-physics/0.1/x86_64-slc6-gcc49-opt/share/ee_ZH_Zmumu_Hbb.txt',
      outputFile="ee_ZH_Zmumu_Hbb.root"
    )
  >>> FCC_PHYSICS.NumberOfEvents = 1000

  """

  def __init__(self, executable='fcc-pythia8-generate', fccConfFile="", outputFile="",
         NumberOfEvents=None, extraInputs=(), extraOutputs=()):

    super(FccAnalysis, self).__init__(outputFile,
                      NumberOfEvents, extraInputs, extraOutputs)

    self.fccConfFile = fccConfFile
    self.fccExecutable = executable
