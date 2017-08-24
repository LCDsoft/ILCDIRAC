"""FCC application is defined by 2 modules which are :

     - ILCDIRAC.Interfaces.API.NewInterface.Applications.Fcc (this module)
     - ILCDIRAC.Workflow.Modules.FccAnalysis

   FCC applications can run under a UserJob located here :

     - ILCDIRAC.Interfaces.API.NewInterface.UserJob

"""

# standard libraries
import os
import re
import shutil
import types

# DIRAC libraries
from ILCDIRAC.Interfaces.API.NewInterface.Application import Application
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Workflow.Parameter import Parameter

__RCSID__ = "$Id$"


class Fcc(Application):
  """Fcc class defines the skeleton of an FCC application
  It inherits from the Application module.

  Fcc application is the base class for :
  -   FccSw
  -   FccAnalysis

  Look their definition at the end of this module.

  """

  def __init__(self):


    # Required
    self.fccExecutable = ''
    
    self.isGaudiOptionsFileNeeded = False

    # Path of FCCSW installation
    self.fccSwPath = ''

    # Read or generate events
    self.read = False

    # Random generator service used to set seed and number of events
    self.randomGenerator = {}

    # Final FCC input sandbox
    self._inputSandbox = set()

    # Temporary FCC input sandbox
    # contains user files/folders not yet checked
    self._tempInputSandbox = set()

    # Some folders have to be filtered (like 'Detector' folder of FCCSW installation)
    # to avoid sandbox overload (sandbox max size = 10 Mb)
    self._foldersToFilter = set()

    # Folder filters
    # which extension to filter
    self._filteredExtensions = []

    # What operation to do for each extension (include it or exclude it)
    self._excludesOrIncludes = []

    # Local path for the temporary sandbox
    self._tempCwd = os.path.realpath('temp_fcc_dirac')

    # Output sandbox (not the real one)
    # used just for printing informations to the user
    self._outputSandbox = set()

    # User log level
    self.logLevel = None

    self.datatype = 'REC'
    self.detectortype = 'ILD'


    super(Fcc, self).__init__()
    # Those 5 need to come after default constructor
    self._modulename = 'FccAnalysis'
    #self._importLocation = 'ILCDIRAC.Workflow.Modules'
    self._moduledescription = "Module running FCC software"
    self.appname = self.__class__.__name__
    self.applicationFolder = ''
    self.version = "v1.0"
    self.energy = 0
    self.numberOfEvents = 0

  def _applicationModule(self):
    """It transfers parameter names of the module
       Fcc to the module FccAnalysis.

    :return: The module for which we give the parameters
    :rtype: moduleinstance

    """

    md1 = self._createModuleDefinition()

    md1.addParameter(Parameter("fccExecutable", "", "string", "", "", False, False,
                               "The executable to run"))

    md1.addParameter(Parameter("isGaudiOptionsFileNeeded", False, "bool", "", "", False, False,
                               "Gaudi configuration file"))

    md1.addParameter(Parameter("logLevel", "", "string", "", "", False, False,
                               "Gaudi Log Level"))

    md1.addParameter(Parameter("read", "", "string", "", "", False, False,
                               "Application can read or generate events"))

    md1.addParameter(Parameter("randomGenerator", {}, "dict", "", "", False, False,
                               "Pythia card files"))

    return md1

  def _applicationModuleValues(self, moduleinstance):
    """It transfers parameter values of the module
    Fcc to the module FccAnalysis.

    :param moduleinstance: The module we load (FCCAnalysis)

    """

    moduleinstance.setValue("fccExecutable", self.fccExecutable)
    moduleinstance.setValue("isGaudiOptionsFileNeeded", self.isGaudiOptionsFileNeeded)
    moduleinstance.setValue("logLevel", self.logLevel)
    moduleinstance.setValue("read", self.read)
    moduleinstance.setValue("randomGenerator", self.randomGenerator)

  def _checkConsistency(self, job=None):
    """This function checks the minimum requirements of the application
    and updates the sandbox with files/folders required by the application.

    :param job: The job containing the application
    :type job: DIRAC.Interfaces.API.Job.Job

    :return: The success or failure of the consistency checking
    :rtype: DIRAC.S_OK, DIRAC.S_ERROR

    """

    infoMessage = (
      "Application general consistency :"
      " _checkConsistency() on '%(name)s'..." % {'name':self.appname}
    )
    self._log.info(infoMessage)

    if not self.version:
      errorMessage = 'Version not set!'
      self._log.error(errorMessage)
      return S_ERROR(errorMessage)

    infoMessage = (
      "Consistency : Version of the application set to :"
      "\nversion : %(version)s" % {'version':self.version}
    )
    self._log.info(infoMessage) 

    # The executable is mandatory and also the configuration file except if the
    # input is taken from an other app hence the use of '_inputapp'
    if not (self.fccExecutable and (self.steeringFile or self._inputapp)):
      errorMessage = (
        "Consistency : Error in parsing '%(name)s' application :\n"
        "You have to provide at least an executable"
        " and a configuration file for each application" % {'name':self.appname}
      )
      self._log.error(errorMessage)
      return S_ERROR(errorMessage)

    if True is not self.steeringFile and not isinstance(self.steeringFile, str):
      errorMessage = (
        "Consistency : Fcc Application accepts only one input configuration file:\n"
        "If you want to run the application '%(name)s' with many configurations then\n"
        "Create an new application with the other configuration\n"
        "You can also use 'getInputFromApp' function to link applications" % {'name':self.appname}
      )
      self._log.error(errorMessage)
      return S_ERROR(errorMessage)

    debugMessage = (
      "Consistency : Executable and configuration of the application set to :"
      "\nexecutable : %(exec)s"
      "\nconfiguration : %(conf)s" % {'exec':self.fccExecutable, 'conf':self.steeringFile}
    )
    self._log.debug(debugMessage)

    # All input files are put in the FCC temporary sandbox for a
    # pre-checking before being added to the FCC final sandbox
    if self.steeringFile and not self._inputapp:
      self._tempInputSandbox.add(self.steeringFile)

    infoMessage = "Sandboxing : Sandboxing in progress..."
    self._log.info(infoMessage)

    # We update the sandbox with files/folders required by the application
    if not self._importToSandbox():
      errorMessage = "_importToSandbox() failed"
      self._log.error(errorMessage)
      return S_ERROR(errorMessage)

    infoMessage = "Sandboxing : Sandboxing successfull"
    self._log.info(infoMessage)
    
    # setOutputFile() method informs the job that this application has an output file
    # This output can be used as input for another application.
    # In this way, app2.getInputFromApp(app1) method knows the ouput file of the given application
    # app1 thanks to its method setOutputFile().
    
    # Before submitting the job, we filter some folders required by applications
    # and we import the filtered folders to the sandbox.
    if not self._setFilterToFolders():
      errorMessage = "_setFilterToFolders() failed"
      self._log.error(errorMessage)
      return S_ERROR(errorMessage)
    
    # self.inputSB is an attribute of the DIRAC Application and not of FCC.
    # The description file of the job (JDL file) contains a section for the input sandbox
    # This section is filled with a list of files (self.inputSB).
    # After user input files, application files, and application additionnal
    # files checked in the temporary sandbox, we set
    # the DIRAC application input sandbox : self.inputSB
    
    
    self._inputSandbox = self._inputSandbox.union(self._foldersToFilter)
    self.inputSB = list(self._inputSandbox)

    
    # Sandbox can be set at the application level or at the job level.
    # Whatever the level choosed, sandbox files are all put
    # in the same final destination which is a list of paths
    # in the JDL file (see Input Sandbox parameter of the JDL file).
    

    infoMessage = (
      "Application general consistency : _checkConsistency()"
      " on '%(name)s' successfull" % {'name':self.appname}
    )
    self._log.info(infoMessage)

    # Flush application sandboxes

    return S_OK(infoMessage)

  def _checkFinalConsistency(self):
    """

    :return: The success of the final consistency checking
    :rtype: DIRAC.S_OK

    """

    applicationStep = len(self._jobapps) + 1
    self.applicationFolder = "%s_%s_Step_%s" % (self.appname, self.version, applicationStep)
    # Take in priority output file given in setOutputFile
    if self.outputFile :
      self.setOutputFile(os.path.join(self.applicationFolder, self.outputFile))
    else:
      # Compute root file name    
      self.setOutputFile(os.path.join(self.applicationFolder, "%s.root" % self.applicationFolder))

    # We add the log file and the output file to the output sandbox
    self._outputSandbox.add(self.logFile)
    self._outputSandbox.add("%s (%s)" % (os.path.basename(self.outputFile), "Name of the eventual output root file") )
      
    infoMessage = (
      "\n********************************FCC SUMMARY******************************\n"
      "You plan to submit this application with its corresponding log :\n"
      "%(name)s --> %(log)s" % {'name':self.appname, 'log':self.logFile}
    )
    self._log.info(infoMessage)


    if self._inputSandbox:
      infoMessage = (
        "\nHere is the content of its input sandbox :\n%(input)s"
        % {'input':'\n'.join(self._inputSandbox)}
      )
      self._log.info(infoMessage)

    if self._outputSandbox:
      infoMessage = (
        "\nHere are the output files :\n%(output)s"
        % {'output':'\n'.join(self._outputSandbox)}
      )
      self._log.info(infoMessage)


    infoMessage = "\n********************************FCC SUMMARY******************************"
    self._log.info(infoMessage)

    return super(Fcc, self)._checkFinalConsistency()

  def _checkWorkflowConsistency(self):
    """Summary of the application done in
    _checkConsistency() method.

    :return: The success or the failure of _checkRequiredApp()
    :rtype: DIRAC.S_OK, DIRAC.S_ERROR

    """
    return self._checkRequiredApp()

  def _prodjobmodules(self, stepdefinition):
    
    ## Here one needs to take care of listoutput
    if self.outputPath:
      self._listofoutput.append({'OutputFile' : '@{OutputFile}', "outputPath" : "@{OutputPath}",
                                 "outputDataSE" : '@{OutputSE}'})

    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setOutputComputeDataList(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
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

###############################  Fcc METHODS #####################################################
  @staticmethod
  def _findPath(path):
    """This function checks if file/folder exists.

    :param path: The path to look for
    :type path: str

    :return: The absolute path and its existence
    :rtype: (str, bool)

    """

    tempPath = os.path.realpath(path)
    return (tempPath, True) if os.path.exists(tempPath) else (path, False)

  def _importFiles(self):
    """This function adds folders/files specified by the user for an application
    to the sandbox.

    :return: The success or the failure of the import
    :rtype: bool

    """

    if not self._tempInputSandbox:
      warnMessage = "Sandboxing : Your application has an empty input sandbox"
      self._log.warn(warnMessage)
      return True

    for path in self._tempInputSandbox:
      # We make a pre-checking of files in reachable filesystems (e.g. AFS, CVMFS)
      path, exists = self._findPath(path)

      # If file does not exist then consistency fails
      if not exists:
        errorMessage = (
          "Sandboxing : The path '%(path)s' does not exist\n"
          "Please ensure that your path exists in an accessible file system "
          "(AFS or CVMFS)" % {'path':path}
        )
        self._log.error(errorMessage)
        return False

      if path.startswith('/afs/'):
        warnMessage = (
          "Sandboxing : You plan to upload '%(path)s'"
          " which is stored on AFS\n"
          "STORING FILES ON AFS IS DEPRECATED" % {'path':path}
        )

        # We log the message into the warning level
        self._log.warn(warnMessage)

      # cvmfs paths do not need to be uploaded, they can be accessed remotely.
      # but for the moment do not be smart about it, add also cvmfs files,
      # no need to check.
      #if not path.startswith('/cvmfs/'):
      # if path is already in the sandbox, set type will kill duplicates
      self._inputSandbox.add(path)

      debugMessage = (
        "Sandboxing : The path '%(path)s' required by the application"
        " has been added to te sandbox" % {'path':path}
      )
      self._log.debug(debugMessage)

    debugMessage = (
      "Sandboxing : Files required by FCC application"
      " verified and added successfully to the sandbox"
    )

    self._log.debug(debugMessage)
    return True

  def _importToSandbox(self):
    """This function checks all the files and folders
    of the FCC temporary sandbox and add them to the FCC 'final' sandbox.

    :return: The success or the failure of the import
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

  @staticmethod
  def _readFromFile(fileName):
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
      errorMessage = 'Sandboxing : FCC configuration file reading failed\n%s' % e
      return None, errorMessage

    debugMessage = 'Sandboxing : FCC configuration file reading successfull'
    return content, debugMessage

###############################  Fcc DAUGHTER CLASSES ##############################################


class FccSw(Fcc):
  """Definition of an FCCSW application.

  Usage:

  >>> fccSw = FccSw(
      fccConfFile='/build/username/FCC/FCCSW/Examples/options/geant_pgun_fullsim.py',
      fccSwPath='/build/username/FCC/FCCSW'
    )

  """

  def __init__(self, fccConfFile="", fccSwPath="", read=False):

    super(FccSw, self).__init__()

    self.fccSwPath = os.path.realpath(fccSwPath)
    self.steeringFile = fccConfFile
    self.read = read

  def _checkConsistency(self, job=None):
    """Redefinition of ILCDIRAC.Workflow.Modules.Fcc._checkConsistency().

    :param job: The job containing the application
    :type job: DIRAC.Interfaces.API.Job.Job

    :return: The success or the failure of the consistency checking
    :rtype: DIRAC.S_OK, DIRAC.S_ERROR

    """

    self._log.debug("FCCSW specific consistency : _checkConsistency()...")

    if not self.fccSwPath or not os.path.exists(self.fccSwPath):
      errorMessage = (
        "FCCSW specific consistency : Error in parsing FCCSW application :\n"
        "You have to provide a valid path of the FCCSW installation"
      )
      self._log.error(errorMessage)
      return S_ERROR(errorMessage)

    debugMessage = (
      "FCCSW specific consistency : Creation of a temporary folder 'temp_fcc_dirac'"
      " in the current working directory..."
    )
    self._log.debug(debugMessage)

    # First, it creates a temporary local directory for folders whose
    # the content does not have to be sandboxed entirely
    # like filtered folders
    if not os.path.exists(self._tempCwd):
      try:
        os.makedirs(self._tempCwd)
      except OSError as e:
        errorMessage = "FCCSW specific consistency : Creation of 'temp_fcc_dirac' folder failed\n%s" % e
        self._log.error(errorMessage)
        return S_ERROR(errorMessage)

      debugMessage = "FCCSW specific consistency : Creation of 'temp_fcc_dirac' folder successfull"
      self._log.debug(debugMessage)
    else:
      debugMessage = "FCCSW specific consistency : The temporary folder 'temp_fcc_dirac' already exists"
      self._log.debug(debugMessage)

    # If InstallArea folder is on cvmfs so nothing to do
    # else download it because 'FCCSW.xenv' needs libraries from this folder

    if not self.fccSwPath.startswith('/cvmfs/'):
      installAreaFolder = os.path.join(self.fccSwPath, 'InstallArea')
      # We do not need all the content of these folders hence the filtering
      self._foldersToFilter.add(installAreaFolder)

    # Actually FCCSW CVMFS run successfully only with configuration files that do not need additionnal files
    # like Generation/data/ParticleTable.txt
    # Release made after 31/08/2017 will put a complete installation of FCCSW to make successfull
    # All examples of Examples/options

    self.fccExecutable = '%s/run gaudirun.py' % self.fccSwPath

    self._log.debug("FCCSW specific consistency : _checkConsistency() successfull")

    return super(FccSw, self)._checkConsistency()

  def _checkFinalConsistency(self):
    """Redefinition of ILCDIRAC.Workflow.Modules.Fcc._checkFinalConsistency().
    Setting True 'isGaudiOptionsFileNeeded' attribute tells that the application needs to generate
    the gaudi option file like for FccSw.

    :return: The success of the final consistency checking
    :rtype: DIRAC.S_OK

    """

    self.isGaudiOptionsFileNeeded = True
    return super(FccSw, self)._checkFinalConsistency()

  def _importToSandbox(self):
    """Redefinition of FCC._importToSandbox() method.
      FCCSW needs extra folders like 'InstallArea', 'Detector'
      and extra files specified in its configuration file.

    :return: The success or the failure of the import
    :rtype: bool

    """

    if not super(FccSw, self)._importToSandbox():
      errorMessage = "Sandboxing : _importToSandbox() failed"
      self._log.error(errorMessage)
      return False

    return self._importFccswFiles()

  def _importFccswFiles(self):
    """FCCSW application needs additional files specified in the configuration file
    It also needs folders like 'InstallArea' and 'Detector'.

    :return: The success or the failure of the import
    :rtype: bool

    """
    
    #installAreaFolder already resolved and added in FccSw class
    # It is present in CVMFS
    #installAreaFolder = os.path.join(self.fccSwPath, 'InstallArea')
    detectorFolder = os.path.join(self.fccSwPath, 'Detector')

    # We do not need all the content of these folders hence the filtering
    self._foldersToFilter.add(detectorFolder)

    # Explanation
    # InstallAreaFolder : all dbg files are excluded
    # detectorFolder : only xml files are included
    self._filteredExtensions += ['.dbg', '.xml']
    self._excludesOrIncludes += [True, False]

    debugMessage = "Sandboxing : FCC configuration file reading..."
    self._log.debug(debugMessage)

    content, message = self._readFromFile(self.steeringFile)

    # If configuration file is not valid then consistency fails
    if not content:
      self._log.error(message)
      return False

    self._log.debug(message)

    # Find all additional files specified in the fccsw configuration file
    #xml_files = re.findall(r'file:(.*.xml)',content)

    txtFiles = re.findall(r'(.*) *= *"(.*.txt)"', content)
    cmdFiles = re.findall(r'(.*) *= *"(.*.cmd)"', content)

    # Upload file not commented
    txtFiles = [txtFile[1] for txtFile in txtFiles if not txtFile[0].startswith("#")]
    cmdFiles = [cmdFile[1] for cmdFile in cmdFiles if not cmdFile[0].startswith("#")]

    lookForPythia = re.findall(r'.* *= *PythiaInterface *\(', content)

    # Check if PythiaInterface is instantiated somewhere and not commented
    isPythiaGeneratorUsed = True if lookForPythia and not lookForPythia[0].startswith("#") else False

    if isPythiaGeneratorUsed and cmdFiles:
      self.randomGenerator["Pythia"] = cmdFiles
    else:
      self.randomGenerator["Gaudi"] = True
      
    # From these paths we re-create the tree in the temporary sandbox
    # with only the desired file.
    # In the configuration file, these paths are relative to FCCSW installation.
    # e.g. Generation/data/foo.xml
    
    if not self._resolveTreeOfFiles(txtFiles, '.txt'):
      errorMessage = "Sandboxing : _resolveTreeOfFiles() failed"
      self._log.error(errorMessage)
      return False
      # Do not continue remaining checks

    # We do the same now for '.cmd' files that may be specified in the configuration file
    return self._resolveTreeOfFiles(cmdFiles, '.cmd')

  def _resolveTreeOfFiles(self, files, extension):
    """FCC configuration file like 'geant_pgun_fullsim.py'
    needs files coming from FCCSW installation. The path of these files
    are hard-coded in the FCC configuration file with a relative path to FCCSW installation.

    This function aims to resolve the absolute path of each hard-coded files
    required by the configuration file.

    Once we have the absolute path of the file, we recreate the tree strucutre
    of the file in a temporary folder and copy the file to it.

    Because the original directory of the file may contain other files, in this
    way we copy only the desired file.

    The file has now a new location and this is this new tree that will be
    added to the sandbox.

    :param files: The files specified in the configuration file
    :type files: list

    :param extension: The extension of file to resolve
    :type extension: str

    :return: success or failure of checking file
    :rtype: bool

    """

    if not files:
      warnMessage = (
        "Sandboxing : FCCSW configuration file"
        " does not seem to need any additional '%(ext)s' files" % {'ext':extension}
      )
      self._log.warn(warnMessage)
      return True

    for file in files:
      # We save the relative path of the file
      # e.g. Generation/data/
      tree = os.path.dirname(file)
      # We prepend the temporary sandbox to the tree
      # which will become the new location of the file.
      treeFullPath = os.path.join(self._tempCwd, tree)

      debugMessage = (
        "Sandboxing : Tree '%(tree)s' of additionnal"
        " '%(ext)s' files creation..." % {'tree':treeFullPath, 'ext':extension}
      )
      self._log.debug(debugMessage)

      # We create the tree locally in the temporary folder
      if not os.path.exists(treeFullPath):
        try:
          os.makedirs(treeFullPath)
        except OSError as e:
          errorMessage = (
            "Sandboxing : Tree '%(tree)s' of additionnal"
            " '%(ext)s' files creation failed\n%(error)s" % {'tree':treeFullPath, 'ext':extension, 'error' : e}
          )
          self._log.error(errorMessage)
          return False

        debugMessage = (
          "Sandboxing : Tree '%(tree)s' of additionnal"
          " '%(ext)s' files creation successfull" % {'tree':treeFullPath, 'ext':extension}
        )
        self._log.debug(debugMessage)

      else:
        debugMessage = "Sandboxing : Tree '%s' already exists" % treeFullPath
        self._log.debug(debugMessage)

      # We take the first directory of the tree
      # We add this root directory to the 'final' sandbox
      rootFolder = tree.split(os.path.sep)[0]
      rootFolderFullPath = os.path.join(self._tempCwd, rootFolder)

      self._inputSandbox.add(rootFolderFullPath)

      source = os.path.realpath(os.path.join(self.fccSwPath, file))
      destination = os.path.realpath(os.path.join(self._tempCwd, file))

      if not os.path.exists(source):
        errorMessage = "Sandboxing : The file '%s' does not exist" % source
        self._log.error(errorMessage)
        return False

      # if paths already exists do not copy it
      # go to the next file  
      if os.path.exists(destination):
        debugMessage = "Sandboxing : The file '%s' already exists" % destination
        self._log.debug(debugMessage)
      else:  
        debugMessage = "Sandboxing : Additional file '%s' copy..." % source
        self._log.debug(debugMessage)

        try:
          shutil.copyfile(source, destination)
        except (IOError, shutil.Error) as e:
          errorMessage = (
            "Sandboxing : Additionnal files"
            " '%(src)s' copy failed\n%(error)s" % {'src':source, 'error':e}
          )
          self._log.error(errorMessage)
          return False

        debugMessage = (
          "Sandboxing : Additionnal files"
          " '%(src)s' copy successfull to '%(dst)s'" % {'src':source, 'dst':destination}
        )
        self._log.debug(debugMessage)

    return True

  def _setFilterToFolders(self):
    """Some folders required by FCCSW do not need to be imported with
    all their content.

    Some files have to be excluded or only some files have to be included.
    Then for each folder, we have the include/exclude parameter and the linked extension.

    This function maps the folders with their corresponding filters if there are.

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
        errorMessage = (
          "Sandboxing : _filterFolders() failed\n"
          "The folder '%(actual)s' does not exist\n"
          "Check if you're FCCSW installation is complete" % {'actual':actualFolder}
        )
        self._log.error(errorMessage)
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

  def _filterFolders(self, tempFolder, actualFolder,
                     filteredExtension, excludeOrInclude):
    """Knowing the filter for each folder, we copy the 'filtered'
    folder in the temporary folder 'temp_fcc_dirac'.

    After, we add the path of the 'filtered' folder to the sandbox instead of
    its original path.

    Like that, we do not import unnecessary files to the sandbox.
    It is like we create a 'light' copy of the orginal folder.

    If folders have to be filtered then, we do not have to add them directly to the sandbox
    because it will put all content of folders.
    
    So we copy only the content we want in the filtered folder
    inside the temporary folder 'temp_fcc_dirac'.

    After, we add the filtered folders located in 'temp_fcc_dirac' to the sandbox.

    :param tempFolder: The temporary working directory (destination) used for the sandboxing
    :type tempFolder: str

    :param actualFolder: The original (source) path of folder checked by the copy process
    :type actualFolder: str

    :param filteredExtension: The extension of file we do (not) want
    :type filteredExtension: str

    :param excludeOrInclude: extension is excluded or included
    :type excludeOrInclude: bool

    :return: The success or the failure of the filtering
    :rtype: bool

    """
    
    debugMessage = "Sandboxing : Checking of the filtered folder '%s'..." % tempFolder
    self._log.debug(debugMessage)

    if not os.path.exists(tempFolder):
      debugMessage = "Sandboxing : Creation of the filtered folder '%s'..." % tempFolder
      self._log.debug(debugMessage)

      try:
        os.makedirs(tempFolder)
      except OSError as e:
        errorMessage = (
          "Sandboxing : Creation of the filtered folder"
          " '%(temp)s' failed\n%(error)s" % {'temp':tempFolder, 'error':e}
        )
        self._log.error(errorMessage)
        return False

      debugMessage = (
        "Sandboxing : Creation of the filtered folder"
        " '%(temp)s' successfull" % {'temp':tempFolder}
      )
      self._log.debug(debugMessage)

    for path in os.listdir(actualFolder):
      source = os.path.realpath(os.path.join(actualFolder, path))
      destination = os.path.realpath(os.path.join(tempFolder, path))

      # If file then check existence
      if not os.path.exists(source):
        errorMessage = "Sandboxing : The file '%s' does not exist" % source
        self._log.error(errorMessage)
        return False
        
      if not os.path.isfile(source):
        # Recursive call for folders
        if not self._filterFolders(destination, source, filteredExtension, excludeOrInclude):
          errorMessage = "Sandboxing : _filterFolders() failed"
          self._log.error(errorMessage)
          return False
      else:
        if os.path.exists(destination):
          debugMessage = "Sandboxing : The file '%s' already exists" % source
          self._log.debug(debugMessage)
        else:
          debugMessage = "Sandboxing : File '%s' copy..." % source
          self._log.debug(debugMessage)

          if ((excludeOrInclude and not path.endswith(filteredExtension))
              or (not excludeOrInclude and path.endswith(filteredExtension))
              or not filteredExtension):

            # Copy considering filters to apply
            try:
              shutil.copyfile(source, destination)
            except (IOError, shutil.Error) as e:
              errorMessage = "Sandboxing : The copy of the file '%s' failed\n%s" % (destination, e)
              self._log.error(errorMessage)
              return False

            debugMessage = (
              "Sandboxing : Copy of the file"
              " '%(src)s' successfull to '%(dst)s'" % {'src':source, 'dst':destination}
            )
            self._log.debug(debugMessage)

    debugMessage = "Sandboxing : Folder '%s' filtering successfull" % tempFolder
    self._log.debug(debugMessage)
    return True

class FccAnalysis(Fcc):
  """Definition of an FCCAnalysis application.
  By default, it runs FCCPHYSICS.

  Usage:

  >>> FCC_PHYSICS = FccAnalysis(
      fccConfFile='/cvmfs/fcc.cern.ch/sw/0.7/fcc-physics/0.1/x86_64-slc6-gcc49-opt/share/ee_ZH_Zmumu_Hbb.txt'
    )

  """

  def __init__(self, executable='fcc-pythia8-generate', fccConfFile=""):

    super(FccAnalysis, self).__init__()

    self.steeringFile = fccConfFile
    self.fccExecutable = executable

    # If it is a different executable
    # then it is :
    # - fcc-physics-read-delphes or
    # - fcc-physics-read
    # So find card file and set the seed !
     
    if executable != 'fcc-pythia8-generate':
      self.read = True
    else:
      self.randomGenerator = {"Pythia":[os.path.basename(fccConfFile)]}

  def _setFilterToFolders(self):
    """FccAnalysis does not need extra folders to filter

    :return: The success value
    :rtype: bool

    """
    self._log.debug("Sandboxing : FccAnalysis does not need extra folders to filter")
    return True
