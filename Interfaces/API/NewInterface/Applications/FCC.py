"""This is the core of an FCC application
   It defines FCC applications and their parameters

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

from XRootD import client
from XRootD.client.flags import DirListFlags, OpenFlags

"""
If user needs files on EOS, it should put EOS full path (xrootd://...)
in input files
else if the file is in eospublic, user can also put relative path to eospublic (/eos/user/...)
because we take care of that, indeed we download the given EOS path to the sandbox
however, inside the job, you have to give the basename path (only filename)

Xrootd python API used for EOS
Here, we use xrootd python API to check EOS paths existence only"""

__RCSID__ = "$Id$"


class FCC(LCApplication):
    """Like existing applications such Marlin, Pythia ...
       It defines the skeleton of an FCC application,
       its settings and module to run FCC softwares

    """

    def __init__(self, fcc_output_file, fcc_input_files,
                 number_of_events, extraInputs, extraOutputs):
        """It specifies location of the execution module, its name and parameters

        :param fcc_output_file: The output file of the application (data)
        :param fcc_input_files: The input files of the application (data)
        :param number_of_events: The number of events
        :param extraInputs: The local input files required by the job (input sandbox)
        :param extraOutputs: The local output files (output sandbox)

        :type fcc_output_file: str
        :type fcc_input_files: list
        :type number_of_events: str
        :type extraInputs: list
        :type extraOutputs: list

        """

        """
        Attributes starting with '_' are not usefull for the execution module (FCCAnalysis)
        Underscore used here to make distinction and give an 'invisibility'
        to the attributes we do not want pass as parameters for the execution module"""

        # required
        self.fcc_executable = ''
        self.fcc_conf_file = ''

        self.fcc_output_file = fcc_output_file
        self.fcc_input_files = fcc_input_files

        self.number_of_events = number_of_events

        """
        transform str to list for future iteration
        like that user can provide a list of paths or a single path as string
        else
        transform list to set to avoid duplicates if there are duplicates
        (DIRAC already do this check before sending the sandbox)"""

        self._extra_inputs = [extraInputs] if isinstance(extraInputs, str) else set(extraInputs)

        self._extra_outputs = [extraOutputs] if isinstance(extraOutputs, str) else set(extraOutputs)


        # fccsw path of FCCSW software
        self._fccsw_path = ''

        # pre-input sandbox of folders
        self._folders_to_upload = []

        # folder filters
        self._filtered_extensions = []
        self._excludes_or_includes = []

        # EOS public location
        self._eos_mgm_url = 'root://eospublic.cern.ch'
        self._myclient = client.FileSystem(self._eos_mgm_url + ':1094')

        # temporary local sandbox folder
        self._temp_cwd = os.path.join(os.getcwd(), 'fcc_temp_dirac')

        # pre-input sandbox
        self.paths = []

        # pre-output sandbox
        self._output_sandbox = []

        self.datatype = 'REC'
        self.detectortype = 'ILD'

        super(FCC, self).__init__()
        # Those 5 need to come after default constructor
        self._modulename = 'FCCAnalysis'
        #self._importLocation = 'ILCDIRAC.Workflow.Modules'
        self._moduledescription = 'Module to run FCC'
        self.job_name = "fcc"
        self.version = "vXYZ"
        self.platform = 'x86_64-slc5-gcc43-opt'

        """
        PUT ABSOLUTE PATH HERE (AFS,EOS...) BUT RELATIVE PATH INSIDE THE JOB
        Example of how to add 'extra' files or folders that can be used by the job on the CE
        fccsw._add_paths(['/my/path/foo.bar','/my/path/HelloWorld'])"""

    def _add_paths(self, paths):
        self.paths += [paths] if isinstance(paths, str) else set(paths)

    #************************* Do we keep this ? - START *******************************************

    def _userjobmodules(self, stepdefinition):
        res1 = self._setApplicationModuleAndParameters(stepdefinition)
        res2 = self._setUserJobFinalization(stepdefinition)
        if not res1["OK"] or not res2["OK"]:
            return S_ERROR('userjobmodules failed')
        return S_OK()

    def _prodjobmodules(self, stepdefinition):

        ## Here one needs to take care of listoutput
        if self.outputPath:
            self._listofoutput.append({'OutputFile':'@{OutputFile}', "outputPath":"@{OutputPath}",
                                       "outputDataSE":'@{OutputSE}'})

        res1 = self._setApplicationModuleAndParameters(stepdefinition)
        res2 = self._setOutputComputeDataList(stepdefinition)
        if not res1["OK"] or not res2["OK"]:
            return S_ERROR('prodjobmodules failed')
        return S_OK()

    #************************* Do we keep this ? - END *******************************************

    def _checkConsistency(self, job=None):
        """It checks minimum requirements of the FCC application
        and update sandbox with files required by the FCC application

        :param job: The job containing the application
        :type job: DIRAC.Interfaces.API.Job.Job

        :return: The success or failure of the consistency
        :rtype: DIRAC.S_OK, DIRAC.S_ERROR

        """

        consistency_result = S_OK()

        if self.version == '' or  self.platform == '':
            return S_ERROR('Version, Name and Platform have to be set!')

        fcc_app_name = self.__class__.__name__
        fcc_app_log = fcc_app_name + '.log'

        if self.fcc_executable == '' or self.fcc_conf_file == '':
            message = "\nError in parsing this application : " + fcc_app_name
            message += "\nYou have to provide at least an executable"
            message += "and a configuration file for each application\n"
            return S_ERROR(message)

        # We add the log to the ouput sandbox
        # (I think it is already done by DIRAC)
        self._output_sandbox += [fcc_app_log]

        # All input files are put in paths (pre-sandbox)
        # because they are checked before being added to the sandbox
        self.paths += [self.fcc_conf_file]


        # We add extra (in/out)puts
        self.paths += self._extra_inputs

        self._output_sandbox += self._extra_outputs

        # These files are already added by default by DIRAC to the sandbox
        #if 'wms' == mode:
        #self._outputSB +=['localEnv.log','std.out']


        # We update the sandbox according to the specification of the application
        consistency_result = self._update_sandbox(self._fccsw_path, self.paths, self.fcc_conf_file)


        """
        setOutputFile informs the job that this application has an output file
        This output can be used as input for another application
        In this way, getInputFromapp method knows the ouput file of the given application"""

        if self.fcc_output_file == '':
            self.setOutputFile(self.fcc_output_file)


        self.setLogFile(fcc_app_log)

        # Before submitting the DIRAC job, we copy and treat all folders of the sandbox
        # in a temporary working directory
        self._copy_sandbox_subfolders()

        """
        Sandbox can be set up at the application level or at the job level
        Sandbox files coming from application or the job are all picked
        and putting on the same final destination which is a list of paths
        in the JDL file (see Input Sandbox parameter)
        no need to re-add them again at the job level"""

        #if not job is None:
        #    job.inputsandbox += self.inputSB
        #    job.outputsandbox += self._output_sandbox

        print('\n***********************************FCC RESUME**********************************\n')


        print("\nYou plan to submit the following application with its corresponding log :\n")
        info = fcc_app_name + ' --> ' + fcc_app_log
        print(info)

        if self.inputSB:
            print('\nHere is the content of its input sandbox :\n')
            info = '\n'.join(self.inputSB)
            print(info)

        if self._output_sandbox:

            print('\nHere is the content of its output sandbox :\n')
            info = '\n'.join(self._output_sandbox) + '\n'
            print(info)


        return consistency_result

    def _applicationModule(self):
        """It passes parameter names of the module
           FCC to the execution module FCCAnalysis

        """

        md1 = self._createModuleDefinition()

        """
        We 'transfer' all attributes (the 'public' ones) to the module FCCAnalysis
        like that, we do not need to call md1.addParameter() for each attribute we want
        If we plan to use a dictionnary keys instead of class attributes
        It is possible to pass this dictionnary from user script (client side)
        to FCC class (server side) and we do not iterate over class attributes
        but over dictionnary keys like that, we can add dynamically 'attributes'
        without adding them statically on these classes before
        which will reduce stuff like (modifying classes of the client, classes of the server
        and merging the new classes with the gitlab repository of ILCDirac)"""

        for attribute in dir(self):
            #attribute starting with _ are not usefull for execution module
            if not attribute.startswith('_'):
                md1.addParameter(Parameter(
                    attribute, '', "string", "", "", False, False, attribute))


        return md1

    def _applicationModuleValues(self, moduleinstance):
        """It passes parameter values of the module
        FCC to the execution module FCCAnalysis

        :param moduleinstance: The module we load (FCCAnalysis)

        """


        """
        We 'transfer' all attributes (the 'public' ones) to the module FCCAnalysis
        like that, we do not need to call md1.addParameter() for each attribute we want
        If we plan to use a dictionnary keys instead of class attributes
        It is possible to pass this dictionnary from user script (client side)
        to FCC class (server side)
        and we do not iterate over class attributes but over dictionnary keys
        like that, we can add dynamically 'attributes' without adding them
        statically on these classes before which will reduce stuff like
        (modifying classes of the client, classes of the server
        and merging the new classes with the gitlab repository of ILCDirac)"""


        for attribute in dir(self):
            #attribute starting with _ are not usefull for execution module
            if not attribute.startswith('_'):
                attribute_name = 'self.' + attribute
                attribute_value = str(eval(attribute_name))
                moduleinstance.setValue(attribute, attribute_value)

    def _checkWorkflowConsistency(self):

        return self._checkRequiredApp()

    def _resolveLinkedStepParameters(self, stepinstance):
        if type(self._linkedidx) == types.IntType:
            self._inputappstep = self._jobsteps[self._linkedidx]
        if self._inputappstep:
            stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
        return S_OK()

    def _read_from_file(self, file_name):
        """Read a file and return its content

        :param file_name: The path of the file to read

        :type file_name: str

        :return: The content of the file
        :rtype: str

        """

        try:
            with open(file_name, 'r') as file:
                content = file.read()
            return content
        except IOError:
            return False

    def _create_temp_tree_from_files(self, files, fccsw_path):
        """Given a relative tree path of file to the local FCCSW folder of the user
        these paths are in the configuration file e.g. geant_pgun_fullsim.py
        it looks for this file, recreates the relative tree in a temporary folder
        containing only this file and not all files of the source folder
        Finally, the '1 file' folder will be added to the DIRAC sandbox

        :param files: The files specified in the configuration file
        :param fccsw_path: The local path of FCCSW

        :type files: list
        :type fccsw_path: str

        :return: success or failure of checking file
        :rtype: DIRAC.S_OK

        """

        if files:
            for file in files:
                tree = os.path.dirname(file)
                tree_full_path = os.path.join(self._temp_cwd, tree)
                if not os.path.exists(tree_full_path):
                    os.makedirs(tree_full_path)
                root_folder = tree.split(os.path.sep)[0]
                root_folder_full_path = os.path.join(self._temp_cwd, root_folder)

                if root_folder_full_path not in self._folders_to_upload:
                    self.inputSB += [root_folder_full_path]

                source = os.path.join(fccsw_path, file)
                destination = os.path.join(self._temp_cwd, file)

                if not os.path.exists(source):
                    message = '\nThe file : ' +  source + ' does not exist\n'
                    return S_ERROR(message)
                else:
                    shutil.copyfile(source, destination)

        return S_OK()

    def _copy(self, temp_folder, actual_folder,
              filtered_extension, exclude_or_include):
        """It copies folders required by the job
        considering applied filters

        :param temp_folder: The temporary working directory (destination) used for the sandboxing
        :param actual_folder: The original (source) path of folder looked by the copy process
        :param filtered_extension: The extension of file we (do not) want
        :param exclude_or_include: extension is excluded or included

        :type temp_folder: str
        :type actual_folder: str
        :type filtered_extension: list
        :type exclude_or_include: bool

        """

        if not os.path.exists(temp_folder):
            os.makedirs(temp_folder)

        for path in os.listdir(actual_folder):
            abs_path_src = os.path.join(actual_folder, path)
            abs_path_dst = os.path.join(temp_folder, path)

            if not os.path.isfile(abs_path_src):
                # Recursive call for folders
                self._copy(abs_path_dst, abs_path_src,
                           filtered_extension, exclude_or_include)
            else:
                if ((exclude_or_include and not path.endswith(filtered_extension))
                        or (not exclude_or_include and path.endswith(filtered_extension))
                        or not filtered_extension):

                    shutil.copyfile(abs_path_src, abs_path_dst)

    def _XRootDStatus2Dictionnary(self, XRootDStatus):
        """Parse status object generated by
        xrootd API when looking for EOS path

        :param XRootDStatus: status returned by xrootd API
        :return: The existence of the path


        """

        start = '<'
        end = '>'

        XRootDStatus2str = str(XRootDStatus)


        #print XRootDStatus
        # check the expected line
        try:
            status_str = re.search('%s(.*)%s' % (start, end), XRootDStatus2str).group(1)
            #print status_str
            status_list = status_str.split(",")

            status_dict = {}

            #print status_list
            # == 2 , ensure that split result to a (key,value) format for the dictionnary
            status_dict = dict((info.split(': '))
                               for info in status_list
                               if ':' in info and len(info.split(': ')) == 2)

            return status_dict, S_OK()
        except:
            msg = "EOS Path checking failed, please enter a valid path"
            return S_ERROR(msg)

    def _find_eos_file(self, file_name):
        """Check if file exists on EOS
        before sending the job to DIRAC

        :param: The file the function looks for in EOS

        :return: The full path of the file and its existence
        :rtype: str

        """

        eos_file_full_path = self._eos_mgm_url + '/' + file_name

        with client.File() as eos_file:
            # OpenFlags.UPDATE removed to make pylint of ILCDirac tests happy
            file_status = eos_file.open(eos_file_full_path)


        # Problem with file created directly on eos
        # no problem with uploded files with xrdcp cmd

        #print eos_file_full_path
        #print file_status
        status, consistency_result = self._XRootDStatus2Dictionnary(file_status)
  
        if (' ok' in status and 'False' == status[' ok']) or not status:
            return file_name, False, consistency_result
        else:
            return eos_file_full_path, True, consistency_result

    def _find_eos_folder(self, folder_name):
        """Check if folder exists on EOS
        before sending the job to DIRAC

        :param: The folder the function looks for in EOS

        :return: The full path of the folder and its existence
        :rtype: str

        """

        eos_folder_full_path = self._eos_mgm_url + '/' + folder_name

        # DirListFlags.STAT removed to make pylint of ILCDirac tests happy
        status, listing = self._myclient.dirlist(folder_name)

        if listing is None:
            return folder_name, False
        else:
            return  eos_folder_full_path, True

    def _find_path(self, path):
        """Check if file/folder exists on AFS,CVMFS
        before checking on EOS (only if user do not
        specify xrootd protocol)

        :param path: The path to look for
        :type path: str

        :return: The full path and its existence
        :rtype: str

        """

        consistency_result = S_OK()

        path = os.path.abspath(path)

        # We suppose that the user enters absolute or relative AFS path
        # or only absolute EOS/CVMFS path

        if not path.startswith('/eos/'):
        # AFS paths are absolute or relative
        # Some software are stored in AFS
        # Users generally submit their job from lxplus machines
            if os.path.exists(path):
                return path, True, consistency_result
            else:
                return path, False, consistency_result

        # Absolute path
        elif path.startswith('/eos/'):
            # EOS paths
            file_path, is_file_exist, consistency_result = self._find_eos_file(path)
            folder_path, is_folder_exist = self._find_eos_folder(path)

            if is_file_exist:
                return file_path, is_file_exist, consistency_result
            elif is_folder_exist:
                return folder_path, is_folder_exist, consistency_result
            else:
                return path, False, consistency_result

    def _upload_sandbox_with_application_files(self, paths):
        """Upload all extra folders or files specified by the user for an application

        :param paths: The paths the application has to add to the sandbox
        :type paths: list

        :return: The success or the failure of the operation
        :rtype: DIRAC.S_OK, DIRAC.S_ERROR

        """


        upload_path_message = " does not exist\n"
        upload_path_message += "Please ensure that your path exist in an accessible file system"
        upload_path_message += "(EOS, AFS or CVMFS)\n"

        for path in paths:
            if path != '':
                # We made a pre-checking of EOS/AFS/CVMFS files
                path, is_exist, consistency_result = self._find_path(path)

                # if EOS problem then consistency fails
                if 'OK' in consistency_result and not consistency_result['OK']:
                    return consistency_result

                # if file does not exist then consistency fails
                if not is_exist:
                    message = "\nThe path '" + path + "'" + upload_path_message
                    print(message)
                    return S_ERROR(message)
                else:
                    # AFS files need to be uploaded but not EOS and CVMFS files
                    # They can be accessed from the grid, remotely
                    # EOS full path returned is root://eospublic...
                    if not (path.startswith('root') or  path.startswith('/cvmfs/')):
                        if path.startswith('/afs/'):
                            message = '\nWARNING : You plan to upload :' + path
                            message += ' which is stored on AFS'
                            message += '\nSTORING FILES ON AFS IS DEPRECATED\n'
                            print(message)

                            # We log the message in the warning level
                            self._log.warn(message)

                        # Files are directly added to the sandbox
                        if os.path.isfile(path):
                            self.inputSB += [path]
                            # Folders are copied before in a temporary sandbox
                            # They are treated before sending to the sandbox
                        else:
                            self._folders_to_upload += [path]

        return S_OK()

    def _upload_sandbox_with_fccsw_files(self, fccsw_path, fcc_conf_file):
        """Upload files called in FCCSW configuration file
        and required folders relative to FCCSW (InstallArea,Detector)

        :param fccsw_path: The local path of FCCSW
        :param fcc_conf_file: The path of the configuration file

        :type fccsw_path: str
        :type fcc_conf_file: str

        :return: The success or the failure of the operation
        :rtype: DIRAC.S_OK, DIRAC.S_ERROR

        """

        install_area_folder = os.path.join(fccsw_path, 'InstallArea')
        detector_folder = os.path.join(fccsw_path, 'Detector')
        fccsw_folders = [install_area_folder, detector_folder]


        # Explanation
        # InstallArea_folder : dbg files are excluded
        # Detector_folder : only xml files are included
        self._filtered_extensions += ['.dbg', '.xml']
        self._excludes_or_includes += [True, False]

        content = self._read_from_file(fcc_conf_file)

        # If configuration file is not valid then consistency fails
        if not content:
            message = "\nError in reading configuration file :\n" + fcc_conf_file
            return S_ERROR(message)


        # List all paths given in the content of the configuration file
        #xml_files = re.findall(r'file:(.*.xml)',content)

        txt_files = re.findall(r'="(.*.txt)', content)

        cmd_files = re.findall(r'filename="(.*.cmd)', content)

        #print txt_files
        #print cmd_files

        # From these paths we recreate the tree in the temporary sandbox
        # with only the desired file
        # In the configuration file, these paths are relative to FCCSW folder
        # e.g. Generation/data/foo.xml
        consistency_result = self._create_temp_tree_from_files(txt_files, fccsw_path)

        # If file does not exist then consistency fails
        if 'OK' in consistency_result and not consistency_result['OK']:
            return consistency_result
            # do not continue remaining checks

        consistency_result = self._create_temp_tree_from_files(cmd_files, fccsw_path)

        self._folders_to_upload = fccsw_folders

        return consistency_result

    def _update_sandbox(self, fccsw_path, paths, fcc_conf_file):
        """Check the files and folders required by the job
        and add them to the sandbox
        Indeed, it calls upload_sandbox_with_* functions

        :param fccsw_path: The local path of FCCSW
        :param paths: The paths the application has to add to the sandbox
        :param fcc_conf_file: The path of the configuration file

        :type fccsw_path: str
        :type paths: list
        :type fcc_conf_file: str

        """

        # First, it creates a temporary local directory for the sandbox
        if not os.path.exists(self._temp_cwd):
            os.makedirs(self._temp_cwd)

        # Update sandbox with application files
        if paths:
            consistency_result = self._upload_sandbox_with_application_files(paths)

            # If upload process fails for some reasons (see functions above for more details)
            # then consistency fails
            if 'OK' in consistency_result and not consistency_result['OK']:
                return consistency_result
                # do not continue remaining checks

        # Update sandbox with FCCSW application files
        if fccsw_path != '':
            consistency_result = self._upload_sandbox_with_fccsw_files(fccsw_path, fcc_conf_file)

        return consistency_result

    def _copy_sandbox_subfolders(self):
        """copy in the temporary sandbox, folders required by the job"""

        copied_folders = []

        for idx, actual_folder in enumerate(self._folders_to_upload):

            if idx < len(self._filtered_extensions):
                filtered_extension = self._filtered_extensions[idx]
                exclude_or_include = self._excludes_or_includes[idx]
            else:
                filtered_extension = False
                exclude_or_include = False

            temp_folder = os.path.join(self._temp_cwd, os.path.basename(actual_folder))

            # DIRAC already compressed the sandbox before submitting the job
            # do not do it

            self._copy(temp_folder, actual_folder,
                       filtered_extension, exclude_or_include)

            copied_folders += [temp_folder]

        if copied_folders:
            self.inputSB += copied_folders


class FCCSW(FCC):
    """It defines an FCCSW application"""

    def __init__(self, fcc_conf_file="", fcc_output_file="", fcc_input_files="",
                 fccsw_path="", number_of_events="", extraInputs=(), extraOutputs=()):

        super(FCCSW, self).__init__(fcc_output_file, fcc_input_files,
                                    number_of_events, extraInputs, extraOutputs)


        self._fccsw_path = fccsw_path
        self.fcc_conf_file = fcc_conf_file

    def _checkConsistency(self, job=None):

        if self._fccsw_path == ''  or self.fcc_conf_file == '':
            message = "\nError in parsing FCCSW application :\n"
            message += "\nYou have to provide the path of the 'InstallArea' folder"
            message += "and a valid configuration file relative to FCCSW installation path\n"
            print(message)
            return S_ERROR(message)

        self.fcc_conf_file = os.path.join(self._fccsw_path, self.fcc_conf_file)

        # Stuff to call gaudirun.py
        python = 'python'
        xenv = '`which xenv`'
        arg_xenv = 'InstallArea/FCCSW.xenv'
        exe = 'gaudirun.py'

        self.fcc_executable = 'exec ' + python + ' ' + xenv + ' --xml ' + arg_xenv + ' ' + exe

        return super(FCCSW, self)._checkConsistency()


class FCCAnalysis(FCC):
    """It defines an FCCAnalysis application, by default, it runs FCCPHYSICS"""

    def __init__(self, executable='fcc-pythia8-generate', fcc_conf_file="", fcc_output_file="",
                 fcc_input_files="", number_of_events="", extraInputs=(), extraOutputs=()):

        super(FCCAnalysis, self).__init__(fcc_output_file, fcc_input_files,
                                          number_of_events, extraInputs, extraOutputs)

        self.fcc_conf_file = fcc_conf_file

        self.fcc_executable = executable
