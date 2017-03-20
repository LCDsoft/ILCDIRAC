#************************** libraries importation **************************#

# standard libraries
import os
import re
import shutil
import types


# If user need files on EOS, it should put EOS full path (xrootd://...)
# else if the file is in eospublic, user can also put relative path to eospublic (/eos/user/...)
# because we take care of that, indeed we download the given relative EOS path to the sandbox
# however, inside the job, you have to call the basename path 
    
# Xrootd python API used for EOS 
# Here we just make an 'existence' checking

from XRootD import client
from XRootD.client.flags import DirListFlags, OpenFlags, MkDirFlags, QueryCode


__RCSID__ = "$Id$"

# DIRAC libraries
from ILCDIRAC.Interfaces.API.NewInterface.LCApplication import LCApplication
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Workflow.Parameter import Parameter

#************************************* Classes - Definition ******************************#


#************************************************************#
# Class : FCC()                                              #
# role : Like existing applications such Marlin, Pythia ...  #
# It defines the skeleton of any FCC applications            #
# its settings and modules in order to run FCC  softwares    #
#************************************************************#

class FCC(LCApplication):
    
    #*******************************************************************#
    # Function name : __init__                                          #
    # role : it specifies location of execution module, its name        #
    # and parameters                                                    #
    #*******************************************************************#
    
    def __init__(self,fcc_output_file, fcc_input_files , number_of_events , extraInputs, extraOutputs):

        
        # Attributes starting with '_' are not usefull for execution module
        # Underscore used here to make distinction and give an 'invisibility'
        # to the parameters sent to the execution module

        # required
        self.fcc_executable = ''   
        self.fcc_conf_file = ''
           
        self.fcc_output_file = fcc_output_file
        self.fcc_input_files = fcc_input_files
 
        self.number_of_events = number_of_events 

        # transform str to list for future iteration 
        # like that user can provide a list of paths or a single path as string 
        # else
        # transform list to set to avoid duplicates if there are duplicates
        # (DIRAC already do this check before sending the sandbox)
        self._extraInputs = [extraInputs] if str is type(extraInputs) else set(extraInputs)

        self._extraOutputs = [extraOutputs] if str is type(extraOutputs) else set(extraOutputs)


        # fccsw path of FCCSW software    
        self._fccsw_path = ''
        
        # pre-input sandbox of folders
        self._folders_to_upload = []

        # folder filters
        self._filtered_extensions = [] 
        self._excludes_or_includes = []
        
        # EOS public location
        self._EOS_MGM_URL= 'root://eospublic.cern.ch'
        self._myclient = client.FileSystem(self._EOS_MGM_URL + ':1094')

        # temporary local sandbox folder
        self._temp_cwd = os.path.join(os.getcwd(),'fcc_temp_dirac') 

        # pre-input sandbox
        self.paths  = []
    
        # pre-output sandbox
        self._outputSB = []

        #************************* Do we keep this ? *******************************************

        #self.outputDstPath = ''
        #self.outputDstFile = ''
        #self.outputRecPath = ''
        #self.outputRecFile = ''
        #self.processorsToUse = []
        #self.processorsToExclude = []
        #self.datatype = 'REC'
        #self.detectortype = 'ILD'
      


        super(FCC, self).__init__()
        # Those 5 need to come after default constructor
        self._modulename = 'FCCAnalysis'
        #self._importLocation = 'ILCDIRAC.Workflow.Modules'
        self._moduledescription = 'Module to run FCC'
        self.jobName = "fcc"
        self.version = "vXYZ"
        self.platform = 'x86_64-slc5-gcc43-opt'

    # ! PUT ABSOLUTE PATH HERE (AFS,EOS...) BUT RELATIVE PATH INSIDE THE JOB !
    # Example of how to add 'extra' files or folders that can be used by the job on the CE 
    # fccsw.add_paths(['/afs/cern.ch/user/<YOUR_INITIAL>/<YOUR_USERNAME>/foo.bar','/afs/cern.ch/user/<YOUR_INITIAL>/<YOUR_USERNAME>/HelloWorld'])
    def _add_paths(self,paths):
        self.paths += [paths] if str is type(paths) else set(paths) 




    #************************* Do we keep this ? *******************************************    

    

    def setOutputRecFile(self, outputRecFile, path = None):
        """Optional: Define output rec file for Marlin. Used only in production
        context. Use :func:`UserJob.setOutputData
        <ILCDIRAC.Interfaces.API.NewInterface.UserJob.UserJob.setOutputData>` if you
        want to keep the file on the grid.

        :param string outputRecFile: output rec file for Marlin
        :param string path: Path where to store the file.

        """
        self._checkArgs( { 'outputRecFile' : types.StringTypes } )
        self.outputRecFile = outputRecFile
        self.prodparameters[self.outputRecFile] = {}
        self.prodparameters[self.outputRecFile]['datatype'] = 'REC'
        if path:
          self.outputRecPath = path

    def setOutputDstFile(self, outputDstFile, path = None):
        """Optional: Define output dst file for Marlin.  Used only in production
        context. Use :func:`UserJob.setOutputData
        <ILCDIRAC.Interfaces.API.NewInterface.UserJob.UserJob.setOutputData>` if you
        want to keep the file on the grid.

        :param string outputDstFile: output dst file for FCC
        :param string path: Path where to store the file.

        """
        self._checkArgs( { 'outputDstFile' : types.StringTypes } )
        self.outputDstFile = outputDstFile
        self.prodparameters[self.outputDstFile] = {}
        self.prodparameters[self.outputDstFile]['datatype'] = 'DST'
        if path:
          self.outputDstPath = path

    def setProcessorsToUse(self, processorlist):
        """  Define processor list to use

        Overwrite the default list (full reco). Useful for users willing to do dedicated analysis (TPC, Vertex digi, etc.)

        >>> ma.setProcessorsToUse(['libMarlinTPC.so','libMarlinReco.so','libOverlay.so','libMarlinTrkProcessors.so'])

        :param processorlist: list of processors to use
        :type processorlist: python:list
        
        """
        self._checkArgs( { 'processorlist' : types.ListType } )
        self.processorsToUse = processorlist

    def setProcessorsToExclude(self, processorlist):
        """ Define processor list to exclude

        Overwrite the default list (full reco). Useful for users willing to do dedicated analysis (TPC, Vertex digi, etc.)

        >>> ma.setProcessorsToExclude(['libLCFIVertex.so'])

        :param processorlist: list of processors to exclude
        :type processorlist: python:list
        """
        self._checkArgs( { 'processorlist' : types.ListType } )
        self.processorsToExclude = processorlist

    def _userjobmodules(self, stepdefinition):
        res1 = self._setApplicationModuleAndParameters(stepdefinition)
        res2 = self._setUserJobFinalization(stepdefinition)
        if not res1["OK"] or not res2["OK"] :
          return S_ERROR('userjobmodules failed')
        return S_OK()

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

    

    #********************************************************************#
    # Function name : _checkConsistency                                  #
    # role : it checks minimum requirements required by FCC applications #
    # and update sandbox with files required by the FCC application      #
    #********************************************************************#
    
    def _checkConsistency(self, job=None):

        consistency_result = S_OK()

        if '' == self.version or  '' == self.platform :
          return S_ERROR('Version, Name and Platform have to be set!')        
 

        fcc_app_name = self.__class__.__name__


        fcc_app_log = fcc_app_name + '.log'


        #**************************************************MINIMUM REQUIREMENTS CHECKING*******************************************************#
        if '' == self.fcc_executable or '' == self.fcc_conf_file  :
            message = "\nError in parsing this application : " + fcc_app_name
            message += "\nYou have to provide at least an executable and a configuration file for each application\n"
            return S_ERROR(message)    
            
        #**************************************************MINIMUM REQUIREMENTS CHECKING*******************************************************#



        # We add the log to the ouput sandbox
        # (I think it is already done by DIRAC)
        self._outputSB += [fcc_app_log]

        # All local input files are checked before added to the sandbox
        self.paths += [self.fcc_conf_file]  


        # We add extra (in/out)puts to the pre-sandbox
        self.paths += self._extraInputs

        self._outputSB += self._extraOutputs

        # These files are already added by default by DIRAC to the sandbox
        #if 'wms' == mode:
        #self._outputSB +=['localEnv.log','std.out']


        # We update the sandbox according to the specification of the application
        consistency_result = self._update_sandbox(self._fccsw_path,self.paths,self.fcc_conf_file)
        

        # This output can be used as input for another application       
        if '' != self.fcc_output_file:
            self.setOutputFile(self.fcc_output_file)


        self.setLogFile(fcc_app_log)

        # Before submitting the DIRAC job, we copy and treat all folders of the sandbox
        # in a temporary working directory
        self._copy_sandbox_subfolders() 

        # Sandbox can be set up at the application level or at the job level
        # Sandbox files coming from application or the job are all picked
        # and putting on the same final destination which is a list of paths 
        # in the JDL file (see Input Sandbox parameter)
        # no need to re-add them again at the job level
        #if not job is None:
        #    job.inputsandbox += self.inputSB
        #    job.outputsandbox += self._outputSB

        print '\n**********************************************FCC RESUME*********************************************\n'
        
        
        print "\nYou plan to submit the following application with its corresponding log :\n"
        print fcc_app_name ,' -- log --> ' ,fcc_app_log
    



        if self.inputSB:                
            print '\nHere is the content of its input sandbox :\n'
            print '\n'.join(self.inputSB) 
                    

        if self._outputSB:
    
            print '\nHere is the content of its output sandbox :\n'
            print '\n'.join(self._outputSB) , '\n'
        

        
        
        #************************* Do we keep this ? *******************************************

        #if not self._jobtype == 'User' :
        #  if not self.outputFile:
        #    self._listofoutput.append({"outputFile":"@{outputREC}", "outputPath":"@{outputPathREC}",
        #                               "outputDataSE":'@{OutputSE}'})
        #    self._listofoutput.append({"outputFile":"@{outputDST}", "outputPath":"@{outputPathDST}",
        #                               "outputDataSE":'@{OutputSE}'})
        #  self.prodparameters['detectorType'] = self.detectortype
        #  self.prodparameters['executable'] = self.executable
        #  self.prodparameters['configuration_file'] = self.configuration_file
        #  self.prodparameters['environment_script'] = self.environment_script
        #  self.prodparameters['fccsw_path'] = self.fccsw_path

        return consistency_result




    #************************************************#
    # Function name : _applicationModule             #
    # role : it passes parameter names of the module #
    # FCC to the execution module FCCAnalysis        #
    #************************************************#
    
    def _applicationModule(self):

        md1 = self._createModuleDefinition()
        
        # We 'transfer' all attributes (the 'public' ones) to the module FCCAnalysis
        # like that, we do not need to call md1.addParameter() for each attribute we want
        # If we plan to use a dictionnary keys instead of class attributes
        # It is possible to pass this dictionnary from user script (client side) to FCC class (server side)
        # and we do not iterate over class attributes but over dictionnary keys
        # like that, we can add dynamically 'attributes' without adding them statically on these classes before
        # which will reduce stuff like (modifying classes of the client, classes of the server
        # and merging the new classes to the gitlab repository of ILCDirac) 

        for attribute in dir(self):
            #attribute starting with _ are not usefull for execution module
            if not attribute.startswith('_'):
                md1.addParameter(Parameter(attribute,              '', "string", "", "", False, False,attribute))
            

        #************************* Do we keep this ? *******************************************
    
            
        #md1.addParameter(Parameter("configuration_file",              '', "string", "", "", False, False,
        #                       "configuration_file"))
        
        
        #md1.addParameter(Parameter("executable",              '', "string", "", "", False, False,
        #                       "executable"))
        #md1.addParameter(Parameter("environment_script",              '', "string", "", "", False, False,
        #                       "environment_script"))
        #md1.addParameter(Parameter("fccsw_path",              '', "string", "", "", False, False,
        #                           "fccsw_path"))
                                                              
        #md1.addParameter(Parameter("ProcessorListToUse",     [],   "list", "", "", False, False,
        #                       "List of processors to use"))
        #md1.addParameter(Parameter("ProcessorListToExclude", [],   "list", "", "", False, False,
        #                       "List of processors to exclude"))
        #md1.addParameter(Parameter("debug",               False,   "bool", "", "", False, False,
        #                       "debug mode"))
        
        return md1

    #*************************************************#
    # Function name : _applicationModuleValues        #
    # role : it passes parameter values of the module #
    # FCC to the execution module FCCAnalysis         #
    #*************************************************#
    
    def _applicationModuleValues(self, moduleinstance):

        # We 'transfer' all attributes (the 'public' ones) to the module FCCAnalysis
        # like that, we do not need to call md1.addParameter() for each attribute we want
        # If we plan to use a dictionnary keys instead of class attributes
        # It is possible to pass this dictionnary from user script (client side) to FCC class (server side)
        # and we do not iterate over class attributes but over dictionnary keys
        # like that, we can add dynamically 'attributes' without adding them statically on these classes before
        # which will reduce stuff like (modifying classes of the client, classes of the server
        # and merging the new classes to the gitlab repository of ILCDirac) 


        for attribute in dir(self):
            #attribute starting with _ are not usefull for execution module
            if not attribute.startswith('_'):
                attribute_name = 'self.' + attribute
                attribute_value = str(eval(attribute_name))
                moduleinstance.setValue(attribute,attribute_value)

            

        #moduleinstance.setValue("configuration_file",              self.configuration_file)
        #moduleinstance.setValue("executable",              self.executable)
        #moduleinstance.setValue("environment_script",              self.environment_script)
        #moduleinstance.setValue("fccsw_path",              self.fccsw_path)

        #moduleinstance.setValue('ProcessorListToUse',     self.processorsToUse)
        #moduleinstance.setValue('ProcessorListToExclude', self.processorsToExclude)
        #moduleinstance.setValue("debug",                  self.debug)

    def _checkWorkflowConsistency(self):
        
        return self._checkRequiredApp()

    def _resolveLinkedStepParameters(self, stepinstance):
        if type(self._linkedidx) == types.IntType:
          self._inputappstep = self._jobsteps[self._linkedidx]
        if self._inputappstep:
          stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
        return S_OK()
        
    #********************************************#
    # Function name : _read_from_file            #
    # role : read a file and return its content  #
    #********************************************#

    def _read_from_file(self,file_name):

        try:
            
            with open(file_name, 'r') as f:
                content = f.read()
            return content
        except:
            return False
    
    #*********************************************************************************#
    # Function name : _create_temp_tree_from_files                                    #
    # role : given a relative tree path of file to the local FCCSW folder of the user #
    # these paths are in the configuration file e.g. geant_pgun_fullsim.py            #
    # it looks for this file, recreates the relative tree in a temporary folder       # 
    # containing only this file and not all files of the source folder                #
    # Finally, the '1 file' folder will be added to the DIRAC sandbox                 #
    #*********************************************************************************#
    
    def _create_temp_tree_from_files(self,files,fccsw_path):
        
                    
        if files:
            for file in files:
                tree = os.path.dirname(file)
                tree_full_path = os.path.join(self._temp_cwd,tree)
                if not os.path.exists(tree_full_path):    
                    os.makedirs(tree_full_path)
                root_folder = tree.split(os.path.sep)[0] 
                root_folder_full_path = os.path.join(self._temp_cwd,root_folder)

                if root_folder_full_path not in self._folders_to_upload:
                    self.inputSB += [root_folder_full_path]
                
                source = os.path.join(fccsw_path,file)
                destination = os.path.join(self._temp_cwd,file)
                
                if not os.path.exists(source):
                    message = '\nThe file : ' +  source + ' does not exist\n'
                    return S_ERROR(message)
                else:     
                    shutil.copyfile(source,destination)
                    
        return S_OK()



    #*****************************************************#
    # Function name : _copy                               #
    # role : it copies entire folders required by the job #
    # considering applied filters                         # 
    #*****************************************************#
        
    def _copy(self,temp_folder,actual_folder,tar_extension,filtered_extension,exclude_or_include):


        
        if not os.path.exists(temp_folder):    
            os.makedirs(temp_folder)
                
        for path in os.listdir(actual_folder):
            abs_path_src = os.path.join(actual_folder,path)
            abs_path_dst = os.path.join(temp_folder,path)

            if not os.path.isfile(abs_path_src):
                # Recursive call for folders
                self._copy(abs_path_dst,abs_path_src,tar_extension,filtered_extension,exclude_or_include)
            else: 
                if (exclude_or_include and not path.endswith(filtered_extension)) or (not exclude_or_include and path.endswith(filtered_extension)) or not filtered_extension:
                    shutil.copyfile(abs_path_src,abs_path_dst)



    #*************************************************#
    # Function name : _XRootDStatus2Dictionnary       #
    # input : xroot status                            #
    # role : parse status object generated by         #
    # xrootd when looking for EOS path                #
    #*************************************************#


    def _XRootDStatus2Dictionnary(self,XRootDStatus):

        start = '<'
        end = '>'

        XRootDStatus2str = str(XRootDStatus)
        s = XRootDStatus2str


        #print XRootDStatus
        # check the expected line
        try:
            
            status_str = re.search('%s(.*)%s' % (start, end), s).group(1)
            
            #print status_str
        
            status_list = status_str.split(",")

            status_dict = {}

            #print status_list
   
            # == 2 , ensure that split result to a (key,value) format for the dictionnary
            status_dict = dict(  (info.split(': ')) for info in status_list if ':' in info and len(info.split(': '))==2)

            return status_dict , S_OK()   
        except:
            msg = "EOS Path checking failed, please enter a valid path"
            return S_ERROR(msg) 
            
            
    #*************************************************#
    # Function name : _find_eos_file                  #
    # input : file_name                               #
    # role : check if file exists on EOS              #
    # before sending the job to DIRAC                 #
    #*************************************************#


    def _find_eos_file(self,file_name):

        eos_file_full_path = self._EOS_MGM_URL + '/' + file_name

        with client.File() as eosFile:
            file_status = eosFile.open(eos_file_full_path,OpenFlags.UPDATE)


        # Problem with file created directly on eos
        # no problem with uploded files with xrdcp cmd

        #print eos_file_full_path
        #print file_status
        status, consistency_result = self._XRootDStatus2Dictionnary(file_status)

        if (' ok' in status and not status[' ok']) or not status:
            return file_name,False, consistency_result
        else:
            return eos_file_full_path,True, consistency_result

    #*************************************************#
    # Function name : _find_eos_folder                #
    # input : folder_name                             #
    # role : check if folder exists on EOS            #
    # before sending the job to DIRAC                 #
    #*************************************************#


    def _find_eos_folder(self,folder_name):
        
        eos_folder_full_path = self._EOS_MGM_URL + '/' + folder_name

        status, listing = self._myclient.dirlist(folder_name, DirListFlags.STAT)
   
        if None == listing:
            return folder_name,False 
        else:
            return  eos_folder_full_path,True       
   
    


    #**************************************************#
    # Function name : _find_path                       #
    # input : file_name                                #
    # role : check if file/folder exists on AFS,CVMFS  #
    # before checking on EOS                           #
    #**************************************************#

    def _find_path(self,path,file_or_dir='file'):
        
        consistency_result =  S_OK()

        path = os.path.abspath(path)    

        # We suppose that the user enter absolute or relative AFS path
        # or only absolute EOS/CVMFS path

        if not path.startswith('/eos/'):
        # AFS paths are absolute or relative
        # Some software are stored in AFS
        # And users generally submit their job from lxplus
            if os.path.exists(path):
                return path, True , consistency_result    
            else:
                return path,False ,consistency_result

        # Absolute path
        elif path.startswith('/eos/'):
            # EOS paths
            
            file_path , is_file_exist, consistency_result = self._find_eos_file(path)
        
            folder_path , is_folder_exist = self._find_eos_folder(path) 
            
            if is_file_exist:
                return file_path , is_file_exist, consistency_result
            elif is_folder_exist:    
                return folder_path , is_folder_exist, consistency_result
            else:
                return path,False, consistency_result

    
    #********************************************************#
    # Function name : _upload_sandbox_with_application_files #
    # role : upload all extra folders or files               #
    # specified by the user for an application               #
    #********************************************************# 
    
    def _upload_sandbox_with_application_files(self,paths):
    
        upload_path_message = " does not exist\nPlease ensure that your path exist in an accessible file system (EOS, AFS or CVMFS)\n"

        for path in paths:
        
            if '' != path:
                # We made a pre-checking of EOS/AFS/CVMFS files
                path , is_exist, consistency_result = self._find_path(path)

                # if EOS problem, consistency fails
                if 'OK' in consistency_result and not consistency_result['OK'] :
                    return consistency_result

                # if file does not exist consistency fails
                if not is_exist:
                    message = "\nThe path '" + path + "'" + upload_path_message
                    print message
                    return S_ERROR(message)
                else:
                    # AFS files need to be uploaded but not EOS and CVMFS files, they can be accessed from the grid, remotely
                    # EOS full path returned is root://eospublic...
                    if not (path.startswith('root') or  path.startswith('/cvmfs/')):
                    
                        if path.startswith('/afs/'):
                            message = '\nWARNING : You plan to upload :' + path + ' which is stored on AFS'
                            message += 'STORING FILES ON AFS IS DEPRECATED\n'
                            print message

                            # We log the message into warning level 
                            self._log.warn(message)

                        # Files are directly added to the sandbox  
                        if os.path.isfile(path):    
                            self.inputSB += [path]
                            # Folders are copied before in a temporary sandbox
                            # They are treated before sending to the sandbox
                        else :
                            self._folders_to_upload += [path]

        return S_OK()

    #*************************************************************#
    # Function name : _upload_sandbox_with_fccsw_files            #
    # role : upload files called in FCCSW configuration file      #
    # and needed folders relative to FCCSW (InstallArea,Detector) #
    #*************************************************************#      
  
    def _upload_sandbox_with_fccsw_files(self,fccsw_path,fcc_conf_file):
  

        InstallArea_folder = os.path.join(fccsw_path,'InstallArea')
        
        Detector_folder = os.path.join(fccsw_path,'Detector')
        
    
        fccsw_folders = [InstallArea_folder,Detector_folder]            

            

        # Explanation
        # InstallArea_folder : dbg files are excluded
        # Detector_folder : only xml files are included
        self._filtered_extensions += ['.dbg','.xml']
        
        self._excludes_or_includes += [True,False]
        
        content = self._read_from_file(fcc_conf_file)

        # If configuration file is not valid, consistency fails
        if not content :
            message = "\nError in reading configuration file :\n" + fcc_conf_file
            return S_ERROR(message)


        # List all paths given in the content of the configuration file            
        #xml_files = re.findall(r'file:(.*.xml)',content)

        txt_files = re.findall(r'="(.*.txt)',content)

        cmd_files = re.findall(r'filename="(.*.cmd)',content)

        #print txt_files
        #print cmd_files
        
        # From these paths we recreate the tree in the temporary sandbox
        # because they are called from a path relative to FCCSW folder
        # e.g. Generation/data/foo.xml
        consistency_result = self._create_temp_tree_from_files(txt_files,fccsw_path)

        # If file does not exist, consistency fails
        if 'OK' in consistency_result and not consistency_result['OK'] :
            return consistency_result
            # do not continue remaining checks    

        consistency_result = self._create_temp_tree_from_files(cmd_files,fccsw_path)
        
        self._folders_to_upload = fccsw_folders
                
        return consistency_result


    #********************************************************#
    # Function name : _update_sandbox                        #
    # role : check the files and folders required by the job #
    # and add them to the sandbox                            #
    # Indeed, it calls upload_sandbox_with_* functions       #
    #********************************************************#    
    
    def _update_sandbox(self,fccsw_path,paths,fcc_conf_file):

        
        # First, it creates a temporary local directory for the sandbox        
        if not os.path.exists(self._temp_cwd):
            os.makedirs(self._temp_cwd)

        # Update sandbox with application files
        if paths:                        
            consistency_result = self._upload_sandbox_with_application_files(paths) 

            # If uploading files fails for some reasons (see functions for more details)
            # then consistency fails
            if 'OK' in consistency_result and not consistency_result['OK'] :
                return consistency_result
                # do not continue remaining checks

        # Update sandbox with FCCSW application files
        if '' != fccsw_path:
            consistency_result = self._upload_sandbox_with_fccsw_files(fccsw_path,fcc_conf_file)
    
        return consistency_result
        
    #******************************************************************#
    # Function name : _copy_sandbox_subfolders                         #
    # role : copy in the temporary sandbox, folders needed by the job  #
    #******************************************************************#
            
    def _copy_sandbox_subfolders(self):
    
        copied_folders = []
            
        for idx,actual_folder in enumerate(self._folders_to_upload):    
            

            tar_extension = '.tgz'

            if idx < len(self._filtered_extensions):
                filtered_extension = self._filtered_extensions[idx]
                exclude_or_include = self._excludes_or_includes[idx]
            else:
                filtered_extension = False
                exclude_or_include = False
                
            temp_folder = os.path.join(self._temp_cwd,os.path.basename(actual_folder)) 

            # DIRAC already compressed the sandbox before submitting the job
            self._copy(temp_folder,actual_folder,tar_extension,filtered_extension,exclude_or_include)

            copied_folders += [temp_folder]
        
        
        if copied_folders:  
            self.inputSB += copied_folders          



#************************************************************#
# Class : FCCSW()                                            #
# role : It defines an FCCSW application                     #
#************************************************************#

class FCCSW(FCC): 


    def __init__(self, fcc_conf_file = "",fcc_output_file = "", fcc_input_files = "", fccsw_path = "", number_of_events = "", extraInputs = (), extraOutputs = ()):
        
        FCC.__init__(self,fcc_output_file, fcc_input_files , number_of_events , extraInputs, extraOutputs)

        self._fccsw_path = fccsw_path

    
        # input checking before DIRAC APPLICATION CONSISTENCY CHECKING
        #**************************************************MINIMUM REQUIREMENTS CHECKING*******************************************************#
        if '' == fccsw_path  or '' == fcc_conf_file:
            message = "\nError in parsing FCCSW application :\n"
            message += "\nYou have to provide the path of the 'InstallArea' folder and a valid configuration file relative to FCCSW installation path\n"
            print message   
            quit()
        #**************************************************MINIMUM REQUIREMENTS CHECKING*******************************************************#

        self.fcc_conf_file = os.path.join(fccsw_path,fcc_conf_file)
                


        # Stuff to call gaudirun.py
        python = 'python'
        xenv = '`which xenv`'
        arg_xenv  = 'InstallArea/FCCSW.xenv'
        exe = 'gaudirun.py' 
                    
        self.fcc_executable = 'exec ' + python + ' ' + xenv + ' --xml ' + arg_xenv + ' ' + exe          


#************************************************************#
# Class : FCCAnalysis()                                      #
# role : It defines an FCCAnalysis application               #
# by default, it runs FCCPHYSICS                             #
#************************************************************#


class FCCAnalysis(FCC): 


    def __init__(self, executable = 'fcc-pythia8-generate',fcc_conf_file = "", fcc_output_file = "", fcc_input_files = "", number_of_events = "", extraInputs = (), extraOutputs = ("*.log",'*.root')):
        
        FCC.__init__(self,fcc_output_file, fcc_input_files , number_of_events , extraInputs, extraOutputs)

        self.fcc_conf_file = fcc_conf_file

        self.fcc_executable = executable    










                  
                
                    

