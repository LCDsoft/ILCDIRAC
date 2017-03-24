"""This is the execution module loaded by FCC
   It runs the asked FCC application

   - FCCSW
   - FCCAnalysis

"""

# standard libraries
import os
import stat
import subprocess

# DIRAC libraries
__RCSID__ = "$Id$"

from ILCDIRAC.Workflow.Modules.ModuleBase import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import getEnvironmentScript
from DIRAC import gLogger, S_OK, S_ERROR

"""
If user needs files on EOS, it should put EOS full path (xrootd://...)
in input files
else if the file is in eospublic, user can also put relative path to eospublic (/eos/user/...)
because we take care of that, indeed we download the given EOS path to the sandbox
however, inside the job, you have to give the basename path (only filename)

Xrootd python API used for EOS
Here, we use xrootd python API to download EOS paths ot the CE"""

from XRootD import client
from XRootD.client.flags import DirListFlags, OpenFlags


class FCCAnalysis(ModuleBase):
    """Generate a script that will run the FCC application considering module parameters"""

    def __init__(self):

        super(FCCAnalysis, self).__init__()

        self.enable = True


        self.fcc_executable = ''
        self.fcc_conf_file = ''
        self.fcc_input_files = ''
        self.fcc_output_file = ''
        self.fccsw_path = ''
        self.number_of_events = ''
        self.environment_script = ''
        self.paths = []


        self.job_name = ''
        self.version = ''
        self.platform = ''
        
        self.debug = True
        self.log = gLogger.getSubLogger("FCCAnalysis")

        self.script_name = os.path.join(os.getcwd(), 'user_temp_job.sh')


        # EOS public location
        self.eos_mgm_url = 'root://eospublic.cern.ch:1094'
        self.myclient = client.FileSystem(self.eos_mgm_url)

    def execute(self):
        """Main method called by the agent"""


        # Try to locate environment script in 'dirac.cfg' file
        res = self.runIt()

        # if error then exit
        if 'OK' in res and not res['OK']:
            self.log.error("Failed to set up environment of the application")
            status = 1
            return self.finalStatusReport(status)


        """
        For files (coming from AFS and EOS) uploaded to the sandbox,
        replace the given full path by the CE working directory path
        because the given full path is no longer accessible
        (true for AFS but for EOS, need to add xrootd://... before eos paths), 
    
        For FCC applications some attributes contain path
        and also the dynamic module paramaters
        we correct path (replace AFS/EOS orginal path by CE cwd) for all attributes that are paths
        and for extra user paths too
    
        We have transfered all attributes (the 'public' ones) to the module FCCAnalysis
        like that, we do not need to call md1.addParameter() for each attribute we want
        If we plan to use a dictionnary keys instead of class attributes
        It is possible to pass this dictionnary from user script (client side) to FCC class (server side)
        and we do not iterate over class attributes but over dictionnary keys
        like that, we can add dynamically 'attributes' without adding them statically on these classes before
        which will reduce stuff like (modifying classes of the client, classes of the server
        and merging the new classes with the gitlab repository of ILCDirac) 
        These dynamic parameters can be catched here and we can also have a parameter which is a bash command 
        that can be evaluated here, but this functionnality has been disabled"""

        # for example, fcc_conf_file is an attribute of FCC
        for attribute in dir(self):
            # if path is user local path (AFS or EOS without xrootd prefix)
            attribute_name = 'self.' + attribute
            attribute_value = str(eval(attribute_name))

            # if attribute is a path
            if attribute_value.startswith(os.path.sep):
                # if the attribute kept user local path
                # it must have been added to the sandbox by FCC module
                # so locate it in the sandbox and change its path to its current real path on the CE
                # change path of attribute else command will run with the AFS path
                # which is a wrong path for the bash running in the CE
                if not (attribute_value.startswith('/cvmfs/')
                        or attribute_value.startswith('/eos/')):

                    attribute_value = os.path.abspath(os.path.basename(attribute_value))
                    #print attribute_value
                    setattr(self, attribute_name, attribute_value)

                # download is not needed if user put directly
                # xrootd://eos..., the full path of the file
                # normally, DIRAC download the full path if is given
                # download is done if the user enters a non rootable path
                # like /eos/fcc/user/...  available on eospublic

                elif attribute_value.startswith('/eos/'):
                    # download file from EOS to CE
                    status = self.myclient.copy(self.eos_mgm_url + '/' + attribute_value,
                                                os.path.join(os.getcwd(),
                                                             os.path.basename(attribute_value)),
                                                force=True)
                    #print status


        # same stuff for extra paths provided by the user
        paths = eval(self.paths)
        for path in paths:
            if path.startswith('/eos/'):
                status = self.myclient.copy(self.eos_mgm_url +'/' + path,
                                            os.path.join(os.getcwd(),
                                                         os.path.basename(path)), force=True)

        # main command
        bash_commands = ['%s %s %s %s' % (self.fcc_executable, self.fcc_conf_file,
                                          self.number_of_events, self.fcc_arguments)]
        status = self.generate_bash_script(bash_commands, self.script_name)


        # set environnement + execute job
        subprocess.call(self.script_name)


        return self.finalStatusReport(status)

    def chmod(self, file, permission):
        """Set the permission of a file

        :param file: The file to set the permission
        :param permisssion: The permission ('W', 'R' or 'X')

        :type file: str
        :type permission: str

        """

        # reflet chmod a+permission
        # make the file x,r, or w for everyone
        user_permission = eval('stat.S_I'+permission+'USR')
        group_permission = eval('stat.S_I'+permission+'GRP')
        other_permission = eval('stat.S_I'+permission+'OTH')

        permission = user_permission | group_permission | other_permission

        # get actual mode of the file
        mode = os.stat(file).st_mode

        # append the new permission to the existing one
        os.chmod(file, mode | permission)

    def generate_bash_script(self, commands, script_name):
        """Generate a bash script containing the environment setup
        and the command related to the FCC application

        :param commands: The command to call the application
        :param script_name: The name of the generated script

        :type commands: list
        :type script_name: str

        """

        status = 0

        # before the job command, we set the FCC environment

        shebang = "#!/bin/bash"

        #if not self.environment_script.startswith('/cvmfs/'):
        #    self.environment_script = os.path.abspath(os.path.basename(self.environment_script))

        self.environment_script = 'source ' + self.environment_script
        bash_script_text = [shebang, self.environment_script] + commands

        #print '\n'.join(bash_script_text)

        # write the temporary job
        res = self.write2file('w', script_name, '\n'.join(bash_script_text) + '\n')

        if 'OK' in res and not res['OK']:
            self.log.error(res["Value"])
            status = 1
            return status

        # make the job executable and readable for all
        self.chmod(script_name, 'R')
        self.chmod(script_name, 'X')

        return status

    def write2file(self, operation, file_name, filetext):
        """Create a new file and
        write the given content into this file

        :param operation: The operation('w' or 'a') of the writting operation
        :param file_name: The name of the created file
        :param filetext: The content of the file

        :type operation: str
        :type file_name: str
        :type filetext: str

        """

        try:
          # create file with w permission
            with open(file_name, operation) as text_file:
                text_file.write(filetext)
            return S_OK()
        except IOError:
            message = 'Error in writting file'
            self.log.error(message)
            print(message)
            return S_ERROR(message)

    def generate_script_on_the_fly(self, sysconfig, appname, appversion):
        """Normally, generate dynamically the
        fcc environment script but nothing for the moment
        Called if CVMFS is not available

        :param sysconfig: The platform required by the application
        :param appname: The name of the application
        :param appversion: The version of the application

        :type operation: str
        :type file_name: str
        :type filetext: str

        """
        # we do not generate the environment script like in MarlinAnalysis etc...
        # because if we do not have access to cvmfs, we can do nothing

        #return S_OK(os.path.join(os.getcwd(),'init_fcc.sh'))

        return S_ERROR('environment script not found, can not generate one dynamically')

    def runIt(self):
        """Get environment script path from 'dirac.cfg' file
        according to the version, application and platform
        (Called by Agent, Must be called get_environment)

        """


        res = getEnvironmentScript(self.platform, self.job_name,
                                   self.version, self.generate_script_on_the_fly)

        if 'OK' in res and not res['OK']:
            self.log.error("Failed to get the environment script")
            return res

        env_script_path = res["Value"]

        self.environment_script = env_script_path

        status = 0

        return self.finalStatusReport(status)

