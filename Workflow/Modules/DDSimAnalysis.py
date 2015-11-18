'''
Run DDSim

:author: Andre Sailer
:since:  November 16, 2015
'''

__RCSID__ = "$Id$"


import os
from DIRAC.Core.Utilities.Subprocess                      import shellCall
from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import getEnvironmentScript, unzip_file_into_dir, getSoftwareFolder
from ILCDIRAC.Core.Utilities.PrepareOptionFiles           import getNewLDLibs
from ILCDIRAC.Core.Utilities.resolvePathsAndNames         import resolveIFpaths, getProdFilename
from ILCDIRAC.Core.Utilities.FindSteeringFileDir          import getSteeringFileDirName

from DIRAC                                                import S_OK, S_ERROR, gLogger

DDSIMINPUTFORMATS = [ '.stdhep', '.hepevt', '.HEPEvt', '.slcio' ]

class DDSimAnalysis(ModuleBase):
  """
  Specific Module to run a SLIC job.
  """
  def __init__(self):
    super(DDSimAnalysis, self).__init__()
    self.enable = True
    self.STEP_NUMBER = ''
    self.log = gLogger.getSubLogger( "DDSimAnalysis" )
    self.result = S_ERROR()
    self.applicationName = 'SLIC'
    self.startFrom = 0
    self.randomSeed = 0
    self.detectorModel = ''
    self.eventstring = ['BeginEvent'] #FIXME

  def applicationSpecificInputs(self):
    """ Resolve all input variables for the module here.

    :return: S_OK()
    """

    if self.WorkflowStartFrom:
      self.startFrom = self.WorkflowStartFrom

    ##Move below to ModuleBase as common to Mokka
    if "IS_PROD" in self.workflow_commons:
      self.randomSeed = int(str(int(self.workflow_commons["PRODUCTION_ID"])) + str(int(self.workflow_commons["JOB_ID"])))
    elif self.jobID:
      self.randomSeed = self.jobID

    if self.workflow_commons.has_key("IS_PROD") and self.workflow_commons["IS_PROD"]:
      self.OutputFile = getProdFilename(self.OutputFile,
                                        int(self.workflow_commons["PRODUCTION_ID"]),
                                        int(self.workflow_commons["JOB_ID"]))

    if not len(self.InputFile) and len(self.InputData):
      for files in self.InputData:
        if files.endswith( DDSIMINPUTFORMATS ):
          self.InputFile.append(files)

    return S_OK('Parameters resolved')



  def runIt(self):
    """
    Called by JobAgent

    Execute the following:
      - get the environment variables that should have been set during installation
      - download the detector model, using CS query to fetch the address
      - prepare the steering file
      - run DDSim on this steering file and catch the exit status

    :return: S_OK(), S_ERROR()
    """
    self.result = S_OK()
    if not self.platform:
      self.result = S_ERROR( 'No ILC platform selected' )
    elif not self.applicationLog:
      self.result = S_ERROR( 'No Log file provided' )
    if not self.result['OK']:
      self.log.error("Failed to resolve input parameters:", self.result['Message'])
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' %(self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('DDSim should not proceed as previous step did not end properly')

    ##TODO: Setup LD_LIBRARY_PATH for extensions
    res = getEnvironmentScript(self.platform, self.applicationName, self.applicationVersion, self.getEnvScript)
    if not res['OK']:
      self.log.error("Could not obtain the environment script: ", res["Message"])
      return res
    env_script_path = res["Value"]

    #get the path to the detector model, either local or from the software
    resXML = self._getDetectorXML()
    if not resXML['OK']:
      return resXML
    compactFile = resXML['Value']

    if len(self.InputFile):
      res = resolveIFpaths(self.InputFile)
      if not res['OK']:
        self.log.error("Generator file not found")
        return res
      self.InputFile = res['Value']

    ## if steering file is set try to find it
    if len(self.SteeringFile) > 0:
      self.SteeringFile = os.path.basename(self.SteeringFile)
      if not os.path.exists(self.SteeringFile):
        res = getSteeringFileDirName(self.platform, self.applicationName, self.applicationVersion)
        if not res['OK']:
          self.log.error("Could not find where the steering files are")
          return res
        steeringfiledirname = res['Value']
        if os.path.exists(os.path.join(steeringfiledirname, self.SteeringFile)):
          self.SteeringFile = os.path.join(steeringfiledirname, self.SteeringFile)
      if not os.path.exists(self.SteeringFile):
        self.log.error("Missing steering file")
        return S_ERROR("Could not find steering file")

    ##Same as for mokka: using ParticleGun does not imply InputFile
    if not len(self.InputFile):
      self.InputFile = ['']
    # macok = prepareDDSimFile(self.SteeringFile, slicmac, self.InputFile[0],
    #                          self.NumberOfEvents, self.startFrom, self.detectorModel,
    #                          self.randomSeed, self.OutputFile, self.debug)
    # if not macok['OK']:
    #   self.log.error('Failed to create SLIC mac file')
    #   return S_ERROR('Error when creating SLIC mac file')

    if self.InputFile:
      self.extraCLIarguments += " --inputFile %s" % self.InputFile[0]

    if self.NumberOfEvents:
      self.extraCLIarguments += " --numberOfEvents %s" % self.NumberOfEvents

    scriptName = 'DDSim_%s_Run_%s.sh' % (self.applicationVersion, self.STEP_NUMBER)
    if os.path.exists(scriptName):
      os.remove(scriptName)
    with open(scriptName, 'w') as script:
      script.write('#!/bin/bash \n')
      script.write('#####################################################################\n')
      script.write('# Dynamically generated script to run a production or analysis job. #\n')
      script.write('#####################################################################\n')
      script.write("source %s\n" % (env_script_path))
      script.write('echo =========\n')
      script.write('env | sort >> localEnv.log\n')
      script.write('echo ddsim:\n')
      script.write("which ddsim\n")
      script.write('echo =========\n')
      comm = 'ddsim --compactFile %(compactFile)s %(extraArgs)s\n' % dict(compactFile=compactFile,
                                                                          extraArgs=self.extraCLIarguments)
      self.log.info("Command:", comm)
      script.write(comm)
      script.write('declare -x appstatus=$?\n')
      script.write('exit $appstatus\n')

      if os.path.exists(self.applicationLog):
        os.remove(self.applicationLog)

    os.chmod(scriptName, 0755)
    comm = 'sh -c "./%s"' % scriptName
    self.setApplicationStatus('DDSim %s step %s' % (self.applicationVersion, self.STEP_NUMBER))
    self.stdError = ''
    self.result = shellCall(0, comm, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)
    resultTuple = self.result['Value']
    if not os.path.exists(self.applicationLog):
      self.log.error("Something went terribly wrong, the log file is not present")
      self.setApplicationStatus('%s failed to produce log file' % (self.applicationName))
      if not self.ignoreapperrors:
        return S_ERROR('%s did not produce the expected log' % (self.applicationName))
    status = resultTuple[0]

    self.log.info( "Status after the application execution is %s" % status )

    return self.finalStatusReport(status)

  def getEnvScript(self, sysconfig, appname, appversion):
    """ This is called in case CVMFS is not there.
    FIXME
    """
    ##Need to fetch the new LD_LIBRARY_PATH
    new_ld_lib_path = getNewLDLibs(sysconfig, appname, appversion)
    softwareFolder = getSoftwareFolder("x86_64-slc5-gcc43-opt", appname, appversion)
    if not softwareFolder['OK']:
      return softwareFolder
    mySoftwareRoot = softwareFolder['Value']
    print "mySoftwareRoot",mySoftwareRoot
    env_name = "DDSimEnv.sh"

    script = []
    script.append("#!/bin/bash")
    script.append("##########################")
    script.append("## Env script for DDSim ##")
    script.append("##########################")

    ##Executable:
    script.append('declare -x PATH=%s/bin:$PATH' % mySoftwareRoot )

    ## ROOTSYS
    #FIXME: Get rootversion from the CS and CVMFS
    script.append('declare -x ROOTSYS=/cvmfs/ilc.desy.de/sw/x86_64_gcc44_sl6/root/5.34.30' )

    ##G4INSTALL
    #FIXME Get Geant4 version from the CS
    script.append('declare -x G4INSTALL=/cvmfs/ilc.desy.de/sw/x86_64_gcc44_sl6/geant4/10.01' )

    ##Python objects, pyroot
    script.append('declare -x PYTHONPATH=%s/lib/python:$PYTHONPATH' % mySoftwareRoot )
    script.append('declare -x PYTHONPATH=$ROOTSYS/lib:$PYTHONPATH' )

    ##Libraries
    if new_ld_lib_path:
      script.append('declare -x LD_LIBRARY_PATH=%s' % new_ld_lib_path)

    #FIXME: Setup LD_LIBRARY_PATH FOR Extensions
    if os.path.exists("%s/lib" % (mySoftwareRoot)):
      script.append('declare -x LD_LIBRARY_PATH=%s/lib:$LD_LIBRARY_PATH' % (mySoftwareRoot))

    script.append('declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:$LD_LIBRARY_PATH')
    script.append('declare -x PATH=$ROOTSYS/bin:$PATH')

    script.append('declare -x LD_LIBRARY_PATH=$G4INSTALL/lib64:$LD_LIBRARY_PATH')

    #FIXME: get DataFolder from the ConfigSystem
    ## Geant 4 datafiles
    script.append('declare -x GEANT4_DATA_ROOT=$G4INSTALL/share/Geant4-10.1.0/data' )
    ###mandatory geant 4 data
    script.append('declare -x G4LEDATA=$(ls -d $GEANT4_DATA_ROOT/G4EMLOW*)')
    script.append('declare -x G4LEVELGAMMADATA=$(ls -d $GEANT4_DATA_ROOT/PhotonEvaporation*)')
    script.append('declare -x G4NEUTRONXSDATA=$(ls -d $GEANT4_DATA_ROOT/G4NEUTRONXS*)')
    script.append('declare -x G4SAIDXSDATA=$(ls -d $GEANT4_DATA_ROOT/G4SAIDDATA*)')
    ### not mandatory, needed for Neutron HP
    script.append('declare -x G4RADIOACTIVEDATA=$(ls -d $GEANT4_DATA_ROOT/RadioactiveDecay*)')
    script.append('declare -x G4NEUTRONHPDATA=$(ls -d $GEANT4_DATA_ROOT/G4NDL*)')

    with open(env_name,"w") as scriptFile:
      scriptFile.write( "\n".join(script) )
      scriptFile.write( "\n" )

    os.chmod(env_name, 0755)
    return S_OK(os.path.abspath(env_name))

  def _getDetectorXML( self ):
    """return the path to the detector XML file"""
    #FIXME
    return S_OK("/data/sailer/DiracLocalArea/ddsimtestVersion/detectors/%s/%s"% (self.detectorModel,self.detectorModel+".xml") )

    if not os.path.exists(self.detectorModel + ".zip"):
      self.log.error('Detector model %s was not found neither locally nor on the web, exiting' % self.detectorModel)
      return S_ERROR('Detector model was not found')

    try:
      unzip_file_into_dir(open(self.detectorModel + ".zip"), os.getcwd())
    except (RuntimeError, OSError) as err: #RuntimeError is for zipfile
      os.unlink(self.detectorModel + ".zip")
      self.log.error('Failed to unzip detector model: ', str(err))
      return S_ERROR('Failed to unzip detector model')
    #unzip detector model
    #self.unzip_file_into_dir(open(self.detectorModel+".zip"),os.getcwd())
