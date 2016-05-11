'''
Run DDSim

:author: Andre Sailer
:since:  November 16, 2015
'''

import os
import tarfile
from DIRAC.Core.Utilities.Subprocess                      import shellCall
from DIRAC                                                import S_OK, S_ERROR, gLogger
from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import getEnvironmentScript, unzip_file_into_dir, getSoftwareFolder
from ILCDIRAC.Core.Utilities.PrepareOptionFiles           import getNewLDLibs
from ILCDIRAC.Core.Utilities.resolvePathsAndNames         import resolveIFpaths, getProdFilename
from ILCDIRAC.Core.Utilities.FindSteeringFileDir          import getSteeringFileDirName

__RCSID__ = "$Id$"

DDSIMINPUTFORMATS = ('.stdhep', '.hepevt', '.HEPEvt', '.slcio', '.hepmc')

class DDSimAnalysis(ModuleBase):
  """
  Specific Module to run a DDSim job.
  """
  def __init__(self):
    super(DDSimAnalysis, self).__init__()
    self.enable = True
    self.STEP_NUMBER = ''
    self.log = gLogger.getSubLogger( "DDSimAnalysis" )
    self.result = S_ERROR()
    self.applicationName = 'ddsim'
    self.startFrom = 0
    self.randomSeed = -1
    self.detectorModel = ''
    self.eventstring = ['+++ Initializing event']

  def applicationSpecificInputs(self):
    """ Resolve all input variables for the module here.

    :return: S_OK()
    """

    if self.WorkflowStartFrom:
      self.startFrom = self.WorkflowStartFrom

    self.randomSeed = self._determineRandomSeed()

    if "IS_PROD" in self.workflow_commons and self.workflow_commons["IS_PROD"]:
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
      - find the detector model xml, using CS query to obtain the path
      - prepare the steering file and command line parameters
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
    envScriptPath = res["Value"]

    #get the path to the detector model, either local or from the software
    resXML = self._getDetectorXML()
    if not resXML['OK']:
      self.log.error("Could not obtain the detector XML file: ", resXML["Message"])
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
      self.extraCLIarguments += " --steeringFile %s " % self.SteeringFile

    if self.startFrom:
      self.extraCLIarguments += " --skipNEvents %s " % self.startFrom

    if self.debug:
      self.extraCLIarguments += " --printLevel DEBUG "

    ##Same as for mokka: using ParticleGun does not imply InputFile
    if self.InputFile:
      self.InputFile = [self.InputFile] if isinstance(self.InputFile, basestring) else self.InputFile
      self.extraCLIarguments += " --inputFile %s " % self.InputFile[0]

    if self.NumberOfEvents:
      self.extraCLIarguments += " --numberOfEvents %s " % self.NumberOfEvents

    self.extraCLIarguments += " --random.seed %s " % self.randomSeed

    if self.OutputFile:
      self.extraCLIarguments += " --outputFile %s " % self.OutputFile

    scriptName = 'DDSim_%s_Run_%s.sh' % (self.applicationVersion, self.STEP_NUMBER)
    if os.path.exists(scriptName):
      os.remove(scriptName)
    script = []
    script.append('#!/bin/bash')
    script.append('#####################################################################')
    script.append('# Dynamically generated script to run a production or analysis job. #')
    script.append('#####################################################################')
    script.append('source %s' % envScriptPath)
    script.append('echo =========')
    script.append('env | sort >> localEnv.log')
    script.append('echo ddsim:`which ddsim`')
    script.append('echo =========')
    comm = 'ddsim --compactFile %(compactFile)s %(extraArgs)s' % dict(compactFile=compactFile,
                                                                      extraArgs=self.extraCLIarguments)
    self.log.info("Command:", comm)
    script.append(comm)
    script.append('declare -x appstatus=$?')
    script.append('exit $appstatus')

    with open(scriptName, 'w') as scriptFile:
      scriptFile.write( "\n".join(script) )

    if os.path.exists(self.applicationLog):
      os.remove(self.applicationLog)

    os.chmod(scriptName, 0755)
    comm = 'bash "./%s"' % scriptName
    self.setApplicationStatus('DDSim %s step %s' % (self.applicationVersion, self.STEP_NUMBER))
    self.stdError = ''
    self.result = shellCall(0, comm, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)
    resultTuple = self.result['Value']
    if not os.path.exists(self.applicationLog):
      self.log.error("Something went terribly wrong, the log file is not present")
      self.setApplicationStatus('%s failed to produce log file' % (self.applicationName))
      if not self.ignoreapperrors:
        return S_ERROR('%s did not produce the expected log %s' % (self.applicationName, self.applicationLog))
    status = resultTuple[0]

    self.log.info( "Status after the application execution is %s" % status )

    return self.finalStatusReport(status)

  def getEnvScript(self, platform, appname, appversion):
    """ Create an environment script for ddsim. Only used when CVMFS native installation not available

    We need ROOTSYS, G4INSTALL and G4DATA as additional environment variables in the CS
    We need to set DD4hepINSTALL !
    """
    ##Need to fetch the new LD_LIBRARY_PATH
    newLDLibraryPath = getNewLDLibs(platform, appname, appversion)
    softwareFolder = getSoftwareFolder(platform, appname, appversion)
    if not softwareFolder['OK']:
      return softwareFolder
    softwareRoot = softwareFolder['Value']
    envName = "DDSimEnv.sh"

    script = []
    script.append("#!/bin/bash")
    script.append("##########################")
    script.append("## Env script for DDSim ##")
    script.append("##########################")

    addEnv = self.ops.getOptionsDict("/AvailableTarBalls/%s/%s/%s/AdditionalEnvVar" % (platform,
                                                                                       appname,
                                                                                       appversion))

    if addEnv['OK']:
      for variable, value in addEnv['Value'].iteritems():
        script.append('declare -x %s=%s' % (variable, value))
    else:
      self.log.verbose("No additional environment variables needed for this application")

    ##Executable:
    script.append('declare -x PATH=%s/bin:$PATH' % softwareRoot )
    script.append('declare -x DD4hepINSTALL=%s' % softwareRoot )

    ##Python objects, pyroot
    script.append('declare -x PYTHONPATH=%s/lib/python:$PYTHONPATH' % softwareRoot )
    script.append('declare -x PYTHONPATH=$ROOTSYS/lib:$PYTHONPATH' )

    ##Libraries
    if newLDLibraryPath:
      script.append('declare -x LD_LIBRARY_PATH=%s' % newLDLibraryPath)

    ## user provided libraries are in lib in the job working directory
    if os.path.exists( "%s/lib" % os.getcwd() ):
      script.append('declare -x LD_LIBRARY_PATH=%s/lib:$LD_LIBRARY_PATH' % os.getcwd() )

    ##Root Path, just in case
    script.append('declare -x PATH=$ROOTSYS/bin:$PATH')

    ##Root and Geant4 Library Path
    script.append('declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:$LD_LIBRARY_PATH')
    script.append('declare -x LD_LIBRARY_PATH=$G4INSTALL/lib64:$LD_LIBRARY_PATH')
    script.append('declare -x LD_LIBRARY_PATH=$DD4hepINSTALL/lib:$LD_LIBRARY_PATH')

    ###mandatory geant 4 data
    script.append('declare -x G4LEDATA=$(ls -d $G4DATA/G4EMLOW*)')
    script.append('declare -x G4LEVELGAMMADATA=$(ls -d $G4DATA/PhotonEvaporation*)')
    script.append('declare -x G4NEUTRONXSDATA=$(ls -d $G4DATA/G4NEUTRONXS*)')
    script.append('declare -x G4SAIDXSDATA=$(ls -d $G4DATA/G4SAIDDATA*)')
    ### not mandatory, needed for Neutron HP
    script.append('declare -x G4RADIOACTIVEDATA=$(ls -d $G4DATA/RadioactiveDecay*)')
    script.append('declare -x G4NEUTRONHPDATA=$(ls -d $G4DATA/G4NDL*)')

    with open(envName,"w") as scriptFile:
      scriptFile.write( "\n".join(script) )
      scriptFile.write( "\n" )

    os.chmod(envName, 0755)
    return S_OK(os.path.abspath(envName))

  def _getDetectorXML( self ):
    """returns the path to the detector XML file

    Checks the Configurartion System for the Path to DetectorModels or extracts the input sandbox detector xml files

    :returns: S_OK(PathToXMLFile), S_ERROR
    """

    if os.path.exists( os.path.join( self.detectorModel, self.detectorModel+ ".xml" ) ):
      self.log.notice( "Found detector model: %s" % os.path.join( self.detectorModel, self.detectorModel+ ".xml" ) )
      return S_OK( os.path.join( self.detectorModel, self.detectorModel+ ".xml" ) )
    elif os.path.exists(self.detectorModel + ".zip"):
      self.log.notice( "Found detector model zipFile: %s" % self.detectorModel+ ".zip" )
      return self._extractZip()
    elif os.path.exists(self.detectorModel + ".tar.gz"):
      self.log.notice( "Found detector model tarball: %s" % self.detectorModel+ ".tar.gz" )
      return self._extractTar()
    elif os.path.exists(self.detectorModel + ".tgz"):
      self.log.notice( "Found detector model tarball: %s" % self.detectorModel+ ".tgz" )
      return self._extractTar( extension=".tgz" )

    detectorModels = self.ops.getOptionsDict("/DDSimDetectorModels/%s" % ( self.applicationVersion ) )
    if not detectorModels['OK']:
      self.log.error("Failed to get list of DetectorModels from the ConfigSystem", detectorModels['Message'])
      return S_ERROR("Failed to get list of DetectorModels from the ConfigSystem")

    softwareFolder = getSoftwareFolder(self.platform, self.applicationName, self.applicationVersion)
    if not softwareFolder['OK']:
      return softwareFolder
    softwareRoot = softwareFolder['Value']

    if self.detectorModel in detectorModels['Value']:
      detModelPath = detectorModels['Value'][self.detectorModel]
      if not detModelPath.startswith("/"):
        detModelPath = os.path.join( softwareRoot, detModelPath )
      self.log.info( "Found path for DetectorModel %s in CS: %s "  % ( self.detectorModel, detModelPath ) )
      return S_OK(detModelPath)


    self.log.error('Detector model %s was not found neither locally nor on the web, exiting' % self.detectorModel)
    return S_ERROR('Detector model was not found')

  def _extractTar( self, extension=".tar.gz" ):
    """ extract the detector tarball for the detectorModel """
    try:
      detTar = tarfile.open(self.detectorModel + extension, "r:gz")
      detTar.extractall()
      xmlPath = os.path.abspath(os.path.join(self.detectorModel, self.detectorModel+".xml") )
      return S_OK(xmlPath)
    except (RuntimeError, OSError, IOError) as e:
      self.log.error( "Failed to untar detector model", str(e) )
      return S_ERROR( "Failed to untar detector model" )

  def _extractZip( self ):
    """ extract the detector zip file for the detectorModel """
    try:
      self.log.notice("Exracting zip file")
      unzip_file_into_dir(open(self.detectorModel + ".zip"), os.getcwd())
      xmlPath = os.path.join(os.getcwd(), self.detectorModel, self.detectorModel+".xml")
      return S_OK( xmlPath )
    except (RuntimeError, OSError, IOError) as err: #RuntimeError is for zipfile
      self.log.error('Failed to unzip detector model: ', str(err))
      return S_ERROR('Failed to unzip detector model')

  def _determineRandomSeed(self):
    """determine what the randomSeed should be, depends on production or not

    .. Note::
      DDSim we use *randomSeed* and not *RandomSeed* as in the other workflow modules

    """
    if self.randomSeed == -1:
      self.randomSeed = self.jobID
    if "IS_PROD" in self.workflow_commons:
      self.randomSeed = int(str(int(self.workflow_commons["PRODUCTION_ID"])) + str(int(self.workflow_commons["JOB_ID"])))
    return self.randomSeed
