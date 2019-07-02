'''
Run DDSim

:author: Andre Sailer
:since:  November 16, 2015
'''

import os

from DIRAC.Core.Utilities.Subprocess                      import shellCall
from DIRAC                                                import S_OK, S_ERROR, gLogger
from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import getEnvironmentScript, getSoftwareFolder
from ILCDIRAC.Core.Utilities.PrepareOptionFiles           import getNewLDLibs
from ILCDIRAC.Core.Utilities.resolvePathsAndNames         import resolveIFpaths, getProdFilename
from ILCDIRAC.Workflow.Utilities.DD4hepMixin              import DD4hepMixin
from ILCDIRAC.Core.Utilities.FindSteeringFileDir          import getSteeringFileDirName


__RCSID__ = '$Id$'
DDSIMINPUTFORMATS = ('.stdhep', '.hepevt', '.HEPEvt', '.slcio', '.hepmc')
LOG = gLogger.getSubLogger(__name__)


class DDSimAnalysis(DD4hepMixin, ModuleBase):
  """
  Specific Module to run a DDSim job.
  """
  def __init__(self):
    super(DDSimAnalysis, self).__init__()
    self.enable = True
    self.STEP_NUMBER = ''
    self.result = S_ERROR()
    self.applicationName = 'ddsim'
    self.startFrom = 0
    self.randomSeed = -1
    self.detectorModel = ''
    self.extraParticles = []
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
                                        int(self.workflow_commons["JOB_ID"]),
                                        self.workflow_commons,
                                       )

    if not len(self.InputFile) and len(self.InputData):
      for files in self.InputData:
        if files.endswith( DDSIMINPUTFORMATS ):
          self.InputFile.append(files)

    return S_OK('Parameters resolved')

  def resolveDDSimOptions(self, envScriptPath, compactFile):
    """Find all the options combining CLI arguments and steering file.

    :return: S_OK(list of strings of the dump of the ddsim steering file)
    """
    scriptName = 'findOptions.sh'
    script = []
    script.append('#!/bin/bash')
    script.append('############################################################')
    script.append('# Dynamically generated script to resolve DDSim parameters #')
    script.append('############################################################')
    script.append('source %s' % envScriptPath)
    script.append('echo =========')
    com1 = 'ddsim --dumpSteeringFile'
    com2 = '--compactFile %(compact)s %(extraArgs)s >& ddsimDump.txt' % dict(compact=compactFile,
                                                                             extraArgs=self.extraCLIarguments)
    script.append('%s %s' % (com1, com2))
    script.append('declare -x appstatus=$?')
    script.append('exit $appstatus')

    with open(scriptName, 'w') as scriptFile:
      scriptFile.write("\n".join(script))

    os.chmod(scriptName, 0o755)
    comm = 'bash "./%s"' % scriptName
    result = shellCall(0, comm, callbackFunction=self.redirectLogOutput, bufferLimit=20971520)
    if not result['OK']:
      return S_ERROR("Could not resolve DDSim options, shell call failed.")

    options = []
    with open("ddsimDump.txt", "r") as ddsimOptions:
      options = ddsimOptions.read().splitlines()
    if not options:
      return S_ERROR("DDSim steering file dump empty")
    return S_OK(options)

  def createParticleTable(self, envScriptPath, options):
    """Resolve the particle.tbl and add or modify the particles.

    :return: S_OK(list of strings of the new/modified particle table)
    """
    # Check if in the options a particle.tbl is set
    particleTableLocation = None
    for line in options:
      if "SIM.physics.pdgfile =" in line:
        if "SIM.physics.pdgfile = None" in line:
          particleTableLocation = None
        else:
          particleTableLocation = line.split('=')[1].strip('" ')

    if not particleTableLocation:
      # Get the env file and extract the location of DD4hep
      dd4hepLocation = None
      with open(envScriptPath, "r") as envScrip:
        for line in envScrip:
          if "DD4hepINSTALL" in line:
            dd4hepLocation = line.split('\"', 1)[-1].split('\"', 1)[0]

        if dd4hepLocation is None:
          return S_ERROR("Did not find env variable DD4hepINSTALL")
        # Get the particle table
        particleTableLocation = "%s/DDG4/examples/particle.tbl" % dd4hepLocation

    if not os.path.exists(particleTableLocation):
      return S_ERROR("Cannot find particle table at location %s" % particleTableLocation)

    particleTable = None
    with open(particleTableLocation, "r") as particleTableFile:
      particleTable = particleTableFile.readlines()

    # First we remove the particles from the table that we want to modify
    newParticleTable = []
    removeIDs = {str(extraParticle[0]) for extraParticle in self.extraParticles}
    for particle in particleTable:
      if particle.split()[0] not in removeIDs:
        newParticleTable.append(particle.rstrip())

    # Add new particles to table
    for particle in self.extraParticles:
      newParticleTable.append(' '.join([str(f) for f in particle]))
    return S_OK(newParticleTable)

  def runIt(self):
    """
    Called by JobAgent

    Execute the following:
      - get the environment variables that should have been set during installation
      - find the detector model xml, using CS query to obtain the path
      - prepare the steering file and command line parameters
      - run DDSim on this steering file and catch the exit status

    :returns: :func:`~DIRAC.Core.Utilities.ReturnValues.S_OK`, :func:`~DIRAC.Core.Utilities.ReturnValues.S_ERROR`
    """
    self.result = S_OK()
    if not self.platform:
      self.result = S_ERROR( 'No ILC platform selected' )
    elif not self.applicationLog:
      self.result = S_ERROR( 'No Log file provided' )
    if not self.result['OK']:
      LOG.error("Failed to resolve input parameters:", self.result['Message'])
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      LOG.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('DDSim should not proceed as previous step did not end properly')

    ##TODO: Setup LD_LIBRARY_PATH for extensions
    res = getEnvironmentScript(self.platform, self.applicationName, self.applicationVersion, self.getEnvScript)
    if not res['OK']:
      LOG.error("Could not obtain the environment script: ", res["Message"])
      return res
    envScriptPath = res["Value"]

    #get the path to the detector model, either local or from the software
    resXML = self._getDetectorXML()
    if not resXML['OK']:
      LOG.error("Could not obtain the detector XML file: ", resXML["Message"])
      return resXML
    compactFile = resXML['Value']

    if len(self.InputFile):
      res = resolveIFpaths(self.InputFile)
      if not res['OK']:
        LOG.error("Generator file not found")
        return res
      self.InputFile = res['Value']

    ## if steering file is set try to find it
    if len(self.SteeringFile) > 0:
      self.SteeringFile = os.path.basename(self.SteeringFile)
      if not os.path.exists(self.SteeringFile):
        res = getSteeringFileDirName(self.platform, self.applicationName, self.applicationVersion)
        if not res['OK']:
          LOG.error("Could not find where the steering files are")
          return res
        steeringfiledirname = res['Value']
        if os.path.exists(os.path.join(steeringfiledirname, self.SteeringFile)):
          self.SteeringFile = os.path.join(steeringfiledirname, self.SteeringFile)
      if not os.path.exists(self.SteeringFile):
        LOG.error("Missing steering file")
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

    if self.extraParticles:
      res = self.resolveDDSimOptions(envScriptPath, compactFile)
      if not res['OK']:
        return res
      resTbl = self.createParticleTable(envScriptPath, res['Value'])
      if not resTbl['OK']:
        return resTbl
      with open("particle.tbl", 'w') as tableFile:
        tableFile.write("\n".join(resTbl['Value']))
      self.extraCLIarguments = "%s --physics.pdgfile particle.tbl" % self.extraCLIarguments

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

    ## for user provided libraries
    if os.path.exists( "lib" ):
      script.append( "export LD_LIBRARY_PATH=$PWD/lib:$LD_LIBRARY_PATH" )

    script.append('env | sort >> localEnv.log')
    script.append('echo ddsim:`which ddsim`')
    script.append('echo =========')
    comm = 'ddsim --compactFile %(compactFile)s %(extraArgs)s' % dict(compactFile=compactFile,
                                                                      extraArgs=self.extraCLIarguments)
    LOG.info("Command:", comm)
    script.append(comm)
    script.append('declare -x appstatus=$?')
    script.append('exit $appstatus')

    with open(scriptName, 'w') as scriptFile:
      scriptFile.write( "\n".join(script) )

    if os.path.exists(self.applicationLog):
      os.remove(self.applicationLog)

    os.chmod(scriptName, 0o755)
    comm = 'bash "./%s"' % scriptName
    self.setApplicationStatus('DDSim %s step %s' % (self.applicationVersion, self.STEP_NUMBER))
    self.stdError = ''
    self.result = shellCall(0, comm, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)
    resultTuple = self.result['Value']
    if not os.path.exists(self.applicationLog):
      LOG.error("Something went terribly wrong, the log file is not present")
      self.setApplicationStatus('%s failed to produce log file' % (self.applicationName))
      if not self.ignoreapperrors:
        return S_ERROR('%s did not produce the expected log %s' % (self.applicationName, self.applicationLog))
    status = resultTuple[0]

    LOG.info("Status after the application execution is %s" % status)

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
      LOG.verbose("No additional environment variables needed for this application")

    ## if these variables are not given in the configuration system we guess them from the G4DATA folder
    for var, folder in { "G4LEDATA" : "G4EMLOW",
                         "G4LEVELGAMMADATA" : "PhotonEvaporation",
                         "G4NEUTRONXSDATA" : "G4NEUTRONXS",
                         "G4SAIDXSDATA" : "G4SAIDDATA",
                         "G4RADIOACTIVEDATA" : "RadioactiveDecay",
                         "G4NEUTRONHPDATA" : "G4NDL",
                       }.iteritems():
      if var not in addEnv.get( 'Value', {} ):
        script.append( 'declare -x %s=$(ls -d $G4DATA/%s*)' %( var, folder ) )

    init = self.ops.getValue("/AvailableTarBalls/%s/%s/%s/InitScript" % (platform,
                                                                         appname,
                                                                         appversion), None)
    if init:
      script.append( 'source %s' % init )

    ##Executable:
    script.append('declare -x PATH=%s/bin:$PATH' % softwareRoot )
    script.append('declare -x DD4hepINSTALL=%s' % softwareRoot )

    ##Python objects, pyroot
    script.append('declare -x PYTHONPATH=%s/lib/python:$PYTHONPATH' % softwareRoot )
    script.append('declare -x PYTHONPATH=$ROOTSYS/lib:$PYTHONPATH' )

    ##For ROOT CLING Stuff
    script.append('declare -x CPATH=%s/include:$CPATH' % softwareRoot )

    ##Libraries
    if newLDLibraryPath:
      script.append('declare -x LD_LIBRARY_PATH=$LD_LIBRARY_PATH:%s' % newLDLibraryPath)

    ## user provided libraries are in lib in the job working directory
    if os.path.exists( "%s/lib" % os.getcwd() ):
      script.append('declare -x LD_LIBRARY_PATH=%s/lib:$LD_LIBRARY_PATH' % os.getcwd() )

    ##Root Path, just in case
    script.append('declare -x PATH=$ROOTSYS/bin:$PATH')

    ##Root and Geant4 Library Path
    script.append('declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:$LD_LIBRARY_PATH')
    script.append('declare -x LD_LIBRARY_PATH=$G4INSTALL/lib64:$LD_LIBRARY_PATH')
    script.append('declare -x LD_LIBRARY_PATH=$DD4hepINSTALL/lib:$LD_LIBRARY_PATH')

    with open(envName,"w") as scriptFile:
      scriptFile.write( "\n".join(script) )
      scriptFile.write( "\n" )

    os.chmod(envName, 0o755)
    return S_OK(os.path.abspath(envName))

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
