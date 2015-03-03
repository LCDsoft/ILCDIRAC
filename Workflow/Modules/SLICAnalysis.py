'''
Run SLIC

@since:  Apr 7, 2010

@author: Stephane Poss
'''

__RCSID__ = "$Id$"


import os, urllib
from DIRAC.Core.Utilities.Subprocess                      import shellCall
#from DIRAC.Core.DISET.RPCClient                           import RPCClient
from ILCDIRAC.Workflow.Modules.ModuleBase                    import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import getEnvironmentScript, unzip_file_into_dir
from ILCDIRAC.Core.Utilities.PrepareOptionFiles           import prepareMacFile, getNewLDLibs
from ILCDIRAC.Core.Utilities.resolvePathsAndNames         import resolveIFpaths, getProdFilename
from ILCDIRAC.Core.Utilities.FindSteeringFileDir          import getSteeringFileDirName

from DIRAC                                                import S_OK, S_ERROR, gLogger
      
class SLICAnalysis(ModuleBase):
  """
  Specific Module to run a SLIC job.
  """
  def __init__(self):
    super(SLICAnalysis, self).__init__()
    self.enable = True
    self.STEP_NUMBER = ''
    self.log = gLogger.getSubLogger( "SLICAnalysis" )
    self.result = S_ERROR()
    self.applicationName = 'SLIC'
    self.startFrom = 0
    self.RandomSeed = 0
    self.detectorModel = ''
    self.eventstring = ['BeginEvent']
    
  def applicationSpecificInputs(self):
    """ Resolve all input variables for the module here.
    @return: S_OK()
    """

    if self.WorkflowStartFrom:
      self.startFrom = self.WorkflowStartFrom

    ##Move below to ModuleBase as common to Mokka
    if "IS_PROD" in self.workflow_commons:
      self.RandomSeed = int(str(int(self.workflow_commons["PRODUCTION_ID"])) + str(int(self.workflow_commons["JOB_ID"])))
    elif self.jobID:
      self.RandomSeed = self.jobID

    if self.workflow_commons.has_key("IS_PROD") and self.workflow_commons["IS_PROD"]:
      self.OutputFile = getProdFilename(self.OutputFile,
                                        int(self.workflow_commons["PRODUCTION_ID"]),
                                        int(self.workflow_commons["JOB_ID"]))
      
    if not len(self.InputFile) and len(self.InputData):
      for files in self.InputData:
        if files.lower().find(".stdhep") > -1 or files.lower().find(".hepevt") > -1:
          self.InputFile.append(files)
          break

    return S_OK('Parameters resolved')
  
  
  
  def runIt(self):
    """
    Called by JobAgent
    
    Execute the following:
      - get the environment variables that should have been set during installation
      - download the detector model, using CS query to fetch the address
      - prepare the mac file using L{prepareMacFile}
      - run SLIC on this mac File and catch the exit status
    @return: S_OK(), S_ERROR()
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
      return S_OK('SLIC should not proceed as previous step did not end properly')
    
    res = getEnvironmentScript(self.platform, self.applicationName, self.applicationVersion, self.getEnvScript)
    if not res['OK']:
      self.log.error("Could not obtain the environment script: ", res["Message"])
      return res
    env_script_path = res["Value"]
    
    #retrieve detector model from web
    detector_urls = self.ops.getValue('/SLICweb/SLICDetectorModels', [''])
    if len(detector_urls[0]) < 1:
      self.log.error('Could not find in CS the URL for detector model')
      return S_ERROR('Could not find in CS the URL for detector model')

    if not os.path.exists(self.detectorModel + ".zip"):
      for detector_url in detector_urls:
        try:
          urllib.urlretrieve("%s%s" % (detector_url, self.detectorModel + ".zip"), 
                             self.detectorModel + ".zip")
        except IOError:
          self.log.error("Download of detector model failed")
          continue

    if not os.path.exists(self.detectorModel + ".zip"):
      self.log.error('Detector model %s was not found neither locally nor on the web, exiting' % self.detectorModel)
      return S_ERROR('Detector model %s was not found neither locally nor on the web, exiting' % self.detectorModel)

    try:
      unzip_file_into_dir(open(self.detectorModel + ".zip"), os.getcwd())
    except (RuntimeError, OSError) as err: #RuntimeError is for zipfile
      os.unlink(self.detectorModel + ".zip")
      self.log.error('Failed to unzip detector model: ', str(err))
      return S_ERROR('Failed to unzip detector model')
    #unzip detector model
    #self.unzip_file_into_dir(open(self.detectorModel+".zip"),os.getcwd())
    
    slicmac = 'slicmac.mac'
    if len(self.InputFile):
      res = resolveIFpaths(self.InputFile)
      if not res['OK']:
        self.log.error("Generator file not found")
        return res
      self.InputFile = res['Value']
    
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
        return S_ERROR("Could not find mac file")    
    ##Same as for mokka: using ParticleGun does not imply InputFile
    if not len(self.InputFile):
      self.InputFile = ['']    
    macok = prepareMacFile(self.SteeringFile, slicmac, self.InputFile[0],
                           self.NumberOfEvents, self.startFrom, self.detectorModel,
                           self.RandomSeed, self.OutputFile, self.debug)
    if not macok['OK']:
      self.log.error('Failed to create SLIC mac file')
      return S_ERROR('Error when creating SLIC mac file')
    
    scriptName = 'SLIC_%s_Run_%s.sh' % (self.applicationVersion, self.STEP_NUMBER)
    if os.path.exists(scriptName): 
      os.remove(scriptName)
    script = open(scriptName, 'w')
    script.write('#!/bin/bash \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')
    script.write("source %s\n" % (env_script_path))
    script.write('echo =========\n')
    script.write('env | sort >> localEnv.log\n')
    script.write('echo SLIC:\n')
    script.write("which slic\n")
    script.write('echo =========\n')
    comm = 'slic -P $PARTICLE_TBL -m %s %s\n' % (slicmac, self.extraCLIarguments)
    self.log.info("Command:", comm)
    script.write(comm)
    script.write('declare -x appstatus=$?\n')
    script.write('exit $appstatus\n')
    script.close()
    if os.path.exists(self.applicationLog): 
      os.remove(self.applicationLog)

    os.chmod(scriptName, 0755)
    comm = 'sh -c "./%s"' % scriptName
    self.setApplicationStatus('SLIC %s step %s' % (self.applicationVersion, self.STEP_NUMBER))
    self.stdError = ''
    self.result = shellCall(0, comm, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)
    #self.result = {'OK':True,'Value':(0,'Disabled Execution','')}
    resultTuple = self.result['Value']
    if not os.path.exists(self.applicationLog):
      self.log.error("Something went terribly wrong, the log file is not present")
      self.setApplicationStatus('%s failed terribly, you are doomed!' % (self.applicationName))
      if not self.ignoreapperrors:
        return S_ERROR('%s did not produce the expected log' % (self.applicationName))
    status = resultTuple[0]
    # stdOutput = resultTuple[1]
    # stdError = resultTuple[2]
    self.log.info( "Status after the application execution is %s" % str( status ) )

    return self.finalStatusReport(status)

  def getEnvScript(self, sysconfig, appname, appversion):
    """ This is called in case CVMFS is not there.
    """
    if not os.environ.has_key('SLIC_DIR'):
      self.log.error('SLIC_DIR not found, probably the software installation failed')
      return S_ERROR('SLIC_DIR not found, probably the software installation failed')
    if not os.environ.has_key('SLIC_VERSION'):
      self.log.error('SLIC_VERSION not found, probably the software installation failed')
      return S_ERROR('SLIC_VERSION not found, probably the software installation failed')
    if not os.environ.has_key('LCDD_VERSION'):
      self.log.error('LCDD_VERSION not found, probably the software installation failed')
      return S_ERROR('LCDD_VERSION not found, probably the software installation failed')
    #if not os.environ.has_key('XERCES_VERSION'):
    #  self.log.error('XERCES_VERSION not found, probably the software installation failed')
    #  return S_ERROR('XERCES_VERSION not found, probably the software installation failed')


    ##Need to fetch the new LD_LIBRARY_PATH
    new_ld_lib_path = getNewLDLibs(sysconfig, appname, appversion)
    #res = getSoftwareFolder(sysconfig, appname, appversion)
    #if not res['OK']:
    #  self.log.error('Directory %s was not found in either the local area or shared area' % (slicDir))
    #  return res
    mySoftwareRoot = os.environ['SLIC_DIR']
    env_name = "SLICEnv.sh"
    script = open(env_name,"w")
    script.write("#/bin/sh\n")
    script.write("######################\n")
    script.write("## Env script for SLIC\n")
    script.write("######################\n")
    if os.environ.has_key('XERCES_VERSION'):
      script.write('declare -x XERCES_LIB_DIR=%s/packages/xerces/%s/lib\n' % (mySoftwareRoot, 
                                                                              os.environ['XERCES_VERSION']))
      if new_ld_lib_path:
        script.write('declare -x LD_LIBRARY_PATH=$XERCES_LIB_DIR:%s\n' % new_ld_lib_path)
      else:
        script.write('declare -x LD_LIBRARY_PATH=$XERCES_LIB_DIR\n')
      
    script.write('declare -x GEANT4_DATA_ROOT=%s/packages/geant4/data\n' % mySoftwareRoot)
    script.write('declare -x G4LEVELGAMMADATA=$(ls -d $GEANT4_DATA_ROOT/PhotonEvaporation*)\n')
    script.write('declare -x G4RADIOACTIVEDATA=$(ls -d $GEANT4_DATA_ROOT/RadioactiveDecay*)\n')
    script.write('declare -x G4LEDATA=$(ls -d $GEANT4_DATA_ROOT/G4EMLOW*)\n')
    script.write('declare -x G4NEUTRONHPDATA=$(ls -d $GEANT4_DATA_ROOT/G4NDL*)\n')
    script.write('declare -x GDML_SCHEMA_DIR=%s/packages/lcdd/%s\n' % (mySoftwareRoot, os.environ['LCDD_VERSION']))
    script.write('declare -x PARTICLE_TBL=%s/packages/slic/%s/data/particle.tbl\n' % (mySoftwareRoot, 
                                                                                      os.environ['SLIC_VERSION']))
    script.write('declare -x MALLOC_CHECK_=0\n')
    if os.path.exists("%s/lib" % (mySoftwareRoot)):
      script.write('declare -x LD_LIBRARY_PATH=%s/lib:$LD_LIBRARY_PATH\n' % (mySoftwareRoot))
    script.write('declare -x PATH=%s/packages/slic/%s/bin/Linux-g++/:$PATH\n' %(mySoftwareRoot, 
                                                                                os.environ['SLIC_VERSION'])) 
    script.close()
    os.chmod(env_name, 0755)
    return S_OK(os.path.abspath(env_name))
  