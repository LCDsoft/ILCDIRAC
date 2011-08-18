#####################################################
# $HeadURL: $
#####################################################
'''
ILCDIRAC.Workflow.Modules.SLICAnalysis Called by Job Agent. 

@since:  Apr 7, 2010

@author: Stephane Poss
'''

__RCSID__ = "$Id: $"


import os,sys,re, urllib, zipfile
from DIRAC.Core.Utilities.Subprocess                      import shellCall
#from DIRAC.Core.DISET.RPCClient                           import RPCClient
from ILCDIRAC.Workflow.Modules.ModuleBase                    import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import LocalArea,SharedArea
from ILCDIRAC.Core.Utilities.PrepareOptionFiles           import PrepareMacFile,GetNewLDLibs
from ILCDIRAC.Core.Utilities.ResolveDependencies          import resolveDepsTar
from ILCDIRAC.Core.Utilities.resolveIFpaths import resolveIFpaths
from ILCDIRAC.Core.Utilities.resolveOFnames import getProdFilename
from ILCDIRAC.Core.Utilities.InputFilesUtilities import getNumberOfevents

from DIRAC                                                import S_OK, S_ERROR, gLogger, gConfig
import DIRAC

class SLICAnalysis(ModuleBase):
  """
  Specific Module to run a SLIC job.
  """
  def __init__(self):
    ModuleBase.__init__(self)
    self.enable = True
    self.STEP_NUMBER = ''
    self.log = gLogger.getSubLogger( "SLICAnalysis" )
    self.result = S_ERROR()
    self.applicationName = 'SLIC'
    self.startFrom = 0
    self.stdhepFile = ''
    self.randomseed = 0
    self.detectorModel = ''
    self.SteeringFile = ''
    self.eventstring = 'BeginEvent'
    
  def applicationSpecificInputs(self):
    """ Resolve all input variables for the module here.
    @return: S_OK()
    """
    ##NEed to keep for backward compat.
    if self.step_commons.has_key('numberOfEvents'):
        self.numberOfEvents = self.step_commons['numberOfEvents']
          
    if self.step_commons.has_key('startFrom'):
      self.startFrom = self.step_commons['startFrom']

    if self.step_commons.has_key('stdhepFile'):
      self.stdhepFile = self.step_commons['stdhepFile']
      
    if self.step_commons.has_key("inputmacFile"):
      self.SteeringFile = self.step_commons['inputmacFile']

    if self.step_commons.has_key('detectorModel'):
      self.detectorModel = self.step_commons['detectorModel'] 

    if self.step_commons.has_key("RandomSeed"):
      self.randomseed = self.step_commons["RandomSeed"]
    ##Move below to ModuleBase as common to Mokka
    elif self.workflow_commons.has_key("IS_PROD"):  
      self.randomseed = int(str(int(self.workflow_commons["PRODUCTION_ID"]))+str(int(self.workflow_commons["JOB_ID"])))
    elif self.jobID:
      self.randomseed = self.jobID

    if self.workflow_commons.has_key("IS_PROD"):
      if self.workflow_commons["IS_PROD"]:
        self.outputFile = getProdFilename(self.outputFile,int(self.workflow_commons["PRODUCTION_ID"]),
                                           int(self.workflow_commons["JOB_ID"]))

    if self.InputData:
      if not self.workflow_commons.has_key("Luminosity") or not self.workflow_commons.has_key("NbOfEvents"):
        res = getNumberOfevents(self.InputData)
        if res.has_key("nbevts") and not self.workflow_commons.has_key("Luminosity") :
          self.workflow_commons["NbOfEvents"]=res["nbevts"]
          if not self.numberOfEvents:
            self.numberOfEvents=res["nbevts"]
        if res.has_key("lumi") and not self.workflow_commons.has_key("NbOfEvents"):
          self.workflow_commons["Luminosity"]=res["lumi"]
      
    if len(self.stdhepFile)==0 and not len(self.InputData)==0:
      inputfiles = self.InputData.split(";")
      for files in inputfiles:
        if files.lower().find(".stdhep")>-1 or files.lower().find(".hepevt")>-1:
          self.stdhepFile = files
          break
          
    return S_OK('Parameters resolved')
  
  def unzip_file_into_dir(self,file, dir):
    """Used to unzip the downloaded detector model
    """
    zfobj = zipfile.ZipFile(file)
    for name in zfobj.namelist():
      if name.endswith('/'):
        os.mkdir(os.path.join(dir, name))
      else:
        outfile = open(os.path.join(dir, name), 'wb')
        outfile.write(zfobj.read(name))
        outfile.close()
  
  def execute(self):
    """
    Called by JobAgent
    
    Execute the following:
      - get the environment variables that should have been set during installation
      - download the detector model, using CS query to fetch the address
      - prepare the mac file using L{PrepareMacFile}
      - run SLIC on this mac File and catch the exit status
    @return: S_OK(), S_ERROR()
    """
    self.result =self.resolveInputVariables()
    if not self.systemConfig:
      self.result = S_ERROR( 'No ILC platform selected' )
    elif not self.applicationLog:
      self.result = S_ERROR( 'No Log file provided' )
    if not self.result['OK']:
      return self.result
    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' %(self.workflowStatus['OK'],self.stepStatus['OK']))
      return S_OK('SLIC should not proceed as previous step did not end properly')
    
    if not os.environ.has_key('SLIC_DIR'):
      self.log.error('SLIC_DIR not found, probably the software installation failed')
      return S_ERROR('SLIC_DIR not found, probably the software installation failed')
    if not os.environ.has_key('SLIC_VERSION'):
      self.log.error('SLIC_VERSION not found, probably the software installation failed')
      return S_ERROR('SLIC_VERSION not found, probably the software installation failed')
    if not os.environ.has_key('LCDD_VERSION'):
      self.log.error('LCDD_VERSION not found, probably the software installation failed')
      return S_ERROR('LCDD_VERSION not found, probably the software installation failed')
    if not os.environ.has_key('XERCES_VERSION'):
      self.log.error('XERCES_VERSION not found, probably the software installation failed')
      return S_ERROR('XERCES_VERSION not found, probably the software installation failed')


    slicDir = os.environ['SLIC_DIR']
    mySoftwareRoot = ''
    localArea = LocalArea()
    sharedArea = SharedArea()
    area = ''
    if os.path.exists('%s%s%s' %(localArea,os.sep,slicDir)):
      mySoftwareRoot = '%s%s%s' %(localArea,os.sep,slicDir)
      area = localArea
    if os.path.exists('%s%s%s' %(sharedArea,os.sep,slicDir)):
      mySoftwareRoot = '%s%s%s' %(sharedArea,os.sep,slicDir)
      area = sharedArea
    if not mySoftwareRoot:
      self.log.error('Directory %s was not found in either the local area %s or shared area %s' %(slicDir,localArea,sharedArea))
      return S_ERROR('Failed to discover software')


    ##Need to fetch the new LD_LIBRARY_PATH
    new_ld_lib_path= GetNewLDLibs(self.systemConfig,"slic",self.applicationVersion,mySoftwareRoot)

    #retrieve detector model from web
    detector_urls = gConfig.getValue('/Operations/SLICweb/SLICDetectorModels',[''])
    if len(detector_urls[0])<1:
      self.log.error('Could not find in CS the URL for detector model')
      return S_ERROR('Could not find in CS the URL for detector model')

    if not os.path.exists(self.detectorModel+".zip"):
      for detector_url in detector_urls:
        try:
          detmodel,headers = urllib.urlretrieve("%s%s"%(detector_url,self.detectorModel+".zip"),self.detectorModel+".zip")
        except:
          self.log.error("Download of detector model failed")
          continue
        try:
          self.unzip_file_into_dir(open(self.detectorModel+".zip"),os.getcwd())
          break
        except:
          os.unlink(self.detectorModel+".zip")
          continue

    if not os.path.exists(self.detectorModel+".zip"):
      self.log.error('Detector model %s was not found neither locally nor on the web, exiting'%self.detectorModel)
      return S_ERROR('Detector model %s was not found neither locally nor on the web, exiting'%self.detectorModel)
    
    #unzip detector model
    #self.unzip_file_into_dir(open(self.detectorModel+".zip"),os.getcwd())
    
    slicmac = 'slicmac.mac'
    if len(self.stdhepFile)>0:
      res = resolveIFpaths([self.stdhepFile])
      if not res['OK']:
        self.log.error("Generator file not found")
        return res
      self.stdhepFile = res['Value'][0]
    if len(self.SteeringFile)>0:
      self.SteeringFile = os.path.basename(self.SteeringFile)
      if not os.path.exists(self.SteeringFile):
        if os.path.exists(os.path.join(area,"steeringfilesV1",self.SteeringFile)):
          self.SteeringFile = os.path.join(area,"steeringfilesV1",self.SteeringFile)
      if not os.path.exists(self.SteeringFile):
        return S_ERROR("Could not find mac file")    
        
    macok = PrepareMacFile(self.SteeringFile,slicmac,self.stdhepFile,self.numberOfEvents,self.startFrom,self.detectorModel,self.randomseed,self.outputFile,self.debug)
    if not macok['OK']:
      self.log.error('Failed to create SLIC mac file')
      return S_ERROR('Error when creating SLIC mac file')
    
    scriptName = 'SLIC_%s_Run_%s.sh' %(self.applicationVersion,self.STEP_NUMBER)
    if os.path.exists(scriptName): os.remove(scriptName)
    script = open(scriptName,'w')
    script.write('#!/bin/sh \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')
    script.write('declare -x XERCES_LIB_DIR=%s/packages/xerces/%s/lib\n'%(mySoftwareRoot,os.environ['XERCES_VERSION']))
    if new_ld_lib_path:
      script.write('declare -x LD_LIBRARY_PATH=$XERCES_LIB_DIR:%s\n'%new_ld_lib_path)
    else:
      script.write('declare -x LD_LIBRARY_PATH=$XERCES_LIB_DIR\n')
      
    script.write('declare -x GEANT4_DATA_ROOT=%s/packages/geant4/data\n'%mySoftwareRoot)
    script.write('declare -x G4LEVELGAMMADATA=$(ls -d $GEANT4_DATA_ROOT/PhotonEvaporation*)\n')
    script.write('declare -x G4RADIOACTIVEDATA=$(ls -d $GEANT4_DATA_ROOT/RadioactiveDecay*)\n')
    script.write('declare -x G4LEDATA=$(ls -d $GEANT4_DATA_ROOT/G4EMLOW*)\n')
    script.write('declare -x G4NEUTRONHPDATA=$(ls -d $GEANT4_DATA_ROOT/G4NDL*)\n')
    script.write('declare -x GDML_SCHEMA_DIR=%s/packages/lcdd/%s\n'%(mySoftwareRoot,os.environ['LCDD_VERSION']))
    script.write('declare -x PARTICLE_TBL=%s/packages/slic/%s/data/particle.tbl\n'%(mySoftwareRoot,os.environ['SLIC_VERSION']))
    script.write('echo =========\n')
    script.write('env | sort >> localEnv.log\n')
    script.write('echo =========\n')
    comm = '%s/packages/slic/%s/bin/Linux-g++/slic -P $PARTICLE_TBL -m %s\n'%(mySoftwareRoot,os.environ['SLIC_VERSION'],slicmac)
    print comm
    script.write(comm)
    script.write('declare -x appstatus=$?\n')
    script.write('exit $appstatus\n')
    script.close()
    if os.path.exists(self.applicationLog): os.remove(self.applicationLog)

    os.chmod(scriptName,0755)
    comm = 'sh -c "./%s"' %scriptName
    self.setApplicationStatus('SLIC %s step %s' %(self.applicationVersion,self.STEP_NUMBER))
    self.stdError = ''
    self.result = shellCall(0,comm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    #self.result = {'OK':True,'Value':(0,'Disabled Execution','')}
    resultTuple = self.result['Value']
    if not os.path.exists(self.applicationLog):
      self.log.error("Something went terribly wrong, the log file is not present")
      self.setApplicationStatus('%s failed terribly, you are doomed!' %(self.applicationName))
      if not self.ignoreapperrors:
        return S_ERROR('%s did not produce the expected log' %(self.applicationName))
    status = resultTuple[0]
    # stdOutput = resultTuple[1]
    # stdError = resultTuple[2]
    self.log.info( "Status after the application execution is %s" % str( status ) )

    return self.finalStatusReport(status)

