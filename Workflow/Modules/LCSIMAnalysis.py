#####################################################
# $HeadURL: $
#####################################################
'''
ILCDIRAC.Workflow.Modules.LCSIMAnalysis Called by Job Agent. 

@since: Apr 7, 2010

@author: Stephane Poss
'''

__RCSID__ = "$Id: $"

import os, sys, re, shutil
from DIRAC.Core.Utilities.Subprocess                         import shellCall
from ILCDIRAC.Workflow.Modules.ModuleBase                    import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation    import LocalArea,SharedArea
from ILCDIRAC.Core.Utilities.PrepareOptionFiles              import PrepareLCSIMFile,GetNewLDLibs 
from ILCDIRAC.Core.Utilities.ResolveDependencies             import resolveDepsTar
from ILCDIRAC.Core.Utilities.resolveIFpaths                  import resolveIFpaths
from ILCDIRAC.Core.Utilities.resolveOFnames                  import getProdFilename
from ILCDIRAC.Core.Utilities.InputFilesUtilities             import getNumberOfevents
from ILCDIRAC.Core.Utilities.PrepareLibs                     import removeLibc
from ILCDIRAC.Core.Utilities.FindSteeringFileDir             import getSteeringFileDirName

from DIRAC                                                   import S_OK, S_ERROR, gLogger, gConfig
import DIRAC

class LCSIMAnalysis(ModuleBase):
  """Define the LCSIM analysis part of the workflow
  """
  def __init__(self):
    ModuleBase.__init__(self)
    self.enable = True
    self.STEP_NUMBER = ''
    self.log = gLogger.getSubLogger( "LCSIMAnalysis" )
    self.result = S_ERROR()
    self.sourcedir = ''
    self.SteeringFile = ''
    self.InputFile = ''
    self.outputREC = ""
    self.outputDST = ""
    self.aliasproperties = ''
    self.applicationName = 'LCSIM'
    self.eventstring = ['']
    self.extraparams = ''
    self.OutputFile = '' #Set in ModuleBase
    self.detectorModel = ''
    self.trackingstrategy = ''
     
  def applicationSpecificInputs(self):
    """ Resolve all input variables for the module here.
    @return: S_OK()
    """
    
    if self.step_commons.has_key('inputXML'):
      self.SteeringFile = self.step_commons['inputXML']

    #TODO: Next is necessary for old interface, should be removed when old prods are archived.
    if self.step_commons.has_key('outputREC'):
      self.outputREC = self.step_commons['outputREC']
      
    if self.step_commons.has_key('outputDST'):
      self.outputDST = self.step_commons['outputDST']
      
    if self.step_commons.has_key("inputSlcio"):
      self.InputFile = self.step_commons["inputSlcio"]
      
    if self.step_commons.has_key('ExtraParams'):
      self.extraparams = self.step_commons['ExtraParams']    

    if self.InputData:
      if not self.workflow_commons.has_key("Luminosity") or not self.workflow_commons.has_key("NbOfEvents"):
        res = getNumberOfevents(self.InputData)
        if res.has_key("nbevts") and not self.workflow_commons.has_key("Luminosity") :
          self.workflow_commons["NbOfEvents"]=res["nbevts"]
        if res.has_key("lumi") and not self.workflow_commons.has_key("NbOfEvents"):
          self.workflow_commons["Luminosity"]=res["lumi"]

    if self.workflow_commons.has_key("IS_PROD"):
      if self.workflow_commons["IS_PROD"]:
        #self.outputREC = getProdFilename(self.outputREC,int(self.workflow_commons["PRODUCTION_ID"]),
        #                                 int(self.workflow_commons["JOB_ID"]))
        #self.outputDST = getProdFilename(self.outputDST,int(self.workflow_commons["PRODUCTION_ID"]),
        #                                 int(self.workflow_commons["JOB_ID"]))
        #if self.workflow_commons.has_key("MokkaOutput"):
        #  self.InputFile = getProdFilename(self.workflow_commons["MokkaOutput"],int(self.workflow_commons["PRODUCTION_ID"]),
        #                                    int(self.workflow_commons["JOB_ID"]))
        if self.workflow_commons.has_key('ProductionOutputData'):
          outputlist = self.workflow_commons['ProductionOutputData'].split(";")
          for obj in outputlist:
            if obj.lower().count("_rec_"):
              self.outputREC = os.path.basename(obj)
            elif obj.lower().count("_dst_"):
              self.outputDST = os.path.basename(obj)
            elif obj.lower().count("_sim_"):
              self.InputFile = os.path.basename(obj)
        else:
          self.outputREC = getProdFilename(self.outputREC,int(self.workflow_commons["PRODUCTION_ID"]),
                                           int(self.workflow_commons["JOB_ID"]))
          self.outputDST = getProdFilename(self.outputDST,int(self.workflow_commons["PRODUCTION_ID"]),
                                           int(self.workflow_commons["JOB_ID"]))
          if self.workflow_commons.has_key("SLICOutput"):
            self.InputFile = getProdFilename(self.workflow_commons["SLICOutput"],int(self.workflow_commons["PRODUCTION_ID"]),
                                              int(self.workflow_commons["JOB_ID"]))

    if self.step_commons.has_key("aliasproperties"):
      self.aliasproperties = self.step_commons["aliasproperties"]

    if len(self.InputFile)==0 and not len(self.InputData)==0:
      inputfiles = self.InputData.split(";")
      for files in inputfiles:
        if files.lower().find(".slcio")>-1:
          self.InputFile += files+";"
      self.InputFile = self.InputFile.rstrip(";")      
    return S_OK('Parameters resolved')

  def execute(self):
    """
    Called by JobAgent
    
    Execute the following:
      - prepend in the LD_LIBRARY_PATH any lib directory of any dependency (e.g. root)
      - prepare the list of files to run on
      - set the cacheDirectory and put in there the alias.properties
      - set the lcsim file using L{PrepareLCSIMFile}
      - run java and catch the exit code
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
      return S_OK('LCSIM should not proceed as previous step did not end properly')
    
    #look for lcsim filename
    lcsim_name = gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s/TarBall'%(self.systemConfig,"lcsim",self.applicationVersion),'')
    if not lcsim_name:
      self.log.error("Could not find lcsim file name from CS")
      return S_ERROR("Could not find lcsim file name from CS")
    
    mySoftwareRoot = ''
    localArea = LocalArea()
    sharedArea = SharedArea()
    if os.path.exists('%s%s%s' %(localArea,os.sep,lcsim_name)):
      mySoftwareRoot = localArea
    if os.path.exists('%s%s%s' %(sharedArea,os.sep,lcsim_name)):
      mySoftwareRoot = sharedArea
    if not mySoftwareRoot:
      self.log.error('Application %s was not found in either the local area %s or shared area %s' %(lcsim_name,localArea,sharedArea))
      return S_ERROR('Failed to discover software')

    ##Need to fetch the new LD_LIBRARY_PATH
    new_ld_lib_path= GetNewLDLibs(self.systemConfig,"lcsim",self.applicationVersion,mySoftwareRoot)

    runonslcio = []
    if self.InputFile:
      inputfilelist = self.InputFile.split(";")
      res = resolveIFpaths(inputfilelist)
      if not res['OK']:
        self.setApplicationStatus('LCSIM: missing input slcio file')
        return S_ERROR('Missing slcio file!')
      runonslcio = res['Value']
    #for inputfile in inputfilelist:
    #  self.log.verbose("Will try using %s"%(os.path.basename(inputfile)))
    #  runonslcio.append(os.path.join(os.getcwd(),os.path.basename(inputfile)))


    ##Collect jar files to put in classspath
    jars = []
    if os.path.exists("lib"):
      for libs in os.listdir("lib"):
        if os.path.basename(libs).find(".jar")>0:
          jars.append(os.path.abspath(os.path.join("lib",libs)))
      new_ld_lib_path= "./lib:%s"%new_ld_lib_path
      #Remove any libc remaining in .lib
      removeLibc("./lib")
    
    
    ###Define cache directory as local folder
    aliasproperties = os.path.basename(self.aliasproperties)
    cachedir = os.getcwd()
    try:
      os.mkdir(os.path.join(cachedir,".lcsim"))
    except:
      self.log.error("Could not create .lcsim folder !")
    if os.path.exists(os.path.join(cachedir,".lcsim")):
      lcsimfolder = os.path.join(cachedir,".lcsim")
      if os.path.exists(aliasproperties):
        self.log.verbose("Copy alias.properties file in %s"%(lcsimfolder))
        shutil.copy(aliasproperties,os.path.join(lcsimfolder,aliasproperties))
      if os.path.exists(os.path.basename(self.detectorModel)):
        try:
          os.mkdir(os.path.join(lcsimfolder,"detectors"))
        except:
          self.log.error("Could not create detectors folder !")
        if os.path.exists(os.path.join(lcsimfolder,"detectors")):
          self.log.verbose("Copy detector model.zip into the .lcsim/detectors folder")
          shutil.copy(os.path.basename(self.detectorModel),os.path.join(lcsimfolder,"detectors",os.path.basename(self.detectorModel)))
          
    paths = {}
    paths[os.path.basename(self.SteeringFile)]= os.path.basename(self.SteeringFile)
    paths[os.path.basename(self.trackingstrategy)] = os.path.basename(self.trackingstrategy)
    for file in paths.keys():  
      if len(file):
        #file = os.path.basename(file)
        if not os.path.exists(file):
          res =  getSteeringFileDirName(self.systemConfig,"lcsim",self.applicationVersion)     
          if not res['OK']:
            return res
          steeringfiledirname = res['Value']
          if os.path.exists(os.path.join(mySoftwareRoot,steeringfiledirname,file)):
            paths[file] = os.path.join(mySoftwareRoot,steeringfiledirname,file)
        if not os.path.exists(paths[file]):
          return S_ERROR("Could not find file %s"%paths[file])    
    self.SteeringFile = paths[os.path.basename(self.SteeringFile)]
    self.trackingstrategy = paths[os.path.basename(self.trackingstrategy)] 
    
    lcsimfile = "job.lcsim"
    res = PrepareLCSIMFile(self.SteeringFile,lcsimfile,self.trackingstrategy,runonslcio,jars,cachedir,self.OutputFile,self.outputREC,self.outputDST,self.debug)
    if not res['OK']:
      self.log.error("Could not treat input lcsim file because %s"%res['Message'])
      return S_ERROR("Error creating lcsim file")
    else:
      self.log.verbose("File job.lcsim created properly")
    self.eventstring = [res['Value']]
    
    scriptName = 'LCSIM_%s_Run_%s.sh' %(self.applicationVersion,self.STEP_NUMBER)
    if os.path.exists(scriptName): os.remove(scriptName)
    script = open(scriptName,'w')
    script.write('#!/bin/sh \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')
    #for lib in os.path("%s/GeomConverter/target/lib"%(mySoftwareRoot)):
    #  script.write("declare -x CLASSPATH=$CLASSPATH:%s\n"%lib)
    #script.write("declare -x CLASSPATH=$CLASSPATH:%s/lcsim/target/lcsim-%s.jar\n"%(mySoftwareRoot,self.applicationVersion))
    #script.write("declare -x BINPATH=%s/bin\n"%(sourcedir))
    #script.write("declare -x SOURCEPATH=%s/src\n"%(sourcedir))
    if new_ld_lib_path:
      script.write("declare -x LD_LIBRARY_PATH=%s\n"%new_ld_lib_path)
    script.write("declare -x JAVALIBPATH=./\n")
    if os.path.exists("lib"):
      script.write("declare -x JAVALIBPATH=./lib\n")
    script.write('echo =========\n')
    script.write('echo java version :\n')
    script.write('java -version\n')
    script.write('env | sort >> localEnv.log\n')
    script.write('echo =========\n')    
    comm = "java -Xmx1536m -Xms256m -server -Djava.library.path=$JAVALIBPATH -Dorg.lcsim.cacheDir=%s -jar %s/%s %s %s\n"%(cachedir,mySoftwareRoot,lcsim_name,self.extraparams,lcsimfile)
    self.log.info("Will run %s"%comm)
    script.write(comm)
    script.write('declare -x appstatus=$?\n')
    script.write('exit $appstatus\n')    
    script.close()
    if os.path.exists(self.applicationLog): os.remove(self.applicationLog)

    os.chmod(scriptName,0755)
    comm = 'sh -c "./%s"' %scriptName
    self.setApplicationStatus('LCSIM %s step %s' %(self.applicationVersion,self.STEP_NUMBER))
    self.stdError = ''
    self.result = shellCall(0,comm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    #self.result = {'OK':True,'Value':(0,'Disabled Execution','')}
    resultTuple = self.result['Value']
    if not os.path.exists(self.applicationLog):
      self.log.error("Something went terribly wrong, the log file is not present")
      self.setApplicationStatus('%s failed terribly, you are doomed!' %(self.applicationName))
      return S_ERROR('%s did not produce the expected log' %(self.applicationName))

    status = resultTuple[0]
    # stdOutput = resultTuple[1]
    # stdError = resultTuple[2]
    self.log.info( "Status after the application execution is %s" % str( status ) )

    return self.finalStatusReport(status)

