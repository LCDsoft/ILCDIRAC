'''
Created on Oct 25, 2010

@author: sposs
'''
import os, urllib, zipfile, shutil, string,sys

from DIRAC.Core.Utilities.Subprocess                      import shellCall

from ILCDIRAC.Workflow.Modules.ModuleBase                    import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import LocalArea,SharedArea
from ILCDIRAC.Core.Utilities.ResolveDependencies          import resolveDepsTar
from ILCDIRAC.Core.Utilities.resolveIFpaths               import resolveIFpaths
from ILCDIRAC.Core.Utilities.InputFilesUtilities import getNumberOfevents

from DIRAC                                                import S_OK, S_ERROR, gLogger, gConfig

class SLICPandoraAnalysis (ModuleBase):
  def __init__(self):
    ModuleBase.__init__(self)
    self.debug = False
    self.result = S_ERROR()
    self.systemConfig = ''
    self.applicationLog = ''
    self.applicationName = 'SLICPandora'
    self.applicationVersion=''
    self.pandorasettings = ""
    self.detectorxml = ""
    self.inputSLCIO = ""
    self.outputslcio = ""
    self.numberOfEvents = 0
    self.startFrom = 0
    self.InputData = ""
    self.jobID = None
    if os.environ.has_key('JOBID'): 
      self.jobID = os.environ['JOBID']

  def resolveInputVariables(self):
    if self.workflow_commons.has_key('SystemConfig'):
        self.systemConfig = self.workflow_commons['SystemConfig']

    if self.step_commons.has_key('applicationVersion'):
        self.applicationVersion = self.step_commons['applicationVersion']
        self.applicationLog = self.step_commons['applicationLog']

    if self.step_commons.has_key("PandoraSettings"):
      self.pandorasettings = self.step_commons["PandoraSettings"]

    if self.step_commons.has_key("DetectorXML"):
      self.detectorxml = self.step_commons["DetectorXML"]

    if self.step_commons.has_key("inputSlcio"):
      self.inputSLCIO = self.step_commons["inputSlcio"]      

    if self.workflow_commons.has_key('InputData'):
      self.InputData = self.workflow_commons['InputData']        

    if self.InputData:
      if not self.workflow_commons.has_key("Luminosity") or not self.workflow_commons.has_key("NbOfEvents"):
        res = getNumberOfevents(self.InputData)
        if res.has_key("nbevts") and not self.workflow_commons.has_key("Luminosity") :
          self.workflow_commons["NbOfEvents"]=res["nbevts"]
        if res.has_key("lumi") and not self.workflow_commons.has_key("NbOfEvents"):
          self.workflow_commons["Luminosity"]=res["lumi"]
        
    if self.step_commons.has_key('EvtsToProcess'):
        self.numberOfEvents = self.step_commons['EvtsToProcess']
          
    if self.step_commons.has_key('startFrom'):
      self.startFrom = self.step_commons['startFrom']
      
    if self.step_commons.has_key('outputFile'):
      self.outputslcio = self.step_commons['outputFile']     

    if self.step_commons.has_key('debug'):
      self.debug =  self.step_commons['debug']
      
    if len(self.inputSLCIO)==0 and not len(self.InputData)==0:
      inputfiles = self.InputData.split(";")
      for files in inputfiles:
        if files.lower().find(".slcio")>-1:
          self.inputSLCIO += files+";"
      self.inputSLCIO = self.inputSLCIO.rstrip(";")
           
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
    self.result =self.resolveInputVariables()
    if not self.systemConfig:
      self.result = S_ERROR( 'No ILC platform selected' )
    elif not self.applicationLog:
      self.result = S_ERROR( 'No Log file provided' )
    if not self.result['OK']:
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' %(self.workflowStatus['OK'],self.stepStatus['OK']))
      return S_OK('SLIC Pandora should not proceed as previous step did not end properly')
    slicPandoraDir = gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s/TarBall'%(self.systemConfig,"slicpandora",self.applicationVersion),'')
    slicPandoraDir = slicPandoraDir.replace(".tgz","").replace(".tar.gz","")
    mySoftwareRoot = ''
    localArea = LocalArea()
    sharedArea = SharedArea()
    if os.path.exists('%s%s%s' %(localArea,os.sep,slicPandoraDir)):
      mySoftwareRoot = localArea
    elif os.path.exists('%s%s%s' %(sharedArea,os.sep,slicPandoraDir)):
      mySoftwareRoot = sharedArea
    else:
      self.setApplicationStatus('SLICPandora: Could not find neither local area not shared area install')
      return S_ERROR('Missing installation of SLICPandora!')
    myslicPandoraDir = os.path.join(mySoftwareRoot,slicPandoraDir)
    ### Resolve dependencies
    deps = resolveDepsTar(self.systemConfig,"slicpandora",self.applicationVersion)
    for dep in deps:
      if os.path.exists(os.path.join(mySoftwareRoot,dep.replace(".tgz","").replace(".tar.gz",""))):
        depfolder = dep.replace(".tgz","").replace(".tar.gz","")
        if os.path.exists(os.path.join(mySoftwareRoot,depfolder,"lib")):
          self.log.verbose("Found lib folder in %s"%(depfolder))
          if os.environ.has_key("LD_LIBRARY_PATH"):
            os.environ["LD_LIBRARY_PATH"] = os.path.join(mySoftwareRoot,depfolder,"lib")+":%s"%os.environ["LD_LIBRARY_PATH"]
          else:
            os.environ["LD_LIBRARY_PATH"] = os.path.join(mySoftwareRoot,depfolder,"lib")

    
    inputfilelist = self.inputSLCIO.split(";")    
    res = resolveIFpaths(inputfilelist)
    if not res['OK']:
      self.setApplicationStatus('SLICPandora: missing slcio file')
      return S_ERROR('Missing slcio file!')
    runonslcio = res['Value'][0]
    
    if not self.detectorxml.count(".xml") or not os.path.exists(os.path.basename(self.detectorxml)):
      detmodel = self.detectorxml.replace("_pandora.xml","")
      if not os.path.exists(detmodel+".zip"):
        #retrieve detector model from web
        detector_urls = gConfig.getValue('/Operations/SLICweb/SLICDetectorModels',[''])
        if len(detector_urls[0])<1:
          self.log.error('Could not find in CS the URL for detector model')
          return S_ERROR('Could not find in CS the URL for detector model')

        for detector_url in detector_urls:
          try:
            detModel,headers = urllib.urlretrieve("%s%s"%(detector_url,detmodel+".zip"),detmodel+".zip")
          except:
            self.log.error("Download of detector model failed")
            continue
          try:
            self.unzip_file_into_dir(open(detmodel+".zip"),os.getcwd())
            break
          except:
            os.unlink(detmodel+".zip")
            continue
      if os.path.exists(detmodel) and os.path.isdir(detmodel):
        self.detectorxml = os.path.join(os.getcwd(),detmodel,self.detectorxml)
        self.detectorxml = self.detectorxml+"_pandora.xml"
    
    if not os.path.exists(self.detectorxml):
      self.log.error('Detector model xml %s was not found, exiting'%detmodel)
      return S_ERROR('Detector model xml %s was not found, exiting'%detmodel)
    
    if not os.path.exists(os.path.basename(self.pandorasettings)):
      self.pandorasettings  = "PandoraSettings.xml"
      if os.path.exists(os.path.join(mySoftwareRoot,slicPandoraDir,'Settings',self.pandorasettings)):
        try:
          shutil.copy(os.path.join(mySoftwareRoot,slicPandoraDir,'Settings',self.pandorasettings),os.path.join(os.getcwd(),self.pandorasettings))
        except Exception,x:
          self.log.error('Could not copy PandoraSettings.xml, exception: %s'%x)
          return S_ERROR('Could not find PandoraSettings file')
    
    scriptName = 'SLICPandora_%s_Run_%s.sh' %(self.applicationVersion,self.STEP_NUMBER)
    if os.path.exists(scriptName): os.remove(scriptName)
    script = open(scriptName,'w')
    script.write('#!/bin/sh \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')

    script.write('declare -x ROOTSYS=%s/ROOT\n'%(myslicPandoraDir))

    if os.environ.has_key('LD_LIBRARY_PATH'):
      script.write('declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:%s/LDLibs:%s\n'%(myslicPandoraDir,os.environ['LD_LIBRARY_PATH']))
    else:
      script.write('declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:%s/LDLibs\n'%(myslicPandoraDir))

    if os.path.exists("./lib"):
      script.write('declare -x LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH\n')
    script.write('echo =============================>> %s\n'%self.applicationLog)
    script.write('echo LD_LIBRARY_PATH is >> %s\n'%self.applicationLog)
    script.write('echo $LD_LIBRARY_PATH | tr ":" "\n" >> %s\n'%self.applicationLog)
    script.write('echo =============================>> %s\n'%self.applicationLog)
    script.write('echo PATH is >> %s\n'%self.applicationLog)
    script.write('echo $PATH | tr ":" "\n" >> %s \n'%self.applicationLog)
    script.write('echo ============================= >> %s \n'%self.applicationLog)
    script.write('env | sort >> localEnv.log\n')
    prefixpath = ""
    if os.path.exists("PandoraFrontend"):
      prefixpath = "."
    elif (os.path.exists("%s/Executable/PandoraFrontend"%myslicPandoraDir)):
      prefixpath ="%s/Executable"%myslicPandoraDir
    if prefixpath:
      comm = '%s/PandoraFrontend %s %s %s %s %s >> %s\n'%(prefixpath,self.detectorxml,self.pandorasettings,runonslcio,self.outputslcio,str(self.numberOfEvents),self.applicationLog)
      self.log.info("Will run %s"%comm)
      script.write(comm)
    else:
      script.close()
      self.log.error("PandoraFrontend executable is missing, something is wrong with the installation!")
      return S_ERROR("PandoraFrontend executable is missing")
    
    script.write('declare -x appstatus=$?\n')
    #script.write('where\n')
    #script.write('quit\n')
    #script.write('EOF\n')
    script.write('exit $appstatus\n')

    script.close()
    if os.path.exists(self.applicationLog): os.remove(self.applicationLog)

    os.chmod(scriptName,0755)
    comm = 'sh -c "./%s"' %(scriptName)
    self.setApplicationStatus('SLICPandora %s step %s' %(self.applicationVersion,self.STEP_NUMBER))
    self.stdError = ''
    self.result = shellCall(0,comm,#callbackFunction=self.redirectLogOutput,
                            bufferLimit=20971520)
    #self.result = {'OK':True,'Value':(0,'Disabled Execution','')}
    resultTuple = self.result['Value']

    status = resultTuple[0]
    # stdOutput = resultTuple[1]
    # stdError = resultTuple[2]
    self.log.info( "Status after the application execution is %s" % str( status ) )

    failed = False
    if status != 0:
      self.log.error( "SLICPandora execution completed with errors:" )
      failed = True
    else:
      self.log.info( "SLICPandora execution completed successfully")

    if failed==True:
      self.log.error( "==================================\n StdError:\n" )
      self.log.error( self.stdError )
      self.setApplicationStatus('%s Exited With Status %s' %(self.applicationName,status))
      self.log.error('SLICPandora Exited With Status %s' %(status))
      return S_ERROR('SLICPandora Exited With Status %s' %(status))
    self.setApplicationStatus('SLICPandora %s Successful' %(self.applicationVersion))       
    return S_OK()
    #############################################################################
  def redirectLogOutput(self, fd, message):
    """Catch the stdout of the application
    """
    #sys.stdout.flush()
    if message:
      print message
      if self.applicationLog:
        log = open(self.applicationLog,'a')
        log.write(message+'\n')
        log.close()
      else:
        self.log.error("Application Log file not defined")
    if fd == 1:
      self.stdError += message
    #############################################################################
    
  
  
  
      