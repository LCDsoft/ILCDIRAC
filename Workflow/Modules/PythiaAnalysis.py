'''
Created on Jun 3, 2011

@author: Stephane Poss
'''
from DIRAC.Core.Utilities.Subprocess                       import shellCall
from ILCDIRAC.Workflow.Modules.ModuleBase                  import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation  import LocalArea,SharedArea
from ILCDIRAC.Core.Utilities.PrepareOptionFiles            import GetNewLDLibs
from ILCDIRAC.Core.Utilities.ResolveDependencies           import resolveDepsTar
from ILCDIRAC.Core.Utilities.resolveOFnames                import getProdFilename
from DIRAC import gLogger,S_OK,S_ERROR, gConfig

import os,shutil


class PythiaAnalysis(ModuleBase):
  def __init__(self):
    ModuleBase.__init__(self)
    self.NumberOfEvents = 1
    self.enable = True
    self.STEP_NUMBER = ''
    self.debug = True
    self.log = gLogger.getSubLogger( "PythiaAnalysis" )
        
  def applicationSpecificInputs(self):
    if self.step_commons.has_key("NbOfEvts"):
      self.NumberOfEvents = self.step_commons["NbOfEvts"]
    else:
      return S_ERROR("Number of events to process not specified")
    
    return S_OK()
  
  def execute(self):
    self.result =self.resolveInputVariables()
    if not self.systemConfig:
      self.result = S_ERROR( 'No ILC platform selected' )
    elif not self.applicationName:
      self.result = S_ERROR("Pythia version name not given")  
    elif not self.applicationLog:
      self.result = S_ERROR( 'No Log file provided' )
    if not self.result['OK']:
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' %(self.workflowStatus['OK'],self.stepStatus['OK']))
      return S_OK('%s should not proceed as previous step did not end properly'%self.applicationName)

    appDir = gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s/TarBall'%(self.systemConfig,self.applicationName,self.applicationVersion),'')
    appDir = appDir.replace(".tgz","").replace(".tar.gz","")
    mySoftwareRoot = ''
    localArea = LocalArea()
    sharedArea = SharedArea()
    if os.path.exists('%s%s%s' %(localArea,os.sep,appDir)):
      mySoftwareRoot = localArea
    elif os.path.exists('%s%s%s' %(sharedArea,os.sep,appDir)):
      mySoftwareRoot = sharedArea
    else:
      self.setApplicationStatus('Pythia: Could not find neither local area not shared area install')
      return S_ERROR('Missing installation of Pythia!')
    myappDir = os.path.join(mySoftwareRoot,appDir)


    deptar = resolveDepsTar(self.systemConfig,self.applicationName,self.applicationVersion)[0]
    depdir = deptar.replace(".tgz","").replace(".tar.gz","")
    path = os.path.join(mySoftwareRoot,depdir)
    if not os.path.exists(path+"/%s.ep"%depdir):
      return S_ERROR("Lumi files not found")
    
    originpath = path+"/%s.ep"%depdir
    try:
      os.symlink(originpath,"/tmp/%s.ep"%depdir)
    except Exception,x:
      return S_ERROR("Cannot sym link lumi file: %s %s"%(Exception,x))
    #try :
    #  shutil.copy(originpath,"/tmp/")
    #except:
    #  return S_ERROR("Could not copy to /tmp")  
    #self.lumifile = path+"/%s.ep"%depdir
    self.lumifile = "/tmp/%s.ep"%depdir
    ##Need to fetch the new LD_LIBRARY_PATH
    new_ld_lib_path= GetNewLDLibs(self.systemConfig,self.applicationName,self.applicationVersion,mySoftwareRoot)
    new_ld_lib_path = myappDir+"/lib:"+new_ld_lib_path

    scriptName = '%s_%s_Run_%s.sh' %(self.applicationName,self.applicationVersion,self.STEP_NUMBER)
    if os.path.exists(scriptName): os.remove(scriptName)
    script = open(scriptName,'w')
    script.write('#!/bin/sh \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')
    if new_ld_lib_path:
      script.write('declare -x LD_LIBRARY_PATH=%s\n'%new_ld_lib_path)
    script.write("declare -x NBEVTS=%s\n"%self.NumberOfEvents)
    script.write("declare -x LumiFile=%s\n"%self.lumifile)
    script.write("declare -x OUTPUTFILE=%s\n"%self.outputFile)
    script.write('echo ======================================\n')
    script.write('env | sort >> localEnv.log\n')
    comm = "%s/%s_%s.exe\n"%(myappDir,self.applicationName,self.applicationVersion)
    self.log.info("Will run %s"%comm)
    script.write(comm)
    script.write('declare -x appstatus=$?\n')
    script.write('exit $appstatus\n')
    script.close()
    if os.path.exists(self.applicationLog): os.remove(self.applicationLog)

    os.chmod(scriptName,0755)
    comm = 'sh -c "./%s"' %scriptName
    self.setApplicationStatus('%s %s step %s' %(self.applicationName,self.applicationVersion,self.STEP_NUMBER))
    self.stdError = ''
    self.result = shellCall(0,comm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    if not self.result['OK']:
      self.log.error('Something wrong during running: %s'%self.result['Message'])
      self.setApplicationStatus('Error during running %s'%self.applicationName)
      return S_ERROR('Failed to run %s'%self.applicationName)

    #self.result = {'OK':True,'Value':(0,'Disabled Execution','')}
    resultTuple = self.result['Value']
    if not os.path.exists(self.applicationLog):
      self.log.error("Something went terribly wrong, the log file is not present")
      self.setApplicationStatus('%s failed terribly, you are doomed!' %(self.applicationName))
      if not self.ignoreapperrors:
        return S_ERROR('%s did not produce the expected log' %(self.applicationName))

    status = resultTuple[0]
    return self.finalStatusReport(status) 