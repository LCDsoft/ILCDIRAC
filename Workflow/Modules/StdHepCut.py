'''
Created on May 11, 2011

@author: Stephane Poss
'''
from DIRAC                                                import S_OK, S_ERROR, gLogger, gConfig
from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import LocalArea,SharedArea
from DIRAC.Core.Utilities.Subprocess                      import shellCall
from ILCDIRAC.Core.Utilities.PrepareLibs                  import removeLibc
from ILCDIRAC.Core.Utilities.PrepareOptionFiles           import GetNewLDLibs

import os

class StdHepCut(ModuleBase):
  def __init__(self):
    ModuleBase.__init__(self)
    self.log = gLogger.getSubLogger( "stdhepCut" )
    self.applicationName = 'stdhepCut'
    self.STEP_NUMBER = ''
    self.SteeringFile = ''
    self.MaxNbEvts = 0
    
  def applicationSpecificInputs(self):
    if self.step_commons.has_key('CutFile'):
      self.SteeringFile = self.step_commons['CutFile']
    else:
      return S_ERROR('Cut file not defined')
  
    if self.step_commons.has_key('MaxNbEvts'):
      self.MaxNbEvts = self.step_commons['MaxNbEvts']
      
    if not self.OutputFile:
      dircont = os.listdir("./")
      for file in dircont:
        if file.count(".stdhep"):
          self.OutputFile = file.rstrip(".stdhep")+"_reduced.stdhep"
          break
      if not self.OutputFile:
        return S_ERROR("Could not find suitable OutputFile name")
    
    return S_OK()

  def execute(self):
    self.result = self.resolveInputVariables()
    if not self.result['OK']:
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' %(self.workflowStatus['OK'],self.stepStatus['OK']))
      return S_OK('StdHepCut should not proceed as previous step did not end properly')

    appDir = gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s/TarBall'%(self.systemConfig,"stdhepcut",self.applicationVersion),'')
    if not appDir:
      self.log.error('Could not get info from CS')
      self.setApplicationStatus('Failed finding info from CS')
      return S_ERROR('Failed finding info from CS')
    appDir = appDir.replace(".tgz","").replace(".tar.gz","")

    mySoftwareRoot = ''
    localArea = LocalArea()
    sharedArea = SharedArea()
    if os.path.exists('%s%s%s' %(localArea,os.sep,appDir)):
      mySoftwareRoot = localArea
    elif os.path.exists('%s%s%s' %(sharedArea,os.sep,appDir)):
      mySoftwareRoot = sharedArea
    else:
      self.setApplicationStatus('%s: Could not find neither local area not shared area install'%self.applicationName)
      return S_ERROR('Missing installation of %s!'%self.applicationName)
    mySoftDir = os.path.join(mySoftwareRoot,appDir)
        
    new_ld_lib_path= GetNewLDLibs(self.systemConfig,self.applicationName,self.applicationVersion,mySoftwareRoot)
    new_ld_lib_path = mySoftDir+"/lib:"+new_ld_lib_path
    if os.path.exists("./lib"):
      new_ld_lib_path = "./lib:"+new_ld_lib_path
    scriptName = '%s_%s_Run_%s.sh' %(self.applicationName,self.applicationVersion,self.STEP_NUMBER)
    if os.path.exists(scriptName): os.remove(scriptName)
    script = open(scriptName,'w')
    script.write('#!/bin/sh \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')
    script.write('declare -x PATH=%s:$PATH\n'%mySoftDir)
    script.write('declare -x LD_LIBRARY_PATH=%s\n'%new_ld_lib_path)
    script.write('env | sort >> localEnv.log\n')      
    script.write('echo =============================\n')
    extraopts = ""
    if self.MaxNbEvts:
      extraopts = '-m %s'%self.MaxNbEvts
    comm = "stdhepCut %s -o %s -c %s  *.stdhep\n"%(extraopts,self.OutputFile,os.path.basename(self.SteeringFile))
    self.log.info("Running %s"%comm)
    script.write(comm)
    script.write('declare -x appstatus=$?\n')    
    script.write('exit $appstatus\n')
    script.close()
    
    if os.path.exists(self.applicationLog): os.remove(self.applicationLog)
    os.chmod(scriptName,0755)
    comm = 'sh -c "./%s"' %(scriptName)    
    self.setApplicationStatus('%s %s step %s' %(self.applicationName,self.applicationVersion,self.STEP_NUMBER))
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
