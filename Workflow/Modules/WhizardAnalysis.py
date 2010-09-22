'''
Created on Sep 22, 2010

@author: sposs
'''
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from ILCDIRAC.Workflow.Modules.ModuleBase                import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation  import LocalArea,SharedArea
from ILCDIRAC.Core.Utilities.ResolveDependencies          import resolveDepsTar
from ILCDIRAC.Core.Utilities.PrepareOptionFiles           import PrepareWhizardFile

from DIRAC import gLogger,S_OK,S_ERROR, gConfig

import os,re,sys

class WhizardAnalysis(ModuleBase):
  """
  Specific Module to run a Whizard job.
  """
  def __init__(self):
    ModuleBase.__init__(self)
    self.enable = True
    self.STEP_NUMBER = ''
    self.debug = True
    self.log = gLogger.getSubLogger( "WhizardAnalysis" )
    self.result = S_ERROR()
    self.inFile = ''
    self.stdhepFile = ''
    self.NumberOfEvents = 1
    self.jobID = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']

    self.systemConfig = ''
    self.applicationLog = ''
    self.applicationVersion = ''
    self.applicationName = 'whizard'
    self.randomseed = 0
    
    
  def resolveInputVariables(self):
    if self.workflow_commons.has_key('SystemConfig'):
      self.systemConfig = self.workflow_commons['SystemConfig']
      
    if self.step_commons.has_key('applicationVersion'):
      self.applicationVersion = self.step_commons['applicationVersion']
      self.applicationLog = self.step_commons['applicationLog']
 
    if self.step_commons.has_key("RandomSeed"):
      self.randomseed = self.step_commons['RandomSeed']
    elif self.workflow.has_key('JOB_ID'):
      self.randomseed = int(self.workflow_commons["JOB_ID"])

    if self.step_commons.has_key('NbOfEvts'):
      self.NumberOfEvents = self.step_commons['NbOfEvts']

    if self.step_commons.has_key("InputFile"):
      self.inFile = self.step_commons["InputFile"]
      

    if self.inFile == "whizard.in":
      os.rename(self.inFile, "whizardnew.in")
      self.inFile = "whizardnew.in"
    return S_OK()

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
      return S_OK('Whizard should not proceed as previous step did not end properly')

    whizardDir = gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s/TarBall'%(self.systemConfig,"whizard",self.applicationVersion),'')
    whizardDir = whizardDir.replace(".tgz","").replace(".tar.gz","")
    mySoftwareRoot = ''
    localArea = LocalArea()
    sharedArea = SharedArea()
    if os.path.exists('%s%s%s' %(localArea,os.sep,whizardDir)):
      mySoftwareRoot = localArea
    elif os.path.exists('%s%s%s' %(sharedArea,os.sep,whizardDir)):
      mySoftwareRoot = sharedArea
    else:
      self.setApplicationStatus('Whizard: Could not find neither local area not shared area install')
      return S_ERROR('Missing installation of Whizard!')
    mySoftDir = os.path.join(mySoftwareRoot,whizardDir)
    if os.environ.has_key('LD_LIBRARY_PATH'):
      os.environ['LD_LIBRARY_PATH']=mySoftDir+"/lib:"+os.environ['LD_LIBRARY_PATH']
    else:
      os.environ['LD_LIBRARY_PATH']=mySoftDir+"/lib"
    ### Resolve dependencies (look for beam_spectra)
    deps = resolveDepsTar(self.systemConfig,"whizard",self.applicationVersion)
    path_to_beam_spectra = ""
    for dep in deps:
      if os.path.exists(os.path.join(mySoftwareRoot,dep.replace(".tgz","").replace(".tar.gz",""))):
        depfolder = dep.replace(".tgz","").replace(".tar.gz","")
        if depfolder.count("beam_spectra"):
          path_to_beam_spectra=os.path.join(mySoftwareRoot,depfolder)

    ##Env variables needed to run whizard: avoids hard coded locations
    os.environ['LUMI_LINKER'] = path_to_beam_spectra+"/lumi_linker_000"
    os.environ['PHOTONS_B1'] = path_to_beam_spectra+"/photons_beam1_linker_000"
    os.environ['PHOTONS_B2'] = path_to_beam_spectra+"/photons_beam2_linker_000"
    os.environ['EBEAM'] = path_to_beam_spectra+"/ebeam_in_linker_000"
    os.environ['PBEAM'] = path_to_beam_spectra+"/pbeam_in_linker_000"

    res = PrepareWhizardFile(self.inFile,self.randomseed,self.NumberOfEvents,"whizard.in")
    if not res:
      self.log.error('Something went wrong with input file generation')
      self.setApplicationStatus('Whizard: something went wrong with input file generation')
      return S_ERROR('Something went wrong with whizard.in file generation')
    
    scriptName = 'Whizard_%s_Run_%s.sh' %(self.applicationVersion,self.STEP_NUMBER)
    if os.path.exists(scriptName): os.remove(scriptName)
    script = open(scriptName,'w')
    script.write('#!/bin/sh \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')
    script.write('declare -x PATH=%s:$PATH\n'%mySoftDir)
    script.write('env | sort >> localEnv.log\n')      
    script.write('echo =============================\n')
    script.write('ln -s %s/whizard.mdl\n'%mySoftDir)
    script.write('ln -s %s/whizard.prc\n'%mySoftDir)
    script.write('whizard \n')
    script.write('declare -x appstatus=$?\n')    
    script.close()
    if os.path.exists(self.applicationLog): os.remove(self.applicationLog)

    os.chmod(scriptName,0755)
    comm = 'sh -c "./%s"' %(scriptName)    
    self.setApplicationStatus('Whizard %s step %s' %(self.applicationVersion,self.STEP_NUMBER))
    self.stdError = ''
    self.result = shellCall(0,comm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    #self.result = {'OK':True,'Value':(0,'Disabled Execution','')}
    resultTuple = self.result['Value']

    status = resultTuple[0]
    # stdOutput = resultTuple[1]
    # stdError = resultTuple[2]
    self.log.info( "Status after the application execution is %s" % str( status ) )
    failed = False
    if status != 0:
      self.log.error( "Whizard execution completed with errors:" )
      failed = True
    else:
      self.log.info( "Whizard execution completed successfully")

    if failed==True:
      self.log.error( "==================================\n StdError:\n" )
      self.log.error( self.stdError )
      self.setApplicationStatus('%s Exited With Status %s' %(self.applicationName,status))
      self.log.error('Whizard Exited With Status %s' %(status))
      return S_ERROR('Whizard Exited With Status %s' %(status))

    self.setApplicationStatus('Whizard %s Successful' %(self.applicationVersion))
    return S_OK('Whizard %s Successful' %(self.applicationVersion))
    
  def redirectLogOutput(self, fd, message):
    """Catch the output from the application
    """
    sys.stdout.flush()
    if message:
      if re.search('Writing events to file',message): print message
    if self.applicationLog:
      log = open(self.applicationLog,'a')
      log.write(message+'\n')
      log.close()
    else:
      self.log.error("Application Log file not defined")
    if fd == 1:
      self.stdError += message    