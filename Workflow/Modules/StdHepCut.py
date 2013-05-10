'''
Run the StdHepCut utility

Apply a set of cuts on input stdhep files

@since: May 11, 2011

@author: Stephane Poss
'''
from DIRAC                                                import S_OK, S_ERROR, gLogger
from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import getSoftwareFolder
from DIRAC.Core.Utilities.Subprocess                      import shellCall
from ILCDIRAC.Core.Utilities.PrepareOptionFiles           import GetNewLDLibs
from ILCDIRAC.Core.Utilities.FindSteeringFileDir          import getSteeringFileDirName
from ILCDIRAC.Core.Utilities.resolvePathsAndNames         import getProdFilename

import os, shutil

class StdHepCut(ModuleBase):
  """ Apply cuts on stdhep files, based on L. Weuste utility.
  """
  def __init__(self):
    super(StdHepCut, self).__init__()
    self.log = gLogger.getSubLogger( "StdhepCut" )
    self.applicationName = 'stdhepCut'
    self.STEP_NUMBER = ''
    self.SteeringFile = ''
    self.MaxNbEvts = 0
    
  def applicationSpecificInputs(self):
    if self.step_commons.has_key('CutFile'):
      self.SteeringFile = self.step_commons['CutFile']
  
    if self.step_commons.has_key('MaxNbEvts'):
      self.MaxNbEvts = self.step_commons['MaxNbEvts']
      
    if not self.OutputFile:
      dircont = os.listdir("./")
      for myfile in dircont:
        if myfile.count(".stdhep"):
          self.OutputFile = myfile.rstrip(".stdhep") + "_reduced.stdhep"
          break
      if not self.OutputFile:
        return S_ERROR("Could not find suitable OutputFile name")
      
    if self.workflow_commons.has_key("IS_PROD"):
      if self.workflow_commons["IS_PROD"]:
        #self.OutputFile = getProdFilename(self.OutputFile,int(self.workflow_commons["PRODUCTION_ID"]),
        #                                  int(self.workflow_commons["JOB_ID"]))
        if self.workflow_commons.has_key('ProductionOutputData'):
          outputlist = self.workflow_commons['ProductionOutputData'].split(";")
          for obj in outputlist:
            if obj.lower().count("_gen_"):
              self.OutputFile = os.path.basename(obj)
              break
        else:
          self.OutputFile = getProdFilename(self.OutputFile,
                                            int(self.workflow_commons["PRODUCTION_ID"]),
                                            int(self.workflow_commons["JOB_ID"]))
    return S_OK()

  def execute(self):
    """ Called from Workflow
    """ 
    self.result = self.resolveInputVariables()
    if not self.result['OK']:
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('StdHepCut should not proceed as previous step did not end properly')

    appDir = self.ops.getValue('/AvailableTarBalls/%s/%s/%s/TarBall'% (self.systemConfig, "stdhepcut", 
                                                                       self.applicationVersion), '')
    if not appDir:
      self.log.error('Could not get info from CS')
      self.setApplicationStatus('Failed finding info from CS')
      return S_ERROR('Failed finding info from CS')
    appDir = appDir.replace(".tgz", "").replace(".tar.gz", "")
    res = getSoftwareFolder(appDir)
    if not res['OK']:
      self.setApplicationStatus('%s: Could not find neither local area not shared area install' % self.applicationName)
      return res
    mySoftDir = res['Value']
        
    new_ld_lib_path = GetNewLDLibs(self.systemConfig, self.applicationName, self.applicationVersion)
    new_ld_lib_path = mySoftDir + "/lib:" + new_ld_lib_path
    if os.path.exists("./lib"):
      new_ld_lib_path = "./lib:" + new_ld_lib_path
    
    self.SteeringFile = os.path.basename(self.SteeringFile)
    if not os.path.exists(self.SteeringFile):
      res = getSteeringFileDirName(self.systemConfig, self.applicationName, self.applicationVersion)
      if not res['OK']:
        return res
      steeringfiledirname = res['Value']
      if os.path.exists(os.path.join(steeringfiledirname, self.SteeringFile)):
        try:
          shutil.copy(os.path.join(steeringfiledirname, self.SteeringFile), "./" + self.SteeringFile )
        except Exception, x:
          return S_ERROR('Failed to access file %s: %s' % (self.SteeringFile, str(x)))  
      
    scriptName = '%s_%s_Run_%s.sh' % (self.applicationName, self.applicationVersion, self.STEP_NUMBER)
    if os.path.exists(scriptName): 
      os.remove(scriptName)
    script = open(scriptName, 'w')
    script.write('#!/bin/sh \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')
    script.write('declare -x PATH=%s:$PATH\n' % mySoftDir)
    script.write('declare -x LD_LIBRARY_PATH=%s\n' % new_ld_lib_path)
    script.write('env | sort >> localEnv.log\n')      
    script.write('echo =============================\n')
    extraopts = ""
    if self.MaxNbEvts:
      extraopts = '-m %s' % self.MaxNbEvts
    comm = "stdhepCut %s -o %s -c %s  *.stdhep\n" % (extraopts, self.OutputFile, self.SteeringFile)
    self.log.info("Running %s" % comm)
    script.write(comm)
    script.write('declare -x appstatus=$?\n')    
    script.write('exit $appstatus\n')
    script.close()
    
    if os.path.exists(self.applicationLog): 
      os.remove(self.applicationLog)
    os.chmod(scriptName, 0755)
    comm = 'sh -c "./%s"' % (scriptName)    
    self.setApplicationStatus('%s %s step %s' % (self.applicationName, self.applicationVersion, self.STEP_NUMBER))
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

    nbevtswritten = -1
    nbevtspassing = 0
    nbevtsread = 0
    logf = file(self.applicationLog, 'r')
    for line in logf:
      line = line.rstrip()
      if line.count('Events kept'):
        nbevtswritten = int(line.split()[-1])
      if line.count('Events passing cuts'):
        nbevtspassing = int(line.split()[-1])
      if line.count('Events total'):
        nbevtsread = int(line.split()[-1])
    logf.close()
    if nbevtswritten > 0 and nbevtspassing > 0 and nbevtsread > 0:
      cut_eff = 1. * nbevtspassing / nbevtsread
      self.log.info('Selection cut efficiency : %s%%' % (100 * cut_eff))
      sel_eff = 1. * nbevtswritten / nbevtspassing
      if nbevtswritten < self.MaxNbEvts:
        self.log.error('Not enough events to fill up')
      if self.workflow_commons.has_key('Luminosity'):
        self.workflow_commons['Luminosity'] = self.workflow_commons['Luminosity'] * sel_eff
      self.workflow_commons['NbOfEvts'] = nbevtswritten
      info = {}
      info['stdhepcut'] = {}
      info['stdhepcut']['Reduction'] = sel_eff
      info['stdhepcut']['CutEfficiency'] = cut_eff
      if 'Info' not in self.workflow_commons:
        self.workflow_commons['Info'] = info
      else:
        self.workflow_commons['Info'].update(info)
    else:
      self.log.error('Not enough events somewhere: read: %s, pass:%s, written:%s' % (nbevtsread, nbevtspassing, 
                                                                                     nbevtswritten))
      status = 1
      
    return self.finalStatusReport(status)
