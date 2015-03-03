'''
Run the StdHepCut utility

Apply a set of cuts on input stdhep files

@since: May 11, 2011

@author: Stephane Poss
'''
__RCSID__ = "$Id$"
from DIRAC                                                import S_OK, S_ERROR, gLogger
from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import getSoftwareFolder
from DIRAC.Core.Utilities.Subprocess                      import shellCall
from ILCDIRAC.Core.Utilities.PrepareOptionFiles           import getNewLDLibs
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
    self.inlineCuts = ""
    self.MaxNbEvts = 0
    self.scriptName = ""
    
  def applicationSpecificInputs(self):

    self.SteeringFile = self.step_commons.get('CutFile', self.SteeringFile)
    self.MaxNbEvts = self.step_commons.get('MaxNbEvts', self.MaxNbEvts)

    if not self.OutputFile:
      dircont = os.listdir("./")
      for myfile in dircont:
        if myfile.count(".stdhep"):
          self.OutputFile = myfile.rstrip(".stdhep") + "_reduced.stdhep"
          break
      if not self.OutputFile:
        return S_ERROR("Could not find suitable OutputFile name")

    if "IS_PROD" in self.workflow_commons and self.workflow_commons["IS_PROD"]:
      #self.OutputFile = getProdFilename(self.OutputFile,int(self.workflow_commons["PRODUCTION_ID"]),
      #                                  int(self.workflow_commons["JOB_ID"]))
      if 'ProductionOutputData' in self.workflow_commons:
        outputlist = self.workflow_commons['ProductionOutputData'].split(";")
        for obj in outputlist:
          if obj.lower().count("_gen_"):
            self.OutputFile = os.path.basename(obj)
            break
      else:
        self.OutputFile = getProdFilename(self.OutputFile,
                                          int(self.workflow_commons["PRODUCTION_ID"]),
                                          int(self.workflow_commons["JOB_ID"]))
          
    self.log.notice("Outputfile: %s" % self.OutputFile)
    if self.inlineCuts:
      cfile = open("cuts_local.txt", "w")
      cfile.write("\n".join(self.inlineCuts.split(";")))
      cfile.close()
      self.SteeringFile = "cuts_local.txt"
    return S_OK()

  def runIt(self):
    """ Called from Workflow
    """ 
    
    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('%s should not proceed as previous step did not end properly' % self.applicationName)

    if not self.OutputFile:
      self.log.error("OutputFile not specified")
      return S_ERROR("OutputFile not specified")

    res = getSoftwareFolder(self.platform, self.applicationName, self.applicationVersion)
    if not res['OK']:
      self.log.error('Application %s was not found in either the local area or shared area' % self.applicationName)
      self.setApplicationStatus('%s: Could not find neither local area not shared area install' % self.applicationName)
      return res
    mySoftDir = res['Value']
        
    self.SteeringFile = os.path.basename(self.SteeringFile)
    if not os.path.exists(self.SteeringFile):
      self.log.verbose('Getting the steering files directory')
      res = getSteeringFileDirName(self.platform, self.applicationName, self.applicationVersion)
      if not res['OK']:
        self.log.error("Could not locate the steering file directory")
        return res
      steeringfiledirname = res['Value']
      if os.path.exists(os.path.join(steeringfiledirname, self.SteeringFile)):
        try:
          shutil.copy(os.path.join(steeringfiledirname, self.SteeringFile), "./" + self.SteeringFile )
        except EnvironmentError, x:
          self.log.error("Failed to get the cuts file")
          return S_ERROR('Failed to access file %s: %s' % (self.SteeringFile, str(x)))  
    cuts  = open(self.SteeringFile, "r")
    cutslines = "".join(cuts.readlines())
    cuts.close()
    self.log.verbose("Content of cuts file: ", cutslines )

    #Create the cut specific run script. Overloaded in the StdhepCutJava
    self.prepareScript(mySoftDir)

    if os.path.exists(self.applicationLog): 
      os.remove(self.applicationLog)
    os.chmod(self.scriptName, 0755)
    comm = 'sh -c "./%s"' % (self.scriptName)    
    self.setApplicationStatus('%s %s step %s' % (self.applicationName, self.applicationVersion, self.STEP_NUMBER))
    self.stdError = ''
    self.result = shellCall(0, comm, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)
    #self.result = {'OK':True,'Value':(0,'Disabled Execution','')}
    resultTuple = self.result['Value']
    if not os.path.exists(self.applicationLog):
      self.log.error("Something went terribly wrong, the log file is not present")
      self.setApplicationStatus('%s failed terribly, you are doomed!' % (self.applicationName))
      if not self.ignoreapperrors:
        self.log.error('Missing log file')
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

  def prepareScript(self, mySoftDir):
    """ Prepare the script
    """
    new_ld_lib_path = getNewLDLibs(self.platform, self.applicationName, self.applicationVersion)
    new_ld_lib_path = mySoftDir + "/lib:" + new_ld_lib_path
    if os.path.exists("./lib"):
      new_ld_lib_path = "./lib:" + new_ld_lib_path
    
      
    self.scriptName = '%s_%s_Run_%s.sh' % (self.applicationName, self.applicationVersion, self.STEP_NUMBER)
    if os.path.exists(self.scriptName): 
      os.remove(self.scriptName)
    script = open(self.scriptName, 'w')
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
    comm = "stdhepCut %s -o %s -c %s  ../*.stdhep\n" % (extraopts, self.OutputFile, self.SteeringFile)
    self.log.info("Running %s" % comm)
    script.write(comm)
    script.write('declare -x appstatus=$?\n')    
    script.write('exit $appstatus\n')
    script.close()
    
