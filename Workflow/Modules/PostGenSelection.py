'''
Run the Post generation filter, not the same as :mod:`~ILCDIRAC.Interfaces.API.NewInterface.Applications.StdHepCut`

:since: Dec 20, 2010

:author: sposs
'''

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.Subprocess                      import shellCall
from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from DIRAC                                                import S_OK, S_ERROR, gLogger
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import getSoftwareFolder
from ILCDIRAC.Core.Utilities.resolvePathsAndNames         import getProdFilename

import os

class PostGenSelection(ModuleBase):
  """ Apply cuts after generator (whizard). Used only by JJ Blaising up to now.
  """
  def __init__(self):
    super(PostGenSelection, self).__init__()
    self.STEP_NUMBER = ''
    self.enable = True 
    self.log = gLogger.getSubLogger( "PostGenSelection" )
    self.applicationName = 'PostGenSel'
    self.InputFile = []
    self.NbEvtsKept = 0
    self.stdhepFile = ''
      
  def applicationSpecificInputs(self):
    """ Called from ModuleBase
    """
    if 'NbEvtsKept' in self.step_commons:
      self.NbEvtsKept = self.step_commons['NbEvtsKept']

    if not self.NbEvtsKept:
      return S_ERROR('Nb of events to keep MUST be specified')  
    
    if 'IS_PROD' in self.workflow_commons:
      if self.workflow_commons["IS_PROD"]:
        #self.OutputFile = getProdFilename(self.OutputFile,int(self.workflow_commons["PRODUCTION_ID"]),
        #                                  int(self.workflow_commons["JOB_ID"]))
        if 'ProductionOutputData' in self.workflow_commons:
          outputlist = self.workflow_commons['ProductionOutputData'].split(";")
          for obj in outputlist:
            if obj.lower().count("_gen_"):
              self.InputFile = [os.path.basename(obj)]
              self.OutputFile = self.InputFile[0]
        else:
          if 'WhizardOutput' in self.workflow_commons:
            self.stdhepFile = getProdFilename(self.workflow_commons["WhizardOutput"],
                                              int(self.workflow_commons["PRODUCTION_ID"]),
                                              int(self.workflow_commons["JOB_ID"]))

      if not len(self.InputFile) and len(self.InputData):
        for files in self.InputData:
          if files.lower().count(".stdhep"):
            self.InputFile.append(files)
            break
    
    return S_OK('Parameters resolved')

  def execute(self):
    """ Run it now.
    """
    self.result = self.resolveInputVariables()
    if not self.applicationLog:
      self.result = S_ERROR( 'No Log file provided' )
    if not self.platform:
      self.result = S_ERROR( 'No ILC platform selected' )  
    if not self.result['OK']:
      self.log.error("Failed to resolve input parameters:", self.result["Message"])
      return self.result
    
    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('PostGenSelection should not proceed as previous step did not end properly')

    if 'ROOTSYS' not in os.environ:
      return S_OK('Root environment is not set') 
    res = getSoftwareFolder(self.platform, "postgensel", self.applicationVersion)
    if not res['OK']:
      self.log.error("Failed finding the sofware")
      self.setApplicationStatus('PostGenSel: Could not find neither local area not shared area install')      
      return res
    
    mySoftDir = res['Value']
    InputFile = os.path.basename(self.InputFile[0])
    base_file = InputFile.replace(".stdhep", "")
    
    scriptName = 'PostGenSel_%s_Run_%s.sh' % (self.applicationVersion, self.STEP_NUMBER)
    if os.path.exists(scriptName): 
      os.remove(scriptName)
    script = open(scriptName,'w')
    script.write('#!/bin/sh \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')
    script.write('declare -x PATH=%s:$PATH\n' % mySoftDir)
    script.write('env | sort >> localEnv.log\n')      
    script.write('echo =============================\n')
    script.write('declare -x DEBUG=ON\n')
    script.write('declare -x INDIR=$PWD/\n')
    script.write('declare -x MCGEN=WHIZARD\n')
    comm = "readstdhep 100000 %s\n" % base_file
    self.log.info("Running %s" % comm)
    script.write(comm)
    script.write('declare -x appstatus=$?\n')    
    script.write('exit $appstatus\n')
    script.close()
    os.chmod(scriptName, 0755)
    comm = 'sh -c "./%s"' % (scriptName)    
    self.setApplicationStatus('PostGenSelection_Read %s step %s' % (self.applicationVersion, self.STEP_NUMBER))
    self.stdError = ''
    self.result = shellCall(0, comm, callbackFunction = self.redirectLogOutput, bufferLimit=20971520)
    resultTuple = self.result['Value']
    status = resultTuple[0]
    if not status == 0:
      self.log.error("Reading did not proceed properly")
      self.setApplicationStatus('PostGenSelection_Read Exited With Status %s' % (status))
      return S_ERROR('PostGenSelection_Read Exited With Status %s' % (status))
    
    if not os.path.exists(base_file + ".dat"):
      self.log.error('%s does not exist locally, something went wrong, cannot proceed' % (base_file + ".dat"))
      self.setApplicationStatus('%s not there!' % (base_file + ".dat"))
      return S_ERROR('%s file does not exist' % (base_file + ".dat"))
    
    os.rename(base_file + ".stdhep", base_file + "-old.stdhep")
    
    os.remove(scriptName)
    script = open(scriptName, 'w')
    script.write('#!/bin/sh \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')
    script.write('declare -x PATH=%s:$PATH\n' % mySoftDir)
    script.write('env | sort >> localEnv.log\n')      
    script.write('echo =============================\n')
    script.write('declare -x DEBUG=ON\n')
    script.write('declare -x INDIR=$PWD/\n')
    script.write('declare -x MCGEN=WHIZARD\n')
    comm = 'writestdhep 100000 %s %s > writestdhep.out\n' % (self.NbEvtsKept, base_file)
    self.log.info('Running %s' % comm )
    script.write(comm)
    script.write('declare -x appstatus=$?\n')    
    script.write('exit $appstatus\n')
    script.close()
    os.chmod(scriptName, 0755)
    comm = 'sh -c "./%s"' % (scriptName)    
    self.setApplicationStatus('PostGenSelection_Write %s step %s' % (self.applicationVersion, self.STEP_NUMBER))
    self.stdError = ''
    self.result = shellCall(0, comm, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)
    resultTuple = self.result['Value']
    status = resultTuple[0]
    
    #need to update the number of events kept
    self.workflow_commons["NbOfEvts"] = self.NbEvtsKept
    
    return self.finalStatusReport(status)
  