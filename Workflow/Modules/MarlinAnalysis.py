'''
LCDDIRAC.Workflow.Modules.MarlinAnalysis Called by Job Agent. 

Created on Feb 9, 2010

@author: sposs
'''
import os,sys,re

from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from LCDDIRAC.Workflow.Modules.ModuleBase                import ModuleBase
from LCDDIRAC.Core.Utilities.CombinedSoftwareInstallation  import MySiteRoot
from LCDDIRAC.Core.Utilities.PrepareSteeringFile import PrepareSteeringFile,MokkaWrapper
from DIRAC                                               import S_OK, S_ERROR, gLogger, gConfig

import DIRAC

class MarlinAnalysis(ModuleBase):
  """ Define the Marlin analysis part of the workflow
  """
  def __init__(self):
    ModuleBase.__init__(self)
    self.enable = True
    self.STEP_NUMBER = ''
    self.debug = True
    self.log = gLogger.getSubLogger( "MokkaAnalysis" )
    self.result = S_ERROR()
    self.inputSLCIO = ''
    self.inputXML=''
    self.inputGEAR =''
    self.jobID = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']
    self.systemConfig = ''
    self.applicationLog = ''
    self.applicationVersion=''
    self.jobType = ''
    
  def resolveInputVariables(self):
    """ Resolve all input variables for the module here.
    """
    if self.workflow_commons.has_key('SystemConfig'):
      self.systemConfig = self.workflow_commons['SystemConfig']
      
    if self.step_commons.has_key('applicationVersion'):
      self.applicationVersion = self.step_commons['applicationVersion']
      self.applicationLog = self.step_commons['applicationLog']
      
    if self.workflow_commons.has_key('JobType'):
      self.jobType = self.workflow_commons['JobType']
      
    if self.step_commons.has_key('inputSlcio'):
      self.inputSLCIO =self.step_commons['inputSlcio']
      
    if self.step_commons.has_key('inputXML'):
      self.inputXML=self.step_commons['inputXML']
      
    if self.step_commons.has_key('inputGEAR'):
      self.inputGEAR=self.step_commons['inputGEAR']
      
    if self.workflow_commons.has_key('JobType'):
      self.jobType = self.workflow_commons['JobType']
      
      
  def execute(self):
    """
    Called by Agent
    """
    self.resolveInputVariables()
    if not self.systemConfig:
      self.result = S_ERROR( 'No LCD platform selected' )
    elif not self.applicationLog:
      self.result = S_ERROR( 'No Log file provided' )

    
    return S_OK('Marlin %s Successful' %(self.applicationVersion))

  def redirectLogOutput(self, fd, message):
    sys.stdout.flush()
    if message:
      if re.search('INFO Evt',message): print message
    if self.applicationLog:
      log = open(self.applicationLog,'a')
      log.write(message+'\n')
      log.close()
    else:
      self.log.error("Application Log file not defined")
    if fd == 1:
      self.stdError += message