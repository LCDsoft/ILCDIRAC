# $HeadURL$
# $Id$
'''
LCDDIRAC.Workflow.Modules.MarlinAnalysis Called by Job Agent. 

Created on Feb 9, 2010

@author: sposs
'''
import os,sys,re,string
 
from DIRAC.Core.Utilities.Subprocess                      import shellCall
from DIRAC.Core.DISET.RPCClient                           import RPCClient
from LCDDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from LCDDIRAC.Core.Utilities.CombinedSoftwareInstallation import MySiteRoot
from LCDDIRAC.Core.Utilities.PrepareOptionFiles         import PrepareXMLFile
from DIRAC                                                import S_OK, S_ERROR, gLogger, gConfig
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
    self.result = S_OK()
    if not self.systemConfig:
      self.result = S_ERROR( 'No LCD platform selected' )
    elif not self.applicationLog:
      self.result = S_ERROR( 'No Log file provided' )
    if not self.result['OK']:
      return self.result
    runonslcio = []
    for inputfile in self.inputSLCIO:
      runonslcio.append(os.path.basename(inputfile))
    listofslcio = string.join(runonslcio, ' ')
    
    finalXML = "marlinxml.xml"
    
    res = PrepareXMLFile(finalXML,self.inputXML,listofslcio)
    if not res:
      self.log.error('Something went wrong with XML generation')
      return S_ERROR('Something went wrong with XML generation')
    
    scriptName = 'Marlin_%s_Run_%s.sh' %(self.applicationVersion,self.STEP_NUMBER)
    if os.path.exists(scriptName): os.remove(scriptName)
    script = open(scriptName,'w')
    script.write('#!/bin/sh \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')
    if(os.path.exists("MarlinLibs")):
      if os.environ.has_key('MARLIN_DLL'):
        marlindll = ""
        for d in os.listdir("MarlinLibs"):
          marlindll = marlindll + "MarlinLibs/%s"%d + ":" 
        script.write('export MARLIN_DLL=%s:%s'%marlindll%os.environ['MARLIN_DLL'])
      else:
        marlindll = ""
        for d in os.listdir("MarlinLibs"):
          marlindll = marlindll + "MarlinLibs/%s"%d + ":" 
        script.write('export MARLIN_DLL=%s:'%marlindll)
          
    if os.environ.has_key('LD_LIBRARY_PATH'):
        script.write('export LD_LIBRARY_PATH=./:%s'%os.environ['LD_LIBRARY_PATH'])
    else:
        script.write('export LD_LIBRARY_PATH=./')
        
    if (os.path.exists("MarlinLibs/Marlin")):
      if (os.path.exists(finalXML)):
        #check
        script.write('./MarlinLibs/Marlin -c $1')
        #real run
        script.write('./MarlinLibs/Marlin $1')
        
    
    ###Here fill the blanks
    
    script.write('declare -x appstatus=$?\n')
    script.write('where\n')
    script.write('quit\n')
    script.write('EOF\n')
    script.write('exit $appstatus\n')

    script.close()
    if os.path.exists(self.applicationLog): os.remove(self.applicationLog)

    os.chmod(scriptName,0755)
    comm = 'sh -c "./%s %s"' %scriptName %finalXML
    self.setApplicationStatus('Marlin %s step %s' %(self.applicationVersion,self.STEP_NUMBER))
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
      self.log.error( "Marlin execution completed with errors:" )
      failed = True
    else:
      self.log.info( "Marlin execution completed successfully")

    if failed==True:
      self.log.error( "==================================\n StdError:\n" )
      self.log.error( self.stdError )
      #self.setApplicationStatus('%s Exited With Status %s' %(self.applicationName,status))
      self.log.error('Marlin Exited With Status %s' %(status))
      return S_ERROR('Marlin Exited With Status %s' %(status))
    self.setApplicationStatus('Marlin %s Successful' %(self.applicationVersion))
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