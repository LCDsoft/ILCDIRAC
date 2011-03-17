'''
Created on Feb 24, 2011

@author: sposs
'''
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation  import LocalArea,SharedArea
from ILCDIRAC.Workflow.Modules.MarlinAnalysis              import MarlinAnalysis
from ILCDIRAC.Core.Utilities.PrepareOptionFiles            import PrepareTomatoSalad,GetNewLDLibs
from ILCDIRAC.Core.Utilities.ResolveDependencies           import resolveDepsTar
from DIRAC                                                 import S_OK,S_ERROR,gLogger,gConfig
import os

class TomatoAnalysis(MarlinAnalysis):
  def __init__(self):
    MarlinAnalysis.__init__()
    self.applicationName = "Tomato"
    self.log = gLogger.getSubLogger( "TomatoAnalysis" )
    self.collection = ''
    self.inputSLCIO = ''
    
  def applicationSpecificInputs(self):
    if self.step_commons.has_key('Collections'):
      self.collection = self.step_commons['Collections']
    
    if self.step_commons.has_key('InputSLCIO'):
      self.inputSLCIO =   self.step_commons['InputSLCIO']
    
      inputfiles = self.InputData.split(";")
      for files in inputfiles:
        if files.lower().find(".slcio")>-1:
          self.inputSLCIO += files+";"
      self.inputSLCIO = self.inputSLCIO.rstrip(";")
    return S_OK()  

  def execute(self):
    self.result = self.resolveInputVariables()
    if not self.systemConfig:
      self.result = S_ERROR( 'No ILC platform selected' )
    elif not self.applicationLog:
      self.result = S_ERROR( 'No Log file provided' )
    if not self.result['OK']:
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' %(self.workflowStatus['OK'],self.stepStatus['OK']))
      return S_OK('%s should not proceed as previous step did not end properly'%self.applicationName)

    tomatoDir = gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s/TarBall'%(self.systemConfig,"tomato",self.applicationVersion),'')
    if not tomatoDir:
      self.log.error('Could not get Tomato tar ball name, cannot proceed')
      return S_ERROR('Problem accessing CS')
    tomatoDir = tomatoDir.replace(".tgz","").replace(".tar.gz","")
    mySoftwareRoot = ''
    localArea = LocalArea()
    sharedArea = SharedArea()
    if os.path.exists('%s%s%s' %(localArea,os.sep,tomatoDir)):
      mySoftwareRoot = localArea
    elif os.path.exists('%s%s%s' %(sharedArea,os.sep,tomatoDir)):
      mySoftwareRoot = sharedArea
    else:
      self.setApplicationStatus('Tomato: Could not find neither local area not shared area install')
      return S_ERROR('Missing installation of Tomato!')
    myTomatoDir = os.path.join(mySoftwareRoot,tomatoDir)

    res = self.prepareMARLIN_DLL(myTomatoDir)
    if not res['OK']:
      self.log.error('Failed building MARLIN_DLL: %s'%res['Message'])
      self.setApplicationStatus('Failed to setup MARLIN_DLL')
      return S_ERROR('Something wrong with software installation')

    self.envdict['MARLIN_DLL']=res['Value']
    
    deps = resolveDepsTar(self.systemConfig,"tomato",self.applicationVersion)
    for dep in deps:
      if dep.lower().count('marlin'):
        marlindir = dep.replace(".tgz","").replace(".tar.gz","")
        if not os.path.exists(os.path.join(mySoftwareRoot,marlindir)):
          self.log.error('Marlin was not found in software directory')
          return S_ERROR('Marlin not found')
        else:
          self.envdict['MarlinDIR'] = os.path.join(mySoftwareRoot,marlindir)
        break

    #new_ldlibs = GetNewLDLibs(self.systemConfig,"tomato",self.applicationVersion,mySoftwareRoot)
    new_ldlibs = ''
    if os.environ.has_key('LD_LIBRARY_PATH'):
      new_ldlibs = os.path.join(myTomatoDir,'LDLibs')+":%s"%os.environ['LD_LIBRARY_PATH']
    else:
      new_ldlibs = os.path.join(myTomatoDir,'LDLibs')
    self.envdict['LD_LIB_PATH']=new_ldlibs
 
    res = self.GetInputFiles()
    if not res['OK']:
      self.log.error(res['Message'])
      return res
    listofslcio = res['Value']
 
    finalXML = 'tomato.xml'   
    res= PrepareTomatoSalad(finalXML,self.outputFile,listofslcio,self.collection)
    if not res['OK']:
      self.log.error('Could not prepare the Tomato XML: %s'%res['Message'])
      self.setApplicationStatus('Failed to setup Tomato')
      return S_ERROR('Failed to setup Tomato')
    
    self.result = self.runMarlin(finalXML, self.envdict)
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
    # stdOutput = resultTuple[1]
    # stdError = resultTuple[2]
    self.log.info( "Status after the application execution is %s" % str( status ) )

    return self.finalStatusReport(status)
