#####################################################
# $HeadURL: $
#####################################################
'''
Run TOMATO

@since: Feb 24, 2011

@author: S. Poss
@author: C. B. Lam
'''

__RCSID__ = "$Id: $"

from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation  import getSoftwareFolder
from ILCDIRAC.Workflow.Modules.MarlinAnalysis              import MarlinAnalysis
from ILCDIRAC.Core.Utilities.PrepareOptionFiles            import PrepareTomatoSalad
from ILCDIRAC.Core.Utilities.ResolveDependencies           import resolveDepsTar
from DIRAC                                                 import S_OK, S_ERROR, gLogger
import os, types

class TomatoAnalysis(MarlinAnalysis):
  """ Module to run Tomato: the auTOMated Analysis TOol by C.B. Lam.
  """
  def __init__(self):
    MarlinAnalysis.__init__()
    self.applicationName = "Tomato"
    self.log = gLogger.getSubLogger( "TomatoAnalysis" )
    self.collection = ''
    self.InputFile = []
    
  def applicationSpecificInputs(self):
    """ Implement the application specific inputs defined in ModuleBase
    """
    if self.step_commons.has_key('Collections'):
      self.collection = self.step_commons['Collections']
    
    if self.step_commons.has_key('InputSLCIO'):
      inputf = self.step_commons["InputSLCIO"]
      if not type(inputf) == types.ListType:
        inputf = inputf.split(";")
      self.InputFile = inputf
    
      for files in self.InputData:
        if files.lower().find(".slcio") > -1:
          self.InputFile.append(files)
    return S_OK()  

  def execute(self):
    """ Run the module
    """
    self.result = self.resolveInputVariables()
    if not self.systemConfig:
      self.result = S_ERROR( 'No ILC platform selected' )
    elif not self.applicationLog:
      self.result = S_ERROR( 'No Log file provided' )
    if not self.result['OK']:
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('%s should not proceed as previous step did not end properly' % self.applicationName)

    tomatoDir = self.ops.getValue('/AvailableTarBalls/%s/%s/%s/TarBall' % (self.systemConfig, "tomato", 
                                                                           self.applicationVersion), '')
    if not tomatoDir:
      self.log.error('Could not get Tomato tar ball name, cannot proceed')
      return S_ERROR('Problem accessing CS')
    tomatoDir = tomatoDir.replace(".tgz", "").replace(".tar.gz", "")
    res = getSoftwareFolder(tomatoDir)
    if not res['Value']:
      self.setApplicationStatus('Tomato: Could not find neither local area not shared area install')
      return res
    myTomatoDir = res['Value']

    res = self.prepareMARLIN_DLL(myTomatoDir)
    if not res['OK']:
      self.log.error('Failed building MARLIN_DLL: %s' % res['Message'])
      self.setApplicationStatus('Failed to setup MARLIN_DLL')
      return S_ERROR('Something wrong with software installation')

    self.envdict['MARLIN_DLL'] = res['Value']
    
    deps = resolveDepsTar(self.systemConfig, "tomato", self.applicationVersion)
    for dep in deps:
      if dep.lower().count('marlin'):
        marlindir = dep.replace(".tgz", "").replace(".tar.gz", "")
        res = getSoftwareFolder(marlindir)
        if not res['OK']:
          self.log.error('Marlin was not found in software directory')
          return res
        else:
          self.envdict['MarlinDIR'] = res['Value']
        break

    new_ldlibs = ''
    if os.environ.has_key('LD_LIBRARY_PATH'):
      new_ldlibs = os.path.join(myTomatoDir, 'LDLibs') + ":%s" % os.environ['LD_LIBRARY_PATH']
    else:
      new_ldlibs = os.path.join(myTomatoDir, 'LDLibs')
    self.envdict['LD_LIB_PATH'] = new_ldlibs
 
    res = self.GetInputFiles()
    if not res['OK']:
      self.log.error(res['Message'])
      return res
    listofslcio = res['Value']
 
    finalXML = 'tomato.xml'   
    res = PrepareTomatoSalad(None, finalXML, self.OutputFile, listofslcio, self.collection)
    if not res['OK']:
      self.log.error('Could not prepare the Tomato XML: %s' % res['Message'])
      self.setApplicationStatus('Failed to setup Tomato')
      return S_ERROR('Failed to setup Tomato')
    
    self.result = self.runMarlin(finalXML, self.envdict)
    if not self.result['OK']:
      self.log.error('Something wrong during running: %s' % self.result['Message'])
      self.setApplicationStatus('Error during running %s' % self.applicationName)
      return S_ERROR('Failed to run %s' % self.applicationName)

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

    return self.finalStatusReport(status)
