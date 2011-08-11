'''
Created on Jul 28, 2011

New Job class, for the new interface.

@author: Stephane Poss
'''

from DIRAC.Interfaces.API.Job                          import Job as DiracJob
from ILCDIRAC.Interfaces.API.NewInterface.Application  import Application
from DIRAC.Core.Workflow.Step                          import StepDefinition
from DIRAC.Core.Workflow.Parameter                     import Parameter 
from DIRAC import S_ERROR,S_OK
import string

class Job(DiracJob):
  """ ILCDIRAC job class
  
  Inherit most functionality from DIRAC Job class
  """
  def __init__(self, script = None):
    DiracJob.__init__(self, script)
    self.applicationlist = []
    self.inputsandbox = []
    self.check = True
    self.systemConfig = ''
    self.stepnumber = 0
    self.steps = []
    
  def setInputData(self, lfns):
    """ Overload method to cancel it
    """
    return self._reportError('%s does not implement setInputData'%self.__name__)
  def setInputSandbox(self, files):
    """ Overload method to cancel it
    """
    return self._reportError('This job class does not implement setInputSandbox')
  def setOuputData(self, lfns, OutputSE = [], OutputPath = '' ):
    """ Overload method to cancel it
    """
    return self._reportError('This job class does not implement setOutputData')
  def setOutputSandbox(self, files):
    """ Overload method to cancel it
    """
    return self._reportError('This job class does not implement setOutputSandbox')
  
  def setIngnoreApplicationErrors(self):
    """ Helper function
    
    Set a flag for all applications that they should not care about errors
    """
    self._addParameter(self.workflow, 'IgnoreAppError', 'JDL', True, 'To ignore application errors')
    return S_OK()
  
  def dontPromptMe(self):
    """ Helper function
    
    Called by users to remove checking of job.
    """
    self.check = False
    return S_OK()
      
  def _askUser(self):
    """ Private function
    
    Called from DiracILC class to prompt the user
    """
    if not self.check:
      return S_OK()
    else:
      """ Ask the user if he wants to proceed
      """
      pass
    return S_OK()
  
  def append(self,application):
    """ Helper function
    
    This is the main part: call for every application
    @param application: Application instance
    
    """
    #Start by defining step number
    self.stepnumber = len(self.steps) + 1
    

    res = application._analyseJob(self)
    if not res['OK']:
      return res
    
    res = application._checkConsistency()
    if not res['OK']:
      return self._reportError("%s failed to check its consistency: %s"%(application,res['Message']))
    
    res = self._jobSpecificParams(application)
    if not res['OK']:
      return self._reportError("Failed job specific checks")
    
    ### Once the consistency has been checked, we can add the application to the list of apps.
    self.applicationlist.append(application)

    ##Get the application's sandbox and add it to the job's
    self.inputsandbox.extend(application.inputSB)

    ##Now we can create the step and add it to the workflow
    #First we need a unique name, let's use the application name and step number
    stepname = "%s_step_%s"%(application.appname,self.stepnumber)
    stepdefinition = StepDefinition(stepname)
    self.steps.append(stepdefinition)

    ##Set the modules needed by the application
    res = self._jobSpecificModules(application,stepdefinition)
    if not res['OK']:
      return self._reportError("Failed to add modules: %s"%res['Message'])
  
    ### add the parameters to  the step
    res = application._addParametersToStep(stepdefinition)
    if not res['OK']:
      return self._reportError("Failed to add parameters: %s"%res['Message'])   
      
    ##Now the step is defined, let's add it to the workflow
    self.workflow.addStep(stepdefinition)
    
    ###Now we need to get a step instance object to set the parameters' values
    stepInstance = self.workflow.createStepInstance(stepdefinition.getType(),stepname)

    ##Set the parameters values to the step instance
    res = application._setStepParametersValues(stepInstance)
    if not res['OK']:
      return self._reportError("Failed to resolve parameters values: %s"%res['Message'])   
    
    ##stepInstance.setLink("InputFile",here lies the step name of the linked step, maybe get it from the application,"OutputFile")
    res = application._resolveLinkedStepParameters(stepInstance)
    if not res['OK']:
      return self._reportError("Failed to resolve linked parameters: %s"%res['Message'])
  
    ##Finally, add the software packages if needed
    if application.appname and application.version:
      self._addSoftware(application.appname, application.version)
      
    return S_OK()
  
  def _jobSpecificModules(self,application,step):
    """ Returns the list of the job specific modules for the passed application. Is overloaded in ProductionJob class. UserJob uses the default.
    """
    return application._userjobmodules(step)

  def _jobSpecificParams(self,application):
    """ Every type of job has to reimplement this method
    """
    return S_OK()

  def _addSoftware( self, appName, appVersion ):
    """ Private method
    """

    currentApp  = "%s.%s" % ( appName.lower(), appVersion )
    swPackages  = 'SoftwarePackages'
    description = 'ILC Software Packages to be installed'

    if not self.workflow.findParameter( swPackages ):
      self._addParameter( self.workflow, swPackages, 'JDL', currentApp, description )
    else:
      apps = self.workflow.findParameter( swPackages ).getValue()

      if not currentApp in string.split( apps, ';' ):
        apps += ';' + currentApp

      self._addParameter( self.workflow, swPackages, 'JDL', apps, description )
