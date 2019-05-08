'''
New Job class, for the new interface. This job class should not be used to create jobs. 
Use :mod:`~ILCDIRAC.Interfaces.API.NewInterface.UserJob` or :mod:`~ILCDIRAC.Interfaces.API.NewInterface.ProductionJob`.

:author: Stephane Poss
:author: Remi Ete
:author: Ching Bon Lam
'''

import inspect

from DIRAC.Interfaces.API.Job                          import Job as DiracJob
from DIRAC.Core.Utilities.PromptUser                   import promptUser
from DIRAC.Core.Workflow.Step                          import StepDefinition

from DIRAC import S_ERROR, S_OK, gLogger

__RCSID__ = "$Id$"

#pylint: disable=protected-access

LOG = gLogger.getSubLogger(__name__)

class Job(DiracJob):
  """ ILCDIRAC job class
  
  Inherit most functionality from DIRAC Job class
  """
  def __init__(self, script = None):
    super(Job, self).__init__(script)
    #DiracJob.__init__(self, script)
    self.applicationlist = []
    self.inputsandbox = []
    self.outputsandbox = []
    self.check = True
    self.steps = []
    self.nbevts = 0
    self.energy = 0
    self.oktosubmit = False
    self.setPlatform('x86_64-slc5-gcc43-opt')


  def setSystemConfig(self, platform):
    """Deprecation warning for setSystemConfig

    .. deprecated:: v23r0

    use :func:`setPlatform`

    """
    LOG.error("""WARNING: setSystemConfig has been deprecated! use

                  setPlatform(platform)

                  instead""")
    self.setPlatform(platform)


  def setInputData(self, lfns):
    """ Overload method to cancel it
    """
    return self._reportError('%s does not implement setInputData' % self.__class__.__name__)
  def setInputSandbox(self, files):
    """ Overload method to cancel it
    """
    return self._reportError('This job class does not implement setInputSandbox')
  def setOutputData(self, lfns, _OutputPath = '', _OutputSE = '' ):
    """ Overload method to cancel it
    """
    return self._reportError('This job class does not implement setOutputData')
  def setOutputSandbox(self, files):
    """ Overload method to cancel it
    """
    return self._reportError('This job class does not implement setOutputSandbox')
  
  def setIgnoreApplicationErrors(self):
    """ Set a flag for all applications that they should not care about errors
    """
    self._addParameter(self.workflow, 'IgnoreAppError', 'JDL', True, 'To ignore application errors')
    return S_OK()
  
  def dontPromptMe(self):
    """ Call this function to automatically confirm job submission

    >>> job = UserJob()
    >>> job.dontPromptMe()
    >>> ...
    >>> job.submit(diracInstance)
    
    """
    self.check = False
    return S_OK()

  def submit(self, _dirac = None, _mode = 'wms'):
    """ Method to submit the job. Not doing anything by default, so that ProductionJobs 
    cannot be submitted by mistake
    """
    return self._reportError("Submit is not available for this job class")
  
  def checkparams(self, dirac = None):
    """ Check job consistency instead of submitting

    :param dirac: DiracILC instance
    :type dirac: :mod:`~ILCDIRAC.Interfaces.API.DiracILC`
    """
    if not dirac:
      return S_ERROR("Missing dirac instance")
    res = self._addToWorkflow()
    if not res['OK']:
      return res
    self.oktosubmit = True    
    return dirac.checkparams(self)
      
  def _askUser(self):
    """ Private function
    
    Called from :mod:`~ILCDIRAC.Interfaces.API.DiracILC` class to prompt the user
    """
    if not self.check:
      return S_OK()
    for app in self.applicationlist:
      LOG.notice(app)
      app.listAttributes()
      LOG.notice("\n")
    res = promptUser('Proceed and submit job(s)?', logger=LOG)
    if not res['OK']:
      return S_ERROR("User did not validate")
    if res['Value'] == 'n':
      return S_ERROR("User did not validate")
    # no more debug output in further loops
    self.check = False
    return S_OK()

  def setConfigPackage(self, appName, version):
    """Define the config package to obtain at job run time.

    :param str appName: name of the ConfigPackage, e.g. 'ClicConfig'
    :param str version: version of the ConfigPackage
    """
    self._addSoftware(appName.lower(), version)
    self._addParameter(self.workflow, appName + 'Package', 'JDL', appName + version, appName + 'package')
    return S_OK()

  def setCLICConfig(self, version):
    """Define the CLIC Configuration package to obtain.

    Copies steering files from CLIC Configuration folder to working directory

    :param str version: version string, e.g.: 'ILCSoft-2017-07-27'
    """
    return self.setConfigPackage('ClicConfig', version)

  def append(self, application):
    """ Helper function
    
    This is the main part: call for every application

    :param application: Application instance
    :type application: :mod:`~ILCDIRAC.Interfaces.API.NewInterface.Application`
    """
    
    res = application._analyseJob(self)
    if not res['OK']:
      return res

    res = application._checkConsistency( self )
    if not res['OK']:
      LOG.error("%s failed to check its consistency:" % application, "%s" % (res['Message']))
      return S_ERROR("%s failed to check its consistency: %s" % (application, res['Message']))
    
    res = self._jobSpecificParams(application)
    if not res['OK']:
      LOG.error("%s failed job specific checks:" % application, "%s" % (res['Message']))
      return S_ERROR("%s failed job specific checks: %s" % (application, res['Message']))

    res = application._checkFinalConsistency()
    if not res['OK']:
      LOG.error("%s failed to check its consistency:" % application, "%s" % (res['Message']))
      return S_ERROR("%s failed to check its consistency: %s" % (application, res['Message']))
    
    ### Once the consistency has been checked, we can add the application to the list of apps.
    self.applicationlist.append(application)
    ##Get the application's sandbox and add it to the job's
    for isb in application.inputSB:
      if isb not in self.inputsandbox:
        self.inputsandbox.append(isb)
    #self.inputsandbox.extend(application.inputSB)

    ##Now we can create the step and add it to the workflow
    #First we need a unique name, let's use the application name and step number
    #stepname = "%s_step_%s"%(application.appname,self.stepCount)
    #stepdefinition = StepDefinition(stepname)
    #self.steps.append(stepdefinition)

    ##Set the modules needed by the application
#    res = self._jobSpecificModules(application,stepdefinition)
#    if not res['OK']:
#      LOG.error("Failed to add modules: %s"%res['Message'])
#      return S_ERROR("Failed to add modules: %s"%res['Message'])
#  
#    ### add the parameters to  the step
#    res = application._addParametersToStep(stepdefinition)
#    if not res['OK']:
#      LOG.error("Failed to add parameters: %s"%res['Message'])
#      return S_ERROR("Failed to add parameters: %s"%res['Message'])   
#      
#    ##Now the step is defined, let's add it to the workflow
#    self.workflow.addStep(stepdefinition)
#    
#    ###Now we need to get a step instance object to set the parameters' values
#    stepInstance = self.workflow.createStepInstance(stepdefinition.getType(),stepname)
#
#    ##Set the parameters values to the step instance
#    res = application._setStepParametersValues(stepInstance)
#    if not res['OK']:
#      LOG.error("Failed to resolve parameters values: %s"%res['Message'])
#      return S_ERROR("Failed to resolve parameters values: %s"%res['Message'])   
#    
#    res = application._resolveLinkedStepParameters(stepInstance)
#    if not res['OK']:
#      LOG.error("Failed to resolve linked parameters: %s"%res['Message'])
#      return S_ERROR("Failed to resolve linked parameters: %s"%res['Message'])
#    #Now prevent overwriting of parameter values.
#    application._addedtojob()
#  
#    self._addParameter(self.workflow, 'TotalSteps', 'String', self.stepCount, 'Total number of steps')
    if application.numberOfEvents:
      self._addParameter(self.workflow, 'NbOfEvts', 'int', application.numberOfEvents, "Number of events to process")
  
    ##Finally, add the software packages if needed
    if application.appname and application.version:
      self._addSoftware(application.appname, application.version)

    ## Pass ApplicationErrors to job, will be checked at submission time
    self.errorDict.update( application._errorDict )

    return S_OK()
  
  def _addToWorkflow(self):
    """ This is called just before submission. It creates the actual workflow. 
    The linking of parameters can only be done here
    """
    for application in self.applicationlist:
      #Start by defining step number 
      self.stepCount += 1
      
      res = application._analyseJob(self)
      if not res['OK']:
        return res
    
      res = application._checkWorkflowConsistency()
      if not res['OK']:
        LOG.error("%s failed to check its consistency:" % application, "%s" % res['Message'])
        return S_ERROR("%s failed to check its consistency: %s" % (application, res['Message']))
      
      ##Now we can create the step and add it to the workflow
      #First we need a unique name, let's use the application name and step number
      stepname = "%s_step_%s" % (application.appname, self.stepCount)
      stepdefinition = StepDefinition(stepname)
      self.steps.append(stepdefinition)
      
      ##Set the modules needed by the application
      res = self._jobSpecificModules(application, stepdefinition)
      if not res['OK']:
        LOG.error("Failed to add modules:", "%s" % res['Message'])
        return S_ERROR("Failed to add modules: %s" % res['Message'])
  
      ### add the parameters to  the step
      res = application._addParametersToStep(stepdefinition)
      if not res['OK']:
        LOG.error("Failed to add parameters:", "%s" % res['Message'])
        return S_ERROR("Failed to add parameters: %s" % res['Message'])   
      
      ##Now the step is defined, let's add it to the workflow
      self.workflow.addStep(stepdefinition)
    
      ###Now we need to get a step instance object to set the parameters' values
      stepInstance = self.workflow.createStepInstance(stepdefinition.getType(), stepname)

      ##Set the parameters values to the step instance
      res = application._setStepParametersValues(stepInstance)
      if not res['OK']:
        LOG.error("Failed to resolve parameters values:", "%s" % res['Message'])
        return S_ERROR("Failed to resolve parameters values: %s" % res['Message'])   
    
      res = application._resolveLinkedStepParameters(stepInstance)
      if not res['OK']:
        LOG.error("Failed to resolve linked parameters:", "%s" % res['Message'])
        return S_ERROR("Failed to resolve linked parameters: %s" % res['Message'])
      #Now prevent overwriting of parameter values.
      application._addedtojob()
  
      self._addParameter(self.workflow, 'TotalSteps', 'String', self.stepCount, 'Total number of steps')
      
    return S_OK()
  
  def _jobSpecificModules(self, application, step): #pylint: disable=no-self-use
    """ Returns the list of the job specific modules for the passed application. Is overloaded in 
    ProductionJob class. UserJob uses the default.
    """
    return application._userjobmodules(step)

  def _jobSpecificParams(self, application):
    """ Every type of job has to reimplement this method. By default, just set the log file if not 
    provided and the energy.
    """
    if not application.logFile:
      logf = application.appname
      if application.version:
        logf += "_" + application.version
      logf += "_Step_%s.log" % (len(self.applicationlist)+1)
      application.setLogFile(logf)
    
    if self.energy:
      if not application.energy:
        application.setEnergy(self.energy)
      elif application.energy != self.energy:
        return S_ERROR("You have to use always the same energy per job.")
    else:
      if application.energy:
        self.energy = application.energy
      else:
        LOG.warn("Energy not set for this step")
      #  return S_ERROR("Energy must be set somewhere.")
     
    if self.energy:
      self._addParameter(self.workflow, "Energy", "float", self.energy, "Energy used")
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

      if currentApp not in apps.split( ';' ):
        apps += ';' + currentApp

      self._addParameter( self.workflow, swPackages, 'JDL', apps, description )

  def _checkArgs( self, argNamesAndTypes ):
    """ Private method to check the validity of the parameters
    """

    # inspect.stack()[1][0] returns the frame object ([0]) of the caller
    # function (stack()[1]).
    # The frame object is required for getargvalues. Getargvalues returns
    # a tuple with four items. The fourth item ([3]) contains the local
    # variables in a dict.

    args = inspect.getargvalues( inspect.stack()[ 1 ][ 0 ] )[ 3 ]

    #

    for argName, argType in argNamesAndTypes.iteritems():

      if argName not in args:
        self._reportError( 'Method does not contain argument \'%s\'' % argName,
                           __name__,
                           **self._getArgsDict( 1 )
                         )
        continue

      if not isinstance( args[argName], argType):
        self._reportError( 'Argument \'%s\' is not of type %s' % ( argName, argType ),
                           __name__,
                           **self._getArgsDict( 1 )
                         )

  def _getArgsDict( self, level = 0 ): #pylint: disable=no-self-use
    """ Private method
    """

    # Add one to stack level such that we take the caller function as the
    # reference point for 'level'

    level += 1

    #

    args = inspect.getargvalues( inspect.stack()[ level ][ 0 ] )
    adict = {}

    for arg in args[0]:

      if arg == "self":
        continue

      # args[3] contains the 'local' variables

      adict[arg] = args[3][arg]

    return adict
