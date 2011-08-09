'''
Created on Jul 28, 2011

@author: Stephane Poss
'''
from DIRAC.Core.Workflow.Module                     import ModuleDefinition

from DIRAC import S_OK,S_ERROR, gLogger
import inspect, sys, string, types, os


class Application:
  """ General application definition. Any new application should inherit from this class.
  """
  #need to define slots
  ## __slots__ = []
  def __init__(self,paramdict = None):
    ##Would be cool to have the possibility to pass a dictionary to set the parameters, a bit like the current interface
    
    #application nane (executable)
    self.appname = None
    #application version
    self.version = None
    #Number of evetns to process
    self.nbevts = 0
    #Steering file (duh!)
    self.steeringfile = None
    #Input sandbox: steering file automatically added to SB
    self.inputSB = []
    #Log file
    self.logfile = None
    #Energy to use (duh! again)
    self.energy = 0
    #Detector type (ILD or SID)
    self.detectortype = None
    #Data type : gen, SIM, REC, DST
    self.datatype = None
    #Prod Parameters: things that appear on the prod details
    self.prodparameters = {}
    
    #Application parameters: used when defining the steps in the workflow
    self.parameters = {}
    self.linkedparameters = {}
        
    #Module name and description: Not to be set by the users, internal call only, used to get the Module objects
    self._modulename = ''
    self._moduledescription = ''
    self._modules = []
    self.importLocation = "ILCDIRAC.Workflow.Modules"
        
    #System Configuration: comes from Job definition
    self._systemconfig = ''
    
    #Internal member: hold the list of the job's application set before self: used when using getInputFromApp
    self._jobapps = []
    self._jobsteps = []
    #input application: will link the OutputFile of the guys in there with the InputFile of the self 
    self._inputapp = []
    #Needed to link the parameters.
    self.inputappstep = None
    
    ####Following are needed for error report
    self.log = gLogger
    self.errorDict = {}
    
    ### Next is to use the setattr method.
    self._setparams(paramdict)
  
  def __repr__(self):
    """ String representation of the application
    """
    str  = "%s"%self.appname
    if self.version:
      str += " %s"%self.version
    return str
  
  def _setparams(self,params):
    """ Try to use setattr(self,param) and raise AttributeError in case it does not work.
    """
    return S_OK()  
    
    
  def setName(self,name):
    """ Define name of application
    """
    self.appname = name
    return S_OK()  
    
  def setVersion(self,version):
    """ Define version to use
    """
    self.version = version
    return S_OK()  
    
  def setSteeringFile(self,steeringfile):
    """ Set the steering file, and add it to sandbox
    """
    self.steeringfile = steeringfile
    if os.path.exists(steeringfile) or steeringfile.lower().count("lfn:"):
      self.inputSB.append(steeringfile) 
    return S_OK()  
    
  def setLogFile(self,logfile):
    """ Define application log file
    """
    self.logfile = logfile
    return S_OK()  
  
  def setNbEvts(self,nbevts):
    """ Set the number of evetns to process
    """
    self.nbevts = nbevts  
    return S_OK()  
    
  def setEnergy(self,energy):
    """ Set the energy to use
    """
    self.energy = energy
    return S_OK()  
    
  def setOutputFile(self,ofile):
    """ Set the output file
    """
    self._checkArgs({ ofile : types.StringTypes } )
    self.parameters['OutputFile']['value']=ofile
    self.prodparameters[ofile]={}
    if self.detectortype:
      self.prodparameters[ofile]['detectortype'] = self.detectortype
    if self.datatype:
      self.prodparameters[ofile]['datatype']= self.datatype
    return S_OK()  
  
  def getInputFromApp(self,app):
    """ Called to link applications
    
    >>> mokka = Mokka()
    >>> marlin = Marlin()
    >>> marlin.getInputFromApp(mokka)
    
    """
    self._inputapp.append(app)
    return S_OK()  


########################################################################################
#    More private methods: called by the applications of the jobs, but not by the users
########################################################################################
  def _getParameters(self):
    """ Called from Job class
    """
    return self.parameters

  def _createModule(self):
    """ Create Module definition. As it's generic code, all apps will use this.
    """
    module = ModuleDefinition(self._modulename)
    module.setDescription(self._moduledescription)
    body = 'from %s.%s import %s\n' % (self.importLocation, self._modulename, self._modulename)
    module.setBody(body)
    return module
  
  def _getUserOutputDataModule(self):
    """ This is separated as not all applications require user specific output data (i.e. GetSRMFile and Overlay)
    
    The UserJobFinalization only runs last. It's called every step, but is running only if last.
    """
    userData = ModuleDefinition('UserJobFinalization')
    userData.setDescription('Uploads user output data files with specific policies.')
    body = 'from %s.%s import %s\n' % (self.importLocation, 'UserJobFinalization', 'UserJobFinalization')
    userData.setBody(body)
    return userData
  
  def _checkConsistency(self):
    """ Called from Job Class, overloaded by every class
    """
    return S_OK()

  def _checkRequiredApp(self):
    """ Called by _checkConsistency when relevant
    """
    if self._inputapp:
      for app in self._inputapp:
        if not app in self._jobapps:
          return S_ERROR("job order not correct: If this app uses some input coming from an other app, the app in question must be passed to job.append() before.")
        else:
          idx = self._jobapps.index(app)
          self.inputappstep = self._jobsteps[idx]
    return S_OK()

  def _analyseJob(self,job):
    """ Called from Job, does nothing for the moment but get the system config
    """
    self.job = job
    self._systemconfig = job.systemConfig
    self._jobapps = job.applicationlist
    self._jobsteps = job.steps
    return S_OK()

  def _checkArgs( self, argNamesAndTypes ):
    """ Private method
    """

    # inspect.stack()[1][0] returns the frame object ([0]) of the caller
    # function (stack()[1]).
    # The frame object is required for getargvalues. Getargvalues returns
    # a typle with four items. The fourth item ([3]) contains the local
    # variables in a dict.

    args = inspect.getargvalues( inspect.stack()[ 1 ][ 0 ] )[ 3 ]

    #

    for argName, argType in argNamesAndTypes.iteritems():

      if not args.has_key(argName):
        self._reportError(
          'Method does not contain argument \'%s\'' % argName,
          __name__,
          **self._getArgsDict( 1 )
        )

      if not isinstance( args[argName], argType):
        self._reportError(
          'Argument \'%s\' is not of type %s' % ( argName, argType ),
          __name__,
          **self._getArgsDict( 1 )
        )

  def _getArgsDict( self, level = 0 ):
    """ Private method
    """

    # Add one to stack level such that we take the caller function as the
    # reference point for 'level'

    level += 1

    #

    args = inspect.getargvalues( inspect.stack()[ level ][ 0 ] )
    dict = {}

    for arg in args[0]:

      if arg == "self":
        continue

      # args[3] contains the 'local' variables

      dict[arg] = args[3][arg]

    return dict

  #############################################################################
  def _reportError( self, message, name = '', **kwargs ):
    """Internal Function. Gets caller method name and arguments, formats the 
       information and adds an error to the global error dictionary to be 
       returned to the user. 
       Stolen from DIRAC Job Class
    """
    className = name
    if not name:
      className = __name__
    methodName = sys._getframe( 1 ).f_code.co_name
    arguments = []
    for key in kwargs:
      if kwargs[key]:
        arguments.append( '%s = %s ( %s )' % ( key, kwargs[key], type( kwargs[key] ) ) )
    finalReport = 'Problem with %s.%s() call:\nArguments: %s\nMessage: %s\n' % ( className, methodName, string.join( arguments, ', ' ), message )
    if self.errorDict.has_key( methodName ):
      tmp = self.errorDict[methodName]
      tmp.append( finalReport )
      self.errorDict[methodName] = tmp
    else:
      self.errorDict[methodName] = [finalReport]
    self.log.verbose( finalReport )
    return S_ERROR( finalReport )
