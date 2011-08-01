'''
Created on Jul 28, 2011

@author: Stephane Poss
'''

from DIRAC import S_OK,S_ERROR, gLogger
import inspect, sys, string, types

class Application:
  def __init__(self,paramdict = None):
    ##Would be cool to have the possibility to pass a dictionary to set the parameters, a bit like the current interface
    self.appname = None
    self.version = None
    self.nbevts = 0
    self.steeringfile = None
    self.inputSB = []
    self.logfile = None
    self.energy = 0
    self.detectortype = None
    self.datatype = None
    self.prodparameters = {}
    self.parameters = {}
    self.inputapp = []
    self.modulename = ''
    self.systemconfig = ''
    self.jobapps = []
    ####Following are needed for error report
    self.log = gLogger
    self.errorDict = {}
    self._setparams(paramdict)
  
  def __repr__(self):
    str  = "%s"%self.appname
    if self.version:
      str += " %s"%self.version
    return str
  
  def _setparams(self,params):
    pass
    
  def setName(self,name):
    """ Define name of application
    """
    self.appname = name
    
  def setVersion(self,version):
    """ Define version to use
    """
    self.version = version
    
  def setSteeringFile(self,steeringfile):
    """ Set the steering file
    """
    self.steeringfile = steeringfile
    self.inputSB.append(steeringfile) 
    
  def setLogFile(self,logfile):
    """ Define application log file
    """
    self.logfile = logfile
  
  def setNbEvts(self,nbevts):
    """ Set the number of evetns to process
    """
    self.nbevts = nbevts  
    
  def setEnergy(self,energy):
    """ Set the energy to use
    """
    self.energy = energy
    
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
  
  def getInputFromApp(self,app):
    """ Called to link applications
    
    >>> mokka = Mokka()
    >>> marlin = Marlin()
    >>> marlin.getInputFromApp(mokka)
    
    """
    self.inputapp.append(app)


########################################################################################
#    More private methods: called by the applications of the jobs, but not by the users
########################################################################################
  def _getParameters(self):
    """ Called from Job class
    """
    return self.parameters
  
  def _checkConsistency(self):
    """ Called from Job Class, overloaded by every class
    """
    return S_OK()

  def _checkRequiredApp(self):
    """ Called by _checkConsistency when relevant
    """
    if self.inputapp:
      for app in self.inputapp:
        if not app in self.jobapps:
          return S_ERROR("job order not correct: Pythia or whizard has to be passed to job.append before stdhepcut")    
    return S_OK()

  def _analyseJob(self,job):
    """ Called from Job, does nothing for the moment but get the system config
    """
    self.job = job
    self.systemconfig = job.systemConfig
    self.jobapps = job.applicationlist
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
