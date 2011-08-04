'''
Created on Jul 28, 2011

@author: Stephane Poss
'''

from DIRAC.Interfaces.API.Job import Job as DiracJob
from ILCDIRAC.Interfaces.API.NewInterface.Application import Application
from DIRAC import S_ERROR,S_OK
import string

class Job(DiracJob):
  """ ILCDIRAC job class
  
  Inherit most functionality from DIRAC Job class
  """
  def __init__(self):
    DiracJob.__init__(self)
    self.applicationlist = []
    self.inputsandbox = []
    self.check = True
    self.systemConfig = ''
    self.stepnumber = 0
    
  def setInputData(self):
    return self._reportError('%s does not implement setInputData'%self.__name__)
  def setInputSandbox(self):
    return self._reportError('This job class does not implement setInputSandbox')
  def setOuputData(self):
    return self._reportError('This job class does not implement setOutputData')
  def setOutputSandbox(self):
    return self._reportError('This job class does not implement setOutputSandbox')
  
  def setIngnoreApplicationErrors(self):
    """ Helper function
    
    Set a flag for all applications that they should not care about errors
    """
    self._addParameter(self.workflow, 'IgnoreAppError', 'JDL', True, 'To ignore application errors')
    return S_OK()
  
  def dontCheckJob(self):
    """ Helper function
    
    Called by users to remove checking of job.
    """
    self.check = False
      
  def askUser(self):
    """ Called from DiracILC class to prompt the user
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
    """
    #Start by defining step number
    self.stepnumber += 1

    res = application._analyseJob(self)
    if not res['OK']:
      return res
    
    res = application._checkConsistency()
    if not res['OK']:
      return self._reportError("%s failed to check its consistency: %s"%(application,res['Message']))
    
    res = self._jobSpecificParams()
    if not res['OK']:
      return self._reportError("Failed job specific checks")
    

    params = application._getParameters()
    
    return S_OK()

  def _jobSpecificParams(self):
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
