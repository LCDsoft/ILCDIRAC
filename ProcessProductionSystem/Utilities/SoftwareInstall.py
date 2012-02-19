'''
Created on Feb 17, 2012

@author: Stephane Poss
'''

from ILCDIRAC.Interfaces.API.NewInterface.Application import Application
from DIRAC import S_OK,S_ERROR

class SoftwareInstall(Application):
  def __init__(self):
    Application.__init__(self)
    self._modulename = "DummyModule"
    self.appname = "DummyModule"
    self._moduledescription = 'Module to install software'

  def _applicationModule(self):
    m1 = self._createModuleDefinition()  
    return m1
  
  def _applicationModuleValues(self,moduleinstance):
    pass
  
  def _userjobmodules(self,stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    if not res1["OK"]:
      return S_ERROR('userjobmodules failed')
    return S_OK()
  
  def _addParametersToStep(self,stepdefinition):
    res = self._addBaseParameters(stepdefinition)
    if not res["OK"]:
      return S_ERROR("Failed to set base parameters")
    return S_OK()
  
  def _setStepParametersValues(self, instance):
    self._setBaseStepParametersValues(instance)
    return S_OK()
      
  def _checkConsistency(self):
    """ Checks that script and dependencies are set.
    """
    
    return S_OK()  
    