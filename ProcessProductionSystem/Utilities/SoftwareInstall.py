'''
Created on Feb 17, 2012

:author: Stephane Poss
'''
#pylint: skip-file

from ILCDIRAC.Interfaces.API.NewInterface.Application import Application
from DIRAC.Core.Workflow.Parameter import Parameter
from DIRAC import S_OK, S_ERROR

class SoftwareInstall( Application ):
  def __init__(self):
    self.appsToInstall = ''
    self.appsToRemove = ''
    Application.__init__(self)
    self._modulename = "InstallSoftModule"
    self.appname = "InstallSoftModule"
    self._moduledescription = 'Module to install software'
    self._importLocation = "ILCDIRAC.ProcessProductionSystem.Utilities"

  def toInstall(self, apps):
    """ Software to install
    """
    self.appsToInstall = ";".join(apps)

  def toRemove(self, apps):
    """ Software to remove
    """
    self.appsToRemove = ";".join(apps)

  def _applicationModule(self):
    m1 = self._createModuleDefinition()  
    m1.addParameter(Parameter("appsToInstallStr",  "", "string", "", "", False, False, "Apps to install"))
    m1.addParameter(Parameter("appsToRemoveStr",   "", "string", "", "", False, False, "Apps to remove"))
    return m1
  
  def _applicationModuleValues(self, moduleinstance):
    moduleinstance.setValue("appsToInstallStr", self.appsToInstall)
    moduleinstance.setValue("appsToRemoveStr", self.appsToRemove)
  
  def _userjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    if not res1["OK"]:
      return S_ERROR('userjobmodules failed')
    return S_OK()
  
  def _addParametersToStep(self, stepdefinition):
    res = self._addBaseParameters(stepdefinition)
    if not res["OK"]:
      return S_ERROR("Failed to set base parameters")
    return S_OK()
  
  def _setStepParametersValues(self, instance):
    self._setBaseStepParametersValues(instance)
    return S_OK()
      
  def _checkConsistency(self, job=None):
    """ Checks that script and dependencies are set.
    """
    
    return S_OK()  
    