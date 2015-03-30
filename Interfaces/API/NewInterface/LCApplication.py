""" Linear Collider Application

Allows setting the Steering File dependency, as well as other LC community things

@author: sposs
@since: Nov 1st, 2013
"""

from ILCDIRAC.Interfaces.API.NewInterface.LCUtilityApplication import LCUtilityApplication
from DIRAC.Core.Workflow.Parameter                  import Parameter

from DIRAC import S_OK

__RCSID__ = "$Id$"

class LCApplication(LCUtilityApplication):
  """ LC specific implementation of the applications
  
  """
  def __init__(self, paramdict = None):
    super(LCApplication, self).__init__(paramdict)
    self.Energy = 0
    self.steeringFileVersion = ""
    self.ForgetAboutInput = False
    self._importLocation = "ILCDIRAC.Workflow.Modules"

    
  def setSteeringFileVersion(self, version):
    """ Define the SteeringFile version to use
    """
    self.steeringFileVersion = version
    
    return S_OK()
  
  def setForgetAboutInput(self, flag = True):
    """ Do not overwrite the input set in the SteeringFile
    """
    
    self.ForgetAboutInput = flag
    
    return S_OK()
  
  def _getSpecificAppParameters(self, stepdef):
    """ Overload of Application._getSpecificAppParameter
    """
    stepdef.addParameter(Parameter("ForgetInput",     False, "boolean", "", "", False, False, 
                                   "Do not overwrite input steering"))
    if self.steeringFileVersion:
      stepdef.addParameter(Parameter("SteeringFileVers", "", "string", "", "",  False, False, 
                                     "SteeringFile version to use"))
    return S_OK()
  
  def _setSpecificAppParameters(self, stepinst):
    """ Overload of Application._setSpecificAppParameters
    """
    stepinst.setValue( "ForgetInput",       self.ForgetAboutInput)

    if self.steeringFileVersion:
      stepinst.setValue("SteeringFileVers", self.steeringFileVersion)
      
    return S_OK()
  
  def _doSomethingWithJob(self):
    """ Overloads the Application._doSomethingWithJob
    """
    if self.steeringFileVersion:
      self._job._addSoftware( "steeringfiles", self.steeringFileVersion )
    return S_OK()
#########################################"  
