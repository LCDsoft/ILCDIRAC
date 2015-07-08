"""
 ROOT master class
"""
__RCSID__ = "$Id"

from ILCDIRAC.Interfaces.API.NewInterface.LCApplication import LCApplication
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Workflow.Parameter import Parameter
import types
class _Root(LCApplication):
  """ Root principal class. Will inherit in RootExe and RootMacro classes, so don't use this (you can't anyway)!
  """

  def __init__(self, paramdict = None):
    self.arguments = ''
    self.script = None
    super(_Root, self).__init__( paramdict )


  def setScript(self, script):
    """ Base method, overloaded in L{RootScript}
    """
    self._log.error("Don't use this!")
    return S_ERROR("Not allowed here")


  def setMacro(self, macro):
    """ Base method, overloaded in L{RootMacro}
    """
    self._log.error("Don't use this!")
    return S_ERROR("Not allowed here")


  def setArguments(self, args):
    """ Optional: Define the arguments of the script

    @param args: Arguments to pass to the command line call
    @type args: string

    """
    self._checkArgs( { 'args' : types.StringTypes } )
    self.arguments = args
    return S_OK()


  def _applicationModule(self):
    m1 = self._createModuleDefinition()
    m1.addParameter(Parameter("arguments",    "", "string", "", "", False, False, "Arguments to pass to the script"))
    m1.addParameter(Parameter("script",       "", "string", "", "", False, False, "Script to execute"))
    m1.addParameter(Parameter("debug",     False,   "bool", "", "", False, False, "debug mode"))
    return m1


  def _applicationModuleValues(self, moduleinstance):
    moduleinstance.setValue('arguments',   self.arguments)
    moduleinstance.setValue("script",      self.script)
    moduleinstance.setValue('debug',       self.debug)


  def _userjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setUserJobFinalization(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('userjobmodules failed')
    return S_OK()


  def _prodjobmodules(self, stepdefinition):
    res1 = self._setApplicationModuleAndParameters(stepdefinition)
    res2 = self._setOutputComputeDataList(stepdefinition)
    if not res1["OK"] or not res2["OK"] :
      return S_ERROR('prodjobmodules failed')
    return S_OK()

  def _checkConsistency(self):
    """ Checks that script is set.
    """
    if not self.script:
      return S_ERROR("Script or macro not defined")
    if not self.version:
      return S_ERROR("You need to specify the Root version")

    #res = self._checkRequiredApp() ##Check that job order is correct
    #if not res['OK']:
    #  return res

    return S_OK()

  def _checkWorkflowConsistency(self):
    return self._checkRequiredApp()

  def _resolveLinkedStepParameters(self, stepinstance):
    if type(self._linkedidx) == types.IntType:
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
    return S_OK()
