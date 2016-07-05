"""
Tomato : Helper to filter generator selection
"""
__RCSID__ = "$Id$"

from ILCDIRAC.Interfaces.API.NewInterface.LCApplication import LCApplication
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Workflow.Parameter import Parameter
import types

class Tomato(LCApplication):
  """ Helper application over Tomato analysis

  Example:

  >>> cannedTomato = Tomato()
  >>> cannedTomato.setInputFile ( [pouette_1.slcio , pouette_2.slcio] )
  >>> cannedTomato.setSteeringFile ( MySteeringFile.xml )
  >>> cannedTomato.setLibTomato ( MyUserVersionOfTomato )


  """
  def __init__(self, paramdict = None):

    self.libTomato = ''
    super(Tomato, self).__init__( paramdict )
    self.version = self.version if self.version else 'HEAD'
    self._modulename = "TomatoAnalysis"
    self.appname = 'tomato'
    self._moduledescription = 'Helper Application over Marlin reconstruction'

  def setLibTomato(self, libTomato):
    """ Optional: Set the the optional Tomato library with the user version

    :param string libTomato: Tomato library

    """
    self._checkArgs( { 'libTomato' : types.StringTypes } )

    self.libTomato = libTomato
    return S_OK()


  def _applicationModule(self):
    m1 = self._createModuleDefinition()
    m1.addParameter( Parameter( "libTomato",           '',   "string", "", "", False, False, "Tomato library" ))
    m1.addParameter( Parameter( "debug",            False,     "bool", "", "", False, False, "debug mode"))
    return m1


  def _applicationModuleValues(self, moduleinstance):
    moduleinstance.setValue('libTomato',     self.libTomato)
    moduleinstance.setValue('debug',         self.debug)

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

  def _checkConsistency(self, job=None):
    """ Checks that all needed parameters are set
    """

    if not self.version:
      return S_ERROR("You need to specify which version of Marlin to use.")

    if not self.libTomato :
      self._log.info('Tomato library not given. It will run without it')

    return S_OK()

  def _checkWorkflowConsistency(self):
    return self._checkRequiredApp()

  def _resolveLinkedStepParameters(self, stepinstance):

    if type(self._linkedidx) == types.IntType:
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
    return S_OK()
