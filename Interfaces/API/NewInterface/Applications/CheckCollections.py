"""
CheckCollections : Helper to check collections
"""
__RCSID__ = "$Id$"

from ILCDIRAC.Interfaces.API.NewInterface.LCUtilityApplication import LCUtilityApplication
from DIRAC.Core.Workflow.Parameter import Parameter
from DIRAC import S_OK, S_ERROR
import types


class CheckCollections(LCUtilityApplication):
  """ Helper to check collections

  Example:

  >>> check = CheckCollections()
  >>> check.setInputFile( [slcioFile_1.slcio , slcioFile_2.slcio , slcioFile_3.slcio] )
  >>> check.setCollections( ["some_collection_name"] )

  """
  def __init__(self, paramdict = None):
    self.collections = []
    super(CheckCollections, self).__init__( paramdict )
    if not self.version:
      self.version = 'HEAD'
    self._modulename = "CheckCollections"
    self.appname = 'lcio'
    self._moduledescription = 'Helper call to define Overlay processor/driver inputs'

  def setCollections(self, collectionList):
    """ Set collections. Must be a list

    @param collectionList: Collections. Must be a list
    @type collectionList: list

    """
    self._checkArgs( { 'collectionList' : types.ListType } )

    self.collections = collectionList
    return S_OK()


  def _applicationModule(self):
    m1 = self._createModuleDefinition()
    m1.addParameter( Parameter( "collections",         [], "list", "", "", False, False, "Collections to check for" ) )
    m1.addParameter( Parameter( "debug",            False, "bool", "", "", False, False, "debug mode"))
    return m1


  def _applicationModuleValues(self, moduleinstance):
    moduleinstance.setValue('collections',             self.collections)
    moduleinstance.setValue('debug',                   self.debug)

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
    """ Checks that all needed parameters are set
    """

    if not self.collections :
      return S_ERROR('No collections to check')

    res = self._checkRequiredApp()
    if not res['OK']:
      return res

    return S_OK()

  def _resolveLinkedStepParameters(self, stepinstance):
    if type(self._linkedidx) == types.IntType:
      self._inputappstep = self._jobsteps[self._linkedidx]
    if self._inputappstep:
      stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
    return S_OK()
