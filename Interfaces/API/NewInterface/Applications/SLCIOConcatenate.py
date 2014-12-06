"""
SLCIOConcatenate : Helper to concatenate SLCIO files
"""
__RCSID__ = "$Id$"

from ILCDIRAC.Interfaces.API.NewInterface.LCUtilityApplication import LCUtilityApplication
from DIRAC.Core.Workflow.Parameter import Parameter
from DIRAC import S_OK, S_ERROR
import types

class SLCIOConcatenate(LCUtilityApplication):
  """ Helper to concatenate slcio files

  Example:

  >>> slcioconcat = SLCIOConcatenate()
  >>> slcioconcat.setInputFile( ["slcioFile_1.slcio" , "slcioFile_2.slcio" , "slcioFile_3.slcio"] )
  >>> slcioconcat.setOutputFile("myNewSLCIOFile.slcio")

  """
  def __init__(self, paramdict = None):

    super(SLCIOConcatenate, self).__init__( paramdict)
    if not self.Version:
      self.Version = 'HEAD'
    self._modulename = "LCIOConcatenate"
    self.appname = 'lcio'
    self._moduledescription = 'Helper call to concatenate SLCIO files'

  def _applicationModule(self):
    m1 = self._createModuleDefinition()
    m1.addParameter( Parameter( "debug",            False,  "bool", "", "", False, False, "debug mode"))
    return m1

  def _applicationModuleValues(self, moduleinstance):
    moduleinstance.setValue('debug',                       self.Debug)

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

    if not self.OutputFile and self._jobtype =='User' :
      self.setOutputFile('LCIOFileConcatenated.slcio')
      self._log.notice('No output file name specified. Output file : LCIOFileConcatenated.slcio')

    if not self._jobtype == 'User':
      self._listofoutput.append({"outputFile":"@{OutputFile}", "outputPath":"@{OutputPath}",
                                 "outputDataSE":'@{OutputSE}'})

    #res = self._checkRequiredApp()
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
