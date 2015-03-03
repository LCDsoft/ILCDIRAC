"""
 PYTHIA: Second Generator application
"""
__RCSID__ = "$Id$"

from ILCDIRAC.Interfaces.API.NewInterface.LCApplication import LCApplication
from DIRAC import S_OK, S_ERROR

class Pythia(LCApplication):
  """ Call pythia.

  Usage:

  >>> py = Pythia()
  >>> py.setVersion("tt_500gev_V2")
  >>> py.setEnergy(500) #Can look like a duplication of info, but trust me, it's needed.
  >>> py.setNbEvts(50)
  >>> py.setOutputFile("myfile.stdhep")

  """
  def __init__(self, paramdict = None):
    self.eventType = ''
    super(Pythia, self).__init__( paramdict )
    self.appname = 'pythia'
    self._modulename = 'PythiaAnalysis'
    self._moduledescription = 'Module to run PYTHIA'
    self.datatype = 'gen'

  def willCut(self):
    """ You need this if you plan on cutting using L{StdhepCut}
    """
    self.willBeCut = True

  def _applicationModule(self):
    m1 = self._createModuleDefinition()
    return m1

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
    if not self.Version:
      return S_ERROR("Version not specified")

    #Resolve event type, needed for production jobs
    self.eventType = self.Version.split("_")[0]

    if not self.NbEvts:
      return S_ERROR("Number of events to generate not defined")

    if not self.OutputFile:
      return S_ERROR("Output File not defined")

    if not self._jobtype == 'User':
      if not self.willBeCut:
        self._listofoutput.append({"outputFile":"@{OutputFile}", "outputPath":"@{OutputPath}",
                                   "outputDataSE":'@{OutputSE}'})
      self.prodparameters['nbevts'] = self.NbEvts
      self.prodparameters['Process'] = self.eventType

    return S_OK()
