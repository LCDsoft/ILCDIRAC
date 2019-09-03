"""
StdhepCutJava: apply generator level cuts after pythia or whizard
"""
__RCSID__ = "$Id$"

from ILCDIRAC.Interfaces.API.NewInterface.Applications import StdhepCut

class StdhepCutJava(StdhepCut):
  """ Call stdhep cut after whizard of pythia

  Usage:

  >>> py = Pythia()
  ...
  >>> cut = StdhepCutJava()
  >>> cut.getInputFromApp(py)
  >>> cut.setSteeringFile("mycut.cfg")
  >>> cut.setMaxNbEvts(10)
  >>> cut.setNbEvtsPerFile(10)

  """
  def __init__(self, paramdict = None):
    super(StdhepCutJava, self).__init__( paramdict )

    self.appname = 'stdhepcutjava'
    self._modulename = 'StdHepCutJava'
    self._moduledescription = 'Module to cut on Generator (Whizard of PYTHIA) written in java'
    self.datatype = 'gen'
    self.fileMask = '*.stdhep'
