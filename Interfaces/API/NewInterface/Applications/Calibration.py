"""
Application used by the Calibration system, not for user jobs
"""
import types
from ILCDIRAC.Interfaces.API.NewInterface.Applications import Marlin

from DIRAC import S_OK
from DIRAC.Core.Workflow.Parameter import Parameter

__RCSID__ = "$Id$"


class Calibration(Marlin):
  """ Application used in the Calibration System

  .. warn: Not For user jobs
  
  """

  def __init__(self, paramdict=None):

    self.calibrationID = 0
    self.workerID = 0
    self.baseSteeringFile = None
    super(Calibration, self).__init__(paramdict)
    ##Those 5 need to come after default constructor
    self._modulename = 'Calibration'
    self._moduledescription = 'Module to run calibration'
    self.appname = 'marlin'

  def _checkConsistency(self, job=None):

    super(Calibration, self)._checkConsistency(job)

    return S_OK()

  def _applicationModule(self):
    md1 = super(Calibration, self)._applicationModule()

    md1.addParameter(Parameter("calibrationID", '0', "int", "", "", False, False,
                               "calibration ID"))
    md1.addParameter(Parameter("workerID", '0', "int", "", "", False, False,
                               "worker ID"))
    md1.addParameter(Parameter("baseSteeringFile", '', "string", "", "", False, False,
                               "basic steering file for calibration reconstructions"))
    return md1

  def _applicationModuleValues(self, moduleinstance):

    super(Calibration, self)._applicationModuleValues(moduleinstance)

    moduleinstance.setValue("calibrationID", self.calibrationID)
    moduleinstance.setValue("workerID", self.workerID)
    moduleinstance.setValue("baseSteeringFile", self.baseSteeringFile)

  def setCalibrationID(self, calibrationID):
    """ Set calibrationID 

    :param int calibrationID: ID of calibration 
    """
    self._checkArgs({'calibrationID': types.IntType})
    self.calibrationID = calibrationID

  def setWorkerID(self, workerID):
    """ Set workerID 

    :param int workerID: ID of worker node
    """
    self._checkArgs({'workerID': types.IntType})
    self.workerID = workerID
