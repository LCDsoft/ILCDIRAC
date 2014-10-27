""" OutputDataPolicy generates the output data that will be created by a workflow task

    DIRAC assumes an execute() method will exist during usage.
"""
__RCSID__   = "$Id: OutputDataPolicy.py 19570 2010-01-07 08:42:02Z joel $"
__VERSION__ = "$Revision: 1.40 $"

from DIRAC                                          import gLogger
from DIRAC.Interfaces.API.Job                       import Job
from ILCDIRAC.Core.Utilities.ProductionData         import constructProductionLFNs

class OutputDataPolicy(object):
  """ This module is called from the TransformationSystem
  """  
  def __init__(self, paramDict):
    self.paramDict = paramDict

  def execute(self):
    """ Execute it.
    """
    jobDescription = self.paramDict['Job']
    prodID = self.paramDict['TransformationID']
    jobID = self.paramDict['TaskID']
    inputData = self.paramDict['InputData']
    
    job = Job(jobDescription)
    commons = job._getParameters()
    code = job.workflow.createCode()
    outputList = []
    for line in code.split("\n"):
      if line.count("listoutput"):
        outputList += eval(line.split("#")[0].split("=")[-1])

    commons['outputList'] = outputList
    commons['PRODUCTION_ID'] = prodID
    commons['JOB_ID'] = jobID
    if inputData:
      commons['InputData'] = inputData

    gLogger.debug(commons)
    result = constructProductionLFNs(commons)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result
