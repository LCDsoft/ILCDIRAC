""" OutputDataPolicy generates the output data that will be created by a workflow task

    DIRAC assumes an execute() method will exist during usage.
"""

from DIRAC                                          import gLogger
from DIRAC.Interfaces.API.Job                       import Job
from ILCDIRAC.Core.Utilities.ProductionData         import constructProductionLFNs
from ILCDIRAC.Core.Utilities.resolvePathsAndNames   import getProdFilenameFromInput

LOG = gLogger.getSubLogger(__name__)
__RCSID__ = "$Id$"

ILDJOBTYPES = [ 'MCGeneration_ILD',
                'MCSimulation_ILD',
                'MCReconstruction_ILD',
                'MCReconstruction_Overlay_ILD',
                'Split_ILD',
              ]

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
    commons = job._getParameters() #pylint: disable=protected-access
    code = job.workflow.createCode()
    outputList = []
    for line in code.split("\n"):
      if line.count("listoutput"):
        outputList += eval(line.split("#")[0].split("=")[-1]) #pylint: disable=eval-used

    commons['outputList'] = outputList
    commons['PRODUCTION_ID'] = prodID
    commons['JOB_ID'] = jobID
    if inputData:
      commons['InputData'] = inputData

    result = constructProductionLFNs(commons)
    if not result['OK']:
      LOG.error(result['Message'])
      return result

    if commons['JobType'] in ILDJOBTYPES and commons['InputData']:
      for index, outputFile in enumerate( result['Value']['ProductionOutputData'] ):
        outputFileILD = getProdFilenameFromInput( commons['InputData'], outputFile, prodID, jobID )
        result['Value']['ProductionOutputData'][index] = outputFileILD
        LOG.debug("Changed output file name from '%s' to '%s' " % (outputFile, outputFileILD))


    return result
