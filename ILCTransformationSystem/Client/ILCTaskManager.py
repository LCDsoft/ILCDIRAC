""" TaskManager contains WorkflowsTasks and RequestTasks modules, for managing jobs and requests tasks
"""
__RCSID__ = "$Id$"

COMPONENT_NAME = 'ILCWorkflowTasks'

from DIRAC import gLogger
from DIRAC.TransformationSystem.Client.TaskManager import WorkflowTasks



class ILCWorkflowTasks( WorkflowTasks ):
  """ Handles jobs
  """

  def __init__( self, transClient = None, logger = None, submissionClient = None, jobMonitoringClient = None,
                outputDataModule = None, jobClass = None, opsH = None ):
    """ Generates some default objects.
        jobClass is by default "DIRAC.Interfaces.API.Job.Job". An extension of it also works:
        VOs can pass in their job class extension, if present
    """

    if not logger:
      logger = gLogger.getSubLogger( 'ILCWorkflowTasks' )

    super( ILCWorkflowTasks, self ).__init__( transClient, logger, submissionClient, 
                                              jobMonitoringClient,
                                              outputDataModule, 
                                              jobClass, opsH )
    
  def _handleInputs( self, oJob, paramsDict ):
    """ set job inputs (+ metadata)
    """
    if 'InputData' in paramsDict:
      if paramsDict['InputData']:
        self.log.verbose( 'Setting input data to %s' % paramsDict['InputData'] )
        lfns = paramsDict['InputData'].split(";") #it comes as one string with;
        final_lfns = []
        for f in lfns:
          lfn = f.split(":")
          final_lfns.append(lfn[0])
          if len(lfn)>1:
            oJob._addJDLParameter( 'StartFrom', lfn[1] )
        oJob.setInputData( ";".join( final_lfns ) )#pass it back as one string
        
        