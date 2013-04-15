''' The Workflow Task Agent takes workflow tasks created in the
    transformation database and submits to the workload management system.
'''

from DIRAC.TransformationSystem.Agent.WorkflowTaskAgent  import WorkflowTaskAgent
from ILCDIRAC.ILCTransformationSystem.Client.ILCTaskManager          import ILCWorkflowTasks

AGENT_NAME = 'Transformation/ILCWorkflowTaskAgent'

class ILCWorkflowTaskAgent( WorkflowTaskAgent ):
  ''' An AgentModule class to submit workflow tasks
  '''
  def __init__( self, *args, **kwargs ):
    ''' c'tor
    '''
    WorkflowTaskAgent.__init__( self, *args, **kwargs )
    self.taskManager = ILCWorkflowTasks( transClient = self.transClient )