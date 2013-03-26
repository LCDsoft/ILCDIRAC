"""
Sub class of TransformationPlugin to allow for extending the ILD sim jobs
"""

from DIRAC.TransformationSystem.Agent.TransformationPlugin import TransformationPlugin as DTP
from DIRAC import S_OK, S_ERROR

class TransformationPlugin(DTP):
  """
  This plugin is ONLY used when illing to limit the number of tasks to a certain number of files.
  """
  def __init__(self, plugin, transClient = None, replicaManager = None):
    DTP.__init__(self, plugin, transClient, replicaManager)
    
  def _Limited(self):
    """
    Limit the number of tasks created to the MaxNumberOfTasks 
    """
    max_tasks = self.params['MaxNumberOfTasks']
    res = self.transClient.getCounters( 'TransformationFiles', ['Status'], 
                                        {'TransformationID':self.params['TransformationID']} )
    if not res['OK']:
      return res
    total_used = 0
    for statustup in res['Value']:
      if statustup[0]['Status'] in ['Assigned', 'Processed']:
        total_used += statustup[1]
    if total_used >= max_tasks:
      return S_ERROR('Too many tasks for this transformation')
    res = self._groupByReplicas()
    if not res['OK']:
      return res
    newTasks = []
    for _se, lfns in res['Value']:
      newTasks.append( ( '', lfns ) )
      total_used += 1
      if total_used >= max_tasks:
        break
    return S_OK( newTasks )
    