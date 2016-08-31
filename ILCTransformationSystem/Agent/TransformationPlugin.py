"""
Sub class of TransformationPlugin to allow for extending the ILD sim jobs
"""

from DIRAC.TransformationSystem.Agent.TransformationPlugin import TransformationPlugin as DTP
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC import S_OK, S_ERROR

class TransformationPlugin(DTP):
  """
  This plugin is ONLY used when willing to limit the number of tasks to a certain number of files.
  """
  def __init__(self, plugin, transClient = None, dataManager = None):
    super(TransformationPlugin, self).__init__(plugin, transClient, dataManager)

  def _Limited(self):
    """
    Limit the number of tasks created to the MaxNumberOfTasks 
    Get the total number of tasks submitted
    Check if that's bigger than the MaxNumberOfTasks
    Extend by the number of tasks if needed to reach MaxNumberOfTasks
    """
    max_tasks = self.params['MaxNumberOfTasks']
    res = self.util.transClient.getCounters( 'TransformationFiles', ['Status'],
                                             {'TransformationID':self.params['TransformationID']} )
    if not res['OK']:
      return res
    total_used = 0
    for statustup in res['Value']:
      if statustup[0]['Status'] in ['Assigned', 'Processed']:
        total_used += statustup[1]
    if total_used >= max_tasks and max_tasks > 0:
      return S_ERROR('Too many tasks for this transformation')
    res = self.util.groupByReplicas( self.data, self.params['Status'] )
    if not res['OK']:
      return res
    newTasks = []
    for _se, lfns in res['Value']:
      newTasks.append( ( '', lfns ) )
      total_used += 1
      if total_used >= max_tasks and max_tasks > 0:
        break
    return S_OK( newTasks )
    
  def _Sliced(self):
    """ 
    Handle the slicing, in fact do nothing particular
    """
    
    lfns = self.data
    #self.data is the dict of replicas for each file d[lfn]=[replicas]
    newTasks = []
    for lfn in lfns.keys():
      newTasks.append( ( '', [lfn] ) )

    return S_OK( newTasks)  
  
  def _SlicedLimited(self):
    """
    For the Sliced productions
    Limit the number of tasks created to the MaxNumberOfTasks 
    Get the total number of tasks submitted
    Check if that's bigger than the MaxNumberOfTasks
    Extend by the number of tasks if needed to reach MaxNumberOfTasks
    """
    max_tasks = self.params['MaxNumberOfTasks']
    res = self.util.transClient.getCounters( 'TransformationFiles', ['Status'],
                                             {'TransformationID':self.params['TransformationID']} )
    if not res['OK']:
      return res
    total_used = 0
    for statustup in res['Value']:
      if statustup[0]['Status'] in ['Assigned', 'Processed']:
        total_used += statustup[1]
    if total_used >= max_tasks and max_tasks > 0:
      return S_ERROR('Too many tasks for this transformation')
    lfns = self.data
    newTasks = []
    for lfn in lfns.keys():
      newTasks.append( ( '', [lfn] ) )
      total_used += 1
      if total_used >= max_tasks and max_tasks > 0:
        break
    return S_OK( newTasks )
    
  def _BroadcastProcessed( self ):
    """ this plug-in only creates tasks for files which have descendents
    """
    inputFiles = self.data
    self.util.logInfo( "Number of input files before selection: %d " % len( inputFiles ) )

    ## query only a maximum of 200 files in one go
    inputFileLists = breakListIntoChunks( inputFiles.keys(), 200 )

    for ifList in inputFileLists:
      resDesc = self.util.fc.getFileDescendents( ifList, depths=1 )
      self.util.logDebug( "Result from getFileDescendents: %s " % resDesc )
      if not resDesc['OK']:
        return resDesc
      descendents = resDesc['Value']

      for lfn in ifList:
        if lfn not in descendents['Successful']:
          self.util.logDebug( "Removed: %s, not in succesful " % lfn )
          inputFiles.pop( lfn, None )
        elif not descendents['Successful'][lfn]:
          self.util.logDebug( "Removed: %s no descendents" % lfn )
          inputFiles.pop( lfn, None )

      self.util.logWarn( "Failed getDescendents: %s " % descendents['Failed'] )

    self.util.logInfo( "Number of input files after selection: %d " % len( inputFiles ) )

    self.data = inputFiles
    return self._Broadcast()
