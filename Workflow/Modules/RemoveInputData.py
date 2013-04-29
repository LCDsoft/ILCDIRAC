"""
Remove input data (used in case of merging
"""

__RCSID__ = "$Id:  $"

from ILCDIRAC.Workflow.Modules.ModuleBase import ModuleBase
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC import S_OK, S_ERROR

class RemoveInputData(ModuleBase):
  """ Remove the input data: to be used when Merging things
  """
  def __init__(self):
    super(RemoveInputData, self).__init__()
    self.rm = ReplicaManager()
    
  def applicationSpecificInputs(self):
    if self.step_commons.has_key('Enable'):
      self.enable = self.step_commons['Enable']
      if not type(self.enable) == type(True):
        self.log.warn('Enable flag set to non-boolean value %s, setting to False' % self.enable)
        self.enable = False
        
    if 'Request' in self.workflow_commons:
      self.request = self.workflow_commons['Request']
    else:
      self.request = RequestContainer()
      self.request.setRequestName('job_%s_request.xml' % self.jobID)
      self.request.setJobID(self.jobID)
      self.request.setSourceComponent("Job_%s" % self.jobID)

    return S_OK('Parameters resolved') 
  
  def execute(self):
    """ Remove the input data, and pass by failover in case of failure
    """ 
    self.result = self.resolveInputVariables()
    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('No output data upload attempted')
    try:
      #Try to remove the file list with failover if necessary
      failover = []
      self.log.info( 'Attempting rm.removeFile("%s")' % ( self.InputData ) )
      result = self.rm.removeFile( self.InputData )
      self.log.verbose( result )
      if not result['OK']:
        self.log.error( 'Could not remove files with message:\n"%s"\n\
        Will set removal requests just in case.' % ( result['Message'] ) )
        failover = self.InputData
      try:
        if result['Value']['Failed']:
          failureDict = result['Value']['Failed']
          if failureDict:
            self.log.info( 'Not all files were successfully removed, see "LFN : reason" below\n%s' % ( failureDict ) )
          failover = failureDict.keys()
      except KeyError:
        self.log.error( 'Setting files for removal request to be the input data: %s' % self.InputData )
        failover = self.InputData
        
      for lfn in failover:
        self.__setFileRemovalRequest( lfn )

      self.workflow_commons['Request'] = self.request

      return S_OK( 'Input Data Removed' )
    except Exception, e:
      self.log.exception( e )
      return S_ERROR( e )
    
    return S_OK() 
  
  def __setFileRemovalRequest( self, lfn ):
    """ Sets a removal request for a file including all replicas.
    """
    self.log.info( 'Setting file removal request for %s' % lfn )
    lastOperationOnFile = self.request._getLastOrder( lfn )
    result = self.request.addSubRequest( {'Attributes':{'Operation':'removeFile',
                                                       'TargetSE':'',
                                                       'ExecutionOrder':lastOperationOnFile + 1}},
                                         'removal' )
    index = result['Value']
    fileDict = {'LFN':lfn, 'Status':'Waiting'}
    result = self.request.setSubRequestFiles( index, 'removal', [fileDict] )
  