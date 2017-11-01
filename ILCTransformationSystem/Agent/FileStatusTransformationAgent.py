"""
declare files deleted that no longer exist on storage.....
"""
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.DataManagementSystem.Client.DataManager import DataManager

__RCSID__ = "$Id$"

AGENT_NAME = 'ILCTransformation/FileStatusTransformationAgent'
MAXRESET = 10

class FileStatusTransformationAgent( AgentModule ):

  def __init__( self, *args, **kwargs ):
    AgentModule.__init__( self, *args, **kwargs )
    self.name = 'FileStatusTransformationAgent'
    
    self.enabled = False
    self.shifterProxy = self.am_setOption( 'shifterProxy', 'DataManager' )

    self.transformationTypes = ["Replication"]
    self.transformationStatuses = ["Active"]
    self.fcClient = FileCatalogClient()
    self.tClient = TransformationClient()
    self.reqClient = ReqClient()
    self.dm = DataManager()

  def beginExecution(self):
    """Resets defaults after one cycle
    """
    self.enabled = self.am_getOption('EnableFlag', False)
    return S_OK()

  def execute( self ):
    # get all transformation for a given type and statuses
    res = self.tClient.getTransformations(condDict = {'Status' : self.transformationStatuses, 'Type' : self.transformationTypes})
    if not res['OK']:
      self.log.error( "Failure to get transformations", res['Message'] )
      return S_ERROR( "Failure to get transformations" )

    transformations = res['Value']

    for trans in transformations:
      # get all tasks for a given transID
      res = self.tClient.getTransformationTasks(condDict = {'TransformationID' : trans['TransformationID']})
      if not res['OK']:
        self.log.error("Failure to get tasks", res['Message'])
        continue
      
      # get requestIDs for tasks
      tasks = res['Value']
      requestIDs = []
      self.log.notice("Number of tasks %d for trans ID %d" %(len(tasks), trans['TransformationID']))
      

      for task in tasks:
        requestName = '%08d_%08d' % (trans['TransformationID'], task['TaskID'])
        res = self.reqClient.getRequestIDForName( requestName )
        if not res['OK']:
          self.log.error("Failure to get request ID for request name %s" % requestName)
          continue
        requestIDs.append( res['Value'] )

      self.log.notice('Number of requests %d' % len(requestIDs))
      for reqID in requestIDs:
        self.log.notice("Trying request with ID %s " % reqID)
        res = reqClient.peekRequest( reqID )
        self.log.notice('processing request with ID %s', reqID)
        if not res['OK']:
          self.log.error("Failure to get request for request ID %s" % reqID)
          continue
        
        request = res['Value']._getJSONData()
        if not request['Operations']:
          self.log.notice("No operations found for request ID %s" % reqID)
          continue

        operations = request['Operations']
        for op in operations:
          files = op._getJSONData()['Files']
          if not files:
            self.log.notice("No files found for operation %s with operation ID %s" % (op.Type, op.OperationID))
            continue
          
          for f in files:
            res = self.fcClient.getReplicas(f.LFN)
            if not res['OK']:
              self.log.error("Failure to get replicas for file %s", f.LFN)
              continue
            
            replica_SEs = []
            if 'Successful' in res['Value'] and f.LFN  in res['Value']['Successful']:
              replica_SEs = [ se for se in res['Value']['Successful'][f.LFN]]
              for se in replicaSEs:
                res = dm.getReplicaIsFile(f.LFN, se)
                if not res['OK']:
                  self.log.error("Failure to determine if the file %s exists physically on %s " % (f.LFN, se))
                  continue
                if 'Failed' in res['Value']:
                  self.log.notice("File %s does not physically exist on Storage Element %s " % (f.LFN, se))
                else:
                  self.log.notice("File %s exists on Storage Element %s " % (f.LFN, se))
