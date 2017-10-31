"""
FileStatusTransformationAgent handles the following two cases:

1) The file is still registered in the FileCatalog, but was lost on the storageElement,
   then the replica is removed from the FileCatalog. If there is no other replica then
   file is removed from the FileCatalog and file status is set to Deleted.


2) The file was removed from Storage Element and from the FileCatalog, for example because
   the DataRecoveryAgent removed it as duplicate. It is still part of the replication
   transformation file list however and should be set to deleted
"""

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule

from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.Resources.Storage.StorageElement import StorageElement

__RCSID__ = "$Id$"

AGENT_NAME = 'ILCTransformation/FileStatusTransformationAgent'

class FileStatusTransformationAgent( AgentModule ):
  """ FileStatusTransformationAgent """

  def __init__( self, *args, **kwargs ):
    AgentModule.__init__( self, *args, **kwargs )
    self.name = 'FileStatusTransformationAgent'
    self.enabled = False
    self.shifterProxy = self.am_setOption( 'shifterProxy', 'DataManager' )

    self.transformationTypes = self.am_getOption( 'TransformationTypes', ["Replication"] )
    self.transformationStatuses = self.am_getOption( 'TransformationStatuses', ["Active"] )
    self.seObjDict = {}

    self.fcClient = FileCatalogClient()
    self.tClient = TransformationClient()
    self.reqClient = ReqClient()

  def beginExecution(self):
    """ Reload the configurations before every cycle """
    self.enabled = self.am_getOption('EnableFlag', False)
    self.transformationTypes = self.am_getOption( 'TransformationTypes', ["Replication"] )
    self.transformationStatuses = self.am_getOption( 'TransformationStatuses', ["Active"] )

    return S_OK()

  def execute( self ):
    """ main execution loop of Agent"""
    res = self.getTransformations()
    if not res['OK']:
      self.log.error('Failure to get transformations', res['Message'])
      return S_ERROR( "Failure to get transformations" )

    transformations = res['Value']
    if not transformations:
      self.log.notice('No transformations found with Status %s and Type %s ' % (self.transformationStatuses, self.transformationTypes))
      return S_OK()

    #debug
    #transformations.append({'TransformationID': 401003L})
    #self.log.notice(transformations)

    requestIDs = []
    for trans in transformations:

      transID = trans['TransformationID']
      res = self.processTransformation( transID )
      if not res['OK']:
        self.log.error('Failure to process transformation with ID: %s' % transID)
        continue
    return S_OK()

  def processTransformation(self, transID):
    """ process transformation for a given transformation ID """

    self.log.notice('Processing Transformation ID: %s' % transID)
    self.log.notice( "Get all tasks for transformation ID %s " % transID )

    res = self.getTransformationTasks(transID)
    if not res['OK']:
      self.log.error('Failure to get Tasks for transformation', res['Message'])
      return res

    tasks = res['Value']
    if not tasks:
      self.log.notice('No Tasks found for transformation %s' % transID)
      return res

    self.log.notice("Number of tasks %d for trans ID %d" %(len(tasks), transID))

    res = self.getRequestsForTasks(tasks)
    requests = res['Value']
    if not requests:
      self.log.notice("No Requests found in RMS for transformation ID %s" % transID)
      return res

    lfns = [f.LFN for request in requests for op in request.__operations__ for f in op.__files__]

    res = self.getReplicasForLFNs(lfns)
    if not res['OK']:
      self.log.error('Failure to find replicas for LFNs', res['Message'])
      return res

    if res['Value']['Failed']:
      self.setFileStatusDeleted( transID, res['Value']['Failed'].keys() )

    if res['Value']['Successful']:
      self.treatFilesFoundInFileCatalog( transID, res['Value']['Successful'])

  def getTransformations(self):
    """ returns transformations of a given type and status """
    res = self.tClient.getTransformations(condDict = {'Status' : self.transformationStatuses, 'Type' : self.transformationTypes})
    if not res['OK']:
      return res

    return S_OK(res['Value'])

  def getTransformationTasks(self, transID):
    """ returns all tasks for a given transformation ID"""
    res = self.tClient.getTransformationTasks(condDict = {'TransformationID' : transID})
    if not res['OK']:
      return res

    return S_OK(res['Value'])

  def getRequestsForTasks(self, tasks):
    """ returns all requests with statuses Done,Failed  which belong to the list of given tasks """
    requestIDs = [task['ExternalID'] for task in tasks]
    requests = []
    for reqID in requestIDs:
      res = self.reqClient.peekRequest( reqID )
      if not res['OK']:
        self.log.error("Failure to get request data for request ID %s" % reqID)
        continue
      
      #only consider done and failed requests
      request = res['Value']
      if request.Status in ['Done', 'Failed']:
        requests.append( request )

    return S_OK(requests)

  def getReplicasForLFNs(self, lfns):
    """ returns all replicas for a list of given LFNs"""
    res = self.fcClient.getReplicas(lfns)
    if not res['OK']:
      self.log.error('Failure to get Replicas for lfns')
      return res

    return S_OK(res['Value'])

  def setFileStatusDeleted(self, transID, lfns):
    """ sets transformation file status to Deleted """
    _newLFNStatuses = {lfn: 'Deleted' for lfn in lfns}

    if self.enabled and _newLFNStatuses:
      res = self.tClient.setFileStatusForTransformation(transID, newLFNsStatus=_newLFNStatuses)
      if not res['OK']:
        self.log.error('Failed to set statuses for LFNs ', res['Message'])
      else:
        self.log.notice('File Statuses updated Successfully %s' % res['Value'])

  def getDanglingLFNs(self, se, lfns):
    """ checks if the given files exist on a storage element and returns all files that were lost on SE """
    seObj = None
    if se not in self.seObjDict:
      self.seObjDict[se] = StorageElement(se)
    seObj = self.seObjDict[se]

    res = seObj.exists(lfns)
    if not res['OK']:
      return res

    filesNotFound = [lfn for lfn in res['Value']['Successful'] if not res['Value']['Successful'][lfn]]
    return S_OK(filesNotFound)

  def treatFilesFoundInFileCatalog(self, transID, lfns):
    """ removes replicas from FC if they no longer exist on SE, if there are no replicas for a file then
        the file is deleted from FC and it's status is set to Deleted """
    #key is SE, value is list of LFNs which supposedly exist on SE
    seLfnsDict = {}
    _newLFNStatuses = {}
    _filesToBeRemoved = []

    for lfn, replicas in lfns.items():
      storageElements = replicas.keys()
      for se in storageElements:
        if not se in seLfnsDict:
          seLfnsDict[se] = list()
        seLfnsDict[se].append(lfn)

    #check if files exists on storage elements
    for se, files in seLfnsDict.items():
      res = self.getDanglingLFNs(se, files)
      if not res['OK']:
        self.log.error('Failed to determine if Files %s exist on SE %s' % (files, se))
        continue

      for lfn in res['Value']:
        self.log.notice("File %s does not physically exists on SE %s " % (lfn , se))
        #remove replica from catalog
        if self.enabled:
          res = self.fcClient.removeReplica( { lfn:{'SE': se} } )
          if not res['OK']:
            self.log.error('Failed to remove Replica %s on SE %s from File Catalog ' % (lfn, se))
            continue
          self.log.notice('Successfully removed Replica %s on SE %s from File Catalog ' %(lfn, se))

    res = self.getReplicasForLFNs(lfns.keys())
    if not res['OK']:
      self.log.error('Failure to find replicas for LFNs', res['Message'])
      continue

    result = res['Value']['Successful']

    for lfn in result:
      if not result[lfn]:
        self.log.notice('LFN %s does not have any replicas, removing file from File Catalog' % lfn)
        _filesToBeRemoved.append(lfn)

    if self.enabled and _filesToBeRemoved:
      res = self.fcClient.removeFile( _filesToBeRemoved )
      if not res['OK']:
        self.log.notice('Failed to remove files from File Catalog', res['Message'])

      for lfn in res['Value']['Successful']:
        self.log.notice('File %s successfully removed from File Catalog' % lfn)
        _newLFNStatuses[lfn] = 'Deleted'

      if _newLFNStatuses:
        res = self.tClient.setFileStatusForTransformation(transID, newLFNsStatus=_newLFNStatuses)
        if not res['OK']:
          self.log.error('Failed to set statuses for LFNs ', res['Message'])
        else:
          self.log.notice('File Statuses updated Successfully %s' % res['Value'])
