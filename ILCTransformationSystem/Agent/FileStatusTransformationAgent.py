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
from DIRAC.Resources.Storage.StorageElement import StorageElement

__RCSID__ = "$Id$"

AGENT_NAME = 'ILCTransformation/FileStatusTransformationAgent'
MAXRESET = 10

class FileStatusTransformationAgent( AgentModule ):

  def __init__( self, *args, **kwargs ):
    AgentModule.__init__( self, *args, **kwargs )
    self.name = 'FileStatusTransformationAgent'
    self.enabled = False
    self.transformationTypes = ["Replication"]
    self.transformationStatuses = ["Active"]
    self.seObjDict = {}

    self.fcClient = FileCatalogClient()
    self.tClient = TransformationClient()
    self.reqClient = ReqClient()

  def beginExecution(self):
    """Resets defaults after one cycle
    """
    self.enabled = self.am_getOption('EnableFlag', False)

    self.shifterProxy = self.am_setOption( 'shifterProxy', 'DataManager' )
    self.transformationTypes = self.am_getOption( 'TransformationTypes', ["Replication"] )
    self.transformationStatuses = self.am_getOption( 'TransformationStatuses', ["Active"] )

    return S_OK()

  def execute( self ):

    res = self.getTransformations()
    if not res['OK']:
      self.log.error('Failure to get transformations', res['Message'])
      return S_ERROR( "Failure to get transformations" )

    transformations = res['Value']

    if not transformations:
      self.log.notice('No transformations found with Status %s and Type %s ' % (self.transformationStatuses, self.transformationTypes))
      return S_OK("No transformations found")

    #debug
    #transformations.append({'TransformationID': 401003L})
    #self.log.notice(transformations)

    for trans in transformations:

      transID = trans['TransformationID']

      self.log.notice('Processing Transformation ID: %s' % transID)
      self.log.notice( "Get all tasks for transformation ID %s " % transID )

      res = self.getTransformationTasks(transID)
      if not res['OK']:
        self.log.error('Failure to get Tasks for transformation', res['Message'])
        continue

      tasks = res['Value']
      if not tasks:
        self.log.notice('No Tasks found for transformation %s' % transID)
        continue

      self.log.notice("Number of tasks %d for trans ID %d" %(len(tasks), transID))

      res = self.getRequestIDsForTasks(tasks, transID)
      requestIDs = res['Value']
      if not requestIDs:
        self.log.error("No request IDs found for tasks %s for transformation %s" % (tasks, transID))
        continue

      res = self.getRequestsForTasks(requestIDs)
      requests = res['Value']
      if not requests:
        self.log.notice("No Requests found in RMS for transformation ID %s" % transID)
        continue

      lfns = [f.LFN for request in requests for op in request.__operations__ for f in op.__files__]
      #lfns.append('/ilc/prod/clic/1.4tev/aa_qqll_all/ILD/DST/00004275/000/aa_qqll_all_dst_4275_9441.slcio')

      res = self.getReplicasForLFNs(lfns)
      if not res['OK']:
        self.log.error('Failure to find replicas for LFNs', res['Message'])
        continue
      
      if res['Value']['Failed']:
        self.treatFilesNotInFileCatalog( transID, res['Value']['Failed'] )

      #debug
      #res['Value']['Successful']['/ilc/prod/clic/1.4tev/aa_qqll_all/ILD/DST/00004275/000/aa_qqll_all_dst_4275_9441.slcio']=\
      #{'CERN-SRM':'/ilc/prod/clic/1.4tev/aa_qqll_all/ILD/DST/00004275/000/aa_qqll_all_dst_4275_9441.slcio', \
      #'CERN-DST-EOS':'/ilc/prod/clic/1.4tev/aa_qqll_all/ILD/DST/00004275/000/aa_qqll_all_dst_4275_9441.slcio', \
      #'DESY-SRM':'/ilc/prod/clic/1.4tev/aa_qqll_all/ILD/DST/00004275/000/aa_qqll_all_dst_4275_9441.slcio'}

      if res['Value']['Successful']:
        self.treatFilesFoundInFileCatalog( transID, res['Value']['Successful'])

    return S_OK()

  def getTransformations(self):
    # get all transformation for a given type and statuses
    res = self.tClient.getTransformations(condDict = {'Status' : self.transformationStatuses, 'Type' : self.transformationTypes})
    if not res['OK']:
      return res

    return S_OK(res['Value'])

  def getTransformationTasks(self, transID):
    # get all tasks for a given transID
    res = self.tClient.getTransformationTasks(condDict = {'TransformationID' : transID})
    if not res['OK']:
      return res

    return S_OK(res['Value'])

  def getRequestIDsForTasks(self, tasks, transID):

    requestIDs = []
    for task in tasks:
      requestName = '%08d_%08d' % (transID, task['TaskID'])
      res = self.reqClient.getRequestIDForName( requestName )
      if not res['OK']:
        self.log.error("Failure to get request ID for request name %s" % requestName)
        continue
      requestIDs.append( res['Value'] )

    return S_OK(requestIDs)

  def getRequestsForTasks(self, requestIDs):

    requests = []
    for reqID in requestIDs:
      res = self.reqClient.peekRequest( reqID )
      if not res['OK']:
        self.log.error("Failure to get request data for request ID %s" % reqID)
        continue
      #only consider done and failed requests
      request = res['Value']
      self.log.notice('Status %s '%request.Status)
      if request.Status in ['Done', 'Failed']:
        requests.append( request )

    return S_OK(requests)

  def getReplicasForLFNs(self, lfns):
    res = self.fcClient.getReplicas(lfns)
    if not res['OK']:
      self.log.error('Failure to get Replicas for lfns')
      return res
    
    return S_OK(res['Value'])

  def treatFilesNotInFileCatalog(self, transID, lfns):

    _newLFNStatuses = {}
    for lfn in lfns:
      self.log.notice('No Record found in file catalog for LFN: %s' %lfn)
      # assumption file does not exist on SE
      _newLFNStatuses[lfn] = 'Deleted'

    if not _newLFNStatuses:
      res = self.tClient(transID, newLFNsStatus=_newLFNStatuses)
      if not res['OK']:
        self.log.error('Failed to set statuses for LFNs ', res['Message'])
      else:
        self.log.notice('File Statuses updated Successfully %s' % res['Value'])


  def treatFilesFoundInFileCatalog(self, transID, lfns):

    #key is SE, value is list of LFNs which supposedly exist on SE
    seLfnsDict = {}
    _newLFNStatuses = {}
    _filesToBeRemoved = []

    for lfn in lfns:
      SEsContainingReplicas = [se for se in lfns[lfn]]
      for se in SEsContainingReplicas:
        if not se in seLfnsDict:
          seLfnsDict[se] = list()
        seLfnsDict[se].append(lfn)

    #check if files exists on storage elements
    for se in seLfnsDict:

      seObj = None
      if se not in self.seObjDict:
        self.seObjDict[se] = StorageElement(se)

      seObj = self.seObjDict[se]

      res = seObj.exists(seLfnsDict[se])
      if not res['OK']:
        self.log.error('Failed to determine if Files %s exist on SE %s' % (seLfnsDict[se], se))
        continue

      for lfn in res['Value']['Successful']:
        if not res['Value']['Successful'][lfn]:
          self.log.notice("File %s does not physically exists on SE %s " % (lfn , se))
          #remove replica from catalog
          res = self.fcClient.removeReplica( { lfn:{'SE': se} } )
          if not res['OK']:
            self.log.error('Failed to remove Replica %s on SE %s from File Catalog ' % (lfn, se))
            continue

          self.log.notice('Successfully removed Replica %s on SE %s from File Catalog ' %(lfn, se))
          #maybe send an email?
        else:
          self.log.notice("File %s physically exists on SE %s " % (lfn , se))

    res = self.getReplicasForLFNs(lfns.keys())
    if not res['OK']:
      self.log.error('Failure to find replicas for LFNs', res['Message'])
      return S_ERROR()

    result = res['Value']['Successful']

    for lfn in result:
      if not result[lfn]:
        self.log.notice('LFN %s does not have any replicas, removing file from File Catalog' % lfn)
        _filesToBeRemoved.append(lfn)

    if _filesToBeRemoved:
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
