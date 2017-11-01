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
    self.shifterProxy = self.am_setOption( 'shifterProxy', 'DataManager' )

    self.transformationTypes = self.am_getOption( 'TransformationTypes', ["Replication"] )
    self.transformationStatuses = self.am_getOption( 'TransformationStatuses', ["Active"] )
    self.seObjDict = {}

    self.fcClient = FileCatalogClient()
    self.tClient = TransformationClient()
    self.reqClient = ReqClient()

  def beginExecution(self):
    """Resets defaults after one cycle
    """
    self.enabled = self.am_getOption('EnableFlag', False)
    self.transformationTypes = self.am_getOption( 'TransformationTypes', ["Replication"] )
    self.transformationStatuses = self.am_getOption( 'TransformationStatuses', ["Active"] )

    return S_OK()

  def execute( self ):

    if not self.enabled:
      return S_OK()

    res = self.getTransformations()
    if not res['OK']:
      self.log.error('Failure to get transformations', res['Message'])
      return S_ERROR( "Failure to get transformations" )

    transformations = res['Value']
    if not transformations:
      self.log.notice('No transformations found with Status %s and Type %s ' % (self.transformationStatuses, self.transformationTypes))
      return S_OK()

    #d#ebug
    transformations.append({'TransformationID': 401003L})
    self.log.notice(transformations)
    requestIDs = []
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

      res = self.getRequestsForTasks(tasks, transID)
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
      
      self.treatFilesNotInFileCatalog( res['Value']['Failed'] )

      #res['Value']['Successful']['/ilc/prod/clic/1.4tev/aa_qqll_all/ILD/DST/00004275/000/aa_qqll_all_dst_4275_9441.slcio']={'CERN-SRM':'/ilc/prod/clic/1.4tev/aa_qqll_all/ILD/DST/00004275/000/aa_qqll_all_dst_4275_9441.slcio', 'CERN-DST-EOS':'/ilc/prod/clic/1.4tev/aa_qqll_all/ILD/DST/00004275/000/aa_qqll_all_dst_4275_9441.slcio','DESY-SRM':'/ilc/prod/clic/1.4tev/aa_qqll_all/ILD/DST/00004275/000/aa_qqll_all_dst_4275_9441.slcio'}
      self.treatFilesFoundInFileCatalog( res['Value']['Successful'])

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

  def getRequestsForTasks(self, tasks, transID):

    requestIDs = []
    for task in tasks:
      requestName = '%08d_%08d' % (transID, task['TaskID'])
      
      res = self.reqClient.getRequestIDForName( requestName )
      if not res['OK']:
        self.log.error("Failure to get request ID for request name %s" % requestName)
        continue
      requestIDs.append( res['Value'] )

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
    res = self.fcClient.getReplicas(lfns)
    if not res['OK']:
      self.log.error('Failure to get Replicas for lfns')
      return res
    
    return S_OK(res['Value'])

  def treatFilesNotInFileCatalog(self, lfns):

    for lfn in lfns:
      self.log.notice('No Record found in file catalog for LFN: %s' %lfn)
      # check if status of file is deleted, if not ,then set the status 'Deleted'
      # assumption file does not exist on SE

  def treatFilesFoundInFileCatalog(self, lfns):
  
      #key is SE, value is list of LFNs which supposedly exist on SE
      seLfnsDict = {}
      for lfn in lfns:
        SEsContainingReplicas = [se for se in lfns[lfn]]
        for se in SEsContainingReplicas:
          if not se in seLfnsDict:
            seLfnsDict[se] = [lfn]
          else:
            seLfnsDict[se].append(lfn)

      #check if files exists on storage elements
      for se in seLfnsDict:
        seObj = None
        if se in self.seObjDict:
          seObj = self.seObjDict[se]
        else:
          seObj = StorageElement(se)
          self.seObjDict[se] = seObj

        res = seObj.exists(seLfnsDict[se])
        if not res['OK']:
          self.log.error('Failed to determine if LFNs %s exist on SE %s' % (seLfnsDict[se], se))
          continue
      
        for lfn in res['Value']['Successful']:
          if not res['Value']['Successful'][lfn]:
            self.log.notice("LFN %s does not physically exists on SE %s " % (lfn , se))
            self.log.notice(res['Value']['Successful'][lfn])
            #remove from file catalog
            #set file status to deleted
          else:
            self.log.notice("LFN %s physically exists on SE %s " % (lfn , se))
