"""
FST Agent DESCRIPTION HEREE
"""

import json

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule

from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.Resources.Storage.StorageElement import StorageElement

__RCSID__ = "$Id$"

AGENT_NAME = 'ILCTransformation/FileStatusTransformationAgent'

ACTION_PROCESSED = 'set_processed'
ACTION_DELETED = 'set_deleted'

ACTION_RETRY = 'retry'
ACTION_UNUSED = 'set_unused'
ACTION_RESET_REQUEST = 'reset_request'

REPLICATION_TRANS = 'Replication'
MOVING_TRANS = 'Moving'

class FileStatusTransformationAgent( AgentModule ):
  """ FileStatusTransformationAgent """

  def __init__( self, *args, **kwargs ):
    AgentModule.__init__( self, *args, **kwargs )
    self.name = 'FileStatusTransformationAgent'
    self.enabled = False
    self.transformationTypes = ["Replication"]
    self.transformationStatuses = ["Active"]
    self.transformationFileStatuses = ["Assigned", "Problematic", "Processed", "Unused"]
    self.seObjDict = {}

    self.fcClient = FileCatalogClient()
    self.tClient = TransformationClient()
    self.reqClient = ReqClient()

  def beginExecution(self):
    """ Reload the configurations before every cycle """
    self.enabled = self.am_getOption('EnableFlag', False)
    self.transformationTypes = self.am_getOption( 'TransformationTypes', ["Replication"] )
    self.transformationStatuses = self.am_getOption( 'TransformationStatuses', ["Active"] )
    self.transformationFileStatuses = self.am_getOption( 'TransformationFileStatuses', ["Assigned", "Problematic", "Processed", "Unused"] )

    for status in self.transformationFileStatuses:
      processFileFuncName = "check_%s_files" % (status.lower())
      if not (hasattr(self, processFileFuncName) and callable(getattr(self, processFileFuncName))):
        self.log.warn("Unable to process transformation files with status %s" % status)
        self.transformationFileStatuses.remove(status)

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

    for trans in transformations:

      transID = trans['TransformationID']
      res = self.processTransformation( transID, trans['SourceSE'], trans['TargetSE'] )
      if not res['OK']:
        self.log.error('Failure to process transformation with ID: %s' % transID)
        continue
    return S_OK()

  def getTransformations(self):
    """ returns transformations of a given type and status """
    res = self.tClient.getTransformations(condDict = {'Status' : self.transformationStatuses, 'Type' : self.transformationTypes})
    if not res['OK']:
      return res

    result = res['Value']
    for trans in result:
      res = self.tClient.getTransformationParameters(trans['TransformationID'], ['SourceSE', 'TargetSE'])
      if not res['OK']:
        self.log.error('Failure to get SourceSE and TargetSE parameters for Transformation ID %d' % trans['TransformationID'])
        return res

      if res['Value']['SourceSE']:
        trans['SourceSE'] = eval( res['Value']['SourceSE'])

      if res['Value']['TargetSE']:
        trans['TargetSE'] = eval( res['Value']['TargetSE'])

    return S_OK(result)

  def getTransformationTasks(self, transID):
    """ returns all tasks for a given transformation ID"""
    res = self.tClient.getTransformationTasks(condDict = {'TransformationID' : transID})
    if not res['OK']:
      return res

    return S_OK(res['Value'])

  def getRequestStatus(self, transID, taskIDs):

    res = self.tClient.getTransformationTasks(condDict={'TransformationID':transID, 'TaskID':taskIDs})
    if not res['OK']:
      return res

    result = res['Value']
    requestStatus = []
    for task in result:
      requestStatus.append({task['TaskID']: {'RequestStatus': task['ExternalStatus'], 'RequestID': task['ExternalID']}})

    return S_OK( requestStatus )

  def getDataTransformationType(self, transID):

    res = self.getTransformationParameters(transID, 'Body')
    if not res['OK']:
      return res

    try:
      body = json.loads(res['Value'])
      for operation in body:
        if 'RemoveReplica' in operation:
          return S_OK( MOVING_TRANS )
    except:
      if 'RemoveReplica' in res['Value']:
        return S_OK( MOVING_TRANS )

    return S_OK( REPLICATION_TRANS )


  def setFileStatus(self, transID, lfns, status):
    """ sets transformation file status to Deleted """
    _newLFNStatuses = {lfn: status for lfn in lfns}

    if self.enabled and _newLFNStatuses:
      res = self.tClient.setFileStatusForTransformation(transID, newLFNsStatus=_newLFNStatuses)
      if not res['OK']:
        self.log.error('Failed to set statuses for LFNs ', res['Message'])
      else:
        self.log.notice('File Statuses updated Successfully %s' % res['Value'])

  def selectFailedRequests( self, transFile):

    res = self.getRequestStatus(transID, transFile['TaskID'])
    if not res['OK']:
      self.log.error('Failure to get Request Status for Assigned File')
      return False
    result = res['Value']

    if result[taskID]['ExternalStatus'] == 'Failed':
      return True

    return False

  def retryStategyForFiles(self, transFiles):

    taskIDs = [f['TaskID'] for f in transFiles]]
    res = self.getRequestStatus( transID, taskIDs)
    if not res['OK']:
      self.log.error('Failure to get Request Status')
      return res

    result = {}
    for f in res['Value']:
      res = self.reqClient.peekRequest(f['ExternalID'])
      if not res['OK']:
        self.log.notice('Request does not exist %d setting file status to unused' % reqID)
        result['TaskID'] = ACTION_UNUSED
      else:
        result['TaskID'] = ACTION_RESET_REQUEST

    return S_OK(result)


  def check_assigned_files(self, actions, transFiles, transType):

    for f in trasFiles:
      if f['AvailableOnSource'] and f['AvailableOnTarget']:
        if transType == 'Replication':
          actions[ACTION_PROCESSED].append(f)
        elif transType == 'Moving':
          actions[ACTION_RETRY].append(f)
        else:
          self.log.warn('Unknown TransType %s '%transType)

      elif f['AvailableOnSource'] and not f['AvailableOnTarget']:
        actions[ACTION_RETRY].append(f)

      elif not f['AvailableOnSource'] and f['AvailableOnTarget']:
        actions[ACTION_PROCESSED].append(f)

      else:
        #not on src and target
        actions[ACTION_DELETED].append(f)

  def check_unused_files(self, actions, transFiles, transType):

    for f in transFiles:
      if not f['AvailableOnSource'] and not f['AvailableOnTarget']:
        actions[ACTION_DELETED].append(f)

      if not f['AvailableOnSource'] and f['AvailableOnTarget']:
        actions[ACTION_PROCESSED].append(f)

  def check_processed_files(self, actions, transFiles, transType):

    for f in transFiles:
      if f['AvailableOnSource'] and f['AvailableOnTarget'] and transType is 'Moving':
        actions[ACTION_RETRY].append(f)

      if f['AvailableOnSource'] and not f['AvailableOnTarget']:
        actions[ACTION_RETRY].append(f)

      if not f['AvailableOnSource'] and not f['AvailableOnTarget']:
        actions[ACTION_DELETED].append(f)

  def check_problematic_files(self, actions, transFiles, transType):

    for f in transFiles:
      if f['AvailableOnSource'] and f['AvailableOnTarget'] and transType is 'Moving':
        actions[ACTION_RETRY].append(f)

      elif f['AvailableOnSource'] and not f['AvailableOnTarget']:
        actions[ACTION_RETRY].append(f)

      elif not f['AvailableOnSource'] and f['AvailableOnTarget']:
        actions[ACTION_PROCESSED].append(f)

      else:
        #not available on source and target
        actions[ACTION_DELETED].append(f)


  def applyActions( self, transID, actions):

    for action in actions:
      if action is ACTION_PROCESSED:
        lfns = [f['LFN'] for f in actions[ACTION_PROCESSED]]
        self.setFileStatus( transID, lfns, 'Processed')

      elif action is ACTION_DELETED:
        lfns = [f['LFN'] for f in actions[ACTION_DELETED]]
        self.setFileStatus( transID, lfns, 'Deleted')

      elif action is ACTION_RETRY:
        #if there is a request in RMS then reset request otherwise set file status unused
        res = retryStategyForFiles(action[ACTION_RETRY])
        if not res['OK']:
          self.log.error('Failure to determine retry strategy ( set unused / reset request) for transformation files')
          continue

        retryStrategy = res['value']
        for f in action[ACTION_RETRY]:
          if retryStategy[f['TaskID']] is ACTION_RESET_REQUEST:

            res = self.reqClient.resetFailedRequest(f['ExternalID'])
            if not res['OK']:
              self.log.error('Failure to reset request %s' %f['ExternalID'])
              continue

            res = self.tClient.setTaskStatus( transID, f['TaskID'], 'Waiting' )
            if not res['OK']:
              self.log.error('Failure to set Waiting status for Task ID %d', f['TaskID'])
              continue

          if retryStategy[f['TaskID']] is ACTION_UNUSED:
            self.setFileStatus( transID, f['LFN'], 'Unused')

      else:
        self.log.warn('Unknown action %s' % action)


  def exists(self, SEs, lfns):
    """ checks if the given files exist on a list of storage elements"""

    res = self.fcClient.getReplicas(lfns)
    if not res['OK']:
      return res

    result = res['Value']['Successful']
    filesFound = [lfn for lfn, replicas in result.items() if set(SEs).issubset(replicas.keys())]

    for se in SEs:
      if se not in self.seObjDict:
        self.seObjDict[se] = StorageElement(se)
      seObj = self.seObjDict[se]

      res = seObj.exists(filesFound)
      if not res['OK']:
        return res
      result = res['Value']['Successful']
      filesFound = [lfn for lfn in result if result[lfn]]

    result = {lfn: True if lfn in filesFound else False for lfn in lfns}

    return S_OK( result )

  def processTransformation(self, transID, sourceSE, targetSEs ):
    """ process transformation for a given transformation ID """
    res = self.getDataTransformationType(transID)
    if not res['OK']:
      self.log.error('Failure to determine Data Transformation Type')
      return res

    transType = res['Value']

    actions = {}
    actions[ACTION_PROCESSED] = []
    actions[ACTION_RETRY] = []
    actions[ACTION_DELETED] = []

    for status in self.transformationFileStatuses:
      res = self.tClient.getTransformationFiles(condDict={'TransformationID': transID, 'Status': status})
      if not res['OK']:
        self.log.error('Failure to get Transformation Files with Status %s for Transformation ID %d' % (status, transID))
        return res

      transFiles = res['Value']
      if not transFiles:
        self.log.notice("No Transformation Files found with status %s for Transformation ID %d" %(status, transID))
        continue
      else:
        self.log.notice("Processing Transformation Files with status %s for TransformationID %s " %(status, transID))

      # only process Assigned files with failed requests
      if status == 'Assigned':
        transFiles = filter(self.selectFailedRequests, transFiles)

      lfns = [f['LFN'] for f in transFiles]

      res = self.exists(sourceSE, lfns)
      if not res['OK']:
        return res

      resultSourceSe = res['Value']

      res = self.exists(targetSEs, lfns)
      if not res['OK']:
        return res

      resultTargetSEs = res['Value']

      #fill transFile dict with files availability on Source and Target SE information
      for transFile in transFiles:
        lfn = transFile['LFN']
        transFile['AvailableOnSource'] = resultSourceSe[lfn]
        transFile['AvailableOnTarget'] = resultTargetSEs[lfn]

      checkFilesFuncName = "check_%s_files" % status.lower()
      checkFiles = getattr(self, checkFilesFuncName)
      checkFiles( actions, transFiles, transType )

    self.applyActions( transID, actions )
