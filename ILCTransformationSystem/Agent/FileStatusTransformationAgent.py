"""
FST Agent DESCRIPTION HERE
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

SET_PROCESSED = 'set_processed'
SET_DELETED = 'set_deleted'

RETRY = 'retry'
SET_UNUSED = 'set_unused'
RESET_REQUEST = 'reset_request'

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
    self.accounting = {}

    self.fcClient = FileCatalogClient()
    self.tClient = TransformationClient()
    self.reqClient = ReqClient()

  def checkFileStatusFuncExists( self, status):
    checkFileStatusFuncName = "check_%s_files" % (status.lower())
    if not (hasattr(self, checkFileStatusFuncName) and callable(getattr(self, checkFileStatusFuncName))):
      self.log.warn("Unable to process transformation files with status %s" % status)
      return False

    return True

  def beginExecution(self):
    """ Reload the configurations before every cycle """
    self.enabled = self.am_getOption('EnableFlag', False)
    self.transformationTypes = self.am_getOption( 'TransformationTypes', ["Replication"] )
    self.transformationStatuses = self.am_getOption( 'TransformationStatuses', ["Active"] )
    self.transformationFileStatuses = self.am_getOption( 'TransformationFileStatuses', ["Assigned", "Problematic", "Processed", "Unused"] )

    self.transformationFileStatuses = filter(self.checkFileStatusFuncExists, self.transformationFileStatuses)
    return S_OK()

  def finalize(self):
    self.log.notice('Accounting Information')
    for trans, actions in self.accounting.items():
      for action, values in  actions.items():
        self.log.notice("Action: %s" % action)
        self.log.notice("Values: %s" % values)

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
      res = self.processTransformation( transID, trans['SourceSE'], trans['TargetSE'], trans['DataTransType'] )
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

      if res['Value']['SourceSE'] and res['Value']['TargetSE']:
        trans['SourceSE'] = eval( res['Value']['SourceSE'])
        trans['TargetSE'] = eval( res['Value']['TargetSE'])
      else:
        return S_ERROR()

      res = self.getDataTransformationType(trans['TransformationID'])
      if not res['OK']:
        self.log.error('Failure to determine Data Transformation Type')
        return res
      trans['DataTransType'] = res['Value']

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
      self.log.error('Failure to get Transformation Tasks for Transformation ID %s ' % transID)
      return res

    result = res['Value']
    requestStatus = {}
    for task in result:
      requestStatus[task['TaskID']]= {'RequestStatus': task['ExternalStatus'], 'RequestID': task['ExternalID']}

    return S_OK( requestStatus )

  def getDataTransformationType(self, transID):

    res = self.tClient.getTransformationParameters(transID, 'Body')
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

    if _newLFNStatuses:
      res = self.tClient.setFileStatusForTransformation(transID, newLFNsStatus=_newLFNStatuses)
      if not res['OK']:
        self.log.error('Failed to set statuses for LFNs %s ' % res['Message'])
      else:
        if transID not in self.accounting:
          self.accounting[transID] = {}

        if status not in self.accounting[transID]:
          self.accounting[transID][status] = []

        self.accounting[transID][status].append(lfns)
        self.log.notice('File Statuses updated Successfully %s' % res['Value'])

  def selectFailedRequests( self, transFile):

    res = self.getRequestStatus(transFile['TransformationID'], transFile['TaskID'])
    if not res['OK']:
      self.log.error('Failure to get Request Status for Assigned File')
      return False
    result = res['Value']

    if result[transFile['TaskID']]['RequestStatus'] == 'Failed':
      return True

    return False

  def retryStrategyForFiles(self, transID, transFiles):

    taskIDs = [transFile['TaskID'] for transFile in transFiles]
    res = self.getRequestStatus( transID, taskIDs)
    if not res['OK']:
      return res
    result = res['Value']
    retryStrategy = {}
    for taskID in taskIDs:
      res = self.reqClient.getRequest(requestID = result[taskID]['RequestID'])
      if not res['OK']:
        self.log.notice('Request %s does not exist setting file status to unused' % result[taskID]['RequestID'])
        retryStrategy[taskID] = SET_UNUSED
      else:
        retryStrategy[taskID] = RESET_REQUEST

    return S_OK(retryStrategy)


  def check_assigned_files(self, actions, transFiles, transType):

    for transFile in transFiles:
      if transFile['AvailableOnSource'] and transFile['AvailableOnTarget']:
        if transType == REPLICATION_TRANS:
          actions[SET_PROCESSED].append(transFile)
        elif transType == MOVING_TRANS:
          actions[RETRY].append(transFile)
        else:
          self.log.warn('Unknown TransType %s '%transType)

      elif transFile['AvailableOnSource'] and not transFile['AvailableOnTarget']:
        actions[RETRY].append(transFile)

      elif not transFile['AvailableOnSource'] and transFile['AvailableOnTarget']:
        actions[SET_PROCESSED].append(transFile)

      else:
        #not on src and target
        actions[SET_DELETED].append(transFile)


  def check_unused_files(self, actions, transFiles, transType):

    for transFile in transFiles:
      if not transFile['AvailableOnSource'] and transFile['AvailableOnTarget']:
        actions[SET_PROCESSED].append(transFile)

      if not transFile['AvailableOnSource'] and not transFile['AvailableOnTarget']:
        actions[SET_DELETED].append(transFile)

  def check_processed_files(self, actions, transFiles, transType):

    for transFile in transFiles:
      if transFile['AvailableOnSource'] and transFile['AvailableOnTarget'] and transType == MOVING_TRANS:
        actions[RETRY].append(transFile)

      if transFile['AvailableOnSource'] and not transFile['AvailableOnTarget']:
        actions[RETRY].append(transFile)

      if not transFile['AvailableOnSource'] and not transFile['AvailableOnTarget']:
        actions[SET_DELETED].append(transFile)

  def check_problematic_files(self, actions, transFiles, transType):

    for transFile in transFiles:
      if transFile['AvailableOnSource'] and transFile['AvailableOnTarget']:
        if transType == REPLICATION_TRANS:
          actions[SET_PROCESSED].append(transFile)
        elif transType == MOVING_TRANS:
          actions[RETRY].append(transFile)
        else:
          self.log.warn('Unknown TransType %s '%transType)

      elif transFile['AvailableOnSource'] and not transFile['AvailableOnTarget']:
        actions[RETRY].append(transFile)

      elif not transFile['AvailableOnSource'] and transFile['AvailableOnTarget']:
        actions[SET_PROCESSED].append(transFile)

      else:
        #not available on source and target
        actions[SET_DELETED].append(transFile)

  def applyActions( self, transID, actions):

    for action in actions.keys():
      if action == SET_PROCESSED and actions[SET_PROCESSED]:
        lfns = [transFile['LFN'] for transFile in actions[SET_PROCESSED]]
        if not self.enabled:
          self.log.notice('Would have set file status to Processesd, %s ' % lfns)
        else:
          self.setFileStatus( transID, lfns, 'Processed')

      elif action == SET_DELETED and actions[SET_DELETED]:
        lfns = [transFile['LFN'] for transFile in actions[SET_DELETED]]
        if not self.enabled:
          self.log.notice('Would have set file status to Deleted, %s ' % lfns)
        else:
          self.setFileStatus( transID, lfns, 'Deleted')

      elif action == RETRY and actions[RETRY]:
        #if there is a request in RMS then reset request otherwise set file status unused
        res = self.retryStrategyForFiles(transID, actions[RETRY])
        if not res['OK']:
          self.log.error('Failure to determine retry strategy ( set unused / reset request) for transformation files')
          continue

        retryStrategy = res['Value']
        setLfnsUnused = []
        for transFile in actions[RETRY]:
          if retryStrategy[transFile['TaskID']] == RESET_REQUEST:

            res = self.tClient.getTransformationTasks(condDict = {'TransformationID': transID, 'TaskID': transFile['TaskID']})
            if not res['OK']:
              self.log.error('Failure to get Transformation Task for Transformation File %s' % transFile['LFN'])
              continue

            requestID = res['Value'][0]['ExternalID']
            if not self.enabled:
              self.log.notice('Would have re-submitted the request %s if it exists in RMS otherwise the file status would be set to Unused' % requestID)
            else:
              res = self.reqClient.resetFailedRequest(requestID)
              if res['OK'] and res['Value'] != "Not reset":
                if transID not in self.accounting:
                  self.accounting[transID] = {}

                if RESET_REQUEST not in self.accounting[transID]:
                  self.accounting[transID][RESET_REQUEST] = []

                self.accounting[transID][RESET_REQUEST].append(requestID)

                res = self.tClient.setTaskStatus( transID, transFile['TaskID'], 'Waiting' )
                if not res['OK']:
                  self.log.error('Failure to set Waiting status for Task ID %d', transFile['TaskID'])
              else:
                setLfnsUnused.append(transFile['LFN'])
          else:
            if not self.enabled:
              self.log.notice('Would have set the file %s status to Unused' % transFile['LFN'])
            else:
              setLfnsUnused.append(transFile['LFN'])

        if setLfnsUnused:
          self.setFileStatus(transID, setLfnsUnused, 'Unused')

      else:
        self.log.warn('Unknown action %s' % action)

  def existsInFC(self, SEs, lfns):

    res = self.fcClient.getReplicas(lfns)
    if not res['OK']:
      return res

    result = {}
    result['Successful'] = {}
    result['Failed'] = {}
    setOfSEs = set(SEs)

    for lfn, msg in res['Value']['Failed'].items():
      if msg == 'No such file or directory':
        result['Successful'][lfn] = False
      else:
        result['Failed'][lfn] = msg

    #check if all replicas are registered in FC
    filesFoundInFC = res['Value']['Successful']
    for lfn,replicas in filesFoundInFC.items():
      if setOfSEs.issubset(replicas.keys()):
        result['Successful'][lfn] = True
      else:
        result['Successful'][lfn] = False

    return S_OK(result)

  def existsOnSE(self, SEs, lfns):
    """ checks if the given files exist on a list of storage elements"""

    result = {}
    result['Failed'] = {}
    result['Successful'] = {}


    for se in SEs:
      if se not in self.seObjDict:
        self.seObjDict[se] = StorageElement(se)
      seObj = self.seObjDict[se]

      res = seObj.exists(lfns)
      if not res['OK']:
        return res

      for lfn, status in res['Value']['Successful'].items():
        if lfn not in result['Successful']:
          result['Successful'][lfn] = status
        else:
          if result['Successful'][lfn] and not status:
            result['Successful'][lfn] = False

      result['Failed'][se] = res['Value']['Failed']

    return S_OK( result )


  def exists(self, SEs, lfns):
    """ checks if files exists on both file catalog and storage elements """

    fcRes = self.existsInFC(SEs, lfns)
    if not fcRes['OK']:
      self.log.error('Failure to determine if files exists in File Catalog, %s' % fcRes['Message'])
      return fcRes

    # check if files found in file catalog also exist on SE
    checkLFNsOnStorage = [lfn for lfn in fcRes['Value']['Successful'] if fcRes['Value']['Successful'][lfn]]
    seRes = self.existsOnSE(SEs, checkLFNsOnStorage)
    if not seRes['OK']:
      self.log.error('Failure to determine if files exist on SE, %s' % seRes['Message'])
      return seRes

    if 'Failed' in seRes['Value'] and seRes['Value']['Failed']:
      self.log.error('Failed to determine if files exist on SE, %s' % seRes['Value']['Failed'])
      return res

    fcResult = fcRes['Value']['Successful']
    seResult = seRes['Value']['Successful']
    for lfn, status in fcResult.items():
      if fcResult[lfn] and not seResult[lfn]:
        fcRes['Value']['Successful'][lfn] = False

    if 'Failed' in fcRes['Value']:
      self.log.notice("FAILED FileCatalog Response %s" % fcRes['Value']['Failed'])

    if 'Failed' in seRes['Value']:
      self.log.notice("FAILED StorageElement Response %s" % seRes['Value']['Failed'])

    return fcRes

  def processTransformation(self, transID, sourceSE, targetSEs, transType ):
    """ process transformation for a given transformation ID """

    actions = {}
    actions[SET_PROCESSED] = []
    actions[RETRY] = []
    actions[SET_DELETED] = []

    for status in self.transformationFileStatuses:
      res = self.tClient.getTransformationFiles(condDict={'TransformationID': transID, 'Status': status})
      if not res['OK']:
        self.log.error('Failure to get Transformation Files with Status %s for Transformation ID %d' % (status, transID))
        continue

      transFiles = res['Value']
      if not transFiles:
        self.log.notice("No Transformation Files found with status %s for Transformation ID %d" %(status, transID))
        continue

      self.log.notice("Processing Transformation Files with status %s for TransformationID %s " %(status, transID))

      # only process Assigned files with failed requests
      if status == 'Assigned':
        transFiles = filter(self.selectFailedRequests, transFiles)

      lfns = [transFile['LFN'] for transFile in transFiles]

      res = self.exists(sourceSE, lfns)
      if not res['OK']:
        continue
      resultSourceSe = res['Value']['Successful']

      res = self.exists(targetSEs, lfns)
      if not res['OK']:
        continue
      resultTargetSEs = res['Value']['Successful']

      #fill transFile dict with file availability information on Source and Target SE
      for transFile in transFiles:
        lfn = transFile['LFN']
        transFile['AvailableOnSource'] = resultSourceSe[lfn]
        transFile['AvailableOnTarget'] = resultTargetSEs[lfn]

      checkFilesFuncName = "check_%s_files" % status.lower()
      checkFiles = getattr(self, checkFilesFuncName)
      checkFiles( actions, transFiles, transType )

    self.applyActions( transID, actions )
    return S_OK()
