"""
FileStatusTransformation Agent performs the following actions:

+-----------------------+------------------+----------------------------+---------------+---------------+---------------------------+
|  Transformation Type  |  Request Status  | Transformation File Status |    Source     |    Target     |          Action           |
+-----------------------+------------------+----------------------------+---------------+---------------+---------------------------+
| Moving                | *                | Problematic / Processed    | Available     | Available     | Retry                     |
| Replication / Moving  | *                | Problematic / Processed    | Available     | Not Available | Retry                     |
| Moving                | Failed           | Assigned                   | Available     | Available     | Retry                     |
| Replication / Moving  | Failed           | Assigned                   | Available     | Not Available | Retry                     |
| Replication           | Failed           | Assigned                   | Available     | Available     | Set file status PROCESSED |
| Replication / Moving  | *                | Other than Processed       | Not Available | Available     | Set file Status PROCESSED |
| Replication / Moving  | *                | *                          | Not Available | Not Available | Set File status DELETED   |
+-----------------------+------------------+----------------------------+---------------+---------------+---------------------------+

* If the action is Retry then the request is reset if it exists in Request Management System, otherwise the file status is set to unused
* Available means the file exists in File Catalog and also exists physically on Storage Elements
* Not Available means the file doesn't exist in File Catalog or one or more replicas are lost on the Storage Elements

"""

import json

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.PrettyPrint import printTable

from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.Resources.Storage.StorageElement import StorageElement

__RCSID__ = "$Id$"

AGENT_NAME = 'ILCTransformation/FileStatusTransformationAgent'

SET_PROCESSED = 'SET_PROCESSED'
SET_DELETED = 'SET_DELETED'
SET_UNUSED = 'SET_UNUSED'

RETRY = 'retry'
RESET_REQUEST = 'RESET_REQUEST'

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

    self.addressTo = ["andre.philippe.sailer@cern.ch", "hamza.zafar@cern.ch"]
    self.addressFrom = "ilcdirac-admin@cern.ch" 
    self.emailSubject = "FileStatusTransformationAgent"

    self.seObjDict = {}
    self.accounting = {}

    self.fcClient = FileCatalogClient()
    self.tClient = TransformationClient()
    self.reqClient = ReqClient()
    self.nClient = NotificationClient()

  def checkFileStatusFuncExists( self, status):
    """ returns True/False if a function to check transformation files with a given status exists or not """
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

    self.addressTo = self.am_getOption( 'MailTo', ["andre.philippe.sailer@cern.ch", "hamza.zafar@cern.ch"] )
    self.addressFrom = self.am_getOption( 'MailFrom', "ilcdirac-admin@cern.ch" )

    self.transformationFileStatuses = filter(self.checkFileStatusFuncExists, self.transformationFileStatuses)
    self.accounting.clear()

    return S_OK()

  def sendNotification(self, transID, transType):
    """ sends email notification about accounting information of a transformation """
    emailBody = "Accounting Information\n\n"
    columns = ["LFN", "Source", "Target", "Old Status", "Action"]

    emailBody += str("Transformation ID: %s\n" % transID)
    emailBody += str("Transformation Type: %s\n" % transType)

    rows = []
    for action, transFiles in self.accounting[transID].items():
      for transFile in transFiles:
        rows.append([[transFile['LFN']], [str(transFile['AvailableOnSource'])], [str(transFile['AvailableOnTarget'])], [transFile['Status']], [action]])

    if rows:
      emailBody += printTable(columns, rows, printOut = False, numbering = False, columnSeparator = ' | ')
      self.log.notice(emailBody)

      for address in self.addressTo:
        res = self.nClient.sendMail( address, self.emailSubject, emailBody, self.addressFrom, localAttempt = False )
        if not res['OK']:
          self.log.error( "Failure to send Email notification" )
          return res

    return S_OK()

  def execute( self ):
    """ main execution loop of Agent """

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

  def getTransformations(self, transID = None):
    """ returns transformations of a given type and status """
    res = None
    if transID:
      res = self.tClient.getTransformations(condDict = {'TransformationID': transID, 'Status' : self.transformationStatuses, 'Type' : self.transformationTypes})
    else:
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
        self.log.error('Failure to determine Data Transformation Type: %s' % res['Message'])
        return res
      trans['DataTransType'] = res['Value']

    return S_OK(result)

  def getTransformationTasks(self, transID):
    """ returns all tasks for a given transformation ID """
    res = self.tClient.getTransformationTasks(condDict = {'TransformationID' : transID})
    if not res['OK']:
      return res

    return S_OK(res['Value'])

  def getRequestStatus(self, transID, taskIDs):
    """ returns request statuses for a given list of task IDs """
    res = self.tClient.getTransformationTasks(condDict={'TransformationID':transID, 'TaskID':taskIDs})
    if not res['OK']:
      self.log.error('Failure to get Transformation Tasks for Transformation ID %s ' % transID)
      return res

    result = res['Value']
    requestStatus = {}
    for task in result:
      requestStatus[task['TaskID']]= {'RequestStatus': task['ExternalStatus'], 'RequestID': long(task['ExternalID'])}

    return S_OK( requestStatus )

  def getDataTransformationType(self, transID):
    """ returns transformation types Replication/Moving/Unknown for a given transformation """
    res = self.tClient.getTransformationParameters(transID, 'Body')
    if not res['OK']:
      return res

    replication = False
    rmReplica = False
    try:
      body = json.loads(res['Value'])
      for operation in body:
        if 'ReplicateAndRegister' in operation:
          replication = True
        if 'RemoveReplica' in operation:
          rmReplica = True
    except:
      if 'ReplicateAndRegister' in res['Value']:
        replication = True
        if 'RemoveReplica' in res['Value']:
          rmReplica = True

    if rmReplica and replication:
      return S_OK(MOVING_TRANS)

    if replication:
      return S_OK(REPLICATION_TRANS)

    return S_ERROR( "Unknown Transformation Type %s" % res['Value'] )

  def setFileStatus(self, transID, transFiles, status):
    """ sets transformation file status  """

    lfns = [transFile['LFN'] for transFile in transFiles]
    lfnStatuses = {lfn: status for lfn in lfns}

    if lfnStatuses:
      if not self.enabled:
        self.log.notice('Would have set status %s for files %s' % (status, lfns))
      else:
        res = self.tClient.setFileStatusForTransformation(transID, newLFNsStatus=lfnStatuses)
        if not res['OK']:
          self.log.error('Failed to set statuses for LFNs %s ' % res['Message'])
        else:
          if status not in self.accounting[transID]:
            self.accounting[transID][status] = []
          for transFile in transFiles:
            self.accounting[transID][status].append({'LFN': transFile['LFN'],
                                                     'Status': transFile['Status'],
                                                     'AvailableOnSource': transFile['AvailableOnSource'],
                                                     'AvailableOnTarget': transFile['AvailableOnTarget']})

          self.log.notice('File Statuses updated Successfully %s' % res['Value'])

  def selectFailedRequests( self, transFile):
    """ returns True if transformation file has a failed request otherwise returns False """
    res = self.getRequestStatus(transFile['TransformationID'], transFile['TaskID'])
    if not res['OK']:
      self.log.error('Failure to get Request Status for Assigned File')
      return False
    result = res['Value']

    if result[transFile['TaskID']]['RequestStatus'] == 'Failed':
      return True

    return False

  def retryStrategyForFiles(self, transID, transFiles):
    """ returns retryStrategy Reset Request if a request is found in RMS, otherwise returns set file status to unused"""
    taskIDs = [transFile['TaskID'] for transFile in transFiles]
    res = self.getRequestStatus( transID, taskIDs)
    if not res['OK']:
      return res
    result = res['Value']
    retryStrategy = {}
    for taskID in taskIDs:
      res = self.reqClient.getRequest(requestID = result[taskID]['RequestID'])
      retryStrategy[taskID] = {}
      if not res['OK']:
        self.log.notice('Request %s does not exist setting file status to unused' % result[taskID]['RequestID'])
        retryStrategy[taskID]['Strategy'] = SET_UNUSED
      else:
        retryStrategy[taskID]['Strategy'] = RESET_REQUEST
        retryStrategy[taskID]['RequestID'] = result[taskID]['RequestID']

    return S_OK(retryStrategy)


  def check_assigned_files(self, actions, transFiles, transType):
    """ treatment for transformation files with assigned status """
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
    """ treatment for transformation files with unused status """
    for transFile in transFiles:
      if not transFile['AvailableOnSource'] and transFile['AvailableOnTarget']:
        actions[SET_PROCESSED].append(transFile)

      if not transFile['AvailableOnSource'] and not transFile['AvailableOnTarget']:
        actions[SET_DELETED].append(transFile)

  def check_processed_files(self, actions, transFiles, transType):
    """ treatment for transformation files with processed status """
    for transFile in transFiles:
      if transFile['AvailableOnSource'] and transFile['AvailableOnTarget'] and transType == MOVING_TRANS:
        actions[RETRY].append(transFile)

      if transFile['AvailableOnSource'] and not transFile['AvailableOnTarget']:
        actions[RETRY].append(transFile)

      if not transFile['AvailableOnSource'] and not transFile['AvailableOnTarget']:
        actions[SET_DELETED].append(transFile)

  def check_problematic_files(self, actions, transFiles, transType):
    """ treatment for transformation files with problematic status """
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

  def retryFiles(self, transID, transFiles):
    """ resubmits request or sets file status to unused based on the retry strategy of transformation file """
    setFilesUnused = []

    res = self.retryStrategyForFiles(transID, transFiles)
    if not res['OK']:
      self.log.error('Failure to determine retry strategy ( set unused / reset request) for transformation files')
    else:
      retryStrategy = res['Value']
      for transFile in transFiles:
        if retryStrategy[transFile['TaskID']]['Strategy'] == RESET_REQUEST:
          requestID = retryStrategy[transFile['TaskID']]['RequestID']

          if not self.enabled:
            self.log.notice('Would have re-submitted the request %s ' % requestID)
          else:
            res = self.reqClient.resetFailedRequest(requestID)
            if res['OK'] and res['Value'] != "Not reset":
              if RESET_REQUEST not in self.accounting[transID]:
                self.accounting[transID][RESET_REQUEST] = []
              self.accounting[transID][RESET_REQUEST].append({'LFN': transFile['LFN'],
                                                              'Status': transFile['Status'],
                                                              'AvailableOnSource': transFile['AvailableOnSource'],
                                                              'AvailableOnTarget': transFile['AvailableOnTarget']})

              res = self.tClient.setTaskStatus( transID, transFile['TaskID'], 'Waiting' )
              if not res['OK']:
                self.log.error('Failure to set Waiting status for Task ID %d', transFile['TaskID'])
        else:
          setFilesUnused.append(transFile)

    if setFilesUnused:
      self.setFileStatus(transID, setFilesUnused, 'Unused')


  def applyActions( self, transID, actions):
    for action, transFiles in actions.items():
      if action == SET_PROCESSED and transFiles:
        self.setFileStatus( transID, transFiles, 'Processed')

      elif action == SET_DELETED and transFiles:
        self.setFileStatus( transID, transFiles, 'Deleted')

      elif action == RETRY and transFiles:
        #if there is a request in RMS then reset request otherwise set file status unused
        self.retryFiles(transID, transFiles)

      else:
        self.log.notice('No %s action to apply for trans ID %s' % (action, transID))

  def existsInFC(self, storageElements, lfns):
    """ checks if files have replicas registered in File Catalog for all given storageElements """
    res = self.fcClient.getReplicas(lfns)
    if not res['OK']:
      return res

    result = {}
    result['Successful'] = {}
    result['Failed'] = {}
    setOfSEs = set(storageElements)

    for lfn, msg in res['Value']['Failed'].items():
      if msg == 'No such file or directory':
        result['Successful'][lfn] = False
      else:
        result['Failed'][lfn] = msg

    #check if all replicas are registered in FC
    filesFoundInFC = res['Value']['Successful']
    for lfn,replicas in filesFoundInFC.items():
      result['Successful'][lfn] = setOfSEs.issubset(replicas.keys())

    return S_OK(result)

  def existsOnSE(self, storageElements, lfns):
    """ checks if the given files exist physically on a list of storage elements"""

    result = {}
    result['Failed'] = {}
    result['Successful'] = {}

    if not lfns:
      return S_OK(result)

    voName = lfns[0].split('/')[1]
    for se in storageElements:
      if voName not in self.seObjDict:
        self.seObjDict[voName] = {}
      if se not in self.seObjDict[voName]:
        self.seObjDict[voName][se] = StorageElement(se, vo=voName)
      seObj = self.seObjDict[voName][se]

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


  def exists(self, storageElements, lfns):
    """ checks if files exists on both file catalog and storage elements """

    fcRes = self.existsInFC(storageElements, lfns)
    if not fcRes['OK']:
      self.log.error('Failure to determine if files exists in File Catalog, %s' % fcRes['Message'])
      return fcRes

    if 'Failed' in fcRes['Value'] and fcRes['Value']['Failed']:
      self.log.notice("FAILED FileCatalog Response %s" % fcRes['Value']['Failed'])

    # check if files found in file catalog also exist on SE
    checkLFNsOnStorage = [lfn for lfn in fcRes['Value']['Successful'] if fcRes['Value']['Successful'][lfn]]

    # no files were found in FC, return the result instead of verifying them on SE
    if not checkLFNsOnStorage:
      return fcRes

    seRes = self.existsOnSE(storageElements, checkLFNsOnStorage)
    if not seRes['OK']:
      self.log.error('Failure to determine if files exist on SE, %s' % seRes['Message'])
      return seRes

    for se in storageElements:
      if 'Failed' in seRes['Value'] and seRes['Value']['Failed'][se]:
        self.log.error('Failed to determine if files exist on %s, %s' % (se, seRes['Value']['Failed'][se]))
        return S_ERROR()

    fcResult = fcRes['Value']['Successful']
    seResult = seRes['Value']['Successful']
    for lfn in fcResult.keys():
      if fcResult[lfn] and not seResult[lfn]:
        fcRes['Value']['Successful'][lfn] = False

    return fcRes

  def processTransformation(self, transID, sourceSE, targetSEs, transType ):
    """ process transformation for a given transformation ID """

    actions = {}
    actions[SET_PROCESSED] = []
    actions[RETRY] = []
    actions[SET_DELETED] = []


    self.accounting[transID] = {}

    for status in self.transformationFileStatuses:
      res = self.tClient.getTransformationFiles(condDict={'TransformationID': transID, 'Status': status})
      if not res['OK']:
        self.log.error('Failure to get Transformation Files with Status %s for Transformation ID %d' % (status, transID))
        continue

      transFiles = res['Value']
      if not transFiles:
        self.log.notice("No Transformation Files found with status %s for Transformation ID %d" %(status, transID))
        continue

      self.log.notice("Processing Transformation Files with status %s for TransformationID %d " %(status, transID))

      # only process Assigned files with failed requests
      if status == 'Assigned':
        transFiles = filter(self.selectFailedRequests, transFiles)

      lfns = [transFile['LFN'] for transFile in transFiles]

      if not lfns:
        continue

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

    self.applyActions( transID,  actions )
    self.sendNotification( transID, transType )

    return S_OK()
