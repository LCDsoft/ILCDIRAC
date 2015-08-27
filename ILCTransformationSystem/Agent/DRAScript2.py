#/usr/env python
"""
getTransformations
getFailedJobsFortransformation
getInputData, OutputData, Descendents for Job


Or should base in inputadata files:

get InputDataFiles for Transformation

get Jobs for InputDataFiles, print status of file and jobs



FIXME: jobDone, but actually Failed, so no(t all) outputData present
FIXME: jobFailed, but outputdata partially there

"""

ENABLED=False

__RCSID__ = "$Id$"

import datetime
from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC import gLogger, S_OK


from DIRAC.RequestManagementSystem.Client.ReqClient            import ReqClient
from DIRAC.Core.Utilities.Time                                 import dateTime
from DIRAC.Resources.Catalog.FileCatalogClient                 import FileCatalogClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB

from DIRAC.TransformationSystem.Client.TransformationClient    import TransformationClient  
from ILCDIRAC.ILCTransformationSystem.Utilities import FileInformation



    
class DRA( object ):
  """DRA Dummy for testing"""
  def __init__( self ):
    self.name = 'DataRecoveryAgent'
    self.log = gLogger.getSubLogger( "DataRecoveryAgent" )
    self.enableFlag = False
    self.transClient = TransformationClient()
    self.requestClient = ReqClient()
    self.fcClient = FileCatalogClient()
    self.taskIDName = 'TaskID'
    self.externalStatus = 'ExternalStatus'
    self.externalID = 'ExternalID'
    self.ops = Operations()
    self.removalOKFlag = False

    self.fileSelectionStatus = ['Assigned', 'MaxReset']
    self.updateStatus = 'Unused'
    self.wmsStatusList = ['Failed']
    #only worry about files > 12hrs since last update
    self.selectDelay = self.am_getOption("Delay", 12) #hours
    self.ignoreLessThan = self.ops.getValue("Transformations/IgnoreLessThan", 5600)
    self.ignoreLessThan = 5600

    self.jobDB = JobDB()
    self.logDB = JobLoggingDB()
    ##  turn into options
    self.transformationTypes = self.am_getOption( "TransformationTypes" ,
                                                  ['MCReconstruction', 'MCSimulation', 'MCReconstruction_Overlay', 'Merge'] )
    self.transformationStatus = self.am_getOption( "TransformationStatus", ['Active', 'Completing'] )

    
  def am_getOption(self, optionName, defaultValue):
    """dummy getOption, just returning default"""
    self.log.info( "using default value for %s " % optionName )
    return defaultValue



  def selectTransformationFiles( self, tInfo, statusList ):
    """ Select files, production jobIDs in specified file status for a given transformation.
    returns dictionary of lfn -> jobID
    """
    #Until a query for files with timestamp can be obtained must rely on the
    #WMS job last update
    res = self.transClient.getTransformationFiles(condDict = {'TransformationID' : tInfo.tID, 'Status' : statusList})
    self.log.debug(res)
    if not res['OK']:
      return res
    resDict = {}
    for fileDict in res['Value']:
      if not 'LFN' in fileDict or not self.taskIDName in fileDict or not 'LastUpdate' in fileDict:
        self.log.info('LFN, %s, and LastUpdate are mandatory, >=1 are missing for:\n%s' % (self.taskIDName, fileDict))
        continue
      taskID = fileDict[self.taskIDName]
      resDict[taskID] = FileInformation( fileDict['LFN'],
                                         fileDict['FileID'],
                                         fileDict['Status'],
                                         taskID,
                                       )
      
    if resDict:
      self.log.notice('Selected %s files overall for transformation %s' % (len(resDict), tInfo.tID))
    return S_OK(resDict)

  def getJobStatuses( self, tInfo, selectedFiles, selectDelay ):
    """ get wms job ids """

    prodJobIDs = [ inputFile.wmsID for _taskID,inputFile in selectedFiles.iteritems() ]
    if not prodJobIDs:
      return
    self.log.notice( "Get transformation tasks..." )
    res = self.transClient.getTransformationTasks( condDict={ 'TransformationID':tInfo.tID, self.taskIDName:prodJobIDs },
                                                   older=dateTime() - datetime.timedelta( hours=selectDelay ),
                                                   timeStamp='LastUpdateTime',
                                                   inputVector=True)
    self.log.notice( "...done" )

    if not res['OK']:
      self.log.error( 'Failing getTransformationTasks' , res['Message'] )
      raise RuntimeError( "Failed to get JobStatuses" )
    self.log.notice( "Found %d tasks " % len(res['Value']) )
    for fileStatus in res['Value']:
      taskID = fileStatus['TaskID']
      if taskID in selectedFiles:
        selectedFiles[taskID].jobID = fileStatus['ExternalID']
        selectedFiles[taskID].jobStatus = fileStatus['ExternalStatus']

  def getOutputFiles( self, tInfo, selectedFiles ):
    """get the outputfiles for all the inputfiles and taskIDs in selectedFiles"""
    self.log.notice( "Getting expected output files..." )
    for taskID, inputFile in selectedFiles.iteritems():
      outFiles = tInfo.getOutputFiles( taskID )
      inputFile.outputFiles = outFiles

  def getOutputFileStatus( self, selectedFiles ):
    for inputFile in selectedFiles.values():
      outputFiles = inputFile.outputFiles
      reps = self.fcClient.exists( outputFiles )
      if not reps['OK']:
        self.log.error( " Failed checking existance: " , reps['Message'] )
        raise RuntimeError("Failed to check existance")
      for lfn in inputFile.outputFiles:
        success = reps['Value']['Successful']
        failed = reps['Value']['Failed']
        if lfn in success:
          inputFile.outputStatus.append("Exists")
        elif lfn in failed and "No such file" in failed[lfn]:
          inputFile.outputStatus.append("Missing")
        else:
          inputFile.outputStatus.append("Unknown")

  def getOutputFileDescendents( self, selectedFiles ):
    """check for descendencts of the output files"""

    for _taskid, inputFile in selectedFiles.items():
      lfnsToCheck = [inputFile.lfn]
      resDescendents = self.fcClient.getFileDescendents( lfnsToCheck, range(1,6) )
      if not resDescendents['OK']:
        self.log.error( "Failed to get descendents: ", resDescendents['Message'] )
        raise RuntimeError ("Failed to get descendents")
      for _lfn, descendents in resDescendents['Value']['Successful'].iteritems():
        for descendent in descendents:
          inputFile.descendents.append(descendent)


  def getEligibleTransformations( self, status, typeList ):
    """ Select transformations of given status and type.
    """
    res = self.transClient.getTransformations(condDict = {'Status' : status, 'Type' : typeList})
    if not res['OK']:
      return res
    transformations = {}
    for prod in res['Value']:
      prodID = prod['TransformationID']
      prodName = prod['TransformationName']
      transformations[str(prodID)] = (prod['Type'], prodName)
    return S_OK(transformations)

    
  def execute( self ):
    """ run the DataRecovery things """
    #transformationInfo = TransformationInfo( 5678, self.transClient, self.jobDB, self.logDB )
    res = self.getEligibleTransformations( ["Active","Completed"], [ "MCSimulation", "MCReconstruction_Overlay", "MCReconstruction"] )
    #prodID = 5678
    prodID = 5732
    transName = res['Value'][str(prodID)]
    transformationInfo = TransformationInfo( prodID, transName, self.transClient, self.jobDB, self.logDB )
    self.log.notice( "Getting transformation tasks..." )
    result = self.selectTransformationFiles( transformationInfo, [ 'Processed', 'Assigned', 'MaxReset'] )
    #result = self.selectTransformationFiles( transformationInfo, ['Assigned'] )
    if not result['OK']:
      return result
    selectedFiles = result['Value']

    ##get the Job Status for all the files
    self.log.notice( "Getting job statuses..." )
    self.getJobStatuses( transformationInfo, selectedFiles, 2 )

    selectedInteresting = {}
    selectedDoneInteresting = {}
    for taskID, inputFile in selectedFiles.iteritems():
      if inputFile.jobStatus == 'Failed' and inputFile.status in ('Processed','Assigned'):
        selectedInteresting[taskID] = inputFile
      elif inputFile.jobStatus == 'Done' and inputFile.status in ('Assigned',):
        selectedDoneInteresting[taskID] = inputFile
    self.log.notice("Found %d interesting files" % len( selectedInteresting ) )
    self.log.notice("Found %d interesting Done Jobs " % len( selectedDoneInteresting ) )
    
    self.log.notice( "Getting output files..." )
    self.getOutputFiles( transformationInfo, selectedInteresting )

    self.log.notice( "Getting outputfile status..." )
    self.getOutputFileStatus( selectedInteresting )
    
    self.log.notice( "Getting descendents..." )
    self.getOutputFileDescendents( selectedInteresting )

    changeCounters = [ [0, "Assigned Files to Processed, Job to Done"],
                       [0, "Assigned Files to Unused"],
                       [0, "Assigned Files to Unused, Cleanup output"],
                       [0, "Processed Files, Job to Done"],
                       [0, "Done Job to Failed"],
                     ]

    for taskID, inputFile in selectedInteresting.iteritems():
      if inputFile.status in ("Assigned",) and inputFile.jobStatus == "Failed":
        if all( "Exists" in status for status in inputFile.outputStatus ):
          print "*"*60
          print inputFile, "\nAll files exist, set job to Done, mark input Processed"
          inputFile.setJobDone( transformationInfo )
          inputFile.setInputProcessed( transformationInfo )
          changeCounters[0][0] += 1
        elif all( "Missing" in status for status in inputFile.outputStatus ) and not inputFile.descendents:
          print "*"*60
          print inputFile, "\nMissing all output files, mark input unused"
          inputFile.setInputUnused( transformationInfo )
          changeCounters[1][0] += 1
        else:
          print "*"*60
          print inputFile, "\nSome output files missing, cleanup and mark input unused"
          #inpputFile.cleanupOutput( transformationInfo )
          #inputFile.setInputUnused( transformationInfo )
          changeCounters[2][0] += 1
      elif inputFile.status in ("Processed",) and inputFile.jobStatus == "Failed":
        if all( "Exists" in status for status in inputFile.outputStatus ):
          print "*"*60
          print inputFile, "\nAll files exist, input Processed: Set job to Done! "
          inputFile.setJobDone( transformationInfo )
          changeCounters[3][0] += 1
        else:
          print "*"*60
          print inputFile, "\nSome output files missing, cleanup and mark input unused"
          #inpputFile.cleanupOutput( transformationInfo )
          #inputFile.setInputUnused( transformationInfo )
          changeCounters[4][0] += 1
      elif inputFile.descendents:
        print "*"*60
        print inputFile, "\nDescendents exist "

    for counter in changeCounters:
      print counter[0], counter[1]

if __name__ == "__main__":
  DRA = DRA()
  DRA.execute()
