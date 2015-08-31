#/usr/env python
"""
getTransformations
getJobsForTransformation including Status etc.
getInputAndOutputFileForJobs
getTaskAndFileStatusForTheInputFiles
"""

__RCSID__="$Id$"

from collections import defaultdict
from sys import stdout
import time
import itertools

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC import S_OK, gLogger

from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.DataManagementSystem.Client.DataManager import DataManager

from ILCDIRAC.ILCTransformationSystem.Utilities import TransformationInfo, FileInformation

class DRA( object ):
  """ DataRecovery primarily based on jobs, not on task"""
  def __init__( self ):
    self.log = gLogger

    self.jobMon = JobMonitoringClient()
    self.fcClient = FileCatalogClient()
    self.tClient = TransformationClient()
    self.reqClient = ReqClient()
    self.jobDB = JobDB()
    self.logDB = JobLoggingDB()
    self.dMan = DataManager()
    self.ignoreTasks = set()
    self.todo = {'MCGeneration':
                 [ dict( Message="MCGeneration: OutputExists: Job 'Done'",
                         ShortMessage="MCGeneration: job 'Done' ",
                         Counter=0,
                         Check=lambda job: job.allFilesExist() and job.status=='Failed',
                         Actions=lambda job,tInfo: [ job.setJobDone(tInfo) ]
                       ),
                   dict( Message="MCGeneration: OutputMissing: Job 'Failed'",
                         ShortMessage="MCGeneration: job 'Failed' ",
                         Counter=0,
                         Check=lambda job: job.allFilesMissing() and job.status=='Done',
                         Actions=lambda job,tInfo: [ job.setJobFailed(tInfo) ]
                       ),
                   # dict( Message="MCGeneration, job 'Done': OutputExists: Task 'Done'",
                   #       ShortMessage="MCGeneration: job already 'Done' ",
                   #       Counter=0,
                   #       Check=lambda job: job.allFilesExist() and job.status=='Done',
                   #       Actions=lambda job,tInfo: [ tInfo._TransformationInfo__setTaskStatus(job, 'Done') ]
                   #     ),
                 ],
                 'OtherProductions':
                 [ \
                   dict( Message="Other Tasks Exist: Do nothing",
                         ShortMessage="Other Tasks",
                         Counter=0,
                         Check=lambda job: job.finalTask or job.taskID in self.ignoreTasks,
                         Actions=lambda job,tInfo: [ None ]
                       ),
                   dict( Message="InputFile missing: mark job 'Failed', mark input 'Deleted'",
                         ShortMessage="Job 'Failed, Input 'Deleted'",
                         Counter=0,
                         Check=lambda job: job.inputFile and not job.inputFileExists and job.status=='Done',
                         Actions=lambda job,tInfo: [ job.setJobFailed(tInfo), job.setInputDeleted(tInfo) ]
                       ),
                   dict( Message="InputFile missing, job'Failed': mark input 'Deleted'",
                         ShortMessage="Input 'Deleted'",
                         Counter=0,
                         Check=lambda job: job.inputFile and not job.inputFileExists and job.status=='Failed',
                         Actions=lambda job,tInfo: [ job.setInputDeleted(tInfo) ]
                       ),
                   dict( Message="All files exist: mark job 'Done', mark input 'Processed'",
                         ShortMessage="Jobs 'Done', Input 'Processed'",
                         Counter=0,
                         Check=lambda job: job.status=='Failed' and job.allFilesExist() and job.taskStatus in ('Assigned',),
                         Actions=lambda job,tInfo: [ job.setJobDone(tInfo), job.setInputProcessed(tInfo) ]
                       ),
                   dict( Message="All files exist, input processed, mark job 'Done'",
                         ShortMessage="Jobs 'Done'",
                         Counter=0,
                         Check=lambda job: job.status=='Failed' and job.taskStatus in ('Processed',) and job.allFilesExist(),
                         Actions=lambda job,tInfo: [ job.setJobDone(tInfo) ]
                       ),
                   dict( Message="Some output files missing: cleanup,  mark input 'Unused'",
                         ShortMessage="Outputs Cleaned, Input 'Unused'",
                         Counter=0,
                         Check=lambda job: job.status=='Failed' and job.someFilesMissing() and not job.finalTask,
                         Actions=lambda job,tInfo: [ job.cleanOutputs(tInfo), job.setInputUnused(tInfo) ]
                       ),
                   dict( Message="Some output files missing, job 'Done': cleanup, mark job 'Failed',  mark input 'Unused'",
                         ShortMessage="Job, 'Failed', Outputs Cleaned, Input 'Unused'",
                         Counter=0,
                         Check=lambda job: job.status=='Failed' and job.someFilesMissing() and not job.finalTask,
                         Actions=lambda job,tInfo: [job.cleanOutputs(tInfo), job.setJobFailed(tInfo), job.setInputUnused(tInfo)]
                       ),
                   dict( Message="Some output files missing: cleanup. Processed in different task",
                         ShortMessage="Outputs Cleaned",
                         Counter=0,
                         Check=lambda job: job.status=='Failed' and job.someFilesMissing(),
                         Actions=lambda job,tInfo: [ job.cleanOutputs(tInfo) ]
                       ),
                   dict( Message="Missing all output files: mark input unused",
                         ShortMessage="Input 'Unused'",
                         Counter=0,
                         Check=lambda job: job.status=='Failed' and job.taskStatus in ('Assigned',) \
                         and job.allFilesMissing() and not job.finalTask,
                         Actions=lambda job,tInfo: [ job.cleanOutputs(tInfo), tInfo.setInputUnused(job) ]
                       ),
                   dict( Message="All files missing, mark job 'Failed'",
                         ShortMessage="Jobs 'Failed'",
                         Counter=0,
                         Check=lambda job: job.status=='Done' and job.allFilesMissing() and job.finalTask,
                         Actions=lambda job,tInfo: [ job.setJobFailed(tInfo) ]
                       ),
                   dict( Message="All files missing, mark job 'Failed', input 'Unused'",
                         ShortMessage="Jobs 'Failed', Input 'Unused'",
                         Counter=0,
                         Check=lambda job: job.status=='Done' and job.allFilesMissing() and not job.finalTask,
                         Actions=lambda job,tInfo: [ job.setJobFailed(tInfo), job.setInputUnused(tInfo) ]
                       ),
                   dict( Message="Some files missing, mark job 'Failed'",
                         ShortMessage="Jobs 'Failed', Outputs Cleaned",
                         Counter=0,
                         Check=lambda job: job.status=='Done' and job.someFilesMissing(),
                         Actions=lambda job,tInfo: [ job.setJobFailed(tInfo), job.cleanOutputs() ]
                       ),
                   dict( Message="All files exist, job is 'Done': mark input 'Processed'",
                         ShortMessage="Input 'Processed'",
                         Counter=0,
                         Check=lambda job: job.status=='Done' and job.allFilesExist() and job.taskStatus in ('Assigned',) \
                         and not job.finalTask,
                         Actions=lambda job,tInfo: [ job.setInputProcessed(tInfo) ]
                       ),
                 ]
                }

  def selectTransformationFiles( self, tInfo, statusList ):
    """ Select files, production jobIDs in specified file status for a given transformation.
    returns dictionary of lfn -> jobID
    """
    #Until a query for files with timestamp can be obtained must rely on the
    #WMS job last update
    res = self.tClient.getTransformationFiles( condDict = {'TransformationID' : tInfo.tID, 'Status' : statusList} )
    self.log.debug(res)
    if not res['OK']:
      return res
    resDict = {}
    for fileDict in res['Value']:
      if not 'LFN' in fileDict or not 'TaskID' in fileDict or not 'LastUpdate' in fileDict:
        self.log.info( "LFN, %s, and LastUpdate are mandatory, >=1 are missing for:\n%s" % ('TaskID', fileDict) )
        continue
      taskID = fileDict['TaskID']
      resDict[taskID] = FileInformation( fileDict['LFN'],
                                         fileDict['FileID'],
                                         fileDict['Status'],
                                         taskID,
                                       )
    if resDict:
      self.log.notice( "Selected %s files overall for transformation %s" % (len(resDict), tInfo.tID))
    return S_OK(resDict)

    
  def getEligibleTransformations( self, status, typeList ):
    """ Select transformations of given status and type.
    """
    res = self.tClient.getTransformations(condDict = {'Status' : status, 'Type' : typeList})
    if not res['OK']:
      return res
    transformations = {}
    for prod in res['Value']:
      prodID = prod['TransformationID']
      prodName = prod['TransformationName']
      transformations[str(prodID)] = (prod['Type'], prodName)
    return S_OK(transformations)

  def execute( self ):
    """ run for this """
    status = ["Active"]
    types = [ "MCSimulation", "MCReconstruction_Overlay", "MCReconstruction", "MCGeneration" ]
    transformations = self.getEligibleTransformations( status, types )
    prodID = 5615
    transType, transName = transformations['Value'][str(prodID)]
    if transType == "MCGeneration":
      self.treatMCGeneration( prodID, transName, transType )
    else:
      self.treatProduction( prodID, transName, transType )

  def treatMCGeneration( self, prodID, transName, transType ):
    """deal with MCGeneration jobs, where there is no inputFile"""
    tInfo = TransformationInfo( prodID, transName, transType,
                                self.tClient, self.jobDB, self.logDB, self.dMan, self.fcClient, self.jobMon )
    #jobs = tInfo.getJobs(statusList=['Done', 'Failed'])
    jobs = tInfo.getJobs(statusList=['Done'])
    while jobs:
      jobs = self.checkAllJobs( jobs, tInfo )
    self.printSummary()

  def treatProduction( self, prodID, transName, transType ):
    """run this thing for given production"""

    tInfo = TransformationInfo( prodID, transName, transType,
                                self.tClient, self.jobDB, self.logDB, self.dMan, self.fcClient, self.jobMon )
    jobs = tInfo.getJobs()

    self.log.notice( "Getting tasks...")
    tasksDict = tInfo.checkTasksStatus()
    lfnTaskDict = dict( [ ( tasksDict[taskID]['LFN'],taskID ) for taskID in tasksDict ] )

    while jobs:
      jobs = self.checkAllJobs( jobs, tInfo, tasksDict, lfnTaskDict )

    self.printSummary()

  def checkJob( self, job, tInfo ):
    """ deal with the job """
    checks = self.todo['MCGeneration'] if job.tType == 'MCGeneration' else self.todo['OtherProductions']
    for do in checks:
      if do['Check'](job):
        do['Counter'] += 1
        stdout.write('\n')
        self.log.notice( do['Message'] )
        self.log.notice( job )
        action = do['Actions']
        action(job, tInfo)
        return

  def checkAllJobs( self, jobs, tInfo, tasksDict=None, lfnTaskDict=None ):
    """run over all jobs and do checks"""
    nextRound = {}
    fileJobDict = defaultdict(list)
    counter = 0
    startTime = time.time()
    nJobs = len(jobs)
    self.log.notice( "Running over all the jobs" )
    for job in jobs.values():
      counter += 1
      stdout.write( "\r %d/%d: %3.1fs " % (counter, nJobs, float(time.time() - startTime) ) )
      stdout.flush()
      try:
        job.checkRequests( self.reqClient )
        if job.pendingRequest:
          self.log.warn( "Job has Pending requests:\n%s" % job )
          continue
        job.getJobInformation( self.jobMon )
        job.checkFileExistance( self.fcClient )
        if tasksDict and lfnTaskDict:
          job.getTaskInfo( tasksDict, lfnTaskDict, self.ignoreTasks)
          print self.ignoreTasks
          fileJobDict[job.inputFile].append(job.jobID)
        self.checkJob( job, tInfo )
      except RuntimeError as e:
        self.log.error( "+++++ Failure for job: %d " % job.jobID )
        self.log.error( "+++++ Exception: ", str(e) )
        nextRound[job.jobID] = job
    ## runs these again because of RuntimeError
    return nextRound

  def printSummary( self ):
    """print summary of changes"""
    self.log.notice( "Summary:" )
    for do in itertools.chain.from_iterable(self.todo.values()):
      self.log.notice( "%s: %s" %(  do['ShortMessage'],do['Counter'] ) )

  def checkRequestsForJobList( self, jobList ):
    """return a dict of jobID to requestID"""
    jobIDList = jobList.keys()
    result = self.reqClient.readRequestsForJobs( jobIDList )
    if not result['OK']:
      raise RuntimeError( "Failed to get requests" )
    for jobID in result['Value']['Successful']:
      request = result['Value']['Successful'][jobID]
      if request.Status != "Done":
        self.log.notice( " Removing job because pending requests ")
        self.log.notice( str(jobList[jobID]) )
        del jobList[jobID]

  def getAllJobInformation( self, jobs ):
    """get the jobInformation for all jobs"""
    counter = 0
    nJobs = len(jobs)
    print "Getting Job Information from JDL"
    for job in jobs.values():
      counter += 1
      stdout.write( "\r %d/%d" % (counter, nJobs) )
      stdout.flush()
      try:
        job.getJobInformation( self.jobMon )
      except RuntimeError as e:
        self.log.error( "+++++ Failure for job: %d " % job.jobID )
        self.log.error( "+++++ Exception: ", str(e) )
        del jobs[job.jobID]

if __name__ == "__main__":
  DRA = DRA()
  DRA.execute()
