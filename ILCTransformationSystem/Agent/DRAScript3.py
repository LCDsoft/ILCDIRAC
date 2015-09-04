#!/usr/bin/env python
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

from DIRAC import S_OK, gLogger

from ILCDIRAC.ILCTransformationSystem.Utilities import TransformationInfo, FileInformation

class Params( object ):
  """Collection of Parameters set via CLI switches"""
  def __init__( self ):
    self.enabled = False
    self.prodID = 0
    self.jobStatus = ['Failed','Done']
    self.firstJob = 0

  def setEnabled(self, _ ):
    self.enabled = True
    return S_OK()

  def setProdID(self, prodID):
    self.prodID = int(prodID)
    return S_OK()

  def setFirstJob(self, jobID):
    self.firstJob = int(jobID)
    return S_OK()

  def setJobStatus(self, jobStatus):
    self.jobStatus = [ status.strip() for status in jobStatus.split(",") ]
    return S_OK()

  def registerSwitches(self):
    Script.registerSwitch( "P:", "ProdID=", "ProdID to Check/Fix", self.setProdID )
    Script.registerSwitch( "F:", "FirstJob=", "First WMS JobID to Check/Fix", self.setFirstJob )
    Script.registerSwitch( "S:", "Status=", "List of Job Statuses to check", self.setJobStatus )
    Script.registerSwitch( "X", "Enabled", "Enable the changes", self.setEnabled )
    Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                         '\nUsage:',
                                         '  %s [option|cfgfile] ...\n' % Script.scriptName ] ) )


class DRA( object ):
  """ DataRecovery primarily based on jobs, not on task"""

  def __init__( self ):

    from DIRAC.DataManagementSystem.Client.DataManager import DataManager
    from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
    from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
    from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
    from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient

    self.log = gLogger

    self.enabled = False
    self.prodID = 0
    self.firstJob = 1
    self.jobStatus = []
    self.dMan = DataManager()
    self.jobMon = JobMonitoringClient()
    self.fcClient = FileCatalogClient()
    self.tClient = TransformationClient()
    self.reqClient = ReqClient()
    self.inputFilesProcessed = set()
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
                   dict( Message="One of many Successful: clean others",
                         ShortMessage="Other Tasks --> Keep",
                         Counter=0,
                         Check=lambda job: job.allFilesExist() and job.otherTasks,
                         Actions=lambda job,tInfo: [ self.inputFilesProcessed.add(job.inputFile), job.setJobDone(tInfo), job.setInputProcessed(tInfo) ]
                       ),
                   dict( Message="Other Task processed Input, no Output: Fail",
                         ShortMessage="Other Tasks --> Fail",
                         Counter=0,
                         Check=lambda job: job.inputFile in self.inputFilesProcessed and job.allFilesMissing(),
                         Actions=lambda job,tInfo: [ job.setJobFailed(tInfo) ]
                       ),
                   dict( Message="Other Task processed Input: Fail and clean",
                         ShortMessage="Other Tasks --> Cleanup",
                         Counter=0,
                         Check=lambda job: job.inputFile in self.inputFilesProcessed and not job.allFilesMissing(),
                         Actions=lambda job,tInfo: [ job.setJobFailed(tInfo), job.cleanOutputs(tInfo) ]
                       ),
                   dict( Message="InputFile missing: mark job 'Failed', mark input 'Deleted', clean",
                         ShortMessage="Input Missing --> Job 'Failed, Input 'Deleted', Cleanup",
                         Counter=0,
                         Check=lambda job: job.inputFile and not job.inputFileExists,
                         Actions=lambda job,tInfo: [ job.cleanOutputs(tInfo), job.setJobFailed(tInfo), job.setInputDeleted(tInfo) ]
                       ),
                   ## All Output Exists
                   dict( Message="Output Exists, job Failed, input not Processed --> Job Done, Input Processed",
                         ShortMessage="Output Exists --> Job Done, Input Processed",
                         Counter=0,
                         Check=lambda job: job.allFilesExist() and not job.otherTasks and job.status=='Failed' and job.fileStatus!="Processed",
                         Actions=lambda job,tInfo: [ job.setJobDone(tInfo), job.setInputProcessed(tInfo) ]
                       ),
                   dict( Message="Output Exists, job Failed, input Processed --> Job Done",
                         ShortMessage="Output Exists --> Job Done",
                         Counter=0,
                         Check=lambda job: job.allFilesExist() and not job.otherTasks and job.status=='Failed' and job.fileStatus=="Processed",
                         Actions=lambda job,tInfo: [ job.setJobDone(tInfo) ]
                       ),
                   dict( Message="Output Exists, job Done, input not Processed --> Input Processed",
                         ShortMessage="Output Exists --> Input Processed",
                         Counter=0,
                         Check=lambda job: job.allFilesExist() and not job.otherTasks and job.status=='Done' and job.fileStatus!="Processed",
                         Actions=lambda job,tInfo: [ job.setInputProcessed(tInfo) ]
                       ),
                   ## outputmissing
                   dict( Message="Output Missing, job Failed, input Assigned --> Input Unused",
                         ShortMessage="Output Missing --> Input Unused",
                         Counter=0,
                         Check=lambda job: job.allFilesMissing() and not job.otherTasks and job.status=='Failed' and job.fileStatus=="Assigned",
                         Actions=lambda job,tInfo: [ job.setInputUnused(tInfo) ]
                       ),
                   dict( Message="Output Missing, job Done, input Assigned --> Job Failed, Input Unused",
                         ShortMessage="Output Missing --> Job Failed, Input Unused",
                         Counter=0,
                         Check=lambda job: job.allFilesMissing() and not job.otherTasks and job.status=='Done' and job.fileStatus=="Assigned",
                         Actions=lambda job,tInfo: [ job.setInputUnused(tInfo), job.setJobFailed(tInfo) ]
                       ),
                   ## some files missing, needing cleanup. Only checking for assigned, because processed could mean an earlier job was succesful and this one is just the duplucate that needed to be removed!
                   dict( Message="Some missing, job Failed, input Assigned --> cleanup, Input 'Unused'",
                         ShortMessage="Output Missing --> Cleanup, Input Unused",
                         Counter=0,
                         Check=lambda job: job.someFilesMissing() and not job.otherTasks and job.status=='Failed' and job.fileStatus=="Assigned",
                         Actions=lambda job,tInfo: [job.cleanOutputs(tInfo),job.setInputUnused(tInfo)]
                         #Actions=lambda job,tInfo: []
                       ),
                   dict( Message="Some missing, job Done, input Assigned --> cleanup, job Failed, Input 'Unused'",
                         ShortMessage="Output Missing --> Cleanup, Job Failed, Input Unused",
                         Counter=0,
                         Check=lambda job: job.someFilesMissing() and not job.otherTasks and job.status=='Done' and job.fileStatus=="Assigned",
                         Actions=lambda job,tInfo: [job.cleanOutputs(tInfo),job.setInputUnused(tInfo),job.setJobFailed(tInfo)]
                         #Actions=lambda job,tInfo: []
                       ),
                   dict ( Message="Something Strange",
                          ShortMessage="Strange",
                          Counter=0,
                          Check=lambda job: job.status not in ("Failed","Done"),
                          Actions=lambda job,tInfo: []
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
    transType, transName = transformations['Value'][str(self.prodID)]
    if transType == "MCGeneration":
      self.treatMCGeneration( self.prodID, transName, transType )
    else:
      self.treatProduction( self.prodID, transName, transType )

  def treatMCGeneration( self, prodID, transName, transType ):
    """deal with MCGeneration jobs, where there is no inputFile"""
    tInfo = TransformationInfo( prodID, transName, transType, self.enabled,
                                self.tClient, self.dMan, self.fcClient, self.jobMon )
    jobs = tInfo.getJobs(statusList=self.jobStatus)
    #jobs = tInfo.getJobs(statusList=['Failed'])
    ## try until all jobs have been treated
    self.checkAllJobs( jobs, tInfo )
    self.printSummary()

  def treatProduction( self, prodID, transName, transType ):
    """run this thing for given production"""

    tInfo = TransformationInfo( prodID, transName, transType, self.enabled,
                                self.tClient, self.dMan, self.fcClient, self.jobMon )
    jobs = tInfo.getJobs(statusList=self.jobStatus)

    self.log.notice( "Getting tasks...")
    tasksDict = tInfo.checkTasksStatus()
    lfnTaskDict = dict( [ ( tasksDict[taskID]['LFN'],taskID ) for taskID in tasksDict ] )

    self.checkAllJobs( jobs, tInfo, tasksDict, lfnTaskDict )

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
        do['Actions'](job, tInfo)
        return

  def checkAllJobs( self, jobs, tInfo, tasksDict=None, lfnTaskDict=None ):
    """run over all jobs and do checks"""
    fileJobDict = defaultdict(list)
    counter = 0
    startTime = time.time()
    nJobs = len(jobs)
    self.log.notice( "Running over all the jobs" )
    for job in jobs.values():
      counter += 1
      stdout.write( "\r %d/%d: %3.1fs " % (counter, nJobs, float(time.time() - startTime) ) )
      stdout.flush()
      if job.jobID < self.firstJob:
        continue
      while True:
        try:
          job.checkRequests( self.reqClient )
          if job.pendingRequest:
            self.log.warn( "Job has Pending requests:\n%s" % job )
            break
          job.getJobInformation( self.jobMon )
          job.checkFileExistance( self.fcClient )
          if tasksDict and lfnTaskDict:
            job.getTaskInfo( tasksDict, lfnTaskDict )
            fileJobDict[job.inputFile].append( job.jobID )
          self.checkJob( job, tInfo )
          break # get out of the while loop
        except RuntimeError as e:
          self.log.error( "+++++ Failure for job: %d " % job.jobID )
          self.log.error( "+++++ Exception: ", str(e) )
          ## runs these again because of RuntimeError

  def printSummary( self ):
    """print summary of changes"""
    self.log.notice( "Summary:" )
    for do in itertools.chain.from_iterable(self.todo.values()):
      self.log.notice( "%s: %s" % ( do['ShortMessage'].ljust(52), str(do['Counter']).rjust(5) ) )

  def checkRequestsForJobList( self, jobList ):
    """return a dict of jobID to requestID"""
    jobIDList = jobList.keys()
    result = self.reqClient.readRequestsForJobs( jobIDList )
    if not result['OK']:
      raise RuntimeError( "Failed to get requests" )
    for jobID in result['Value']['Successful']:
      request = result['Value']['Successful'][jobID]
      if request.Status not in ( "Done", "Canceled" ):
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
  PARAMS = Params()
  PARAMS.registerSwitches()
  Script.parseCommandLine( ignoreErrors = True )
  DRA = DRA()
  DRA.enabled = PARAMS.enabled
  DRA.prodID = PARAMS.prodID
  DRA.jobStatus = PARAMS.jobStatus
  DRA.firstJob = PARAMS.firstJob
  DRA.execute()
