#/usr/env python
"""
getTransformations
getJobsForTransformation including Status etc.
getInputAndOutputFileForJobs
getTaskAndFileStatusForTheInputFiles
"""

__RCSID__="$Id$"

from collections import defaultdict

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC import S_OK, gLogger

from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient

from ILCDIRAC.ILCTransformationSystem.Utilities import TransformationInfo, FileInformation, JobInfo

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
    self.todo = [ dict( Message="All files exist: mark job 'Done', mark input 'Processed'",
                        ShortMessage="Jobs 'Done', Input 'Processed'",
                        Counter=0,
                        Check=lambda job: job.status=='Failed' and job.allFilesExist() and job.taskStatus in ('Asssiged',),
                        Actions=lambda job,tInfo: [ job.setJobDone(tInfo), job.setInputProcessed(tInfo) ]
                      ),
                  dict( Message="All files exist, input processed, mark job 'Done'",
                        ShortMessage="Jobs 'Done'",
                        Counter=0,
                        Check=lambda job: job.status == "Failed" and job.taskStatus in ("Processed",) and job.allFilesExist(),
                        Actions=lambda job,tInfo: [ job.setJobDone(tInfo) ]
                      ),
                  dict( Message="All files missing, mark job 'Failed'",
                        ShortMessage="Jobs 'Failed'",
                        Counter=0,
                        Check=lambda job: job.status == "Done" and job.allFilesMissing(),
                        Actions=lambda job,tInfo: [ job.setJobFailed(tInfo) ]
                      ),
                  dict( Message="Some output files missing: cleanup,  mark input 'Unused'",
                        ShortMessage="Outputs Cleaned, Input 'Unused'",
                        Counter=0,
                        Check=lambda job: job.status=='Failed' and job.someFilesMissing() and not job.finalTask,
                        Actions=lambda job,tInfo: [ job.cleanOutputs(tInfo), job.setInputUnused(tInfo) ]
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
                        Check=lambda job: job.status == "Failed" and job.taskStatus in ("Assigned",) and job.allFilesMissing(),
                        Actions=lambda job,tInfo: [ job.cleanOutputs(tInfo), tInfo.setInputUnused(job) ]
                      ),

                ]

  def getJobs( self, transformationID, status ):
    """returns list of done jobs"""
    attrDict = dict( Status=status, JobGroup="%08d" % int(transformationID) )
    res = self.jobMon.getJobs( attrDict )
    if res['OK']:
      self.log.debug("Found Prod jobs: %s" % res['Value'] )
    else:
      self.log.error("Error finding jobs: ", res['Message'] )
    return res

  def selectTransformationFiles( self, tInfo, statusList ):
    """ Select files, production jobIDs in specified file status for a given transformation.
    returns dictionary of lfn -> jobID
    """
    #Until a query for files with timestamp can be obtained must rely on the
    #WMS job last update
    res = self.tClient.getTransformationFiles(condDict = {'TransformationID' : tInfo.tID, 'Status' : statusList})
    self.log.debug(res)
    if not res['OK']:
      return res
    resDict = {}
    for fileDict in res['Value']:
      if not 'LFN' in fileDict or not 'TaskID' in fileDict or not 'LastUpdate' in fileDict:
        self.log.info('LFN, %s, and LastUpdate are mandatory, >=1 are missing for:\n%s' % ('TaskID', fileDict))
        continue
      taskID = fileDict['TaskID']
      resDict[taskID] = FileInformation( fileDict['LFN'],
                                         fileDict['FileID'],
                                         fileDict['Status'],
                                         taskID,
                                       )
      
    if resDict:
      self.log.notice('Selected %s files overall for transformation %s' % (len(resDict), tInfo.tID))
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
    """run this thing"""
    status = ["Active"]
    types = [ "MCSimulation", "MCReconstruction_Overlay", "MCReconstruction"]
    transformations = self.getEligibleTransformations( status, types )
    prodID = 5678
    #prodID = 4732
    #prodID = 5702
    transName = transformations['Value'][str(prodID)]
    tInfo = TransformationInfo( prodID, transName, self.tClient, self.jobDB, self.logDB )

    self.log.notice( "Getting 'Done' Jobs..." )
    done = self.getJobs( tInfo.tID, ["Done"] )
    done = S_OK([])
    self.log.notice( "Getting 'Failed' Jobs..." )
    failed = self.getJobs( tInfo.tID, ["Failed"] )
    if not done['OK']:
      raise RuntimeError( "Failed to get Done Jobs" )
    if not failed['OK']:
      raise RuntimeError( "Failed to get Failed Jobs" )
    done = done['Value']
    failed = failed['Value']

    jobs = {}
    for job in done:
      jobs[int(job)] = JobInfo( job, "Done", tInfo.tID )
    for job in failed:
      jobs[int(job)] = JobInfo( job, "Failed", tInfo.tID )

    self.log.notice( "Getting tasks...")
    tasksDict = tInfo.checkTasksStatus()
    lfnTaskDict = dict( [ ( tasksDict[taskID]['LFN'],taskID ) for taskID in tasksDict ] )

    fileJobDict = defaultdict(list)
    for job in jobs.values():
      try:
        job.checkRequests( self.reqClient )
        job.getJobInformation( self.jobMon )
        job.checkFileExistance( self.fcClient )
        job.getTaskInfo( tasksDict, lfnTaskDict )
        fileJobDict[job.inputFile].append(job.jobID)
        self.checkJob( job, tInfo )
      except RuntimeError as e:
        self.log.error( "Failure for job: %d " % job.jobID )
        self.log.error(" Exception: ", str(e) )

    print "Check multiple used input files"
    for lfn, jobs in fileJobDict.iteritems():
      if len(jobs) > 1:
        print lfn, jobs

    self.printSummary()

  def checkJob( self, job, tInfo ):
    """ deal with the job """
    for do in self.todo:
      if do['Check'](job):
        do['Counter'] += 1
        print do['Message']
        print job
        action = do['Actions']
        action(job, tInfo)

  def printSummary( self ):
    """print summary of changes"""
    print "Summary:"
    for do in self.todo:
      print do['ShortMessage'],":",do['Counter']

if __name__ == "__main__":
  DRA = DRA()
  DRA.execute()
