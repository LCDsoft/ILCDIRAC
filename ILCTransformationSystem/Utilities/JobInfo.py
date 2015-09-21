"""Job Information"""
__RCSID__ = "$Id$"

ENABLED=False

from itertools import izip_longest
import re

class JobInfo( object ):
  """ hold information about jobs"""
  def __init__( self , jobID, status, tID ):
    self.tID = int(tID)
    self.jobID = int(jobID)
    self.status = status
    self.inputFile = None
    self.inputFileExists = False
    self.outputFiles = None
    self.outputFileStatus = []
    self.taskID = None
    self.taskStatus = None
    self.taskFileID = None
    self.pendingRequest = False
    self.finalTask = None

  def __str__( self ):
    info = "%d: %s" % ( self.jobID, self.status )
    if self.tID and self.taskID:
      info += " Transformation: %d -- %d " % ( self.tID, self.taskID )
    if self.taskStatus:
      info += "TaskStatus: %s " % self.taskStatus
      if self.finalTask:
        info += "(File was processed in task %d)" % self.finalTask
    if self.inputFile:
      info += "\n---> inputFile: %s (%s)" % ( self.inputFile, self.inputFileExists )
    if self.outputFiles:
      info += "\n---> OutputFiles: "
      efInfo = [ "%s (%s)" % _ for _ in izip_longest( self.outputFiles, self.outputFileStatus ) ]
      info += ", ".join( efInfo )
    if self.pendingRequest:
      info += "\n PENDING REQUEST IGNORE THIS JOB!!!"
    else:
      info += "\n No Pending Requests"
    
    return info

  def allFilesExist( self ):
    """check if all files exists"""
    return all( "Exists" in status for status in self.outputFileStatus )

  def allFilesMissing( self ):
    """check if all files are missing"""
    return all( "Missing" in status for status in self.outputFileStatus )

  def someFilesMissing( self ):
    """check if some files are missing and therefore some files exist """
    return not ( self.allFilesExist() or self.allFilesMissing() )
  
  def getJobInformation( self, jobMon ):
    """get all the information for the job"""
    jdlString = self.__getJDL( jobMon )
    self.__getOutputFiles( jdlString )
    self.__getTaskID( jdlString )
    self.__getInputFile( jdlString )

  def getTaskInfo( self, tasksDict, lfnTaskDict ):
    """extract the task information from the taskDict"""
    if self.taskID not in tasksDict:
      #print "taskID %d not in tasksDict" % self.taskID
      taskDict = tasksDict[ lfnTaskDict[self.inputFile] ]
      self.finalTask = lfnTaskDict[self.inputFile]
    else:
      taskDict = tasksDict[self.taskID]

    #dict( FileID=fileID, LFN=lfn, Status=status )
    if self.inputFile != taskDict['LFN']:
      raise RuntimeError("Task info does not fit with job info: \n %s" % str(self) )
    self.taskStatus = taskDict['Status']
    self.taskFileID = taskDict['FileID']
    
  def checkFileExistance( self, fcClient ):
    """check if input and outputfile still exist"""
    lfns = [self.inputFile] + self.outputFiles
    reps = fcClient.exists( lfns )
    if not reps['OK']:
      raise RuntimeError( "Failed to check existance: %s" % reps['Message'] )
    statuses = reps['Value']
    success = statuses['Successful']

    self.inputFileExists = self.inputFile in success
    for lfn in self.outputFiles:
      if lfn in success and success[lfn]:
        self.outputFileStatus.append("Exists")
      elif lfn in success:
        self.outputFileStatus.append("Missing")
      else:
        self.outputFileStatus.append("Unknown")
      
  def checkRequests( self, reqClient ):
    """check if there are pending Requests"""
    result = reqClient.readRequestsForJobs( [self.jobID] )
    if not result['OK']:
      raise RuntimeError( "Failed to check Requests: %s " % result['Message'] )
    if self.jobID in result['Value']['Successful']:
      request = result['Value']['Successful'][self.jobID]
      self.pendingRequest = request.Status != "Done"
    
  def __getJDL( self, jobMon ):
    """return jdlstring for this job"""
    res = jobMon.getJobJDL(int(self.jobID), False)
    if not res['OK']:
      raise RuntimeError( "Failed to get jobJDL: %s" % res['Message'] )
    jdlString = res['Value']
    return jdlString

  def __getOutputFiles( self, jdlString ):
    """get the Production Outputfiles for the given Job"""
    if 'ProductionOutputData = "' in jdlString:
      lfns = JobInfo.__getSingleLFN( jdlString )
    else:
      lfns = JobInfo.__getMultiLFN(jdlString)
    self.outputFiles = lfns
    
  def __getInputFile( self, jdlString ):
    """get the Inputdata for the given job"""
    for val in jdlString.split('\n'):
      if 'InputData' in val:
        lfn = re.search('".*"', val)
        lfn = lfn.group(0).strip('"')
        self.inputFile = lfn
        
  def __getTaskID( self, jdlString ):
    """get the taskID """
    for val in jdlString.split('\n'):
      if 'TaskID' in val:
        try:
          self.taskID = int(val.strip(";").split("=")[1].strip(' "'))
        except ValueError:
          print "*"*80
          print "ERROR"
          print val
          print self
          print "*"*80
          raise
        break
    
    
  @staticmethod
  def __getSingleLFN(jdlString):
    """get the only productionOutputData LFN from the jdlString"""
    for val in jdlString.split('\n'):
      if 'ProductionOutputData' in val:
        lfn = re.search('".*"', val)
        lfn = lfn.group(0).strip('"')
        return [lfn]

  @staticmethod
  def __getMultiLFN( jdlString ):
    """ get multiple outputfiles """
    lfns = []
    counter = 0
    getEm = False
    for val in jdlString.split('\n'):
      if 'ProductionOutputData' in val:
        counter += 1
        continue
      if counter == 1 and '{' in val:
        getEm = True
        continue
      if '}' in val and getEm:
        break
      if getEm:
        lfn = re.search('".*"', val)
        lfn = lfn.group(0).strip('"')
        lfns.append(lfn)
    return lfns

  def setJobDone( self , tInfo ):
    """mark job as done in wms and transformationsystem"""
    if ENABLED:
      tInfo.setJobDone( self.taskID )

  def setJobFailed( self, tInfo ):
    """mark job as failed in  wms and transformationsystem"""
    if ENABLED:
      tInfo.setJobFailed( self.taskID )

  def setInputProcessed( self, tInfo ):
    """mark input file as Processed"""
    if ENABLED:
      tInfo.setInputProcessed( self )

  def setInputUnused( self, tInfo ):
    """mark input file as Unused"""
    if ENABLED:
      tInfo.setInputProcessed( self )

  def cleanOutputs( self, tInfo ):
    """remove all job outputs"""
    pass
