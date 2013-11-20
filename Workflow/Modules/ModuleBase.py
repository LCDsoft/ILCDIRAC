#####################################################
# $HeadURL$
#####################################################
""" 
Base class for ILC workflow modules. 

Stolen by S. Poss from LHCbSystem.Workflow.Modules

@since: Feb 02, 2010
@author: S. Poss
@author: S. Paterson
"""
import shutil

__RCSID__ = "$Id$"

from DIRAC                                                import S_OK, S_ERROR, gLogger
from DIRAC.Core.Security.ProxyInfo                        import getProxyInfoAsString
from DIRAC.Core.Utilities.Adler                           import fileAdler
from DIRAC.TransformationSystem.Client.FileReport         import FileReport
from DIRAC.Core.Utilities.File                            import makeGuid
from DIRAC.ConfigurationSystem.Client.Helpers.Operations  import Operations
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import getSoftwareFolder
from ILCDIRAC.Core.Utilities.FindSteeringFileDir          import getSteeringFileDir
from ILCDIRAC.Core.Utilities.InputFilesUtilities          import getNumberOfevents

import os, string, sys, re, types, urllib, glob
from random import choice

def GenRandString(length=8, chars = string.letters + string.digits):
  """Return random string of 8 chars, used by Pythia and Mokka
  """
  return ''.join([choice(chars) for i in range(length)])

class ModuleBase(object):
  """ Base class of the ILCDIRAC modules. Several common utilities are defined here.
  In particular, all sub classes should call the L{resolveInputVariables} method, and implement
  the L{applicationSpecificInputs}.
  """
  #############################################################################
  def __init__(self):
    """ Initialization of module base.
    """
    super(ModuleBase, self).__init__()
    self.log = gLogger.getSubLogger('ModuleBase')
    # FIXME: do we need to do this for every module?
    result = getProxyInfoAsString()
    if not result['OK']:
      self.log.error('Could not obtain proxy information in module environment with message:\n', result['Message'])
    else:
      self.log.info('Payload proxy information:\n', result['Value'])

    self.ops = Operations()

    self.systemConfig = ''
    self.applicationLog = ''
    self.applicationVersion = ''
    self.applicationName = ''
    self.InputData = [] #Will have to become empty list
    self.SteeringFile = ''
    self.energy = 0
    self.NumberOfEvents = 0
    self.WorkflowStartFrom = 0
    self.result = S_ERROR()
    self.InputFile = []
    self.ignoremissingInput = False
    self.OutputFile = ''
    self.jobType = ''
    self.stdError = ''
    self.debug = False
    self.extraCLIarguments = ""
    self.jobID = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']
    self.eventstring = ['']
    self.excludeAllButEventString = False
    self.ignoreapperrors = False
    self.inputdataMeta = {}
    #############
    #Set from workflow object
    self.workflow_commons = {}
    self.step_commons = {}
    self.workflowStatus = S_OK()
    self.stepStatus = S_OK()
    self.isProdJob = False
    
    self.basedirectory = os.getcwd()
    

  #############################################################################
  def setApplicationStatus(self, status, sendFlag=True):
    """Wraps around setJobApplicationStatus of state update client
    """
    if not self.jobID:
      return S_OK('JobID not defined') # e.g. running locally prior to submission

    self.log.verbose('setJobApplicationStatus(%s,%s)' %(self.jobID, status))

    if self.workflow_commons.has_key('JobReport'):
      self.jobReport  = self.workflow_commons['JobReport']

    if not self.jobReport:
      return S_OK('No reporting tool given')
    jobStatus = self.jobReport.setApplicationStatus(status, sendFlag)
    if not jobStatus['OK']:
      self.log.warn(jobStatus['Message'])

    return jobStatus

  #############################################################################
  def sendStoredStatusInfo(self):
    """Wraps around sendStoredStatusInfo of state update client
    """
    if not self.jobID:
      return S_OK('JobID not defined') # e.g. running locally prior to submission

    if self.workflow_commons.has_key('JobReport'):
      self.jobReport = self.workflow_commons['JobReport']

    if not self.jobReport:
      return S_OK('No reporting tool given')

    sendStatus = self.jobReport.sendStoredStatusInfo()
    if not sendStatus['OK']:
      self.log.error(sendStatus['Message'])

    return sendStatus

  #############################################################################
  def setJobParameter(self, name, value, sendFlag = True):
    """Wraps around setJobParameter of state update client
    @param name: job parameter 
    @param value: value of the job parameter
    @param sendFlag: passed to setJobParameter
    @return: S_OK(), S_ERROR()
    """
    if not self.jobID:
      return S_OK('JobID not defined') # e.g. running locally prior to submission

    self.log.verbose('setJobParameter(%s,%s,%s)' % (self.jobID, name, value))

    if self.workflow_commons.has_key('JobReport'):
      self.jobReport = self.workflow_commons['JobReport']

    if not self.jobReport:
      return S_OK('No reporting tool given')
    jobParam = self.jobReport.setJobParameter(str(name), str(value), sendFlag)
    if not jobParam['OK']:
      self.log.warn(jobParam['Message'])

    return jobParam

  #############################################################################
  def sendStoredJobParameters(self):
    """Wraps around sendStoredJobParameters of state update client
    """
    if not self.jobID:
      return S_OK('JobID not defined') # e.g. running locally prior to submission

    if self.workflow_commons.has_key('JobReport'):
      self.jobReport = self.workflow_commons['JobReport']

    if not self.jobReport:
      return S_OK('No reporting tool given')

    sendStatus = self.jobReport.sendStoredJobParameters()
    if not sendStatus['OK']:
      self.log.error(sendStatus['Message'])

    return sendStatus

  #############################################################################
  def setFileStatus(self, production, lfn, status):
    """ Set the file status for the given production in the Production Database
    
    @param production: production ID
    @param lfn: logical file name of the file that needs status change
    @param status: status to set
    @return: S_OK(), S_ERROR()
    """
    self.log.verbose('setFileStatus(%s,%s,%s)' %(production, lfn, status))

    if not self.workflow_commons.has_key('FileReport'):
      fileReport = FileReport('Transformation/TransformationManager')
      self.workflow_commons['FileReport'] = fileReport

    fileReport = self.workflow_commons['FileReport']
    result = fileReport.setFileStatus(production, lfn, status)
    if not result['OK']:
      self.log.warn(result['Message'])

    self.workflow_commons['FileReport'] = fileReport

    return result

  #############################################################################
#  def setReplicaProblematic(self, lfn, se, pfn = '', reason = 'Access failure'):
#    """ Set replica status to Problematic in the File Catalog
#    @param lfn: lfn of the problematic file
#    @param se: storage element
#    @param pfn: physical file name
#    @param reason: as name suggests...
#    @return: S_OK()
#    """
#
#    rm = ReplicaManager()
#    source = "Job %d at %s" % (self.jobID, DIRAC.siteName())
#    result = rm.setReplicaProblematic((lfn, pfn, se, reason), source)
#    if not result['OK'] or result['Value']['Failed']:
#      # We have failed the report, let's attempt the Integrity DB faiover
#      integrityDB = RPCClient('DataManagement/DataIntegrity', timeout=120)
#      fileMetadata = { 'Prognosis' : reason, 'LFN' : lfn, 'PFN' : pfn, 'StorageElement' : se }
#      result = integrityDB.insertProblematic(source, fileMetadata)
#      if not result['OK']:
#        # Add it to the request
#        if self.workflow_commons.has_key('Request'):
#          request  = self.workflow_commons['Request']
#          subrequest = DISETSubRequest(result['rpcStub']).getDictionary()
#          request.addSubRequest(subrequest, 'integrity')
#
#    return S_OK()

  #############################################################################
  def getCandidateFiles(self, outputList, outputLFNs, fileMask):
    """ Returns list of candidate files to upload, check if some outputs are missing.
        
      @param outputList: has the following structure:
      [ ('outputDataType':'','outputDataSE':'','outputDataName':'') , (...) ] 
          
      @param outputLFNs: list of output LFNs for the job
        
      @param fileMask:  output file extensions to restrict the outputs to
        
      @return: dictionary containing type, SE and LFN for files restricted by mask
    """
    fileInfo = {}
    for outputFile in outputList:
      if outputFile.has_key('outputFile') and outputFile.has_key('outputDataSE') and outputFile.has_key('outputPath'):
        fname = outputFile['outputFile']
        fileSE = outputFile['outputDataSE']
        filePath = outputFile['outputPath']
        fileInfo[fname] = {'path' : filePath, 'workflowSE' : fileSE}
      else:
        self.log.error('Ignoring malformed output data specification', str(outputFile))

    for lfn in outputLFNs:
      if os.path.basename(lfn) in fileInfo.keys():
        fileInfo[os.path.basename(lfn)]['lfn']=lfn
        self.log.verbose('Found LFN %s for file %s' %(lfn, os.path.basename(lfn)))
        if len(os.path.basename(lfn))>127:
          self.log.error('Your file name is WAAAY too long for the FileCatalog. Cannot proceed to upload.')
          return S_ERROR('Filename too long')
        if len(lfn)>256+127:
          self.log.error('Your LFN is WAAAAY too long for the FileCatalog. Cannot proceed to upload.')
          return S_ERROR('LFN too long')
        
    #Check that the list of output files were produced
    for fileName, metadata in fileInfo.items():
      if not os.path.exists(fileName):
        self.log.error('Output data file %s does not exist locally' % fileName)
        if not self.ignoreapperrors:
          return S_ERROR('Output Data Not Found')
        del fileInfo[fileName]
    #Check the list of files against the output file mask (if it exists)
    #candidateFiles = {}
    #if fileMask:
      ##nothing to do yet, as FileMask is not used
      #for fileName,metadata in fileInfo.items():
      #  if metadata['type'].lower() in fileMask or fileName.split('.')[-1] in fileMask:
      #    candidateFiles[fileName]=metadata
      #  else:
      #    self.log.info('Output file %s was produced but will not be treated (outputDataFileMask is %s)' %(fileName,
      #                                                                    string.join(self.outputDataFileMask,', ')))

      #if not candidateFiles.keys():
      #  return S_OK({}) #nothing to do
    #  candidateFiles = fileInfo
    #else:
      #do not apply mask to files
      
    candidateFiles = fileInfo
    #Sanity check all final candidate metadata keys are present (return S_ERROR if not)
    mandatoryKeys = ['path', 'workflowSE', 'lfn'] #filedict is used for requests
    for fileName, metadata in candidateFiles.items():
      for key in mandatoryKeys:
        if not metadata.has_key(key):
          return S_ERROR('File %s has missing %s' % (fileName, key))
    
    return S_OK(candidateFiles)  

  #############################################################################
  def getFileMetadata(self, candidateFiles):
    """Returns the candidate file dictionary with associated metadata.
    
    @param candidateFiles: The input candidate files dictionary has the structure:
    {'lfn':'','path':'','workflowSE':''}
       
    This also assumes the files are in the current working directory.
    @return: File Metadata
    """
    #Retrieve the POOL File GUID(s) for any final output files
    self.log.info('Will search for POOL GUIDs for: %s' %(', '.join(candidateFiles.keys())))
    pfnGUIDs = {}
    generated = []
    for fname in candidateFiles.keys():
      guid = makeGuid(fname)
      pfnGUIDs[fname] = guid
      generated.append(fname)
    pfnGUID = S_OK(pfnGUIDs)
    pfnGUID['generated'] = generated
    #pfnGUID = getGUID(candidateFiles.keys())
    #if not pfnGUID['OK']:
    #  self.log.error('PoolXMLFile failed to determine POOL GUID(s) for output file list, these will be generated by \
    #                   the ReplicaManager',pfnGUID['Message'])
    #  for fileName in candidateFiles.keys():
    #    candidateFiles[fileName]['guid']=''
    #if pfnGUID['generated']:
    self.log.debug('Generated GUID(s) for the following files ', ', '.join(pfnGUID['generated']))
    #else:
    #  self.log.info('GUIDs found for all specified POOL files: %s' %(string.join(candidateFiles.keys(),', ')))

    for pfn, guid in pfnGUID['Value'].items():
      candidateFiles[pfn]['GUID'] = guid

    #Get all additional metadata about the file necessary for requests
    final = {}
    for fileName, metadata in candidateFiles.items():
      fileDict = {}
      fileDict['LFN'] = metadata['lfn']
      fileDict['Size'] = os.path.getsize(fileName)
      fileDict['Addler'] = fileAdler(fileName)
      fileDict['GUID'] = metadata['GUID']
      fileDict['Status'] = "Waiting"   
      
      final[fileName] = metadata
      final[fileName]['filedict'] = fileDict
      final[fileName]['localpath'] = '%s/%s' % (os.getcwd(), fileName)  

    #Sanity check all final candidate metadata keys are present (return S_ERROR if not)
    mandatoryKeys = ['GUID', 'filedict'] #filedict is used for requests (this method adds guid and filedict)
    for fileName, metadata in final.items():
      for key in mandatoryKeys:
        if not metadata.has_key(key):
          return S_ERROR('File %s has missing %s' % (fileName, key))

    return S_OK(final)
  
  def resolveInputVariables(self):
    """ Common utility for all sub classes, resolve the workflow parameters 
    for the current step. Module parameters are resolved directly. 
    """
    self.log.verbose("Workflow commons:", self.workflow_commons)
    self.log.verbose("Step commons:", self.step_commons)
    if self.workflow_commons.has_key("IS_PROD"):
      if self.workflow_commons["IS_PROD"]:
        self.isProdJob = True
        
    if self.workflow_commons.has_key('SystemConfig'):
      self.systemConfig = self.workflow_commons['SystemConfig']
      
    if self.workflow_commons.has_key('IgnoreAppError'):
      self.ignoreapperrors = self.workflow_commons['IgnoreAppError']
      
    if self.step_commons.has_key('applicationName'):
      self.applicationName = self.step_commons['applicationName']
      
    if self.step_commons.has_key('applicationVersion'):
      self.applicationVersion = self.step_commons['applicationVersion']
      
    if self.step_commons.has_key('applicationLog'):
      self.applicationLog = self.step_commons['applicationLog']
    
    if self.step_commons.has_key('ExtraCLIArguments'):
      self.extraCLIarguments = urllib.unquote(self.step_commons['ExtraCLIArguments']) 
      
    if self.step_commons.has_key('SteeringFile'):
      self.SteeringFile = self.step_commons['SteeringFile']
      
    if self.workflow_commons.has_key('JobType'):
      self.jobType = self.workflow_commons['JobType']
      
    if self.workflow_commons.has_key('Energy'):
      self.energy = self.workflow_commons['Energy']

    if self.workflow_commons.has_key('NbOfEvts'):
      if self.workflow_commons['NbOfEvts'] > 0:
        self.NumberOfEvents = self.workflow_commons['NbOfEvts']

    if 'StartFrom' in self.workflow_commons:
      if self.workflow_commons['StartFrom'] > 0:
        self.WorkflowStartFrom = self.workflow_commons['StartFrom']

    if self.step_commons.has_key('InputFile'):
      ### This must stay, otherwise, linking between steps is impossible: OutputFile is a string 
      inputf = self.step_commons['InputFile']
      if not type(inputf) == types.ListType:
        if len(inputf):
          inputf = inputf.split(";")
        else:
          inputf = []
      self.InputFile = inputf
    
    if self.step_commons.has_key('ForgetInput'):
      self.ignoremissingInput = self.step_commons['ForgetInput']
            
    if self.workflow_commons.has_key('InputData'):
      inputdata = self.workflow_commons['InputData']
      if not type(inputdata) == types.ListType:
        if len(inputdata):
          self.InputData = inputdata.split(";")
          self.InputData = [x.replace("LFN:","") for x in self.InputData]
      
    if self.workflow_commons.has_key('ParametricInputData'):
      paramdata = self.workflow_commons['ParametricInputData']
      if not type(paramdata) == types.ListType:
        if len(paramdata):
          self.InputData = paramdata.split(";")

    if not self.OutputFile:
      if self.step_commons.has_key("OutputFile"):
        self.OutputFile = self.step_commons["OutputFile"]

    #Next is also a module parameter, should be already set
    if self.step_commons.has_key('debug'):
      self.debug = self.step_commons['debug']

          
    if self.InputData:
      res = getNumberOfevents(self.InputData)
      self.inputdataMeta.update(res['AdditionalMeta'])
      if res["nbevts"]:
        if self.NumberOfEvents > res['nbevts'] or self.NumberOfEvents == 0:
          self.NumberOfEvents = res['nbevts']
        
    res = self.applicationSpecificInputs()
    if not res['OK']:
      return res
    return S_OK('Parameters resolved')
  
  def applicationSpecificInputs(self):
    """ Method overwritten by sub classes. Called from the above.
    """
    return S_OK()

  def execute(self):
    """ The execute method. This is called by the workflow wrapper when the module is needed
    Here we do preliminary things like resolving the application parameters, and getting a dedicated directory
    """
    workdir = os.path.join(self.basedirectory, self.step_commons["STEP_DEFINITION_NAME"])
    if not os.path.exists(workdir):
      try:
        os.makedirs( workdir )
      except OSError, e:
        self.log.error("Failed to create the work directory :", str(e))
    
    #now go there
    os.chdir( workdir )
    self.log.verbose("We are now in ", workdir)
    
    result = self.resolveInputVariables()
    if not result['OK']:
      self.log.error("Failed to resolve input variables:", result['Message'])
      return result

    if self.InputFile:
      ##Try to copy the input file to the work fdfir
      for inf in self.InputFile:
        bpath = os.path.join(self.basedirectory, inf)
        if os.path.exists(bpath):
          try:
            shutil.move(bpath, "./"+inf)
          except EnvironmentError, why:
            self.log.error("Failed to get the file:", str(why))
    
          
    if self.SteeringFile:
      bpath = os.path.join(self.basedirectory, os.path.basename(self.SteeringFile))
      if os.path.exists(bpath):
        try:
          shutil.move(bpath, "./"+os.path.basename(self.SteeringFile))
        except EnvironmentError, why:
          self.log.error("Failed to get the file:", str(why))
          
    # because we need to make sure this does not overwrite steering files provided  by the user
    # it must be here.
    if "SteeringFileVers" in self.step_commons:
      steeringfilevers = self.step_commons["SteeringFileVers"]
      self.log.verbose("Will get all the files from the steeringfiles%s" % steeringfilevers)
      res = getSteeringFileDir(self.systemConfig, steeringfilevers)
      if not res['OK']:
        self.log.error("Cannot find the steering file directory: %s" % steeringfilevers, 
                       res['Message'])
        return S_ERROR("Failed to locate steering files %s" % steeringfilevers)
      path = res['Value']
      list_f = os.listdir(path)
      for f in list_f:
        if os.path.exists("./"+f):
          self.log.verbose("Found local file, don't overwrite")
          #Do not overwrite local files with the same name
          continue
        try:
          if os.path.isdir(os.path.join(path, f)):
            shutil.copytree(os.path.join(path, f), "./"+f)
          else:
            shutil.copy2(os.path.join(path, f), "./"+f)
        except EnvironmentError, why:
          self.log.error('Could not copy %s here because :' % f, str(why) )

    if 'ILDConfigPackage' in self.workflow_commons:
      config_dir = self.workflow_commons['ILDConfigPackage']
      #seems it's not on CVMFS, try local install then:
      res = getSoftwareFolder(self.systemConfig, "ILDConfig", config_dir.replace("ILDConfig", ""))
      if not res['OK']:
        self.log.error("Cannot find %s" % config_dir, res['Message'])
        return S_ERROR('Failed to locate %s as config dir' % config_dir)
      path = res['Value']
      list_f = os.listdir(path)
      for f in list_f:
        if os.path.exists("./"+f):
          self.log.verbose("Found local file, don't overwrite")
          #Do not overwrite local files with the same name
          continue
        try:
          if os.path.isdir(os.path.join(path, f)):
            shutil.copytree(os.path.join(path, f), "./"+f)
          else:
            shutil.copy2(os.path.join(path, f), "./"+f)
        except EnvironmentError, why:
          self.log.error('Could not copy %s here because %s!' % (f, str(why)))


    if self.SteeringFile:
      if os.path.exists(os.path.basename(self.SteeringFile)):
        self.log.verbose("Found local copy of %s" % self.SteeringFile)
 
    
    if os.path.isdir(os.path.join(self.basedirectory, 'lib')):
      try:
        shutil.copytree(os.path.join(self.basedirectory, 'lib'), './lib')
      except EnvironmentError, why:
        self.log.error("Failed to get the lib directory:", str(why))
    
    if "Required" in self.step_commons:
      reqs = self.step_commons["Required"].rstrip(";").split(";")
      for reqitem in reqs:
        if os.path.exists(reqitem):
          #file or dir is already here
          continue
        if os.path.exists(os.path.join(self.basedirectory, reqitem)):
          try:
            if os.path.isdir(os.path.join(self.basedirectory, reqitem)):
              shutil.copytree(os.path.join(self.basedirectory, reqitem), reqitem)
            else:
              shutil.copy2(os.path.join(self.basedirectory, reqitem), reqitem)
          except EnvironmentError, why:
            self.log.error("Failed to get %s:" % reqitem, str(why))
        else:
          self.log.error("Missing file or folder in base directory: ", reqitem)
          return S_ERROR("Missing item %s in base directory" % reqitem)
    
    try:
      self.applicationSpecificMoveBefore()    
    except EnvironmentError, e:
      self.log.error("Failed to copy the required files", str(e))
      return S_ERROR("Failed to copy the required files%s" % str(e))
    
    before_app_dir = os.listdir(os.getcwd())

    appres = self.runIt()
    if not appres["OK"]:
      self.log.error("Somehow the application did not exit properly")
    
    ##Try to move things back to the base directory
    if self.OutputFile:
      for ofile in glob.glob("*"+self.OutputFile+"*"):
        try:
          shutil.move(ofile, os.path.join(self.basedirectory, ofile))
        except EnvironmentError, why:
          self.log.error('Failed to move the file back to the main directory:', str(why))
          appres = S_ERROR("Failed moving files")
          
    if os.path.exists(self.applicationLog):
      try:
        shutil.move("./"+self.applicationLog, os.path.join(self.basedirectory, self.applicationLog))
      except EnvironmentError, why:
        self.log.error("Failed to move the log to the basedir", str(why))
      
    try:
      self.applicationSpecificMoveAfter()
    except EnvironmentError, e:
      self.log.warn("Failed to move things back, next step may fail")
      
    #now move all the new stuff that wasn't moved before
    for item in os.listdir(os.getcwd()):
      if item not in before_app_dir and item != os.path.basename(self.SteeringFile) and not os.path.isdir(item):        
        try:
          shutil.move("./" + item, os.path.join(self.basedirectory, item) )
        except EnvironmentError, why:
          self.log.error("Failed to move the file %s to the basedir" % item, str(why))
        
    #move the InputFile back too if it's here
    for inf in self.InputFile:
      localname = os.path.join("./", os.path.basename(inf))
      if os.path.exists(localname):
        try:
          shutil.move(localname, os.path.join(self.basedirectory, os.path.basename(inf)))
        except EnvironmentError, why:
          self.log.error("Failed to move the input file back to the basedir", str(why))
      
    ##Now we go back to the base directory
    os.chdir(self.basedirectory)
    
    self.log.verbose("We are now back to ", self.basedirectory)
    self.listDir()
    
    return appres
  
  def listDir(self):
    """ List the current directories content
    """
    ldir = os.listdir(os.getcwd())
    self.log.verbose("Base directory content:", "\n".join(ldir))
  
  def runIt(self):
    """ Dummy call, needs to be overwritten by the actual applications
    """
    return S_OK()

  def applicationSpecificMoveBefore(self):
    """ If some application need specific things: Marlin needs the GearFile from Mokka
    """
    return S_OK()
  
  def applicationSpecificMoveAfter(self):
    """ If some application need specific things: Marlin needs send back its output
    """
    return S_OK()

  def finalStatusReport(self, status):
    """ Catch the resulting application status, and return corresponding workflow status
    """
    message = '%s %s Successful' % (self.applicationName, self.applicationVersion)
    if status:
      self.log.error( "==================================\n StdError:\n" )
      self.log.error( self.stdError )
      message = '%s exited With Status %s' % (self.applicationName, status)
      self.setApplicationStatus(message)
      self.log.error(message)
      if not self.ignoreapperrors:
        return S_ERROR(message)
    else: 
      self.setApplicationStatus('%s %s Successful' % (self.applicationName, self.applicationVersion))
    return S_OK(message)    

  def redirectLogOutput(self, fd, message):
    """Catch the output from the application
    """
    sys.stdout.flush()
    if message:
      if type(self.eventstring) == type(' '):
        self.eventstring = [self.eventstring]
      if len(self.eventstring): 
        if len(self.eventstring[0]):
          for mystring in self.eventstring:
            if re.search(mystring, message):
              print message 
      else:
        print message
      if self.applicationLog:
        log = open(self.applicationLog, 'a')
        if self.excludeAllButEventString:
          if len(self.eventstring): 
            if len(self.eventstring[0]):  
              for mystring in self.eventstring:
                if re.search(mystring, message):
                  log.write(message+'\n')
        else:
          log.write(message+'\n')
        log.close()
      else:
        self.log.error("Application Log file not defined")
    if fd == 1:
      self.stdError += message
        